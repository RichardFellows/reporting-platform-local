"""Long-running console operations, run in the background and polled.

A targeted dbt build takes minutes. Holding an HTTP request open for that
gives the caller one bit of information at the end, and nothing at all while
it runs -- which for a dev waiting on their own feed's tests is the wrong way
round: the interesting part is the failing test's output, and they want it as
it appears.

So a job is started, gets an id, and streams its output into a bounded buffer
the UI polls. `__main__.py` runs a single uvicorn worker, so this in-process
registry is the whole story -- there is no second process holding a different
view of it.

ONLY ONE BUILD AT A TIME, deliberately. Every Spark application on this
platform caps itself at 2 cores of the worker's 6 (see `spark_session`), and
Airflow's own builds are serialised through the `lakehouse_write` pool. A
console that let someone start five builds would be the one component able to
starve that pool from outside it, and the symptom -- a DAG run sitting at
"Initial job has not accepted any resources" -- points nowhere near the
console. A second request is refused with the id of the one already running.

WHICH IS WHY A JOB MUST BE ABLE TO END. Exclusivity and an unbounded wait
together are a wedge: a dbt run blocked on the `lakehouse_write` pool or on a
Nessie call that never answers leaves the job `running` for the life of the
process, every later build is refused with its id, and the eviction below
deliberately never reclaims a running job. Restarting the container was the
only way out. So there are two ways for a job's subprocess to stop -- a
deadline it cannot outlive, and `cancel()` -- and both leave the job FAILED
with the reason in its log, which is a state the console already renders.

KILLED BY PROCESS GROUP, not by pid. `dbt` here spawns `spark-submit`, which
spawns a JVM; terminating only the direct child orphans the part actually
holding the cluster's cores. `start_new_session=True` puts the whole tree in
its own group so one signal reaches all of it.
"""
from __future__ import annotations

import os
import signal
import subprocess
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

MAX_LINES = 4000

# How long a job's subprocess may run before it is killed. Generous, because
# a real dbt build against the cluster is minutes and a cold Spark session is
# most of a minute of that -- this is the line between "slow" and "stuck", not
# a performance budget. Overridable for a slower box.
JOB_TIMEOUT_SECONDS = float(os.environ.get("FEED_UI_JOB_TIMEOUT", 3600))

# Grace between SIGTERM and SIGKILL. dbt handles SIGTERM and prints why it
# stopped, which is worth waiting for; a JVM that ignores it is not.
KILL_GRACE_SECONDS = 15


class JobBusy(RuntimeError):
    """An exclusive job is already running. Carries the running job's id."""

    def __init__(self, job_id: str, kind: str):
        self.job_id = job_id
        self.kind = kind
        super().__init__(f"a {kind} job is already running ({job_id})")


@dataclass
class Job:
    id: str
    kind: str
    label: str
    status: str = "running"          # running | success | failed
    lines: deque = field(default_factory=lambda: deque(maxlen=MAX_LINES))
    started: str = ""
    finished: str | None = None
    exit_code: int | None = None
    result: dict[str, Any] = field(default_factory=dict)
    # Lines ever produced, not the buffer length -- see snapshot().
    produced: int = 0
    # Why the job's subprocess was stopped from outside: 'timeout', 'cancel',
    # or None for one that ran to its own end. Reported, because a build that
    # was killed and a build that failed its tests are different answers and
    # a non-zero exit code alone cannot tell them apart.
    stopped: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _proc: subprocess.Popen | None = field(default=None, repr=False)

    def log(self, text: str) -> None:
        """The ONLY way lines enter the job.

        Appending to `lines` directly would leave `produced` behind, and
        `produced` is what a polling caller uses to keep its place -- the
        buffer would then look like it had replayed old output as new.
        """
        with self._lock:
            for line in text.splitlines() or [""]:
                self.lines.append(line)
                self.produced += 1

    def snapshot(self, since: int = 0) -> dict[str, Any]:
        """State plus the lines after `since`.

        `total` counts lines EVER produced, so a caller polling with `since`
        keeps its place even once the bounded buffer has begun discarding from
        the front.
        """
        with self._lock:
            lines = list(self.lines)
            total = self.produced
        start = max(0, since - (total - len(lines)))
        return {
            "id": self.id, "kind": self.kind, "label": self.label,
            "status": self.status, "started": self.started,
            "finished": self.finished, "exit_code": self.exit_code,
            "result": self.result, "stopped": self.stopped,
            "lines": lines[start:], "total": total,
        }

    def stop(self, reason: str) -> bool:
        """Signal this job's subprocess, if it has one running.

        Returns whether there was anything to signal. Safe to call from
        another thread, and safe to call twice: the second call finds the
        process already gone and says so.
        """
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                return False
            self.stopped = reason
        _signal_group(proc)
        return True


_jobs: dict[str, Job] = {}
_registry_lock = threading.Lock()


def running(kind: str) -> Job | None:
    with _registry_lock:
        for job in _jobs.values():
            if job.kind == kind and job.status == "running":
                return job
    return None


def get(job_id: str) -> Job | None:
    with _registry_lock:
        return _jobs.get(job_id)


def cancel(job_id: str) -> bool:
    """Stop a running job's subprocess. Returns False if there was none.

    The job then FAILS in the ordinary way -- its subprocess exits non-zero,
    the target raises, `_run` records it -- so nothing here has to reach into
    the job's status. What a caller needs beyond that is in `stopped`.
    """
    job = get(job_id)
    return bool(job and job.stop("cancel"))


def _signal_group(proc: subprocess.Popen) -> None:
    """SIGTERM the process's whole group, then SIGKILL what is left.

    By group because of the dbt -> spark-submit -> JVM chain (see the module
    header). `os.killpg` on a process that has already exited raises
    ProcessLookupError, which is the race this is called in the middle of.
    """
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=KILL_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def start(kind: str, label: str, target, exclusive: bool = True) -> Job:
    """Run `target(job)` on a background thread.

    `target` is handed the Job so it can log, set `result`, and raise to fail.
    """
    if exclusive:
        current = running(kind)
        if current is not None:
            raise JobBusy(current.id, kind)

    job = Job(id=uuid.uuid4().hex[:12], kind=kind, label=label,
              started=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    with _registry_lock:
        _jobs[job.id] = job
        # Keep the registry from growing for the life of the process. Finished
        # jobs are only useful until someone has read them.
        if len(_jobs) > 40:
            for old in sorted([j for j in _jobs.values() if j.status != "running"],
                              key=lambda j: j.started)[:10]:
                _jobs.pop(old.id, None)

    def _run() -> None:
        try:
            target(job)
            job.status = "success"
        except Exception as exc:                               # noqa: BLE001
            job.log(f"\n{type(exc).__name__}: {exc}")
            job.status = "failed"
        finally:
            job.finished = datetime.now(timezone.utc).isoformat(timespec="seconds")

    threading.Thread(target=_run, name=f"job-{job.id}", daemon=True).start()
    return job


def stream(job: Job, args: list[str], cwd: str | None = None,
           timeout: float | None = None) -> int:
    """Run a subprocess, streaming its output into the job as it arrives.

    Line-buffered and merged stderr into stdout: dbt writes its progress to
    stdout and its errors to both depending on the failure, and interleaving
    them in arrival order is what makes the log readable. `capture_output`
    with a wait would give the same bytes and none of the point.

    BOUNDED BY A DEADLINE. Reading `proc.stdout` to exhaustion is an unbounded
    wait on a process that may never write another line, and this job holds an
    exclusive slot while it waits -- see the module header. The deadline is
    enforced by a timer rather than by a read timeout because the blocking call
    is the iteration itself; killing the process is what ends it.
    """
    timeout = JOB_TIMEOUT_SECONDS if timeout is None else timeout
    job.log("$ " + " ".join(args))
    proc = subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1,
                            # Its own process group, so one signal reaches the
                            # whole dbt -> spark-submit -> JVM tree.
                            start_new_session=True)
    with job._lock:
        job._proc = proc
    deadline = threading.Timer(timeout, job.stop, args=("timeout",))
    deadline.daemon = True
    deadline.start()
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            job.log(line.rstrip("\n"))
        code = proc.wait()
    finally:
        deadline.cancel()
        with job._lock:
            job._proc = None
    if job.stopped == "timeout":
        job.log(f"\nKILLED after {timeout:.0f}s without finishing. The job "
                f"held the only {job.kind} slot, so it was stopped rather "
                f"than left to hold it for the life of the process.")
    elif job.stopped == "cancel":
        job.log("\nCANCELLED on request.")
    job.exit_code = code
    return code
