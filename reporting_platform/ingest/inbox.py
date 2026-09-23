"""Watch a host folder and ingest whatever is dropped into it.

Drop `TRADE_20260901.csv` into `inbox/` and it lands in
`landing/fo_trade/TRADE_20260901.csv`, the ingest DAG runs, and the file moves
to `inbox/.processed/trade/`. That is what an upstream actually does: write a
file to a directory. The console's upload button is for a person with a file
in their hand; this is for the twenty that arrive overnight.

POLLED, NOT inotify, AND THAT IS DELIBERATE. Filesystem events do not cross a
Docker Desktop bind mount on Windows or macOS -- the host writes the file, the
container is never told, and a watcher built on inotify sits there reporting
itself healthy while nothing happens.
See docs/DECISIONS.md#inbox-is-polled.

FOUR THINGS IT DOES THAT A NAIVE LOOP WOULD NOT:

**It waits for the file to stop changing.** A file appears when it is created,
not when it is finished; a half-written CSV ingests cleanly and
`expected_min_rows` is the only thing between that and a silent truncation.
Ready means size and mtime unchanged across two consecutive polls.

**It routes by the feeds' own filename patterns**, so no configuration here
repeats `feeds.yml`. A file matching no feed moves to `.rejected/` rather than
being left for the watcher to retry forever.

**A file matching MORE than one feed is rejected, not guessed.** Overlapping
patterns are a configuration error, and picking one arbitrarily would put a
delivery in the wrong raw table -- which looks like data, not an error.

**It moves the file before triggering.** If the trigger fails the file is
already out of the way and recorded as landed, so the next pass does not
re-upload it as a new `_file_version`. A duplicate ingest is much harder to
undo than a retrigger.

AND IT IS THE CONFORMANCE GATE. For a feed with an `arrival:` block this
watcher waits for the control file, verifies what it declares, derives the COB
date, and promotes the delivery under the name `filename_pattern` describes,
with a `.meta.json` sibling. A feed with no `arrival:` block is a conformant
upstream and its file is uploaded under its own name.

**An unchanged file sent twice lands nothing.** The gate compares the bytes
against what is already landed for that COB date and reports `duplicate`. A
conformant upstream gets this free by writing the same key twice; the gate had
to be taught it, because its rename would otherwise turn the second copy into
`_v2` and the raw table into a restatement that never happened.

That gate is why the two names in play are different strings and must stay so:
`positions.csv` is what the upstream sends, `trs_position_20260801.csv` is
what landing holds. `Feed.claims_source` answers the first,
`Feed.parse_filename` the second, and `ingest/conform.py` is the only thing
that crosses between them.
See docs/DECISIONS.md#the-inbox-is-the-conformance-gate.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from reporting_platform.common.context import Feed, feeds
from reporting_platform.ingest import conform
from reporting_platform.ingest import normalize as norm
from reporting_platform.ingest.arrival import (
    landed_md5_lookup, list_landing, put_landing, put_landing_bytes,
)

log = logging.getLogger("inbox")

INBOX = Path(os.environ.get("REPORTING_INBOX", "/opt/platform/inbox"))

# Whether a landed delivery triggers its ingest DAG. `--no-trigger` turns it
# off for a caller that ingests by itself and has no Airflow to call --
# scripts/ci_build_tier.sh, which runs the gate then `bulk_ingest`. Without it
# a stack with no webserver logs a traceback per delivery, and the exception
# abandons the rest of that pass.
TRIGGER = True
PROCESSED = ".processed"
REJECTED = ".rejected"
# The --loop health server's port (deploy/helm/reporting-platform/inbox.yaml's
# liveness/readiness probes). Not published as a host port in
# docker-compose.yml: nothing outside the container needs it there, only a
# Kubernetes probe does.
INBOX_HEALTH_PORT = int(os.environ.get("INBOX_HEALTH_PORT", "8090"))
# Two consecutive identical observations, so a file being written is not
# uploaded half-finished. At the default interval that is a few seconds of
# quiet, which every real delivery has and no partial write does.
STABLE_POLLS = 2


def _skip(path: Path) -> bool:
    """Directories, dotfiles and our own bookkeeping folders."""
    return (path.is_dir() or path.name.startswith(".")
            or path.name.endswith((".tmp", ".part", ".crdownload", ".filepart")))


def route(filename: str) -> tuple[Feed | None, str | None, bool]:
    """Which feed claims this filename, by the feeds' own patterns.

    Returns (feed, reason-it-was-rejected, is_control). Exactly one of the
    first two is set.

    FOUR THINGS CAN CLAIM A NAME, checked in this order:

    1. `filename_pattern` -- an ALREADY-CONFORMANT delivery, dropped into the
       inbox rather than PUT straight to landing. Uploaded under its own name.
    2. `arrival.source_pattern` -- a legacy delivery under the name its
       upstream sends. Goes through the conformance gate and is renamed.
    3. `arrival.control.pattern` -- a legacy delivery's control file, consumed
       at the door.
    4. `delivery.control.pattern` -- a conformant upstream's control file,
       which passes through to landing and gates the delivery there.

    Conformant first, on purpose: a feed can have both patterns, and a file
    that already satisfies `filename_pattern` needs no renaming, no control
    file and no verification. Making it take the legacy path would demand a
    control file that a conformant sender has no reason to include.

    A CONTROL FILE MATCHES NO DATA PATTERN -- it names no COB date, it
    says something about a delivery that does -- so it is checked only once no
    data pattern claims the name. Without that, the inbox would reject it to
    `.rejected/` and the delivery it belongs to would wait forever on a file
    that can never arrive.
    """
    registry = list(feeds().values())

    for claimant, what, why in (
        (lambda fd: fd.parse_filename(filename) is not None,
         "filename_pattern",
         "overlapping filename_patterns are a configuration error, and "
         "guessing would put the delivery in the wrong raw table"),
        (lambda fd: fd.claims_source(filename),
         "arrival.source_pattern",
         "overlapping arrival.source_patterns are a configuration error, and "
         "guessing would put the delivery in the wrong raw table"),
    ):
        matched = [fd for fd in registry if claimant(fd)]
        if len(matched) > 1:
            return None, (f"matches more than one feed's {what} ("
                          + ", ".join(sorted(f.name for f in matched))
                          + f") -- {why}"), False
        if matched:
            return matched[0], None, False

    control_matched = [fd for fd in registry
                       if conform.is_source_control_file(fd, filename)
                       or norm.is_control_file(fd, filename)]
    if len(control_matched) > 1:
        return None, ("matches more than one feed's control pattern ("
                      + ", ".join(sorted(f.name for f in control_matched))
                      + ") -- overlapping control patterns are a "
                        "configuration error, and guessing would gate the "
                        "wrong feed's delivery"), False
    if control_matched:
        return control_matched[0], None, True

    return None, ("matches no feed's filename_pattern, arrival.source_pattern "
                  "or control pattern -- check the name, or the patterns in "
                  "feeds.yml"), False


def _quarantine(feed, path, reason_class: str, reason: str,
                dry_run: bool = False) -> None:
    """Copy a refused file to `quarantine/` and record why (REQ-106).

    BEFORE `_move`, always. `_move` is what makes the file stop being
    reprocessed, and once it has happened the bytes are on this container's
    bind mount and nowhere else; doing the durable copy first means a crash
    between the two leaves the file to be rejected again rather than leaves no
    record of it at all.

    Never raises -- see `registry.rejections.quarantine_quietly`. Keeping a
    bad file out of `landing/` is this function's caller's actual job, and it
    must still happen when Postgres or MinIO is unreachable.
    """
    if dry_run:
        return
    from reporting_platform.registry.rejections import quarantine_quietly

    try:
        content = path.read_bytes()
        received = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError as exc:
        log.warning("cannot read %s to quarantine it: %s", path.name, exc)
        return
    quarantine_quietly(feed, path.name, content, reason_class=reason_class,
                       reason=reason, received_at=received)


def list_rejected() -> list[dict]:
    """Files sitting in `.rejected/` -- landed nowhere, claimed by no feed.

    This is docs/DELIVERY-SHAPES.md#5-onboard-from-a-real-file's "unclaimed
    deliveries" backlog: the console's entry point for sniffing a file
    nobody has a feed for yet. `route()` is re-run rather than reading a
    stored reason, because feeds.yml may have changed since rejection --
    the reason (or a feed claiming it now) should reflect the CURRENT
    config, not the moment it was rejected.

    Only `INBOX/.rejected/` -- landing's own "unrecognised object" count
    (`retention/landing.py`) is a narrower, per-feed case (a file for an
    ALREADY-onboarded feed with the wrong name) and is not surfaced here.
    """
    d = INBOX / REJECTED
    if not d.is_dir():
        return []
    out = []
    for path in sorted(d.iterdir()):
        if _skip(path):
            continue
        feed, reason, is_control = route(path.name)
        stat = path.stat()
        out.append({
            "filename": path.name,
            "bytes": stat.st_size,
            "rejected_at": stat.st_mtime,
            "reason": reason,
            # feeds.yml may have moved on since this was rejected -- either
            # of these means a re-drop into inbox is now the right move,
            # not a sniff.
            "now_claimed_by": feed.name if feed else None,
            "now_routes_as_control": is_control,
        })
    return out


def read_rejected(filename: str) -> bytes:
    """Bytes of one file in `.rejected/`, for the console to sniff.

    `filename` reaches this from an HTTP request, so it is validated as a
    bare filename before being joined onto `INBOX` -- the same
    directory-traversal concern `ingest/normalize.py`'s `_safe_member_name`
    guards against for an archive member, here for a name coming over the
    API instead of out of a zip.
    """
    if "/" in filename or "\\" in filename or filename in ("", ".", ".."):
        raise ValueError(f"{filename!r} is not a bare filename")
    path = INBOX / REJECTED / filename
    if not path.is_file():
        raise FileNotFoundError(f"no {filename!r} in .rejected/")
    return path.read_bytes()


def _move(path: Path, folder: str, feed_name: str | None = None) -> Path:
    dest_dir = INBOX / folder / (feed_name or "")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / path.name
    if dest.exists():
        # Same filename delivered twice is ordinary -- a corrected file keeps
        # its name. Keep both rather than overwriting the evidence.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = dest_dir / f"{path.stem}.{stamp}{path.suffix}"
    shutil.move(str(path), str(dest))
    return dest


def _trigger(feed: Feed, key: str | None) -> dict:
    """Unpause if needed, then trigger one run.

    `key` is the DATA object to ingest, and is what `resolve_arrival` acts on
    directly. Pass None for a CONTROL file: it names no delivery of its own,
    so the run falls back to `resolve_arrival`'s `find_pending` path instead,
    which reconciles `ready/` and picks up whichever waiting delivery this
    control file has just unblocked -- including one from an earlier, already
    -triggered run that hit `normalize.NotReady` and was skipped rather than
    retried into it.

    Imports the console's orchestration module rather than opening a second
    HTTP client: there is one definition of how this platform talks to
    Airflow's API, and a copy here would drift from it.
    """
    if not TRIGGER:
        return {"triggered": False,
                "reason": "--no-trigger: the caller ingests"}
    from reporting_platform.ui import orchestration

    dag_id = f"ingest_{feed.name}"
    dag = orchestration.get_dag(dag_id)
    if dag is None:
        return {"triggered": False,
                "reason": f"Airflow has not parsed {dag_id} yet"}
    unpaused = False
    if dag.get("is_paused"):
        orchestration.set_paused(dag_id, False)
        unpaused = True
    conf = {"object_key": key} if key else {}
    run = orchestration.trigger(dag_id, conf=conf,
                                note="dropped into the inbox")
    return {"triggered": True, "dag_id": dag_id, "unpaused": unpaused,
            "run_id": run.get("dag_run_id")}


def sweep(seen: dict[str, tuple[int, float, int]], *, dry_run: bool = False) -> list[dict]:
    """One pass. `seen` carries stability state between passes."""
    results: list[dict] = []
    if not INBOX.is_dir():
        raise RuntimeError(
            f"no inbox directory at {INBOX}. Create it and mount it, or set "
            f"REPORTING_INBOX.")

    for path in sorted(INBOX.iterdir()):
        if _skip(path):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:          # moved or removed mid-pass
            seen.pop(path.name, None)
            continue

        size, mtime, count = seen.get(path.name, (-1, -1.0, 0))
        if (size, mtime) != (stat.st_size, stat.st_mtime):
            seen[path.name] = (stat.st_size, stat.st_mtime, 1)
            continue
        if count < STABLE_POLLS:
            seen[path.name] = (stat.st_size, stat.st_mtime, count + 1)
            continue

        feed, reason, is_control = route(path.name)
        if feed is None:
            log.warning("rejecting %s: %s", path.name, reason)
            if not dry_run:
                # `route` returns one string for two different situations, and
                # they are worth counting apart: nothing claimed the name
                # (usually a name nobody has onboarded) versus two feeds
                # claiming it (always a configuration error in feeds.yml).
                _quarantine(None, path,
                            "ambiguous" if "more than one" in (reason or "")
                            else "unroutable", reason or "", dry_run)
                _move(path, REJECTED)
            seen.pop(path.name, None)
            results.append({"file": path.name, "status": "rejected",
                            "reason": reason})
            continue

        # A LEGACY CONTROL FILE IS NOT PROMOTED ON ITS OWN. It is consumed at
        # the door as part of its data file's delivery, so it is left in place
        # here and picked up when that data file is processed -- usually the
        # next pass, or a later one if the data file has not arrived yet. Its
        # own arrival is what unblocks a data file that was waiting.
        #
        # A CONFORMANT feed's control file is different: it belongs in landing,
        # where `delivery.control` gates the delivery, so it falls through to
        # the ordinary upload below.
        if is_control and conform.is_source_control_file(feed, path.name):
            results.append({"file": path.name, "status": "held (control file)",
                            "feed": feed.name})
            continue

        if dry_run:
            results.append({"file": path.name,
                            "status": "would conform" if feed.needs_conforming
                                      and not feed.parse_filename(path.name)
                                      else "would ingest",
                            "feed": feed.name})
            continue

        # Already-conformant names take the plain path even for a feed that
        # HAS an arrival block -- see route()'s docstring.
        if feed.needs_conforming and feed.parse_filename(path.name) is None:
            results.extend(_promote(feed, path, seen))
            continue

        try:
            key = put_landing(feed, str(path), path.name)
        except Exception as exc:                        # noqa: BLE001
            # Left in place on purpose: an upload failure is usually MinIO
            # being unreachable, which the next pass may well survive.
            log.error("upload failed for %s: %s", path.name, str(exc)[:300])
            results.append({"file": path.name, "status": "upload failed",
                            "error": str(exc)[:300]})
            continue

        # Moved BEFORE the trigger -- see the module docstring.
        moved = _move(path, PROCESSED, feed.name)
        seen.pop(path.name, None)
        outcome = {"file": path.name, "status": "landed", "feed": feed.name,
                   "key": key, "moved_to": str(moved.relative_to(INBOX)),
                   "is_control": is_control}
        # A control file names no delivery of its own -- see _trigger's
        # docstring for why the run gets no object_key.
        outcome.update(_trigger(feed, None if is_control else key))
        log.info("landed %s -> %s%s%s", path.name, key,
                 " (control file)" if is_control else "",
                 "" if outcome.get("triggered") else
                 f" (NOT triggered: {outcome.get('reason')})")
        results.append(outcome)

    return results


def _promote(feed: Feed, path: Path,
             seen: dict[str, tuple[int, float, int]]) -> list[dict]:
    """Run one inbox file through the conformance gate and write what it says.

    ONE WRITER FOR EVERY ARRIVAL SHAPE. What a file BECOMES is
    `conform.plan_arrival`'s answer -- one delivery for a plain file, N for an
    archive, and whatever a shape added later returns; what happens to the
    objects and to the inbox copy is decided here, once. There were two of
    these functions, a plain one and an archive one, and they had already
    drifted: one treated a missing control file as a wait and the other could
    not express the idea at all, and the write order was written down twice.

    ORDER OF WRITES, per delivery: control file, then data file, then
    metadata. The data file is what a triggered run acts on, so writing it
    after its control file means a run can never find a delivery whose control
    file has not arrived yet. If the metadata write then fails, the delivery
    is complete and will ingest with its provenance missing -- the failure to
    prefer over the reverse. Originals stay in the inbox on any failure, so
    the next pass retries; every write is a PutObject to a derived key, so a
    retry overwrites rather than duplicating.

    THE INBOX COPY MOVES LAST, and where it moves is decided from the outcomes
    as a whole: `.rejected/` if the file itself could not be named,
    `.processed/` if anything at all came of it, and nowhere if it is merely
    waiting or if every write failed.
    """
    content = path.read_bytes()
    received = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    inbox_names = [p.name for p in INBOX.iterdir() if not _skip(p)]

    def _read_sibling(name: str) -> bytes:
        return (INBOX / name).read_bytes()

    landed = list_landing(feed)
    try:
        outcomes = conform.plan_arrival(
            feed, path.name, content,
            siblings=conform.Siblings(names=inbox_names, read=_read_sibling),
            received_at=received,
            # A re-delivery for a date already landed must not overwrite the
            # original -- landing is the evidence copy. The listing is what
            # lets the gate pick the next free `_vN`...
            taken={k.rsplit("/", 1)[-1] for k in landed},
            # ...and this is what stops it picking one for a delivery that is
            # not a re-delivery at all, only the same file sent twice.
            landed_md5=landed_md5_lookup(feed, landed))
    except OSError as exc:
        # Reading a sibling the planner asked for. Left in place: the next
        # pass retries, and a half-read control file must not land a delivery.
        log.error("cannot read a file %s needs: %s", path.name, exc)
        return [{"file": path.name, "status": "control unreadable",
                 "feed": feed.name, "error": str(exc)[:300]}]

    results: list[dict] = []
    consumed: set[str] = set()
    written, refused_itself, failed = 0, False, 0
    refusal_reason = ""

    for outcome in outcomes:
        consumed.update(getattr(outcome, "consumed", ()))

        if isinstance(outcome, conform.Waiting):
            # The ordinary case this whole mechanism exists for. Reported at
            # DEBUG and not in the results: on every poll it would bury the
            # outcomes that matter.
            log.debug("%s", outcome.reason)
            continue

        if isinstance(outcome, conform.Duplicate):
            # NOT a rejection. The delivery is already landed and already
            # ingested; there is nothing to write and nothing to trigger.
            log.info("%s", outcome.reason)
            written += 1
            results.append({"file": outcome.source_name, "status": "duplicate",
                            "feed": feed.name,
                            "landed_as": outcome.landing_filename,
                            "reason": outcome.reason})
            continue

        if isinstance(outcome, conform.Refused):
            # AN IDENTITY FAILURE, and the only kind that can happen here. The
            # delivery cannot be NAMED -- no COB date, or a control file that
            # does not say what it was configured to say -- so there is no
            # landing key to write it to and no amount of waiting fixes it.
            # Content failures are not checked here at all: they land and the
            # ingest refuses.
            log.warning("rejecting %s: %s", outcome.source_name, outcome.reason)
            if outcome.source_name == path.name:
                refused_itself, refusal_reason = True, outcome.reason
                _quarantine(feed, path, "identity", outcome.reason)
            else:
                # THE MEMBER'S OWN BYTES, not the container's. The container is
                # never landed -- the members are the deliveries -- so
                # quarantining the whole archive would keep the nineteen good
                # members alongside the one bad one and make the evidence
                # harder to read. The container is named in the reason instead.
                from reporting_platform.registry.rejections import (
                    quarantine_quietly,
                )
                quarantine_quietly(feed, outcome.source_name, outcome.data,
                                   reason_class="member",
                                   reason=outcome.reason, received_at=received)
            results.append({"file": outcome.source_name, "status": "rejected",
                            "feed": feed.name, "reason": outcome.reason})
            continue

        try:
            if outcome.control is not None and outcome.control_landing_filename:
                put_landing_bytes(feed, outcome.control_landing_filename,
                                  outcome.control)
            key = put_landing_bytes(feed, outcome.landing_filename, outcome.data)
            put_landing_bytes(feed, outcome.metadata_filename,
                              conform.metadata_bytes(outcome.metadata),
                              content_type="application/json")
        except Exception as exc:                            # noqa: BLE001
            log.error("upload failed for %s: %s", outcome.source_name,
                      str(exc)[:300])
            failed += 1
            results.append({"file": outcome.source_name,
                            "status": "upload failed", "feed": feed.name,
                            "error": str(exc)[:300]})
            continue

        written += 1
        results.append({"file": outcome.source_name, "status": "conformed",
                        "feed": feed.name, "key": key,
                        "landed_as": outcome.landing_filename,
                        "control_landed_as": outcome.control_landing_filename,
                        "cob_date": outcome.cob_date.isoformat()})

    # WHERE THE INBOX COPY GOES, from the outcomes as a whole.
    if refused_itself:
        destination = REJECTED
    elif written:
        destination = PROCESSED
    else:
        # Waiting on a control file, or every write failed. Either way the file
        # is still worth another pass, so it stays exactly where it is.
        return results

    moved = _move(path, destination, None if refused_itself else feed.name)
    seen.pop(path.name, None)
    for name in sorted(consumed):
        sibling = INBOX / name
        if not sibling.is_file():
            continue
        if refused_itself:
            # Quarantined with the delivery it gates: on its own a control
            # file is unreadable evidence, and the commonest identity failure
            # is that the two disagree.
            _quarantine(feed, sibling, "identity",
                        f"control file for {path.name}: {refusal_reason}")
        _move(sibling, destination, None if refused_itself else feed.name)
        seen.pop(name, None)

    # THE TRIGGER COMES AFTER THE MOVE -- see the module docstring. One run per
    # delivery that actually landed; a duplicate names nothing new to ingest.
    for result in results:
        if result["status"] != "conformed":
            continue
        result["moved_to"] = str(moved.relative_to(INBOX))
        result.update(_trigger(feed, result["key"]))
        log.info("conformed %s -> %s (%s)%s", result["file"], result["key"],
                 result["cob_date"],
                 "" if result.get("triggered") else
                 f" (NOT triggered: {result.get('reason')})")
    if failed:
        log.warning("%s: %d delivery(ies) could not be written; the inbox copy "
                    "was moved because others were", path.name, failed)
    return results


def health(last_poll_at: float | None, interval: float, now: float) -> tuple[int, str]:
    """(HTTP status, body) for `--loop`'s /healthz -- a PURE function so
    tests/test_healthz.py exercises it with no server, no thread and no clock.

    Fresh means the last COMPLETED poll (a full `sweep()`, success or
    exception -- see main()'s `except` below) finished less than 3x the loop
    interval ago. THE WINDOW HAS TO CONTAIN THE THING IT DESCRIBES: at 1x a
    poll that is merely running a little long would flap the probe every
    cycle, and a fixed window unrelated to `--loop`'s own value would either
    never fire (a short interval) or never stop firing (a long one) --
    matching the interval is what makes one number describe "the loop is
    stuck", not "the loop is mid-poll".
    `last_poll_at is None` means no poll has EVER completed (the server
    starts before the first `sweep()` returns), which is exactly as
    unready as a stale one -- not a pass by omission.
    """
    if last_poll_at is None:
        return 503, "no poll has completed yet"
    age = now - last_poll_at
    limit = 3 * interval
    if age < limit:
        return 200, f"ok: last poll {age:.1f}s ago"
    return 503, f"stale: last poll {age:.1f}s ago, limit {limit:.0f}s"


class _HealthState:
    """The one piece of mutable state the health server and the poll loop
    share -- `interval` never changes after `main()` sets it."""

    def __init__(self, interval: float):
        self.interval = interval
        self.last_poll_at: float | None = None


def _start_health_server(state: _HealthState, port: int):
    """A tiny stdlib HTTP server on its own daemon thread, serving /healthz
    from `state`. No framework: this is one route, and the watcher's own
    dependencies stay `pyyaml`/`ruamel.yaml`/`requests`/boto3 -- the same
    ~10s cheap-tier set CLAUDE.md pins, with nothing added for a probe.
    """
    import http.server
    import threading

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 (stdlib's own naming)
            if self.path != "/healthz":
                self.send_response(404)
                self.end_headers()
                return
            status, body = health(state.last_poll_at, state.interval, time.time())
            payload = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):  # noqa: D102
            # The default logs every probe hit to stderr at INFO -- a
            # 10s-interval liveness probe would then outnumber every real
            # sweep log line within minutes.
            pass

    server = http.server.HTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True,
                              name="inbox-healthz")
    thread.start()
    return server


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--loop", type=int, metavar="SECONDS",
                   help="poll forever at this interval instead of once")
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be ingested; move and upload nothing")
    p.add_argument("--json", action="store_true")
    p.add_argument("--no-trigger", action="store_true",
                   help="land only; do not trigger the ingest DAG (for a "
                        "caller that ingests itself)")
    a = p.parse_args(argv)
    global TRIGGER
    TRIGGER = not a.no_trigger

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")

    seen: dict[str, tuple[int, float, int]] = {}
    if not a.loop:
        results = sweep(seen, dry_run=a.dry_run)
        # Once-off cannot observe stability across passes, so give it the
        # observations it needs rather than reporting an empty inbox. That is
        # STABLE_POLLS + 1 passes, not two: the first records a file, the next
        # STABLE_POLLS confirm it. This used to stop after two and reported
        # "inbox empty" for every freshly dropped file.
        for _ in range(STABLE_POLLS):
            if results:
                break
            time.sleep(1)
            results = sweep(seen, dry_run=a.dry_run)
        print(json.dumps(results, indent=2) if a.json else
              "\n".join(f"{r['status']:14} {r['file']}" for r in results)
              or "inbox empty")
        return 0

    health_state = _HealthState(a.loop)
    _start_health_server(health_state, INBOX_HEALTH_PORT)
    log.info("watching %s every %ss (health on :%s/healthz)",
             INBOX, a.loop, INBOX_HEALTH_PORT)
    while True:
        try:
            sweep(seen, dry_run=a.dry_run)
        except Exception:                               # noqa: BLE001
            # A watcher that dies on one bad pass stops watching, which is the
            # failure it exists to prevent.
            log.exception("sweep failed")
        # AFTER the pass, success or exception: a poll that raised still
        # completed one iteration of the loop, which is what health() means
        # by "not stuck".
        health_state.last_poll_at = time.time()
        time.sleep(a.loop)


if __name__ == "__main__":
    raise SystemExit(main())
