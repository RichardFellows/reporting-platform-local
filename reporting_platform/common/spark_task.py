"""Run one Spark-using platform operation in its own process, emitting JSON.

WHY THIS EXISTS. Airflow tasks that build a SparkSession in-process do their
work correctly and then fail anyway: the task callable returns, the value is
xcom-pushed, and the process never exits, because the Spark JVM and its py4j
gateway keep non-daemon threads alive. Heartbeats stop, and ~300s later the
scheduler reaps the task as a zombie and marks it failed. The task log shows
the work completing, which makes it a confusing failure to diagnose --
`resolve_arrival` finished in 13 seconds and was killed five minutes later.

Running the Spark work in a child process fixes it: the JVM dies with the
child, the parent task returns promptly, and Airflow sees a clean exit. This
is the same reasoning that made scripts/bulk_ingest.py chunk into subprocesses
 -- there for heap reclamation, here for process exit.

stdout is JSON on the last line so the caller can parse it; everything Spark
writes to stderr stays out of the way.

Usage:
    python -m reporting_platform.common.spark_task pending <feed>
    python -m reporting_platform.common.spark_task ingest <feed> <key> [run_id] [cob_date]
    python -m reporting_platform.common.spark_task ingest-batch <feed> <key>...
    python -m reporting_platform.common.spark_task ingest-v2 <normalization_manifest_key> [run_id]
    python -m reporting_platform.common.spark_task raw-delivery-ids <feed>
    python -m reporting_platform.common.spark_task reconcile-committed <feed>
    python -m reporting_platform.common.spark_task maintain-metrics <fqn:layer>...
    python -m reporting_platform.common.spark_task maintain <force|noforce> <fqn:layer>...
    python -m reporting_platform.common.spark_task retention <dry|real> <fqn:layer>...
    python -m reporting_platform.common.spark_task completeness [lookback_business_days]
    python -m reporting_platform.common.spark_task reproducibility [published_tag]
    python -m reporting_platform.common.spark_task run-inputs <branch>
    python -m reporting_platform.common.spark_task migration-compare <feed> <business_date>

`pending` returns MANIFEST keys under ready/; `ingest` takes one of those or a
landing object key. See reporting_platform/ingest/normalize.py.

IN CORE, AND IT WAS `scripts/_spark_task.py`. Every component's DAGs launch
through `run()`, and `scripts/` ships in none of them, so the launcher moved
here and each operation's body moved to a `spark_ops.py` in the component
that owns it -- `OPS` below maps the name to it. `python -m
scripts._spark_task` still works locally; it is a shim over this module.
See docs/PACKAGING.md.
"""
from __future__ import annotations

import importlib
import json
import sys

# What a driver runs, in a child process or a pod: this module, by name.
MODULE = "reporting_platform.common.spark_task"


def run(*args: str) -> dict:
    """Launch one of the operations below as its own driver; parse its JSON.

    THE CALLER SIDE LIVES BESIDE THE CALLEE SIDE, so the argument list and the
    dispatch table below cannot drift. It was a private helper in
    `feed_ingest.py`, and the build DAG needed the same thing -- a second copy
    is how two launchers end up parsing output two different ways.

    WHERE THE DRIVER RUNS is PLATFORM_EXECUTION's answer, and nothing else
    changes with it -- same module, same arguments, same JSON on the last
    line, same error text. `local`: a child process, the JVM dies with it.
    `kubernetes`: a pod of the platform image, so the driver is off the
    Airflow worker entirely and its executors are pods through a k8s://
    master. Every DAG calls this and nothing else, so no DAG changes shape.
    See docs/DECISIONS.md#execution-mode-is-configuration

    Importing this module costs nothing: it pulls in json, sys and datetime,
    and every Spark import is inside the branch that needs it. Nothing here
    starts a JVM in the calling process, which is the whole point of the
    module (docs/DECISIONS.md#spark-in-a-subprocess).
    """
    from reporting_platform.common import settings

    if settings.execution() == "kubernetes":
        code, stdout, stderr = _run_in_pod(args)
    else:
        import subprocess

        proc = subprocess.run(
            [sys.executable, "-m", MODULE, *args],
            capture_output=True, text=True,
        )
        code, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
    return parse_result(args, code, stdout, stderr)


def parse_result(args, code: int, stdout: str, stderr: str) -> dict:
    """The JSON a run printed last, or the error it died with.

    One parser for both launchers. In a pod, stdout and stderr arrive as one
    log, so `stdout` carries both and `stderr` is empty; the JSON is still the
    last line starting with `{`.
    """
    if code != 0:
        # Head of the last traceback as well as the tail: a Py4JJavaError's
        # Java stack pushes the exception MESSAGE off the front of a tail-only
        # budget. See docs/DECISIONS.md#log-tail-plus-head
        err = stderr or stdout or ""
        cut = err.rfind("Traceback (most recent call last)")
        head = err[cut:cut + 2500] if cut >= 0 else ""
        tail = ((stdout or "")[-1500:] + "\n" + head
                + "\n...\n" + err[-2000:])
        raise RuntimeError(
            f"spark task {tuple(args)!r} failed (exit {code})\n{tail}")
    for line in reversed((stdout or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError(
        f"spark task {tuple(args)!r} produced no JSON:\n{(stdout or '')[-1500:]}")


def driver_pod(args, name: str) -> dict:
    """The driver pod for one run, as a plain manifest -- no kubernetes import,
    so the cheap test tier can check it.

    Its whole environment comes from the platform ConfigMap and Secret the
    chart writes (envFrom), the same ones the Airflow pods read, so a driver
    cannot be configured differently from the task that launched it. The
    two things only a pod knows are added: its own IP, which executors call
    back to in client mode, and PLATFORM_EXECUTION itself, so the driver
    builds a k8s:// session rather than looping back here.
    """
    from reporting_platform.common import settings

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": {"app.kubernetes.io/name": "reporting-platform",
                       "app.kubernetes.io/component": "spark-driver",
                       "reporting-platform/spark-task": str(args[0])},
        },
        "spec": {
            "restartPolicy": "Never",
            "serviceAccountName": settings.kubernetes("SPARK_SERVICE_ACCOUNT"),
            "containers": [{
                "name": "driver",
                "image": settings.kubernetes("SPARK_DRIVER_IMAGE"),
                "command": ["python", "-m", MODULE, *args],
                "envFrom": [
                    {"configMapRef": {"name": settings.kubernetes("PLATFORM_ENV_CONFIGMAP")}},
                    {"secretRef": {"name": settings.kubernetes("PLATFORM_ENV_SECRET")}},
                ],
                "env": [
                    {"name": "PLATFORM_EXECUTION", "value": "kubernetes"},
                    {"name": "POD_IP",
                     "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                ],
                # The driver's heap is spark_session()'s 2g plus headroom for
                # the Python process.
                "resources": {"requests": {"cpu": "500m", "memory": "3Gi"},
                              "limits": {"memory": "3Gi"}},
            }],
        },
    }


def _run_in_pod(args) -> tuple[int, str, str]:
    """Create the driver pod, wait for it, return (exit code, log, "").

    THE POD IS ALWAYS DELETED, including when the Airflow task is killed
    under it (a timeout, a mark-failed): `finally` runs on the SIGTERM
    Airflow sends. An orphaned driver holds its executors, and the
    `lakehouse_write` slot is released while the write it guards is still
    going -- the failure the feed console's process-group kill exists to
    prevent locally.
    """
    import time
    import uuid

    from kubernetes import client, config

    from reporting_platform.common import settings

    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    api = client.CoreV1Api()
    namespace = settings.kubernetes("SPARK_K8S_NAMESPACE")
    op = str(args[0]).lower().replace("_", "-")[:30]
    name = f"spark-task-{op}-{uuid.uuid4().hex[:8]}"
    api.create_namespaced_pod(namespace, driver_pod(args, name))
    try:
        while True:
            pod = api.read_namespaced_pod(name, namespace)
            if pod.status.phase in ("Succeeded", "Failed"):
                break
            time.sleep(5)
        state = pod.status.container_statuses[0].state.terminated
        code = state.exit_code if state else 1
        log = api.read_namespaced_pod_log(name, namespace, container="driver")
        return code, log, ""
    finally:
        try:
            api.delete_namespaced_pod(name, namespace, grace_period_seconds=0)
        except Exception:                                     # noqa: BLE001
            pass


# THE OPERATIONS, BY NAME, AND THE COMPONENT THAT OWNS EACH. Strings, not
# imports: this launcher is core and every component calls it, while each
# operation's code ships with the component it belongs to
# (`components.yml`). A driver image carries only the components it was built
# with, so an op whose module is absent is refused by NAME, with the module it
# needed, rather than as a bare ModuleNotFoundError from inside a branch.
# `tests/test_components.py` checks every entry resolves, and that every DAG
# only calls ops its own component may import.
OPS = {
    "pending": "reporting_platform.ingest.spark_ops:op_pending",
    "ingest": "reporting_platform.ingest.spark_ops:op_ingest",
    "ingest-batch": "reporting_platform.ingest.spark_ops:op_ingest_batch",
    "ingest-v2": "reporting_platform.ingest.spark_ops:op_ingest_v2",
    "raw-delivery-ids": "reporting_platform.ingest.spark_ops:op_raw_delivery_ids",
    "reconcile-committed": "reporting_platform.ingest.spark_ops:op_reconcile_committed",
    "migrate-raw": "reporting_platform.ingest.spark_ops:op_migrate_raw",
    "maintain": "reporting_platform.maintenance.spark_ops:op_maintain",
    "maintain-metrics": "reporting_platform.maintenance.spark_ops:op_maintain_metrics",
    "completeness": "reporting_platform.monitoring.spark_ops:op_completeness",
    "reproducibility": "reporting_platform.monitoring.spark_ops:op_reproducibility",
    "retention": "reporting_platform.retention.spark_ops:op_retention",
    "run-inputs": "reporting_platform.registry.spark_ops:op_run_inputs",
    "migration-compare": "reporting_platform.migration.spark_ops:op_migration_compare",
}


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    op, args = argv[0], argv[1:]
    target = OPS.get(op)
    if target is None:
        raise SystemExit(f"unknown op {op!r}")
    module, _, function = target.partition(":")
    try:
        impl = importlib.import_module(module)
    except ModuleNotFoundError as exc:
        # Only when the op's OWN module (or a package above it) is the one
        # missing: a dependency missing from inside it is a real error.
        missing = exc.name or ""
        if not (module == missing or module.startswith(missing + ".")):
            raise
        raise SystemExit(
            f"op {op!r} is implemented by {module}, which is not installed "
            f"in this image -- build the driver image with the component that "
            f"owns it (components.yml)") from exc
    return getattr(impl, function)(args)


if __name__ == "__main__":
    raise SystemExit(main())
