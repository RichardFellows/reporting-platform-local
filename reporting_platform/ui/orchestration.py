"""Drive Airflow from the feed console, over its REST API.

WHY THE REST API AND NOT A SUBPROCESS. The console runs in its own container;
`airflow dags trigger` would mean either shelling into another container or
running an Airflow CLI against the metadata DB from outside the scheduler.
The REST API is the supported way in, it is the same path CLAUDE.md prescribes
for humans (`trigger` with a distinct run id, never `dags test`), and Airflow 3
serves it too -- so the console does not have to be rewritten when the estate
finishes migrating.

TRIGGERING IS ALL IT DOES. There is no "run the pipeline" endpoint that walks
raw -> prepared -> reporting, because the platform already does that: the
ingest DAG emits its feed's asset, `prepared_build` is scheduled on the OR of
every raw asset, and `reporting_build` on prepared's. Adding a second,
UI-owned sequencer beside that would be a fork of the scheduling logic that
could disagree with it. The console triggers ingest and then WATCHES.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests

BASE = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AUTH = (os.environ.get("AIRFLOW_API_USER", "admin"),
        os.environ.get("AIRFLOW_API_PASSWORD", "admin"))
TIMEOUT = 30

PREPARED_BUILD_DAG = "prepared_build"
REPORTING_BUILD_DAG = "reporting_build"


class AirflowError(RuntimeError):
    """An Airflow API call failed, with the server's own explanation kept.

    requests' raise_for_status() drops the body, and Airflow puts the useful
    half of the message there -- the same reasoning as `Nessie._req` in
    common/context.py.
    """


def _req(method: str, path: str, **kwargs) -> Any:
    try:
        r = requests.request(method, f"{BASE}{path}", auth=AUTH,
                             timeout=TIMEOUT, **kwargs)
    except requests.RequestException as exc:
        raise AirflowError(f"cannot reach Airflow at {BASE}: {exc}") from exc
    if not r.ok:
        raise AirflowError(f"{method} {path} -> {r.status_code} {r.reason}: "
                           f"{r.text[:800]}")
    return r.json() if r.content else {}


def health() -> dict[str, Any]:
    """Is Airflow reachable, and is its scheduler alive?

    The scheduler half matters: the API is served by the webserver, so a dead
    scheduler answers every call here perfectly while nothing anyone triggers
    ever starts. The console shows the two separately rather than a single
    green light.
    """
    try:
        r = requests.get(f"{BASE.rsplit('/api/v1', 1)[0]}/health",
                         timeout=TIMEOUT)
        body = r.json()
        return {
            "reachable": True,
            "metadatabase": body.get("metadatabase", {}).get("status"),
            "scheduler": body.get("scheduler", {}).get("status"),
        }
    except Exception as exc:                                    # noqa: BLE001
        return {"reachable": False, "error": str(exc)}


def get_dag(dag_id: str) -> dict[str, Any] | None:
    try:
        return _req("GET", f"/dags/{dag_id}")
    except AirflowError as exc:
        if "404" in str(exc):
            return None
        raise


def set_paused(dag_id: str, paused: bool) -> dict[str, Any]:
    return _req("PATCH", f"/dags/{dag_id}", params={"update_mask": "is_paused"},
                json={"is_paused": paused})


def trigger(dag_id: str, conf: dict[str, Any] | None = None,
            note: str | None = None) -> dict[str, Any]:
    """Trigger a run with a distinct, console-owned run id.

    The run id is returned so the caller can poll THAT run. Polling "the
    latest run of this DAG" instead is the mistake CLAUDE.md warns about from
    the other direction: with `max_active_runs=1` on every ingest DAG, the
    latest run may well be someone else's, and a console that reported its
    state as yours would be confidently wrong.
    """
    run_id = f"console__{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
    body: dict[str, Any] = {"dag_run_id": run_id, "conf": conf or {}}
    if note:
        body["note"] = note
    return _req("POST", f"/dags/{dag_id}/dagRuns", json=body)


def run_state(dag_id: str, run_id: str) -> dict[str, Any]:
    run = _req("GET", f"/dags/{dag_id}/dagRuns/{run_id}")
    tasks = _req("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": run.get("state"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "tasks": [{"task_id": t["task_id"], "state": t["state"],
                   "try_number": t.get("try_number")}
                  for t in sorted(tasks.get("task_instances", []),
                                  key=lambda t: t["task_id"])],
    }


def recent_runs(dag_id: str, limit: int = 5) -> list[dict[str, Any]]:
    try:
        body = _req("GET", f"/dags/{dag_id}/dagRuns",
                    params={"order_by": "-execution_date", "limit": limit})
    except AirflowError as exc:
        if "404" in str(exc):
            return []
        raise
    return [{"run_id": r["dag_run_id"], "state": r["state"],
             "start_date": r.get("start_date"), "end_date": r.get("end_date"),
             "run_type": r.get("run_type")}
            for r in body.get("dag_runs", [])]


def stale_non_terminal(dag_id: str) -> list[str]:
    """Run ids sitting in a non-terminal state.

    Worth surfacing in the console rather than leaving to be discovered: a run
    left `queued` or `running` under `max_active_runs=1` makes the scheduler
    spin on it and starves every other run of that DAG, and the symptom
    (a newly triggered run that never starts) points nowhere near the cause.
    """
    return [r["run_id"] for r in recent_runs(dag_id, limit=10)
            if r["state"] in ("queued", "running")]
