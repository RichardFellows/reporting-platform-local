"""Shared idempotent-trigger helper for the Phase 6 Transport DAGs.

Leading underscore: not a DAG file, the same convention
`common/spark_task.py` uses and `scripts/check_dag_imports.py` enforces
(its `_dag_files` docstring: "`_`-prefixed ... are helpers by convention, not
DAG files").

Airflow's own DagRun uniqueness on ``(dag_id, run_id)`` is the dedup
mechanism here, not a new table. `transport_watch` and `transport_reconcile`
both derive a DETERMINISTIC run_id from the TransportID, so triggering the
same Transport twice -- a duplicate `_COMPLETE.json` event, the fast path and
reconciliation racing the same discovery, or a replayed manual trigger --
raises `DagRunAlreadyExists`, which is exactly the harmless outcome duplicate
Transport processing requires. See
docs/AIRFLOW-ORCHESTRATION.md#idempotency-across-retries-and-duplicate-events.
"""
from __future__ import annotations

import logging

TRIGGER_DAG_ID = "transport_ingest"


def run_id_for(transport_id: str) -> str:
    """The `transport_ingest` DagRun id for one TransportID.

    Deterministic and stable across the fast path, reconciliation, and a
    manual replay: the same TransportID always addresses the same DagRun,
    which is what makes "trigger again" a safe no-op rather than a second
    attempt racing the first.
    """
    return f"transport__{transport_id}"


def trigger_transport(transport_id: str, marker_key: str) -> bool:
    """Start `transport_ingest` for one TransportID. True if a NEW run started.

    ``marker_key`` is the full completion-marker S3 key, carried through in
    `conf` so `transport_ingest.validate_transport` never has to reconstruct
    it: since Contract v2 (`docs/TRANSPORT-CONTRACT.md`) a marker's key also
    encodes `cob_date`/`source_system`, which `transport_id` alone no longer
    determines. `run_id_for` still keys off `transport_id` alone -- it stays
    one opaque path-safe token, where a marker key contains `/` and `=`,
    which Airflow run ids do not accept.

    False means a DagRun with this id already exists: triggered already by
    this call site or another, queued, running, or finished either way. That
    is deliberately not distinguished here -- transport_ingest's own domain
    calls are what decide whether there is still work to do when that run
    executes, and reading DagRun state here to decide would duplicate
    Airflow's own run history as a second index. Not queuing an identical run
    is enough for a caller that only wants "make sure this is scheduled".
    """
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists

    try:
        trigger_dag(
            dag_id=TRIGGER_DAG_ID,
            run_id=run_id_for(transport_id),
            conf={"transport_id": transport_id, "marker_key": marker_key},
        )
        return True
    except DagRunAlreadyExists:
        logging.getLogger("airflow.task").info(
            "transport_ingest already has a run for %s; not queuing another",
            transport_id)
        return False
