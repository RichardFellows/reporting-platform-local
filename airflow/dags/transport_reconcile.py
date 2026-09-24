"""Correctness path: rebuild Transport pipeline progress from durable evidence.

Phase 6. `transport_watch` is an optimisation; this is not. A lost
`_COMPLETE.json` event, a missed poke, or `transport_watch` itself being down
must not permanently lose a Delivery -- this DAG is what makes that true,
independently of Airflow's own run history. It derives what stage every
completed Transport has reached from object storage evidence alone
(`reporting_platform/ingest/transport_reconcile.py`) and triggers
`transport_ingest` for whatever is not yet complete, the same idempotent way
`transport_watch` does.

No mutable "current stage" is stored anywhere for this. See
`docs/AIRFLOW-ORCHESTRATION.md#reconciliation` for the staged evidence walk
and why Raw ingestion state needs one bulk query per Feed rather than a
cheaper index -- there isn't one; Raw is the only v2 ingestion ledger by
design (`docs/RAW-INGESTION-CONTRACT.md`).

Contract v2 (`docs/TRANSPORT-CONTRACT.md`) partitions `received/` by COB date,
so the DEFAULT scheduled run now bounds its scan to the last
`TRANSPORT_RECONCILE_WINDOW_DAYS` calendar days (default 7) instead of
re-listing every Transport ever published -- see
`docs/AIRFLOW-ORCHESTRATION.md#reconciliation-scale`. This is a genuine
narrowing of the earlier "always list everything" design, not an addition to
it: a Transport whose COB date is older than the window, or a v1 marker
(which has no `cob_date=` partition to be found by), is no longer discovered
by the scheduled run. Trigger with `-c '{"full_sweep": true}'` for the
unbounded catch-all -- an occasional/manual operation, not the periodic
default; see that same doc section for why this is a deliberate scope
narrowing rather than a new durable-state subsystem.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

try:                                    # Airflow 3
    from airflow.sdk import dag, task
except ImportError:                     # Airflow 2.x
    from airflow.decorators import dag, task  # type: ignore

# 3x the base delay: same reasoning as platform_housekeeping.py's slower
# retry -- this walks the same object-storage evidence a transient hiccup is
# most likely to still be recovering from.
RETRY_DELAY = timedelta(
    seconds=3 * int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))
DEFAULT_ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": RETRY_DELAY}

WINDOW_DAYS = int(os.environ.get("TRANSPORT_RECONCILE_WINDOW_DAYS", "7"))


@dag(
    dag_id="transport_reconcile",
    description="Correctness path: recover any Transport the fast path missed",
    # Coarser than transport_watch on purpose -- this is the safety net, not
    # the primary path, and every completed Transport it reads costs at least
    # one HEAD/GET even when there is nothing to do. See
    # docs/AIRFLOW-ORCHESTRATION.md#reconciliation-scale.
    schedule=timedelta(minutes=20),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reporting-platform", "ingest", "transport"],
    params={"full_sweep": False},
)
def _dag():

    @task(task_id="discover_progress")
    def discover_progress(**context) -> dict:
        """Classify every completed Transport by durable evidence alone.

        Bounded to the last `WINDOW_DAYS` COB partitions by default -- see
        this file's module docstring. `-c '{"full_sweep": true}'` (or
        `params.full_sweep` on a manual trigger) instead scans the whole
        `received/` prefix, unbounded, the same as every scheduled run did
        before Contract v2: use it to catch a v1 marker or one whose COB date
        fell outside the window, occasionally, not on the default schedule.
        """
        import logging

        from reporting_platform.ingest.transport_reconcile import (
            discover_transport_progress,
        )

        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        full_sweep = bool(conf.get("full_sweep",
                                   context["params"].get("full_sweep", False)))
        from reporting_platform.ingest.transport_steps import window_cob_dates

        cob_dates = None if full_sweep else window_cob_dates(WINDOW_DAYS)
        report = discover_transport_progress(cob_dates=cob_dates)
        if report["failed"]:
            logging.getLogger("airflow.task").warning(
                "transport_reconcile: %d transport(s) could not be read: %s",
                len(report["failed"]), report["failed"][:5])
        return report

    @task(task_id="sync_receipts")
    def sync_receipts(report: dict) -> dict:
        """Converge `registry.transport_receipt` onto this evidence walk.

        The correctness-path half of idempotent TransportReceipt discovery --
        `transport_watch`'s `trigger_discovered` is the fast-path half. Runs
        independently of `check_raw`/`trigger_pending`: it observes what this
        walk already found, it does not decide what to trigger.
        """
        from reporting_platform.registry.transports import sync_from_progress

        return sync_from_progress(report)

    @task(task_id="check_raw")
    def check_raw(report: dict) -> list[str]:
        """Normalized Deliveries this cannot yet prove are committed to Raw.

        One Spark READ per Feed that has a candidate -- never per Delivery,
        never per Transport. Deliberately not on the writer pool: this reads
        Raw, it does not write it, the same as every other read-only Spark
        job in this platform (completeness, reproducibility, maintenance
        metrics -- see docs/ARCHITECTURE.md, "Where Spark actually runs").
        """
        from reporting_platform.common.spark_task import run

        from reporting_platform.ingest.transport_reconcile import raw_pending

        candidates_by_feed = report["candidates_by_feed"]
        delivered_by_feed = {
            feed_name: run("raw-delivery-ids", feed_name)["delivery_ids"]
            for feed_name in candidates_by_feed
        }
        return raw_pending(candidates_by_feed, delivered_by_feed)

    @task(task_id="trigger_pending")
    def trigger_pending(report: dict, raw_gap: list[str]) -> dict:
        """Trigger `transport_ingest` for everything still incomplete.

        Idempotent via the same DagRun-id dedup as transport_watch
        (`_transport_trigger.py`): a Transport this DAG and transport_watch
        both notice at once is triggered once, not twice.
        """
        from _transport_trigger import trigger_transport

        pending = sorted(set(report["needs_full_chain"]) | set(raw_gap))
        marker_keys = report["marker_keys"]
        triggered = [t for t in pending
                    if t in marker_keys and trigger_transport(t, marker_keys[t])]
        return {"pending": pending, "newly_triggered": triggered,
                "unread_transports": len(report["failed"])}

    progress = discover_progress()
    sync_receipts(progress)
    trigger_pending(progress, check_raw(progress))


transport_reconcile = _dag()
