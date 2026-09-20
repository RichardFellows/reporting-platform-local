"""Phase 8: generic dual-run comparison, independent of transport_ingest.

ONE DAG for every feed in `migration.mode: dual_run` (or `new_primary`,
which keeps comparing for rollback/observation -- section 4). There is no
per-feed migration DAG and no per-feed sensor (sections 3/17/38/49's guard
list): `discover_candidates` returns the (feed, business_date) pairs worth
attempting, and Airflow's dynamic task mapping fans a single mapped task out
over them, the same shape `transport_reconcile` already uses for its
per-Feed Raw check -- one task definition, many runtime instances.

INDEPENDENT OF `transport_ingest` ON PURPOSE (section 24). This DAG only
READS what transport_ingest already published (registered Deliveries, Raw/
prepared/reporting tables) and a legacy adapter; it triggers nothing in the
ingestion path and nothing in the ingestion path triggers it. A legacy outage
makes comparisons for that day come back `NOT_COMPARABLE`, never blocks or
fails a new-platform ingest.

XCOM CARRIES IDS/COUNTS ONLY (section 39/49's guard list) -- the mapped
task's return value is the same small JSON `evidence.record` writes to
Postgres, never row-level data.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

try:                                    # Airflow 3
    from airflow.sdk import dag, task
except ImportError:                     # Airflow 2.x
    from airflow.decorators import dag, task  # type: ignore

RETRY_DELAY = timedelta(
    seconds=3 * int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))
DEFAULT_ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": RETRY_DELAY}

LOOKBACK_DAYS = int(os.environ.get("MIGRATION_RECONCILE_LOOKBACK_DAYS", "14"))


@dag(
    dag_id="migration_reconcile",
    description="Phase 8: generic dual-run comparison for every dual_run feed",
    # Coarse on purpose -- this is assurance, not the ingestion path, and a
    # tighter schedule buys nothing when the legacy adapter's own data does
    # not change faster than this. See docs/MIGRATION.md#airflow.
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reporting-platform", "migration"],
)
def _dag():

    @task(task_id="discover_candidates")
    def discover_candidates() -> list[list[str]]:
        """(feed, business_date) pairs to attempt this run. Bounded lookback,
        never a full historical rescan -- see run.py's docstring."""
        from reporting_platform.common.context import feeds
        from reporting_platform.migration.run import (
            discover_candidates as _discover,
        )

        pairs = _discover(list(feeds().values()), lookback_days=LOOKBACK_DAYS)
        return [list(p) for p in pairs]

    @task(task_id="compare_one", pool="lakehouse_write")
    def compare_one(pair: list[str]) -> dict:
        """One (feed, business_date) comparison. Spark-backed -- see
        `scripts/_spark_task.py migration-compare`, subprocess-only for the
        same reason every other Spark call here is
        (`docs/DECISIONS.md#spark-in-a-subprocess`)."""
        from scripts._spark_task import run

        feed_name, business_date = pair
        return run("migration-compare", feed_name, business_date)

    compare_one.expand(pair=discover_candidates())


migration_reconcile = _dag()
