"""Fast path: notice a newly completed Transport and trigger its processing.

Phase 6. An OPTIMISATION over `transport_reconcile`, never the sole source of
correctness -- see that DAG's docstring and
`docs/AIRFLOW-ORCHESTRATION.md#trigger-design` for why events alone are never
enough here (a lost `_COMPLETE.json` notification must not permanently lose a
Delivery).

ONE sensor watches the WHOLE `received/` prefix for any `_COMPLETE.json`, not
one sensor per Feed. The unit of acquisition is now Transport, and DCM's
eventual hundreds of Feeds would otherwise mean hundreds of permanently
running pollers -- exactly the shape Phase 6 is told not to build.

`S3KeySensor(deferrable=True)` -- from `apache-airflow-providers-amazon`,
already an image dependency and otherwise unused, which is what this DAG is
for -- holds no worker slot while it waits: the triggerer polls, not a task
process. It is the chosen mechanism per the preference order in the Phase 6
brief (an approved object-store event integration is not available locally;
Airflow's own asset/dataset mechanism does not reach an external object-store
arrival Airflow itself never wrote), one rung above bare periodic discovery.

VERIFIED AGAINST A LIVE SCHEDULER: the sensor reaches MinIO via `aws_default`
(`docker-compose.yml`'s hand-built connection JSON) and triggers
`transport_ingest`. See
`docs/AIRFLOW-ORCHESTRATION.md#verifying-the-fast-path-locally`.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

try:                                    # Airflow 3
    from airflow.sdk import dag, task
except ImportError:                     # Airflow 2.x
    from airflow.decorators import dag, task  # type: ignore

RETRY_DELAY = timedelta(seconds=int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))
DEFAULT_ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": RETRY_DELAY}


def _bucket_and_pattern() -> tuple[str, str]:
    """One fnmatch pattern that finds a marker at ANY depth under received/.

    fnmatch's ``*`` matches ``/`` too (S3 keys are flat strings, not real
    paths), so ``received/*_COMPLETE.json`` matches both v1's flat
    ``received/<transport-id>/_COMPLETE.json`` and v2's partitioned
    ``received/cob_date=.../source_system=.../<transport-id>/_COMPLETE.json``
    with the same sensor -- no version-specific wiring here.
    """
    from reporting_platform.ingest import transport as transport_contract

    return (transport_contract._bucket(),  # noqa: SLF001
           f"{transport_contract.received_prefix()}/*"
           f"{transport_contract.COMPLETE_FILENAME}")


@dag(
    dag_id="transport_watch",
    description="Fast path: discover and trigger newly completed Transports",
    # Short enough that a Transport is usually picked up within a minute or
    # two of its marker landing; bounded so a stuck sensor cannot silently
    # stop watching. See docs/DECISIONS.md#no-arrival-timeout for the sibling
    # decision on the legacy path -- this is deliberately NOT that: a missed
    # cycle here is recovered by transport_reconcile, not left waiting.
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reporting-platform", "ingest", "transport"],
)
def _dag():
    bucket, pattern = _bucket_and_pattern()

    wait_for_marker = S3KeySensor(
        task_id="wait_for_completed_transport",
        bucket_name=bucket,
        bucket_key=pattern,
        wildcard_match=True,
        aws_conn_id="aws_default",
        deferrable=True,
        poke_interval=30,
        timeout=50,
        # Finding nothing new is the ordinary steady state, not a failure --
        # same reasoning as resolve_arrival's AirflowSkipException in
        # feed_ingest.py. soft_fail turns the timeout into a skip, and the
        # default trigger rule then skips trigger_discovered too: nothing to
        # report this cycle.
        soft_fail=True,
    )

    @task(task_id="trigger_discovered")
    def trigger_discovered() -> dict:
        """Every currently-completed Transport, triggered idempotently.

        Lists the whole prefix rather than tracking a "new since last look"
        cursor: Airflow's own DagRun uniqueness (`_transport_trigger.py`) is
        what makes re-listing history harmless, so there is no cursor to lose
        and nothing here can go stale. Cheap at the scale this platform
        targets -- see docs/AIRFLOW-ORCHESTRATION.md#reconciliation-scale for
        the same argument made about `transport_reconcile`, which lists the
        identical prefix on a coarser schedule.
        """
        from _transport_trigger import trigger_transport
        from reporting_platform.ingest import transport as transport_contract
        from reporting_platform.registry import transports as receipts

        triggered: list[str] = []
        already: list[str] = []
        for marker_key in transport_contract.list_completed_transports():
            # transport_id is always the marker's immediate parent directory,
            # regardless of how many cob_date=/source_system= segments (v2)
            # or none at all (v1) precede it.
            transport_id = marker_key.rsplit("/", 2)[-2]
            # THE S3-EVENT DISCOVERY HALF of "converge on the same idempotent
            # TransportReceipt logic" -- `transport_reconcile` is the other.
            # Insert-once (registry/transports.py), so re-listing the whole
            # prefix every cycle costs one no-op upsert per already-known
            # Transport, not a growing table.
            try:
                parsed = transport_contract.read_transport(marker_key)
                receipts.record_discovered_quietly(parsed, marker_key)
            except Exception:                                       # noqa: BLE001
                # A marker that cannot be read/parsed is trigger_transport's
                # problem, not this observability sync's -- it still attempts
                # the trigger below, which is what actually matters.
                pass
            if trigger_transport(transport_id, marker_key):
                triggered.append(transport_id)
            else:
                already.append(transport_id)
        return {"triggered": triggered, "already_queued_or_done": len(already)}

    wait_for_marker >> trigger_discovered()


transport_watch = _dag()
