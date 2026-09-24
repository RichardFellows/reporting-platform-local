"""One generic DAG that carries any Transport from `received/` to Raw.

Phase 6. Replaces per-feed bespoke orchestration with per-feed CONFIGURATION:
the same four tasks run for every Feed, and which Feed a given run belongs to
is resolved from the Transport's declared external feed id
(`docs/DELIVERY-CONTRACT.md`), not from DAG topology.

    validate_transport -> create_delivery -> normalize_delivery -> ingest_raw
        -> report_drift -> record_snapshot

Every task is one call into `reporting_platform/ingest/transport_steps.py`
-- the same steps `python -m reporting_platform.ingest transport` runs with
no Airflow -- plus one xcom-carried reference between them, never the
manifest itself. See
`docs/AIRFLOW-ORCHESTRATION.md` for the full design, including why this DAG
is triggered rather than scheduled, and why XCom stays reference-only.

Never triggered directly by an operator watching a filesystem: `transport_id`
arrives in `dag_run.conf`, from `transport_watch` (the fast path),
`transport_reconcile` (the correctness path), or a manual replay
(`docs/AIRFLOW-ORCHESTRATION.md#replaying-a-transport`).
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

# ---------------------------------------------------------------- AF2/AF3 shim
# See feed_ingest.py for why this exists on every DAG in this repo.
try:                                    # Airflow 3
    from airflow.sdk import Asset, AssetAlias, dag, task
    _AF3 = True
except ImportError:                     # Airflow 2.x
    from airflow.datasets import Dataset as Asset  # type: ignore
    from airflow.datasets import DatasetAlias as AssetAlias  # type: ignore
    from airflow.decorators import dag, task       # type: ignore
    _AF3 = False

RETRY_DELAY = timedelta(seconds=int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": RETRY_DELAY,
    "email_on_failure": False,
}

# One alias for every Feed's raw asset. Which CONCRETE Asset a run resolves it
# to is only known once `create_delivery` has resolved the Feed -- a single
# Transport DAG serving hundreds of Feeds cannot declare hundreds of static
# outlets, and a static list would mark EVERY Feed's asset updated on every
# run regardless of which one this Transport actually touched. AssetAlias
# (DatasetAlias pre-3.0, Airflow 2.10+) is exactly this: outlets declared at
# parse time, the concrete Asset resolved and emitted at run time.
# See docs/AIRFLOW-ORCHESTRATION.md#raw-asset-emission
RAW_ASSET_ALIAS = AssetAlias("raw-table-updated")



def _caller(context):
    """Who is running the step: this DAG, this run. See `transport_steps.Caller`."""
    from reporting_platform.ingest.transport_steps import Caller

    return Caller(execution_ref=context["run_id"], dag_id="transport_ingest",
                  run_id=context["run_id"])


def _run_step(step, *args, context):
    """Call one `transport_steps` function, failing once on a refusal.

    `retries: 2` is for a transient hiccup. Retrying a refusal spends two
    retry delays and, before each attempt had its own branch, replaced the
    real message with a Nessie 409 as the task's last error.
    See docs/DECISIONS.md#a-refusal-is-not-retried
    """
    from reporting_platform.ingest.transport_steps import is_refusal

    try:
        return step(*args, _caller(context))
    except Exception as exc:
        if is_refusal(exc):
            from airflow.exceptions import AirflowFailException

            raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc
        raise


@dag(
    dag_id="transport_ingest",
    description="Transport -> DeliveryManifest -> NormalizationManifest -> Raw",
    # Triggered only -- by transport_watch, transport_reconcile, or a manual
    # replay. A schedule here would mean this DAG deciding for itself when a
    # Transport is due, which is exactly the job the two trigger DAGs own.
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Independent Transports -- of different Feeds, or the same Feed on
    # different days -- may validate/create/normalize concurrently: none of
    # those three tasks touches the lakehouse_write pool. Bounded rather than
    # unbounded so a burst of arrivals cannot flood the scheduler; the actual
    # writer serialisation still comes from the pool on ingest_raw alone.
    # See docs/DECISIONS.md#one-shared-write-pool
    max_active_runs=10,
    default_args=DEFAULT_ARGS,
    tags=["reporting-platform", "ingest", "transport"],
    params={"transport_id": "", "marker_key": "", "cob_date": "",
           "source_system": ""},
)
def _dag():
    # EACH TASK IS ONE `transport_steps` CALL. The steps -- and the receipt
    # and validation evidence each records -- live there, so the standalone
    # runner takes a Transport with the same code. A task adds only what
    # Airflow alone knows. `tests/test_transport_steps.py` fails if one grows
    # its own copy again.

    @task(task_id="validate_transport")
    def validate_transport(**context) -> str:
        """`_COMPLETE.json` -> a verified Transport. Returns the marker key.

        `conf.marker_key` (set by `transport_watch`/`transport_reconcile` via
        `_transport_trigger.py`) is used directly when present -- since
        Contract v2 a marker key also encodes `cob_date`/`source_system`,
        which `transport_id` alone no longer determines. A manual replay
        (`docs/AIRFLOW-ORCHESTRATION.md#replaying-a-transport`) may instead
        supply `marker_key` directly, or `cob_date`+`source_system` alongside
        `transport_id` for a v2 Transport, or bare `transport_id` for a
        legacy v1 one.
        """
        from reporting_platform.ingest import transport as transport_contract
        from reporting_platform.ingest import transport_steps

        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        params = context["params"]
        transport_id = conf.get("transport_id") or params.get("transport_id")
        marker_key = conf.get("marker_key") or params.get("marker_key")
        if not marker_key:
            cob_date = conf.get("cob_date") or params.get("cob_date")
            source_system = conf.get("source_system") or params.get("source_system")
            if not transport_id:
                raise ValueError(
                    "transport_ingest requires marker_key, or transport_id, "
                    "in dag_run.conf or params")
            if cob_date and source_system:
                marker_key = transport_contract.complete_key(
                    cob_date, source_system, transport_id)
            else:
                marker_key = transport_contract.complete_key_v1(transport_id)
        # Retried as before: nothing it raises is a refusal by `is_refusal`,
        # and an unreadable object may be a store hiccup.
        return transport_steps.validate(marker_key, _caller(context),
                                        transport_id=transport_id)

    @task(task_id="create_delivery")
    def create_delivery_task(marker_key: str, **context) -> str:
        """Accepted Transport -> immutable DeliveryManifest. Returns its key.
        See `transport_steps.deliver`."""
        from reporting_platform.ingest import transport_steps

        return _run_step(transport_steps.deliver, marker_key, context=context)

    @task(task_id="normalize_delivery")
    def normalize_delivery_task(delivery_manifest_key: str, **context) -> str:
        """DeliveryManifest -> NormalizationManifest v2. Returns its key.
        See `transport_steps.normalize`."""
        from reporting_platform.ingest import transport_steps

        return _run_step(transport_steps.normalize, delivery_manifest_key,
                         context=context)

    @task(task_id="ingest_raw", pool="lakehouse_write",
         outlets=[RAW_ASSET_ALIAS])
    def ingest_raw_task(normalization_manifest_key: str, **context) -> dict:
        """NormalizationManifest v2 -> Raw Iceberg, merged onto `main`.
        See `transport_steps.ingest_raw`. Fails here for schema/row-count/
        checksum validation, never for anything Delivery- or Normalization-
        shaped. The raw asset event is added only AFTER it returns."""
        from reporting_platform.common.context import ingest_attempt_id
        from reporting_platform.ingest import transport_steps

        # A branch PER ATTEMPT: see `ingest_attempt_id`.
        attempt_id = ingest_attempt_id(context["run_id"],
                                       context["ti"].try_number)
        result = _run_step(transport_steps.ingest_raw,
                           normalization_manifest_key, attempt_id,
                           context=context)

        # THE COMMIT ALREADY HAPPENED, above -- this only tells prepared_build
        # about it. Emitting on the "already_ingested" no-op path is correct,
        # not merely harmless: that path returned True precisely because the
        # Delivery IS committed on main, so the asset update is just as true
        # on a duplicate/retried run as on the one that did the write.
        context["outlet_events"][RAW_ASSET_ALIAS].add(Asset(result["asset_uri"]))

        # A reference/summary, not the row data ingest_raw read or wrote:
        # identifiers, counts, the merge commit, and column NAMES for the
        # drift report -- what the two tasks after this need, and no more.
        return {
            "feed": result["feed"],
            "delivery_id": result["delivery_id"],
            "cob_date": result["cob_date"],
            "rows": result["rows"],
            "already_ingested": result["already_ingested"],
            "asset_uri": result["asset_uri"],
            "run_id": result["run_id"],
            "commit": result.get("commit"),
            **{k: result.get(k, []) for k in (
                "missing_columns", "extra_columns", "columns_added",
                "columns_orphaned")},
        }

    # THE SAME TWO TASKS `ingest_<feed>` ENDS WITH, calling the same
    # functions: a Transport ingest is reported and pinned exactly as an
    # inbox one is. Outside the write pool, which is safe because the tag
    # names the merge commit, not main's head.
    # See docs/DECISIONS.md#a-snapshot-tag-names-its-merge-commit

    @task(task_id="report_drift")
    def report_drift(result: dict) -> dict:
        """Schema drift is reported, never fatal. See
        `ingest.steps.drift_warnings`."""
        import logging

        from reporting_platform.ingest.steps import drift_warnings

        for message in drift_warnings(result["feed"], result):
            logging.getLogger("airflow.task").warning("%s", message)
        return result

    @task(task_id="record_snapshot")
    def record_snapshot(result: dict) -> dict:
        """Pin the commit this ingest's merge made:
        `snapshot/<feed>/<bd>/<run_id>`. No tag when raw already held the
        Delivery. See `ingest.steps.record_snapshot`."""
        from reporting_platform.ingest.steps import record_snapshot as pin

        return pin(result["feed"], result)

    marker = validate_transport()
    delivery_manifest = create_delivery_task(marker)
    normalization_manifest = normalize_delivery_task(delivery_manifest)
    ingested = ingest_raw_task(normalization_manifest)
    report_drift(ingested) >> record_snapshot(ingested)


transport_ingest = _dag()
