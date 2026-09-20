"""One generic DAG that carries any Transport from `received/` to Raw.

Phase 6. Replaces per-feed bespoke orchestration with per-feed CONFIGURATION:
the same four tasks run for every Feed, and which Feed a given run belongs to
is resolved from the Transport's declared external feed id
(`docs/DELIVERY-CONTRACT.md`), not from DAG topology.

    validate_transport -> create_delivery -> normalize_delivery -> ingest_raw

Every task is a thin call into the existing Phase 1-4 domain functions
(`reporting_platform/ingest/transport.py`, `delivery.py`, `normalization.py`,
`ingest_feed.ingest_normalized_delivery`) plus one xcom-carried reference
between them -- never the manifest itself. See
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


def _spark_subprocess(*args: str) -> dict:
    """Run a Spark-using operation in a child process and parse its JSON.

    Identical reasoning to feed_ingest.py's helper of the same name: an
    in-process SparkSession keeps the JVM alive past the task returning, and
    the scheduler zombie-reaps it. See docs/DECISIONS.md#spark-in-a-subprocess.
    """
    from scripts._spark_task import run

    return run(*args)


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
    params={"transport_id": ""},
)
def _dag():

    @task(task_id="validate_transport")
    def validate_transport(**context) -> str:
        """`_COMPLETE.json` -> a verified Transport. Returns the marker key.

        Read-only and side-effect free, so a retry after a transient object
        store hiccup is exactly the ordinary case, not a special one. Fails
        here, and only here, for a Transport that is not what it claims: a
        missing/mismatched object, a bad hash, or a marker the version-1
        contract does not accept.
        """
        from reporting_platform.ingest import transport as transport_contract

        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        transport_id = conf.get("transport_id") or context["params"].get("transport_id")
        if not transport_id:
            raise ValueError(
                "transport_ingest requires transport_id in dag_run.conf or params")
        marker_key = transport_contract.complete_key(transport_id)
        transport_contract.read_validated_transport(marker_key)
        return marker_key

    @task(task_id="create_delivery")
    def create_delivery_task(marker_key: str) -> str:
        """Accepted Transport -> immutable DeliveryManifest. Returns its key.

        `create_delivery` is create-once: the same marker_key always returns
        the same DeliveryManifest, written on first call and read-and-verified
        on every one after. Fails here for an unknown external Feed id or a
        business-identity conflict (`docs/DELIVERY-CONTRACT.md`) -- both are
        about THIS Delivery's interpretation, never about Transport evidence,
        which validate_transport already settled.
        """
        from reporting_platform.ingest import delivery as delivery_contract
        from reporting_platform.ingest import transport as transport_contract

        delivery_contract.create_delivery(marker_key)
        # The key is a pure function of transport identity, so this is a
        # cheap re-parse of the small completion marker -- not a re-hash of
        # the source bytes create_delivery already validated. Matches the
        # idiom tests/test_delivery.py uses.
        parsed = transport_contract.read_transport(marker_key)
        return delivery_contract.manifest_key(parsed)

    @task(task_id="normalize_delivery")
    def normalize_delivery_task(delivery_manifest_key: str) -> str:
        """DeliveryManifest -> rebuildable NormalizationManifest v2.

        Also create-once/idempotent (`docs/NORMALIZATION-CONTRACT.md`): a
        retry after a partial archive extraction resumes from whatever Ready
        parts already exist and accepts them only when byte-identical. Fails
        here for an unsafe archive member, an invalid zip, or a normalization
        contract conflict -- never for anything about raw ingestion.
        """
        from reporting_platform.ingest.normalization import normalize_delivery

        result = normalize_delivery(delivery_manifest_key)
        return result.key

    @task(task_id="ingest_raw", pool="lakehouse_write",
         outlets=[RAW_ASSET_ALIAS])
    def ingest_raw_task(normalization_manifest_key: str, **context) -> dict:
        """NormalizationManifest v2 -> Raw Iceberg, merged onto `main`.

        `ingest-v2` is the process-isolated adapter over
        `ingest_feed.ingest_normalized_delivery`
        (`docs/RAW-INGESTION-CONTRACT.md`): branch, write, validate, and
        merge all happen inside that one subprocess call, so it does not
        return until the Delivery is committed on `main` -- or raises,
        leaving `main` untouched and the branch retained for inspection, same
        as every other write path in this platform. The raw asset event is
        therefore only ever added AFTER that call returns successfully.
        Fails here for schema/row-count/checksum validation, never for
        anything Delivery- or Normalization-shaped.
        """
        run_id = context["run_id"].replace(":", "").replace("+", "")[-24:]
        result = _spark_subprocess("ingest-v2", normalization_manifest_key, run_id)

        # THE COMMIT ALREADY HAPPENED, above -- this only tells prepared_build
        # about it. Emitting on the "already_ingested" no-op path is correct,
        # not merely harmless: that path returned True precisely because the
        # Delivery IS committed on main, so the asset update is just as true
        # on a duplicate/retried run as on the one that did the write.
        context["outlet_events"][RAW_ASSET_ALIAS].add(Asset(result["asset_uri"]))

        # A reference/summary, not the row data ingest_raw read or wrote.
        return {
            "feed": result["feed"],
            "delivery_id": result["delivery_id"],
            "cob_date": result["cob_date"],
            "rows": result["rows"],
            "already_ingested": result["already_ingested"],
            "asset_uri": result["asset_uri"],
        }

    marker = validate_transport()
    delivery_manifest = create_delivery_task(marker)
    normalization_manifest = normalize_delivery_task(delivery_manifest)
    ingest_raw_task(normalization_manifest)


transport_ingest = _dag()
