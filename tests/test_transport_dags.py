"""Structural properties of the Phase 6 DAG files, pinned by source text.

Same idiom as test_dbt_artifacts.test_artifact_and_input_guards_run_before_the_
nessie_merge: these DAGs import Airflow, which this cheap tier does not
install (see tests/README.md), so behaviour is pinned by reading the file
rather than importing it. Real DAG import/graph structure is proven by
`scripts/check_dag_imports.py` against a live Airflow metadata db -- the
`parse` CI tier, not this one. docs/AIRFLOW-ORCHESTRATION.md says how to run
that locally.
"""
from __future__ import annotations

from tests.support import DAGS


def _source(name: str) -> str:
    return (DAGS / name).read_text(encoding="utf-8")


# ---------------------------------------------------------- transport_ingest
def test_raw_asset_is_emitted_only_after_ingest_v2_returns():
    """Never emit success merely because Spark started (Phase 6 brief, #14).

    `ingest-v2` returns only after `_ingest_manifest` has already merged onto
    `main` or raised -- so the outlet_events.add call being textually AFTER
    the _spark_subprocess("ingest-v2", ...) call is what proves the asset is
    never emitted before the commit is visible on main.
    """
    source = _source("transport_ingest.py")
    task = source[source.index('def ingest_raw_task('):]
    spark_call = task.index('_spark_subprocess("ingest-v2"')
    emit = task.index('outlet_events"][RAW_ASSET_ALIAS].add(')
    assert spark_call < emit


def test_ingest_raw_is_the_only_task_on_the_write_pool():
    """Non-writing stages must not be serialised behind lakehouse_write
    (Phase 6 brief, #13): validate/create/normalize touch no Iceberg write
    path, so only ingest_raw may hold the pool.
    """
    source = _source("transport_ingest.py")
    assert source.count('pool="lakehouse_write"') == 1
    pool_at = source.index('pool="lakehouse_write"')
    task_at = source.index("def ingest_raw_task(")
    # The pool= kwarg sits on the @task(...) decorator immediately above
    # ingest_raw_task's def line, and nowhere else in the file.
    assert 0 < task_at - pool_at < 100


def test_transport_ingest_is_never_self_scheduled():
    """Triggered only -- by transport_watch, transport_reconcile, or a
    manual replay (Phase 6 brief, #6/#19); a schedule here would let this DAG
    decide for itself when a Transport is due.
    """
    source = _source("transport_ingest.py")
    assert "schedule=None" in source


def test_transport_ingest_xcom_is_references_not_manifests():
    """XCom carries identifiers, never authoritative evidence (Phase 6 brief,
    #8): every inter-task return is a short key string or a small reference
    dict -- never `.as_manifest()`, a raw response body, or a Delivery/
    Transport object.
    """
    source = _source("transport_ingest.py")
    assert "as_manifest()" not in source
    # The returns between tasks are all `-> str` (a key) until the final
    # summary dict, which is itself small and reference-shaped.
    assert "def validate_transport(**context) -> str:" in source
    assert "def create_delivery_task(marker_key: str, **context) -> str:" in source
    assert ("def normalize_delivery_task(delivery_manifest_key: str, **context) -> str:"
           in source)


# ----------------------------------------------------------- transport_watch
def test_transport_watch_uses_one_sensor_not_one_per_feed():
    """The unit of acquisition is Transport, not Feed (Phase 6 brief, #5):
    exactly one S3KeySensor instance watches the whole `received/` prefix,
    and nothing here loops over the Feed registry to build more of them.
    """
    source = _source("transport_watch.py")
    assert source.count("= S3KeySensor(") == 1
    assert "feeds()" not in source


def test_transport_watch_sensor_is_deferrable_and_does_not_fail_on_nothing_new():
    source = _source("transport_watch.py")
    sensor = source[source.index("= S3KeySensor("):
                    source.index('@task(task_id="trigger_discovered")')]
    assert "deferrable=True" in sensor
    assert "soft_fail=True" in sensor
    assert "wildcard_match=True" in sensor


# ------------------------------------------------------- transport_reconcile
def test_reconciliation_raw_check_is_outside_the_write_pool():
    """check_raw only READS Raw -- see docs/ARCHITECTURE.md, "Where Spark
    actually runs": read-only Spark jobs sit outside lakehouse_write.
    """
    source = _source("transport_reconcile.py")
    body = source[source.index("def check_raw("):source.index("def trigger_pending(")]
    assert 'pool="lakehouse_write"' not in body
    assert "raw-delivery-ids" in body


def test_reconciliation_is_scheduled_coarser_than_the_fast_path():
    watch = _source("transport_watch.py")
    reconcile = _source("transport_reconcile.py")
    assert "schedule=timedelta(minutes=1)" in watch
    assert "schedule=timedelta(minutes=20)" in reconcile


def test_reconciliation_has_no_mutable_stage_status():
    """Phase 6 brief, #9/#11: no transport.processed / delivery.status /
    registry.delivery.ingested column, ever, anywhere in this DAG file.
    """
    source = _source("transport_reconcile.py")
    for forbidden in ("transport.processed", "delivery.status",
                      ".ingested =", "UPDATE registry"):
        assert forbidden not in source


# --------------------------------------------------------- shared dedup path
def test_both_trigger_dags_share_one_dedup_implementation():
    """A single idempotent-trigger mechanism, not two that could drift."""
    watch = _source("transport_watch.py")
    reconcile = _source("transport_reconcile.py")
    for source in (watch, reconcile):
        assert "from _transport_trigger import trigger_transport" in source


def test_trigger_dedup_relies_on_airflow_run_identity_not_a_new_table():
    source = _source("_transport_trigger.py")
    assert "DagRunAlreadyExists" in source
    assert "run_id=run_id_for(transport_id)" in source
