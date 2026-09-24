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
    the task's call to `transport_steps.ingest_raw`, which is the `ingest-v2`
    launch, is what proves the asset is never emitted before the commit is
    visible on main.
    """
    import inspect

    from reporting_platform.ingest import transport_steps

    source = _source("transport_ingest.py")
    task = source[source.index('def ingest_raw_task('):]
    step_call = task.index('_run_step(transport_steps.ingest_raw,')
    emit = task.index('outlet_events"][RAW_ASSET_ALIAS].add(')
    assert step_call < emit
    step = inspect.getsource(transport_steps.ingest_raw)
    assert 'return run("ingest-v2", normalization_manifest_key, attempt_id)' in step


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


def test_transport_watch_pattern_matches_v1_and_v2_marker_depths():
    """fnmatch's '*' matches '/' too, so one pattern finds a marker at any
    depth under received/ -- v1's flat layout and v2's cob_date=/
    source_system= partitioning both, with no version-specific wiring."""
    source = _source("transport_watch.py")
    assert 'f"{transport_contract.received_prefix()}/*"' in source


def test_transport_watch_triggers_by_marker_key():
    source = _source("transport_watch.py")
    body = source[source.index("def trigger_discovered("):]
    assert "trigger_transport(transport_id, marker_key)" in body


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


def test_reconciliation_defaults_to_a_bounded_cob_window_not_a_full_scan():
    """Contract v2 partitions received/ by cob_date, so the scheduled run
    should no longer re-list every Transport ever published -- see
    docs/AIRFLOW-ORCHESTRATION.md, 'Reconciliation scale (v2)'. The unbounded
    walk remains available, but only opt-in via full_sweep."""
    source = _source("transport_reconcile.py")
    assert "cob_dates=cob_dates" in source
    assert "full_sweep" in source
    assert "window_cob_dates(WINDOW_DAYS)" in source


def test_full_sweep_conf_param_selects_the_unbounded_scan():
    source = _source("transport_reconcile.py")
    body = source[source.index("def discover_progress("):
                 source.index("@task(task_id=\"check_raw\")")]
    assert 'conf.get("full_sweep"' in body
    assert "cob_dates = None if full_sweep else" in body


def test_trigger_pending_addresses_transport_ingest_by_marker_key():
    """`trigger_transport` needs the full marker key since Contract v2 --
    transport_id alone no longer determines cob_date/source_system."""
    source = _source("transport_reconcile.py")
    body = source[source.index('def trigger_pending('):]
    assert "marker_keys[t]" in body
    assert "trigger_transport(t, marker_keys[t])" in body


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


def test_trigger_transport_carries_the_marker_key_in_conf():
    """Since Contract v2 a marker key also encodes cob_date/source_system,
    which transport_id alone no longer determines -- validate_transport must
    receive it directly rather than reconstructing it."""
    source = _source("_transport_trigger.py")
    assert "def trigger_transport(transport_id: str, marker_key: str)" in source
    assert '"marker_key": marker_key' in source
