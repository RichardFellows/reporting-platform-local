"""Delivery identity from Raw through Prepared and run-input collection."""
from __future__ import annotations

from datetime import date

from tests.support import REPO, config_dir


def test_v2_delivery_id_wins_over_same_physical_filename_and_restatement():
    from tests.test_dedupe_rank import _render
    from tests.test_scd2_incremental import _connect, _duck

    con = _connect()
    con.execute("""
        create table raw_qa_happy_position as
        select * from (values
          (date '2026-09-17', 1, 1, 'P1', 'RATES', '10', 'GBP', '2026-09-17',
           'Y', 'first', 'received/t-a/same.csv', 'batch-a', timestamp '2026-09-17 06:00',
           timestamp '2026-09-17 06:01', 'dlv_A', 'sv1', 'QA'),
          (date '2026-09-17', 2, 1, 'P1', 'RATES', '20', 'GBP', '2026-09-17',
           'Y', 'restated', 'received/t-b/same.csv', 'batch-b', timestamp '2026-09-17 07:00',
           timestamp '2026-09-17 07:01', 'dlv_B', 'sv1', 'QA'),
          (date '2026-09-18', 1, 1, 'P2', 'RATES', '30', 'GBP', '2026-09-18',
           'Y', 'second date', 'received/t-c/same.csv', 'batch-c', timestamp '2026-09-18 06:00',
           timestamp '2026-09-18 06:01', 'dlv_C', 'sv1', 'QA'),
          (date '2026-09-18', 1, 2, 'P3', 'RATES', '40', 'GBP', '2026-09-18',
           'Y', 'legacy', 'landing/qa_happy_position/legacy.csv', 'batch-c', null,
           timestamp '2026-09-18 06:01', null, 'sv0', 'QA')
        ) t(_cob_date, _file_version, _row_number, position_id, desk_code, amount,
            currency, effective_date, is_active, description, _source_file,
            _batch_id, _received_at, _ingest_ts, _delivery_id, _schema_version,
            _source_system)
    """)
    model = (REPO / "dbt/models/prepared/qa_happy_position.sql").read_text(encoding="utf-8")
    con.execute("create table prepared_result as " + _duck(_render(model)))
    assert con.execute(
        "select position_id, amount, delivery_id, source_file, source_file_version "
        "from prepared_result order by position_id"
    ).fetchall() == [
        ("P1", 20, "dlv_B", "received/t-b/same.csv", 2),
        ("P2", 30, "dlv_C", "received/t-c/same.csv", 1),
        ("P3", 40, "legacy.csv", "landing/qa_happy_position/legacy.csv", 1),
    ]
    # dlv_B and dlv_C came from the same producer filename, but remain two
    # accepted Deliveries in exactly the set registry.inputs.collect reads.
    assert con.execute(
        "select distinct delivery_id from prepared_result order by delivery_id"
    ).fetchall() == [("dlv_B",), ("dlv_C",), ("legacy.csv",)]


class _Row(dict):
    pass


class _Result:
    def __init__(self, columns=(), rows=()):
        self.columns = list(columns)
        self._rows = list(rows)

    def collect(self):
        return self._rows


class _Spark:
    def __init__(self):
        self.stopped = False

    def sql(self, query):
        if "LIMIT 0" in query:
            return _Result(["delivery_id", "cob_date"])
        if "DISTINCT delivery_id" in query:
            deliveries = (["dlv_A", "dlv_B"] if ".prepared.feed_a" in query
                          else ["legacy.csv", "dlv_C"])
            return _Result(rows=[_Row(delivery_id=d) for d in deliveries])
        if "MAX(cob_date)" in query:
            return _Result(rows=[_Row(d=date(2026, 9, 18))])
        raise AssertionError(query)

    def stop(self):
        self.stopped = True


def test_collect_unions_distinct_delivery_ids_across_feeds_on_the_branch():
    """This is also reporting lineage: reporting reads the prepared state."""
    config_dir()
    from reporting_platform.common import context
    from reporting_platform.registry import inputs

    spark = _Spark()
    originals = context.CATALOG, context.models_in, context.feeds, context.spark_session
    context.CATALOG = "lakehouse"
    context.models_in = lambda layer: ["feed_a", "feed_b"]
    context.feeds = lambda: {"feed_a": object(), "feed_b": object()}
    seen = []
    context.spark_session = lambda name, ref: (seen.append((name, ref)) or spark)
    try:
        result = inputs.collect("build/reporting/2026-09-18/r1")
    finally:
        context.CATALOG, context.models_in, context.feeds, context.spark_session = originals
    assert seen == [("run-inputs", "build/reporting/2026-09-18/r1")]
    assert result["inputs"] == [
        ["feed_a", "dlv_A"], ["feed_a", "dlv_B"],
        ["feed_b", "dlv_C"], ["feed_b", "legacy.csv"],
    ]
    assert result["max_cob_date"] == "2026-09-18"
    assert spark.stopped
