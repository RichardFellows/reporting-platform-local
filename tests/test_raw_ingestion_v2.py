"""Phase 4's explicit NormalizationManifest v2 -> Raw contract.

The Spark/Iceberg path is exercised by ``scripts/verify_phase4_raw.py`` against
the real local stack.  These fast tests pin the decisions that must remain
true even where Spark is unavailable: historical contract selection and the
DeliveryID ledger query.
"""
from __future__ import annotations

from tests.support import config_dir


def _manifest(fd, **overrides):
    from reporting_platform.ingest.delivery import normalization_contract

    manifest = {
        "normalization_manifest_version": 2,
        "delivery_id": "dlv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "feed": fd.name,
        "business_date": "2026-09-17",
        "received_at": "2026-09-17T05:43:02Z",
        "schema_version": "historical-schema",
        "source_system": "HISTORICAL_SOURCE",
        "format": normalization_contract(fd)["format"],
        "normalization_contract": {
            **normalization_contract(fd),
            "source_system": "HISTORICAL_SOURCE",
            "expected_min_rows": 7,
            "schema_drift": "fail",
            "columns": ["position_id", "historical_value"],
            "source_columns": {"historical_value": "Old Value"},
        },
        "parts": [{"object_key": "received/t-1/awkward name.csv"}],
        "checksum_objects": ["received/t-1/awkward name.csv"],
        "delivery_manifest": "deliveries/DCM/t-1/delivery-manifest.json",
        "normalizer": "file/v2",
        "normalization_contract_version": 1,
        "contract_source": "delivery_manifest",
        "source_object": "received/t-1/awkward name.csv",
    }
    manifest.update(overrides)
    return manifest


def test_v2_uses_frozen_business_schema_and_source_contract():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest.ingest_feed import (
        _canonical_v2_manifest, _v2_feed_contract,
    )

    current = feeds()["qa_happy_position"]
    manifest = _canonical_v2_manifest(_manifest(current), "ready/x.json")
    historical = _v2_feed_contract(current, manifest)

    assert manifest["cob_date"] == "2026-09-17"
    assert manifest["parts"][0]["object_key"].endswith("awkward name.csv")
    assert historical.columns == ["position_id", "historical_value"]
    assert historical.source_column("historical_value") == "Old Value"
    assert historical.source_system == "HISTORICAL_SOURCE"
    assert historical.expected_min_rows == 7
    assert historical.schema_drift == "fail"
    assert manifest["schema_version"] == "historical-schema"


def test_v2_rejects_a_filename_masquerading_as_delivery_id():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest.ingest_feed import _canonical_v2_manifest

    manifest = _manifest(
        feeds()["qa_happy_position"], delivery_id="positions.csv")
    try:
        _canonical_v2_manifest(manifest, "ready/x.json")
    except ValueError as exc:
        assert "not opaque" in str(exc)
    else:
        raise AssertionError("v2 accepted a producer filename as DeliveryID")


def test_v2_rejects_a_top_level_format_that_conflicts_with_the_snapshot():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest.ingest_feed import (
        _canonical_v2_manifest, _v2_feed_contract,
    )

    current = feeds()["qa_happy_position"]
    manifest = _canonical_v2_manifest(
        _manifest(current, format={**_manifest(current)["format"],
                                   "delimiter": ","}),
        "ready/x.json")
    try:
        _v2_feed_contract(current, manifest)
    except ValueError as exc:
        assert "conflicts" in str(exc)
    else:
        raise AssertionError("v2 accepted two different parser contracts")


class _Rows:
    def __init__(self, rows=(), dtypes=()):
        self._rows = list(rows)
        self.dtypes = list(dtypes)

    def collect(self):
        return self._rows


class _Catalog:
    def __init__(self, exists=True):
        self.exists = exists
        self.refreshed = []

    def tableExists(self, _table):
        return self.exists

    def refreshTable(self, table):
        self.refreshed.append(table)


class _Spark:
    def __init__(self, present: set[str] = frozenset(), *, exists=True):
        self.present = present
        self.catalog = _Catalog(exists)
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if "LIMIT 0" in query:
            return _Rows(dtypes=[("_delivery_id", "string"),
                                 ("_source_file", "string")])
        return _Rows([{"present": 1}] if any(x in query for x in self.present) else [])


def test_v2_ledger_queries_delivery_id_not_physical_source_file():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest.ingest_feed import already_ingested_delivery

    delivery_id = "dlv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    spark = _Spark({delivery_id})
    assert already_ingested_delivery(
        spark, feeds()["qa_happy_position"], delivery_id)
    ledger_query = spark.queries[-1]
    assert "_delivery_id" in ledger_query
    assert "_source_file" not in ledger_query
    assert spark.catalog.refreshed == [feeds()["qa_happy_position"].raw_table]


def test_distinct_deliveries_are_not_collapsed_by_filename_or_bytes():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest.ingest_feed import already_ingested_delivery

    first = "dlv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    second = "dlv_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark = _Spark({first})
    assert already_ingested_delivery(spark, feeds()["qa_happy_position"], first)
    assert not already_ingested_delivery(spark, feeds()["qa_happy_position"], second)
    # Their producer filename and bytes are irrelevant to both ledger queries.
    assert all("positions.csv" not in query for query in spark.queries)


def test_missing_raw_table_means_retryable_not_ingested():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest.ingest_feed import already_ingested_delivery

    spark = _Spark(exists=False)
    assert not already_ingested_delivery(
        spark, feeds()["qa_happy_position"],
        "dlv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert spark.queries == []
