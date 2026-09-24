"""Read-only assertions for tests/fixtures/happy_path after UI ingestion.

This is the live-stack complement to the fast unit suite: it follows one
control-gated delivery through object storage, the registry, raw Iceberg,
prepared, and reporting without creating or modifying any platform state.
"""
from datetime import date
from decimal import Decimal
import hashlib
import json

from reporting_platform.common.context import feeds
from reporting_platform.ingest import arrival, normalize
from reporting_platform.registry import db
from scripts.duckdb_console import connect

FEED = "qa_happy_position"
DELIVERY_ID = "qa_happy_position_20260914.csv"
LANDING_KEY = f"landing/{FEED}/{DELIVERY_ID}"
CONTROL_KEY = f"landing/{FEED}/qa_happy_position_20260914.ctl"
MANIFEST_KEY = f"ready/{FEED}/{DELIVERY_ID}.json"
EXPECTED_BYTES = 250
EXPECTED_MD5 = "5d662086c4b513b3acf5d0a6adc044b8"
EXPECTED_CONTROL = (
    b"BUSINESS_DATE|FILE_VERSION|RECORD_COUNT|CHECKSUM\n"
    b"20260914|1|3|5d662086c4b513b3acf5d0a6adc044b8\n"
)


def _object_bytes(key: str) -> bytes:
    return arrival._client().get_object(  # noqa: SLF001 - platform storage API
        Bucket=arrival._bucket(), Key=key)["Body"].read()


def _registry_delivery() -> dict:
    """Read the delivery and parts without running schema-ensure DDL."""
    columns = (
        "feed", "delivery_id", "source_system", "cob_date", "source_object",
        "manifest_key", "normalizer", "schema_version", "origin",
        "source_filename", "control_object", "declared_row_count",
        "declared_md5",
    )
    with db.connect(ensure=False) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(columns)} FROM registry.delivery "
            "WHERE feed = %s AND delivery_id = %s",
            (FEED, DELIVERY_ID),
        )
        row = cur.fetchone()
        assert row is not None, (FEED, DELIVERY_ID)
        result = dict(zip(columns, row))
        cur.execute(
            "SELECT part_no, object_key, bytes FROM registry.delivery_part "
            "WHERE feed = %s AND delivery_id = %s ORDER BY part_no",
            (FEED, DELIVERY_ID),
        )
        result["parts"] = [
            {"part_no": part_no, "object_key": object_key, "bytes": size}
            for part_no, object_key, size in cur.fetchall()
        ]
        cur.execute(
            "SELECT count(*) FROM registry.delivery "
            "WHERE feed = %s AND delivery_id = %s",
            (FEED, DELIVERY_ID),
        )
        assert cur.fetchone()[0] == 1
    return result


def main():
    csv_bytes = _object_bytes(LANDING_KEY)
    assert len(csv_bytes) == EXPECTED_BYTES, len(csv_bytes)
    assert hashlib.md5(csv_bytes).hexdigest() == EXPECTED_MD5
    assert _object_bytes(CONTROL_KEY) == EXPECTED_CONTROL

    fd = feeds()[FEED]
    manifest = normalize.read_manifest(MANIFEST_KEY)
    assert manifest["feed"] == FEED, manifest
    assert manifest["delivery_id"] == DELIVERY_ID, manifest
    assert manifest["cob_date"] == "2026-09-14", manifest
    assert manifest["source_object"] == LANDING_KEY, manifest
    assert manifest["parts"] == [
        {"object_key": LANDING_KEY, "bytes": len(csv_bytes)}
    ], manifest
    assert manifest["control_object"] == CONTROL_KEY, manifest
    assert manifest["declared_row_count"] == 3, manifest
    assert manifest["declared_md5"] == EXPECTED_MD5, manifest
    assert manifest["checksum_objects"] == [LANDING_KEY], manifest
    assert manifest["normalizer"] == "file/v1", manifest

    registry = _registry_delivery()
    assert registry["source_system"] == fd.source_system, registry
    assert registry["cob_date"] == date(2026, 9, 14), registry
    assert registry["source_object"] == LANDING_KEY, registry
    assert registry["manifest_key"] == MANIFEST_KEY, registry
    assert registry["normalizer"] == "file/v1", registry
    assert registry["schema_version"] == fd.schema_version, registry
    assert registry["origin"] == "direct", registry
    assert registry["source_filename"] is None, registry
    assert registry["control_object"] == CONTROL_KEY, registry
    assert registry["declared_row_count"] == 3, registry
    assert registry["declared_md5"] == EXPECTED_MD5, registry
    assert registry["parts"] == [
        {"part_no": 0, "object_key": LANDING_KEY, "bytes": len(csv_bytes)}
    ], registry
    for derived_state in ("ingested", "status", "processed", "state"):
        assert derived_state not in registry, derived_state

    con = connect()
    raw = con.execute("""
        select position_id, desk_code, amount, currency, effective_date,
               is_active, description
        from lakehouse.raw.qa_happy_position
        where _cob_date = DATE '2026-09-14' order by position_id
    """).fetchall()
    assert raw == [
        ('HP001', ' rates ', '1250.50', 'gbp', '2026-09-14', 'Y', 'First dummy position'),
        ('HP002', 'credit', '249.50', 'GBP', '2026-09-14', 'N', 'Second dummy position'),
        ('HP003', 'rates', '-100.00', 'usd', '2026-09-14', 'true', 'Quoted | pipe'),
    ], raw
    raw_provenance = con.execute("""
        select distinct _source_file, _delivery_id, _cob_date, _file_version,
                        _schema_version, _source_system
        from lakehouse.raw.qa_happy_position
        where _cob_date = DATE '2026-09-14'
    """).fetchall()
    assert raw_provenance == [(
        LANDING_KEY, DELIVERY_ID, date(2026, 9, 14), 1,
        fd.schema_version, fd.source_system,
    )], raw_provenance
    prepared = con.execute("""
        select position_id, desk_code, amount, currency, effective_date, is_active,
               description
        from lakehouse.prepared.qa_happy_position
        where cob_date = DATE '2026-09-14' order by position_id
    """).fetchall()
    assert prepared == [
        ('HP001', 'RATES', Decimal('1250.50'), 'GBP', date(2026, 9, 14), True, 'First dummy position'),
        ('HP002', 'CREDIT', Decimal('249.50'), 'GBP', date(2026, 9, 14), False, 'Second dummy position'),
        ('HP003', 'RATES', Decimal('-100.00'), 'USD', date(2026, 9, 14), True, 'Quoted | pipe'),
    ], prepared
    prepared_provenance = con.execute("""
        select distinct source_file, delivery_id, cob_date,
                        source_file_version, schema_version, source_system
        from lakehouse.prepared.qa_happy_position
        where cob_date = DATE '2026-09-14'
    """).fetchall()
    assert prepared_provenance == [(
        LANDING_KEY, DELIVERY_ID, date(2026, 9, 14), 1,
        fd.schema_version, fd.source_system,
    )], prepared_provenance
    reporting = con.execute("""
        select currency, position_count, total_amount, active_position_count
        from lakehouse.reporting.qa_happy_position_summary
        where cob_date = DATE '2026-09-14' order by currency
    """).fetchall()
    assert reporting == [('GBP', 2, Decimal('1500.00'), 1),
                         ('USD', 1, Decimal('-100.00'), 1)], reporting
    schemas = {}
    for layer, table in [('raw', 'qa_happy_position'),
                         ('prepared', 'qa_happy_position'),
                         ('reporting', 'qa_happy_position_summary')]:
        schemas[layer] = con.execute(f'DESCRIBE lakehouse.{layer}.{table}').fetchall()
    raw_types = {row[0]: row[1] for row in schemas['raw']}
    assert all(raw_types[c] == 'VARCHAR' for c in (
        'position_id', 'desk_code', 'amount', 'currency', 'effective_date',
        'is_active', 'description')), raw_types
    prepared_types = {row[0]: row[1] for row in schemas['prepared']}
    assert prepared_types['amount'] == 'DECIMAL(18,2)', prepared_types
    assert prepared_types['effective_date'] == 'DATE', prepared_types
    assert prepared_types['is_active'] == 'BOOLEAN', prepared_types
    print(json.dumps({'status': 'PASS', 'catalog_ref': 'main',
                      'objects': [LANDING_KEY, CONTROL_KEY, MANIFEST_KEY],
                      'manifest': manifest, 'registry': registry,
                      'raw': raw, 'raw_provenance': raw_provenance,
                      'prepared': prepared,
                      'prepared_provenance': prepared_provenance,
                      'reporting': reporting, 'schemas': schemas},
                     default=str, indent=2))
    con.close()


if __name__ == '__main__':
    main()
