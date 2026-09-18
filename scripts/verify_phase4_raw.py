"""Exercise Phase 4 against real MinIO, Spark, Iceberg and Nessie.

This is intentionally not part of the no-stack ``tests.run`` tier.  It writes
uniquely named v2 evidence and Raw rows into the local stack, then verifies the
physical data and the failure boundary on ``main``.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import uuid

from reporting_platform.common.context import Nessie, feed, spark_session
from reporting_platform.ingest import arrival
from reporting_platform.ingest.delivery import normalization_contract
from reporting_platform.ingest.ingest_feed import (
    already_ingested_delivery, ingest_normalized_delivery,
)


def _id(label: str) -> str:
    return "dlv_" + hashlib.sha256(label.encode()).hexdigest()[:32]


def _put(key: str, body: bytes) -> None:
    arrival._client().put_object(Bucket=arrival._bucket(), Key=key, Body=body)  # noqa: SLF001


def _manifest(fd, token: str, delivery_id: str, parts: list[str], *,
              business_date: str, declared_rows=None, declared_md5=None,
              contract_overrides=None) -> str:
    contract = {**normalization_contract(fd), **(contract_overrides or {})}
    key = f"ready/{fd.name}/{delivery_id}/normalization-manifest.json"
    manifest = {
        "normalization_manifest_version": 2,
        "delivery_id": delivery_id,
        "feed": fd.name,
        "delivery_manifest": f"deliveries/DCM/{token}/delivery-manifest.json",
        "business_date": business_date,
        "received_at": "2026-09-18T08:30:00Z",
        "schema_version": f"phase4-{token[:8]}",
        "source_system": fd.source_system,
        "normalizer": "archive/v2" if len(parts) > 1 else "file/v2",
        "format": contract["format"],
        "normalization_contract": contract,
        "contract_source": "delivery_manifest",
        "parts": [{"object_key": part, "bytes": None,
                   "materialized": part.startswith("ready/")} for part in parts],
        "checksum_objects": [parts[0]],
        "declared_row_count": declared_rows,
        "declared_md5": declared_md5,
        "source_object": parts[0],
    }
    _put(key, json.dumps(manifest, sort_keys=True).encode())
    return key


def _rows(spark, table: str, ids: list[str]):
    quoted = ", ".join("'" + value.replace("'", "''") + "'" for value in ids)
    return spark.sql(
        f"SELECT _delivery_id, _source_file, CAST(_cob_date AS STRING) AS business_date, "
        f"_schema_version, _source_system, _ingest_ts, _file_version "
        f"FROM {table} WHERE _delivery_id IN ({quoted})"
    ).collect()


def _must_fail(key: str, run_id: str, phrase: str, spark) -> None:
    try:
        ingest_normalized_delivery(key, run_id=run_id, spark=spark)
    except Exception as exc:  # noqa: BLE001 - verifier checks the public failure
        if phrase.lower() not in str(exc).lower():
            raise AssertionError(f"expected {phrase!r}, got {exc}") from exc
    else:
        raise AssertionError(f"expected {phrase!r} failure for {key}")


def main() -> int:
    fd = feed("qa_happy_position")
    token = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + uuid.uuid4().hex[:6]
    spark = spark_session(f"verify-phase4-{token}", ref="main")
    valid = (b"position_id|desk_code|amount|currency|effective_date|is_active|description\n"
             b"P1|D1|10.25|GBP|2020-01-02|true|phase4\n")

    # Plain file: awkward physical name and a business date deliberately not
    # present anywhere in that name.
    plain_id = _id(token + "-plain")
    plain_part = f"received/{token}-plain/awkward producer filename.csv"
    _put(plain_part, valid)
    plain_key = _manifest(fd, token + "-plain", plain_id, [plain_part],
                          business_date="2020-01-02", declared_rows=1,
                          declared_md5=hashlib.md5(valid).hexdigest())
    first = ingest_normalized_delivery(
        plain_key, run_id=f"p4-{token}-plain", spark=spark)
    second = ingest_normalized_delivery(
        plain_key, run_id=f"p4-{token}-plain-retry", spark=spark)
    assert first["rows"] == 1 and not first["already_ingested"]
    assert second["already_ingested"] and second["rows"] == 0

    # Archive/multipart: one DeliveryID, two physical Ready objects.
    archive_id = _id(token + "-archive")
    archive_parts = [
        f"ready/{fd.name}/{archive_id}/part-0001.csv",
        f"ready/{fd.name}/{archive_id}/part-0002.csv",
    ]
    _put(archive_parts[0], valid.replace(b"P1", b"A1"))
    _put(archive_parts[1], valid.replace(b"P1", b"A2"))
    archive_key = _manifest(fd, token + "-archive", archive_id, archive_parts,
                            business_date="2020-01-03", declared_rows=2,
                            contract_overrides={"kind": "archive",
                                                "member_pattern": ".*\\.csv"})
    archive_result = ingest_normalized_delivery(
        archive_key, run_id=f"p4-{token}-archive", spark=spark)
    assert archive_result["rows"] == 2

    # Same filename and identical bytes in distinct Transport occurrences.
    collision_ids = [_id(token + "-collision-a"), _id(token + "-collision-b")]
    collision_keys = []
    for number, delivery_id in enumerate(collision_ids, 1):
        part = f"received/{token}-collision-{number}/positions.csv"
        _put(part, valid)
        collision_keys.append(_manifest(
            fd, f"{token}-collision-{number}", delivery_id, [part],
            business_date="2020-01-04"))
    for number, key in enumerate(collision_keys, 1):
        ingest_normalized_delivery(
            key, run_id=f"p4-{token}-collision-{number}", spark=spark)

    # Existing controls still fail before a commit to main.
    failures = []
    cases = [
        ("declared", valid, {"declared_rows": 2}, "declared 2"),
        ("minimum", valid, {"contract_overrides": {"expected_min_rows": 2}},
         "below expected minimum"),
        ("checksum", valid, {"declared_md5": "0" * 32}, "hashes to"),
        ("drift", valid.replace(
            b"description\n", b"description|surprise\n").replace(
                b"phase4\n", b"phase4|extra\n"),
         {"contract_overrides": {"schema_drift": "fail"}}, "schema drift"),
        ("parse", b'position_id|desk_code\n"unterminated', {}, "malformed"),
    ]
    for label, body, kwargs, phrase in cases:
        delivery_id = _id(f"{token}-fail-{label}")
        part = f"received/{token}-fail-{label}/source.csv"
        _put(part, body)
        key = _manifest(fd, f"{token}-fail-{label}", delivery_id, [part],
                        business_date="2020-01-05", **kwargs)
        _must_fail(key, f"p4-{token}-fail-{label}", phrase, spark)
        failures.append(delivery_id)

    # Fail after the branch append but before publication.  Main must still
    # say absent, and a new attempt must commit exactly once.
    retry_id = _id(token + "-merge-retry")
    retry_part = f"received/{token}-merge-retry/source.csv"
    _put(retry_part, valid)
    retry_key = _manifest(fd, token + "-merge-retry", retry_id, [retry_part],
                          business_date="2020-01-06")
    original_merge = Nessie.merge

    def fail_merge(self, branch, into="main"):
        raise RuntimeError("induced merge failure before main commit")

    Nessie.merge = fail_merge
    try:
        _must_fail(retry_key, f"p4-{token}-retry-fail",
                   "induced merge failure", spark)
    finally:
        Nessie.merge = original_merge

    assert not already_ingested_delivery(spark, fd, retry_id)
    assert all(not already_ingested_delivery(spark, fd, item)
               for item in failures)
    retry_result = ingest_normalized_delivery(
        retry_key, run_id=f"p4-{token}-retry-success", spark=spark)
    assert retry_result["rows"] == 1 and not retry_result["already_ingested"]
    retry_noop = ingest_normalized_delivery(
        retry_key, run_id=f"p4-{token}-retry-noop", spark=spark)
    assert retry_noop["already_ingested"]

    wanted = [plain_id, archive_id, *collision_ids, retry_id]
    rows = _rows(spark, fd.raw_table, wanted)

    by_id = {delivery_id: [row for row in rows
                           if row["_delivery_id"] == delivery_id]
             for delivery_id in wanted}
    assert len(by_id[plain_id]) == 1
    plain = by_id[plain_id][0]
    assert plain["_source_file"] == plain_part
    assert plain["business_date"] == "2020-01-02"
    assert plain["_schema_version"] == f"phase4-{(token + '-plain')[:8]}"
    assert plain["_source_system"] == fd.source_system
    assert plain["_ingest_ts"] is not None and plain["_file_version"] > 0
    assert len(by_id[archive_id]) == 2
    assert {row["_source_file"] for row in by_id[archive_id]} == set(archive_parts)
    assert all(row["_delivery_id"] == archive_id for row in by_id[archive_id])
    assert all(len(by_id[item]) == 1 for item in collision_ids)
    assert len(by_id[retry_id]) == 1
    spark.stop()

    print(json.dumps({
        "status": "ok", "token": token, "plain": plain_id,
        "archive": archive_id, "collisions": collision_ids,
        "failure_retry": retry_id, "validated_control_failures": failures,
        "rows_verified": len(rows),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
