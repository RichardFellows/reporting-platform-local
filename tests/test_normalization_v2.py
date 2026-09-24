"""Phase 3 DeliveryManifest -> NormalizationManifest v2 contract."""
from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import io
import json
import zipfile
from contextlib import contextmanager

from reporting_platform.common.context import Feed
from reporting_platform.ingest.delivery import (
    Delivery, DeliverySourceFile, normalization_contract, serialize_manifest,
)
from reporting_platform.ingest import normalization
from tests.fakes3 import FakeS3


NOW = datetime(2026, 9, 17, 5, 43, 2, tzinfo=timezone.utc)


def _feed(*, kind="file", delimiter=",", member_pattern=r".*\.csv") -> Feed:
    delivery = {"kind": kind}
    if kind == "archive":
        delivery["member_pattern"] = member_pattern
    return Feed(
        name="qa_happy_position", description="test", source_system="QA",
        filename_pattern=r"never-used-(?P<date>\d{8})\.csv",
        business_key=["id"], columns=["id", "value"],
        delimiter=delimiter, delivery=delivery,
        source_columns={"value": "Producer Value"},
    )


def _zip(items: list[tuple[str, bytes]]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        for name, body in items:
            zf.writestr(name, body)
    return out.getvalue()


def _install_delivery(s3: FakeS3, fd: Feed, *, delivery_id="dlv_a",
                      filename="awkward original name.csv", body=b"id,value\n1,x\n",
                      snapshot=True, assertions=None):
    source_key = f"received/transport-{delivery_id}/{filename}"
    s3.put(source_key, body, NOW)
    source = DeliverySourceFile(
        role="data", original_filename=filename, object_key=source_key,
        bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
    control = DeliverySourceFile(
        role="control", original_filename="unrelated-name.ctl",
        object_key=f"received/transport-{delivery_id}/unrelated-name.ctl",
        bytes=10, sha256="0" * 64)
    contract = {
        "schema_version": fd.schema_version,
        "identity_sources": ["control"],
        "filename_pattern": fd.filename_pattern,
        "control": {},
    }
    if snapshot:
        contract["normalization"] = normalization_contract(fd)
    delivery = Delivery(
        delivery_id=delivery_id, feed=fd.name,
        transport_id=f"transport-{delivery_id}", transport_source="DCM",
        external_feed_id="42", business_date=date(2026, 9, 17),
        file_version=2, received_at=NOW, source_observed_at=NOW,
        schema_version=fd.schema_version,
        completion_marker=f"received/transport-{delivery_id}/_COMPLETE.json",
        source_files=(source, control),
        producer_assertions=assertions or {"row_count": 1, "md5": "abc"},
        identity={"business_date_source": "control"}, feed_contract=contract)
    key = f"deliveries/DCM/transport-{delivery_id}/delivery-manifest.json"
    s3.put(key, serialize_manifest(delivery), NOW)
    return delivery, key, source_key


def _normalize(s3, key, registry):
    return normalization.normalize_delivery(
        key, client=s3, bucket="lakehouse", registry=registry)


def test_plain_file_passes_received_object_through_without_filename_parsing_or_copy():
    s3, fd = FakeS3(), _feed()
    delivery, key, source_key = _install_delivery(s3, fd)
    before = set(s3.objects)

    result = _normalize(s3, key, {fd.name: fd})

    manifest = result.manifest
    assert result.key == f"ready/{fd.name}/{delivery.delivery_id}/normalization-manifest.json"
    assert manifest["business_date"] == "2026-09-17"
    assert manifest["delivery_id"] == delivery.delivery_id
    assert manifest["parts"] == [{
        "object_key": source_key, "bytes": len(b"id,value\n1,x\n"),
        "source": {"original_filename": "awkward original name.csv"},
        "materialized": False}]
    assert set(s3.objects) - before == {result.key}
    assert manifest["declared_row_count"] == 1
    assert manifest["declared_md5"] == "abc"
    assert manifest["source_system"] == fd.source_system
    assert not any("unrelated-name.ctl" in call for call in s3.calls)


def test_same_filename_in_two_deliveries_cannot_collide_and_retries_are_identical():
    s3, fd = FakeS3(), _feed()
    _, first_key, _ = _install_delivery(s3, fd, delivery_id="dlv_first")
    _, second_key, _ = _install_delivery(s3, fd, delivery_id="dlv_second")
    first = _normalize(s3, first_key, {fd.name: fd})
    original = s3.objects[first.key][0]
    second = _normalize(s3, second_key, {fd.name: fd})
    retried = _normalize(s3, first_key, {fd.name: fd})
    assert first.key != second.key
    assert retried.manifest == first.manifest
    assert s3.objects[first.key][0] == original


def test_archive_extracts_sorted_members_to_deterministic_part_keys_and_rebuilds():
    s3, fd = FakeS3(), _feed(kind="archive")
    archive = _zip([("z.csv", b"z\n"), ("ignore.txt", b"x"), ("a.csv", b"a\n")])
    delivery, key, source_key = _install_delivery(
        s3, fd, filename="producer.zip", body=archive)
    first = _normalize(s3, key, {fd.name: fd})
    expected_keys = [
        f"ready/{fd.name}/{delivery.delivery_id}/part-0001.csv",
        f"ready/{fd.name}/{delivery.delivery_id}/part-0002.csv",
    ]
    assert [part["object_key"] for part in first.manifest["parts"]] == expected_keys
    assert [part["source"]["archive_member"] for part in first.manifest["parts"]] == [
        "a.csv", "z.csv"]
    assert source_key in s3.objects
    captured = {k: s3.objects[k][0] for k in [first.key, *expected_keys]}
    for artifact in captured:
        del s3.objects[artifact]
    rebuilt = _normalize(s3, key, {fd.name: fd})
    assert rebuilt.manifest == first.manifest
    assert {k: s3.objects[k][0] for k in captured} == captured


def test_archive_rejects_unsafe_or_empty_matching_content():
    for items, message in [
            ([("../escape.csv", b"x")], "contains a path"),
            ([("not-data.txt", b"x")], "no member matching")]:
        s3, fd = FakeS3(), _feed(kind="archive")
        _, key, _ = _install_delivery(
            s3, fd, filename="producer.zip", body=_zip(items))
        try:
            _normalize(s3, key, {fd.name: fd})
        except normalization.NormalizationError as exc:
            assert message in str(exc), exc
        else:
            raise AssertionError(f"expected NormalizationError containing {message!r}")


def test_partial_retry_is_safe_and_conflicting_part_is_refused():
    s3, fd = FakeS3(), _feed(kind="archive")
    delivery, key, _ = _install_delivery(
        s3, fd, filename="producer.zip", body=_zip([("a.csv", b"a"), ("b.csv", b"b")]))
    part1 = f"ready/{fd.name}/{delivery.delivery_id}/part-0001.csv"
    s3.put(part1, b"a")
    result = _normalize(s3, key, {fd.name: fd})
    assert len(result.manifest["parts"]) == 2
    del s3.objects[result.key]
    s3.put(part1, b"different")
    try:
        _normalize(s3, key, {fd.name: fd})
    except normalization.DerivedObjectConflict:
        pass
    else:
        raise AssertionError("expected conflicting deterministic part")


def test_snapshot_wins_over_changed_current_feed_and_legacy_fallback_is_explicit():
    s3, original = FakeS3(), _feed(delimiter="|")
    changed = _feed(delimiter=",")
    _, key, _ = _install_delivery(s3, original)
    result = _normalize(s3, key, {changed.name: changed})
    assert result.manifest["format"]["delimiter"] == "|"
    assert result.manifest["contract_source"] == "delivery_manifest"

    legacy_s3 = FakeS3()
    _, legacy_key, _ = _install_delivery(legacy_s3, original, snapshot=False)
    legacy = _normalize(legacy_s3, legacy_key, {original.name: original})
    assert legacy.manifest["contract_source"] == "current_feed_compatibility"
    # Once materialized, the v2 snapshot wins even if today's YAML changes.
    again = _normalize(legacy_s3, legacy_key, {changed.name: changed})
    assert again.manifest == legacy.manifest


def test_conflicting_existing_manifest_is_not_silently_replaced():
    s3, fd = FakeS3(), _feed()
    delivery, key, _ = _install_delivery(s3, fd)
    manifest_key = normalization.manifest_key(delivery)
    s3.put(manifest_key, json.dumps({
        "normalization_manifest_version": 2,
        "delivery_id": delivery.delivery_id,
    }))
    try:
        _normalize(s3, key, {fd.name: fd})
    except normalization.NormalizationError as exc:
        assert "missing fields" in str(exc), exc
    else:
        raise AssertionError("expected malformed existing manifest to fail")


def test_registry_projection_separates_source_evidence_from_normalization_parts():
    from reporting_platform.registry.deliveries import observations_v2

    s3, fd = FakeS3(), _feed(kind="archive")
    delivery, key, _ = _install_delivery(
        s3, fd, filename="producer.zip", body=_zip([("a.csv", b"a")]))
    result = normalization.normalize_delivery(
        key, client=s3, bucket="lakehouse", registry={fd.name: fd}, write=False)
    row = observations_v2(fd, delivery, result.manifest, "md5", "lakehouse")
    assert row["parts"] == []
    assert row["normalization_parts"] == [{
        "part_no": 0,
        "object_key": f"ready/{fd.name}/{delivery.delivery_id}/part-0001.csv",
        "bytes": 1, "source_member": "a.csv", "materialized": True}]
    assert row["source_object"].startswith("received/")
    assert row["origin"] == "transport:DCM"
    forbidden = {"ingested", "normalized", "processed", "status", "state"}
    assert forbidden.isdisjoint(row)


def test_registry_schema_keeps_v2_parts_out_of_legacy_delivery_part():
    from reporting_platform.registry import db

    assert "CREATE TABLE IF NOT EXISTS registry.normalization_part" in db.SCHEMA
    start = db.SCHEMA.index(
        "CREATE TABLE IF NOT EXISTS registry.normalization_part")
    ddl = db.SCHEMA[start:db.SCHEMA.index(");", start)]
    assert "materialized  BOOLEAN NOT NULL" in ddl
    assert "status" not in ddl.lower()


def test_v2_reconciliation_is_repeatable_and_uses_delivery_manifest_as_manifest_key():
    from reporting_platform.registry import db, deliveries

    s3, fd = FakeS3(), _feed()
    delivery, delivery_key, _ = _install_delivery(s3, fd)
    _normalize(s3, delivery_key, {fd.name: fd})

    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, args=None):
            statements.append((" ".join(sql.split()), args))

    class Connection:
        def cursor(self):
            return Cursor()

        def rollback(self):
            raise AssertionError("successful reconciliation must not roll back")

    @contextmanager
    def fake_connect():
        yield Connection()

    original = db.connect
    db.connect = fake_connect
    try:
        first = deliveries.reconcile_v2(
            client=s3, bucket="lakehouse", registry={fd.name: fd})
        second = deliveries.reconcile_v2(
            client=s3, bucket="lakehouse", registry={fd.name: fd})
    finally:
        db.connect = original
    assert first == second == {
        "registered": [delivery.delivery_id],
        "missing_normalization": [], "failed": []}
    delivery_inserts = [sql for sql, _ in statements
                        if sql.startswith("INSERT INTO registry.delivery ")]
    assert len(delivery_inserts) == 2
    assert all("ON CONFLICT (feed, delivery_id) DO UPDATE" in sql
               for sql in delivery_inserts)
    # Existing run_input is not touched by the reconstruction path.
    assert all("registry.run_input" not in sql for sql, _ in statements)
