"""Phase 2 Transport -> Delivery contract tests (pure Python + FakeS3)."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json

from reporting_platform.common.context import (
    Feed, check_source_identifiers_are_unique,
)
from reporting_platform.ingest import delivery, transport
from tests.fakes3 import FakeS3


def _feed(*, name="ref_positions", external_id="1234",
          pattern=r"positions_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv",
          identity=None, control=None, columns=None):
    return Feed(
        name=name, description="positions", source_system="REF",
        filename_pattern=pattern, business_key=["id"],
        columns=columns or ["id", "value"],
        source_identifiers={"DCM": external_id} if external_id else {},
        delivery_identity=identity or ["filename"],
        delivery={"kind": "file", **({"control": control} if control else {})},
    )


CONTROL = {
    "pattern": r"{stem}\.ctl",
    "format": {"kind": "key_value", "separator": "="},
    "cob_date": "BUSINESS_DATE",
    "version": "FILE_VERSION",
    "row_count": "ROWS",
    "md5": "MD5",
}


def _put_transport(s3, transport_id="dcm-1234-98765", *,
                   data=None, controls=None, external_id="1234",
                   filename="positions_20260917_v2.csv",
                   producer_run_id="DCM-849217"):
    data = [(filename, b"id,value\n1,x\n")] if data is None else data
    controls = [] if controls is None else controls
    files = []
    for role, items in (("data", data), ("control", controls)):
        for original_filename, body in items:
            object_key = f"received/{transport_id}/{original_filename}"
            s3.put(object_key, body)
            files.append({
                "role": role,
                "original_filename": original_filename,
                "object_key": object_key,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            })
    raw = {
        "transport_contract_version": 1,
        "transport_id": transport_id,
        "source": "DCM",
        "legacy_feed_id": external_id,
        "source_observed_at": "2026-09-17T05:42:17Z",
        "uploaded_at": "2026-09-17T05:43:02Z",
        "files": files,
    }
    if producer_run_id is not None:
        raw["producer_run_id"] = producer_run_id
    marker = f"received/{transport_id}/_COMPLETE.json"
    s3.put(marker, json.dumps(raw, sort_keys=True))
    return marker


def _accepted(transport_id="dcm-1234-98765", external_id="1234",
              filename="positions_20260917_v2.csv"):
    s3 = FakeS3()
    marker = _put_transport(s3, transport_id, external_id=external_id,
                            filename=filename)
    return transport.read_validated_transport(
        marker, client=s3, bucket="lakehouse")


def _create(s3, marker, fd):
    return delivery.create_delivery(
        marker, client=s3, bucket="lakehouse", registry={fd.name: fd})


def test_known_external_feed_id_resolves_without_filename_guessing():
    accepted = _accepted(filename="utterly-awkward.bin")
    fd = _feed(pattern=r"never-matches", identity=["control"], control=CONTROL)
    assert delivery.resolve_transport_feed(accepted, {fd.name: fd}) is fd


def test_unknown_external_feed_id_fails_clearly():
    accepted = _accepted(external_id="unknown")
    fd = _feed()
    try:
        delivery.resolve_transport_feed(accepted, {fd.name: fd})
    except delivery.FeedResolutionError as exc:
        assert "unknown DCM external feed id 'unknown'" in str(exc), exc
    else:
        raise AssertionError("expected unknown mapping to fail")


def test_duplicate_external_mapping_is_rejected_by_config_validation():
    registry = {"one": _feed(name="one"), "two": _feed(name="two")}
    try:
        check_source_identifiers_are_unique(registry)
    except ValueError as exc:
        assert "must resolve exactly one Feed" in str(exc), exc
    else:
        raise AssertionError("expected duplicate source identifier to fail")


def test_unmapped_legacy_feed_coexists_with_mapped_feed():
    accepted = _accepted()
    mapped = _feed()
    legacy = _feed(name="legacy", external_id=None)
    assert delivery.resolve_transport_feed(
        accepted, {mapped.name: mapped, legacy.name: legacy}) is mapped


def test_same_transport_retry_returns_same_id_without_rewrite():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3)
    first = _create(s3, marker, fd)
    key = delivery.manifest_key(
        transport.read_transport(marker, client=s3, bucket="lakehouse"))
    before = s3.objects[key]
    call_at = len(s3.calls)
    second = _create(s3, marker, fd)
    assert second == first
    assert s3.objects[key] == before
    assert not any(call == f"put:{key}" for call in s3.calls[call_at:])


def test_different_transports_with_same_filename_are_distinct_occurrences():
    fd = _feed()
    s3 = FakeS3()
    one = _create(s3, _put_transport(s3, "dcm-one"), fd)
    two = _create(s3, _put_transport(s3, "dcm-two"), fd)
    assert one.delivery_id != two.delivery_id
    assert one.source_files[0].original_filename == two.source_files[0].original_filename


def test_delivery_id_does_not_depend_on_producer_filename():
    first = _accepted(filename="positions_20260917_v2.csv")
    second = replace(first, files=(replace(
        first.files[0], original_filename="renamed.csv",
        object_key="received/dcm-1234-98765/renamed.csv"),))
    assert delivery.delivery_id_for(first) == delivery.delivery_id_for(second)


def test_control_identity_allows_an_awkward_data_filename_and_captures_assertions():
    body = (b"BUSINESS_DATE=20260917\nFILE_VERSION=2\nROWS=4521847\n"
            b"MD5=0123456789abcdef0123456789abcdef\n")
    s3 = FakeS3()
    marker = _put_transport(
        s3, filename="positions_final_FINAL2.zip",
        controls=[("positions_final_FINAL2.ctl", body)])
    fd = _feed(pattern=r"never-matches", identity=["control", "filename"],
               control=CONTROL)
    got = _create(s3, marker, fd)
    assert got.business_date.isoformat() == "2026-09-17"
    assert got.file_version == 2
    assert got.identity["business_date_source"] == "control"
    assert got.identity["business_date_source_object"].endswith(
        "/positions_final_FINAL2.ctl")
    assert got.producer_assertions == {
        "business_date": "2026-09-17", "file_version": 2,
        "row_count": 4521847,
        "md5": "0123456789abcdef0123456789abcdef",
        "producer_run_id": "DCM-849217",
    }


def test_original_filename_identity_requires_no_rename():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3)
    got = _create(s3, marker, fd)
    assert got.business_date.isoformat() == "2026-09-17"
    assert got.file_version == 2
    assert got.identity["business_date_source"] == "filename"
    assert got.source_files[0].object_key.startswith("received/")
    assert not any(key.startswith("landing/") for key in s3.objects)


def test_control_and_filename_date_conflict_fails():
    body = (b"BUSINESS_DATE=20260918\nFILE_VERSION=2\nROWS=1\n"
            b"MD5=0123456789abcdef0123456789abcdef\n")
    s3 = FakeS3()
    marker = _put_transport(
        s3, controls=[("positions_20260917_v2.ctl", body)])
    fd = _feed(identity=["control", "filename"], control=CONTROL)
    try:
        _create(s3, marker, fd)
    except delivery.IdentityResolutionError as exc:
        assert "conflicting business_date" in str(exc), exc
    else:
        raise AssertionError("expected date conflict")


def test_conflicting_multi_data_file_dates_fail():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3, data=[
        ("positions_20260917_v2.csv", b"one"),
        ("positions_20260918_v2.csv", b"two"),
    ])
    try:
        _create(s3, marker, fd)
    except delivery.IdentityResolutionError as exc:
        assert "conflicting business_date in filename" in str(exc), exc
    else:
        raise AssertionError("expected multi-data conflict")


def test_conflicting_controls_fail():
    one = (b"BUSINESS_DATE=20260917\nFILE_VERSION=2\nROWS=1\n"
           b"MD5=0123456789abcdef0123456789abcdef\n")
    two = one.replace(b"20260917", b"20260918")
    s3 = FakeS3()
    marker = _put_transport(s3, data=[
        ("a.csv", b"one"), ("b.csv", b"two")], controls=[
        ("a.ctl", one), ("b.ctl", two)])
    fd = _feed(pattern=r"never", identity=["control"], control=CONTROL)
    try:
        _create(s3, marker, fd)
    except delivery.IdentityResolutionError as exc:
        assert "conflicting business_date in control" in str(exc), exc
    else:
        raise AssertionError("expected control conflict")


def test_missing_required_business_identity_fails():
    s3 = FakeS3()
    marker = _put_transport(s3, filename="no-date.csv")
    fd = _feed(pattern=r"never", identity=["filename"])
    try:
        _create(s3, marker, fd)
    except delivery.IdentityResolutionError as exc:
        assert "business date is missing" in str(exc), exc
    else:
        raise AssertionError("expected missing identity")


def test_manifest_preserves_received_evidence_without_copy_or_rename():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3)
    got = _create(s3, marker, fd)
    manifest = got.as_manifest()
    source = manifest["source_files"][0]
    assert source["object_key"] == "received/dcm-1234-98765/positions_20260917_v2.csv"
    assert source["original_filename"] == "positions_20260917_v2.csv"
    assert source["sha256"] == hashlib.sha256(b"id,value\n1,x\n").hexdigest()
    assert manifest["timestamps"]["received_at"] == "2026-09-17T05:43:02Z"
    assert not any(key.startswith("landing/") for key in s3.objects)


def test_corrupt_transport_prevents_delivery_creation():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3)
    data_key = "received/dcm-1234-98765/positions_20260917_v2.csv"
    s3.put(data_key, b"corrupt")
    try:
        _create(s3, marker, fd)
    except transport.TransportEvidenceError:
        pass
    else:
        raise AssertionError("expected corrupt Transport to fail")
    assert not any(key.startswith("deliveries/") for key in s3.objects)


def test_later_feed_change_does_not_reinterpret_or_overwrite_history():
    s3 = FakeS3()
    marker = _put_transport(s3)
    original = _feed(columns=["id", "value"])
    first = _create(s3, marker, original)
    changed = replace(original, columns=["id", "value", "new_column"],
                      filename_pattern=r"now-never-matches")
    second = _create(s3, marker, changed)
    assert second == first
    assert second.schema_version == original.schema_version
    assert second.feed_contract["filename_pattern"] == original.filename_pattern


def test_conflicting_existing_manifest_is_rejected_not_overwritten():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3)
    first = _create(s3, marker, fd)
    key = delivery.manifest_key(
        transport.read_transport(marker, client=s3, bucket="lakehouse"))
    raw = first.as_manifest()
    raw["transport"]["transport_id"] = "different"
    s3.put(key, json.dumps(raw))
    before = s3.objects[key]
    try:
        _create(s3, marker, fd)
    except delivery.DeliveryManifestError as exc:
        assert "conflicts with immutable Transport evidence" in str(exc), exc
    else:
        raise AssertionError("expected conflicting manifest")
    assert s3.objects[key] == before


def test_existing_manifest_business_interpretation_is_verified_from_its_snapshot():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3)
    first = _create(s3, marker, fd)
    key = delivery.manifest_key(
        transport.read_transport(marker, client=s3, bucket="lakehouse"))
    raw = first.as_manifest()
    raw["business_date"] = "2026-09-18"
    s3.put(key, json.dumps(raw))
    try:
        _create(s3, marker, replace(fd, filename_pattern=r"changed"))
    except delivery.DeliveryManifestError as exc:
        assert "business_date" in str(exc), exc
    else:
        raise AssertionError("expected historical interpretation conflict")


def test_delivery_manifest_has_no_mutable_processing_state():
    s3, fd = FakeS3(), _feed()
    manifest = _create(s3, _put_transport(s3), fd).as_manifest()
    forbidden = {"status", "ingested", "normalized", "published"}
    assert forbidden.isdisjoint(manifest)
    assert forbidden.isdisjoint(manifest["producer_assertions"])


def test_received_at_is_transport_upload_time_not_interpretation_time():
    s3, fd = FakeS3(), _feed()
    got = _create(s3, _put_transport(s3), fd)
    assert got.received_at == datetime(
        2026, 9, 17, 5, 43, 2, tzinfo=timezone.utc)


def test_new_delivery_snapshots_the_normalization_contract():
    s3 = FakeS3()
    fd = replace(_feed(), delimiter="|", quote_char="'", header=False,
                 file_encoding="cp1252", columns=["id", "value"],
                 source_columns={"value": "Producer Value"})
    got = _create(s3, _put_transport(s3), fd)
    snapshot = got.feed_contract["normalization"]
    assert snapshot["kind"] == "file"
    assert snapshot["format"]["delimiter"] == "|"
    assert snapshot["format"]["quote_char"] == "'"
    assert snapshot["format"]["header"] is False
    assert snapshot["format"]["encoding"] == "cp1252"
    assert snapshot["columns"] == ["id", "value"]
    assert snapshot["source_columns"] == {"value": "Producer Value"}
