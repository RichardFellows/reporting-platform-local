"""The DCM -> S3 transport evidence boundary: contract v1 (legacy-read) and
v2 (current) parsing, path rules, and evidence validation.

Publisher-side behaviour (idempotent upload, conflict detection, deterministic
TransportID derivation, the CLI) lives in ``reporting_transport`` and is
tested in ``tests/test_reporting_transport.py`` -- this file is the RPL
consumer side only.
"""
from __future__ import annotations

from datetime import timezone
import hashlib
import json

from tests.fakes3 import FakeS3


def _contract(transport_id="dcm-1234-98765", files=None):
    """A v1 (legacy) marker document, still the platform's read-compat shape."""
    from reporting_platform.ingest import transport

    bodies = files or [("data", "positions_final_FINAL2.zip", b"zip bytes")]
    declarations = [{
        "role": role,
        "original_filename": name,
        "object_key": f"received/{transport_id}/{name}",
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    } for role, name, body in bodies]
    document = {
        "transport_contract_version": 1,
        "transport_id": transport_id,
        "source": "DCM",
        "legacy_feed_id": "1234",
        "source_observed_at": "2026-09-17T05:42:17Z",
        "uploaded_at": "2026-09-17T05:43:02Z",
        "producer_run_id": "DCM-849217",
        "files": declarations,
    }
    key = transport.complete_key_v1(transport_id)
    return document, key, bodies


def _contract_v2(transport_id="dcm-1234-849217", files=None,
                 cob_date="2026-09-21", source_system="RISK_ENGINE_X"):
    from reporting_platform.ingest import transport

    bodies = files or [("data", "positions_20260921.csv", b"id,value\n1,x\n")]
    declarations = [{
        "role": role,
        "original_filename": name,
        "object_key": (f"received/cob_date={cob_date}/"
                       f"source_system={source_system}/{transport_id}/{name}"),
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    } for role, name, body in bodies]
    document = {
        "transport_contract_version": 2,
        "transport_id": transport_id,
        "source": "DCM",
        "legacy_feed_id": "1234",
        "producer_run_id": "849217",
        "cob_date": cob_date,
        "source_system": source_system,
        "source_observed_at": "2026-09-17T05:42:17Z",
        "uploaded_at": "2026-09-17T05:43:02Z",
        "files": declarations,
    }
    key = transport.complete_key(cob_date, source_system, transport_id)
    return document, key, bodies


def _parse(document, key):
    from reporting_platform.ingest import transport
    return transport.parse_transport(json.dumps(document), key)


def _fails(document, key, contains):
    from reporting_platform.ingest import transport
    try:
        _parse(document, key)
    except transport.TransportContractError as exc:
        assert contains in str(exc), exc
    else:
        raise AssertionError("expected TransportContractError")


def _put_evidence(s3, document, key, bodies, marker=True):
    for declaration, (_, _, body) in zip(document["files"], bodies):
        s3.put(declaration["object_key"], body)
    if marker:
        s3.put(key, json.dumps(document))


# ------------------------------------------------------------ parsing/model
def test_valid_single_file_transport_is_typed():
    document, key, _ = _contract()
    parsed = _parse(document, key)
    assert parsed.transport_contract_version == 1
    assert parsed.transport_id == "dcm-1234-98765"
    assert parsed.source == "DCM"
    assert parsed.legacy_feed_id == "1234"
    assert parsed.producer_run_id == "DCM-849217"
    assert parsed.source_observed_at.tzinfo == timezone.utc
    assert len(parsed.data_files) == 1
    assert parsed.control_files == ()


def test_valid_data_and_control_transport():
    document, key, _ = _contract(files=[
        ("data", "positions.zip", b"data"),
        ("control", "positions.ctl", b"ROWS=1"),
    ])
    parsed = _parse(document, key)
    assert [f.role for f in parsed.files] == ["data", "control"]
    assert parsed.control_files[0].original_filename == "positions.ctl"


def test_valid_multiple_data_objects():
    document, key, _ = _contract(files=[
        ("data", "positions_1.csv", b"one"),
        ("data", "positions_2.csv", b"two"),
    ])
    parsed = _parse(document, key)
    assert [f.original_filename for f in parsed.data_files] == [
        "positions_1.csv", "positions_2.csv"]


def test_unsupported_contract_version_is_cleanly_rejected():
    document, key, _ = _contract()
    document["transport_contract_version"] = 3
    _fails(document, key, "unsupported transport contract version 3")


def test_malformed_json_and_missing_required_fields_are_rejected():
    from reporting_platform.ingest import transport

    _, key, _ = _contract()
    try:
        transport.parse_transport("{not JSON", key)
    except transport.TransportContractError as exc:
        assert "malformed completion JSON" in str(exc), exc
    else:
        raise AssertionError("expected malformed JSON to fail")

    try:
        transport.parse_transport(
            '{"transport_contract_version":1,'
            '"transport_contract_version":1}', key)
    except transport.TransportContractError as exc:
        assert "duplicate JSON member" in str(exc), exc
    else:
        raise AssertionError("expected duplicate JSON member to fail")

    document, key, _ = _contract()
    for field in ("transport_id", "source", "legacy_feed_id",
                  "source_observed_at", "uploaded_at", "files"):
        broken = dict(document)
        broken.pop(field)
        _fails(broken, key, field)


def test_empty_transport_and_bad_file_values_are_rejected():
    document, key, _ = _contract()
    document["transport_id"] = ""
    _fails(document, key, "transport_id must not be empty")

    document, key, _ = _contract()
    document["files"] = []
    _fails(document, key, "at least one data object")

    for field, value, message in (
        ("bytes", -1, "invalid byte count"),
        ("sha256", "not-a-digest", "malformed SHA-256"),
        ("role", "manifest", "unsupported file role"),
        ("role", ["data"], "unsupported file role"),
    ):
        document, key, _ = _contract()
        document["files"][0][field] = value
        _fails(document, key, message)


def test_duplicate_object_declarations_are_rejected():
    document, key, _ = _contract()
    document["files"].append(dict(document["files"][0]))
    _fails(document, key, "duplicate object declaration")


def test_prefix_escape_and_unsafe_original_names_are_rejected():
    for object_key in (
        "received/other/positions.zip",
        "received/dcm-1234-98765/../positions.zip",
        "landing/feed/positions.zip",
    ):
        document, key, _ = _contract()
        document["files"][0]["object_key"] = object_key
        _fails(document, key, "must be stored unchanged")

    for filename in ("../positions.zip", "folder/positions.zip",
                     "folder\\positions.zip", "_COMPLETE.json"):
        document, key, _ = _contract()
        document["files"][0]["original_filename"] = filename
        document["files"][0]["object_key"] = \
            f"received/dcm-1234-98765/{filename}"
        _fails(document, key, "original filename" if filename != "_COMPLETE.json"
               else "cannot be a source filename")


def test_marker_path_must_agree_with_its_transport_id():
    document, _, _ = _contract()
    _fails(document, "received/someone-else/_COMPLETE.json",
           "marker key must be")


def test_transport_id_is_opaque_but_must_be_one_safe_segment():
    document, key, _ = _contract()
    for transport_id in ("../escape", "nested/id", "nested\\id", ".."):
        broken = dict(document)
        broken["transport_id"] = transport_id
        _fails(broken, key, "transport_id")


def test_received_prefix_is_configurable_without_becoming_landing():
    from reporting_platform.ingest import transport

    document, _, _ = _contract()
    document["files"][0]["object_key"] = (
        "dcm-received/dcm-1234-98765/positions_final_FINAL2.zip")
    key = "dcm-received/dcm-1234-98765/_COMPLETE.json"
    parsed = transport.parse_transport(json.dumps(document), key,
                                       prefix="dcm-received")
    assert parsed.files[0].object_key.startswith("dcm-received/")
    assert not parsed.files[0].object_key.startswith("landing/")


# --------------------------------------------------------- contract v2
def test_valid_v2_marker_is_typed_with_cob_date_and_source_system():
    document, key, _ = _contract_v2()
    parsed = _parse(document, key)
    assert parsed.transport_contract_version == 2
    assert parsed.cob_date == "2026-09-21"
    assert parsed.source_system == "RISK_ENGINE_X"
    assert parsed.producer_run_id == "849217"
    assert parsed.files[0].object_key == (
        "received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/"
        "dcm-1234-849217/positions_20260921.csv")


def test_v2_producer_run_id_is_required():
    document, key, _ = _contract_v2()
    document.pop("producer_run_id")
    _fails(document, key, "producer_run_id")


def test_v2_rejects_an_invalid_cob_date():
    for bad in ("2026-9-21", "not-a-date", "20260921", ""):
        document, key, _ = _contract_v2()
        document["cob_date"] = bad
        _fails(document, key, "cob_date")


def test_v2_rejects_an_unsafe_source_system_segment():
    for bad in ("..", ".", "risk/engine", "risk engine", ""):
        document, key, _ = _contract_v2()
        document["source_system"] = bad
        _fails(document, key, "source_system")


def test_v2_marker_key_must_agree_with_cob_date_and_source_system():
    document, key, _ = _contract_v2()
    _fails(document, "received/cob_date=2026-09-21/source_system=OTHER/"
           "dcm-1234-849217/_COMPLETE.json", "marker key must be")
    _fails(document, "received/cob_date=2026-09-22/"
           "source_system=RISK_ENGINE_X/dcm-1234-849217/_COMPLETE.json",
           "marker key must be")


def test_v2_object_key_must_include_the_full_partition_path():
    document, key, _ = _contract_v2()
    document["files"][0]["object_key"] = (
        "received/dcm-1234-849217/positions_20260921.csv")
    _fails(document, key, "must be stored unchanged")


def test_v1_and_v2_markers_both_parse_and_stay_distinguishable():
    """Migration policy: v1 markers remain permanently readable, never
    rewritten. There is no tool in this platform that migrates historical
    ``received/`` evidence -- see docs/TRANSPORT-CONTRACT.md, "v1
    compatibility"."""
    v1_document, v1_key, _ = _contract()
    v2_document, v2_key, _ = _contract_v2()
    v1 = _parse(v1_document, v1_key)
    v2 = _parse(v2_document, v2_key)
    assert v1.transport_contract_version == 1
    assert v1.cob_date is None and v1.source_system is None
    assert v2.transport_contract_version == 2
    assert v2.cob_date is not None and v2.source_system is not None


# ------------------------------------------------------- stored evidence
def test_referenced_evidence_is_checked_by_size_and_sha256():
    from reporting_platform.ingest import transport

    document, key, bodies = _contract()
    parsed = _parse(document, key)
    s3 = FakeS3()
    _put_evidence(s3, document, key, bodies)
    assert transport.validate_transport(parsed, client=s3,
                                        bucket="lakehouse") is parsed
    assert any(call.startswith("get:") for call in s3.calls)


def test_missing_object_is_rejected():
    from reporting_platform.ingest import transport

    document, key, _ = _contract()
    parsed = _parse(document, key)
    try:
        transport.validate_transport(parsed, client=FakeS3(),
                                     bucket="lakehouse")
    except transport.TransportEvidenceError as exc:
        assert parsed.transport_id in str(exc) and "missing object" in str(exc)
    else:
        raise AssertionError("expected missing evidence to fail")


def test_byte_count_and_sha256_mismatches_are_rejected():
    from reporting_platform.ingest import transport

    document, key, bodies = _contract()
    s3 = FakeS3()
    s3.put(document["files"][0]["object_key"], b"wrong length")
    try:
        transport.validate_transport(_parse(document, key), client=s3,
                                     bucket="lakehouse")
    except transport.TransportEvidenceError as exc:
        assert "declares" in str(exc) and "bytes" in str(exc)
    else:
        raise AssertionError("expected byte mismatch to fail")

    same_length_wrong_bytes = b"bad bytes"
    assert len(same_length_wrong_bytes) == len(bodies[0][2])
    s3.put(document["files"][0]["object_key"], same_length_wrong_bytes)
    try:
        transport.validate_transport(_parse(document, key), client=s3,
                                     bucket="lakehouse")
    except transport.TransportEvidenceError as exc:
        assert "SHA-256" in str(exc)
    else:
        raise AssertionError("expected SHA-256 mismatch to fail")


# --------------------------------------------------------- completion signal
def test_sources_without_marker_are_not_completed_transports():
    from reporting_platform.ingest import transport

    document, key, bodies = _contract()
    s3 = FakeS3()
    _put_evidence(s3, document, key, bodies, marker=False)
    assert transport.list_completed_transports(
        client=s3, bucket="lakehouse") == []


def test_marker_makes_transport_discoverable_and_readable():
    from reporting_platform.ingest import transport

    document, key, bodies = _contract()
    s3 = FakeS3()
    _put_evidence(s3, document, key, bodies)
    assert transport.list_completed_transports(
        client=s3, bucket="lakehouse") == [key]
    result = transport.read_validated_transport(
        key, client=s3, bucket="lakehouse")
    assert result.transport_id == document["transport_id"]


def test_discoverable_marker_with_missing_object_fails_validation():
    from reporting_platform.ingest import transport

    document, key, _ = _contract()
    s3 = FakeS3()
    s3.put(key, json.dumps(document))
    assert transport.list_completed_transports(
        client=s3, bucket="lakehouse") == [key]
    try:
        transport.read_validated_transport(key, client=s3, bucket="lakehouse")
    except transport.TransportEvidenceError as exc:
        assert "missing object" in str(exc)
    else:
        raise AssertionError("expected invalid visible transport to fail")


# ------------------------------------------------- discovery (v1+v2, bounded)
# Publisher-side behaviour (idempotent upload, conflict detection,
# deterministic TransportID derivation, the CLI) is exercised against
# reporting_transport.publisher directly in test_reporting_transport.py.
def test_v2_marker_is_discoverable_at_any_depth_and_readable():
    from reporting_platform.ingest import transport

    document, key, bodies = _contract_v2()
    s3 = FakeS3()
    _put_evidence(s3, document, key, bodies)
    assert transport.list_completed_transports(
        client=s3, bucket="lakehouse") == [key]
    result = transport.read_validated_transport(
        key, client=s3, bucket="lakehouse")
    assert result.transport_id == document["transport_id"]
    assert result.cob_date == "2026-09-21"


def test_unbounded_listing_finds_both_v1_and_v2_markers():
    from reporting_platform.ingest import transport

    v1_document, v1_key, v1_bodies = _contract(transport_id="dcm-legacy-1")
    v2_document, v2_key, v2_bodies = _contract_v2(transport_id="dcm-1234-1")
    s3 = FakeS3()
    _put_evidence(s3, v1_document, v1_key, v1_bodies)
    _put_evidence(s3, v2_document, v2_key, v2_bodies)
    assert transport.list_completed_transports(
        client=s3, bucket="lakehouse") == sorted([v1_key, v2_key])


def test_bounded_listing_targets_only_the_given_cob_partitions():
    """Normal reconciliation should not have to re-list every Transport ever
    published -- docs/AIRFLOW-ORCHESTRATION.md, "Reconciliation scale (v2)".
    A v1 marker has no cob_date= partition and is therefore never found this
    way; only the unbounded (cob_dates=None) listing finds it."""
    from reporting_platform.ingest import transport

    v1_document, v1_key, v1_bodies = _contract(transport_id="dcm-legacy-1")
    in_window, in_key, in_bodies = _contract_v2(
        transport_id="dcm-1234-in-window", cob_date="2026-09-20")
    out_window, out_key, out_bodies = _contract_v2(
        transport_id="dcm-1234-out-of-window", cob_date="2026-08-01")
    s3 = FakeS3()
    _put_evidence(s3, v1_document, v1_key, v1_bodies)
    _put_evidence(s3, in_window, in_key, in_bodies)
    _put_evidence(s3, out_window, out_key, out_bodies)

    bounded = transport.list_completed_transports(
        client=s3, bucket="lakehouse",
        cob_dates=["2026-09-19", "2026-09-20", "2026-09-21"])
    assert bounded == [in_key]

    unbounded = transport.list_completed_transports(
        client=s3, bucket="lakehouse")
    assert unbounded == sorted([v1_key, in_key, out_key])
