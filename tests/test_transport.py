"""The additive DCM -> S3 transport evidence boundary."""
from __future__ import annotations

from datetime import timezone
import hashlib
import json
from pathlib import Path
import tempfile

from tests.fakes3 import FakeS3


def _contract(transport_id="dcm-1234-98765", files=None):
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
    key = transport.complete_key(transport_id)
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
    document["transport_contract_version"] = 2
    _fails(document, key, "unsupported transport contract version 2")


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


# --------------------------------------------------------------- simulator
def _simulate(s3, transport_id, path, *,
              uploaded_at="2026-09-17T05:43:02Z"):
    from reporting_platform.ingest.dcm_simulator import simulate_transport
    return simulate_transport(
        transport_id=transport_id,
        legacy_feed_id="1234",
        source_observed_at="2026-09-17T05:42:17Z",
        uploaded_at=uploaded_at,
        producer_run_id="DCM-849217",
        files=[("data", path)],
        client=s3,
        bucket="lakehouse",
    )


def test_simulator_uploads_marker_after_all_source_objects():
    with tempfile.TemporaryDirectory() as folder:
        data = Path(folder) / "positions_final_FINAL2.zip"
        control = Path(folder) / "positions_final_FINAL2.ctl"
        data.write_bytes(b"zip bytes")
        control.write_bytes(b"ROWS=1")
        s3 = FakeS3()
        from reporting_platform.ingest.dcm_simulator import simulate_transport
        result = simulate_transport(
            transport_id="dcm-order-1", legacy_feed_id="1234",
            source_observed_at="2026-09-17T05:42:17Z",
            files=[("data", data), ("control", control)],
            client=s3, bucket="lakehouse")
        puts = [call.removeprefix("put:") for call in s3.calls
                if call.startswith("put:")]
        assert puts == [f.object_key for f in result.files] + [
            "received/dcm-order-1/_COMPLETE.json"], puts
        assert s3.objects[result.files[0].object_key][0] == data.read_bytes()
        assert s3.objects[result.files[1].object_key][0] == control.read_bytes()


def test_same_transport_and_bytes_is_an_idempotent_retry():
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"id,value\n1,x\n")
        s3 = FakeS3()
        first = _simulate(s3, "dcm-retry-1", path)
        before = dict(s3.objects)
        call_at = len(s3.calls)
        second = _simulate(
            s3, "dcm-retry-1", path,
            uploaded_at="2026-09-18T12:00:00Z")
        assert second == first
        assert second.uploaded_at == first.uploaded_at
        assert s3.objects == before
        assert not any(call.startswith("put:")
                       for call in s3.calls[call_at:]), s3.calls[call_at:]


def test_retry_is_independent_of_caller_file_order():
    from reporting_platform.ingest.dcm_simulator import simulate_transport

    with tempfile.TemporaryDirectory() as folder:
        data = Path(folder) / "positions.csv"
        control = Path(folder) / "positions.ctl"
        data.write_bytes(b"id,value\n1,x\n")
        control.write_bytes(b"ROWS=1")
        s3 = FakeS3()
        common = {
            "transport_id": "dcm-reordered-retry-1",
            "legacy_feed_id": "1234",
            "source_observed_at": "2026-09-17T05:42:17Z",
            "client": s3,
            "bucket": "lakehouse",
        }
        first = simulate_transport(
            files=[("control", control), ("data", data)], **common)
        second = simulate_transport(
            files=[("data", data), ("control", control)], **common)
        assert second == first
        assert [f.role for f in first.files] == ["data", "control"]


def test_same_transport_with_changed_bytes_fails_without_overwrite():
    from reporting_platform.ingest.dcm_simulator import TransportConflictError

    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"first")
        s3 = FakeS3()
        _simulate(s3, "dcm-conflict-1", path)
        before = dict(s3.objects)
        path.write_bytes(b"changed")
        try:
            _simulate(s3, "dcm-conflict-1", path)
        except TransportConflictError as exc:
            assert "different evidence" in str(exc), exc
        else:
            raise AssertionError("expected conflicting retry to fail")
        assert s3.objects == before


def test_same_transport_with_changed_metadata_fails_without_overwrite():
    from reporting_platform.ingest.dcm_simulator import (
        TransportConflictError, simulate_transport)

    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"same bytes")
        s3 = FakeS3()
        _simulate(s3, "dcm-metadata-conflict-1", path)
        before = dict(s3.objects)
        try:
            simulate_transport(
                transport_id="dcm-metadata-conflict-1",
                legacy_feed_id="different-feed",
                source_observed_at="2026-09-17T05:42:17Z",
                files=[("data", path)], client=s3, bucket="lakehouse")
        except TransportConflictError as exc:
            assert "different evidence" in str(exc), exc
        else:
            raise AssertionError("expected conflicting metadata to fail")
        assert s3.objects == before


def test_two_transports_can_preserve_the_same_original_filename():
    from reporting_platform.ingest import transport

    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"same bytes")
        s3 = FakeS3()
        first = _simulate(s3, "dcm-execution-1", path)
        second = _simulate(s3, "dcm-execution-2", path)
        assert first.files[0].original_filename == second.files[0].original_filename
        assert first.files[0].object_key != second.files[0].object_key
        assert transport.list_completed_transports(
            client=s3, bucket="lakehouse") == [
                "received/dcm-execution-1/_COMPLETE.json",
                "received/dcm-execution-2/_COMPLETE.json",
            ]
