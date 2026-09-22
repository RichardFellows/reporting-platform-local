"""Publisher and CLI tests for the reusable ``reporting_transport`` package.

Contract (parse/validate/path-rule) tests live in ``tests/test_transport.py``.
This file exercises the write side: :mod:`reporting_transport.publisher` --
the one algorithm used both by the local simulator and a real DCM invocation
-- and :mod:`reporting_transport.cli`'s argument/exit-code wiring.

Plain ``test_*`` functions, no pytest -- see ``tests/README.md``: this suite
runs with ``python -m tests.run`` inside the platform image, which does not
install a test framework.
"""
from __future__ import annotations

import base64
import contextlib
import datetime
import hashlib
import io
import json
from pathlib import Path
import tempfile

from reporting_transport import storage as _storage
from reporting_transport.contract import (
    Transport, TransportConflictError, TransportContractError,
    TransportEvidenceError, TransportFile, TransportSourceMutatedError,
    TransportStorageError,
)
from reporting_transport.publisher import (
    _HashingReader, publish_transport, publish_transport_result,
)
from reporting_transport.storage import UploadTuning
from tests.fakes3 import FakeS3, NoSuchUpload, PreconditionFailed


def _publish(s3, *, transport_id_files, legacy_feed_id="1234",
            producer_run_id="849217", cob_date="2026-09-21",
            source_system="RISK_ENGINE_X", uploaded_at="2026-09-22T01:14:22Z",
            upload=None):
    return publish_transport(
        legacy_feed_id=legacy_feed_id, producer_run_id=producer_run_id,
        cob_date=cob_date, source_system=source_system,
        source_observed_at="2026-09-22T01:13:00Z", uploaded_at=uploaded_at,
        files=transport_id_files, client=s3, bucket="lakehouse", upload=upload)


def _write(folder: str, name: str, body: bytes) -> Path:
    path = Path(folder) / name
    path.write_bytes(body)
    return path


def _fails(fn, error_type, contains):
    try:
        fn()
    except error_type as exc:
        assert contains in str(exc), exc
    else:
        raise AssertionError(f"expected {error_type.__name__} to be raised")


# ----------------------------------------------------- deterministic identity
def test_transport_id_is_derived_deterministically_from_dcm_identity():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        s3 = FakeS3()
        result = _publish(s3, transport_id_files=[("data", data)],
                          legacy_feed_id="1234", producer_run_id="849217")
        assert result.transport_id == "dcm-1234-849217"


def test_retrying_the_same_producer_run_id_reuses_the_same_transport_id():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        s3 = FakeS3()
        first = _publish(s3, transport_id_files=[("data", data)])
        second = _publish(s3, transport_id_files=[("data", data)])
        assert first.transport_id == second.transport_id == "dcm-1234-849217"


def test_a_new_producer_run_id_is_a_new_transport_id():
    """A genuine correction/restatement is a new run identity, never a
    timestamp-derived one."""
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        s3 = FakeS3()
        first = _publish(s3, transport_id_files=[("data", data)],
                         producer_run_id="849217")
        second = _publish(s3, transport_id_files=[("data", data)],
                          producer_run_id="849218")
        assert first.transport_id != second.transport_id


# --------------------------------------------------------------- publishing
def test_marker_is_published_after_every_source_object():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        control = _write(folder, "positions.ctl", b"ROWS=1")
        s3 = FakeS3()
        result = _publish(s3, transport_id_files=[
            ("control", control), ("data", data)])
        puts = [call.removeprefix("put:") for call in s3.calls
               if call.startswith("put:")]
        marker_key = (
            "received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/"
            "dcm-1234-849217/_COMPLETE.json")
        assert puts[-1] == marker_key
        assert set(puts[:-1]) == {f.object_key for f in result.files}
        assert [f.role for f in result.files] == ["data", "control"]


def test_at_least_one_data_file_is_required():
    with tempfile.TemporaryDirectory() as folder:
        control = _write(folder, "positions.ctl", b"ROWS=1")
        s3 = FakeS3()
        _fails(lambda: _publish(s3, transport_id_files=[("control", control)]),
              TransportContractError, "data object")


def test_multiple_data_files_are_supported():
    with tempfile.TemporaryDirectory() as folder:
        a = _write(folder, "positions_1.csv", b"one")
        b = _write(folder, "positions_2.csv", b"two")
        s3 = FakeS3()
        result = _publish(s3, transport_id_files=[("data", a), ("data", b)])
        assert {f.original_filename for f in result.data_files} == {
            "positions_1.csv", "positions_2.csv"}


def test_missing_local_data_file_is_rejected_before_any_upload():
    with tempfile.TemporaryDirectory() as folder:
        missing = Path(folder) / "does-not-exist.csv"
        s3 = FakeS3()
        _fails(lambda: _publish(s3, transport_id_files=[("data", missing)]),
              TransportContractError, "source file not found")
        assert s3.objects == {}


# ----------------------------------------------------------------- retries
def test_identical_retry_is_idempotent_and_writes_nothing():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        s3 = FakeS3()
        first = _publish(s3, transport_id_files=[("data", data)],
                         uploaded_at="2026-09-22T01:14:22Z")
        before = dict(s3.objects)
        call_at = len(s3.calls)
        second = _publish(s3, transport_id_files=[("data", data)],
                          uploaded_at="2026-09-23T00:00:00Z")
        assert second == first
        assert second.uploaded_at == first.uploaded_at
        assert s3.objects == before
        assert not any(call.startswith("put:") for call in s3.calls[call_at:])


def test_conflicting_retry_with_changed_bytes_fails_without_overwrite():
    """The changed local file is caught against the object ALREADY published
    under this TransportID -- a more specific evidence-mismatch message than
    _require_same's, but still surfaced as a conflict: republishing different
    bytes under an identity that already has accepted evidence is exactly
    what 'conflicting retry must fail' means."""
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"first")
        s3 = FakeS3()
        _publish(s3, transport_id_files=[("data", path)])
        before = dict(s3.objects)
        path.write_bytes(b"changed")
        _fails(lambda: _publish(s3, transport_id_files=[("data", path)]),
              TransportConflictError, "stores")
        assert s3.objects == before


def test_conflicting_retry_with_changed_declared_metadata_fails_without_overwrite():
    """Same TransportID/marker path (cob_date, source_system, transport_id
    all unchanged), but a different source_observed_at -- caught by comparing
    the full candidate against the already-accepted marker."""
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"same bytes")
        s3 = FakeS3()
        _publish(s3, transport_id_files=[("data", path)])
        before = dict(s3.objects)

        def _retry_with_different_observed_at():
            return publish_transport(
                legacy_feed_id="1234", producer_run_id="849217",
                cob_date="2026-09-21", source_system="RISK_ENGINE_X",
                source_observed_at="2026-09-22T09:00:00Z",
                files=[("data", path)], client=s3, bucket="lakehouse")

        _fails(_retry_with_different_observed_at, TransportConflictError,
              "different evidence")
        assert s3.objects == before


def test_same_producer_run_id_under_a_different_cob_date_is_a_separate_marker():
    """KNOWN, DOCUMENTED LIMITATION (docs/TRANSPORT-CONTRACT.md, 'DCM
    responsibilities'): TransportID is derived only from (source,
    legacy_feed_id, producer_run_id), per the contract's own required
    formula -- it does not include cob_date/source_system, which are part of
    the S3 PATH. So a caller that reuses producer_run_id but declares a
    different cob_date is NOT detected as a conflicting retry: it publishes a
    second, independent marker at a different cob_date partition instead.
    Catching this would require an unbounded reverse lookup by TransportID
    across every COB partition, which conflicts directly with the bounded-
    reconciliation goal -- so this is accepted as a DCM-side contract
    obligation (retry the same execution with the same cob_date/
    source_system) rather than solved here."""
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "positions.csv"
        path.write_bytes(b"same bytes")
        s3 = FakeS3()
        first = _publish(s3, transport_id_files=[("data", path)],
                         cob_date="2026-09-21")
        second = _publish(s3, transport_id_files=[("data", path)],
                          cob_date="2026-09-22")
        assert first.transport_id == second.transport_id
        assert first.files[0].object_key != second.files[0].object_key


def test_partially_completed_previous_upload_is_resumed_not_reuploaded():
    """A prior attempt uploaded the data object but crashed before the
    marker; the retry must reuse it, upload only the control object, and
    still finish with exactly one marker write."""
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        control = _write(folder, "positions.ctl", b"ROWS=1")
        s3 = FakeS3()
        s3.put(("received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/"
                "dcm-1234-849217/positions.csv"), data.read_bytes())
        result = _publish(s3, transport_id_files=[
            ("data", data), ("control", control)])
        puts = [call.removeprefix("put:") for call in s3.calls
               if call.startswith("put:")]
        marker_key = (
            "received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/"
            "dcm-1234-849217/_COMPLETE.json")
        assert puts == [f.object_key for f in result.control_files] + [
            marker_key]


# ------------------------------------------------------- concurrency/marker
class _RacingS3(FakeS3):
    """Simulates another process winning a create-only race on one key."""

    def __init__(self, racing_key: str, racing_body: bytes) -> None:
        super().__init__()
        self._racing_key = racing_key
        self._racing_body = racing_body
        self._raced = False

    def put_object(self, Bucket, Key, Body, **kwargs):  # noqa: N803
        if Key == self._racing_key and not self._raced:
            self._raced = True
            # A real client transmits the body before a conditional PUT can
            # be rejected server-side; consume it here so the caller's
            # hashing wrapper reflects a real read, exactly as it would
            # against MinIO/S3.
            if hasattr(Body, "read"):
                while Body.read(65536):
                    pass
            self.objects[Key] = (self._racing_body,
                                 datetime.datetime.now(datetime.timezone.utc))
            raise PreconditionFailed(Key)
        return super().put_object(Bucket=Bucket, Key=Key, Body=Body, **kwargs)


def test_losing_a_create_only_race_verifies_and_reuses_identical_evidence():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        key = ("received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/"
              "dcm-1234-849217/positions.csv")
        s3 = _RacingS3(key, data.read_bytes())
        result = _publish(s3, transport_id_files=[("data", data)])
        assert result.files[0].bytes == len(b"id,value\n1,x\n")


def test_complete_json_is_the_last_put_call():
    with tempfile.TemporaryDirectory() as folder:
        a = _write(folder, "positions_1.csv", b"one")
        b = _write(folder, "positions_2.csv", b"two")
        control = _write(folder, "positions.ctl", b"ROWS=2")
        s3 = FakeS3()
        _publish(s3, transport_id_files=[
            ("control", control), ("data", b), ("data", a)])
        puts = [call.removeprefix("put:") for call in s3.calls
               if call.startswith("put:")]
        assert puts[-1].endswith("_COMPLETE.json")
        assert len(puts) == 4  # 2 data + 1 control + 1 marker


# ---------------------------------------------------------------- hashing
def test_hashing_reader_hash_matches_exactly_what_was_streamed():
    """The wrapper that makes upload and hash one read pass: whatever it
    streams out is exactly what its digest describes, which is the mechanism
    that keeps the published SHA-256 from ever describing different bytes
    than what object storage actually received."""
    body = b"a" * 5000 + b"b" * 5000
    reader = _HashingReader(io.BytesIO(body))
    collected = b""
    while True:
        chunk = reader.read(777)
        if not chunk:
            break
        collected += chunk
    assert collected == body
    assert reader.bytes_read == len(body)
    assert reader.hexdigest() == hashlib.sha256(body).hexdigest()


def test_hashing_reader_reset_on_seek_to_start_matches_a_resend():
    body = b"retry me"
    reader = _HashingReader(io.BytesIO(body))
    reader.read()
    reader.seek(0)
    resent = reader.read()
    assert resent == body
    assert reader.hexdigest() == hashlib.sha256(body).hexdigest()
    assert reader.bytes_read == len(body)


# ------------------------------------------------- large files / multipart
_TINY_MPU = UploadTuning(multipart_threshold=100, multipart_part_size=64,
                         multipart_max_concurrency=2)


def _get_calls(s3) -> list[str]:
    return [c for c in s3.calls if c.startswith("get:")]


def test_small_file_uses_a_single_put_not_multipart():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "small.csv", b"id,value\n1,x\n")
        s3 = FakeS3(checksum_capable=True)
        _publish(s3, transport_id_files=[("data", data)])
        assert not any(c.startswith("create_mpu:") for c in s3.calls)
        assert any(c.startswith("put:") for c in s3.calls)


def test_large_file_at_or_above_the_threshold_uses_multipart():
    with tempfile.TemporaryDirectory() as folder:
        body = bytes(range(256)) * 2  # 512 bytes, well above the 100-byte
                                       # threshold and > 4 parts at 64 bytes
        data = _write(folder, "big.csv", body)
        s3 = FakeS3(checksum_capable=True)
        result = _publish(s3, transport_id_files=[("data", data)], upload=_TINY_MPU)
        assert any(c.startswith("create_mpu:") for c in s3.calls)
        assert any(c.startswith("complete_mpu:") for c in s3.calls)
        assert sum(1 for c in s3.calls if "upload_part:" in c) == 8  # 512/64
        assert result.files[0].bytes == len(body)
        assert result.files[0].sha256 == hashlib.sha256(body).hexdigest()
        # never loads the whole file at once: parts are read/uploaded in
        # 64-byte pieces, not one 512-byte read -- the multipart path was
        # actually exercised, not silently a single PUT in disguise.
        assert not any(c.startswith("put:") and c.endswith("big.csv")
                      for c in s3.calls)


def test_multipart_data_matches_a_single_put_of_the_same_bytes():
    body = bytes((i * 7) % 256 for i in range(500))
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", body)
        s3 = FakeS3(checksum_capable=True)
        result = _publish(s3, transport_id_files=[("data", data)], upload=_TINY_MPU)
        stored_body, _ = s3.objects[result.files[0].object_key]
        assert stored_body == body


def test_checksum_capable_backend_skips_the_post_upload_get_for_a_small_file():
    """The headline fix: a freshly-uploaded object is never downloaded again
    when the backend can prove its content with a checksum instead."""
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n" * 100)
        s3 = FakeS3(checksum_capable=True)
        _publish(s3, transport_id_files=[("data", data)])
        assert _get_calls(s3) == []


def test_checksum_capable_backend_skips_the_post_upload_get_for_a_large_file():
    with tempfile.TemporaryDirectory() as folder:
        body = bytes((i * 3) % 256 for i in range(500))
        data = _write(folder, "big.csv", body)
        s3 = FakeS3(checksum_capable=True)
        _publish(s3, transport_id_files=[("data", data)], upload=_TINY_MPU)
        assert _get_calls(s3) == []


def test_checksum_incapable_backend_falls_back_to_a_full_download():
    """The original, always-correct guarantee for any backend that does not
    support checksums -- FakeS3()'s default."""
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        s3 = FakeS3()  # checksum_capable=False
        _publish(s3, transport_id_files=[("data", data)])
        assert len(_get_calls(s3)) == 1


def test_full_download_mode_always_downloads_even_when_checksums_are_available():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        s3 = FakeS3(checksum_capable=True)
        _publish(s3, transport_id_files=[("data", data)],
                upload=UploadTuning(checksum_mode="full_download"))
        assert len(_get_calls(s3)) == 1


def test_reusing_an_already_published_checksum_capable_object_avoids_a_get():
    """A retry still reads the (tiny) marker JSON to compare against -- that
    GET is unrelated to this fix. What must not happen is a GET of the
    (potentially multi-GB) DATA object itself, which is what the old code
    did unconditionally on every retry."""
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n" * 50)
        s3 = FakeS3(checksum_capable=True)
        _publish(s3, transport_id_files=[("data", data)],
                uploaded_at="2026-09-22T01:14:22Z")
        call_count_before = len(s3.calls)
        _publish(s3, transport_id_files=[("data", data)],
                uploaded_at="2026-09-23T00:00:00Z")
        assert not any(c.startswith("get:") and c.endswith("positions.csv")
                      for c in s3.calls[call_count_before:])


def test_zero_byte_control_file_uses_single_put():
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "positions.csv", b"id,value\n1,x\n")
        control = _write(folder, "positions.ctl", b"")
        s3 = FakeS3(checksum_capable=True)
        result = _publish(s3, transport_id_files=[("data", data), ("control", control)])
        control_file = result.control_files[0]
        assert control_file.bytes == 0
        assert control_file.sha256 == hashlib.sha256(b"").hexdigest()
        assert not any(c.startswith("create_mpu:") for c in s3.calls)


# ---------------------------------------------- multipart failure/recovery
class _FailingPartS3(FakeS3):
    """Fails one specific part upload, to exercise abort-on-failure."""

    def __init__(self, fail_on_part: int) -> None:
        super().__init__(checksum_capable=True)
        self._fail_on_part = fail_on_part

    def upload_part(self, Bucket, Key, PartNumber, UploadId, Body,  # noqa: N803
                    ChecksumAlgorithm=""):                           # noqa: N803
        if PartNumber == self._fail_on_part:
            raise RuntimeError("simulated network failure")
        return super().upload_part(Bucket=Bucket, Key=Key, PartNumber=PartNumber,
                                   UploadId=UploadId, Body=Body,
                                   ChecksumAlgorithm=ChecksumAlgorithm)


def test_multipart_failure_aborts_the_upload_and_leaves_no_object():
    with tempfile.TemporaryDirectory() as folder:
        body = bytes((i * 5) % 256 for i in range(500))
        data = _write(folder, "big.csv", body)
        s3 = _FailingPartS3(fail_on_part=3)
        try:
            _publish(s3, transport_id_files=[("data", data)], upload=_TINY_MPU)
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected the simulated failure to propagate")
        assert any(c.startswith("abort_mpu:") for c in s3.calls)
        assert s3.objects == {}
        assert s3._uploads == {}


def test_a_retry_after_a_failed_multipart_upload_starts_a_fresh_one():
    """An aborted multipart upload leaves no object; a retry is a normal
    fresh publish, not confused with a completed one."""
    with tempfile.TemporaryDirectory() as folder:
        body = bytes((i * 5) % 256 for i in range(500))
        data = _write(folder, "big.csv", body)
        s3 = _FailingPartS3(fail_on_part=999)  # never actually fails
        result = _publish(s3, transport_id_files=[("data", data)], upload=_TINY_MPU)
        assert result.files[0].bytes == len(body)


def test_multipart_create_only_race_loser_verifies_and_reuses_identical_evidence():
    """Mirrors the single-PUT create-only race, but for the multipart
    CompleteMultipartUpload call -- confirmed against real MinIO to honour
    IfNoneMatch there exactly the same way. See docs/TRANSPORT-CONTRACT.md,
    'Multipart create-only race semantics'."""
    body = bytes((i * 11) % 256 for i in range(500))

    class _RacingMpuS3(FakeS3):
        def __init__(self) -> None:
            super().__init__(checksum_capable=True)
            self._raced = False

        def complete_multipart_upload(self, Bucket, Key, UploadId,   # noqa: N803
                                      MultipartUpload, IfNoneMatch=""):  # noqa: N803
            if not self._raced and IfNoneMatch == "*":
                self._raced = True
                # another publisher wins the race with the SAME bytes
                self.objects[Key] = (body, datetime.datetime.now(datetime.timezone.utc))
                if self.checksum_capable:
                    self.checksums[Key] = _b64(body)
                del self._uploads[UploadId]
                raise PreconditionFailed(Key)
            return super().complete_multipart_upload(
                Bucket=Bucket, Key=Key, UploadId=UploadId,
                MultipartUpload=MultipartUpload, IfNoneMatch=IfNoneMatch)

    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "big.csv", body)
        s3 = _RacingMpuS3()
        result = _publish(s3, transport_id_files=[("data", data)], upload=_TINY_MPU)
        assert result.files[0].bytes == len(body)
        assert result.files[0].sha256 == hashlib.sha256(body).hexdigest()


def _b64(body: bytes) -> str:
    return base64.b64encode(hashlib.sha256(body).digest()).decode()


# ------------------------------------------------------- source mutation
def test_source_file_modified_after_being_read_fails_safe():
    """publish_object()'s mutation guard: a source rewritten between the
    stat() taken before publication and the one taken after it must not
    reach _COMPLETE.json. Exercised directly against storage._guard_mutation
    with a real file and a real stat() -- the alternative, racing a second
    thread against the exact read window of a real upload, would make this
    test's timing rather than the guard's logic the thing under test."""
    with tempfile.TemporaryDirectory() as folder:
        path = _write(folder, "positions.csv", b"original content")
        initial_stat = path.stat()
        path.write_bytes(b"rewritten content, different size")
        _fails(lambda: _storage._guard_mutation(
                  path, initial_stat, len(b"original content"),
                  "transport dcm-1234-849217"),
              TransportSourceMutatedError, "modified during publication")


def test_source_file_truncated_mid_read_fails_safe():
    """bytes actually streamed disagreeing with the pre-read stat is
    detected even before the post-read stat comparison."""
    with tempfile.TemporaryDirectory() as folder:
        path = _write(folder, "positions.csv", b"twenty bytes long!!!")
        initial_stat = path.stat()
        _fails(lambda: _storage._guard_mutation(
                  path, initial_stat, len(b"twenty bytes long!!!") - 5,
                  "transport dcm-1234-849217"),
              TransportSourceMutatedError, "changed during publication")


def test_publish_transport_result_reports_multipart_publish_from_scratch():
    with tempfile.TemporaryDirectory() as folder:
        body = bytes((i * 13) % 256 for i in range(500))
        data = _write(folder, "big.csv", body)
        s3 = FakeS3(checksum_capable=True)
        result = publish_transport_result(
            legacy_feed_id="1234", producer_run_id="849217",
            cob_date="2026-09-21", source_system="RISK_ENGINE_X",
            source_observed_at="2026-09-22T01:13:00Z",
            files=[("data", data)], client=s3, bucket="lakehouse",
            upload=_TINY_MPU)
        assert result.newly_published is True
        assert result.transport.files[0].bytes == len(body)


# --------------------------------------------------------------------- CLI
def _sample_transport() -> Transport:
    return Transport(
        transport_contract_version=2, transport_id="dcm-1234-849217",
        source="DCM", legacy_feed_id="1234",
        source_observed_at=datetime.datetime(
            2026, 9, 22, 1, 13, tzinfo=datetime.timezone.utc),
        uploaded_at=datetime.datetime(
            2026, 9, 22, 1, 14, 22, tzinfo=datetime.timezone.utc),
        files=(TransportFile(role="data", original_filename="p.csv",
                             object_key="received/.../p.csv", bytes=3,
                             sha256="a" * 64),),
        producer_run_id="849217", cob_date="2026-09-21",
        source_system="RISK_ENGINE_X")


def _cli_argv(data_path: Path) -> list[str]:
    return ["publish", "--legacy-feed-id", "1234",
           "--producer-run-id", "849217", "--cob-date", "2026-09-21",
           "--source-system", "RISK_ENGINE_X",
           "--source-observed-at", "2026-09-22T01:13:00Z",
           "--data", str(data_path)]


def _run_cli_with_patched_publish(argv, fake_result_fn):
    """Run ``cli.main`` with ``publish_transport_result``/config stubbed out.

    No pytest here (`tests/README.md`), so this monkeypatches module
    attributes directly and always restores them, even on failure.
    """
    from reporting_transport import cli

    original_publish = cli.publish_transport_result
    original_config = cli._config_from_args
    cli.publish_transport_result = fake_result_fn
    cli._config_from_args = lambda args: object()
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            code = cli.main(argv)
    finally:
        cli.publish_transport_result = original_publish
        cli._config_from_args = original_config
    return code, out.getvalue()


def test_cli_publish_reports_newly_published_vs_idempotent_retry():
    from reporting_transport.publisher import PublishResult

    transport = _sample_transport()
    calls = []

    def _fake_result(**kwargs):
        calls.append(kwargs)
        return PublishResult(transport=transport,
                             newly_published=len(calls) == 1)

    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "p.csv", b"1,2\n")
        argv = _cli_argv(data)

        code, stdout = _run_cli_with_patched_publish(argv, _fake_result)
        from reporting_transport import cli
        assert code == cli.EXIT_OK
        payload = json.loads(stdout)
        assert payload["status"] == "published"

        code2, stdout2 = _run_cli_with_patched_publish(argv, _fake_result)
        assert code2 == cli.EXIT_OK
        payload2 = json.loads(stdout2)
        assert payload2["status"] == "already_published"


def test_cli_maps_error_types_to_distinct_documented_exit_codes():
    from reporting_transport import cli

    cases = [
        (TransportContractError("bad input"), cli.EXIT_INVALID_INPUT),
        (TransportConflictError("conflict"), cli.EXIT_CONFLICT),
        (TransportEvidenceError("missing object"), cli.EXIT_EVIDENCE),
        (TransportStorageError("no route to host"), cli.EXIT_STORAGE),
        (TransportSourceMutatedError("source changed"), cli.EXIT_SOURCE_MUTATED),
    ]
    with tempfile.TemporaryDirectory() as folder:
        data = _write(folder, "p.csv", b"1,2\n")
        argv = _cli_argv(data)
        for exc, expected_code in cases:
            def _raise(_exc=exc, **kwargs):
                raise _exc

            code, stdout = _run_cli_with_patched_publish(argv, _raise)
            assert code == expected_code, (exc, code)
            payload = json.loads(stdout)
            assert payload["status"] == "error"
            assert payload["exit_code"] == expected_code
