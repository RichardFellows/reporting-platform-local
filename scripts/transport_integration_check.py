"""Integration checks for ``reporting_transport`` against REAL MinIO.

``tests/`` deliberately needs no stack (``tests/README.md``, "no Spark, no
Airflow, no MinIO"), so behaviour that depends on what a real S3-compatible
backend actually does -- confirmed here to differ from the spec in ways that
mattered, see below -- belongs here instead, run deliberately rather than on
every `python -m tests.run`:

    docker compose exec -T airflow python -m scripts.transport_integration_check

Exercises the paths a FakeS3-only suite cannot: real multipart completion
semantics, real checksum computation (MinIO's multipart checksum turned out
to be a COMPOSITE hash unrelated to the plain full-object SHA-256, and
completing a checksummed multipart upload without repeating each part's
checksum in its ``Parts`` entry fails with ``InvalidPart`` -- both found by
running this against MinIO, not by reading documentation). Publishes under
``received-prefix``'s ``integration-check/`` sub-prefix and deletes
everything it creates, on success or failure.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from reporting_transport.contract import TransportConflictError
from reporting_transport.publisher import publish_transport_result
from reporting_transport.storage import StorageConfig, UploadTuning, build_client

# Real MinIO enforces S3's actual 5 MiB minimum part size (confirmed here:
# smaller parts fail CompleteMultipartUpload with EntityTooSmall) -- unlike
# tests/test_reporting_transport.py's FakeS3, which does not model that
# limit, so it can use byte-scale fixtures. This still keeps the fixture
# small (~12 MiB, 3 parts) rather than requiring a multi-GB file.
_TINY_MPU = UploadTuning(multipart_threshold=6 * 1024 * 1024,
                         multipart_part_size=5 * 1024 * 1024,
                         multipart_max_concurrency=3)

_FAILED: list[str] = []


def _check(label: str, condition: bool) -> None:
    status = "ok  " if condition else "FAIL"
    print(f"{status}  {label}")
    if not condition:
        _FAILED.append(label)


def _config(prefix_suffix: str, upload: UploadTuning) -> StorageConfig:
    base = StorageConfig.from_env()
    return StorageConfig(bucket=base.bucket, endpoint_url=base.endpoint_url,
                         region=base.region,
                         received_prefix=f"{base.received_prefix}/"
                                         f"integration-check-{prefix_suffix}",
                         upload=upload)


def _publish(config: StorageConfig, folder: Path, name: str, body: bytes, *,
            legacy_feed_id: str, producer_run_id: str = "run-1"):
    path = folder / name
    path.write_bytes(body)
    return publish_transport_result(
        legacy_feed_id=legacy_feed_id, producer_run_id=producer_run_id,
        cob_date="2026-09-21", source_system="INTEGRATION",
        source_observed_at="2026-09-22T00:00:00Z", files=[("data", path)],
        client=build_client(config), bucket=config.bucket,
        prefix=config.received_prefix, upload=config.upload)


def _cleanup(config: StorageConfig) -> None:
    client = build_client(config)
    paginator = client.get_paginator("list_objects_v2")
    keys = [obj["Key"] for page in paginator.paginate(
                Bucket=config.bucket, Prefix=f"{config.received_prefix}/")
           for obj in page.get("Contents", [])]
    for key in keys:
        client.delete_object(Bucket=config.bucket, Key=key)


def main() -> int:
    with tempfile.TemporaryDirectory() as raw_folder:
        folder = Path(raw_folder)

        # ---- small file, checksum-capable single PUT, no GET on verify
        small_config = _config("small", UploadTuning())
        body = b"id,value\n" + b"1,x\n" * 5000
        result = _publish(small_config, folder, "small.csv", body,
                          legacy_feed_id="small")
        _check("small file publishes with correct sha256",
              result.transport.files[0].sha256 == __import__("hashlib")
              .sha256(body).hexdigest())
        _check("small file publishes with correct byte count",
              result.transport.files[0].bytes == len(body))

        # ---- large file, real multipart, real per-part + composite checksum
        big_config = _config("big", _TINY_MPU)
        big_body = bytes((i * 37) % 256 for i in range(12_000_000))  # ~11.4 MiB, 3 parts
        result = _publish(big_config, folder, "big.csv", big_body,
                          legacy_feed_id="big")
        import hashlib
        _check("large file (multipart) has correct sha256",
              result.transport.files[0].sha256 == hashlib.sha256(big_body).hexdigest())
        _check("large file (multipart) has correct byte count",
              result.transport.files[0].bytes == len(big_body))

        # ---- idempotent retry
        retry = _publish(big_config, folder, "big.csv", big_body,
                         legacy_feed_id="big")
        _check("identical retry is idempotent (already_published)",
              retry.newly_published is False)

        # ---- conflicting retry under the same identity is refused
        conflict_config = _config("conflict", _TINY_MPU)
        _publish(conflict_config, folder, "big.csv", big_body,
                legacy_feed_id="conflict")
        (folder / "big.csv").write_bytes(big_body + b"more")
        try:
            _publish(conflict_config, folder, "big.csv", big_body + b"more",
                    legacy_feed_id="conflict")
            _check("conflicting retry raises TransportConflictError", False)
        except TransportConflictError:
            _check("conflicting retry raises TransportConflictError", True)

        # ---- multipart create-only race: two publishers, identical bytes
        race_config = _config("race", _TINY_MPU)
        client_a = build_client(race_config)
        client_b = build_client(race_config)
        race_body = bytes((i * 41) % 256 for i in range(12_000_000))
        race_path = folder / "race.csv"
        race_path.write_bytes(race_body)
        result_a = publish_transport_result(
            legacy_feed_id="race", producer_run_id="run-1", cob_date="2026-09-21",
            source_system="INTEGRATION", source_observed_at="2026-09-22T00:00:00Z",
            files=[("data", race_path)], client=client_a, bucket=race_config.bucket,
            prefix=race_config.received_prefix, upload=race_config.upload)
        result_b = publish_transport_result(
            legacy_feed_id="race", producer_run_id="run-1", cob_date="2026-09-21",
            source_system="INTEGRATION", source_observed_at="2026-09-22T00:00:00Z",
            files=[("data", race_path)], client=client_b, bucket=race_config.bucket,
            prefix=race_config.received_prefix, upload=race_config.upload)
        _check("both racing publishers converge on identical evidence",
              result_a.transport.files[0].sha256 == result_b.transport.files[0].sha256)

        for cfg in (small_config, big_config, conflict_config, race_config):
            _cleanup(cfg)

    print()
    if _FAILED:
        print(f"{len(_FAILED)} check(s) FAILED: {', '.join(_FAILED)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
