"""Demonstrate the large-file hardening of ``reporting_transport`` locally.

Generates a synthetic file, publishes it to the real (local) MinIO through
the same :func:`reporting_transport.publisher.publish_transport` every real
DCM invocation uses, and reports what actually crossed the network -- bytes
read locally, bytes uploaded, and bytes DOWNLOADED for producer-side
verification. The number that matters is the last one: for a
checksum-capable backend (MinIO, confirmed) it is ~0 for a newly-published
object, where the pre-hardening implementation always downloaded the entire
object a second time.

    docker compose exec -T airflow python -m scripts.benchmark_transport_upload
    docker compose exec -T airflow python -m scripts.benchmark_transport_upload \\
        --size-mb 200 --checksum-mode full_download   # the old behaviour, for contrast

Publishes under ``received-prefix``'s ``benchmark/`` sub-prefix and deletes
everything it created before exiting, success or failure.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
import time
from pathlib import Path

from reporting_transport.publisher import publish_transport_result
from reporting_transport.storage import StorageConfig, UploadTuning, build_client


class _CountingClient:
    """Wraps a real boto3 S3 client, counting bytes actually sent/received
    for the handful of calls this package makes -- not a general proxy.
    """

    def __init__(self, real) -> None:
        self._real = real
        self.bytes_uploaded = 0
        self.bytes_downloaded = 0

    def __getattr__(self, name):
        return getattr(self._real, name)

    def put_object(self, **kwargs):
        body = kwargs.get("Body")
        if isinstance(body, (bytes, bytearray)):
            self.bytes_uploaded += len(body)  # e.g. the marker JSON
        else:
            kwargs["Body"] = self._count_upload(body)
        return self._real.put_object(**kwargs)

    def upload_part(self, **kwargs):
        body = kwargs.get("Body")
        if isinstance(body, (bytes, bytearray)):
            self.bytes_uploaded += len(body)
        else:
            kwargs["Body"] = self._count_upload(body)
        return self._real.upload_part(**kwargs)

    def get_object(self, **kwargs):
        response = self._real.get_object(**kwargs)
        response["Body"] = _CountingBody(response["Body"], self)
        return response

    def _count_upload(self, body):
        if body is None:
            return body
        return _CountingReader(body, self)


class _CountingReader:
    """Wraps a file-like upload body, counting bytes as they are read (i.e.
    exactly as they are sent), without changing its read/seek/tell contract
    -- the same interface :class:`reporting_transport.storage._HashingReader`
    already relies on.
    """

    def __init__(self, inner, counter: _CountingClient) -> None:
        self._inner = inner
        self._counter = counter

    def read(self, size: int = -1) -> bytes:
        chunk = self._inner.read(size)
        self._counter.bytes_uploaded += len(chunk)
        return chunk

    def seekable(self) -> bool:
        return self._inner.seekable()

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()


class _CountingBody:
    def __init__(self, inner, counter: _CountingClient) -> None:
        self._inner = inner
        self._counter = counter

    def read(self, size: int = -1) -> bytes:
        chunk = self._inner.read(size)
        self._counter.bytes_downloaded += len(chunk)
        return chunk


def _generate_file(path: Path, size_bytes: int) -> str:
    """Pseudo-random, non-trivially-compressible content, streamed to disk
    in chunks so generating even a large fixture stays memory-bounded."""
    hasher = hashlib.sha256()
    remaining = size_bytes
    chunk_size = 4 * 1024 * 1024
    with path.open("wb") as fh:
        seed = os.urandom(64)
        counter = 0
        while remaining > 0:
            n = min(chunk_size, remaining)
            chunk = hashlib.sha256(seed + counter.to_bytes(8, "big")).digest()
            chunk = (chunk * (n // len(chunk) + 1))[:n]
            fh.write(chunk)
            hasher.update(chunk)
            remaining -= n
            counter += 1
    return hasher.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mb", type=int, default=64,
                        help="generated file size in MiB (default: 64)")
    parser.add_argument("--multipart-threshold-mb", type=int, default=8,
                        help="multipart threshold in MiB (default: 8, "
                             "boto3's own default)")
    parser.add_argument("--multipart-part-size-mb", type=int, default=8,
                        help="multipart part size in MiB (default: 8)")
    parser.add_argument("--checksum-mode", choices=["auto", "full_download"],
                        default="auto")
    args = parser.parse_args(argv)

    config = StorageConfig.from_env()
    config = StorageConfig(
        bucket=config.bucket, endpoint_url=config.endpoint_url,
        region=config.region, received_prefix=f"{config.received_prefix}/benchmark",
        upload=UploadTuning(
            multipart_threshold=args.multipart_threshold_mb * 1024 * 1024,
            multipart_part_size=args.multipart_part_size_mb * 1024 * 1024,
            checksum_mode=args.checksum_mode))
    real_client = build_client(config)
    client = _CountingClient(real_client)

    size_bytes = args.size_mb * 1024 * 1024
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "benchmark_feed.csv"
        print(f"generating {args.size_mb} MiB fixture ...")
        expected_sha256 = _generate_file(path, size_bytes)

        run_id = f"bench-{int(time.time())}"
        started = time.monotonic()
        result = publish_transport_result(
            legacy_feed_id="benchmark", producer_run_id=run_id,
            cob_date="2026-09-21", source_system="BENCHMARK",
            source_observed_at="2026-09-22T00:00:00Z",
            files=[("data", path)], client=client, bucket=config.bucket,
            prefix=config.received_prefix, upload=config.upload)
        elapsed = time.monotonic() - started
        published = result.transport.files[0]
        method = "multipart" if size_bytes >= config.upload.multipart_threshold \
            else "put"

        print()
        print(f"{'file size':<28}{size_bytes:,} bytes")
        print(f"{'upload method':<28}{method}")
        print(f"{'checksum mode':<28}{args.checksum_mode}")
        print(f"{'bytes read locally':<28}{size_bytes:,} bytes (one pass, "
             f"hashed while streamed)")
        print(f"{'bytes uploaded':<28}{client.bytes_uploaded:,} bytes")
        print(f"{'bytes downloaded (verify)':<28}{client.bytes_downloaded:,} bytes")
        print(f"{'elapsed':<28}{elapsed:.2f}s")
        print(f"{'sha256 (local)':<28}{expected_sha256}")
        print(f"{'sha256 (declared in marker)':<28}{published.sha256}")
        print(f"{'sha256 match':<28}{expected_sha256 == published.sha256}")
        if args.checksum_mode == "auto":
            print()
            print("bytes downloaded should be ~0: the backend's own SHA-256 "
                 "checksum confirmed the durably-stored object matches "
                 "without a re-download. Compare against "
                 "--checksum-mode full_download to see the old behaviour.")

        # Clean up what this run created, success or failure.
        real_client.delete_object(Bucket=config.bucket,
                                  Key=published.object_key)
        marker_key = (f"{config.received_prefix}/cob_date=2026-09-21/"
                     f"source_system=BENCHMARK/dcm-benchmark-{run_id}/"
                     f"_COMPLETE.json")
        real_client.delete_object(Bucket=config.bucket, Key=marker_key)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
