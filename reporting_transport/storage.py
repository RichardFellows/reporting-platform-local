"""Explicit, reusable S3-compatible storage configuration and evidence I/O.

Deliberately does not import anything from ``reporting_platform``: a real DCM
process should be able to install just this package plus ``boto3`` and run
the CLI. Credentials are never accepted as configuration here -- only
endpoint/bucket/region/prefix, which is what varies between local MinIO and an
enterprise S3-compatible target. Authentication is left entirely to the
standard AWS/boto3 credential-provider chain (environment variables, shared
credentials/config files, an EC2/ECS/pod IAM role, ...), so nothing in this
module -- or the CLI built on it -- ever accepts a secret as an argument.

Large-file upload strategy (see ``docs/TRANSPORT-CONTRACT.md``, "Large-file
upload and verification strategy", for the full writeup and the trust
boundary it documents):

    publish_object()
            |
      object already exists at this key?
            |                         |
           yes                        no
            |                         |
      hash it locally once     stat it, then:
      (one read, unavoidable:  bytes < multipart_threshold -> single PUT
      it is the only way to        else                    -> multipart
      know whether a retry              |
      is identical or                   v
      conflicting)               upload, hashing exactly what is
            |                    streamed (never a second local read)
            +---------------+---------------+
                             |
                    confirm durable storage matches the declared
                    evidence -- via a server-computed SHA-256
                    checksum when the backend supports one and it
                    is usable (never for a composite/multipart
                    checksum against a PRE-EXISTING object whose
                    part boundaries are unknown), falling back to
                    a full GET + rehash otherwise. Every mismatch,
                    on either path, raises TransportEvidenceError.

A single confirmation pass per file, always run, always either cheap-and-
sufficient or the original full download -- never both, and never skipped.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import logging
import os
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from reporting_transport.contract import (
    TransportContractError, TransportEvidenceError, TransportSourceMutatedError,
    validate_prefix,
)

logger = logging.getLogger(__name__)

_CHUNK = 1024 * 1024

# ---------------------------------------------------------- S3 API constants
# Protocol limits, not configuration -- these are what boto3/S3 actually
# enforce (and MinIO follows the same numbers), not a local-MinIO assumption.
# See "Very large object limits" in docs/TRANSPORT-CONTRACT.md.
S3_MIN_MULTIPART_PART_SIZE = 5 * 1024 * 1024             # 5 MiB, all but last part
S3_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024                # 5 GiB per part
S3_MAX_PART_COUNT = 10_000
S3_MAX_SINGLE_PUT_SIZE = 5 * 1024 * 1024 * 1024          # 5 GiB, single PUT ceiling
S3_MAX_OBJECT_SIZE = 5 * 1024 * 1024 * 1024 * 1024       # 5 TiB, absolute ceiling

# boto3's own s3transfer TransferConfig defaults -- "based on boto3/S3
# conventions", per the hardening brief, not a number picked for local MinIO.
DEFAULT_MULTIPART_THRESHOLD = 8 * 1024 * 1024            # 8 MiB
DEFAULT_MULTIPART_PART_SIZE = 8 * 1024 * 1024             # 8 MiB
DEFAULT_MULTIPART_MAX_CONCURRENCY = 4

CHECKSUM_MODES = frozenset({"auto", "full_download"})

_COMPOSITE_CHECKSUM = re.compile(r"-\d+$")


@dataclass(frozen=True)
class UploadTuning:
    """Configuration for HOW a file is uploaded -- independent of WHERE
    (bucket/endpoint/region), so it can be resolved even when a caller
    supplies its own ``client``/``bucket`` and never builds a
    :class:`StorageConfig` at all (every test in this repo does exactly
    that). See ``publisher.publish_transport_result``.
    """

    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD
    multipart_part_size: int = DEFAULT_MULTIPART_PART_SIZE
    multipart_max_concurrency: int = DEFAULT_MULTIPART_MAX_CONCURRENCY
    checksum_mode: str = "auto"

    def __post_init__(self) -> None:
        # NOT enforced here: real S3/MinIO requires every part but the last
        # to be >= S3_MIN_MULTIPART_PART_SIZE, but only at
        # CompleteMultipartUpload time, against the actual part boundaries
        # of a real upload -- duplicating it as a config-time floor would
        # make it impossible to exercise the multipart path in tests against
        # small fixtures with a low threshold, which is the whole point of
        # it being configurable. A part size too small for the file actually
        # being published surfaces as a real error from the backend itself.
        if self.multipart_part_size < 1:
            raise TransportContractError("multipart_part_size must be positive")
        if self.multipart_part_size > S3_MAX_PART_SIZE:
            raise TransportContractError(
                f"multipart_part_size {self.multipart_part_size} exceeds "
                f"the S3 maximum of {S3_MAX_PART_SIZE} bytes per part")
        if self.multipart_threshold < 0:
            raise TransportContractError("multipart_threshold must not be negative")
        if self.multipart_threshold > S3_MAX_SINGLE_PUT_SIZE:
            raise TransportContractError(
                f"multipart_threshold {self.multipart_threshold} exceeds "
                f"the {S3_MAX_SINGLE_PUT_SIZE}-byte single-PUT limit -- a "
                f"file at or above that size would never take the "
                f"multipart path")
        if self.multipart_max_concurrency < 1:
            raise TransportContractError("multipart_max_concurrency must be >= 1")
        if self.checksum_mode not in CHECKSUM_MODES:
            raise TransportContractError(
                f"checksum_mode {self.checksum_mode!r} must be one of "
                f"{sorted(CHECKSUM_MODES)}")

    @classmethod
    def from_env(cls) -> "UploadTuning":
        return cls(
            multipart_threshold=_int_env(
                "REPORTING_TRANSPORT_MULTIPART_THRESHOLD_BYTES",
                DEFAULT_MULTIPART_THRESHOLD),
            multipart_part_size=_int_env(
                "REPORTING_TRANSPORT_MULTIPART_PART_SIZE_BYTES",
                DEFAULT_MULTIPART_PART_SIZE),
            multipart_max_concurrency=_int_env(
                "REPORTING_TRANSPORT_MULTIPART_MAX_CONCURRENCY",
                DEFAULT_MULTIPART_MAX_CONCURRENCY),
            checksum_mode=os.environ.get(
                "REPORTING_TRANSPORT_CHECKSUM_MODE", "auto"))


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise TransportContractError(
            f"{name}={raw!r} is not an integer") from exc


@dataclass(frozen=True)
class StorageConfig:
    bucket: str
    endpoint_url: str | None = None
    region: str | None = None
    received_prefix: str = "received"
    upload: UploadTuning = field(default_factory=UploadTuning)

    def __post_init__(self) -> None:
        if not self.bucket or not isinstance(self.bucket, str):
            raise TransportContractError("storage bucket must not be empty")
        validate_prefix(self.received_prefix)

    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Resolve config from ``REPORTING_TRANSPORT_*``.

        Falls back to the same variable names ``docker-compose.yml`` already
        sets for the rest of this platform (``S3_ENDPOINT``,
        ``REPORTING_WAREHOUSE``, ``AWS_REGION``, ``REPORTING_RECEIVED_PREFIX``)
        so the local simulator needs no new configuration. A standalone DCM
        deployment that has none of those should set the ``REPORTING_
        TRANSPORT_*`` names explicitly -- see docs/TRANSPORT-CONTRACT.md.
        """
        values = cls._env_values()
        if not values["bucket"]:
            raise TransportContractError(
                "no bucket configured: set REPORTING_TRANSPORT_BUCKET (or, "
                "for local RPL development, REPORTING_WAREHOUSE)")
        return cls(bucket=values["bucket"], endpoint_url=values["endpoint_url"],
                   region=values["region"],
                   received_prefix=values["received_prefix"],
                   upload=UploadTuning.from_env())

    @staticmethod
    def _env_values() -> dict[str, str | None]:
        """The same environment resolution as :meth:`from_env`, without
        requiring a bucket to be present -- used by the CLI, which applies
        ``--bucket``/etc. overrides before a missing value is an error."""
        return {
            "bucket": (os.environ.get("REPORTING_TRANSPORT_BUCKET")
                      or _bucket_from_warehouse(
                          os.environ.get("REPORTING_WAREHOUSE"))),
            "endpoint_url": (os.environ.get("REPORTING_TRANSPORT_S3_ENDPOINT")
                            or os.environ.get("S3_ENDPOINT")),
            "region": (os.environ.get("REPORTING_TRANSPORT_REGION")
                      or os.environ.get("AWS_REGION")),
            "received_prefix": (
                os.environ.get("REPORTING_TRANSPORT_RECEIVED_PREFIX")
                or os.environ.get("REPORTING_RECEIVED_PREFIX", "received")),
        }


def _bucket_from_warehouse(warehouse: str | None) -> str | None:
    if not warehouse:
        return None
    return warehouse.replace("s3a://", "").replace("s3://", "").split("/")[0]


def build_client(config: StorageConfig):
    """A boto3 S3 client using the standard credential-provider chain.

    No ``aws_access_key_id``/``aws_secret_access_key`` are ever passed here --
    that is the whole point of taking a ``StorageConfig`` rather than a
    credentials bag as this module's public surface.

    Pins botocore's OWN, independent default checksum behaviour to
    ``when_required`` rather than its default ``when_supported``. Since
    botocore 1.36 (January 2025), every ``PutObject``/``UploadPart`` request
    carries an automatic CRC32 (or CRC64NVME) checksum trailer *regardless*
    of whether this package's own ``UploadTuning.checksum_mode`` ever asks
    for one -- confirmed (Apache Iceberg's own S3FileIO hardening,
    `apache/iceberg#17177`) to make Dell ECS specifically reject the write
    outright, not degrade gracefully. ``when_required`` only suppresses
    botocore's UNREQUESTED default; it still honours this package's own
    explicit ``ChecksumAlgorithm="SHA256"`` on the calls that ask for one, so
    ``checksum_mode="auto"`` keeps working exactly as designed. Silently
    left unset on a botocore old enough not to recognise these ``Config``
    keys at all (a real standalone DCM host is not guaranteed a recent
    boto3) -- one that old predates the default-on behaviour this exists to
    suppress, so there is nothing to protect against there.
    """
    import boto3
    from botocore.config import Config

    try:
        client_config = Config(request_checksum_calculation="when_required",
                               response_checksum_validation="when_required")
    except TypeError:
        client_config = None

    return boto3.client(
        "s3", endpoint_url=config.endpoint_url, region_name=config.region,
        config=client_config)


# ------------------------------------------------------------- evidence I/O
@dataclass(frozen=True)
class PublishedObject:
    """The outcome of one :func:`publish_object` call.

    ``storage_checksum_verified`` is the trust-boundary flag: ``True`` means
    the durable-storage confirmation was a server-computed SHA-256 checksum
    (no GET); ``False`` means it was the original full download + rehash --
    always correct, only ever slower. Either way, ``bytes``/``sha256`` are
    guaranteed to describe what is actually, durably stored -- the caller
    never has to know which path was taken to trust the values.
    """

    bytes: int
    sha256: str
    method: str            # "put" | "multipart" | "reused"
    storage_checksum_verified: bool
    part_count: int = 1


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001 - boto and the test fake differ
        if is_missing(exc):
            return False
        raise


def is_missing(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound", "NoSuchUpload"} or \
        type(exc).__name__ in {"NoSuchKey", "NotFound", "NoSuchUpload"}


def is_precondition_failed(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"409", "412", "ConditionalRequestConflict",
                    "PreconditionFailed"} or \
        type(exc).__name__ == "PreconditionFailed"


def sha256_stream(stream) -> tuple[int, str]:
    """Read a stream to exhaustion, returning (bytes read, hex digest)."""
    digest = hashlib.sha256()
    total = 0
    while True:
        chunk = stream.read(_CHUNK)
        if not chunk:
            break
        digest.update(chunk)
        total += len(chunk)
    return total, digest.hexdigest()


def verify_object_evidence(client, bucket: str, key: str, *,
                           expected_bytes: int, expected_sha256: str,
                           label: str) -> None:
    """Re-derive an object's actual size/SHA-256 from its stored bytes.

    Never trusts an S3 ETag: ETag is not guaranteed to be an MD5 (multipart
    uploads, some S3-compatible backends) and is not SHA-256 at all, so this
    always streams and re-hashes the object itself.

    This is the UNCONDITIONAL, always-correct verification: it is what the
    reporting platform's Transport consumer uses to independently verify
    accepted evidence (``reporting_platform/ingest/transport.py``), and it is
    what :func:`publish_object` falls back to whenever a cheaper,
    checksum-based confirmation is not available. Deliberately left
    unchanged by the large-file hardening work -- see "Producer vs consumer
    verification" in docs/TRANSPORT-CONTRACT.md.
    """
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001
        if is_missing(exc):
            raise TransportEvidenceError(
                f"{label}: missing object {key!r}") from exc
        raise
    actual_bytes = head.get("ContentLength")
    if actual_bytes != expected_bytes:
        raise TransportEvidenceError(
            f"{label}: object {key!r} declares {expected_bytes} bytes but "
            f"stores {actual_bytes}")
    try:
        stream = client.get_object(Bucket=bucket, Key=key)["Body"]
        actual_bytes2, actual_sha256 = sha256_stream(stream)
    except Exception as exc:  # noqa: BLE001
        if is_missing(exc):
            raise TransportEvidenceError(
                f"{label}: missing object {key!r}") from exc
        raise
    if actual_bytes2 != expected_bytes or actual_sha256 != expected_sha256:
        raise TransportEvidenceError(
            f"{label}: object {key!r} SHA-256 does not match its declaration")


def _is_composite_checksum(value: str) -> bool:
    """A multipart object's checksum carries a ``-<part-count>`` suffix and
    is the hash of the parts' own hashes concatenated, never the plain
    full-object SHA-256 -- confirmed against MinIO directly, not assumed
    from the spec. It cannot stand in for the canonical digest unless the
    caller already knows the exact part boundaries (only true for the parts
    THIS call itself just uploaded -- see ``_upload_multipart``)."""
    return bool(_COMPOSITE_CHECKSUM.search(value))


def _try_cheap_verify(client, bucket: str, key: str, *, expected_bytes: int,
                      expected_sha256: str, label: str) -> bool | None:
    """HEAD-only confirmation via a server-stored full-object SHA-256.

    Returns ``True``/raises when it reaches a verdict without a GET; returns
    ``None`` when the object's checksum cannot be used this way (absent --
    the backend does not support it, or this object predates checksums being
    requested -- or composite, i.e. this object was created by SOME
    multipart upload whose part boundaries are not known here), and the
    caller must fall back to :func:`verify_object_evidence`.
    """
    try:
        head = client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except Exception as exc:  # noqa: BLE001
        if is_missing(exc):
            raise TransportEvidenceError(
                f"{label}: missing object {key!r}") from exc
        return None
    actual_bytes = head.get("ContentLength")
    if actual_bytes != expected_bytes:
        raise TransportEvidenceError(
            f"{label}: object {key!r} declares {expected_bytes} bytes but "
            f"stores {actual_bytes}")
    checksum = head.get("ChecksumSHA256")
    if not checksum or _is_composite_checksum(checksum):
        return None
    expected_b64 = base64.b64encode(bytes.fromhex(expected_sha256)).decode()
    if checksum != expected_b64:
        raise TransportEvidenceError(
            f"{label}: object {key!r} SHA-256 does not match its declaration")
    return True


def _confirm_durable(client, bucket: str, key: str, *, expected_bytes: int,
                     expected_sha256: str, label: str,
                     checksum_mode: str) -> bool:
    """One confirmation pass: cheap where possible, otherwise the original
    full download. Always raises TransportEvidenceError on any mismatch."""
    if checksum_mode != "full_download":
        cheap = _try_cheap_verify(client, bucket, key, expected_bytes=expected_bytes,
                                  expected_sha256=expected_sha256, label=label)
        if cheap is not None:
            return cheap
    verify_object_evidence(client, bucket, key, expected_bytes=expected_bytes,
                           expected_sha256=expected_sha256, label=label)
    return False


# ------------------------------------------------------------- publication
class _HashingReader:
    """A read-only file wrapper that hashes exactly what it streams out.

    boto3 reads a file-like ``Body`` in chunks via ``.read(size)`` and may
    ``seek(0)`` to resend it on a transient retry; both are supported so this
    can be handed straight to ``put_object`` in place of a plain file object.
    """

    def __init__(self, fileobj) -> None:
        self._file = fileobj
        self._hasher = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._file.read(size)
        self._hasher.update(chunk)
        self.bytes_read += len(chunk)
        return chunk

    def hexdigest(self) -> str:
        return self._hasher.hexdigest()

    def seekable(self) -> bool:
        return self._file.seekable()

    def seek(self, offset: int, whence: int = 0) -> int:
        result = self._file.seek(offset, whence)
        if offset == 0 and whence == 0:
            # A resend from the start must hash exactly what is resent.
            self._hasher = hashlib.sha256()
            self.bytes_read = 0
        return result

    def tell(self) -> int:
        return self._file.tell()


def publish_object(client, bucket: str, key: str, path: Path, *,
                   upload: UploadTuning, label: str) -> PublishedObject:
    """Publish ``path`` to ``key``, create-only, hashing exactly what is
    streamed, choosing single PUT vs. multipart by size, and confirming
    durable storage as cheaply as the backend allows.

    If ``key`` already holds an object, this reuses it: ``path`` is hashed
    locally once (the only way to tell an identical retry from a conflicting
    one -- there is no server-side way to compare an object to a local file
    without reading one of them) and the remote object is confirmed against
    that hash, cheaply where possible. It is never re-uploaded.

    A create-only race lost during upload (another publisher's write won)
    is handled the same way: the object that is now there is confirmed
    against exactly the bytes THIS process streamed, with no second local
    read. See "Multipart create-only race semantics" in
    docs/TRANSPORT-CONTRACT.md for the exact interleaving this handles.
    """
    initial_stat = path.stat()
    if object_exists(client, bucket, key):
        file_bytes, digest = _hash_path(path)
        _guard_mutation(path, initial_stat, file_bytes, label)
        verified = _confirm_durable(client, bucket, key, expected_bytes=file_bytes,
                                    expected_sha256=digest, label=label,
                                    checksum_mode=upload.checksum_mode)
        return PublishedObject(bytes=file_bytes, sha256=digest, method="reused",
                               storage_checksum_verified=verified)

    size = initial_stat.st_size
    _validate_size(size, upload, label)
    if size < upload.multipart_threshold:
        result = _upload_single(client, bucket, key, path, upload=upload, label=label)
    else:
        result = _upload_multipart(client, bucket, key, path, upload=upload, label=label)
    _guard_mutation(path, initial_stat, result.bytes, label)
    return result


def _hash_path(path: Path) -> tuple[int, str]:
    with path.open("rb") as stream:
        return sha256_stream(stream)


def _validate_size(size: int, upload: UploadTuning, label: str) -> None:
    if size > S3_MAX_OBJECT_SIZE:
        raise TransportContractError(
            f"{label}: {size} bytes exceeds the {S3_MAX_OBJECT_SIZE}-byte "
            f"S3 maximum object size")
    if size >= upload.multipart_threshold:
        part_count = -(-size // upload.multipart_part_size)  # ceil div
        if part_count > S3_MAX_PART_COUNT:
            raise TransportContractError(
                f"{label}: {size} bytes at {upload.multipart_part_size}-byte "
                f"parts needs {part_count} parts, exceeding the "
                f"{S3_MAX_PART_COUNT}-part S3 limit; increase "
                f"multipart_part_size")


def _guard_mutation(path: Path, initial_stat, bytes_read: int, label: str) -> None:
    """Fail safe if the source changed during publication.

    Not the primary integrity mechanism -- the hash always describes exactly
    what was streamed, regardless of this check -- but a source file that
    grew, shrank or was touched while being read violates DCM's own
    "complete before invoking the publisher" contract, and publishing
    ``_COMPLETE.json`` over it must not proceed silently. Metadata alone is
    not an integrity proof (a rewrite of identical size/mtime would not be
    caught), which is why the hash-what-you-stream property above remains
    the thing that actually protects the declared SHA-256.
    """
    if bytes_read != initial_stat.st_size:
        raise TransportSourceMutatedError(
            f"{label}: {path} was {initial_stat.st_size} bytes before "
            f"publication but {bytes_read} bytes were read -- the source "
            f"changed during publication")
    try:
        final_stat = path.stat()
    except OSError as exc:
        raise TransportSourceMutatedError(
            f"{label}: {path} could not be re-checked after publication: "
            f"{exc}") from exc
    if (final_stat.st_size != initial_stat.st_size
            or final_stat.st_mtime != initial_stat.st_mtime):
        raise TransportSourceMutatedError(
            f"{label}: {path} was modified during publication (size/mtime "
            f"changed after the read completed)")


def _upload_single(client, bucket: str, key: str, path: Path, *,
                   upload: UploadTuning, label: str) -> PublishedObject:
    """Small/default-path upload: one create-only PUT, hashing exactly what
    is streamed. Requests a server-side SHA-256 checksum when checksum_mode
    allows it -- for a single PUT this checksum is, unambiguously, the
    full-object digest (confirmed against MinIO: it is never composite for a
    non-multipart object), so a match lets the durable-storage confirmation
    skip the GET entirely.
    """
    want_checksum = upload.checksum_mode != "full_download"
    with path.open("rb") as raw:
        body = _HashingReader(raw)
        kwargs = dict(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
        if want_checksum:
            kwargs["ChecksumAlgorithm"] = "SHA256"
        try:
            response = client.put_object(**kwargs)
        except Exception as exc:  # noqa: BLE001
            if not is_precondition_failed(exc):
                raise
            # Lost a create-only race: someone else published this object
            # first. Confirm it is byte-identical to what THIS process just
            # streamed, without re-reading the local file.
            verified = _confirm_durable(
                client, bucket, key, expected_bytes=body.bytes_read,
                expected_sha256=body.hexdigest(), label=label,
                checksum_mode=upload.checksum_mode)
            return PublishedObject(bytes=body.bytes_read, sha256=body.hexdigest(),
                                   method="put", storage_checksum_verified=verified)

    storage_verified = False
    if want_checksum:
        returned = response.get("ChecksumSHA256")
        if returned and not _is_composite_checksum(returned):
            expected_b64 = base64.b64encode(
                bytes.fromhex(body.hexdigest())).decode()
            storage_verified = returned == expected_b64
            # A mismatch here is not trusted blindly either way: falling
            # through to the full GET below re-derives the truth rather than
            # raising off a value this process has not independently proven.
    if not storage_verified:
        verify_object_evidence(client, bucket, key, expected_bytes=body.bytes_read,
                               expected_sha256=body.hexdigest(), label=label)
    return PublishedObject(bytes=body.bytes_read, sha256=body.hexdigest(),
                           method="put", storage_checksum_verified=storage_verified)


def _upload_multipart(client, bucket: str, key: str, path: Path, *,
                      upload: UploadTuning, label: str) -> PublishedObject:
    """Large-file upload: bounded-memory multipart, hashing exactly what is
    streamed in one sequential pass while uploading up to
    ``multipart_max_concurrency`` parts concurrently.

    Create-only is enforced with ``IfNoneMatch: "*"`` on
    ``CompleteMultipartUpload`` -- confirmed against MinIO to behave exactly
    like the single-PUT case (atomic, race loser gets PreconditionFailed,
    the winner's bytes are untouched). See "Multipart create-only race
    semantics" in docs/TRANSPORT-CONTRACT.md for backends where that
    conditional header may not be honoured.

    Verification: each part's server-returned SHA-256 checksum is compared
    to this process's own hash of the exact bytes it sent for that part
    (proves the network transit of every part was intact); the assembled
    object's server-returned COMPOSITE checksum is compared to the same
    composite formula computed locally from those same part digests (proves
    S3 assembled exactly, and only, the parts this process sent, in order).
    Both are confirmed against MinIO's actual formula, not assumed from the
    spec -- see storage.py's module docstring. Either check failing falls
    back to the unconditional full GET + rehash rather than trusting a
    partial result.
    """
    want_checksum = upload.checksum_mode != "full_download"
    part_size = upload.multipart_part_size
    concurrency = upload.multipart_max_concurrency

    mpu_kwargs: dict = {"Bucket": bucket, "Key": key}
    if want_checksum:
        mpu_kwargs["ChecksumAlgorithm"] = "SHA256"
    mpu = client.create_multipart_upload(**mpu_kwargs)
    upload_id = mpu["UploadId"]
    logger.info("multipart upload started: %s key=%s upload_id=%s part_size=%d "
               "concurrency=%d", label, key, upload_id, part_size, concurrency)

    whole_hasher = hashlib.sha256()
    total = 0
    part_digests: list[bytes] = []
    parts_meta: dict[int, dict] = {}
    started = time.monotonic()

    def _upload_one_part(part_number: int, chunk: bytes) -> dict:
        kwargs: dict = {"Bucket": bucket, "Key": key, "PartNumber": part_number,
                        "UploadId": upload_id, "Body": chunk}
        if want_checksum:
            kwargs["ChecksumAlgorithm"] = "SHA256"
        return client.upload_part(**kwargs)

    try:
        with path.open("rb") as fh, ThreadPoolExecutor(max_workers=concurrency) as pool:
            pending: list[tuple[int, Future]] = []
            part_number = 1
            while True:
                chunk = fh.read(part_size)
                if not chunk:
                    break
                whole_hasher.update(chunk)
                total += len(chunk)
                part_digests.append(hashlib.sha256(chunk).digest())
                if len(pending) >= concurrency:
                    done_number, done_future = pending.pop(0)
                    parts_meta[done_number] = done_future.result()
                pending.append(
                    (part_number, pool.submit(_upload_one_part, part_number, chunk)))
                part_number += 1
            for number, future in pending:
                parts_meta[number] = future.result()
    except Exception:
        logger.warning("multipart upload failed, aborting: %s key=%s upload_id=%s",
                       label, key, upload_id)
        _abort_multipart(client, bucket, key, upload_id)
        raise

    if not parts_meta:
        _abort_multipart(client, bucket, key, upload_id)
        raise TransportContractError(f"{label}: multipart upload produced no parts")

    ordered_numbers = sorted(parts_meta)
    # A part entry passed to CompleteMultipartUpload must repeat its own
    # ChecksumSHA256 when the upload requested one -- confirmed against
    # MinIO: completion WITHOUT it fails the whole upload with InvalidPart,
    # even though ETag alone is all CompleteMultipartUpload needs when no
    # checksum was requested at all.
    ordered_parts = []
    parts_checksums_ok = want_checksum
    for n in ordered_numbers:
        part = {"PartNumber": n, "ETag": parts_meta[n]["ETag"]}
        if want_checksum:
            returned = parts_meta[n].get("ChecksumSHA256")
            expected = base64.b64encode(part_digests[n - 1]).decode()
            if returned != expected:
                parts_checksums_ok = False
            if returned:
                part["ChecksumSHA256"] = returned
        ordered_parts.append(part)

    try:
        complete = client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": ordered_parts}, IfNoneMatch="*")
    except Exception as exc:  # noqa: BLE001
        if not is_precondition_failed(exc):
            _abort_multipart(client, bucket, key, upload_id)
            raise
        # Lost a create-only race on completion itself: confirm what is now
        # there against exactly the bytes THIS process streamed.
        digest_hex = whole_hasher.hexdigest()
        verified = _confirm_durable(
            client, bucket, key, expected_bytes=total, expected_sha256=digest_hex,
            label=label, checksum_mode=upload.checksum_mode)
        return PublishedObject(bytes=total, sha256=digest_hex, method="multipart",
                               storage_checksum_verified=verified,
                               part_count=len(ordered_parts))

    digest_hex = whole_hasher.hexdigest()
    storage_verified = False
    if want_checksum and parts_checksums_ok:
        composite_digest = hashlib.sha256(b"".join(part_digests)).digest()
        expected_composite = (f"{base64.b64encode(composite_digest).decode()}"
                              f"-{len(part_digests)}")
        storage_verified = complete.get("ChecksumSHA256") == expected_composite
    if not storage_verified:
        verify_object_evidence(client, bucket, key, expected_bytes=total,
                               expected_sha256=digest_hex, label=label)

    elapsed = time.monotonic() - started
    logger.info("multipart upload complete: %s key=%s parts=%d bytes=%d "
               "elapsed=%.1fs checksum_verified=%s", label, key,
               len(ordered_parts), total, elapsed, storage_verified)
    return PublishedObject(bytes=total, sha256=digest_hex, method="multipart",
                           storage_checksum_verified=storage_verified,
                           part_count=len(ordered_parts))


def _abort_multipart(client, bucket: str, key: str, upload_id: str) -> None:
    """Best-effort cleanup. Its failure is logged, never raised -- the
    caller is already unwinding a real failure, and a bucket lifecycle rule
    is the durable backstop for whatever this cannot reach; see "Abandoned
    multipart uploads" in docs/TRANSPORT-CONTRACT.md.
    """
    try:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not abort multipart upload key=%s upload_id=%s: %s",
                       key, upload_id, exc)
