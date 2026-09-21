"""Explicit, reusable S3-compatible storage configuration and evidence I/O.

Deliberately does not import anything from ``reporting_platform``: a real DCM
process should be able to install just this package plus ``boto3`` and run
the CLI. Credentials are never accepted as configuration here -- only
endpoint/bucket/region/prefix, which is what varies between local MinIO and an
enterprise S3-compatible target. Authentication is left entirely to the
standard AWS/boto3 credential-provider chain (environment variables, shared
credentials/config files, an EC2/ECS/pod IAM role, ...), so nothing in this
module -- or the CLI built on it -- ever accepts a secret as an argument.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os

from reporting_transport.contract import (
    TransportContractError, TransportEvidenceError, validate_prefix,
)

_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class StorageConfig:
    bucket: str
    endpoint_url: str | None = None
    region: str | None = None
    received_prefix: str = "received"

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
                   received_prefix=values["received_prefix"])

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
    """
    import boto3

    return boto3.client(
        "s3", endpoint_url=config.endpoint_url, region_name=config.region)


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
    return code in {"404", "NoSuchKey", "NotFound"} or \
        type(exc).__name__ in {"NoSuchKey", "NotFound"}


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
