"""DCM -> S3 transport evidence contract v1.

This module begins at object storage. DCM owns DFS watching and the decision
that a source delivery is complete; the reporting platform accepts that
assertion only when ``_COMPLETE.json`` exists and every declared object passes
the checks below.

Transport is deliberately not Delivery. Nothing here writes ``landing/``,
creates a delivery id, normalizes an archive, updates the registry, or changes
raw provenance. Phase 2 owns that mapping.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
import re
from typing import Any, Iterable

CONTRACT_VERSION = 1
COMPLETE_FILENAME = "_COMPLETE.json"
ROLES = frozenset({"data", "control"})
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_TRANSPORT_FIELDS = frozenset({
    "transport_contract_version", "transport_id", "source",
    "legacy_feed_id", "source_observed_at", "uploaded_at",
    "producer_run_id", "files",
})
_REQUIRED_TRANSPORT_FIELDS = _TRANSPORT_FIELDS - {"producer_run_id"}
_FILE_FIELDS = frozenset({
    "role", "original_filename", "object_key", "bytes", "sha256",
})


class TransportContractError(ValueError):
    """The marker does not satisfy the versioned transport contract."""


class TransportEvidenceError(TransportContractError):
    """Stored evidence is absent or disagrees with the marker."""


@dataclass(frozen=True)
class TransportFile:
    role: str
    original_filename: str
    object_key: str
    bytes: int
    sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "original_filename": self.original_filename,
            "object_key": self.object_key,
            "bytes": self.bytes,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class Transport:
    transport_contract_version: int
    transport_id: str
    source: str
    legacy_feed_id: str
    source_observed_at: datetime
    uploaded_at: datetime
    files: tuple[TransportFile, ...]
    producer_run_id: str | None = None

    @property
    def data_files(self) -> tuple[TransportFile, ...]:
        return tuple(f for f in self.files if f.role == "data")

    @property
    def control_files(self) -> tuple[TransportFile, ...]:
        return tuple(f for f in self.files if f.role == "control")

    def as_dict(self) -> dict[str, Any]:
        out = {
            "transport_contract_version": self.transport_contract_version,
            "transport_id": self.transport_id,
            "source": self.source,
            "legacy_feed_id": self.legacy_feed_id,
            "source_observed_at": _timestamp(self.source_observed_at),
            "uploaded_at": _timestamp(self.uploaded_at),
            "files": [f.as_dict() for f in self.files],
        }
        if self.producer_run_id is not None:
            out["producer_run_id"] = self.producer_run_id
        return out


def received_prefix(value: str | None = None) -> str:
    """The configured transport evidence prefix, without boundary slashes."""
    raw = value if value is not None else os.environ.get(
        "REPORTING_RECEIVED_PREFIX", "received")
    if not isinstance(raw, str) or not raw:
        raise TransportContractError("received prefix must not be empty")
    if raw != raw.strip("/") or "\\" in raw:
        raise TransportContractError(
            f"received prefix {raw!r} must not have boundary slashes")
    segments = raw.split("/")
    if any(not part or part in (".", "..") for part in segments):
        raise TransportContractError(
            f"received prefix {raw!r} contains an unsafe path segment")
    return raw


def transport_prefix(transport_id: str, prefix: str | None = None) -> str:
    _validate_transport_id(transport_id)
    return f"{received_prefix(prefix)}/{transport_id}/"


def complete_key(transport_id: str, prefix: str | None = None) -> str:
    return f"{transport_prefix(transport_id, prefix)}{COMPLETE_FILENAME}"


def parse_transport(document: bytes | str,
                    marker_key: str,
                    prefix: str | None = None) -> Transport:
    """Parse and validate an untrusted v1 ``_COMPLETE.json`` document."""
    try:
        text = document.decode("utf-8") if isinstance(document, bytes) else document
    except UnicodeDecodeError as exc:
        raise TransportContractError(
            f"{marker_key}: completion marker is not valid UTF-8") from exc
    try:
        raw = json.loads(text, object_pairs_hook=_unique_object)
    except (json.JSONDecodeError, _DuplicateMember) as exc:
        raise TransportContractError(
            f"{marker_key}: malformed completion JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise TransportContractError(
            f"{marker_key}: completion document must be a JSON object")

    missing = sorted(_REQUIRED_TRANSPORT_FIELDS - set(raw))
    if missing:
        raise TransportContractError(
            f"{marker_key}: missing required field(s): {', '.join(missing)}")
    unknown = sorted(set(raw) - _TRANSPORT_FIELDS)
    if unknown:
        raise TransportContractError(
            f"{marker_key}: unknown field(s): {', '.join(unknown)}")

    version = raw["transport_contract_version"]
    if type(version) is not int:  # bool is an int and is not a version
        raise TransportContractError(
            f"{marker_key}: transport_contract_version must be an integer")
    if version != CONTRACT_VERSION:
        raise TransportContractError(
            f"{marker_key}: unsupported transport contract version {version!r}; "
            f"this platform accepts version {CONTRACT_VERSION}")

    transport_id = raw["transport_id"]
    _validate_transport_id(transport_id, marker_key)
    expected_marker = complete_key(transport_id, prefix)
    if marker_key != expected_marker:
        raise TransportContractError(
            f"transport {transport_id}: marker key must be {expected_marker!r}, "
            f"not {marker_key!r}")

    source = _required_text(raw, "source", marker_key)
    legacy_feed_id = _required_text(raw, "legacy_feed_id", marker_key)
    producer_run_id = raw.get("producer_run_id")
    if producer_run_id is not None:
        if (not isinstance(producer_run_id, str)
                or not producer_run_id.strip()
                or producer_run_id != producer_run_id.strip()):
            raise TransportContractError(
                f"transport {transport_id}: producer_run_id must be non-empty "
                f"without surrounding whitespace")

    source_observed_at = _parse_timestamp(
        raw["source_observed_at"], "source_observed_at", transport_id)
    uploaded_at = _parse_timestamp(raw["uploaded_at"], "uploaded_at",
                                   transport_id)
    file_values = raw["files"]
    if not isinstance(file_values, list):
        raise TransportContractError(
            f"transport {transport_id}: files must be a JSON array")
    files = tuple(_parse_file(value, transport_id, prefix)
                  for value in file_values)
    if not any(f.role == "data" for f in files):
        raise TransportContractError(
            f"transport {transport_id}: at least one data object is required")

    keys: set[str] = set()
    names: set[str] = set()
    for file in files:
        if file.object_key in keys:
            raise TransportContractError(
                f"transport {transport_id}: duplicate object declaration "
                f"{file.object_key!r}")
        if file.original_filename in names:
            raise TransportContractError(
                f"transport {transport_id}: duplicate original filename "
                f"{file.original_filename!r}")
        keys.add(file.object_key)
        names.add(file.original_filename)

    return Transport(
        transport_contract_version=version,
        transport_id=transport_id,
        source=source,
        legacy_feed_id=legacy_feed_id,
        source_observed_at=source_observed_at,
        uploaded_at=uploaded_at,
        producer_run_id=producer_run_id,
        files=files,
    )


def serialize_transport(transport: Transport) -> bytes:
    """Canonical bytes used by the simulator for the completion marker."""
    return json.dumps(transport.as_dict(), indent=2, sort_keys=True).encode("utf-8")


def read_transport(marker_key: str, *, client=None, bucket: str | None = None,
                   prefix: str | None = None) -> Transport:
    """Read and parse a completion marker without accepting its evidence."""
    client = client or _client()
    bucket = bucket or _bucket()
    try:
        body = client.get_object(Bucket=bucket, Key=marker_key)["Body"].read()
    except Exception as exc:  # noqa: BLE001 - boto and the test fake differ
        if _is_missing(exc):
            raise TransportEvidenceError(
                f"{marker_key}: completion marker does not exist") from exc
        raise
    return parse_transport(body, marker_key, prefix)


def validate_transport(transport: Transport, *, client=None,
                       bucket: str | None = None) -> Transport:
    """Verify every declared object's existence, byte count and SHA-256.

    The function only reads object storage and returns the same immutable
    value on success, so repeated validation is side-effect free.
    """
    client = client or _client()
    bucket = bucket or _bucket()
    for file in transport.files:
        _validate_file_evidence(transport.transport_id, file,
                                client=client, bucket=bucket)
    return transport


def read_validated_transport(marker_key: str, *, client=None,
                             bucket: str | None = None,
                             prefix: str | None = None) -> Transport:
    transport = read_transport(marker_key, client=client, bucket=bucket,
                               prefix=prefix)
    return validate_transport(transport, client=client, bucket=bucket)


def list_completed_transports(*, client=None, bucket: str | None = None,
                              prefix: str | None = None) -> list[str]:
    """List completion-marker keys; unmarked source objects are invisible."""
    client = client or _client()
    bucket = bucket or _bucket()
    root = f"{received_prefix(prefix)}/"
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=root):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            remainder = key[len(root):]
            parts = remainder.split("/")
            if len(parts) == 2 and parts[1] == COMPLETE_FILENAME:
                try:
                    _validate_transport_id(parts[0], key)
                except TransportContractError:
                    continue
                keys.append(key)
    return sorted(set(keys))


def _parse_file(raw: Any, transport_id: str,
                prefix: str | None) -> TransportFile:
    if not isinstance(raw, dict):
        raise TransportContractError(
            f"transport {transport_id}: every files entry must be an object")
    missing = sorted(_FILE_FIELDS - set(raw))
    if missing:
        raise TransportContractError(
            f"transport {transport_id}: file declaration missing field(s): "
            f"{', '.join(missing)}")
    unknown = sorted(set(raw) - _FILE_FIELDS)
    if unknown:
        raise TransportContractError(
            f"transport {transport_id}: file declaration has unknown field(s): "
            f"{', '.join(unknown)}")

    role = raw["role"]
    if not isinstance(role, str) or role not in ROLES:
        raise TransportContractError(
            f"transport {transport_id}: unsupported file role {role!r}")
    filename = raw["original_filename"]
    _validate_filename(filename, transport_id)
    object_key = raw["object_key"]
    if not isinstance(object_key, str):
        raise TransportContractError(
            f"transport {transport_id}: object_key must be a string")
    expected_key = f"{transport_prefix(transport_id, prefix)}{filename}"
    if object_key != expected_key:
        raise TransportContractError(
            f"transport {transport_id}: object {object_key!r} must be stored "
            f"unchanged as {expected_key!r}")
    if object_key.endswith(f"/{COMPLETE_FILENAME}"):
        raise TransportContractError(
            f"transport {transport_id}: {COMPLETE_FILENAME} cannot be a "
            f"data/control object")

    size = raw["bytes"]
    if type(size) is not int or size < 0:
        raise TransportContractError(
            f"transport {transport_id}: object {object_key!r} has invalid "
            f"byte count {size!r}")
    digest = raw["sha256"]
    if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
        raise TransportContractError(
            f"transport {transport_id}: object {object_key!r} has malformed "
            f"SHA-256")
    return TransportFile(role=role, original_filename=filename,
                         object_key=object_key, bytes=size,
                         sha256=digest.lower())


def _validate_transport_id(value: Any, context: str | None = None) -> None:
    label = f"{context}: " if context else ""
    if not isinstance(value, str) or not value.strip():
        raise TransportContractError(f"{label}transport_id must not be empty")
    if value != value.strip() or value in (".", ".."):
        raise TransportContractError(
            f"{label}transport_id {value!r} is not a safe path segment")
    if "/" in value or "\\" in value or "\x00" in value:
        raise TransportContractError(
            f"{label}transport_id {value!r} must be one opaque path segment")


def _validate_filename(value: Any, transport_id: str) -> None:
    if not isinstance(value, str) or not value or value in (".", ".."):
        raise TransportContractError(
            f"transport {transport_id}: original_filename must be a basename")
    if value != value.strip() or "/" in value or "\\" in value or "\x00" in value:
        raise TransportContractError(
            f"transport {transport_id}: unsafe original filename {value!r}")
    if value == COMPLETE_FILENAME:
        raise TransportContractError(
            f"transport {transport_id}: {COMPLETE_FILENAME} cannot be a "
            f"source filename")


def _required_text(raw: dict[str, Any], field: str, context: str) -> str:
    value = raw[field]
    if (not isinstance(value, str) or not value.strip()
            or value != value.strip()):
        raise TransportContractError(
            f"{context}: {field} must be a non-empty string without "
            f"surrounding whitespace")
    return value


def _parse_timestamp(value: Any, field: str, transport_id: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise TransportContractError(
            f"transport {transport_id}: {field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TransportContractError(
            f"transport {transport_id}: {field} is not a valid ISO-8601 "
            f"timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TransportContractError(
            f"transport {transport_id}: {field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class _DuplicateMember(ValueError):
    pass


def _unique_object(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise _DuplicateMember(f"duplicate JSON member {key!r}")
        out[key] = value
    return out


def _sha256_stream(stream) -> str:
    digest = hashlib.sha256()
    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    return digest.hexdigest()


def _validate_file_evidence(transport_id: str, file: TransportFile, *,
                            client, bucket: str) -> None:
    try:
        head = client.head_object(Bucket=bucket, Key=file.object_key)
    except Exception as exc:  # noqa: BLE001
        if _is_missing(exc):
            raise TransportEvidenceError(
                f"transport {transport_id}: missing object "
                f"{file.object_key!r}") from exc
        raise
    actual_bytes = head.get("ContentLength")
    if actual_bytes != file.bytes:
        raise TransportEvidenceError(
            f"transport {transport_id}: object {file.object_key!r} declares "
            f"{file.bytes} bytes but stores {actual_bytes}")
    try:
        stream = client.get_object(Bucket=bucket, Key=file.object_key)["Body"]
        actual_sha256 = _sha256_stream(stream)
    except Exception as exc:  # noqa: BLE001
        if _is_missing(exc):
            raise TransportEvidenceError(
                f"transport {transport_id}: missing object "
                f"{file.object_key!r}") from exc
        raise
    if actual_sha256 != file.sha256:
        raise TransportEvidenceError(
            f"transport {transport_id}: object {file.object_key!r} SHA-256 "
            f"does not match its declaration")


def _is_missing(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"} or \
        type(exc).__name__ in {"NoSuchKey", "NotFound"}


def _client():
    from reporting_platform.ingest.arrival import _client as arrival_client
    return arrival_client()


def _bucket() -> str:
    from reporting_platform.ingest.arrival import _bucket as arrival_bucket
    return arrival_bucket()
