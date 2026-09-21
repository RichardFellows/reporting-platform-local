"""The DCM -> S3 Transport evidence contract, versions 1 and 2.

Pure parsing/validation/path logic only -- no S3, no boto3, no environment
reads. This is deliberate: it is the one piece both a real DCM Python
subprocess and the reporting platform's consumer import, and a producer
reference implementation should not need object-storage credentials just to
validate a filename.

Contract v2 adds ``cob_date`` and ``source_system`` as producer-supplied
business/classification context and reorganises the S3 layout under them:

    received/cob_date=<date>/source_system=<system>/<transport-id>/
        <original data filename>
        <original control filename>            # zero or more
        _COMPLETE.json                         # uploaded last

Contract v1 markers (flat ``received/<transport-id>/...``, no cob_date/
source_system) remain PARSEABLE forever -- this platform has no tool that
rewrites already-published evidence, so a bucket may legitimately hold both
shapes side by side. See ``docs/TRANSPORT-CONTRACT.md``, "v1 compatibility".

The marker is authoritative transport metadata; the S3 path is physical
organisation only. ``parse_transport`` therefore checks that a v2 marker's
declared ``cob_date``/``source_system``/``transport_id`` and the key it was
read from agree, rather than ever inferring those values from the path alone.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import re
from typing import Any, Iterable

CONTRACT_VERSION = 2
SUPPORTED_VERSIONS = frozenset({1, CONTRACT_VERSION})
COMPLETE_FILENAME = "_COMPLETE.json"
ROLES = frozenset({"data", "control"})
DEFAULT_RECEIVED_PREFIX = "received"

_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")

_V1_FIELDS = frozenset({
    "transport_contract_version", "transport_id", "source",
    "legacy_feed_id", "source_observed_at", "uploaded_at",
    "producer_run_id", "files",
})
_V1_REQUIRED_FIELDS = _V1_FIELDS - {"producer_run_id"}

_V2_FIELDS = _V1_FIELDS | {"cob_date", "source_system"}
# producer_run_id is required in v2: the deterministic TransportID formula
# (source-lower + legacy_feed_id + producer_run_id) has no meaning without
# it, so an omitted value cannot be interpreted as "no run identity" the way
# v1 allowed -- see publisher.py.
_V2_REQUIRED_FIELDS = _V2_FIELDS - set()

_FILE_FIELDS = frozenset({
    "role", "original_filename", "object_key", "bytes", "sha256",
})


class TransportError(Exception):
    """Base class for every error this package raises."""


class TransportContractError(TransportError, ValueError):
    """The marker or producer input does not satisfy the versioned contract."""


class TransportEvidenceError(TransportContractError):
    """Stored evidence is absent or disagrees with its declaration."""


class TransportConflictError(TransportContractError):
    """A TransportID or object key already holds different evidence."""


class TransportStorageError(TransportError):
    """The object store could not be reached, or denied access.

    Deliberately NOT a TransportContractError: this is a connectivity/auth
    failure, not a statement about the transport's content, and a DCM caller
    needs to tell the two apart (retry later vs. fix the input).
    """


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
    # None for a v1 marker; always set for v2.
    cob_date: str | None = None
    source_system: str | None = None

    @property
    def data_files(self) -> tuple[TransportFile, ...]:
        return tuple(f for f in self.files if f.role == "data")

    @property
    def control_files(self) -> tuple[TransportFile, ...]:
        return tuple(f for f in self.files if f.role == "control")

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
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
        if self.cob_date is not None:
            out["cob_date"] = self.cob_date
        if self.source_system is not None:
            out["source_system"] = self.source_system
        return out


# --------------------------------------------------------------- path rules
def validate_prefix(value: str | None) -> str:
    """The configured transport evidence prefix, without boundary slashes."""
    raw = value if value is not None else DEFAULT_RECEIVED_PREFIX
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


def _validate_segment(value: Any, label: str, context: str | None) -> str:
    prefix = f"{context}: " if context else ""
    if not isinstance(value, str) or not value:
        raise TransportContractError(f"{prefix}{label} must not be empty")
    # '.'/'..' pass the charset check below (both are made only of allowed
    # characters) but must still be refused: either would otherwise let a
    # segment collapse the prefix it is joined into, e.g. transport_id='..'
    # building 'received/cob_date=x/source_system=y/../' one level up.
    if value in (".", ".."):
        raise TransportContractError(
            f"{prefix}{label} {value!r} is not a safe path segment")
    if not _SEGMENT.fullmatch(value):
        raise TransportContractError(
            f"{prefix}{label} {value!r} must be one opaque path segment of "
            f"letters, digits, '.', '_' or '-'")
    return value


def validate_transport_id(value: Any, context: str | None = None) -> str:
    return _validate_segment(value, "transport_id", context)


def validate_source_system(value: Any, context: str | None = None) -> str:
    return _validate_segment(value, "source_system", context)


def validate_cob_date(value: Any, context: str | None = None) -> str:
    """Business date, not inferred from a filename or an upload timestamp.

    Producer-supplied and validated only for SHAPE (a real ISO calendar
    date, round-tripping exactly): this module has no feed configuration to
    check it against, and never will -- that remains Delivery's job
    (docs/DELIVERY-CONTRACT.md).
    """
    label = f"{context}: " if context else ""
    if not isinstance(value, str):
        raise TransportContractError(f"{label}cob_date must be a string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise TransportContractError(
            f"{label}cob_date {value!r} is not a valid ISO-8601 date "
            f"(YYYY-MM-DD)") from exc
    if parsed.isoformat() != value:
        raise TransportContractError(
            f"{label}cob_date {value!r} must be written as YYYY-MM-DD")
    return value


def transport_prefix(cob_date: str, source_system: str, transport_id: str,
                     prefix: str | None = None) -> str:
    """The v2 object-storage prefix for one Transport's evidence."""
    validate_cob_date(cob_date)
    validate_source_system(source_system)
    validate_transport_id(transport_id)
    return (f"{validate_prefix(prefix)}/cob_date={cob_date}/"
           f"source_system={source_system}/{transport_id}/")


def complete_key(cob_date: str, source_system: str, transport_id: str,
                 prefix: str | None = None) -> str:
    return (f"{transport_prefix(cob_date, source_system, transport_id, prefix)}"
           f"{COMPLETE_FILENAME}")


def transport_prefix_v1(transport_id: str, prefix: str | None = None) -> str:
    """The legacy flat v1 prefix. Read-compatibility only; never written."""
    validate_transport_id(transport_id)
    return f"{validate_prefix(prefix)}/{transport_id}/"


def complete_key_v1(transport_id: str, prefix: str | None = None) -> str:
    return f"{transport_prefix_v1(transport_id, prefix)}{COMPLETE_FILENAME}"


# ------------------------------------------------------------------- parsing
def parse_transport(document: bytes | str, marker_key: str,
                    prefix: str | None = None) -> Transport:
    """Parse and validate an untrusted ``_COMPLETE.json`` document, v1 or v2."""
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

    version = raw.get("transport_contract_version")
    if type(version) is not int:  # bool is an int and is not a version
        raise TransportContractError(
            f"{marker_key}: transport_contract_version must be an integer")
    if version not in SUPPORTED_VERSIONS:
        supported = ", ".join(str(v) for v in sorted(SUPPORTED_VERSIONS))
        raise TransportContractError(
            f"{marker_key}: unsupported transport contract version {version!r}; "
            f"this platform accepts version(s) {supported}")

    if version == 1:
        return _parse_v1(raw, marker_key, prefix)
    return _parse_v2(raw, marker_key, prefix)


def _parse_v1(raw: dict[str, Any], marker_key: str,
             prefix: str | None) -> Transport:
    missing = sorted(_V1_REQUIRED_FIELDS - set(raw))
    if missing:
        raise TransportContractError(
            f"{marker_key}: missing required field(s): {', '.join(missing)}")
    unknown = sorted(set(raw) - _V1_FIELDS)
    if unknown:
        raise TransportContractError(
            f"{marker_key}: unknown field(s): {', '.join(unknown)}")

    transport_id = raw["transport_id"]
    validate_transport_id(transport_id, marker_key)
    expected_marker = complete_key_v1(transport_id, prefix)
    if marker_key != expected_marker:
        raise TransportContractError(
            f"transport {transport_id}: marker key must be {expected_marker!r}, "
            f"not {marker_key!r}")

    source = _required_text(raw, "source", marker_key)
    legacy_feed_id = _required_text(raw, "legacy_feed_id", marker_key)
    producer_run_id = _optional_text(raw, "producer_run_id", transport_id)
    source_observed_at = _parse_timestamp(
        raw["source_observed_at"], "source_observed_at", transport_id)
    uploaded_at = _parse_timestamp(raw["uploaded_at"], "uploaded_at",
                                   transport_id)
    expected_prefix = transport_prefix_v1(transport_id, prefix)
    files = _parse_files(raw, transport_id, expected_prefix)

    return Transport(
        transport_contract_version=1, transport_id=transport_id,
        source=source, legacy_feed_id=legacy_feed_id,
        source_observed_at=source_observed_at, uploaded_at=uploaded_at,
        producer_run_id=producer_run_id, cob_date=None, source_system=None,
        files=files)


def _parse_v2(raw: dict[str, Any], marker_key: str,
             prefix: str | None) -> Transport:
    missing = sorted(_V2_REQUIRED_FIELDS - set(raw))
    if missing:
        raise TransportContractError(
            f"{marker_key}: missing required field(s): {', '.join(missing)}")
    unknown = sorted(set(raw) - _V2_FIELDS)
    if unknown:
        raise TransportContractError(
            f"{marker_key}: unknown field(s): {', '.join(unknown)}")

    transport_id = raw["transport_id"]
    validate_transport_id(transport_id, marker_key)
    cob_date = validate_cob_date(raw["cob_date"], marker_key)
    source_system = validate_source_system(raw["source_system"], marker_key)
    expected_marker = complete_key(cob_date, source_system, transport_id, prefix)
    if marker_key != expected_marker:
        raise TransportContractError(
            f"transport {transport_id}: marker key must be {expected_marker!r}, "
            f"not {marker_key!r}")

    source = _required_text(raw, "source", marker_key)
    legacy_feed_id = _required_text(raw, "legacy_feed_id", marker_key)
    producer_run_id = _required_text(raw, "producer_run_id", marker_key)
    source_observed_at = _parse_timestamp(
        raw["source_observed_at"], "source_observed_at", transport_id)
    uploaded_at = _parse_timestamp(raw["uploaded_at"], "uploaded_at",
                                   transport_id)
    expected_prefix = transport_prefix(cob_date, source_system, transport_id, prefix)
    files = _parse_files(raw, transport_id, expected_prefix)

    return Transport(
        transport_contract_version=2, transport_id=transport_id,
        source=source, legacy_feed_id=legacy_feed_id,
        source_observed_at=source_observed_at, uploaded_at=uploaded_at,
        producer_run_id=producer_run_id, cob_date=cob_date,
        source_system=source_system, files=files)


def _parse_files(raw: dict[str, Any], transport_id: str,
                 expected_prefix: str) -> tuple[TransportFile, ...]:
    file_values = raw["files"]
    if not isinstance(file_values, list):
        raise TransportContractError(
            f"transport {transport_id}: files must be a JSON array")
    files = tuple(_parse_file(value, transport_id, expected_prefix)
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
    return files


def _parse_file(raw: Any, transport_id: str,
                expected_prefix: str) -> TransportFile:
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
    expected_key = f"{expected_prefix}{filename}"
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


def serialize_transport(transport: Transport) -> bytes:
    """Canonical bytes for the completion marker."""
    return json.dumps(transport.as_dict(), indent=2, sort_keys=True).encode("utf-8")


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


def _optional_text(raw: dict[str, Any], field: str,
                   context: str) -> str | None:
    value = raw.get(field)
    if value is None:
        return None
    if (not isinstance(value, str) or not value.strip()
            or value != value.strip()):
        raise TransportContractError(
            f"transport {context}: {field} must be non-empty without "
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
