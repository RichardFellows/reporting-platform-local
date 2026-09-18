"""Accepted Transport -> immutable DeliveryManifest v1.

Transport records what an acquisition system transferred.  This module is
the first business interpretation of that evidence: it resolves one Feed,
derives business identity without renaming source objects, and creates one
immutable manifest below ``deliveries/``.  It deliberately has no Airflow,
normalization, Raw, or registry dependency.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import PurePath
import re
from typing import Any, Callable, Iterable

from reporting_platform.common.context import Feed, feeds
from reporting_platform.common.parsing import feed_format
from reporting_platform.ingest import control
from reporting_platform.ingest import transport as transport_contract


MANIFEST_VERSION = 1
DELIVERIES_PREFIX = "deliveries"


class DeliveryError(ValueError):
    """The accepted Transport cannot be interpreted as a Delivery."""


class FeedResolutionError(DeliveryError):
    """Transport external identity does not resolve exactly one Feed."""


class IdentityResolutionError(DeliveryError):
    """Business identity is missing, malformed, or contradictory."""


class DeliveryManifestError(DeliveryError):
    """An existing DeliveryManifest is malformed or conflicts with evidence."""


@dataclass(frozen=True)
class DeliverySourceFile:
    role: str
    original_filename: str
    object_key: str
    bytes: int
    sha256: str

    @classmethod
    def from_transport(cls, item: transport_contract.TransportFile):
        return cls(**item.as_dict())

    def as_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "original_filename": self.original_filename,
            "object_key": self.object_key,
            "bytes": self.bytes,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    feed: str
    transport_id: str
    transport_source: str
    external_feed_id: str
    business_date: date
    file_version: int | None
    received_at: datetime
    source_observed_at: datetime
    schema_version: str
    completion_marker: str
    source_files: tuple[DeliverySourceFile, ...]
    producer_assertions: dict[str, Any]
    identity: dict[str, Any]
    feed_contract: dict[str, Any]

    def as_manifest(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "delivery_manifest_version": MANIFEST_VERSION,
            "delivery_id": self.delivery_id,
            "feed": self.feed,
            "transport": {
                "source": self.transport_source,
                "transport_id": self.transport_id,
                "external_feed_id": self.external_feed_id,
                "completion_marker": self.completion_marker,
            },
            "business_date": self.business_date.isoformat(),
            "timestamps": {
                "source_observed_at": _timestamp(self.source_observed_at),
                "received_at": _timestamp(self.received_at),
            },
            "feed_contract": self.feed_contract,
            "identity": self.identity,
            "source_files": [item.as_dict() for item in self.source_files],
            "producer_assertions": self.producer_assertions,
        }
        if self.file_version is not None:
            out["file_version"] = self.file_version
        return out


@dataclass(frozen=True)
class ResolvedIdentity:
    business_date: date
    file_version: int | None
    provenance: dict[str, Any]
    producer_assertions: dict[str, Any]


def normalization_contract(feed: Feed) -> dict[str, Any]:
    """The resolved, historical contract needed to normalize this Delivery.

    Identity fields remain at ``feed_contract``'s top level for v1 readers.
    This nested block was added additively in Phase 3; an already-created
    DeliveryManifest is immutable and is never backfilled.
    """
    configured = feed.delivery or {}
    kind = configured.get("kind", "file")
    out: dict[str, Any] = {
        "contract_version": 1,
        "kind": kind,
        # These are ingestion semantics, not transport metadata.  Snapshot
        # them beside the parser contract so a delayed Delivery is not read
        # using whichever Feed YAML happens to be current at ingest time.
        "source_system": feed.source_system,
        "expected_min_rows": feed.expected_min_rows,
        "schema_drift": feed.schema_drift,
        "format": feed_format(feed),
        "columns": list(feed.columns),
        "source_columns": dict(feed.source_columns),
    }
    if kind == "archive":
        out["member_pattern"] = configured["member_pattern"]
    return out


def delivery_id_for(transport: transport_contract.Transport) -> str:
    """Opaque deterministic identity for one immutable Transport occurrence."""
    material = (f"delivery-v1\0{transport.source}\0{transport.transport_id}"
                .encode("utf-8"))
    return "dlv_" + hashlib.sha256(material).hexdigest()[:32]


def manifest_key(transport: transport_contract.Transport) -> str:
    _safe_segment(transport.source, "transport source")
    return (f"{DELIVERIES_PREFIX}/{transport.source}/"
            f"{transport.transport_id}/delivery-manifest.json")


def resolve_transport_feed(
        transport: transport_contract.Transport,
        registry: dict[str, Feed] | None = None) -> Feed:
    """Resolve explicit ``(source, external id)`` metadata to exactly one Feed."""
    registry = feeds() if registry is None else registry
    matches = [fd for fd in registry.values()
               if fd.source_identifiers.get(transport.source)
               == transport.legacy_feed_id]
    if not matches:
        raise FeedResolutionError(
            f"transport {transport.transport_id}: unknown {transport.source} "
            f"external feed id {transport.legacy_feed_id!r}")
    if len(matches) != 1:
        # Loading the normal registry rejects this already.  Keeping the pure
        # operation defensive makes it correct with an injected test registry.
        raise FeedResolutionError(
            f"transport {transport.transport_id}: {transport.source} external "
            f"feed id {transport.legacy_feed_id!r} maps to more than one Feed: "
            f"{', '.join(sorted(fd.name for fd in matches))}")
    return matches[0]


def resolve_business_identity(
        transport: transport_contract.Transport,
        feed: Feed,
        read_object: Callable[[str], bytes]) -> ResolvedIdentity:
    """Resolve date/version and declarations from original Transport evidence.

    All configured evidence is inspected even after the first value is found.
    The configured order selects provenance only when every source agrees.
    """
    by_source: dict[str, dict[str, list[tuple[Any, str]]]] = {
        source: {"business_date": [], "file_version": []}
        for source in feed.delivery_identity
    }
    assertions: dict[str, list[tuple[Any, str]]] = {
        "business_date": [], "file_version": [],
        "row_count": [], "md5": [],
    }

    if "filename" in by_source:
        for item in transport.data_files:
            parsed = feed.parse_filename(item.original_filename)
            if parsed is not None:
                cob, version = parsed
                by_source["filename"]["business_date"].append(
                    (cob, item.object_key))
                by_source["filename"]["file_version"].append(
                    (version, item.object_key))

    ctl = (feed.delivery or {}).get("control") or {}
    configured_fields = tuple(
        field for field in ("cob_date", "version", "row_count", "md5")
        if field in ctl)
    if transport.control_files and configured_fields:
        _check_control_names(feed, transport)
        for item in transport.control_files:
            body = read_object(item.object_key)
            text = control.decode(feed, body, item.original_filename)
            raw = control.read(
                ctl, text, fields=configured_fields, feed_name=feed.name,
                filename=item.original_filename, block="delivery.control")
            for field, value in raw.items():
                try:
                    parsed = control.value(field, value)
                except ValueError as exc:
                    raise IdentityResolutionError(
                        f"transport {transport.transport_id}: control object "
                        f"{item.object_key!r}: {exc}") from exc
                target = {"cob_date": "business_date",
                          "version": "file_version"}.get(field, field)
                if field == "cob_date":
                    parsed = date.fromisoformat(parsed)
                assertions[target].append((parsed, item.object_key))
                if "control" in by_source and target in (
                        "business_date", "file_version"):
                    by_source["control"][target].append(
                        (parsed, item.object_key))

    # A configured control source with no matching field is not secretly a
    # filename rule. It simply contributes no evidence and the next explicit
    # source may resolve the identity.
    selected: dict[str, tuple[Any, str, list[str]]] = {}
    for field in ("business_date", "file_version"):
        all_evidence: list[tuple[Any, str, str]] = []
        for source in feed.delivery_identity:
            evidence = by_source[source][field]
            _require_consistent(transport, field, evidence, source)
            all_evidence.extend((value, object_key, source)
                                for value, object_key in evidence)
        _require_consistent(
            transport, field,
            [(value, object_key) for value, object_key, _ in all_evidence],
            "configured evidence")
        for source in feed.delivery_identity:
            evidence = by_source[source][field]
            if evidence:
                objects = sorted({object_key for _, object_key in evidence})
                selected[field] = (evidence[0][0], source, objects)
                break

    if "business_date" not in selected:
        raise IdentityResolutionError(
            f"transport {transport.transport_id}: business date is missing; "
            f"configured evidence order is {feed.delivery_identity}")

    for field, evidence in assertions.items():
        _require_consistent(transport, field, evidence, "control")
    producer_assertions: dict[str, Any] = {}
    for field, evidence in assertions.items():
        if evidence:
            value = evidence[0][0]
            producer_assertions[field] = (
                value.isoformat() if isinstance(value, date) else value)
    if transport.producer_run_id is not None:
        producer_assertions["producer_run_id"] = transport.producer_run_id

    business_date, date_source, date_objects = selected["business_date"]
    provenance: dict[str, Any] = {
        "resolution_order": list(feed.delivery_identity),
        "business_date_source": date_source,
        "business_date_source_object": date_objects[0],
        "business_date_evidence_objects": date_objects,
    }
    version = None
    if "file_version" in selected:
        version, version_source, version_objects = selected["file_version"]
        provenance.update({
            "file_version_source": version_source,
            "file_version_source_object": version_objects[0],
            "file_version_evidence_objects": version_objects,
        })
    return ResolvedIdentity(
        business_date=business_date, file_version=version,
        provenance=provenance, producer_assertions=producer_assertions)


def create_delivery(marker_key: str, *, client=None, bucket: str | None = None,
                    prefix: str | None = None,
                    registry: dict[str, Feed] | None = None) -> Delivery:
    """Validate a Transport and create/read its immutable DeliveryManifest."""
    client = client or transport_contract._client()  # noqa: SLF001
    bucket = bucket or transport_contract._bucket()  # noqa: SLF001
    accepted = transport_contract.read_validated_transport(
        marker_key, client=client, bucket=bucket, prefix=prefix)
    key = manifest_key(accepted)

    def read_object(object_key: str) -> bytes:
        return client.get_object(Bucket=bucket, Key=object_key)["Body"].read()

    existing = _read_optional(key, client=client, bucket=bucket)
    if existing is not None:
        return _parse_and_verify(
            existing, key, accepted, marker_key, read_object)

    feed = resolve_transport_feed(accepted, registry)

    resolved = resolve_business_identity(accepted, feed, read_object)
    source_files = tuple(DeliverySourceFile.from_transport(item)
                         for item in accepted.files)
    delivery = Delivery(
        delivery_id=delivery_id_for(accepted),
        feed=feed.name,
        transport_id=accepted.transport_id,
        transport_source=accepted.source,
        external_feed_id=accepted.legacy_feed_id,
        business_date=resolved.business_date,
        file_version=resolved.file_version,
        received_at=accepted.uploaded_at,
        source_observed_at=accepted.source_observed_at,
        schema_version=feed.schema_version,
        completion_marker=marker_key,
        source_files=source_files,
        producer_assertions=resolved.producer_assertions,
        identity=resolved.provenance,
        feed_contract={
            "schema_version": feed.schema_version,
            "identity_sources": list(feed.delivery_identity),
            "filename_pattern": feed.filename_pattern,
            "control": ((feed.delivery or {}).get("control") or {}),
            "normalization": normalization_contract(feed),
        },
    )
    body = serialize_manifest(delivery)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body,
                          ContentType="application/json", IfNoneMatch="*")
    except Exception as exc:  # noqa: BLE001 - boto and test fake differ
        if not _is_precondition_failed(exc):
            raise
        raced = _read_optional(key, client=client, bucket=bucket)
        if raced is None:
            raise DeliveryManifestError(
                f"{key}: create-only write conflicted but no manifest can be read") from exc
        existing_delivery = _parse_and_verify(
            raced, key, accepted, marker_key, read_object)
        if serialize_manifest(existing_delivery) != body:
            raise DeliveryManifestError(
                f"{key}: concurrent creation produced a conflicting "
                f"DeliveryManifest for transport {accepted.transport_id}") from exc
        return existing_delivery
    return delivery


def read_delivery_manifest(key: str, *, client=None,
                           bucket: str | None = None) -> Delivery:
    client = client or transport_contract._client()  # noqa: SLF001
    bucket = bucket or transport_contract._bucket()  # noqa: SLF001
    body = _read_optional(key, client=client, bucket=bucket)
    if body is None:
        raise DeliveryManifestError(f"{key}: DeliveryManifest does not exist")
    return _parse_manifest(body, key)


def serialize_manifest(delivery: Delivery) -> bytes:
    return json.dumps(delivery.as_manifest(), indent=2,
                      sort_keys=True).encode("utf-8")


def _parse_and_verify(body: bytes, key: str,
                      accepted: transport_contract.Transport,
                      completion_marker: str,
                      read_object: Callable[[str], bytes]) -> Delivery:
    delivery = _parse_manifest(body, key)
    expected_key = manifest_key(accepted)
    if key != expected_key:
        raise DeliveryManifestError(
            f"{key}: manifest for transport {accepted.transport_id} must be "
            f"stored at {expected_key}")
    expected_files = tuple(DeliverySourceFile.from_transport(item)
                           for item in accepted.files)
    conflicts = []
    checks = {
        "delivery_id": (delivery.delivery_id, delivery_id_for(accepted)),
        "transport.source": (delivery.transport_source, accepted.source),
        "transport.transport_id": (delivery.transport_id, accepted.transport_id),
        "transport.external_feed_id": (delivery.external_feed_id,
                                         accepted.legacy_feed_id),
        "transport.completion_marker": (delivery.completion_marker,
                                          completion_marker),
        "timestamps.source_observed_at": (delivery.source_observed_at,
                                            accepted.source_observed_at),
        "timestamps.received_at": (delivery.received_at, accepted.uploaded_at),
        "source_files": (delivery.source_files, expected_files),
    }
    for field, (actual, expected) in checks.items():
        if actual != expected:
            conflicts.append(field)
    expected_run = accepted.producer_run_id
    actual_run = delivery.producer_assertions.get("producer_run_id")
    if actual_run != expected_run:
        conflicts.append("producer_assertions.producer_run_id")
    try:
        historical_feed = Feed(
            name=delivery.feed, description="DeliveryManifest snapshot",
            source_system="", business_key=[], columns=[],
            filename_pattern=delivery.feed_contract["filename_pattern"],
            delivery={"kind": "file",
                      "control": delivery.feed_contract.get("control") or {}},
            delivery_identity=list(
                delivery.feed_contract["identity_sources"]),
        )
        historical = resolve_business_identity(
            accepted, historical_feed, read_object)
    except Exception as exc:  # malformed snapshot or evidence disagreement
        raise DeliveryManifestError(
            f"{key}: existing DeliveryManifest identity snapshot cannot be "
            f"verified against Transport evidence: {exc}") from exc
    if delivery.business_date != historical.business_date:
        conflicts.append("business_date")
    if delivery.file_version != historical.file_version:
        conflicts.append("file_version")
    if delivery.identity != historical.provenance:
        conflicts.append("identity")
    if delivery.producer_assertions != historical.producer_assertions:
        conflicts.append("producer_assertions")
    if conflicts:
        raise DeliveryManifestError(
            f"{key}: existing DeliveryManifest conflicts with immutable "
            f"Transport evidence in: {', '.join(conflicts)}")
    return delivery


def _parse_manifest(body: bytes, key: str) -> Delivery:
    try:
        raw = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeliveryManifestError(
            f"{key}: malformed DeliveryManifest JSON: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("delivery_manifest_version") != 1:
        raise DeliveryManifestError(
            f"{key}: unsupported or missing delivery_manifest_version")
    try:
        transport = raw["transport"]
        timestamps = raw["timestamps"]
        source_files = tuple(DeliverySourceFile(
            role=item["role"], original_filename=item["original_filename"],
            object_key=item["object_key"], bytes=item["bytes"],
            sha256=item["sha256"])
            for item in raw["source_files"])
        business_date = date.fromisoformat(raw["business_date"])
        source_observed_at = _parse_timestamp(
            timestamps["source_observed_at"], key)
        received_at = _parse_timestamp(timestamps["received_at"], key)
        feed_contract = raw["feed_contract"]
        schema_version = feed_contract["schema_version"]
        identity = raw["identity"]
        assertions = raw["producer_assertions"]
        delivery = Delivery(
            delivery_id=raw["delivery_id"], feed=raw["feed"],
            transport_id=transport["transport_id"],
            transport_source=transport["source"],
            external_feed_id=transport["external_feed_id"],
            business_date=business_date, file_version=raw.get("file_version"),
            received_at=received_at, source_observed_at=source_observed_at,
            schema_version=schema_version,
            completion_marker=transport["completion_marker"],
            source_files=source_files, producer_assertions=assertions,
            identity=identity, feed_contract=feed_contract)
    except (KeyError, TypeError, ValueError) as exc:
        raise DeliveryManifestError(
            f"{key}: malformed DeliveryManifest v1: {exc}") from exc
    if (not isinstance(delivery.delivery_id, str)
            or not delivery.delivery_id.startswith("dlv_")
            or not isinstance(delivery.feed, str)
            or not delivery.feed
            or not isinstance(identity, dict)
            or not isinstance(assertions, dict)
            or not isinstance(feed_contract, dict)
            or type(delivery.file_version) not in (int, type(None))):
        raise DeliveryManifestError(f"{key}: malformed DeliveryManifest v1 values")
    return delivery


def _check_control_names(feed: Feed,
                         transport: transport_contract.Transport) -> None:
    pattern = ((feed.delivery or {}).get("control") or {}).get("pattern")
    if not pattern:
        return
    data_stems = [PurePath(item.original_filename).stem
                  for item in transport.data_files]
    for item in transport.control_files:
        if not any(re.fullmatch(pattern.format(stem=re.escape(stem)),
                                item.original_filename)
                   for stem in data_stems):
            raise IdentityResolutionError(
                f"transport {transport.transport_id}: control object "
                f"{item.original_filename!r} matches no declared data object "
                f"under feed {feed.name}'s delivery.control.pattern")


def _require_consistent(transport: transport_contract.Transport, field: str,
                        evidence: Iterable[tuple[Any, str]], label: str) -> None:
    evidence = list(evidence)
    values = {value for value, _ in evidence}
    if len(values) > 1:
        rendered = ", ".join(
            f"{value.isoformat() if isinstance(value, date) else value!r} "
            f"from {object_key}" for value, object_key in evidence)
        raise IdentityResolutionError(
            f"transport {transport.transport_id}: conflicting {field} "
            f"in {label}: {rendered}")


def _read_optional(key: str, *, client, bucket: str) -> bytes | None:
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:  # noqa: BLE001
        if transport_contract._is_missing(exc):  # noqa: SLF001
            return None
        raise


def _is_precondition_failed(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"409", "412", "ConditionalRequestConflict",
                    "PreconditionFailed"} or type(exc).__name__ == "PreconditionFailed"


def _safe_segment(value: str, label: str) -> None:
    if (not value or value in (".", "..") or value != value.strip()
            or "/" in value or "\\" in value or "\x00" in value):
        raise DeliveryError(f"{label} {value!r} is not one safe path segment")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any, key: str) -> datetime:
    if not isinstance(value, str):
        raise DeliveryManifestError(f"{key}: manifest timestamp is not text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DeliveryManifestError(f"{key}: manifest timestamp has no UTC offset")
    return parsed.astimezone(timezone.utc)
