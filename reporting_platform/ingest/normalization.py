"""DeliveryManifest v1 -> rebuildable NormalizationManifest v2.

This is deliberately separate from :mod:`reporting_platform.ingest.normalize`.
That module is the legacy Landing -> Ready manifest v1 path and retains its
filename parsing and sibling-control gate until Raw ingestion migrates.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
import json
from pathlib import PurePosixPath
import re
from typing import Any
import zipfile

from reporting_platform.common.context import Feed, feeds
from reporting_platform.ingest import delivery as delivery_contract
from reporting_platform.ingest import transport as transport_contract


MANIFEST_VERSION = 2
MANIFEST_NAME = "normalization-manifest.json"
NORMALIZER_VERSIONS = {"file": "file/v2", "archive": "archive/v2"}


class NormalizationError(ValueError):
    """A Delivery cannot produce or agree with its derived Ready v2 state."""


class DerivedObjectConflict(NormalizationError):
    """A deterministic Ready key already contains different bytes."""


@dataclass(frozen=True)
class NormalizationResult:
    key: str
    manifest: dict[str, Any]


def ready_prefix(delivery: delivery_contract.Delivery) -> str:
    _safe_segment(delivery.feed, "feed")
    _safe_segment(delivery.delivery_id, "delivery id")
    return f"ready/{delivery.feed}/{delivery.delivery_id}/"


def manifest_key(delivery: delivery_contract.Delivery) -> str:
    return f"{ready_prefix(delivery)}{MANIFEST_NAME}"


def serialize_manifest(manifest: dict[str, Any]) -> bytes:
    return json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")


def read_normalization_manifest(key: str, *, client, bucket: str) -> dict[str, Any]:
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        raw = json.loads(body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - clients expose different errors
        raise NormalizationError(f"{key}: cannot read NormalizationManifest: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("normalization_manifest_version") != 2:
        raise NormalizationError(
            f"{key}: unsupported or missing normalization_manifest_version")
    required = {"delivery_id", "feed", "delivery_manifest", "business_date",
                "received_at", "schema_version", "normalizer", "format",
                "parts", "checksum_objects", "source_object",
                "normalization_contract", "contract_source"}
    missing = sorted(required - set(raw))
    if missing:
        raise NormalizationError(f"{key}: missing fields: {', '.join(missing)}")
    return raw


def normalize_delivery(delivery_manifest_key: str, *, client=None,
                       bucket: str | None = None,
                       registry: dict[str, Feed] | None = None,
                       write: bool = True) -> NormalizationResult:
    """Normalize immutable Delivery evidence without entering Landing.

    Existing deterministic objects are accepted only when byte-identical.
    Therefore retries, recovery from a partial archive extraction, and a full
    Ready deletion/rebuild all have the same result.
    """
    client = client or transport_contract._client()  # noqa: SLF001
    bucket = bucket or transport_contract._bucket()  # noqa: SLF001
    delivery = delivery_contract.read_delivery_manifest(
        delivery_manifest_key, client=client, bucket=bucket)
    key = manifest_key(delivery)
    existing = _read_optional_manifest(key, client=client, bucket=bucket)
    contract, contract_source = _contract(delivery, registry, existing)
    data_files = [item for item in delivery.source_files if item.role == "data"]
    if len(data_files) != 1:
        raise NormalizationError(
            f"{delivery_manifest_key}: Normalization v2 requires exactly one "
            f"data source object; found {len(data_files)}")
    source = data_files[0]
    kind = contract["kind"]
    _verify_source(source, client=client, bucket=bucket,
                   verify_hash=(kind == "file"))
    if kind == "file":
        parts = [{
            "object_key": source.object_key,
            "bytes": source.bytes,
            "source": {"original_filename": source.original_filename},
            "materialized": False,
        }]
    elif kind == "archive":
        parts = _archive_parts(
            delivery, source, contract, client=client, bucket=bucket,
            write=write)
    else:
        raise NormalizationError(
            f"{delivery_manifest_key}: unsupported delivery kind {kind!r}")

    assertions = delivery.producer_assertions
    manifest: dict[str, Any] = {
        "normalization_manifest_version": MANIFEST_VERSION,
        "delivery_id": delivery.delivery_id,
        "feed": delivery.feed,
        "delivery_manifest": delivery_manifest_key,
        "business_date": delivery.business_date.isoformat(),
        "received_at": _timestamp(delivery.received_at),
        "schema_version": delivery.schema_version,
        "normalizer": NORMALIZER_VERSIONS[kind],
        "format": contract["format"],
        "normalization_contract": contract,
        "contract_source": contract_source,
        "parts": parts,
        "checksum_objects": [source.object_key],
        "declared_row_count": assertions.get("row_count"),
        "declared_md5": assertions.get("md5"),
        "source_object": source.object_key,
    }
    if delivery.file_version is not None:
        manifest["file_version"] = delivery.file_version
    if existing is not None and serialize_manifest(existing) != serialize_manifest(manifest):
        raise DerivedObjectConflict(
            f"{key}: existing NormalizationManifest conflicts with Delivery evidence")
    if write:
        _put_identical(key, serialize_manifest(manifest), client=client,
                       bucket=bucket, content_type="application/json")
        # An index failure cannot invalidate the object-store evidence.
        try:
            from reporting_platform.registry.deliveries import register_v2_quietly
            register_v2_quietly(delivery, manifest, delivery_manifest_key,
                                client=client, bucket=bucket, registry=registry)
        except Exception:  # pragma: no cover - import itself is defensive
            pass
    return NormalizationResult(key=key, manifest=manifest)


def _contract(delivery: delivery_contract.Delivery,
              registry: dict[str, Feed] | None,
              existing: dict[str, Any] | None) -> tuple[dict[str, Any], str]:
    if existing is not None:
        if (existing["delivery_id"] != delivery.delivery_id
                or existing["feed"] != delivery.feed):
            raise DerivedObjectConflict(
                f"{manifest_key(delivery)}: existing manifest names another Delivery")
        recorded = existing["normalization_contract"]
        _validate_contract(recorded, "existing NormalizationManifest")
        return recorded, existing["contract_source"]
    snapshotted = delivery.feed_contract.get("normalization")
    if snapshotted is not None:
        _validate_contract(snapshotted, "DeliveryManifest snapshot")
        return snapshotted, "delivery_manifest"
    current = (feeds() if registry is None else registry).get(delivery.feed)
    if current is None:
        raise NormalizationError(
            f"{delivery.delivery_id}: legacy Phase 2 DeliveryManifest has no "
            f"normalization snapshot and current Feed {delivery.feed!r} is unavailable")
    fallback = delivery_contract.normalization_contract(current)
    _validate_contract(fallback, "current Feed compatibility fallback")
    return fallback, "current_feed_compatibility"


def _read_optional_manifest(key: str, *, client, bucket: str) -> dict[str, Any] | None:
    try:
        client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # missing is the ordinary first-normalization case
        if transport_contract._is_missing(exc):  # noqa: SLF001
            return None
        raise
    return read_normalization_manifest(key, client=client, bucket=bucket)


def _validate_contract(contract: Any, source: str) -> None:
    if not isinstance(contract, dict):
        raise NormalizationError(f"{source}: normalization contract is not an object")
    kind = contract.get("kind")
    if kind not in NORMALIZER_VERSIONS:
        raise NormalizationError(f"{source}: unsupported normalization kind {kind!r}")
    if not isinstance(contract.get("format"), dict):
        raise NormalizationError(f"{source}: format is missing or malformed")
    if kind == "archive" and not isinstance(contract.get("member_pattern"), str):
        raise NormalizationError(f"{source}: archive member_pattern is missing")


def _verify_source(source: delivery_contract.DeliverySourceFile, *, client,
                   bucket: str, verify_hash: bool) -> None:
    try:
        head = client.head_object(Bucket=bucket, Key=source.object_key)
    except Exception as exc:  # noqa: BLE001
        raise NormalizationError(
            f"{source.object_key}: Delivery source object is unavailable: {exc}") from exc
    if int(head["ContentLength"]) != source.bytes:
        raise NormalizationError(
            f"{source.object_key}: source size {head['ContentLength']} conflicts "
            f"with DeliveryManifest size {source.bytes}")
    if verify_hash:
        body = client.get_object(Bucket=bucket, Key=source.object_key)["Body"].read()
        if hashlib.sha256(body).hexdigest() != source.sha256:
            raise NormalizationError(
                f"{source.object_key}: source sha256 conflicts with DeliveryManifest")


def _archive_parts(delivery: delivery_contract.Delivery,
                   source: delivery_contract.DeliverySourceFile,
                   contract: dict[str, Any], *, client, bucket: str,
                   write: bool) -> list[dict[str, Any]]:
    body = client.get_object(Bucket=bucket, Key=source.object_key)["Body"].read()
    if hashlib.sha256(body).hexdigest() != source.sha256:
        raise NormalizationError(
            f"{source.object_key}: archive sha256 conflicts with DeliveryManifest")
    try:
        member_re = re.compile(contract["member_pattern"])
    except re.error as exc:
        raise NormalizationError(f"invalid snapshotted member_pattern: {exc}") from exc
    selected: list[tuple[str, bytes]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            for info in sorted(zf.infolist(), key=lambda item: item.filename):
                if info.is_dir() or not member_re.fullmatch(info.filename):
                    continue
                _safe_member_name(info.filename)
                selected.append((info.filename, zf.read(info)))
    except zipfile.BadZipFile as exc:
        raise NormalizationError(f"{source.object_key}: invalid zip archive") from exc
    if not selected:
        raise NormalizationError(
            f"{source.object_key}: archive contains no member matching "
            f"{contract['member_pattern']!r}")
    width = max(4, len(str(len(selected))))
    parts: list[dict[str, Any]] = []
    prefix = ready_prefix(delivery)
    for number, (member, member_body) in enumerate(selected, 1):
        suffix = PurePosixPath(member).suffix
        key = f"{prefix}part-{number:0{width}d}{suffix}"
        if write:
            _put_identical(key, member_body, client=client, bucket=bucket,
                           content_type="application/octet-stream")
        parts.append({
            "object_key": key,
            "bytes": len(member_body),
            "source": {"archive_member": member},
            "materialized": True,
        })
    return parts


def _put_identical(key: str, body: bytes, *, client, bucket: str,
                   content_type: str) -> None:
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body,
                          ContentType=content_type, IfNoneMatch="*")
        return
    except Exception as exc:  # noqa: BLE001
        try:
            existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception:
            raise NormalizationError(
                f"{key}: create-only write failed and existing bytes cannot be read") from exc
        if existing != body:
            raise DerivedObjectConflict(
                f"{key}: deterministic Ready object already exists with different bytes") from exc


def _safe_member_name(name: str) -> None:
    if name in ("", ".", "..") or "/" in name or "\\" in name:
        raise NormalizationError(
            f"archive member {name!r} contains a path; only flat members are allowed")


def _safe_segment(value: str, label: str) -> None:
    if not value or value in (".", "..") or "/" in value or "\\" in value:
        raise NormalizationError(f"unsafe {label}: {value!r}")


def _timestamp(value) -> str:
    return value.isoformat().replace("+00:00", "Z")
