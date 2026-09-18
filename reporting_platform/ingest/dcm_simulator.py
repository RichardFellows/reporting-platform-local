"""Development producer for the DCM -> S3 transport contract.

This is not an acquisition service. It has no watcher or poll loop; one call
publishes caller-supplied local files under one stable TransportID.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Sequence

from reporting_platform.ingest import transport


class TransportConflictError(transport.TransportContractError):
    """A TransportID or object key already holds different evidence."""


def simulate_transport(*, transport_id: str, legacy_feed_id: str,
                       source_observed_at: str | datetime,
                       files: Sequence[tuple[str, str | Path]],
                       producer_run_id: str | None = None,
                       source: str = "DCM",
                       uploaded_at: str | datetime | None = None,
                       client=None, bucket: str | None = None,
                       prefix: str | None = None) -> transport.Transport:
    """Publish source objects and then ``_COMPLETE.json``, safely retryable."""
    client = client or transport._client()  # noqa: SLF001 - shared S3 boundary
    bucket = bucket or transport._bucket()  # noqa: SLF001
    marker_key = transport.complete_key(transport_id, prefix)

    local: list[tuple[str, Path]] = []
    for role, value in files:
        path = Path(value)
        local.append((role, path))
    if not local:
        raise transport.TransportContractError(
            f"transport {transport_id}: at least one source file is required")
    # The contract is a set of evidence, not caller argument order. A stable
    # data-then-control ordering also models DCM's required upload sequence.
    local.sort(key=lambda item: (0 if item[0] == "data" else 1,
                                 item[1].name))

    declarations = []
    for role, path in local:
        declarations.append({
            "role": role,
            "original_filename": path.name,
            "object_key": f"{transport.transport_prefix(transport_id, prefix)}"
                          f"{path.name}",
            "bytes": path.stat().st_size,
            "sha256": _sha256_path(path),
        })
    observed = _as_timestamp(source_observed_at)
    uploaded = _as_timestamp(uploaded_at or datetime.now(timezone.utc))
    raw = {
        "transport_contract_version": transport.CONTRACT_VERSION,
        "transport_id": transport_id,
        "source": source,
        "legacy_feed_id": legacy_feed_id,
        "source_observed_at": observed,
        "uploaded_at": uploaded,
        "files": declarations,
    }
    if producer_run_id is not None:
        raw["producer_run_id"] = producer_run_id
    candidate = transport.parse_transport(
        json.dumps(raw), marker_key, prefix)

    if _exists(client, bucket, marker_key):
        existing = transport.read_validated_transport(
            marker_key, client=client, bucket=bucket, prefix=prefix)
        _require_same(existing, candidate)
        return existing

    paths = {declaration["object_key"]: path
             for declaration, (_, path) in zip(declarations, local)}
    missing: list[str] = []
    for file in candidate.files:
        if not _exists(client, bucket, file.object_key):
            missing.append(file.object_key)
            continue
        _require_object(client, bucket, candidate.transport_id, file)

    # Source objects first. Conditional creation closes the check/write race;
    # a concurrent identical retry is accepted after verification.
    for key in missing:
        try:
            with paths[key].open("rb") as body:
                client.put_object(Bucket=bucket, Key=key, Body=body,
                                  IfNoneMatch="*")
        except Exception as exc:  # noqa: BLE001
            if not _is_precondition_failed(exc):
                raise
            file = next(f for f in candidate.files if f.object_key == key)
            _require_object(client, bucket, candidate.transport_id, file)

    transport.validate_transport(candidate, client=client, bucket=bucket)
    marker_body = transport.serialize_transport(candidate)
    try:
        client.put_object(Bucket=bucket, Key=marker_key, Body=marker_body,
                          ContentType="application/json", IfNoneMatch="*")
    except Exception as exc:  # noqa: BLE001
        if not _is_precondition_failed(exc):
            raise
        existing = transport.read_validated_transport(
            marker_key, client=client, bucket=bucket, prefix=prefix)
        _require_same(existing, candidate)
        return existing
    return candidate


def _require_same(existing: transport.Transport,
                  candidate: transport.Transport) -> None:
    # uploaded_at describes the first successful publication and naturally
    # differs on a retry. Every producer/source observation must remain exact.
    left = existing.as_dict()
    right = candidate.as_dict()
    left.pop("uploaded_at")
    right.pop("uploaded_at")
    left["files"] = sorted(left["files"], key=lambda item: item["object_key"])
    right["files"] = sorted(right["files"], key=lambda item: item["object_key"])
    if left != right:
        raise TransportConflictError(
            f"transport {candidate.transport_id}: TransportID already holds "
            f"different evidence")


def _require_object(client, bucket: str, transport_id: str,
                    file: transport.TransportFile) -> None:
    try:
        transport._validate_file_evidence(  # noqa: SLF001
            transport_id, file, client=client, bucket=bucket)
    except transport.TransportEvidenceError as exc:
        raise TransportConflictError(str(exc)) from exc


def _exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001
        if transport._is_missing(exc):  # noqa: SLF001
            return False
        raise


def _is_precondition_failed(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"409", "412", "ConditionalRequestConflict",
                    "PreconditionFailed"} or \
        type(exc).__name__ == "PreconditionFailed"


def _as_timestamp(value: str | datetime) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise transport.TransportContractError(
                "simulator timestamps must include a UTC offset")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
