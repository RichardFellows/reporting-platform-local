"""The one reusable implementation of Transport publication semantics.

Used both by the local RPL simulator (``scripts/simulate_dcm_transport.py``)
and by :mod:`reporting_transport.cli` for a real DCM invocation -- there is
exactly one algorithm here, not a simulated one and a separate "real" one.

DCM supplies only what it authoritatively knows: legacy feed identity, its own
run identity, COB date, source system, its observation time, and the source
files themselves. Everything else -- TransportID, S3 keys, byte counts,
SHA-256, upload timestamp -- is derived here. See ``docs/TRANSPORT-CONTRACT.md``
for the full field-by-field contract and the required publish sequence this
function implements.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Sequence

from reporting_transport import contract, storage
from reporting_transport.sources import source_reader
from reporting_transport.contract import (
    Transport, TransportConflictError, TransportContractError,
    TransportEvidenceError,
)
from reporting_transport.storage import StorageConfig, UploadTuning, _HashingReader  # noqa: F401 - re-exported for tests

# Matches contract._SEGMENT: legacy_feed_id/producer_run_id/source become part
# of the derived TransportID, so they must already be safe path-segment text.
# A hash-based encoding was considered instead (it would tolerate arbitrary
# input) and rejected: the contract's own example format
# (``dcm-1234-849217``) is meant to stay human-debuggable in an S3 console/DCM
# operator's log, which a digest would defeat, and DCM's own identifiers are
# expected to already be simple tokens.
_TOKEN = contract._SEGMENT  # noqa: SLF001 - same rule, not worth re-declaring


@dataclass(frozen=True)
class PublishResult:
    """The outcome of one :func:`publish_transport_result` call.

    ``newly_published`` is the ground truth for "newly published" vs.
    "already published/idempotent retry" -- the CLI reports it directly
    rather than inferring it from timestamps, which cannot reliably tell the
    two apart (a fixed ``--uploaded-at`` makes them indistinguishable by
    value alone).
    """

    transport: Transport
    newly_published: bool


def publish_transport(*, legacy_feed_id: str, producer_run_id: str,
                      cob_date: str, source_system: str,
                      source_observed_at: str | datetime,
                      files: Sequence[tuple[str, str | Path]],
                      source: str = "DCM",
                      uploaded_at: str | datetime | None = None,
                      config: StorageConfig | None = None,
                      client=None, bucket: str | None = None,
                      prefix: str | None = None,
                      upload: UploadTuning | None = None) -> Transport:
    """Publish one DCM transfer's evidence, then ``_COMPLETE.json``, last.

    Idempotent and safely retryable: the same ``(source, legacy_feed_id,
    producer_run_id)`` always derives the same TransportID, and calling this
    again with byte-identical files returns the existing accepted Transport
    without writing anything. Different bytes or metadata under that same
    identity raises :class:`TransportConflictError` rather than overwriting
    evidence. A genuine correction must use a new ``producer_run_id``.

    Returns the accepted :class:`Transport`. Callers that also need to know
    whether this call was the one that actually created the marker (rather
    than an idempotent retry) should call :func:`publish_transport_result`.
    """
    return publish_transport_result(
        legacy_feed_id=legacy_feed_id, producer_run_id=producer_run_id,
        cob_date=cob_date, source_system=source_system,
        source_observed_at=source_observed_at, files=files, source=source,
        uploaded_at=uploaded_at, config=config, client=client,
        bucket=bucket, prefix=prefix, upload=upload).transport


def publish_transport_result(*, legacy_feed_id: str, producer_run_id: str,
                             cob_date: str, source_system: str,
                             source_observed_at: str | datetime,
                             files: Sequence[tuple[str, str | Path]],
                             source: str = "DCM",
                             uploaded_at: str | datetime | None = None,
                             config: StorageConfig | None = None,
                             client=None,
                             bucket: str | None = None,
                             prefix: str | None = None,
                             upload: UploadTuning | None = None) -> PublishResult:
    """Same as :func:`publish_transport`, also reporting new-vs-retry.

    ``config`` is only resolved from the environment (``StorageConfig.
    from_env()``) when actually needed to fill in a missing ``client``,
    ``bucket`` or ``prefix`` -- a caller supplying all three directly (every
    test in this repo; a caller with its own client construction) never
    touches the environment at all.

    ``upload`` tunes multipart thresholds/concurrency and checksum
    verification strategy (``UploadTuning``); it is resolved from the
    environment the same way when not supplied directly, independently of
    ``config`` -- a caller that passes its own ``client``/``bucket`` and
    never builds a ``StorageConfig`` still gets multipart/checksum
    configuration from the environment rather than silently always using
    hard-coded defaults.
    """
    if config is None and (client is None or bucket is None):
        config = StorageConfig.from_env()
    client = client if client is not None else storage.build_client(config)
    bucket = bucket if bucket is not None else config.bucket
    if prefix is None:
        prefix = (config.received_prefix if config is not None
                 else contract.DEFAULT_RECEIVED_PREFIX)
    if upload is None:
        upload = config.upload if config is not None else UploadTuning.from_env()

    source = _require_token(source, "source")
    legacy_feed_id = _require_token(legacy_feed_id, "legacy_feed_id")
    producer_run_id = _require_token(producer_run_id, "producer_run_id")
    cob_date = contract.validate_cob_date(cob_date, "cob_date")
    source_system = contract.validate_source_system(source_system,
                                                     "source_system")
    transport_id = f"{source.lower()}-{legacy_feed_id}-{producer_run_id}"
    contract.validate_transport_id(transport_id)

    local = _ordered_local_files(files, transport_id)
    prefix_str = contract.transport_prefix(cob_date, source_system,
                                           transport_id, prefix)
    marker_key = contract.complete_key(cob_date, source_system, transport_id,
                                       prefix)
    observed = _as_timestamp(source_observed_at)
    uploaded = _as_timestamp(uploaded_at or datetime.now(timezone.utc))

    # Publish (or reuse) every declared object. storage.publish_object()
    # hashes exactly what it streams -- one local read for a fresh upload,
    # one for a pre-existing object being compared against a retry -- and
    # confirms durable storage matches that hash BEFORE returning: cheaply,
    # via a server-computed SHA-256 checksum, when the backend supports one
    # usable for this object; via the original full GET + rehash otherwise.
    # This single per-file confirmation is what the old code's separate,
    # unconditional second verification pass over every file used to do --
    # removed here because it is now redundant, not because the guarantee it
    # provided (the marker can never describe different bytes than what is
    # durably stored, however the source file behaved after being read) is
    # weakened: publish_object() provides exactly that guarantee itself, per
    # file, every time. See "Large-file upload and verification strategy" in
    # docs/TRANSPORT-CONTRACT.md.
    declared: list[contract.TransportFile] = []
    for role, path, name in local:
        object_key = f"{prefix_str}{name}"
        try:
            published = storage.publish_object(
                client, bucket, object_key, path, upload=upload,
                label=f"transport {transport_id}")
        except TransportEvidenceError as exc:
            raise TransportConflictError(str(exc)) from exc
        declared.append(contract.TransportFile(
            role=role, original_filename=name, object_key=object_key,
            bytes=published.bytes, sha256=published.sha256))

    raw = {
        "transport_contract_version": contract.CONTRACT_VERSION,
        "transport_id": transport_id,
        "source": source,
        "legacy_feed_id": legacy_feed_id,
        "producer_run_id": producer_run_id,
        "cob_date": cob_date,
        "source_system": source_system,
        "source_observed_at": observed,
        "uploaded_at": uploaded,
        "files": [f.as_dict() for f in declared],
    }
    candidate = contract.parse_transport(json.dumps(raw), marker_key, prefix)

    if storage.object_exists(client, bucket, marker_key):
        existing = _read_marker(client, bucket, marker_key, prefix)
        _require_same(existing, candidate)
        return PublishResult(transport=existing, newly_published=False)

    marker_body = contract.serialize_transport(candidate)
    try:
        client.put_object(Bucket=bucket, Key=marker_key, Body=marker_body,
                          ContentType="application/json", IfNoneMatch="*")
    except Exception as exc:  # noqa: BLE001 - boto and the test fake differ
        if not storage.is_precondition_failed(exc):
            raise
        existing = _read_marker(client, bucket, marker_key, prefix)
        _require_same(existing, candidate)
        return PublishResult(transport=existing, newly_published=False)
    return PublishResult(transport=candidate, newly_published=True)


def _read_marker(client, bucket: str, marker_key: str,
                 prefix: str | None) -> Transport:
    body = client.get_object(Bucket=bucket, Key=marker_key)["Body"].read()
    return contract.parse_transport(body, marker_key, prefix)


def _require_same(existing: Transport, candidate: Transport) -> None:
    # uploaded_at describes the first successful publication and naturally
    # differs on a retry; every producer/source observation must still match
    # exactly.
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


def _ordered_local_files(
        files: Sequence[tuple[str, str | Path]],
        transport_id: str) -> list[tuple[str, Path, str]]:
    if not files:
        raise TransportContractError(
            f"transport {transport_id}: at least one source file is required")
    local: list[tuple[str, Path, str]] = []
    names: set[str] = set()
    for role, value in files:
        if role not in contract.ROLES:
            raise TransportContractError(
                f"transport {transport_id}: unsupported file role {role!r}")
        path = source_reader(value)
        if not path.is_file():
            raise TransportContractError(
                f"transport {transport_id}: source file not found: {path}")
        if path.name in names:
            raise TransportContractError(
                f"transport {transport_id}: duplicate original filename "
                f"{path.name!r}")
        names.add(path.name)
        local.append((role, path, path.name))
    if not any(role == "data" for role, _, _ in local):
        raise TransportContractError(
            f"transport {transport_id}: at least one data object is required")
    # The contract is a set of evidence, not caller argument order. A stable
    # data-then-control ordering also models DCM's required upload sequence.
    local.sort(key=lambda item: (0 if item[0] == "data" else 1, item[2]))
    return local


def _require_token(value: str, label: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise TransportContractError(
            f"{label} {value!r} must be one token of letters, digits, '.', "
            f"'_' or '-'")
    return value


def _as_timestamp(value: str | datetime) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise TransportContractError(
                "publisher timestamps must include a UTC offset")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value
