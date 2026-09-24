"""RPL's consumer side of the DCM -> S3 transport evidence contract.

The wire contract itself (parsing, validation, path rules, versions 1 and 2)
lives in :mod:`reporting_transport.contract` and is shared, unmodified, with
the standalone producer package -- there is exactly one implementation of
what a marker means. This module adds only what belongs to the reporting
platform specifically: its own S3 client/bucket wiring (the same one every
other ``ingest/`` module uses), re-verification of accepted evidence, and
discovery (`list_completed_transports`).

Transport is deliberately not Delivery. Nothing here writes ``landing/``,
creates a delivery id, normalizes an archive, updates the registry, or changes
raw provenance -- see ``docs/DELIVERY-CONTRACT.md``.
"""
from __future__ import annotations

import os
from typing import Iterable

from reporting_transport import storage as _storage
from reporting_transport.contract import (  # noqa: F401 - re-exported API
    CONTRACT_VERSION, COMPLETE_FILENAME, ROLES, SUPPORTED_VERSIONS,
    Transport, TransportConflictError, TransportContractError,
    TransportEvidenceError, TransportFile, TransportStorageError,
    complete_key, complete_key_v1, parse_transport, serialize_transport,
    transport_prefix, transport_prefix_v1, validate_prefix,
)


def received_prefix(value: str | None = None) -> str:
    """The configured transport evidence prefix, RPL's env var included."""
    raw = value if value is not None else os.environ.get(
        "REPORTING_RECEIVED_PREFIX", "received")
    return validate_prefix(raw)


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

    Only reads object storage and returns the same immutable value on
    success, so repeated validation is side-effect free.
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
                              prefix: str | None = None,
                              cob_dates: Iterable[str] | None = None
                              ) -> list[str]:
    """List completion-marker keys; unmarked source objects are invisible.

    With ``cob_dates`` omitted, this lists the WHOLE ``received/`` prefix --
    both v1's flat layout and v2's ``cob_date=.../source_system=.../`` layout
    -- exactly as Phase 1/6 always did. That remains correct but is an
    unbounded, whole-history scan; see ``docs/AIRFLOW-ORCHESTRATION.md``,
    "Reconciliation scale (v2)" for why the periodic reconciliation DAG no
    longer calls it this way.

    With ``cob_dates`` given, only those v2 ``cob_date=<date>/`` partitions
    are listed -- a bounded read whose cost no longer grows with total
    Transport history. v1 markers have no ``cob_date`` in their path and are
    therefore never found this way; a full sweep (the default, cob_dates
    omitted) remains the only way to discover them, which is why v1 support
    is a permanent read path, not a one-time migration window.
    """
    client = client or _client()
    bucket = bucket or _bucket()
    root = f"{received_prefix(prefix)}/"
    if cob_dates is None:
        return _list_markers_under(client, bucket, root)
    keys: list[str] = []
    for cob_date in cob_dates:
        keys.extend(_list_markers_under(client, bucket, f"{root}cob_date={cob_date}/"))
    return sorted(set(keys))


def _list_markers_under(client, bucket: str, root_prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=root_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(f"/{COMPLETE_FILENAME}"):
                keys.append(key)
    return sorted(set(keys))


def _validate_file_evidence(transport_id: str, file: TransportFile, *,
                            client, bucket: str) -> None:
    _storage.verify_object_evidence(
        client, bucket, file.object_key, expected_bytes=file.bytes,
        expected_sha256=file.sha256, label=f"transport {transport_id}")


def _is_missing(exc: Exception) -> bool:
    return _storage.is_missing(exc)


def _client():
    from reporting_platform.ingest.arrival import _client as arrival_client
    return arrival_client()


def _bucket() -> str:
    from reporting_platform.ingest.arrival import _bucket as arrival_bucket
    return arrival_bucket()
