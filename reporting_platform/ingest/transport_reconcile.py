"""Phase 6 correctness path: derive Transport pipeline progress from evidence.

This is deliberately NOT a mutable "current stage" table. Every answer here
comes from a cheap read of durable object-storage evidence -- a completion
marker, a DeliveryManifest, a NormalizationManifest -- or, for the one stage
that has no cheaper index, a bulk Raw query. See
``docs/AIRFLOW-ORCHESTRATION.md`` for why the walk is staged this way rather
than a single query.

:func:`discover_transport_progress` never raises for one bad Transport: a
transport whose evidence cannot be read is reported in ``failed`` so the
correctness path keeps making progress on every other Transport, the same
best-effort convention ``registry.deliveries.reconcile_v2`` already uses.
"""
from __future__ import annotations

from typing import Any

from reporting_platform.common.context import Feed, feeds
from reporting_platform.ingest import delivery as delivery_contract
from reporting_platform.ingest import normalization
from reporting_platform.ingest import transport as transport_contract


def _exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001 - boto and the test fake differ
        if transport_contract._is_missing(exc):  # noqa: SLF001
            return False
        raise


def discover_transport_progress(*, client=None, bucket: str | None = None,
                                registry: dict[str, Feed] | None = None,
                                cob_dates: list[str] | None = None
                                ) -> dict[str, Any]:
    """Classify every completed Transport by how far it has reached.

    Three cheap HEAD/GET reads per completed Transport -- the marker (already
    read by ``list_completed_transports``' caller elsewhere is not assumed;
    this reads it itself), the candidate DeliveryManifest, and the candidate
    NormalizationManifest -- none of which touch the original source bytes.
    Validating and hashing those only happens for a Transport this finds
    genuinely incomplete, inside ``create_delivery``/``normalize_delivery``
    themselves when the caller acts on ``needs_full_chain``.

    Returns:
        ``needs_full_chain``: transport ids with no DeliveryManifest yet, or a
            DeliveryManifest but no NormalizationManifest yet. Resuming either
            case means re-entering the pipeline from the top; both domain
            operations are create-once and idempotent, so this costs one
            validation/hash of a Transport that turns out to need it and a
            no-op read for a Transport that does not.
        ``candidates_by_feed``: ``{feed: [(delivery_id, transport_id), ...]}``
            for every Transport that has reached NormalizationManifest but
            whose Raw ingestion state this function cannot itself determine
            -- Raw is the only ledger for that. The caller resolves these
            with one bulk Raw query per feed (:func:`raw_pending`).
        ``failed``: ``{"marker": ..., "error": ...}`` for evidence this could
            not read or parse. Never raised: one bad Transport must not stop
            reconciliation from making progress on the rest.
        ``marker_keys``: ``{transport_id: marker_key}`` for every Transport
            named anywhere else in this report -- callers that need to
            trigger/replay a specific Transport (`transport_ingest`) address
            it by marker key, not by re-deriving one from a bare id.

    ``cob_dates``, when given, bounds the underlying
    ``list_completed_transports`` scan to those COB partitions only -- see
    that function's docstring for why this is the shape a periodic
    reconciliation DAG should use, and why ``None`` (the default) still means
    "the whole received/ prefix, v1 markers included."
    """
    client = client or transport_contract._client()  # noqa: SLF001
    bucket = bucket or transport_contract._bucket()  # noqa: SLF001
    registry = feeds() if registry is None else registry

    needs_full_chain: list[str] = []
    candidates_by_feed: dict[str, list[tuple[str, str]]] = {}
    failed: list[dict[str, str]] = []
    marker_keys: dict[str, str] = {}

    for marker_key in transport_contract.list_completed_transports(
            client=client, bucket=bucket, cob_dates=cob_dates):
        try:
            parsed = transport_contract.read_transport(
                marker_key, client=client, bucket=bucket)
            marker_keys[parsed.transport_id] = marker_key
            delivery_key = delivery_contract.manifest_key(parsed)
            if not _exists(client, bucket, delivery_key):
                needs_full_chain.append(parsed.transport_id)
                continue
            deliv = delivery_contract.read_delivery_manifest(
                delivery_key, client=client, bucket=bucket)
            if deliv.feed not in registry:
                raise ValueError(f"unknown Feed {deliv.feed!r}")
            norm_key = normalization.manifest_key(deliv)
            if not _exists(client, bucket, norm_key):
                needs_full_chain.append(parsed.transport_id)
                continue
            candidates_by_feed.setdefault(deliv.feed, []).append(
                (deliv.delivery_id, parsed.transport_id))
        except Exception as exc:  # noqa: BLE001
            failed.append({"marker": marker_key,
                           "error": f"{type(exc).__name__}: {exc}"})

    return {
        "needs_full_chain": sorted(set(needs_full_chain)),
        "candidates_by_feed": {
            feed: sorted(set(pairs))
            for feed, pairs in candidates_by_feed.items()},
        "failed": failed,
        "marker_keys": marker_keys,
    }


def raw_pending(candidates_by_feed: dict[str, list[tuple[str, str]]],
               delivered_by_feed: dict[str, list[str]] | dict[str, set[str]]
               ) -> list[str]:
    """Transport ids whose Delivery is normalized but absent from Raw.

    Pure set difference -- no I/O -- so the caller does exactly one Raw query
    per feed (:func:`reporting_platform.ingest.ingest_feed.raw_delivered_ids`,
    via ``spark_task raw-delivery-ids``) and hands the result here,
    rather than one Spark call per candidate Delivery.
    """
    pending: list[str] = []
    for feed_name, candidates in candidates_by_feed.items():
        delivered = set(delivered_by_feed.get(feed_name, ()))
        pending.extend(transport_id for delivery_id, transport_id in candidates
                       if delivery_id not in delivered)
    return sorted(set(pending))
