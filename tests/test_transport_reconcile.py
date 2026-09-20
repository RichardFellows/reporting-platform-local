"""Phase 6 correctness path: staged Transport progress from durable evidence.

Mirrors the fixture style of test_delivery.py / test_normalization_v2.py --
FakeS3, no Spark, no Airflow -- and drives the REAL chain (create_delivery
then normalize_delivery) rather than hand-installing manifests, because what
this module classifies is exactly "how far did the real chain get".
"""
from __future__ import annotations

import hashlib
import json

from reporting_platform.common.context import Feed
from reporting_platform.ingest import delivery, normalization, transport
from reporting_platform.ingest.transport_reconcile import (
    discover_transport_progress, raw_pending,
)
from tests.fakes3 import FakeS3


def _feed(*, name="qa_happy_position", external_id="1234",
          pattern=r"positions_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv"):
    return Feed(
        name=name, description="test", source_system="QA",
        filename_pattern=pattern, business_key=["id"],
        columns=["id", "value"],
        source_identifiers={"DCM": external_id} if external_id else {},
        delivery_identity=["filename"], delivery={"kind": "file"},
    )


def _put_transport(s3, transport_id, *, external_id="1234",
                   filename="positions_20260917.csv", body=b"id,value\n1,x\n"):
    object_key = f"received/{transport_id}/{filename}"
    s3.put(object_key, body)
    raw = {
        "transport_contract_version": 1,
        "transport_id": transport_id,
        "source": "DCM",
        "legacy_feed_id": external_id,
        "source_observed_at": "2026-09-17T05:42:17Z",
        "uploaded_at": "2026-09-17T05:43:02Z",
        "files": [{
            "role": "data", "original_filename": filename,
            "object_key": object_key, "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        }],
    }
    marker = f"received/{transport_id}/_COMPLETE.json"
    s3.put(marker, json.dumps(raw, sort_keys=True))
    return marker


def _create(s3, marker, fd):
    return delivery.create_delivery(
        marker, client=s3, bucket="lakehouse", registry={fd.name: fd})


def _normalize(s3, delivery_manifest_key, fd):
    return normalization.normalize_delivery(
        delivery_manifest_key, client=s3, bucket="lakehouse",
        registry={fd.name: fd})


def _discover(s3, fd):
    return discover_transport_progress(
        client=s3, bucket="lakehouse", registry={fd.name: fd})


# --------------------------------------------------------------- staged progress
def test_transport_only_needs_the_full_chain():
    s3, fd = FakeS3(), _feed()
    _put_transport(s3, "t-1")
    report = _discover(s3, fd)
    assert report["needs_full_chain"] == ["t-1"]
    assert report["candidates_by_feed"] == {}
    assert report["failed"] == []


def test_delivery_without_normalization_still_needs_the_full_chain():
    """A DeliveryManifest alone is resumed from the top, not half-way.

    create_delivery is create-once and idempotent, so re-entering the chain
    for a Transport at this stage costs one re-validation, not a duplicate
    Delivery -- see docs/AIRFLOW-ORCHESTRATION.md#reconciliation.
    """
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3, "t-2")
    _create(s3, marker, fd)
    report = _discover(s3, fd)
    assert report["needs_full_chain"] == ["t-2"]
    assert report["candidates_by_feed"] == {}


def test_normalized_delivery_is_a_raw_candidate_not_a_full_chain_replay():
    s3, fd = FakeS3(), _feed()
    marker = _put_transport(s3, "t-3")
    deliv = _create(s3, marker, fd)
    result = _normalize(s3, delivery.manifest_key(
        transport.read_transport(marker, client=s3, bucket="lakehouse")), fd)
    assert result.key  # sanity: normalization actually produced something

    report = _discover(s3, fd)
    assert report["needs_full_chain"] == []
    assert report["candidates_by_feed"] == {fd.name: [(deliv.delivery_id, "t-3")]}


def test_fully_ingested_transport_is_not_reported_pending_by_raw_pending():
    candidates = {"qa_happy_position": [("dlv_aaa", "t-4")]}
    delivered = {"qa_happy_position": {"dlv_aaa"}}
    assert raw_pending(candidates, delivered) == []


def test_normalized_but_uningested_transport_is_reported_pending():
    candidates = {"qa_happy_position": [("dlv_aaa", "t-4")]}
    delivered: dict[str, set[str]] = {"qa_happy_position": set()}
    assert raw_pending(candidates, delivered) == ["t-4"]


def test_raw_pending_never_confuses_one_feeds_delivered_set_with_anothers():
    candidates = {"fo_trade": [("dlv_aaa", "t-a")],
                  "ref_counterparty": [("dlv_bbb", "t-b")]}
    delivered = {"fo_trade": {"dlv_aaa"}}  # ref_counterparty absent entirely
    assert raw_pending(candidates, delivered) == ["t-b"]


def test_multiple_feeds_are_discovered_and_grouped_independently():
    s3 = FakeS3()
    fd_a = _feed(name="qa_happy_position", external_id="1234")
    fd_b = _feed(name="qa_other_position", external_id="5678")
    registry = {fd_a.name: fd_a, fd_b.name: fd_b}

    marker_a = _put_transport(s3, "t-a", external_id="1234")
    deliv_a = delivery.create_delivery(marker_a, client=s3, bucket="lakehouse",
                                       registry=registry)
    normalization.normalize_delivery(
        delivery.manifest_key(transport.read_transport(marker_a, client=s3, bucket="lakehouse")),
        client=s3, bucket="lakehouse", registry=registry)

    marker_b = _put_transport(s3, "t-b", external_id="5678")
    deliv_b = delivery.create_delivery(marker_b, client=s3, bucket="lakehouse",
                                       registry=registry)
    normalization.normalize_delivery(
        delivery.manifest_key(transport.read_transport(marker_b, client=s3, bucket="lakehouse")),
        client=s3, bucket="lakehouse", registry=registry)

    report = discover_transport_progress(client=s3, bucket="lakehouse",
                                         registry=registry)
    assert report["needs_full_chain"] == []
    assert report["candidates_by_feed"] == {
        fd_a.name: [(deliv_a.delivery_id, "t-a")],
        fd_b.name: [(deliv_b.delivery_id, "t-b")],
    }


def test_same_original_filename_different_transports_stay_independent():
    """Two Transports sharing a producer filename must not collapse.

    Their DeliveryIDs are derived from (source, TransportID), never from
    filename -- docs/DELIVERY-CONTRACT.md -- so both must surface as distinct
    candidates.
    """
    s3, fd = FakeS3(), _feed()
    registry = {fd.name: fd}
    for tid in ("t-same-1", "t-same-2"):
        marker = _put_transport(s3, tid, filename="positions_20260917.csv")
        delivery.create_delivery(marker, client=s3, bucket="lakehouse",
                                 registry=registry)
        normalization.normalize_delivery(
            delivery.manifest_key(
                transport.read_transport(marker, client=s3, bucket="lakehouse")),
            client=s3, bucket="lakehouse", registry=registry)

    report = _discover(s3, fd)
    transports_seen = sorted(t for _, t in report["candidates_by_feed"][fd.name])
    assert transports_seen == ["t-same-1", "t-same-2"]
    delivery_ids = {d for d, _ in report["candidates_by_feed"][fd.name]}
    assert len(delivery_ids) == 2, "distinct Transports must get distinct DeliveryIDs"


def test_unreadable_transport_is_collected_not_raised():
    """A subject that cannot be read is reported as failed, never silently
    dropped and never allowed to abort reconciliation for everything else --
    CLAUDE.md, 'the one habit that matters'.
    """
    s3, fd = FakeS3(), _feed()
    s3.put("received/bad-1/_COMPLETE.json", "{not JSON")
    _put_transport(s3, "t-good")

    report = _discover(s3, fd)
    assert report["needs_full_chain"] == ["t-good"]
    assert len(report["failed"]) == 1
    assert report["failed"][0]["marker"] == "received/bad-1/_COMPLETE.json"
    assert "malformed" in report["failed"][0]["error"].lower()
