"""A Transport into raw, as steps a person or any scheduler can call.

    validate(marker)        _COMPLETE.json -> verified Transport evidence
    deliver(marker)         -> immutable DeliveryManifest (create-once)
    normalize(delivery)     -> NormalizationManifest v2 (create-once)
    ingest_raw(norm, id)    -> raw, on its own Nessie branch, merged on success
    ingest_transport(marker)   all four, in order, then `steps.after_ingest`
                               (drift report + snapshot tag), as the DAG does
    pending(cob_dates)      marker keys of every Transport not yet in raw

THE `transport_ingest` DAG'S TASKS WERE THESE BODIES. Each task now calls one
of the first four and adds only what Airflow alone knows -- the dag and run
id, the try number, turning a refusal into AirflowFailException, the raw
asset event. `python -m reporting_platform.ingest transport` calls the same
functions with nothing listening, which is what lets the standalone runner
take Transports instead of the inbox. `tests/test_transport_steps.py` fails
if the DAG grows its own copy of a step again. `pending` is the walk
`transport_reconcile` makes (`transport_reconcile.discover_transport_progress`
plus one raw read per feed), so the two agree on what is outstanding.

Every step records the Transport's progress on `registry.transport_receipt`
and a failure's evidence in `registry.validation_result`, best effort, as the
DAG always did: a registry outage never becomes the step's error.

A REFUSAL IS NOT A FAILURE TO RUN. `is_refusal` says whether the same
evidence will fail the same way again (docs/DECISIONS.md#a-refusal-is-not-
retried); the DAG uses it to stop retrying, `ingest_transport` to report
`refused` apart from `error`, and the pipeline to keep building on the
Transports that did land -- a refused one never reached raw.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Caller:
    """Who is running a step, for the receipt and the evidence rows.

    `execution_ref` is what a validation row is keyed by -- the Airflow run
    id, or the CLI's own run id. `dag_id`/`run_id` are Airflow's and stay
    None elsewhere: the receipt's `airflow_*` columns mean exactly that.
    """
    execution_ref: str
    dag_id: str | None = None
    run_id: str | None = None

    def receipt(self) -> dict[str, str | None]:
        return {"airflow_dag_id": self.dag_id, "airflow_run_id": self.run_id}


def is_refusal(exc: Exception) -> bool:
    """Will the same evidence fail the same way? A DeliveryError is about how
    the Transport's own immutable evidence reads against the Feed; a
    SparkTaskRefused is a blocking Raw check on the same bytes. Neither
    changes between attempts. See docs/DECISIONS.md#a-refusal-is-not-retried
    """
    from reporting_platform.common.spark_task import SparkTaskRefused
    from reporting_platform.ingest import delivery as delivery_contract

    return isinstance(exc, (delivery_contract.DeliveryError, SparkTaskRefused))


def record_failure(exc: Exception, *, control_id: str, evidence_ref: str,
                   caller: Caller, feed: str | None = None,
                   delivery_id: str | None = None) -> None:
    """Durable evidence for a Transport/Delivery control that DID NOT pass.

    Phase 7 (`docs/VALIDATION.md`). A failed Transport/Delivery validation
    leaves no DeliveryManifest and is NEVER given a fake one -- the immutable
    evidence under `received/<transport_id>/` is the record of what arrived,
    and this row is the record of what the platform decided about it. FAIL is
    a known validation exception (the control ran and found a real problem);
    ERROR is anything else (the control itself could not execute). Best
    effort, like every other registry write on this path -- see
    `registry/validation.py`.
    """
    from reporting_platform.ingest import transport as transport_contract
    from reporting_platform.registry import validation

    outcome = "FAIL" if is_refusal(exc) or isinstance(
        exc, transport_contract.TransportContractError) else "ERROR"
    validation.record_quietly(
        layer="delivery", control_id=control_id, control_name=control_id,
        outcome=outcome, severity="blocking",
        attempt_key=caller.execution_ref, execution_ref=caller.execution_ref,
        transport_id=evidence_ref, feed=feed, delivery_id=delivery_id,
        evidence_ref=evidence_ref, message=f"{type(exc).__name__}: {exc}")


def _reason(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:2000]


def validate(marker_key: str, caller: Caller, *,
             transport_id: str | None = None) -> str:
    """`_COMPLETE.json` -> a verified Transport. Returns the marker key.

    Read-only and side-effect free on object storage, so a retry after a
    transient hiccup is the ordinary case. Fails here, and only here, for a
    Transport that is not what it claims: a missing or mismatched object, a
    bad hash, or a marker version the contract does not accept.

    `transport_id` is only for a failure's receipt, when the caller knows it
    without being able to read the marker.
    """
    from reporting_platform.ingest import transport as transport_contract
    from reporting_platform.registry import transports as receipts

    try:
        validated = transport_contract.read_validated_transport(marker_key)
    except Exception as exc:
        if transport_id:
            receipts.record_stage_quietly(
                transport_id, "failed", failure_reason=_reason(exc),
                **caller.receipt())
        record_failure(exc, control_id="transport_contract",
                       evidence_ref=transport_id or marker_key, caller=caller)
        raise
    # SELF-HEALING: `record_discovered` is insert-once, so this is a no-op
    # when transport_watch/transport_reconcile already wrote the row, and a
    # full recovery of it when nothing discovered this Transport first -- a
    # manual replay, or the standalone runner. See registry/transports.py.
    receipts.record_discovered_quietly(validated, marker_key)
    receipts.record_stage_quietly(validated.transport_id, "validated",
                                  **caller.receipt())
    return marker_key


def deliver(marker_key: str, caller: Caller) -> str:
    """Accepted Transport -> immutable DeliveryManifest. Returns its key.

    Create-once: the same marker always returns the same DeliveryManifest,
    written on the first call and read-and-verified on every one after. Fails
    here for an unknown external Feed id, a business-identity conflict, or a
    declared control file that did not arrive (`docs/DELIVERY-CONTRACT.md`)
    -- all about THIS Delivery's interpretation, never the Transport
    evidence, which `validate` already settled.
    """
    from reporting_platform.ingest import delivery as delivery_contract
    from reporting_platform.ingest import transport as transport_contract
    from reporting_platform.registry import transports as receipts

    transport_id = None
    try:
        parsed = transport_contract.read_transport(marker_key)
        transport_id = parsed.transport_id
        created = delivery_contract.create_delivery(marker_key)
    except Exception as exc:
        if transport_id:
            # Best-effort: the FEED may be perfectly resolvable even though
            # identity (date/version) resolution is what failed -- and a
            # FAILED row with no feed never surfaces on that feed's COB Status
            # row, which is exactly the case an operator most wants to see.
            # Never let this secondary lookup mask the real exception.
            failed_feed = None
            try:
                failed_feed = delivery_contract.resolve_transport_feed(parsed).name
            except Exception:                                    # noqa: BLE001
                pass
            receipts.record_stage_quietly(
                transport_id, "failed", feed=failed_feed,
                failure_reason=_reason(exc), **caller.receipt())
        record_failure(exc, control_id="delivery_identity",
                       evidence_ref=transport_id or marker_key, caller=caller)
        raise
    receipts.record_stage_quietly(
        transport_id, "delivered", feed=created.feed,
        delivery_id=created.delivery_id, **caller.receipt())
    # A pure function of transport identity: a cheap re-parse of the small
    # marker, not a re-hash of the bytes create_delivery already validated.
    return delivery_contract.manifest_key(parsed)


def normalize(delivery_manifest_key: str, caller: Caller) -> str:
    """DeliveryManifest -> rebuildable NormalizationManifest v2. Returns its key.

    Create-once and idempotent (`docs/NORMALIZATION-CONTRACT.md`): a retry
    after a partial archive extraction resumes from whatever Ready parts
    already exist and accepts them only when byte-identical. Fails here for
    an unsafe archive member, an invalid zip, or a normalization contract
    conflict -- never for anything about raw ingestion.
    """
    from reporting_platform.ingest import delivery as delivery_contract
    from reporting_platform.ingest.normalization import normalize_delivery
    from reporting_platform.registry import transports as receipts

    # A cheap re-read of the immutable manifest this was HANDED, only to
    # recover the Transport identity for the receipt row.
    transport_id = None
    try:
        transport_id = delivery_contract.read_delivery_manifest(
            delivery_manifest_key).transport_id
    except Exception:                                            # noqa: BLE001
        pass

    try:
        result = normalize_delivery(delivery_manifest_key)
    except Exception as exc:
        if transport_id:
            receipts.record_stage_quietly(
                transport_id, "failed", failure_reason=_reason(exc),
                **caller.receipt())
        record_failure(exc, control_id="normalization_contract",
                       evidence_ref=delivery_manifest_key, caller=caller)
        raise
    if transport_id:
        receipts.record_stage_quietly(transport_id, "normalized",
                                      **caller.receipt())
    return result.key


def ingest_raw(normalization_manifest_key: str, attempt_id: str,
               caller: Caller) -> dict:
    """NormalizationManifest v2 -> Raw Iceberg, merged onto `main`.

    `ingest-v2` is the process-isolated adapter over
    `ingest_feed.ingest_normalized_delivery` (`docs/RAW-INGESTION-CONTRACT.md`):
    branch, write, validate and merge all happen inside that one call, so it
    does not return until the Delivery is committed on `main` -- or raises,
    leaving `main` untouched and the branch kept for inspection. A Delivery
    already in raw returns at once with `already_ingested`.

    `attempt_id` names the branch, and must be unique per ATTEMPT: see
    `context.ingest_attempt_id`.
    """
    from reporting_platform.common.spark_task import run
    from reporting_platform.registry import transports as receipts

    try:
        return run("ingest-v2", normalization_manifest_key, attempt_id)
    except Exception as exc:
        # (feed, delivery_id) FROM THE PATH, not a re-read of any manifest:
        # `ready/<feed>/<delivery_id>/...` is the stable convention
        # `normalization.manifest_key` builds (docs/NORMALIZATION-CONTRACT.md).
        segments = normalization_manifest_key.split("/")
        if len(segments) >= 3:
            receipts.record_stage_by_delivery_id_quietly(
                segments[1], segments[2], "failed",
                failure_reason=_reason(exc), **caller.receipt())
        record_failure(exc, control_id="raw_ingestion",
                       evidence_ref=normalization_manifest_key, caller=caller)
        raise


def ingest_transport(marker_key: str, *, run_id: str | None = None) -> dict:
    """All four steps for one Transport, with nothing listening.

    Returns the raw result plus `transport_id` and `marker_key`; or, for one
    that did not reach raw, `stage` (which step stopped it), `error`, and
    `refused` -- True when the evidence itself is wrong and running again
    will fail the same way. Never raises for one Transport, so a caller
    working through several keeps going; the caller must look.
    """
    from reporting_platform.common.context import new_run_id

    run_id = run_id or new_run_id()
    caller = Caller(execution_ref=f"cli:{run_id}")
    out: dict[str, Any] = {"marker_key": marker_key}
    stage = "validate"
    try:
        validate(marker_key, caller)
        stage = "deliver"
        delivery_key = deliver(marker_key, caller)
        stage = "normalize"
        norm_key = normalize(delivery_key, caller)
        stage = "ingest_raw"
        # One attempt, so one branch: the run id is already unique per call.
        result = ingest_raw(norm_key, run_id, caller)
    except Exception as exc:                                     # noqa: BLE001
        refused = is_refusal(exc)
        (log.warning if refused else log.error)(
            "%s: %s %s at %s: %s", marker_key,
            "REFUSED" if refused else "FAILED", "(will not change on a re-run)"
            if refused else "", stage, _reason(exc)[:500])
        return {**out, "stage": stage, "refused": refused,
                "error": _reason(exc)}
    # The same drift report and snapshot tag the inbox path's ingest gets,
    # from the same function. See docs/DECISIONS.md#a-snapshot-tag-names-its-merge-commit
    from reporting_platform.ingest.steps import after_ingest

    return {**out, **after_ingest(result["feed"], result)}


def window_cob_dates(days: int, *, today: date | None = None) -> list[str]:
    """The last `days` calendar dates, inclusive of today, ISO-formatted.

    `transport_reconcile`'s bounded window and the CLI's `--window`: one
    definition, so "the last 7 days" is the same set of partitions for both.
    """
    anchor = today or date.today()
    return [(anchor - timedelta(days=offset)).isoformat()
            for offset in range(days)]


def pending(cob_dates: list[str] | None = None) -> dict[str, Any]:
    """Every completed Transport not yet in raw, by the reconcile's walk.

    `cob_dates` bounds the scan to those `cob_date=` partitions; None scans
    all of `received/`, v1 markers included -- the full sweep. Returns
    `marker_keys` (sorted, what to ingest) and `unreadable`: markers the walk
    could not read, which are NOT reported as nothing pending
    (CLAUDE.md, "a subject it could not READ is not a subject that is EMPTY").

    A Transport that was refused is still pending by this walk -- it never
    reached raw -- and is attempted again, and refused again, every time.
    That is the same set `transport_reconcile` computes; what differs is that
    Airflow triggers each Transport once per run id and a person running this
    sees the refusal every time until the Transport is corrected.
    """
    from reporting_platform.common.spark_task import run
    from reporting_platform.ingest.transport_reconcile import (
        discover_transport_progress, raw_pending,
    )

    report = discover_transport_progress(cob_dates=cob_dates)
    delivered = {feed: run("raw-delivery-ids", feed)["delivery_ids"]
                 for feed in report["candidates_by_feed"]}
    ids = sorted(set(report["needs_full_chain"])
                 | set(raw_pending(report["candidates_by_feed"], delivered)))
    return {"marker_keys": [report["marker_keys"][t] for t in ids
                            if t in report["marker_keys"]],
            "unreadable": report["failed"]}
