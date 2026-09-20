"""Orchestration entry points shared by the CLI and `migration_reconcile` DAG.

ONE GENERIC PATH FOR EVERY FEED (sections 3/38/39): there is exactly one
function that runs a comparison (`compare_business_date`) and one that finds
candidates (`discover_candidates`), and both take a Feed/business_date as
data -- never a per-feed branch. Onboarding feed #501 into dual-run changes
nothing here.

NEW-PLATFORM INDEPENDENCE (section 24): nothing in the Transport/Delivery/
Raw/prepared/reporting pipeline calls anything in this module. A legacy
outage, a missing fixture, or this module raising outright cannot fail an
otherwise-valid new-platform ingest -- the reverse dependency does not exist
to break.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from reporting_platform.common.context import Feed
from reporting_platform.migration import comparators, contract, correlate, diffs, evidence
from reporting_platform.migration.legacy import LegacyResultSource

log = logging.getLogger("migration.run")

NOT_COMPARABLE = "NOT_COMPARABLE"  # never persisted -- see module docstring


def _new_side_evidence_for_raw(feed_name: str, business_date: date
                              ) -> list[dict[str, Any]]:
    """Registered new-platform deliveries for one feed/date, with the fuller
    identity columns `registry.deliveries.deliveries_on` does not select."""
    from reporting_platform.registry import db

    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT delivery_id, source_filename, source_container, md5, "
            "       producer_run_id, source_system, origin "
            "FROM registry.delivery WHERE feed = %s AND cob_date = %s "
            "ORDER BY sequence_no", (feed_name, business_date))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _new_evidence_from_delivery_row(feed_name: str, business_date: date,
                                    row: dict[str, Any]
                                    ) -> correlate.CorrelationEvidence:
    return correlate.CorrelationEvidence(
        feed=feed_name, business_date=business_date,
        source_system=row.get("source_system") or "DCM",
        source_filename=row.get("source_filename") or row.get("source_container"),
        source_sha256=None,  # registry.delivery.md5 is a different algorithm;
                             # see docs/MIGRATION.md#correlation
        producer_run_id=row.get("producer_run_id"))


def compare_business_date(feed: Feed, business_date: date, *,
                          legacy_source: LegacyResultSource,
                          spark=None,
                          allow_business_date_only: bool = False
                          ) -> dict[str, Any]:
    """Run the configured comparison for one feed/business_date.

    Returns a dict always; `outcome` is `NOT_COMPARABLE` (never persisted,
    see docs/VALIDATION.md's "missing one side is not FAIL") when either
    side has nothing yet, or when correlation could not pair the two sides.
    An unhandled comparator failure is caught here and recorded as ERROR --
    the control failing to run, not a data mismatch (section 34).
    """
    if feed.migration_mode not in ("dual_run", "new_primary"):
        return {"feed": feed.name, "business_date": business_date,
               "outcome": NOT_COMPARABLE,
               "reason": f"migration.mode is {feed.migration_mode!r}, not dual_run"}

    compare_cfg = (feed.migration or {}).get("compare") or {"checkpoint": "raw"}
    checkpoint = compare_cfg.get("checkpoint", "raw")
    key = compare_cfg.get("key") or []
    columns = compare_cfg.get("columns") or []
    aggregates = compare_cfg.get("aggregates") or []
    table_override = compare_cfg.get("table")

    new_rows = (_new_side_evidence_for_raw(feed.name, business_date)
               if checkpoint == "raw" else [])
    if checkpoint == "raw" and not new_rows:
        return {"feed": feed.name, "business_date": business_date,
               "outcome": NOT_COMPARABLE,
               "reason": "no registered new-platform delivery for this date yet"}

    legacy_result = legacy_source.fetch(feed.name, business_date, checkpoint)
    if legacy_result is None:
        return {"feed": feed.name, "business_date": business_date,
               "outcome": NOT_COMPARABLE,
               "reason": "no legacy result available for this date yet"}

    if checkpoint == "raw":
        new_row = new_rows[0]
        new_ref = new_row["delivery_id"]
        new_evidence = _new_evidence_from_delivery_row(feed.name, business_date, new_row)
    else:
        # prepared/reporting: the new-side reference is the checkpoint table
        # itself for this business date; correlation still runs on whatever
        # identity the caller can supply through `new_evidence_override`
        # (the DAG passes the raw delivery's evidence forward -- see
        # docs/MIGRATION.md#comparison-checkpoints).
        new_ref = f"{checkpoint}:{feed.name}:{business_date.isoformat()}"
        new_evidence = correlate.CorrelationEvidence(
            feed=feed.name, business_date=business_date)

    pairing = correlate.correlate(
        legacy_result.evidence, new_evidence,
        allow_business_date_only=allow_business_date_only)
    if pairing is None:
        return {"feed": feed.name, "business_date": business_date,
               "outcome": NOT_COMPARABLE,
               "reason": "legacy and new evidence could not be correlated "
                         "to the same logical upstream delivery"}

    contract_hash = contract.comparison_contract_hash(
        checkpoint, key, columns, aggregates)
    cid = contract.comparison_id(feed.name, checkpoint, legacy_result.reference,
                                new_ref, contract_hash)

    try:
        outcomes: list[comparators.ComparisonOutcome] = []
        diff_material: dict[str, Any] = {}

        if spark is not None:
            from reporting_platform.migration.new_side import checkpoint_summary
            from reporting_platform.common.context import CATALOG

            new_summary = checkpoint_summary(
                spark, catalog=CATALOG, feed_name=feed.name,
                checkpoint=checkpoint, business_date=business_date, key=key,
                columns=columns, aggregates=aggregates,
                delivery_id=new_ref if checkpoint == "raw" else None,
                table_override=table_override)
        else:
            new_summary = {"row_count": None, "row_hashes": {}, "aggregates": {}}

        legacy_row_count = legacy_result.effective_row_count()
        if legacy_row_count is not None and new_summary["row_count"] is not None:
            outcomes.append(comparators.compare_row_count(
                legacy_row_count, new_summary["row_count"]))

        if key and columns and legacy_result.rows is not None and spark is not None:
            legacy_hashes = comparators.hashes_by_key(legacy_result.rows, key, columns)
            new_hashes = {tuple(k.split("\x1f")): v
                         for k, v in new_summary["row_hashes"].items()}
            key_outcome = comparators.compare_key_set(
                set(legacy_hashes), set(new_hashes))
            outcomes.append(key_outcome)
            hash_outcome = comparators.compare_row_hash(legacy_hashes, new_hashes)
            outcomes.append(hash_outcome)
            if key_outcome.outcome != "PASS" or hash_outcome.outcome != "PASS":
                diff_material = {
                    "legacy_only": sorted(str(k) for k in
                                          set(legacy_hashes) - set(new_hashes)),
                    "new_only": sorted(str(k) for k in
                                       set(new_hashes) - set(legacy_hashes)),
                    "changed": sorted(str(k) for k in set(legacy_hashes) & set(new_hashes)
                                     if legacy_hashes[k] != new_hashes[k]),
                }

        for agg in aggregates:
            column, function = agg["column"], agg["function"]
            legacy_value = legacy_result.aggregates.get(column)
            new_value = new_summary["aggregates"].get(f"{function}:{column}")
            if legacy_value is None or new_value is None:
                continue
            outcomes.append(comparators.compare_aggregate(
                legacy_value, new_value, function=function,
                tolerance=agg.get("tolerance")))

        if not outcomes:
            return {"feed": feed.name, "business_date": business_date,
                   "outcome": NOT_COMPARABLE,
                   "reason": "no comparator produced a result -- check "
                             "migration.compare configuration"}

        final_outcome = comparators.worst_outcome(outcomes)
        severity = "blocking"
        summary = {"strategies": [
            {"outcome": o.outcome, **o.summary, "message": o.message}
            for o in outcomes]}

        diff_ref = None
        if final_outcome in ("FAIL", "WARN"):
            diff_ref = diffs.write(feed.name, cid, summary=summary,
                                   **diff_material)

        row = evidence.record_quietly(
            comparison_id=cid, feed=feed.name, business_date=business_date,
            checkpoint=checkpoint, correlation_key=pairing.key,
            new_ref=new_ref, legacy_ref=legacy_result.reference,
            comparison_contract_hash=contract_hash, outcome=final_outcome,
            severity=severity, summary=summary, diff_ref=diff_ref,
            message="; ".join(o.message for o in outcomes if o.message) or None)
        return row or {"comparison_id": cid, "outcome": final_outcome,
                      "feed": feed.name, "business_date": business_date}
    except Exception as exc:                                     # noqa: BLE001
        log.exception("migration comparison errored for %s %s", feed.name,
                     business_date)
        row = evidence.record_quietly(
            comparison_id=cid, feed=feed.name, business_date=business_date,
            checkpoint=checkpoint, correlation_key=pairing.key, new_ref=new_ref,
            legacy_ref=legacy_result.reference,
            comparison_contract_hash=contract_hash, outcome="ERROR",
            severity="blocking", summary={"error": str(exc)},
            message=f"{type(exc).__name__}: {exc}")
        return row or {"comparison_id": cid, "outcome": "ERROR",
                      "feed": feed.name, "business_date": business_date}


def discover_candidates(feeds: list[Feed], *, lookback_days: int = 14,
                        today: date | None = None) -> list[tuple[str, str]]:
    """(feed, business_date-iso) pairs worth attempting a comparison for.

    BOUNDED, not "every business date this feed has ever seen" (section 39:
    scale to hundreds of feeds without a full historical rescan on every
    tick). `migration_reconcile` maps a task per pair with Airflow dynamic
    task mapping -- this is the list that mapping fans out over.
    """
    today = today or date.today()
    out: list[tuple[str, str]] = []
    for fd in feeds:
        if fd.migration_mode not in ("dual_run", "new_primary"):
            continue
        for offset in range(lookback_days):
            bd = today - timedelta(days=offset)
            out.append((fd.name, bd.isoformat()))
    return out
