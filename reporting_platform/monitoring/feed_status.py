"""COB Feed Status: for one COB date, which feeds are where. REQ-unlabelled.

THE QUESTION THIS ANSWERS AND THE ONE COMPLETENESS.PY ALREADY ANSWERS ARE
DIFFERENT. `completeness.py` infers its expected calendar from CORROBORATION
-- a COB date is a business day because some feed delivered on it -- which is
exactly wrong for a date that has not finished arriving yet: on the evening a
COB is still landing, most feeds have not delivered, and corroboration would
read that as "not a business day" and hide every genuine gap. This module
judges a single, usually-recent COB date the caller names explicitly, so
"expected" is a plain function of `Feed.cadence`/`Feed.delivery_expected` --
see `is_expected` -- with NO calendar file and NO corroboration, the same
"do not invent a holiday calendar" choice `calendar_rules.py` and
`completeness.py` already made, applied to a different question.

PERSISTED FACTS, DERIVED STATUS -- the whole design brief. Nothing here is a
stored verdict: `status_of` is a pure function of facts already durable
elsewhere --

  * `registry.transport_receipt` -- has a Transport for this (feed, cob_date)
    been discovered/validated, and did it fail before becoming a Delivery?
    Only ever populated for Transport-origin (v2) deliveries.
  * `registry.delivery` -- has a Delivery been normalized for this (feed,
    cob_date)? True for BOTH the legacy and the Transport path (see that
    module's header: a row lands here before Raw is ever touched), which is
    what makes `has_delivery` the uniform PROCESSING signal across both.
  * `registry.delivery_committed` -- has it reached Raw on `main`? Also
    uniform across both paths -- see registry/db.py's header on that table.
  * `Feed.expected_by` -- REUSES `monitoring.lateness.deadline`, not a second
    copy of "day after the COB date, at this wall-clock time".

STATUS VOCABULARY IS DELIBERATELY SMALL, and LATE IS NOT IN IT. A feed that
arrived after its deadline is exactly as COMPLETE/PROCESSING as one that
arrived on time -- lateness and processing state are different questions,
which is `monitoring/lateness.py`'s own module-header rule applied here too.
`late` rides as a separate flag on an arrived feed's entry instead of being a
seventh status that would collide with COMPLETE/PROCESSING.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone

from reporting_platform.common.context import Feed, feeds
from reporting_platform.monitoring import lateness
from reporting_platform.registry import deliveries as registry_deliveries
from reporting_platform.registry import transports as registry_transports

log = logging.getLogger("monitoring.feed_status")

STATUSES = ("NOT_EXPECTED", "WAITING", "RECEIVED", "PROCESSING", "COMPLETE",
           "FAILED", "MISSING")


def is_expected(fd: Feed, cob_date: date, *,
               weekly_isoweeks_with_delivery: set[tuple[int, int]] | None = None
               ) -> bool:
    """Whether `fd` is expected to have delivered for `cob_date`.

    `daily` is expected on every calendar date named -- there is no business-
    day calendar here, on purpose (see the module header): a daily feed asked
    about a weekend reports MISSING rather than the platform silently
    inventing a calendar to excuse it. Document this rather than build one,
    per the task's own instruction; a feed whose weekends genuinely are not
    business days should declare `delivery_expected: false` for them, which
    this platform has no per-date mechanism for yet -- a real limitation,
    named rather than hidden.

    `weekly` reuses the SAME "ask only that each ISO week saw one delivery"
    rule `completeness.find_gaps` already uses -- not a duplicate concept,
    the same one, because a weekly feed asked about a single day cannot
    otherwise say which day of the week it owes. Expected only on a date
    whose ISO week has not already been satisfied by an EARLIER delivery.
    """
    if not fd.delivery_expected:
        return False
    if fd.cadence == "weekly":
        iso = cob_date.isocalendar()
        week = (iso[0], iso[1])
        return week not in (weekly_isoweeks_with_delivery or set())
    return True


def status_of(*, expected: bool, committed: bool, has_delivery: bool,
             receipt_status: str | None, expected_by: str, cob_date: date,
             now: datetime) -> str:
    """The derived status for one feed on one COB date. Pure -- see the
    module header for what each input is a fact about, and why the four
    checked in this order are checked in this order:

      1. `committed` wins over everything -- a Delivery that failed once and
         was superseded by a later, successful one is COMPLETE, not FAILED.
      2. `has_delivery` (normalized, not yet in Raw) is PROCESSING regardless
         of what any earlier failed Transport for the same date did.
      3. Only once neither of those is true does a 'failed' receipt matter.
      4. Only once there is no Transport at all does the deadline question
         (MISSING vs WAITING) arise -- see `Feed.expected_by`'s own docstring
         on why silence there means "no deadline asserted", not "on time".
    """
    if not expected:
        return "NOT_EXPECTED"
    if committed:
        return "COMPLETE"
    if has_delivery:
        return "PROCESSING"
    if receipt_status == "failed":
        return "FAILED"
    if receipt_status is not None:
        return "RECEIVED"
    if expected_by and now > lateness.deadline(cob_date, expected_by):
        return "MISSING"
    return "WAITING"


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value.isoformat()


def _week_bounds(cob_date: date) -> tuple[date, date]:
    start = cob_date - timedelta(days=cob_date.isoweekday() - 1)
    return start, start + timedelta(days=6)


def feed_entry(fd: Feed, cob_date: date, *, now: datetime,
              receipts: list[dict], deliveries: list[dict],
              committed: list[dict],
              weekly_isoweeks_with_delivery: set[tuple[int, int]] | None = None
              ) -> dict:
    """One feed's row in the report. Assembles already-fetched facts; does no
    I/O itself, so it is exactly as testable as `status_of` with real-shaped
    rows instead of bare booleans.
    """
    expected = is_expected(
        fd, cob_date, weekly_isoweeks_with_delivery=weekly_isoweeks_with_delivery)
    latest_receipt = (sorted(receipts, key=lambda r: r["updated_at"])[-1]
                      if receipts else None)
    latest_delivery = (sorted(deliveries, key=lambda d: d["received_at"])[-1]
                       if deliveries else None)
    latest_commit = (sorted(committed, key=lambda c: c["committed_at"])[-1]
                     if committed else None)

    received_at = (latest_delivery["received_at"] if latest_delivery
                  else (latest_receipt["uploaded_at"] if latest_receipt else None))
    status = status_of(
        expected=expected, committed=bool(latest_commit),
        has_delivery=bool(latest_delivery),
        receipt_status=latest_receipt["status"] if latest_receipt else None,
        expected_by=fd.expected_by, cob_date=cob_date, now=now)
    late = bool(fd.expected_by and received_at is not None
               and received_at > lateness.deadline(cob_date, fd.expected_by))

    delivery_id = ((latest_delivery or {}).get("delivery_id")
                  or (latest_receipt or {}).get("delivery_id"))
    return {
        "feed": fd.name,
        "name": fd.description or fd.name,
        "expected": expected,
        "expected_by": fd.expected_by or None,
        "status": status,
        "late": late,
        "received_at": _iso(received_at),
        "completed_at": _iso((latest_commit or {}).get("committed_at")),
        "transport_id": (latest_receipt or {}).get("transport_id"),
        "delivery_id": delivery_id,
        "failure_reason": ((latest_receipt or {}).get("failure_reason")
                          if status == "FAILED" else None),
        "airflow_run_id": (latest_receipt or {}).get("airflow_run_id"),
    }


_SUMMARY_KEYS = ("expected", "complete", "processing", "received", "waiting",
                 "missing", "failed")


def _summarize(entries: list[dict]) -> dict:
    counts = {k: 0 for k in _SUMMARY_KEYS}
    for e in entries:
        if not e["expected"]:
            continue
        counts["expected"] += 1
        key = e["status"].lower()
        if key in counts:
            counts[key] += 1
    return counts


def build_report(cob_date: date, *, now: datetime | None = None) -> dict:
    """The whole COB Status page's data, for one COB date. One round of
    cheap, indexed Postgres reads -- no S3, no Spark, no Airflow -- because
    every fact it needs was already made durable by the code that produced
    it (see the module header). Grouped by `Feed.source_system`, the feed's
    own domain classification -- NOT Transport's unrelated field of the same
    name (docs/TRANSPORT-CONTRACT.md's own naming-collision note).
    """
    now = now or datetime.now(timezone.utc)
    registry = feeds()

    receipts_by_feed: dict[str, list[dict]] = {}
    for row in registry_transports.for_cob_date(cob_date):
        if row["feed"]:
            receipts_by_feed.setdefault(row["feed"], []).append(row)

    deliveries_by_feed: dict[str, list[dict]] = {}
    for row in registry_deliveries.deliveries_on(cob_date):
        deliveries_by_feed.setdefault(row["feed"], []).append(row)

    committed_by_feed: dict[str, list[dict]] = {}
    for row in registry_deliveries.committed_for_cob_date(cob_date):
        committed_by_feed.setdefault(row["feed"], []).append(row)

    week_start, week_end = _week_bounds(cob_date)
    weekly_weeks: dict[str, set[tuple[int, int]]] = {}
    for fd in registry.values():
        if fd.cadence != "weekly":
            continue
        dates = registry_deliveries.delivered_dates(fd.name, week_start, week_end)
        weekly_weeks[fd.name] = {(d.isocalendar()[0], d.isocalendar()[1])
                                 for d in dates}

    systems: dict[str, list[dict]] = {}
    for name, fd in sorted(registry.items()):
        entry = feed_entry(
            fd, cob_date, now=now, receipts=receipts_by_feed.get(name, []),
            deliveries=deliveries_by_feed.get(name, []),
            committed=committed_by_feed.get(name, []),
            weekly_isoweeks_with_delivery=weekly_weeks.get(name))
        systems.setdefault(fd.source_system, []).append(entry)

    systems_out = [
        {"source_system": system, "feeds": sorted(fs, key=lambda e: e["feed"])}
        for system, fs in sorted(systems.items())]
    all_entries = [e for s in systems_out for e in s["feeds"]]
    return {"cob_date": cob_date.isoformat(), "generated_at": _iso(now),
           "summary": _summarize(all_entries), "systems": systems_out}


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--cob-date", required=True, type=date.fromisoformat)
    a = p.parse_args(argv)
    print(json.dumps(build_report(a.cob_date), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
