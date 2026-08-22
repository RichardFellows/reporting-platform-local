"""Business-date completeness: which days are MISSING from a feed's history.

WHY THIS EXISTS AND WHAT FRESHNESS CANNOT DO. `dbt source
freshness` measures the age of the newest `_ingest_ts`. It catches a feed that
has stopped arriving. It cannot catch a hole in the middle of a history that
later resumed, because a newer delivery resets the clock — the seed's
deliberately absent counterparty day raises no freshness warning at all.
`ARCHITECTURE.md`'s claim that "the freshness test flags it" is therefore true
only of the still-late case.

A gap is worse than a late feed. A late feed is visibly missing and the report
is visibly incomplete. A gap is a report that runs, returns numbers, and is
quietly wrong for one date, forever.

HOW THE EXPECTED CALENDAR IS DERIVED, AND WHY NOT FROM A HOLIDAY FILE. The
obvious implementation is Mon-Fri minus a holiday calendar. `calendar_rules`
already argues against maintaining one — it is a dataset and a support burden
per jurisdiction — and for a completeness check it is worse than that: every
public holiday the calendar did not know about becomes a false gap, and a
check that cries wolf on Boxing Day is a check people switch off.

So the calendar is inferred from the platform's own data: **a business date is
one on which at least one feed delivered.** A holiday needs no entry anywhere
because no feed delivers on it. A date where trade and rating landed but
counterparty did not is unambiguously a gap in counterparty, and saying so
needs no external knowledge.

WHAT THIS DELIBERATELY CANNOT SEE, stated plainly because a monitor whose
blind spot is undocumented is worse than no monitor:

  * A day on which EVERY feed missed. There is no corroborating evidence, so
    the date simply is not in the inferred calendar. A platform-wide outage is
    invisible here — that is the orchestrator's and freshness's job, and the
    watchdog's.
  * A feed that does not deliver daily. Corroboration would mark every
    non-delivery day a gap -- the seed's `rating` feed arrives weekly and the
    first run of this check duly reported five false gaps for it. Hence
    `cadence: weekly`, which asks only that each week containing business
    dates saw at least one delivery, and `completeness: false` to opt out
    entirely. Note what opting out costs: a `completeness: false` feed that
    stops delivering for a month is invisible here, and only freshness will
    notice.
  * Anything outside a feed's own observed range: dates before its first
    delivery are not gaps, they are history it does not have.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from reporting_platform.common.context import feeds, retention_policy, spark_session

log = logging.getLogger("completeness")

# How far back to look. Bounded because a gap older than retention is not
# actionable -- the date is about to be expired anyway -- and an unbounded
# check re-reports the same ancient hole every night until someone mutes it.
# Defaults to the raw layer's own keep_business_days so the window that is
# checked is exactly the window that still exists.
DEFAULT_LOOKBACK = None


def observed_dates(spark, table: str) -> set[date]:
    rows = spark.sql(
        f"SELECT DISTINCT _business_date AS bd FROM {table} "
        f"WHERE _business_date IS NOT NULL"
    ).collect()
    return {r["bd"] for r in rows}


def find_gaps(per_feed: dict[str, set[date]], lookback: int,
              cadence: dict[str, str] | None = None) -> dict:
    """Dates each feed is missing that other feeds prove were business days.

    Split out from the Spark reads so the interesting logic can be exercised
    without a cluster -- the calendar inference is the part with edge cases,
    and it should not need thirty seconds of JVM start-up to test.
    """
    calendar = sorted(set().union(*per_feed.values())) if per_feed else []
    # Bound to the most recent `lookback` business dates. Note this counts
    # OBSERVED dates, not calendar days, exactly as keep_business_days does --
    # so a run of holidays does not silently shorten the window checked.
    window = set(calendar[-lookback:]) if lookback else set(calendar)

    cadence = cadence or {}
    report: dict = {"business_dates_in_window": len(window),
                    "lookback_business_days": lookback, "feeds": []}
    for name, dates in sorted(per_feed.items()):
        if not dates:
            report["feeds"].append(
                {"feed": name, "status": "no data", "missing": []})
            continue
        # A feed is only accountable for dates inside its OWN history. Dates
        # before its first delivery are not gaps, and a date after its last is
        # lateness -- which is freshness's job, not this check's, and counting
        # it here would double-report every feed still waiting for today.
        first, last = min(dates), max(dates)
        expected = {d for d in window if first <= d <= last}
        how = cadence.get(name, "daily")

        if how == "weekly":
            # Ask only that each ISO week containing business dates saw at
            # least one delivery. Weeks are used rather than a "at most N days
            # between deliveries" rule because a week is unambiguous and needs
            # no calendar: a run of holidays shortens the week's business
            # dates without changing which week they are in.
            weeks = {(d.isocalendar()[0], d.isocalendar()[1]) for d in expected}
            got = {(d.isocalendar()[0], d.isocalendar()[1]) for d in dates}
            missing = [f"{y}-W{w:02d}" for y, w in sorted(weeks - got)]
            n_expected = len(weeks)
            n_observed = len(weeks & got)
        else:
            missing = [d.isoformat() for d in sorted(expected - dates)]
            n_expected = len(expected)
            n_observed = len(dates & window)

        report["feeds"].append({
            "feed": name,
            "cadence": how,
            "status": "gaps" if missing else "complete",
            "observed": n_observed,
            "expected": n_expected,
            "missing": missing,
        })
    report["total_missing"] = sum(len(f["missing"]) for f in report["feeds"])
    return report


def run(lookback: int | None = None) -> dict:
    """Read every checked feed's raw table and report gaps."""
    if lookback is None:
        lookback = retention_policy("raw").get("keep_business_days", 10)

    checked, skipped, cadence = {}, [], {}
    spark = spark_session("completeness", ref="main")
    try:
        for name, fd in feeds().items():
            # A feed that does not deliver daily would show every non-delivery
            # day as a gap, so opting out must be possible -- and must be
            # explicit, or a feed silently drops out of the check.
            if getattr(fd, "completeness", True) is False:
                skipped.append(name)
                continue
            cadence[name] = getattr(fd, "cadence", "daily")
            try:
                checked[name] = observed_dates(spark, fd.raw_table)
            except Exception as exc:
                log.warning("cannot read %s: %s", fd.raw_table, str(exc)[:200])
                checked[name] = set()
    finally:
        spark.stop()

    report = find_gaps(checked, lookback, cadence)
    report["skipped_feeds"] = skipped
    for f in report["feeds"]:
        if f["missing"]:
            log.warning("feed %s (%s) is missing %d period(s) other feeds "
                        "delivered on: %s", f["feed"], f.get("cadence"),
                        len(f["missing"]), ", ".join(f["missing"]))
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--lookback", type=int, default=None,
                   help="business dates to check back over "
                        "(default: raw layer's keep_business_days)")
    p.add_argument("--fail-on-gap", action="store_true",
                   help="exit non-zero if any gap is found")
    a = p.parse_args(argv)
    report = run(a.lookback)
    print(json.dumps(report, indent=2, default=str))
    return 1 if (a.fail_on_gap and report["total_missing"]) else 0


if __name__ == "__main__":
    sys.exit(main())
