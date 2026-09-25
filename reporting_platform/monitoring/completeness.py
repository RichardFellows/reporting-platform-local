"""COB-date completeness: which days are MISSING from a feed's history.

WHY THIS EXISTS AND WHAT FRESHNESS CANNOT DO. `dbt source freshness` measures
the age of the newest `_ingest_ts`, so it catches a feed that has stopped
arriving. It cannot catch a hole in the middle of a history that later resumed,
because a newer delivery resets the clock -- the seed's deliberately absent
counterparty day raises no freshness warning at all.

A gap is worse than a late feed. A late feed is visibly missing and the report
visibly incomplete. A gap is a report that runs, returns numbers, and is
quietly wrong for one date, forever.

HOW THE EXPECTED CALENDAR IS DERIVED, AND WHY NOT FROM A HOLIDAY FILE. The
obvious implementation is Mon-Fri minus a holiday calendar. `calendar_rules`
already argues against maintaining one, and for a completeness check it is
worse: every public holiday the calendar did not know about becomes a false
gap, and a check that cries wolf on Boxing Day is one people switch off.

So the calendar is inferred from the platform's own data: **a COB date is one
on which at least one feed delivered.** A holiday needs no entry because no
feed delivers on it. A date where trade and rating landed but counterparty did
not is unambiguously a gap in counterparty.

WHAT THIS DELIBERATELY CANNOT SEE, stated plainly because a monitor whose blind
spot is undocumented is worse than no monitor:

  * A day on which EVERY feed missed. There is no corroborating evidence, so
    the date is not in the inferred calendar. A platform-wide outage is
    invisible here -- that is the orchestrator's, freshness's and the
    watchdog's job.
  * A feed that does not deliver daily. Corroboration would mark every
    non-delivery day a gap -- the seed's weekly `rating` feed produced five
    false gaps on the first run. Hence `cadence: weekly`, and
    `delivery_expected: false` to opt out entirely. Opting out costs
    visibility: such a feed that stops for a month is invisible here.
  * Anything outside a feed's own observed range: dates before its first
    delivery are not gaps, they are history it does not have.

A TABLE IT COULD NOT READ IS NOT A TABLE WITH NO DATA. The two answers look
identical from here -- an empty set of observed dates -- and reporting the
first as the second turns a broken monitor into a passing one: no gaps, no
`--fail-on-gap`, nothing on screen but a feed that has "no data". So a read
failure is carried through as its own status and fails the check.

FOUR ANSWERS FOR A FEED WITH NO DATES, and only one of them fails:

  never delivered  the table exists and is empty, and the REGISTRY has no
                   delivery for the feed. The ordinary state of a new feed.
  no data          the table exists and is empty, and the registry has
                   deliveries for it (landed, not yet in raw) -- or the
                   registry could not be asked, which the entry says.
  no table         Spark said `TABLE_OR_VIEW_NOT_FOUND`. Not a failure of
                   THIS check, but no longer the normal state of anything:
                   every declared feed gets its raw table at deploy time, so
                   this is a feed declared since, and every prepared build
                   fails on it until `ingest.migrate_raw` has run.
  unreadable       anything else -- a catalog that will not answer, a branch
                   that is gone, a permissions error. That one FAILS.

"Never delivered" used to be read off the missing table. It cannot be any
more: the table is created, empty, before the first delivery, so that one
undelivered feed does not stop every other feed publishing
(docs/DECISIONS.md#a-declared-feed-has-a-raw-table-before-it-delivers). An
empty table is not evidence of never having delivered, so the registry --
which records every delivery at normalize time, on both the legacy and the
Transport path -- is asked instead.
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
# actionable -- the date is about to be expired -- and an unbounded check
# re-reports the same ancient hole every night until someone mutes it. Defaults
# to the raw layer's keep_business_days, so the window checked is the window
# that still exists.
DEFAULT_LOOKBACK = None


def observed_dates(spark, table: str) -> set[date]:
    rows = spark.sql(
        f"SELECT DISTINCT _cob_date AS bd FROM {table} "
        f"WHERE _cob_date IS NOT NULL"
    ).collect()
    return {r["bd"] for r in rows}


def delivered_feeds(names) -> set[str]:
    """Which of `names` the delivery registry holds at least one delivery for.

    Raises if the registry cannot be asked; the caller says so rather than
    guessing either way.
    """
    from reporting_platform.registry import db

    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT feed FROM registry.delivery "
                    "WHERE feed = ANY(%s)", (sorted(names),))
        return {row[0] for row in cur.fetchall()}


def find_gaps(per_feed: dict[str, set[date]], lookback: int,
              cadence: dict[str, str] | None = None,
              unreadable: dict[str, str] | None = None,
              absent: dict[str, str] | None = None,
              never_delivered: set[str] | None = None,
              registry_error: str | None = None) -> dict:
    """Dates each feed is missing that other feeds prove were business days.

    Split out from the Spark reads so the interesting logic can be exercised
    without a cluster -- the calendar inference is the part with edge cases,
    and it should not need thirty seconds of JVM start-up to test.

    `unreadable` maps a feed to why its raw table could not be read, and
    `absent` to why it has no raw table at all -- two different answers, and
    only the first is a defect (see the module header).

    `never_delivered` names the feeds whose EMPTY table the registry says has
    never had a delivery; `registry_error` is why the registry could not be
    asked, in which case an empty table stays `no data` and says so. Neither
    of `unreadable` and `absent` contributes
    anything to the inferred calendar: an unread feed is not evidence that a
    date was a business day, and treating it as one would let a broken read
    move the window every other feed is judged against.
    """
    calendar = sorted(set().union(*per_feed.values())) if per_feed else []
    # Bound to the most recent `lookback` COB dates. Note this counts
    # OBSERVED dates, not calendar days, exactly as keep_business_days does --
    # so a run of holidays does not silently shorten the window checked.
    window = set(calendar[-lookback:]) if lookback else set(calendar)

    cadence = cadence or {}
    unreadable = unreadable or {}
    absent = absent or {}
    report: dict = {"cob_dates_in_window": len(window),
                    "lookback_business_days": lookback, "feeds": []}
    for name, why in sorted(unreadable.items()):
        report["feeds"].append(
            {"feed": name, "status": "unreadable", "error": why,
             "missing": []})
    for name, why in sorted(absent.items()):
        report["feeds"].append(
            {"feed": name, "status": "no table", "error": why, "missing": []})
    never_delivered = never_delivered or set()
    for name, dates in sorted(per_feed.items()):
        if not dates:
            if name in never_delivered:
                entry = {"feed": name, "status": "never delivered"}
            else:
                entry = {"feed": name, "status": "no data"}
                if registry_error:
                    # NOT "never delivered", and not quietly "no data"
                    # either: the question was not answered.
                    entry["error"] = ("the delivery registry could not be "
                                      "asked whether this feed ever "
                                      f"delivered: {registry_error}")
            report["feeds"].append({**entry, "missing": []})
            continue
        # A feed is only accountable for dates inside its OWN history. Dates
        # before its first delivery are not gaps, and a date after its last is
        # lateness -- which is freshness's job, not this check's, and counting
        # it here would double-report every feed still waiting for today.
        first, last = min(dates), max(dates)
        expected = {d for d in window if first <= d <= last}
        how = cadence.get(name, "daily")

        if how == "weekly":
            # Ask only that each ISO week containing COB dates saw at least
            # one delivery. Weeks rather than "at most N days between
            # deliveries" because a week is unambiguous and needs no calendar:
            # a run of holidays shortens the week's COB dates without changing
            # which week they are in.
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
    # Named separately as well as flagged per feed, because this is the number
    # that decides whether the check ANSWERED, and `total_missing: 0` beside
    # it would otherwise read as a pass.
    report["unreadable_feeds"] = sorted(unreadable)
    # Reported beside it and NOT counted with it: a feed that has never
    # delivered is not a broken check.
    report["feeds_without_a_table"] = sorted(absent)
    report["feeds_never_delivered"] = sorted(
        n for n in never_delivered if n in per_feed and not per_feed[n])
    return report


def run(lookback: int | None = None) -> dict:
    """Read every checked feed's raw table and report gaps."""
    if lookback is None:
        lookback = retention_policy("raw").get("keep_business_days", 10)

    checked, skipped, cadence = {}, [], {}
    unreadable: dict[str, str] = {}
    absent: dict[str, str] = {}
    spark = spark_session("completeness", ref="main")
    try:
        for name, fd in feeds().items():
            # A feed that does not deliver daily would show every non-delivery
            # day as a gap, so opting out must be possible -- and must be
            # explicit, or a feed silently drops out of the check.
            if getattr(fd, "delivery_expected", True) is False:
                skipped.append(name)
                continue
            cadence[name] = getattr(fd, "cadence", "daily")
            try:
                checked[name] = observed_dates(spark, fd.raw_table)
            except Exception as exc:
                # NOT `checked[name] = set()`. That is the answer for a table
                # that exists and is empty, and this one is "we do not know" --
                # which `find_gaps` would render as `no data`, contribute 0 to
                # `total_missing`, and pass.
                #
                # Spark names the one benign case exactly, so it is matched on
                # the ERROR CLASS rather than guessed at from the prose; an
                # error class that changes falls through to `unreadable`,
                # which is the conservative direction.
                why = str(exc)[:200]
                if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
                    log.warning(
                        "%s does not exist. Every prepared build fails until "
                        "it does: run `python -m "
                        "reporting_platform.ingest.migrate_raw`", fd.raw_table)
                    absent[name] = why
                else:
                    log.error("cannot read %s: %s", fd.raw_table, why)
                    unreadable[name] = why
    finally:
        spark.stop()

    # An empty table no longer means "never delivered" -- see the module
    # header -- so ask the registry, for the empty ones only.
    empty = [name for name, dates in checked.items() if not dates]
    never, registry_error = set(), None
    if empty:
        try:
            never = set(empty) - delivered_feeds(empty)
        except Exception as exc:                            # noqa: BLE001
            registry_error = str(exc)[:200]
            log.error("cannot read the delivery registry, so %s may or may "
                      "not ever have delivered: %s", ", ".join(empty),
                      registry_error)

    report = find_gaps(checked, lookback, cadence, unreadable, absent,
                       never, registry_error)
    report["skipped_feeds"] = skipped
    for name in report["unreadable_feeds"]:
        log.error("feed %s was NOT CHECKED: its raw table could not be read. "
                  "This report says nothing about its completeness.", name)
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
                   help="COB dates to check back over "
                        "(default: raw layer's keep_business_days)")
    p.add_argument("--fail-on-gap", action="store_true",
                   help="exit non-zero if any gap is found, or if any feed's "
                        "raw table could not be read")
    a = p.parse_args(argv)
    report = run(a.lookback)
    print(json.dumps(report, indent=2, default=str))
    # A feed that could not be READ fails this too. The check's promise is
    # that it looked; a green exit for a feed it never managed to look at is
    # the failure this whole module exists to prevent, one level up.
    if a.fail_on_gap and (report["total_missing"] or report["unreadable_feeds"]):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
