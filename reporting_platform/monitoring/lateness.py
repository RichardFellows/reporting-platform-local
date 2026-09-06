"""Deliveries that arrived, but after the time they were promised. REQ-201.

THE ONE LATENESS CONCEPT. Two config keys that pretended to be this --
`arrival_timeout_hours` and `arrival_poke_seconds` -- were deleted in phase 0
for being settings nothing read, and one of them had already been mistaken for
a mechanism once. `Feed.expected_by` is the replacement and it is deliberately
a WALL-CLOCK TIME rather than a duration: what an upstream actually commits to
is "by 07:00", and a duration needs an origin event that a delivery arriving by
PutObject does not have.

THIS IS NOT THE COMPLETENESS CHECK AND MUST NOT BECOME IT. `completeness.py`
asks which business dates a feed is MISSING. This asks, of the deliveries that
did arrive, which arrived late. A date with no delivery at all is a gap, not an
infinitely late delivery, and reporting it here as well would double-report
every outage -- so a date with nothing registered is simply not judged. The two
checks are deliberately separate for the same reason evidence and
reproducibility are: a green from one means something different from a green
from the other.

NO SPARK. The arrival time is `registry.delivery.received_at` -- the landing
object's LastModified, the same value the manifest carries -- so this is
psycopg2 and nothing else and runs in the task process directly.

THE DEADLINE IS `expected_by` ON THE DAY AFTER THE BUSINESS DATE, and the
offset is fixed rather than configurable. A delivery describes a business date,
so that date has to have ENDED before the extract can be taken: a position file
as at Tuesday is produced after Tuesday's close and lands on Wednesday morning.
Fixing it at +1 day rather than adding a second key is a choice in the
FORGIVING direction -- a reference snapshot that legitimately arrives the same
day is judged against a later deadline than it needed, so this check can
under-report lateness and cannot invent it. That is the same direction
`completeness.py` argues for at length: a monitor that cries wolf is a monitor
somebody switches off, and this one is new.

A BACKFILL IS REPORTED AS ONE EVENT. If every late date for a feed arrived on
the same calendar day, that is one bulk load and the log says so instead of
listing ten missed deadlines. The finding is not suppressed -- `total_late` and
`--fail-on-late` are unchanged -- because a backfill of dates that were due
weeks ago genuinely IS late; what changes is that a human reading the log is
told what happened rather than left to notice that ten timestamps are equal.
The seeded stack is exactly this case and is what it was written against.

WHAT IT CANNOT SEE, said plainly: a delivery that is late and has not yet
arrived. It has no row, so there is nothing to be late. That is the gap check's
question up to a point and the watchdog's beyond it.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, time, timedelta, timezone

from reporting_platform.common.context import feeds, retention_policy
from reporting_platform.registry import db

log = logging.getLogger("monitoring.lateness")


def deadline(business_date: date, expected_by: str) -> datetime:
    """When a delivery for `business_date` was due, in UTC.

    `expected_by` has already been validated at load by `parse_expected_by`,
    so this parses rather than checks -- a second copy of the validation here
    would be a second place for the rules to differ.
    """
    hh, mm = expected_by.split(":")
    return datetime.combine(business_date + timedelta(days=1),
                            time(int(hh), int(mm)), tzinfo=timezone.utc)


def is_bulk_load(late: list[dict]) -> bool:
    """Did these late dates all arrive in one load?

    A DERIVATION, NOT A THRESHOLD. More than one business date, and exactly
    one distinct arrival DAY between them, IS a backfill -- there is no
    tolerance to tune and no way for it to be nearly true. Pulled out of
    `run()` so it can be tested without a database.
    """
    return len(late) > 1 and len({x["arrived"][:10] for x in late}) == 1


def run(lookback: int | None = None) -> dict:
    """Late deliveries in the recent window, per feed.

    Bounded by the raw layer's `keep_business_days` for `completeness.py`'s
    reason: a lateness finding about a date that retention is about to expire
    is not actionable, and an unbounded check re-reports the same old finding
    every night until somebody mutes the whole thing.
    """
    if lookback is None:
        lookback = int(retention_policy("raw").get("keep_business_days", 10))

    report: dict = {"lookback": lookback, "feeds": [], "total_late": 0,
                    "bulk_loads": [], "skipped_feeds": []}
    with db.connect() as conn, conn.cursor() as cur:
        for name, fd in sorted(feeds().items()):
            if not fd.expected_by:
                # NOT a finding. A feed with no declared expectation has made
                # no promise, and inventing one to have something to measure
                # is how a monitor starts reporting policy it made up.
                report["skipped_feeds"].append(name)
                continue

            # The last `lookback` business dates this feed actually has, so a
            # feed that has never delivered is not judged against dates it was
            # never party to. FIRST arrival per date: a `_v2` correction
            # landing days later is a re-delivery, not the original being
            # late, and judging the newest would report every corrected date
            # as a missed deadline.
            cur.execute(
                "SELECT business_date, MIN(received_at) AS first_arrival, "
                "       COUNT(*) AS deliveries "
                "FROM registry.delivery WHERE feed = %s "
                "GROUP BY business_date ORDER BY business_date DESC LIMIT %s",
                (name, lookback))
            rows = cur.fetchall()

            late = []
            for business_date, first_arrival, deliveries in rows:
                due = deadline(business_date, fd.expected_by)
                if first_arrival > due:
                    late.append({
                        "business_date": business_date.isoformat(),
                        "due": due.isoformat(),
                        "arrived": first_arrival.isoformat(),
                        "hours_late": round(
                            (first_arrival - due).total_seconds() / 3600, 2),
                        "deliveries": deliveries,
                    })
            # A BULK LOAD IS ONE EVENT, NOT N MISSED DEADLINES. If every late
            # date for this feed arrived on the SAME calendar day, what
            # happened was one backfill -- a seed, a migration, a re-delivery
            # of history after an outage -- and reporting it as ten separate
            # findings buries whatever else the check found. This is a
            # derivation, not a threshold: one distinct arrival day across
            # more than one business date IS a bulk load, by definition.
            #
            # The finding is DESCRIBED differently, not suppressed. `late` and
            # `total_late` are untouched, so `--fail-on-late` still fails and
            # nothing has been quietly forgiven -- only the log line changes,
            # which is the thing a human actually reads.
            #
            # This is what the seeded stack looks like: `generate_feeds.py`
            # writes every historical business date at once, ~17 days behind
            # the current date, so all four feeds report every date late with
            # one arrival timestamp. Verified there before it was written.
            arrival_days = {x["arrived"][:10] for x in late}
            bulk = is_bulk_load(late)
            entry = {"feed": name, "expected_by": fd.expected_by,
                     "dates_checked": len(rows), "late": late,
                     "bulk_load": bulk,
                     "distinct_arrival_days": len(arrival_days)}
            report["feeds"].append(entry)
            report["total_late"] += len(late)
            if bulk:
                report["bulk_loads"].append(name)
                log.warning(
                    "feed %s: %d of %d recent business date(s) are past their "
                    "%s deadline, but all of them arrived on %s -- that is ONE "
                    "backfill, not %d missed deliveries. Dates: %s%s", name,
                    len(late), len(rows), fd.expected_by,
                    sorted(arrival_days)[0], len(late),
                    ", ".join(x["business_date"] for x in late[:5]),
                    "..." if len(late) > 5 else "")
            elif late:
                log.warning(
                    "feed %s promised %s and was late on %d of %d recent "
                    "business date(s); worst %.1fh: %s", name, fd.expected_by,
                    len(late), len(rows), max(x["hours_late"] for x in late),
                    ", ".join(x["business_date"] for x in late[:5]))

    if report["skipped_feeds"]:
        log.info("no `expected_by` declared for %s, so no deadline was "
                 "asserted for them", ", ".join(report["skipped_feeds"]))
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--lookback", type=int, default=None,
                   help="business dates to check back over "
                        "(default: raw layer's keep_business_days)")
    p.add_argument("--fail-on-late", action="store_true",
                   help="exit non-zero if any delivery was late")
    a = p.parse_args(argv)
    report = run(a.lookback)
    print(json.dumps(report, indent=2, default=str))
    return 1 if (a.fail_on_late and report["total_late"]) else 0


if __name__ == "__main__":
    sys.exit(main())
