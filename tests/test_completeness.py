"""Gap detection: the calendar inference, without a cluster.

`monitoring/completeness.py` splits `find_gaps` out from its Spark reads for
exactly this -- its docstring says so -- and the calendar inference is the part
with the edge cases: a business day is a day on which SOME feed delivered, a
feed is only accountable inside its own history, and a weekly feed is judged by
ISO week rather than by day.

The case worth having a test for above all others is the one where the check
LIES: a raw table it could not read reports as a feed with no data, contributes
nothing to `total_missing`, and passes `--fail-on-gap`. A monitor that goes
green on a table it never opened is worse than no monitor.

What this cannot tell you is whether `observed_dates` reads the right column
out of a real raw table, or whether `run()` reaches a Nessie branch -- those
need Spark and were verified by running them.
"""
from __future__ import annotations

from datetime import date

from reporting_platform.monitoring.completeness import find_gaps


def _d(*days: str) -> set[date]:
    return {date.fromisoformat(s) for s in days}


def _feed(report: dict, name: str) -> dict:
    return next(f for f in report["feeds"] if f["feed"] == name)


# ---------------------------------------------- the calendar is corroborated
def test_a_date_another_feed_delivered_on_is_a_gap():
    report = find_gaps({"a": _d("2026-08-03", "2026-08-04", "2026-08-05"),
                        "b": _d("2026-08-03", "2026-08-05")}, lookback=0)
    assert _feed(report, "b")["missing"] == ["2026-08-04"]
    assert _feed(report, "a")["status"] == "complete"
    assert report["total_missing"] == 1


def test_a_day_every_feed_missed_is_invisible():
    """Stated as a blind spot in the module header, so it is pinned here.

    Nothing corroborates 2026-08-04, so it is not a business day as far as
    this check is concerned. A platform-wide outage is somebody else's alert.
    """
    report = find_gaps({"a": _d("2026-08-03", "2026-08-05"),
                        "b": _d("2026-08-03", "2026-08-05")}, lookback=0)
    assert report["total_missing"] == 0


def test_dates_before_a_feeds_first_delivery_are_not_gaps():
    """They are history it does not have. A new feed must not light up red
    for every date the platform ran before it existed.
    """
    report = find_gaps({"old": _d("2026-08-03", "2026-08-04", "2026-08-05"),
                        "new": _d("2026-08-05")}, lookback=0)
    assert _feed(report, "new")["missing"] == []


def test_dates_after_a_feeds_last_delivery_are_lateness_not_gaps():
    """Otherwise every feed still waiting for today is reported every day,
    which is freshness's job and would make this check unreadable.
    """
    report = find_gaps({"early": _d("2026-08-03", "2026-08-04", "2026-08-05"),
                        "late": _d("2026-08-03", "2026-08-04")}, lookback=0)
    assert _feed(report, "late")["missing"] == []


def test_the_window_counts_observed_dates_not_calendar_days():
    """`lookback` is business days, the same currency `keep_business_days`
    uses -- so a run of holidays does not silently shorten what is checked.
    """
    per_feed = {"a": _d("2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"),
                "b": _d("2026-08-03", "2026-08-06")}
    windowed = find_gaps(per_feed, lookback=2)
    assert windowed["cob_dates_in_window"] == 2
    # Only 08-05 is inside the two-date window; 08-04 is out of scope.
    assert _feed(windowed, "b")["missing"] == ["2026-08-05"]


def test_lookback_zero_means_the_whole_history():
    per_feed = {"a": _d("2026-08-03", "2026-08-04", "2026-08-05"),
                "b": _d("2026-08-03", "2026-08-05")}
    assert find_gaps(per_feed, lookback=0)["cob_dates_in_window"] == 3


def test_no_feeds_at_all_is_an_empty_report_not_a_crash():
    report = find_gaps({}, lookback=10)
    assert report["feeds"] == [] and report["total_missing"] == 0


# ------------------------------------------------------------------ weekly
def test_a_weekly_feed_is_judged_by_week_not_by_day():
    """Daily judgement produced five false gaps on the seed's `rating` feed."""
    daily = _d("2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06",
               "2026-08-07", "2026-08-10")
    report = find_gaps({"daily": daily, "weekly": _d("2026-08-03", "2026-08-10")},
                       lookback=0, cadence={"weekly": "weekly"})
    assert _feed(report, "weekly")["missing"] == []
    assert _feed(report, "weekly")["cadence"] == "weekly"


def test_a_weekly_feed_that_skipped_a_whole_week_is_a_gap():
    daily = _d("2026-08-03", "2026-08-10", "2026-08-17")
    report = find_gaps({"daily": daily, "weekly": _d("2026-08-03", "2026-08-17")},
                       lookback=0, cadence={"weekly": "weekly"})
    assert _feed(report, "weekly")["missing"] == ["2026-W33"]


# --------------------------------------------- unreadable is not "no data"
def test_a_feed_with_an_empty_table_reads_as_no_data():
    report = find_gaps({"a": _d("2026-08-03"), "empty": set()}, lookback=0)
    assert _feed(report, "empty")["status"] == "no data"
    assert report["unreadable_feeds"] == []


def test_a_table_that_could_not_be_read_is_not_a_table_with_no_data():
    """THE FAILURE THIS SPLIT EXISTS TO CATCH.

    Both answers are an absence of observed dates, and reporting the first as
    the second is a green check on a table nobody opened -- no gaps, no
    `--fail-on-gap`, nothing on screen but a feed with "no data".
    """
    report = find_gaps({"a": _d("2026-08-03")}, lookback=0,
                       unreadable={"broken": "Table or view not found"})
    entry = _feed(report, "broken")
    assert entry["status"] == "unreadable"
    assert "not found" in entry["error"]
    assert report["unreadable_feeds"] == ["broken"]


def test_an_unread_feed_is_not_evidence_that_a_date_was_a_business_day():
    """It contributes nothing to the inferred calendar. A broken read must not
    be able to shrink -- or widen -- the window every other feed is judged in.
    """
    per_feed = {"a": _d("2026-08-03", "2026-08-04")}
    with_broken = find_gaps(per_feed, lookback=0,
                            unreadable={"broken": "boom"})
    without = find_gaps(per_feed, lookback=0)
    assert with_broken["cob_dates_in_window"] == without["cob_dates_in_window"]
    assert with_broken["total_missing"] == without["total_missing"]


def test_an_unreadable_feed_carries_no_missing_periods_of_its_own():
    """So `total_missing` alone cannot describe the run, which is why
    `unreadable_feeds` is a separate key and the CLI reads both.
    """
    report = find_gaps({"a": _d("2026-08-03")}, lookback=0,
                       unreadable={"broken": "boom"})
    assert _feed(report, "broken")["missing"] == []
    assert report["total_missing"] == 0
    assert report["unreadable_feeds"]


def test_unreadable_feeds_is_always_present_so_a_reader_can_rely_on_it():
    assert find_gaps({}, lookback=0)["unreadable_feeds"] == []


def test_a_feed_that_has_never_delivered_has_no_table_and_that_is_not_a_defect():
    """THREE ANSWERS, NOT TWO. Spark names this case exactly
    (`TABLE_OR_VIEW_NOT_FOUND`), and it is the ordinary state of a feed
    declared in `feeds.yml` that has not arrived yet -- not a broken monitor.
    """
    report = find_gaps({"a": _d("2026-08-03")}, lookback=0,
                       absent={"brand_new": "TABLE_OR_VIEW_NOT_FOUND ..."})
    assert _feed(report, "brand_new")["status"] == "no table"
    assert report["feeds_without_a_table"] == ["brand_new"]
    assert report["unreadable_feeds"] == []


def test_the_two_absences_are_counted_apart():
    """Only one of them says the check failed to look."""
    report = find_gaps({}, lookback=0, unreadable={"broken": "boom"},
                       absent={"brand_new": "not found"})
    assert report["unreadable_feeds"] == ["broken"]
    assert report["feeds_without_a_table"] == ["brand_new"]
