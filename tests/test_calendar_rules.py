"""The COB-date keep-set: what retention decides to delete.

`common/calendar_rules.py` is pure arithmetic over a list of dates, and it is
the last thing between `retention.py` and a `DELETE FROM`. Its own docstring
records what a wrong answer here did last time -- "quietly shortened the
retention window by one month, every month" -- which is exactly the shape of
bug nothing else in this repo would notice: no error, no red run, a keep-set
one date short and a table that has already been written.

Nothing here needs the stack, and nothing here needs config: `keep_set` takes
the policy as an argument. What these cannot tell you is whether the dates fed
in are the dates the table actually holds -- `observed_dates` reads that from
Spark, and it was verified by running it.
"""
from __future__ import annotations

from datetime import date

from reporting_platform.common.calendar_rules import (expire_set, keep_set,
                                                      month_end_dates)


def _d(*days: str) -> list[date]:
    return [date.fromisoformat(s) for s in days]


# ------------------------------------------------------------- month ends
def test_the_month_in_progress_is_not_a_month_end():
    """The whole subtlety, and the bug the docstring records.

    August is still arriving, so its latest date is just the latest date. If
    it counted as a month-end it would spend a `keep_month_ends` slot on a
    date `keep_business_days` is already holding, and push the oldest genuine
    month-end out of the keep set a month early -- permanently, since
    retention deletes.
    """
    observed = _d("2026-06-29", "2026-06-30", "2026-07-30", "2026-07-31",
                  "2026-08-03", "2026-08-04")
    assert month_end_dates(observed) == _d("2026-06-30", "2026-07-31")


def test_the_month_in_progress_is_available_on_request():
    observed = _d("2026-07-31", "2026-08-03")
    assert month_end_dates(observed, include_current=True) == \
        _d("2026-07-31", "2026-08-03")


def test_a_single_month_has_no_complete_month():
    """Not an error, and not "the last date". Nothing has closed yet."""
    assert month_end_dates(_d("2026-08-03", "2026-08-04")) == []


def test_no_dates_at_all():
    assert month_end_dates([]) == []


def test_month_end_is_the_last_observed_date_not_the_calendar_last():
    """31 March 2029 is a Saturday. There is no such COB date.

    The module header's own example, pinned: a calendar-driven rule would look
    for the 31st, not find it, and either skip the month or keep nothing.
    """
    observed = _d("2029-03-29", "2029-03-30", "2029-04-02")
    assert month_end_dates(observed) == _d("2029-03-30")


def test_a_month_nobody_delivered_in_is_simply_absent():
    """No holiday calendar, so a skipped month is not a gap to be filled."""
    observed = _d("2026-06-30", "2026-08-31", "2026-09-01")
    assert month_end_dates(observed) == _d("2026-06-30", "2026-08-31")


def test_dates_need_not_arrive_sorted():
    observed = _d("2026-07-31", "2026-06-30", "2026-07-01", "2026-08-03")
    assert month_end_dates(observed) == _d("2026-06-30", "2026-07-31")


# -------------------------------------------------------------- keep set
def test_business_days_keeps_the_n_most_recent():
    observed = _d("2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06")
    assert keep_set(observed, keep_business_days=2) == \
        set(_d("2026-08-05", "2026-08-06"))


def test_business_days_counts_observed_dates_not_calendar_days():
    """A run of holidays must not silently shorten the window.

    Four observed dates spanning three weeks: "the last 3" is three dates,
    not three days.
    """
    observed = _d("2026-08-03", "2026-08-14", "2026-08-21", "2026-08-28")
    assert keep_set(observed, keep_business_days=3) == \
        set(_d("2026-08-14", "2026-08-21", "2026-08-28"))


def test_asking_for_more_than_exists_keeps_everything():
    observed = _d("2026-08-03", "2026-08-04")
    assert keep_set(observed, keep_business_days=99) == set(observed)


def test_the_two_rules_are_a_union_not_a_choice():
    """A date survives under either rule. The month-end is far outside the
    recent window and is kept anyway; that is the whole point of having both.
    """
    observed = _d("2026-06-30", "2026-07-31", "2026-08-03", "2026-08-04")
    assert keep_set(observed, keep_business_days=1, keep_month_ends=2) == \
        set(_d("2026-06-30", "2026-07-31", "2026-08-04"))


def test_years_is_months_and_the_larger_of_the_two_wins():
    """`keep_month_ends_years` is 12 month-ends per year, and it does not
    OVERRIDE `keep_month_ends` -- it competes with it. A policy declaring both
    keeps the longer window, which is the safe direction for a rule that
    deletes.
    """
    observed = [date(2024, m, 28) for m in range(1, 13)] + \
        [date(2025, m, 28) for m in range(1, 13)] + [date(2026, 1, 5)]
    one_year = keep_set(observed, keep_month_ends_years=1)
    assert len(one_year) == 12
    assert min(one_year) == date(2025, 1, 28)
    # 20 month-ends beats 12, rather than being replaced by them.
    assert len(keep_set(observed, keep_month_ends=20,
                        keep_month_ends_years=1)) == 20


def test_nothing_observed_keeps_nothing():
    assert keep_set([], keep_business_days=10) == set()


def test_a_policy_that_declares_neither_rule_keeps_nothing():
    """Pinned because it is surprising, not because it is convenient.

    `retention.yml` always declares at least one, and `apply_table_retention`
    raises on the empty keep-set (`min()` of an empty set) rather than
    deleting the table quietly -- but the arithmetic here is what makes that
    the outcome, so it is written down.
    """
    assert keep_set(_d("2026-08-03", "2026-08-04")) == set()


# ------------------------------------------------------------ expire set
def test_expire_is_observed_minus_kept():
    observed = _d("2026-06-30", "2026-07-31", "2026-08-03", "2026-08-04")
    assert expire_set(observed, {"keep_business_days": 2}) == \
        set(_d("2026-06-30", "2026-07-31"))


def test_expire_reads_the_policy_keys_retention_yml_uses():
    """The three names, spelled as `retention.yml` spells them. A rename on
    one side alone expires everything the other rule was holding.
    """
    observed = _d("2026-06-30", "2026-07-31", "2026-08-03", "2026-08-04")
    policy = {"keep_business_days": 1, "keep_month_ends": 1,
              "keep_month_ends_years": None}
    assert expire_set(observed, policy) == set(_d("2026-06-30", "2026-08-03"))


def test_expire_keeps_duplicates_out_of_the_answer():
    """`observed` comes from a DISTINCT query, but a repeated date must not
    turn into a date that is both kept and expired.
    """
    observed = _d("2026-08-03", "2026-08-03", "2026-08-04")
    assert expire_set(observed, {"keep_business_days": 1}) == \
        set(_d("2026-08-03"))


def test_nothing_expires_when_nothing_was_observed():
    assert expire_set([], {"keep_business_days": 1}) == set()
