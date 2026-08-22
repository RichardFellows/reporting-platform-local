"""Business-date keep-set calculation.

Deliberately derives everything from the dates actually observed in the data
rather than from a holiday calendar. Two reasons:

1. We would otherwise need a maintained holiday calendar per jurisdiction in
   scope, which is a whole dataset and a whole support burden of its own.
2. The question the retention rule is really answering is "the last 10 dates
   we have", which is what report users mean by "the last two weeks". If
   upstream skipped a day, keeping 10 real dates is the correct behaviour and
   a calendar-driven rule would silently keep 9.

Month-end means "the last observed business date within that calendar month",
not the 30th/31st — 31 March 2029 is a Saturday and there is no such date.
"""
from __future__ import annotations

from datetime import date


def month_end_dates(dates: list[date], include_current: bool = False) -> list[date]:
    """Last observed date in each COMPLETE calendar month, ascending.

    "Complete" is the whole subtlety, and getting it wrong quietly shortened
    the retention window by one month, every month.

    The latest date of the month currently in progress is not a month-end; it
    is just today. Counting it as one spends a slot from `keep_month_ends` on a
    date that `keep_business_days` is already retaining, and pushes the oldest
    genuine month-end out of the keep set a month early — permanently, since
    retention deletes.

    A month is treated as complete when a date in a LATER month has been
    observed. That is corroboration from the data rather than a calendar, for
    the same reason the module header gives: we do not want to maintain a
    holiday calendar per jurisdiction, and "has the next month started
    arriving" is a question the data can answer on its own.

    There is no window where this loses a date. Between a real month-end and
    the first delivery of the next month, that date is the most recent one
    observed, so `keep_business_days` retains it; it graduates to a month-end
    exactly when the next month's data proves the month closed.

    `include_current=True` restores the old behaviour for a caller that really
    does want the in-progress month.
    """
    by_month: dict[tuple[int, int], date] = {}
    for d in dates:
        key = (d.year, d.month)
        if key not in by_month or d > by_month[key]:
            by_month[key] = d
    if not include_current and by_month:
        del by_month[max(by_month)]
    return sorted(by_month.values())


def keep_set(
    observed: list[date],
    keep_business_days: int = 0,
    keep_month_ends: int = 0,
    keep_month_ends_years: int | None = None,
) -> set[date]:
    """Compute the set of business dates to retain.

    keep_business_days   the N most recent observed dates
    keep_month_ends      the M most recent month-end dates
    keep_month_ends_years  alternative expressed in years (12 * years month-ends)

    The result is the UNION: a date survives if it qualifies under either rule.
    """
    if not observed:
        return set()

    dates = sorted(set(observed))
    keep: set[date] = set()

    if keep_business_days:
        keep.update(dates[-keep_business_days:])

    n_month_ends = keep_month_ends
    if keep_month_ends_years:
        n_month_ends = max(n_month_ends, keep_month_ends_years * 12)

    if n_month_ends:
        keep.update(month_end_dates(dates)[-n_month_ends:])

    return keep


def expire_set(observed: list[date], policy: dict) -> set[date]:
    """Dates to remove = observed minus keep-set."""
    keep = keep_set(
        observed,
        keep_business_days=policy.get("keep_business_days", 0),
        keep_month_ends=policy.get("keep_month_ends", 0),
        keep_month_ends_years=policy.get("keep_month_ends_years"),
    )
    return set(observed) - keep
