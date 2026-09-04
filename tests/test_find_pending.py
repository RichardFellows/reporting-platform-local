"""`find_pending` over manifests, and the two filters that must survive.

Step 2 changed what this returns (manifest keys, not landing keys) and where
the business date comes from (the manifest, not a regex). What it must NOT
change is which deliveries are pending.
"""
from __future__ import annotations

from datetime import date, timedelta

from tests.fakes3 import FakeS3, install, uninstall
from tests.support import config_dir


def _setup(dates, ingested=()):
    """Land one TRADE file per date, with `already_ingested` stubbed."""
    config_dir()
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import arrival, normalize as norm

    fd = feeds()["fo_trade"]
    keys = []
    for d in dates:
        key = f"landing/fo_trade/TRADE_{d:%Y%m%d}.csv"
        s3.put(key, "trade_id\nT1\n")
        keys.append(key)

    # Spark, and the only thing here that is not pure config + object storage.
    monkey.append((arrival, "already_ingested", arrival.already_ingested))
    arrival.already_ingested = lambda feed: set(ingested)
    return s3, monkey, fd, arrival, norm, keys


def _recent(n):
    """n consecutive weekdays ending today, so the keep-set contains them."""
    out, d = [], date.today()
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


def test_returns_manifest_keys_not_landing_keys():
    s3, monkey, fd, arrival, norm, keys = _setup(_recent(2))
    try:
        pending = arrival.find_pending(fd)
        assert len(pending) == 2, pending
        assert all(p.startswith("ready/fo_trade/") for p in pending), pending
        assert all(p.endswith(".json") for p in pending), pending
    finally:
        uninstall(monkey)


def test_reconciles_before_listing():
    """A file pushed straight into the bucket has no manifest until this runs.

    Without reconciliation it would never become pending and nothing anywhere
    would report it -- the failure this stage exists to prevent.
    """
    s3, monkey, fd, arrival, norm, keys = _setup(_recent(1))
    try:
        assert norm.list_manifests(fd) == []
        assert len(arrival.find_pending(fd)) == 1
        assert len(norm.list_manifests(fd)) == 1
    finally:
        uninstall(monkey)


def test_reconcile_false_is_read_only():
    s3, monkey, fd, arrival, norm, keys = _setup(_recent(1))
    try:
        before = set(s3.objects)
        assert arrival.find_pending(fd, reconcile=False) == []
        assert set(s3.objects) == before, "a read-only call wrote to ready/"
    finally:
        uninstall(monkey)


def test_already_ingested_is_matched_on_the_part_not_the_manifest():
    """`_source_file` holds the PART's key.

    Writing the manifest key into that column would make every delivery look
    un-ingested forever and re-ingest on the next pass -- so this filter has
    to compare against parts.
    """
    days = _recent(2)
    ingested = [f"landing/fo_trade/TRADE_{days[0]:%Y%m%d}.csv"]
    s3, monkey, fd, arrival, norm, keys = _setup(days, ingested=ingested)
    try:
        pending = arrival.find_pending(fd)
        assert len(pending) == 1, pending
        assert days[1].strftime("%Y%m%d") in pending[0], pending
    finally:
        uninstall(monkey)


def test_expired_dates_are_not_resurrected():
    """The keep-set filter, and why it comes from landing/.

    A delivery whose business date retention has already expired is not new.
    Without this filter: ingest -> expire -> re-ingest -> expire, quietly
    undoing the retention policy.

    TWO dates in the same old month, because the keep-set is computed from
    the dates OBSERVED rather than from a calendar (see common/calendar_rules)
    -- a lone old date is the last one in its month, so it is that month's
    month-end and correctly kept. The EARLIER of the pair is the one that is
    neither recent nor a month-end.

    TWELVE recent dates, because `keep_business_days` keeps the 10 most
    recent OBSERVED dates -- with only a handful landed, everything is inside
    the window and nothing can be shown to expire.
    """
    stale, month_end = date(2019, 6, 3), date(2019, 6, 4)
    s3, monkey, fd, arrival, norm, keys = _setup(
        [*_recent(12), stale, month_end])
    try:
        pending = arrival.find_pending(fd)
        assert not any("20190603" in p for p in pending), pending
        assert any("20190604" in p for p in pending), pending
    finally:
        uninstall(monkey)
