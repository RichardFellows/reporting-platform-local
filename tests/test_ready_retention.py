"""`ready/` retention: a cache, swept in days -- but never before ingest.

The rule that matters is negative: a manifest whose parts are not yet in the
raw table is left alone regardless of age. Sweeping one is not data loss --
landing still holds the object -- but nothing re-normalizes it on its own, so
it is a SILENT drop. See reporting_platform/retention/ready.py.
"""
from __future__ import annotations

from datetime import date, timedelta

from tests.fakes3 import FakeS3, install, uninstall
from tests.support import config_dir

OLD = date(2019, 6, 3)
RECENT = date.today() - timedelta(days=1)


def _setup(ingested=()):
    config_dir()
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import arrival, normalize as norm
    from reporting_platform.retention import ready

    fd = feeds()["fo_trade"]
    for d in (OLD, RECENT):
        s3.put(f"landing/fo_trade/TRADE_{d:%Y%m%d}.csv", "trade_id\nT1\n")
    norm.reconcile(fd)

    monkey.append((ready, "already_ingested", ready.already_ingested))
    ready.already_ingested = lambda feed: set(ingested)
    return s3, monkey, fd, ready


def _all_ingested():
    return [f"landing/fo_trade/TRADE_{d:%Y%m%d}.csv" for d in (OLD, RECENT)]


def test_old_and_ingested_is_swept():
    s3, monkey, fd, ready = _setup(ingested=_all_ingested())
    try:
        report = ready.sweep_feed(fd, date.today() - timedelta(days=7),
                                  dry_run=False)
        assert report["expired"] == 1, report
        assert report["retained"] == 1, report
        assert "ready/fo_trade/TRADE_20190603.csv.json" not in s3.objects
    finally:
        uninstall(monkey)


def test_old_but_not_ingested_is_held():
    """The rule this module exists for."""
    s3, monkey, fd, ready = _setup(ingested=[])
    try:
        report = ready.sweep_feed(fd, date.today() - timedelta(days=7),
                                  dry_run=False)
        assert report["expired"] == 0, report
        assert report["held_uningested"] == 1, report
        assert "ready/fo_trade/TRADE_20190603.csv.json" in s3.objects
    finally:
        uninstall(monkey)


def test_the_landing_object_is_never_deleted():
    """`ready/` is derived. The evidence copy is not this job's to touch."""
    s3, monkey, fd, ready = _setup(ingested=_all_ingested())
    try:
        ready.sweep_feed(fd, date.today() - timedelta(days=7), dry_run=False)
        assert "landing/fo_trade/TRADE_20190603.csv" in s3.objects
    finally:
        uninstall(monkey)


def test_dry_run_deletes_nothing():
    s3, monkey, fd, ready = _setup(ingested=_all_ingested())
    try:
        before = set(s3.objects)
        report = ready.sweep_feed(fd, date.today() - timedelta(days=7),
                                  dry_run=True)
        assert report["expired"] == 1, report
        assert set(s3.objects) == before
    finally:
        uninstall(monkey)
