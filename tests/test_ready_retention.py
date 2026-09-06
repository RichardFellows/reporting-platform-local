"""`ready/` retention: a cache, swept in days -- but never before ingest, and
never a manifest whose landing object is still there.

Two negative rules, and both are about deleting something that comes straight
back or should never have gone.

  * A manifest whose landing object is still in `landing/` is KEPT at any age.
    `normalize.reconcile` recreates one for every landing object that lacks
    one, so sweeping it is a nightly no-op that both subsystems log as success
    -- and the manifest is what `deliveries.reconcile` rebuilds the registry
    from, so it is not merely a queue entry.
  * A manifest whose parts are not yet in the raw table has its derived parts
    left alone regardless of age. Deleting them is not data loss -- landing
    still holds the object -- but nothing re-normalizes it on its own, so it
    is a SILENT drop.

See reporting_platform/retention/ready.py.
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


def test_old_and_ingested_manifest_is_kept():
    """The churn this module was fixed for.

    Old, ingested, every part pointing back into `landing/` -- so there is
    nothing derived to reclaim, and deleting the manifest would only give
    `normalize.reconcile` something to recreate tonight.
    """
    s3, monkey, fd, ready = _setup(ingested=_all_ingested())
    try:
        report = ready.sweep_feed(fd, date.today() - timedelta(days=7),
                                  dry_run=False)
        assert report["parts_deleted"] == 0, report
        assert report["manifests_deleted"] == 0, report
        assert report["retained"] == 2, report
        assert "ready/fo_trade/TRADE_20190603.csv.json" in s3.objects
    finally:
        uninstall(monkey)


def test_the_sweep_survives_its_own_reconcile():
    """The end of it, stated as the property rather than as a count.

    Sweep, then reconcile, and nothing may have been recreated -- otherwise
    the two subsystems are undoing each other and both are reporting success.
    """
    s3, monkey, fd, ready = _setup(ingested=_all_ingested())
    try:
        from reporting_platform.ingest import normalize as norm

        ready.sweep_feed(fd, date.today() - timedelta(days=7), dry_run=False)
        after_sweep = set(s3.objects)
        created = norm.reconcile(fd)["created"]
        assert created == [], created
        assert set(s3.objects) == after_sweep
    finally:
        uninstall(monkey)


def test_an_orphaned_manifest_is_swept_at_any_age():
    """Landing has let it go, so nothing recreates it. The one lasting
    reclaim a manifest offers."""
    s3, monkey, fd, ready = _setup(ingested=_all_ingested())
    try:
        del s3.objects[f"landing/fo_trade/TRADE_{RECENT:%Y%m%d}.csv"]
        report = ready.sweep_feed(fd, date.today() - timedelta(days=7),
                                  dry_run=False)
        assert report["manifests_deleted"] == 1, report
        assert f"ready/fo_trade/TRADE_{RECENT:%Y%m%d}.csv.json" not in s3.objects
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
        del s3.objects[f"landing/fo_trade/TRADE_{RECENT:%Y%m%d}.csv"]
        before = set(s3.objects)
        report = ready.sweep_feed(fd, date.today() - timedelta(days=7),
                                  dry_run=True)
        assert report["manifests_deleted"] == 1, report
        assert set(s3.objects) == before
    finally:
        uninstall(monkey)
