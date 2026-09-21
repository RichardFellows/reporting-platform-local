"""COB Feed Status: the pure parts -- expectation and status derivation.

Everything here is `is_expected`/`status_of`/`feed_entry`/`_summarize`, which
take already-fetched facts and touch no database, no S3 and no Airflow -- see
tests/README.md on why nothing that talks to Postgres is covered here.
`build_report`'s own reads (registry.transport_receipt, registry.delivery,
registry.delivery_committed) were exercised against the live stack; see
docs/OPERATIONAL-CONTROL-PLANE.md.

`now` is passed explicitly everywhere, never read from the wall clock, so a
run at any time of day gives the same answer.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from reporting_platform.common.context import Feed
from reporting_platform.monitoring import feed_status as fs

NOW = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
COB = date(2026, 9, 21)


def _feed(**overrides) -> Feed:
    base = dict(name="ratings", description="Credit Ratings",
               source_system="CREDIT",
               filename_pattern=r"RATING_(?P<cob_date>\d{8})\.csv",
               business_key=["counterparty_id"],
               columns=["counterparty_id", "rating"])
    base.update(overrides)
    return Feed(**base)


# --------------------------------------------------------------- is_expected
def test_delivery_expected_false_opts_out_entirely():
    fd = _feed(delivery_expected=False, cadence="daily")
    assert fs.is_expected(fd, COB) is False


def test_daily_is_expected_every_calendar_date():
    fd = _feed(cadence="daily")
    assert fs.is_expected(fd, COB) is True
    # A Saturday too -- no business-day calendar here, by design. See the
    # module header.
    assert fs.is_expected(fd, date(2026, 9, 19)) is True


def test_weekly_is_expected_only_when_the_week_has_not_delivered_yet():
    fd = _feed(cadence="weekly")
    monday = date(2026, 9, 21)
    this_week = {monday.isocalendar()[:2]}
    assert fs.is_expected(fd, monday, weekly_isoweeks_with_delivery=set()) is True
    assert fs.is_expected(
        fd, monday, weekly_isoweeks_with_delivery=this_week) is False
    # A date in a DIFFERENT week is unaffected by this week's delivery.
    next_week = date(2026, 9, 28)
    assert fs.is_expected(
        fd, next_week, weekly_isoweeks_with_delivery=this_week) is True


# ----------------------------------------------------------------- status_of
def test_not_expected_short_circuits_everything_else():
    assert fs.status_of(expected=False, committed=True, has_delivery=True,
                        receipt_status="failed", expected_by="09:00",
                        cob_date=COB, now=NOW) == "NOT_EXPECTED"


def test_no_deadline_declared_is_waiting_forever_not_missing():
    """`expected_by: ''` means no promise was made -- see Feed.expected_by.
    A monitor that invented a deadline here would be reporting policy nobody
    declared, exactly the failure `lateness.py`'s module header names."""
    far_future_now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    status = fs.status_of(expected=True, committed=False, has_delivery=False,
                          receipt_status=None, expected_by="", cob_date=COB,
                          now=far_future_now)
    assert status == "WAITING"


def test_waiting_becomes_missing_only_after_its_own_deadline():
    before = fs.status_of(expected=True, committed=False, has_delivery=False,
                          receipt_status=None, expected_by="22:00",
                          cob_date=COB, now=datetime(2026, 9, 21, 23, 0,
                                                     tzinfo=timezone.utc))
    after = fs.status_of(expected=True, committed=False, has_delivery=False,
                         receipt_status=None, expected_by="22:00",
                         cob_date=COB, now=datetime(2026, 9, 22, 23, 0,
                                                    tzinfo=timezone.utc))
    assert before == "WAITING"
    assert after == "MISSING"


def test_a_discovered_transport_with_no_delivery_yet_is_received():
    status = fs.status_of(expected=True, committed=False, has_delivery=False,
                          receipt_status="discovered", expected_by="22:00",
                          cob_date=COB, now=NOW)
    assert status == "RECEIVED"


def test_a_normalized_delivery_is_processing_even_with_no_receipt():
    """The legacy path has no Transport at all -- `has_delivery` alone must
    be enough, because `registry.delivery` is the ONE signal both paths
    share. See the module header."""
    status = fs.status_of(expected=True, committed=False, has_delivery=True,
                          receipt_status=None, expected_by="22:00",
                          cob_date=COB, now=NOW)
    assert status == "PROCESSING"


def test_a_failed_receipt_with_no_delivery_is_failed():
    status = fs.status_of(expected=True, committed=False, has_delivery=False,
                          receipt_status="failed", expected_by="22:00",
                          cob_date=COB, now=NOW)
    assert status == "FAILED"


def test_a_superseding_success_after_an_earlier_failure_is_not_failed():
    """A corrected re-delivery under a NEW TransportID must read as its own
    progress, not as the earlier attempt's failure -- `has_delivery` is
    checked before `receipt_status`."""
    status = fs.status_of(expected=True, committed=False, has_delivery=True,
                          receipt_status="failed", expected_by="22:00",
                          cob_date=COB, now=NOW)
    assert status == "PROCESSING"


def test_committed_is_complete_regardless_of_receipt_history():
    status = fs.status_of(expected=True, committed=True, has_delivery=True,
                          receipt_status="failed", expected_by="22:00",
                          cob_date=COB, now=NOW)
    assert status == "COMPLETE"


# ----------------------------------------------------------------- feed_entry
def test_feed_entry_flags_a_late_arrival_without_a_late_status():
    fd = _feed(expected_by="07:00")
    late_receipt = {"transport_id": "dcm-1-1", "delivery_id": None,
                    "status": "discovered", "updated_at": NOW,
                    "uploaded_at": datetime(2026, 9, 22, 12, 0,
                                           tzinfo=timezone.utc),
                    "failure_reason": None, "airflow_run_id": None}
    entry = fs.feed_entry(fd, COB, now=NOW, receipts=[late_receipt],
                         deliveries=[], committed=[])
    assert entry["status"] == "RECEIVED"
    assert entry["late"] is True


def test_feed_entry_picks_the_latest_of_several_deliveries():
    fd = _feed()
    older = {"delivery_id": "d1", "received_at": datetime(2026, 9, 21, 20, 0,
                                                          tzinfo=timezone.utc)}
    newer = {"delivery_id": "d2", "received_at": datetime(2026, 9, 21, 21, 0,
                                                          tzinfo=timezone.utc)}
    entry = fs.feed_entry(fd, COB, now=NOW, receipts=[],
                         deliveries=[older, newer], committed=[])
    assert entry["delivery_id"] == "d2"
    assert entry["status"] == "PROCESSING"


def test_summary_counts_only_expected_feeds():
    entries = [
        {"expected": True, "status": "COMPLETE"},
        {"expected": True, "status": "MISSING"},
        {"expected": False, "status": "COMPLETE"},
    ]
    summary = fs._summarize(entries)
    assert summary["expected"] == 2
    assert summary["complete"] == 1
    assert summary["missing"] == 1
