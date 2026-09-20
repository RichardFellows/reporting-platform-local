"""Cutover readiness as derived evidence (Phase 8, sections 19-22/37/41).

Drives `acceptance._evaluate` directly with in-memory comparison rows -- the
pure computation, same separation `test_validation.py` draws around
`parse_run_results` versus the Postgres-touching `record`/`for_delivery`.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from reporting_platform.migration.acceptance import NOT_READY, NOT_STARTED, READY, _evaluate

POLICY = {"consecutive_successes": 3, "allow_warnings": False}


def _row(day: int, outcome: str, ts: datetime | None = None) -> dict:
    return {"business_date": date(2026, 9, day), "outcome": outcome,
           "checkpoint": "raw", "comparison_id": f"mcmp_{day}_{outcome}",
           "message": None,
           "executed_at": ts or datetime(2026, 9, day, tzinfo=timezone.utc)}


def test_no_evidence_is_not_started():
    result = _evaluate("f", "dual_run", [], POLICY)
    assert result["status"] == NOT_STARTED


def test_reaching_the_required_streak_is_ready():
    rows = [_row(d, "PASS") for d in (17, 18, 19)]
    result = _evaluate("f", "dual_run", rows, POLICY)
    assert result["status"] == READY
    assert result["streak"] == 3


def test_a_blocking_failure_resets_the_streak_per_policy():
    """Section 19's worked example: 9 PASS, 1 FAIL, 3 PASS -> streak counts
    only since the last blocking outcome."""
    rows = ([_row(d, "PASS") for d in range(1, 10)]
           + [_row(10, "FAIL")]
           + [_row(d, "PASS") for d in (11, 12, 13)])
    result = _evaluate("f", "dual_run", rows, POLICY)
    assert result["streak"] == 3
    assert result["status"] == READY  # required is 3 in this policy

    stricter = {**POLICY, "consecutive_successes": 10}
    result2 = _evaluate("f", "dual_run", rows, stricter)
    assert result2["status"] == NOT_READY


def test_warn_counts_only_when_configured():
    rows = [_row(d, "WARN") for d in (17, 18, 19)]
    assert _evaluate("f", "dual_run", rows, POLICY)["status"] == NOT_READY
    lenient = {**POLICY, "allow_warnings": True}
    assert _evaluate("f", "dual_run", rows, lenient)["status"] == READY


def test_error_breaks_the_streak_like_a_failure():
    """An ERROR is not comparable, but it must not silently count as
    success -- readiness requires ACTUAL passing evidence, not an absence of
    failure."""
    rows = ([_row(d, "PASS") for d in (17, 18, 19)]
           + [_row(20, "ERROR")]
           + [_row(21, "PASS")])
    result = _evaluate("f", "dual_run", rows, POLICY)
    assert result["streak"] == 1


def test_restated_delivery_supersedes_the_earlier_comparison_for_its_date():
    """Section 27: two comparison rows for the SAME business date (an
    original delivery, then a correction) -- acceptance uses the latest."""
    original = _row(17, "FAIL", ts=datetime(2026, 9, 17, 1, tzinfo=timezone.utc))
    corrected = _row(17, "PASS", ts=datetime(2026, 9, 17, 5, tzinfo=timezone.utc))
    rows = [original, corrected, _row(18, "PASS"), _row(19, "PASS")]
    result = _evaluate("f", "dual_run", rows, POLICY)
    assert result["status"] == READY
    assert result["evidence_count"] == 3  # 17 collapses to one date


def test_changing_policy_does_not_rewrite_history_it_only_reinterprets_it():
    """Section 37: raising the bar changes today's verdict, not the rows."""
    rows = [_row(d, "PASS") for d in (17, 18, 19)]
    easy = _evaluate("f", "dual_run", rows, {"consecutive_successes": 3,
                                            "allow_warnings": False})
    hard = _evaluate("f", "dual_run", rows, {"consecutive_successes": 10,
                                            "allow_warnings": False})
    assert easy["status"] == READY
    assert hard["status"] == NOT_READY
    assert easy["evidence_count"] == hard["evidence_count"] == 3
