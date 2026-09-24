"""Generic comparison strategies (Phase 8, sections 9/10/41). Pure functions."""
from __future__ import annotations

from datetime import date

from reporting_platform.migration.comparators import (
    canonical_row_hash, canonical_value, compare_aggregate, compare_key_set,
    compare_row_count, compare_row_hash, drop_technical_columns, hashes_by_key,
    worst_outcome,
)
from reporting_platform.migration.comparators import ComparisonOutcome


def test_row_count_pass():
    outcome = compare_row_count(100, 100)
    assert outcome.outcome == "PASS"


def test_row_count_fail():
    outcome = compare_row_count(100, 97)
    assert outcome.outcome == "FAIL"
    assert outcome.summary["difference"] == -3


def test_key_set_pass():
    outcome = compare_key_set({("A",), ("B",)}, {("A",), ("B",)})
    assert outcome.outcome == "PASS"


def test_key_set_fail_reports_legacy_only_and_new_only():
    outcome = compare_key_set({("A",), ("B",), ("C",)}, {("B",), ("D",)})
    assert outcome.outcome == "FAIL"
    assert outcome.summary["legacy_only"] == 2  # A, C
    assert outcome.summary["new_only"] == 1     # D
    assert outcome.summary["matched"] == 1      # B


def test_canonical_hash_is_stable_across_equivalent_representations():
    row_a = {"amount": 100, "flag": True, "d": date(2026, 1, 1), "name": "x"}
    row_b = {"amount": 100.0, "flag": True, "d": "2026-01-01", "name": "x"}
    cols = ["amount", "flag", "d", "name"]
    assert canonical_row_hash(row_a, cols) == canonical_row_hash(row_b, cols)


def test_canonical_hash_distinguishes_null_from_empty_string():
    assert canonical_value(None) != canonical_value("")


def test_row_hash_pass_despite_technical_column_differences():
    legacy_rows = [{"id": "A", "amount": 100, "_source_file": "legacy.csv"}]
    new_rows = [{"id": "A", "amount": 100, "_source_file": "s3://.../new.csv",
                "_delivery_id": "dlv_abc", "_ingest_ts": "2026-09-20T00:00:00"}]
    key, columns = ["id"], ["amount"]
    legacy_hashes = hashes_by_key(
        [drop_technical_columns(r) for r in legacy_rows], key, columns)
    new_hashes = hashes_by_key(
        [drop_technical_columns(r) for r in new_rows], key, columns)
    outcome = compare_row_hash(legacy_hashes, new_hashes)
    assert outcome.outcome == "PASS"


def test_row_hash_fail_on_a_changed_business_value():
    legacy_rows = [{"id": "A", "amount": 100}]
    new_rows = [{"id": "A", "amount": 101}]
    key, columns = ["id"], ["amount"]
    legacy_hashes = hashes_by_key(legacy_rows, key, columns)
    new_hashes = hashes_by_key(new_rows, key, columns)
    outcome = compare_row_hash(legacy_hashes, new_hashes)
    assert outcome.outcome == "FAIL"
    assert outcome.summary["different"] == 1


def test_aggregate_within_absolute_tolerance_passes():
    outcome = compare_aggregate(100.0, 100.005, function="sum",
                                tolerance={"absolute": 0.01})
    assert outcome.outcome == "PASS"


def test_aggregate_outside_tolerance_fails():
    outcome = compare_aggregate(100.0, 101.0, function="sum",
                                tolerance={"absolute": 0.01})
    assert outcome.outcome == "FAIL"


def test_aggregate_with_no_tolerance_requires_exact_equality():
    assert compare_aggregate(100.0, 100.0, function="sum").outcome == "PASS"
    assert compare_aggregate(100.0, 100.001, function="sum").outcome == "FAIL"


def test_worst_outcome_ranks_fail_above_error_above_warn_above_pass():
    assert worst_outcome([
        ComparisonOutcome("PASS", {}), ComparisonOutcome("WARN", {}),
    ]) == "WARN"
    assert worst_outcome([
        ComparisonOutcome("WARN", {}), ComparisonOutcome("ERROR", {}),
    ]) == "ERROR"
    assert worst_outcome([
        ComparisonOutcome("ERROR", {}), ComparisonOutcome("FAIL", {}),
    ]) == "FAIL"


def test_invalid_outcome_is_refused():
    try:
        ComparisonOutcome("MAYBE", {})
    except ValueError:
        pass
    else:
        raise AssertionError("expected a refusal")
