"""Generic, engine-agnostic comparison strategies (Phase 8, sections 9-10).

Every function here takes already-summarised, comparison-scale Python data
(counts, sets of key tuples, {key: canonical_hash} maps, scalar aggregates)
-- never raw DataFrames and never a full table's rows. At platform scale the
SUMMARISING happens in Spark (`new_side.py`, mirroring `ingest_feed.py`'s own
Spark-native aggregation) or in whatever the legacy adapter's own query
layer is; what lands here is small by construction, which is what keeps
Airflow XCom and Postgres out of the business of holding datasets (section
39/49's guard list).

Outcome vocabulary is REUSED, not reinvented: PASS/WARN/FAIL/ERROR is
`registry/validation.py`'s vocabulary, because "a control ran and the
expectation held/breached a warning/breached a blocking threshold/could not
execute" is exactly what a migration comparison is. See docs/VALIDATION.md.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable

OUTCOMES = ("PASS", "WARN", "FAIL", "ERROR")

# Columns that exist ONLY because of how the NEW platform ingests data, never
# because of anything the legacy estate would recognise as a business value.
# Excluded by default from column/hash comparisons -- see section 7: "do not
# require the same internal audit timestamps". A caller that genuinely wants
# to compare provenance passes `columns=` explicitly naming one; this list
# only affects the convenience helper `drop_technical_columns`.
TECHNICAL_COLUMNS = frozenset({
    "_cob_date", "_ingest_ts", "_source_file", "_file_version", "_row_number",
    "_batch_id", "_delivery_id", "_received_at", "_schema_version",
    "_source_system",
})


@dataclass(frozen=True)
class ComparisonOutcome:
    """One comparator's verdict. `summary` is what gets persisted -- small,
    aggregate evidence only, never the differing rows (section 13)."""
    outcome: str
    summary: dict[str, Any]
    message: str = ""

    def __post_init__(self):
        if self.outcome not in OUTCOMES:
            raise ValueError(f"outcome must be one of {OUTCOMES}, got {self.outcome!r}")


def drop_technical_columns(row: dict[str, Any],
                          extra: frozenset[str] = frozenset()) -> dict[str, Any]:
    exclude = TECHNICAL_COLUMNS | extra
    return {k: v for k, v in row.items() if k not in exclude}


# --------------------------------------------------------- canonical hashing
def canonical_value(value: Any) -> str:
    """One value -> one deterministic string, regardless of source engine.

    Section 10's checklist, handled explicitly rather than via `str()`:
      * NULL is a distinct sentinel, never the empty string (an empty string
        and NULL must never hash the same).
      * booleans are lower-case 'true'/'false', never Python's 'True'/'1'.
      * dates/timestamps are ISO-8601, so a Spark `date` and a legacy
        adapter's `datetime.date` collapse to the same text.
      * numbers are normalised through `float`, formatted with a fixed
        number of decimal places -- so `100`, `100.0` and Decimal('100.00')
        from three different engines hash identically. This trades exact
        decimal precision for cross-engine determinism, which is the
        stated goal; a comparison that needs exact decimal equality should
        use the `aggregate` strategy's explicit tolerance instead.
    """
    if value is None:
        return "\x00NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return f"{float(value):.10f}"
    # Decimal and anything Decimal-like exposes __float__.
    if hasattr(value, "__float__") and not isinstance(value, str):
        try:
            return f"{float(value):.10f}"
        except (TypeError, ValueError):
            pass
    return str(value).strip()


def canonical_row_hash(row: dict[str, Any], columns: list[str]) -> str:
    """A stable hash over `columns`, in the ORDER GIVEN, from one row dict."""
    material = "\x1f".join(canonical_value(row.get(c)) for c in columns)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()  # noqa: S324


def key_tuple(row: dict[str, Any], key: list[str]) -> tuple:
    return tuple(canonical_value(row.get(k)) for k in key)


def hashes_by_key(rows: list[dict[str, Any]], key: list[str],
                  columns: list[str]) -> dict[tuple, str]:
    """{business_key_tuple: canonical_row_hash} -- what both sides reduce to
    before a `row_hash` comparison, so neither side's raw rows ever meet."""
    return {key_tuple(r, key): canonical_row_hash(r, columns) for r in rows}


# ------------------------------------------------------------- strategies
def compare_row_count(legacy_count: int, new_count: int) -> ComparisonOutcome:
    """Section 9: useful but insufficient alone -- pair with key_set/row_hash."""
    if legacy_count == new_count:
        return ComparisonOutcome(
            "PASS", {"legacy_count": legacy_count, "new_count": new_count})
    return ComparisonOutcome(
        "FAIL", {"legacy_count": legacy_count, "new_count": new_count,
                 "difference": new_count - legacy_count},
        f"row count differs: legacy={legacy_count} new={new_count}")


def compare_key_set(legacy_keys: set, new_keys: set) -> ComparisonOutcome:
    legacy_only = legacy_keys - new_keys
    new_only = new_keys - legacy_keys
    matched = legacy_keys & new_keys
    summary = {"matched": len(matched), "legacy_only": len(legacy_only),
              "new_only": len(new_only)}
    if not legacy_only and not new_only:
        return ComparisonOutcome("PASS", summary)
    return ComparisonOutcome(
        "FAIL", summary,
        f"{len(legacy_only)} key(s) only in legacy, "
        f"{len(new_only)} key(s) only in new")


def compare_row_hash(legacy_hashes: dict[tuple, str],
                     new_hashes: dict[tuple, str]) -> ComparisonOutcome:
    """For keys present on both sides, compare canonical business-value
    hashes. Key presence/absence is `compare_key_set`'s job, not this one --
    a caller typically runs both and takes the worse outcome."""
    common = legacy_hashes.keys() & new_hashes.keys()
    different = [k for k in common if legacy_hashes[k] != new_hashes[k]]
    summary = {"compared": len(common), "matched": len(common) - len(different),
              "different": len(different)}
    if not different:
        return ComparisonOutcome("PASS", summary)
    return ComparisonOutcome(
        "FAIL", summary, f"{len(different)} row(s) differ on business columns")


def compare_aggregate(legacy_value: float, new_value: float, *, function: str,
                      tolerance: dict[str, float] | None = None) -> ComparisonOutcome:
    """A control-total comparison, with EXPLICIT (never global) tolerance.

    `tolerance` may name `absolute` and/or `relative`; either satisfying the
    difference is enough. No tolerance means exact equality is required --
    the deliberately strict default (section 9: "do not introduce fuzzy
    tolerances globally").
    """
    tolerance = tolerance or {}
    diff = abs(float(new_value) - float(legacy_value))
    summary = {"function": function, "legacy_value": legacy_value,
              "new_value": new_value, "difference": diff,
              "tolerance": tolerance}
    if diff == 0:
        return ComparisonOutcome("PASS", summary)
    absolute = tolerance.get("absolute")
    relative = tolerance.get("relative")
    within_absolute = absolute is not None and diff <= absolute
    denom = abs(float(legacy_value)) or 1.0
    within_relative = relative is not None and (diff / denom) <= relative
    if within_absolute or within_relative:
        return ComparisonOutcome("PASS", summary)
    return ComparisonOutcome(
        "FAIL", summary,
        f"{function}({legacy_value}) vs {function}({new_value}): "
        f"difference {diff} exceeds configured tolerance")


def worst_outcome(outcomes: list[ComparisonOutcome]) -> str:
    """FAIL > ERROR > WARN > PASS -- the aggregate outcome across several
    strategies run for one comparison. ERROR outranks WARN: a strategy that
    could not execute is a bigger unknown than one that ran and warned."""
    rank = {"PASS": 0, "WARN": 1, "ERROR": 2, "FAIL": 3}
    if not outcomes:
        return "ERROR"
    return max(outcomes, key=lambda o: rank[o.outcome]).outcome
