"""Cutover readiness as DERIVED evidence (Phase 8, sections 19-22).

`evaluate_acceptance()` never writes anything and never flips
`migration.mode` -- it reads `registry.migration_comparison` and the feed's
own `migration.acceptance` policy and answers READY/NOT_READY with reasons.
The policy is read fresh each call, so raising `consecutive_successes` from
10 to 20 changes today's READY/NOT_READY answer immediately without
rewriting a single historical row (section 37) -- readiness is a live
question, comparison evidence is the historical record.

RESTATEMENT SEMANTICS (section 27, resolved here): a business date may have
more than one comparison row (an original delivery, then a corrected one).
Acceptance uses the LATEST comparison per business date -- the corrected
delivery's evidence supersedes the earlier one FOR ACCEPTANCE PURPOSES, while
both rows remain in the evidence table forever, distinct and queryable. This
is a choice, not the only reasonable one, and it is documented in
docs/MIGRATION.md#restatements: an operator who wants "every attempt must
pass" rather than "the latest attempt must pass" is asking a different, valid
question that this function does not answer.
"""
from __future__ import annotations

from typing import Any

from reporting_platform.migration import evidence

READY, NOT_READY, NOT_STARTED = "READY", "NOT_READY", "NOT_STARTED"


def _latest_per_date(comparisons: list[dict]) -> list[dict]:
    """One row per business_date -- the most recently EXECUTED comparison."""
    by_date: dict = {}
    for row in comparisons:
        bd = row["business_date"]
        existing = by_date.get(bd)
        if existing is None or row["executed_at"] > existing["executed_at"]:
            by_date[bd] = row
    return [by_date[bd] for bd in sorted(by_date)]


def _evaluate(feed_name: str, mode: str, comparisons: list[dict],
              policy: dict[str, Any]) -> dict[str, Any]:
    """The pure computation, independent of where `comparisons` came from --
    tested directly in `tests/test_migration_acceptance.py` against in-memory
    rows, the same separation `registry/validation.py`'s `parse_run_results`
    draws between parsing (pure, tested) and persistence (verified live)."""
    required = policy.get("consecutive_successes", 10)
    allow_warnings = bool(policy.get("allow_warnings", False))

    comparisons = _latest_per_date(comparisons)
    if not comparisons:
        return {"feed": feed_name, "status": NOT_STARTED,
                "mode": mode, "required": required,
                "streak": 0, "evidence_count": 0, "reasons": [
                    "no comparison evidence recorded for this feed yet"]}

    def _is_success(row: dict) -> bool:
        if row["outcome"] == "PASS":
            return True
        if row["outcome"] == "WARN" and allow_warnings:
            return True
        return False

    # Walk from the most recent business date backwards, counting an
    # unbroken run of successes -- section 19's worked example ("9 PASS, 1
    # FAIL, 3 PASS -> streak is 3, counted since the last blocking outcome").
    streak = 0
    blocking_break: dict | None = None
    for row in reversed(comparisons):
        if _is_success(row):
            streak += 1
            continue
        blocking_break = row
        break

    status = READY if streak >= required else NOT_READY
    reasons: list[str] = []
    if status == READY:
        reasons.append(
            f"{streak} consecutive successful comparison(s), "
            f"meeting the configured {required}")
    else:
        reasons.append(
            f"only {streak} consecutive successful comparison(s) since the "
            f"last blocking outcome, needs {required}")
        if blocking_break is not None:
            reasons.append(
                f"last blocking outcome: {blocking_break['outcome']} on "
                f"{blocking_break['business_date']} "
                f"({blocking_break.get('message') or 'no message recorded'})")

    return {
        "feed": feed_name, "status": status, "mode": mode,
        "required": required, "allow_warnings": allow_warnings,
        "streak": streak, "evidence_count": len(comparisons),
        "reasons": reasons,
        "recent": [{"business_date": r["business_date"],
                    "outcome": r["outcome"], "checkpoint": r["checkpoint"],
                    "comparison_id": r["comparison_id"]}
                   for r in comparisons[-required:]],
    }


def evaluate_acceptance(feed) -> dict[str, Any]:
    """`feed` is a `context.Feed`. Returns a dict with `status` in
    (READY, NOT_READY, NOT_STARTED) plus the evidence the verdict rests on.

    Thin wrapper around `_evaluate`: fetches evidence from the registry (not
    unit-tested here -- see `tests/test_validation.py`'s note on why
    Postgres-touching code is verified live, not mocked) and defers all
    actual computation to the pure function.
    """
    policy = (feed.migration or {}).get("acceptance") or {
        "consecutive_successes": 10, "allow_warnings": False}
    comparisons = evidence.for_feed(feed.name, limit=10_000)
    return _evaluate(feed.name, feed.migration_mode, comparisons, policy)


def overview(feeds: list) -> list[dict[str, Any]]:
    """One row per feed -- `context.feeds().values()`, typically -- for the
    migration-wide status table (section 33). A feed still in `legacy` mode
    is reported NOT_STARTED without querying the evidence table at all."""
    out = []
    for fd in feeds:
        if fd.migration_mode == "legacy":
            out.append({"feed": fd.name, "mode": "legacy",
                       "status": NOT_STARTED, "streak": None, "required": None})
            continue
        result = evaluate_acceptance(fd)
        out.append({"feed": fd.name, "mode": result["mode"],
                   "status": result["status"], "streak": result["streak"],
                   "required": result["required"]})
    return out
