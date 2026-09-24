"""Durable comparison evidence: `registry.migration_comparison` (Phase 8).

Same append-only, idempotent-by-construction shape as
`registry/validation.py` -- see that module's docstring and
`registry/db.py`'s table comment for the reasoning this deliberately does not
repeat. The one addition this table needs beyond validation's columns is a
LEGACY reference and a CORRELATION key, neither of which fits
`validation_result`'s Delivery/Transport/dbt-node identity.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from reporting_platform.registry import db

log = logging.getLogger("migration.evidence")

OUTCOMES = ("PASS", "WARN", "FAIL", "ERROR")
SEVERITIES = ("blocking", "warn")


def record(*, comparison_id: str, feed: str, business_date: date,
          checkpoint: str, correlation_key: str, new_ref: str,
          legacy_ref: str, comparison_contract_hash: str, outcome: str,
          severity: str, summary: dict[str, Any],
          new_run_id: str | None = None, diff_ref: str | None = None,
          message: str | None = None,
          executed_at: datetime | None = None) -> dict:
    """Write one durable migration_comparison row. Idempotent per
    `comparison_id` (see contract.py: the id already encodes identity)."""
    if outcome not in OUTCOMES:
        raise ValueError(f"outcome must be one of {OUTCOMES}, got {outcome!r}")
    if severity not in SEVERITIES:
        raise ValueError(f"severity must be one of {SEVERITIES}, got {severity!r}")
    row = {
        "comparison_id": comparison_id, "feed": feed,
        "business_date": business_date, "checkpoint": checkpoint,
        "correlation_key": correlation_key, "new_ref": new_ref,
        "new_run_id": new_run_id, "legacy_ref": legacy_ref,
        "comparison_contract_hash": comparison_contract_hash,
        "outcome": outcome, "severity": severity,
        "summary": json.dumps(summary, default=str),
        "diff_ref": diff_ref,
        "executed_at": executed_at or datetime.now(timezone.utc),
        "message": (message or "")[:4000] or None,
    }
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO registry.migration_comparison "
            "  (comparison_id, feed, business_date, checkpoint, "
            "   correlation_key, new_ref, new_run_id, legacy_ref, "
            "   comparison_contract_hash, outcome, severity, summary, "
            "   diff_ref, executed_at, message) "
            "VALUES (%(comparison_id)s, %(feed)s, %(business_date)s, "
            "  %(checkpoint)s, %(correlation_key)s, %(new_ref)s, "
            "  %(new_run_id)s, %(legacy_ref)s, %(comparison_contract_hash)s, "
            "  %(outcome)s, %(severity)s, %(summary)s, %(diff_ref)s, "
            "  %(executed_at)s, %(message)s) "
            "ON CONFLICT (comparison_id) DO NOTHING", row)
    return row


def record_quietly(**kwargs) -> dict | None:
    """`record`, but a registry outage never blocks the comparison itself
    from completing -- same reasoning as `validation.record_quietly`."""
    try:
        return record(**kwargs)
    except Exception as exc:                                     # noqa: BLE001
        log.warning("could not record migration comparison evidence "
                    "(%s/%s): %s", kwargs.get("feed"), kwargs.get("checkpoint"),
                    f"{type(exc).__name__}: {exc}")
        return None


_COLUMNS = (
    "comparison_id, feed, business_date, checkpoint, correlation_key, "
    "new_ref, new_run_id, legacy_ref, comparison_contract_hash, outcome, "
    "severity, summary, diff_ref, executed_at, message"
)


def _rows(sql: str, args: list) -> list[dict]:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def for_feed(feed: str, limit: int = 50) -> list[dict]:
    """Most recent comparisons for one feed, newest first."""
    return _rows(
        f"SELECT {_COLUMNS} FROM registry.migration_comparison "
        f"WHERE feed = %s ORDER BY business_date DESC, executed_at DESC "
        f"LIMIT %s", [feed, limit])


def for_business_date(feed: str, business_date: date) -> list[dict]:
    """Every comparison recorded for one feed and business date -- more than
    one when a restated Delivery produced a distinct, later comparison."""
    return _rows(
        f"SELECT {_COLUMNS} FROM registry.migration_comparison "
        f"WHERE feed = %s AND business_date = %s ORDER BY executed_at",
        [feed, business_date])


def get(comparison_id: str) -> dict | None:
    rows = _rows(
        f"SELECT {_COLUMNS} FROM registry.migration_comparison "
        f"WHERE comparison_id = %s", [comparison_id])
    return rows[0] if rows else None
