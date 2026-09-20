"""Durable, queryable validation execution evidence (Phase 7).

THIS IS NOT A VALIDATION ENGINE. It records the outcome of controls that
already run somewhere else:

  * Delivery/Transport integrity and identity controls run in
    `reporting_platform/ingest/transport.py` and `delivery.py`, and are
    captured from `airflow/dags/transport_ingest.py`.
  * Raw ingestion controls (schema drift, row-count floors/ceilings, producer
    checksum) run in `reporting_platform/ingest/ingest_feed.py`, which calls
    `record` directly next to each check.
  * Data/business controls are dbt tests. `capture_dbt_task` parses the
    already-durable `manifest.json`/`run_results.json` a dbt task archived to
    `dbt-artifacts/` (`registry/artifacts.py`) into one row per test.

See `docs/VALIDATION.md` for the full model. In short:

  outcome   PASS    control ran, expectation satisfied
            WARN    control ran, breached a warning threshold, continues
            FAIL    control ran, breached a BLOCKING expectation
            ERROR   the control itself could not execute

  severity  blocking | warn | info -- what a FAIL is ALLOWED to do to the
            pipeline, independent of what happened this particular time.

IDENTITY AND IDEMPOTENCY. `validation_id` is computed deterministically from
the (layer, control, subject, attempt) tuple, so retrying the same logical
execution -- an Airflow task retry, a re-run of `capture_dbt_task` against the
same archived artifacts -- writes the SAME row (`ON CONFLICT DO NOTHING`,
never overwritten) instead of a duplicate. A later, genuinely new execution
(a new `run_id`/`try_number`/ingest run) gets a different id and its own row,
so history accumulates rather than being overwritten. See
docs/DECISIONS.md#validation-evidence-is-append-only.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from reporting_platform.registry import db

log = logging.getLogger("registry.validation")

LAYERS = ("delivery", "raw", "dbt")
SEVERITIES = ("blocking", "warn", "info")
OUTCOMES = ("PASS", "WARN", "FAIL", "ERROR")

# dbt's own `run_results.json` status vocabulary, mapped explicitly rather
# than reused -- see the module docstring on FAIL vs ERROR. `skipped` is not
# mapped: a skipped test did not execute (usually because an upstream model
# failed first) and recording it as any of the four would claim it ran.
_DBT_STATUS_TO_OUTCOME = {
    "pass": "PASS",
    "warn": "WARN",
    "fail": "FAIL",
    "error": "ERROR",
}


def _id(*parts: str) -> str:
    """A short, deterministic id from an ordered tuple of identity parts."""
    joined = "\x1f".join(p if p is not None else "" for p in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:24]  # noqa: S324


def record(*, layer: str, control_id: str, outcome: str, severity: str,
          attempt_key: str, run_id: str | None = None,
          execution_ref: str | None = None, feed: str | None = None,
          delivery_id: str | None = None, transport_id: str | None = None,
          control_name: str | None = None, model_name: str | None = None,
          column_name: str | None = None, expected_value: Any = None,
          observed_value: Any = None, failure_count: int | None = None,
          evidence_ref: str | None = None, message: str | None = None,
          executed_at: datetime | None = None) -> dict:
    """Write one durable validation_result row. Idempotent per `attempt_key`.

    `attempt_key` is the caller's own identity for "this one logical
    execution" -- e.g. `f"{run_id}:{delivery_id}"` for a raw check, or
    `f"{run_id}:{task_id}:{try_number}"` for a dbt task. Combined with
    `layer` and `control_id` to form `validation_id`, so the same attempt
    reported twice (a retry re-observing the same outcome) writes once.
    """
    if layer not in LAYERS:
        raise ValueError(f"validation layer must be one of {LAYERS}, got {layer!r}")
    if severity not in SEVERITIES:
        raise ValueError(f"validation severity must be one of {SEVERITIES}, got {severity!r}")
    if outcome not in OUTCOMES:
        raise ValueError(f"validation outcome must be one of {OUTCOMES}, got {outcome!r}")

    row = {
        "validation_id": _id(layer, control_id, attempt_key),
        "run_id": run_id,
        "execution_ref": execution_ref,
        "feed": feed,
        "delivery_id": delivery_id,
        "transport_id": transport_id,
        "layer": layer,
        "control_id": control_id,
        "control_name": control_name,
        "model_name": model_name,
        "column_name": column_name,
        "severity": severity,
        "outcome": outcome,
        "expected_value": None if expected_value is None else str(expected_value),
        "observed_value": None if observed_value is None else str(observed_value),
        "failure_count": failure_count,
        "executed_at": (executed_at or datetime.now(timezone.utc)),
        "evidence_ref": evidence_ref,
        "message": (message or "")[:4000] or None,
    }
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO registry.validation_result "
            "  (validation_id, run_id, execution_ref, feed, delivery_id, "
            "   transport_id, layer, control_id, control_name, model_name, "
            "   column_name, severity, outcome, expected_value, "
            "   observed_value, failure_count, executed_at, evidence_ref, "
            "   message) "
            "VALUES (%(validation_id)s, %(run_id)s, %(execution_ref)s, "
            "  %(feed)s, %(delivery_id)s, %(transport_id)s, %(layer)s, "
            "  %(control_id)s, %(control_name)s, %(model_name)s, "
            "  %(column_name)s, %(severity)s, %(outcome)s, "
            "  %(expected_value)s, %(observed_value)s, %(failure_count)s, "
            "  %(executed_at)s, %(evidence_ref)s, %(message)s) "
            "ON CONFLICT (validation_id) DO NOTHING", row)
    return row


def record_quietly(**kwargs) -> dict | None:
    """`record`, but a registry outage never changes ingestion behaviour.

    Validation evidence is observational (see the module docstring): a
    control that already ran and already decided PASS/FAIL/WARN must not be
    retried or reversed because writing its evidence failed. The gap is
    logged loudly, the same as `rejections.quarantine_quietly`.
    """
    try:
        return record(**kwargs)
    except Exception as exc:                                     # noqa: BLE001
        log.warning("could not record validation evidence (%s/%s): %s",
                    kwargs.get("layer"), kwargs.get("control_id"),
                    f"{type(exc).__name__}: {exc}")
        return None


# ------------------------------------------------------------------- dbt tests

def _dbt_node_context(manifest: dict, unique_id: str) -> dict:
    """model/column/severity for one test node, from `manifest.json`.

    Falls back to bare defaults for a unique_id `manifest.json` does not
    know about (a manifest/run_results pair that do not match) rather than
    raising -- the row is still worth recording with what run_results alone
    gives us.
    """
    nodes = manifest.get("nodes", {})
    node = nodes.get(unique_id)
    if not node:
        # dbt's own shape: `test.<package>.<test_name>.<hash>`. The hash is
        # the last segment and never the readable name.
        parts = unique_id.split(".")
        fallback_name = parts[2] if len(parts) >= 3 else unique_id
        return {"model_name": None, "column_name": None, "severity": "blocking",
                "control_name": fallback_name}
    depends_on = (node.get("depends_on") or {}).get("nodes") or []
    model_name = None
    for dep in depends_on:
        dep_node = nodes.get(dep)
        if dep_node and dep_node.get("resource_type") in ("model", "seed", "snapshot"):
            model_name = dep_node.get("name")
            break
    dbt_severity = str((node.get("config") or {}).get("severity", "error")).lower()
    return {
        "model_name": model_name,
        "column_name": node.get("column_name"),
        # dbt's config severity is 'error'/'warn'; this platform's severity
        # vocabulary is 'blocking'/'warn'/'info' -- mapped explicitly rather
        # than reused verbatim, same reasoning as the outcome map above.
        "severity": "warn" if dbt_severity == "warn" else "blocking",
        "control_name": node.get("name") or unique_id.rsplit(".", 1)[-1],
    }


def parse_run_results(target_path: str | Path) -> list[dict]:
    """Read `manifest.json` + `run_results.json` from a dbt target dir.

    Returns one dict per TEST node dbt actually executed (status in
    `_DBT_STATUS_TO_OUTCOME`; `skipped` nodes are omitted -- see the module
    docstring). Pure and side-effect free: callers decide whether/how to
    persist what this returns.
    """
    target = Path(target_path)
    manifest_path, results_path = target / "manifest.json", target / "run_results.json"
    if not results_path.is_file():
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {"nodes": {}}
    run_results = json.loads(results_path.read_text(encoding="utf-8"))

    out = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        if not unique_id.startswith("test."):
            continue
        status = str(result.get("status", "")).lower()
        outcome = _DBT_STATUS_TO_OUTCOME.get(status)
        if outcome is None:
            continue  # 'skipped', or a status this platform does not model
        ctx = _dbt_node_context(manifest, unique_id)
        message = result.get("message")
        out.append({
            "unique_id": unique_id,
            "outcome": outcome,
            "failure_count": result.get("failures"),
            "message": None if message is None else str(message),
            **ctx,
        })
    return out


def capture_dbt_task(run_id: str, task_id: str, try_number: int, *,
                     target_path: str | Path, evidence_ref: str) -> list[dict]:
    """Persist one dbt task invocation's test results as validation_result rows.

    Called from `airflow/dags/dbt_builds.py`'s existing artifact callback,
    which already runs for BOTH success and failure -- so a failing test task
    gets its evidence recorded exactly like a passing one, before publication
    is even considered. `attempt_key` includes `try_number`, so a task retry
    within the same Airflow run appends a new row for the new attempt rather
    than overwriting the first one's outcome; re-running THIS function against
    the same attempt (e.g. a callback re-invoked) is idempotent.
    """
    tests = parse_run_results(target_path)
    written = []
    for test in tests:
        row = record_quietly(
            layer="dbt",
            control_id=test["unique_id"],
            control_name=test["control_name"],
            model_name=test["model_name"],
            column_name=test["column_name"],
            severity=test["severity"],
            outcome=test["outcome"],
            failure_count=test["failure_count"],
            message=test["message"],
            run_id=run_id,
            evidence_ref=evidence_ref,
            attempt_key=f"{run_id}:{task_id}:{try_number}",
        )
        if row:
            written.append(row)
    return written


# --------------------------------------------------------------------- reads

_COLUMNS = (
    "validation_id, run_id, execution_ref, feed, delivery_id, transport_id, "
    "layer, control_id, control_name, model_name, column_name, severity, "
    "outcome, expected_value, observed_value, failure_count, executed_at, "
    "evidence_ref, message"
)


def _rows(sql: str, args: list) -> list[dict]:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def for_delivery(delivery_id: str, feed: str | None = None) -> list[dict]:
    """Every validation result tied to one Delivery, oldest first."""
    sql = f"SELECT {_COLUMNS} FROM registry.validation_result WHERE delivery_id = %s"
    args: list = [delivery_id]
    if feed:
        sql += " AND feed = %s"
        args.append(feed)
    sql += " ORDER BY executed_at"
    return _rows(sql, args)


def for_transport(transport_id: str) -> list[dict]:
    """Every validation result tied to one Transport (pre-Delivery failures)."""
    return _rows(
        f"SELECT {_COLUMNS} FROM registry.validation_result "
        f"WHERE transport_id = %s ORDER BY executed_at",
        [transport_id])


def for_run(run_id: str) -> list[dict]:
    """Every dbt-layer validation result belonging to one `registry.run`."""
    return _rows(
        f"SELECT {_COLUMNS} FROM registry.validation_result "
        f"WHERE run_id = %s ORDER BY executed_at",
        [run_id])
