"""Probe harness for retention partial-application recovery.

NOT part of the platform. Three jobs:

  snapshot  photograph the retention-relevant state of a set of tables, so a
            crashed-then-resumed run can be compared byte-for-byte against a
            clean one.
  inject    add synthetic OLD business dates by copying an existing date's
            rows. They are chosen to be neither month-ends nor inside the
            10-business-day window, so they -- and only they -- are what
            retention expires. The test therefore destroys only what it
            created.
  crashrun  run the real retention chain with a failure injected at a chosen
            point, reproducing the failure shape to expect: the row DELETE commits
            and the step after it raises.

    python -m scripts._retention_probe snapshot <fqn:layer>...
    python -m scripts._retention_probe inject <fqn> <source_date> <new_date>...
    python -m scripts._retention_probe crashrun <fqn_to_crash_on> <fqn:layer>...
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from reporting_platform.common.context import Nessie, spark_session


def _date_col(layer: str) -> str:
    return "_business_date" if layer == "raw" else "business_date"


def _refs() -> list[dict]:
    return [{"type": r["type"], "name": r["name"], "hash": r["hash"]}
            for r in sorted(Nessie().list_references(),
                            key=lambda r: (r["type"], r["name"]))]


def snapshot(specs: list[str]) -> dict:
    out: dict = {"at": datetime.now(timezone.utc).isoformat(),
                 "refs": _refs(), "tables": []}
    spark = spark_session("probe", ref="main")
    try:
        for spec in specs:
            table, layer = spec.split(":", 1)
            col = _date_col(layer)
            rows = spark.sql(
                f"SELECT {col} AS bd, COUNT(*) AS n FROM {table} "
                f"GROUP BY {col} ORDER BY bd").collect()
            snaps = spark.sql(
                f"SELECT COUNT(*) AS n FROM {table}.snapshots").collect()[0]["n"]
            out["tables"].append({
                "table": table, "layer": layer,
                "total_rows": sum(r["n"] for r in rows),
                "snapshots": snaps,
                "dates": {str(r["bd"]): r["n"] for r in rows},
            })
    finally:
        spark.stop()
    return out


def inject(table: str, source: str, new_dates: list[str]) -> dict:
    spark = spark_session("probe-inject", ref="main")
    try:
        cols = [f.name for f in spark.table(table).schema.fields]
        made = {}
        for nd in new_dates:
            proj = ", ".join(
                f"DATE '{nd}' AS business_date" if c == "business_date" else c
                for c in cols)
            spark.sql(
                f"INSERT INTO {table} SELECT {proj} FROM {table} "
                f"WHERE business_date = DATE '{source}'")
            made[nd] = spark.sql(
                f"SELECT COUNT(*) n FROM {table} WHERE business_date = "
                f"DATE '{nd}'").collect()[0]["n"]
        return {"table": table, "injected": made}
    finally:
        spark.stop()


def crashrun(crash_on: str, specs: list[str]) -> dict:
    """Run the real retention chain, failing inside apply_table_retention for
    `crash_on` AFTER its row DELETE has committed.

    The injection point is `iceberg_gc_enabled`, which apply_table_retention
    calls on the line immediately after the DELETE -- exactly where a real crash
    blew up. Nothing else is stubbed: the branch sweep, tag expiry, Nessie GC
    and the deferred-delete pass all really run.
    """
    from reporting_platform.retention import retention as R

    real = R.iceberg_gc_enabled

    def boom(spark, table):
        if table == crash_on:
            raise RuntimeError(
                f"INJECTED FAILURE: {table} rows are "
                f"already DELETEd and committed, and the step after it died")
        return real(spark, table)

    R.iceberg_gc_enabled = boom
    tables = [tuple(s.split(":", 1)) for s in specs]
    try:
        R.run(tables, dry_run=False)
        return {"crashed": False}
    except R.RetentionPartialFailure as e:
        return {"crashed": True, "error": f"{type(e).__name__}: {e}",
                "partial_report": e.report}
    except Exception as e:
        return {"crashed": True, "error": f"{type(e).__name__}: {e}"}
    finally:
        R.iceberg_gc_enabled = real


def crashdeferred(after_hours: int | None = None) -> dict:
    """Crash the deferred-delete pass BETWEEN the files being deleted and the
    live-set bookkeeping row being dropped.

    That leaves a live-set marked EXPIRY_SUCCESS, past its window, still
    listing deferred files that no longer exist -- which is precisely the
    state `deferred_deletes()`'s docstring claims to tolerate and which has
    never been produced.
    """
    from reporting_platform.retention import retention as R

    real = R._run_gc

    state = {"deleted_files": False}

    def boom(cmd, phase):
        # Only crash on the bookkeeping `delete` that FOLLOWS a real
        # deferred-deletes. Crashing on the first `delete` of the pass hits
        # the empty-live-set prune instead, which proves nothing.
        if phase == "delete" and state["deleted_files"]:
            raise RuntimeError(
                "INJECTED FAILURE: files are deleted, the live-set row is not "
                "yet dropped")
        out = real(cmd, phase)
        if phase == "deferred-deletes":
            state["deleted_files"] = True
        return out

    R._run_gc = boom
    try:
        return {"crashed": False,
                "result": R.deferred_deletes(False, after_hours)}
    except Exception as e:
        return {"crashed": True, "error": f"{type(e).__name__}: {e}"}
    finally:
        R._run_gc = real


if __name__ == "__main__":
    op = sys.argv[1]
    if op == "snapshot":
        print(json.dumps(snapshot(sys.argv[2:]), indent=2, default=str))
    elif op == "inject":
        print(json.dumps(inject(sys.argv[2], sys.argv[3], sys.argv[4:]),
                         indent=2, default=str))
    elif op == "crashdeferred":
        ah = int(sys.argv[2]) if len(sys.argv) > 2 else None
        print(json.dumps(crashdeferred(ah), indent=2, default=str))
    elif op == "crashrun":
        print(json.dumps(crashrun(sys.argv[2], sys.argv[3:]), indent=2,
                         default=str))
    else:
        raise SystemExit(f"unknown op {op!r}")
