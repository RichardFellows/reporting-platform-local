"""Run one Spark-using platform operation in its own process, emitting JSON.

WHY THIS EXISTS. Airflow tasks that build a SparkSession in-process do their
work correctly and then fail anyway: the task callable returns, the value is
xcom-pushed, and the process never exits, because the Spark JVM and its py4j
gateway keep non-daemon threads alive. Heartbeats stop, and ~300s later the
scheduler reaps the task as a zombie and marks it failed. The task log shows
the work completing, which makes it a confusing failure to diagnose --
`resolve_arrival` finished in 13 seconds and was killed five minutes later.

Running the Spark work in a child process fixes it: the JVM dies with the
child, the parent task returns promptly, and Airflow sees a clean exit. This
is the same reasoning that made scripts/bulk_ingest.py chunk into subprocesses
 -- there for heap reclamation, here for process exit.

stdout is JSON on the last line so the caller can parse it; everything Spark
writes to stderr stays out of the way.

Usage:
    python -m scripts._spark_task pending <feed>
    python -m scripts._spark_task ingest <feed> <key> [run_id] [business_date]
    python -m scripts._spark_task maintain-metrics <fqn:layer>...
    python -m scripts._spark_task maintain <force|noforce> <fqn:layer>...
    python -m scripts._spark_task retention <dry|real> <fqn:layer>...
    python -m scripts._spark_task completeness [lookback_business_days]

`pending` returns MANIFEST keys under ready/; `ingest` takes one of those or a
landing object key. See reporting_platform/ingest/normalize.py.
"""
from __future__ import annotations

import json
import sys
from datetime import date


def main() -> int:
    op = sys.argv[1]

    if op == "pending":
        from reporting_platform.common.context import feed as get_feed
        from reporting_platform.ingest.arrival import find_pending

        fd = get_feed(sys.argv[2])
        print(json.dumps({"pending": find_pending(fd)}))
        return 0

    if op == "ingest":
        from reporting_platform.ingest.ingest_feed import ingest

        feed_name, key = sys.argv[2], sys.argv[3]
        run_id = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else None
        bd = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] else None
        result = ingest(
            feed_name=feed_name,
            object_key=key,
            run_id=run_id,
            business_date=date.fromisoformat(bd) if bd else None,
        )
        print(json.dumps(result, default=str))
        return 0

    if op == "maintain":
        from reporting_platform.maintenance.maintain import run

        tables = [tuple(t.split(":", 1)) for t in sys.argv[3:]]
        force = sys.argv[2] == "force"
        print(json.dumps(run(tables, force=force, dry_run=False), default=str))
        return 0

    if op == "maintain-metrics":
        from reporting_platform.maintenance.maintain import run

        tables = [tuple(t.split(":", 1)) for t in sys.argv[2:]]
        print(json.dumps(run(tables, force=False, dry_run=True), default=str))
        return 0

    if op == "completeness":
        from reporting_platform.monitoring.completeness import run

        lookback = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
        print(json.dumps(run(lookback), default=str))
        return 0

    if op == "retention":
        from reporting_platform.retention.retention import (
            RetentionPartialFailure, run,
        )

        dry = sys.argv[2] == "dry"
        tables = [tuple(t.split(":", 1)) for t in sys.argv[3:]]
        try:
            print(json.dumps(run(tables, dry_run=dry), default=str))
        except RetentionPartialFailure as e:
            # Emit the partial report on stdout and THEN fail. The task must
            # go red -- some tables were not retained -- but the caller's
            # error message includes the stdout tail, so this is what puts
            # "applied to A and B, failed on C" in the Airflow log instead of
            # a bare traceback.
            print(json.dumps(e.report, default=str))
            print(f"RETENTION PARTIAL: {e}", file=sys.stderr)
            return 1
        return 0

    raise SystemExit(f"unknown op {op!r}")


if __name__ == "__main__":
    raise SystemExit(main())
