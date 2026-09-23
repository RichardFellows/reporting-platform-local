"""The Spark-task operations this package owns.

Each runs in its own driver process, launched by
`reporting_platform.common.spark_task`, which names it in `OPS` and
dispatches to it by that name. JSON on the last line of stdout; the return
value is the exit code; `args` is everything after the op name. See
`common/spark_task.py` for why a separate process at all.
"""
from __future__ import annotations

import json
import sys


def op_retention(args: list[str]) -> int:
    """`retention`."""
    from reporting_platform.retention.retention import (
        RetentionPartialFailure, run,
    )

    dry = args[0] == "dry"
    tables = [tuple(t.split(":", 1)) for t in args[1:]]
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
