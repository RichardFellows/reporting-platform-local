"""The Spark-task operations this package owns.

Each runs in its own driver process, launched by
`reporting_platform.common.spark_task`, which names it in `OPS` and
dispatches to it by that name. JSON on the last line of stdout; the return
value is the exit code; `args` is everything after the op name. See
`common/spark_task.py` for why a separate process at all.
"""
from __future__ import annotations

import json


def op_maintain(args: list[str]) -> int:
    """`maintain`."""
    from reporting_platform.maintenance.maintain import run

    tables = [tuple(t.split(":", 1)) for t in args[1:]]
    force = args[0] == "force"
    print(json.dumps(run(tables, force=force, dry_run=False), default=str))
    return 0


def op_maintain_metrics(args: list[str]) -> int:
    """`maintain-metrics`."""
    from reporting_platform.maintenance.maintain import run

    tables = [tuple(t.split(":", 1)) for t in args[0:]]
    print(json.dumps(run(tables, force=False, dry_run=True), default=str))
    return 0
