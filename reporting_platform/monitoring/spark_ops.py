"""The Spark-task operations this package owns.

Each runs in its own driver process, launched by
`reporting_platform.common.spark_task`, which names it in `OPS` and
dispatches to it by that name. JSON on the last line of stdout; the return
value is the exit code; `args` is everything after the op name. See
`common/spark_task.py` for why a separate process at all.
"""
from __future__ import annotations

import json


def op_completeness(args: list[str]) -> int:
    """`completeness`."""
    from reporting_platform.monitoring.completeness import run

    lookback = int(args[0]) if len(args) > 0 and args[0] else None
    print(json.dumps(run(lookback), default=str))
    return 0


def op_reproducibility(args: list[str]) -> int:
    """`reproducibility`."""
    from reporting_platform.monitoring.reproducibility import run

    tag = args[0] if len(args) > 0 and args[0] else None
    print(json.dumps(run(tag), default=str))
    return 0
