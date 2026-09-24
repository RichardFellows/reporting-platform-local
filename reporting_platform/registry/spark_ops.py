"""The Spark-task operations this package owns.

Each runs in its own driver process, launched by
`reporting_platform.common.spark_task`, which names it in `OPS` and
dispatches to it by that name. JSON on the last line of stdout; the return
value is the exit code; `args` is everything after the op name. See
`common/spark_task.py` for why a separate process at all.
"""
from __future__ import annotations

import json


def op_run_inputs(args: list[str]) -> int:
    """`run-inputs`."""
    # REQ-400. Which deliveries the build on this branch actually read,
    # read from the branch itself before it is merged. See
    # reporting_platform/registry/inputs.py.
    from reporting_platform.registry.inputs import collect

    print(json.dumps(collect(args[0]), default=str))
    return 0
