"""Moved to `reporting_platform/common/spark_task.py`; this is a shim.

`python -m scripts._spark_task <op> ...` is what the docs and operators have
typed for as long as the platform has existed, and it still works locally.
Nothing packaged imports it: `scripts/` ships in no component
(docs/PACKAGING.md), so DAGs import the module in `common/`.
"""
from reporting_platform.common.spark_task import (  # noqa: F401
    MODULE, OPS, driver_pod, main, parse_result, run,
)

if __name__ == "__main__":
    raise SystemExit(main())
