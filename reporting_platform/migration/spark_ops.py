"""The Spark-task operations this package owns.

Each runs in its own driver process, launched by
`reporting_platform.common.spark_task`, which names it in `OPS` and
dispatches to it by that name. JSON on the last line of stdout; the return
value is the exit code; `args` is everything after the op name. See
`common/spark_task.py` for why a separate process at all.
"""
from __future__ import annotations

import json
from datetime import date


def op_migration_compare(args: list[str]) -> int:
    """`migration-compare`."""
    # Phase 8. `feed_name` and `business_date` only -- the legacy adapter
    # is resolved from MIGRATION_LEGACY_FIXTURES_DIR, matching every
    # other Spark-subprocess op's "no state but what's on argv/env".
    import os as _os

    from reporting_platform.common.context import feed as get_feed, spark_session
    from reporting_platform.migration.legacy import LocalFixtureLegacySource
    from reporting_platform.migration.run import compare_business_date

    feed_name, bd = args[0], args[1]
    fd = get_feed(feed_name)
    fixtures_dir = _os.environ.get(
        "MIGRATION_LEGACY_FIXTURES_DIR", "/opt/platform/migration-fixtures")
    legacy_source = LocalFixtureLegacySource(fixtures_dir)
    spark = spark_session(f"migration-compare-{feed_name}", ref="main")
    try:
        result = compare_business_date(
            fd, date.fromisoformat(bd), legacy_source=legacy_source,
            spark=spark)
    finally:
        spark.stop()
    print(json.dumps(result, default=str))
    return 0
