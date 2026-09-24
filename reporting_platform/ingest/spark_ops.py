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


def op_pending(args: list[str]) -> int:
    """`pending`."""
    from reporting_platform.common.context import feed as get_feed
    from reporting_platform.ingest.arrival import find_pending

    fd = get_feed(args[0])
    print(json.dumps({"pending": find_pending(fd)}))
    return 0


def op_ingest(args: list[str]) -> int:
    """`ingest`."""
    from reporting_platform.ingest.ingest_feed import ingest

    feed_name, key = args[0], args[1]
    run_id = args[2] if len(args) > 2 and args[2] else None
    bd = args[3] if len(args) > 3 and args[3] else None
    result = ingest(
        feed_name=feed_name,
        object_key=key,
        run_id=run_id,
        cob_date=date.fromisoformat(bd) if bd else None,
    )
    print(json.dumps(result, default=str))
    return 0


def op_ingest_batch(args: list[str]) -> int:
    """`ingest-batch <feed> <key>...`: several deliveries, ONE Spark session.

    A session per delivery was measured at ~16s of executor acquisition and
    catalog start-up each, before a few seconds of real work (see
    scripts/_ingest_chunk.py, which this is the packaged form of). Each key
    still gets its own branch and merges on its own, so one bad delivery
    fails alone: its error is reported and the rest carry on.
    """
    from reporting_platform.common.context import spark_session
    from reporting_platform.ingest.ingest_feed import ingest

    feed_name, keys = args[0], args[1:]
    # Bound to main; each delivery names its own branch (ingest_feed._at_branch).
    spark = spark_session(f"ingest-batch-{feed_name}", ref="main")
    results = []
    try:
        for key in keys:
            try:
                results.append(ingest(feed_name, key, spark=spark))
            except Exception as exc:                            # noqa: BLE001
                results.append({"object_key": key,
                                "error": f"{type(exc).__name__}: {exc}"[:2000]})
    finally:
        spark.stop()
    print(json.dumps({"feed": feed_name, "results": results}, default=str))
    return 0


def op_ingest_v2(args: list[str]) -> int:
    """`ingest-v2`."""
    from reporting_platform.ingest.ingest_feed import ingest_normalized_delivery

    key = args[0]
    run_id = args[1] if len(args) > 1 and args[1] else None
    result = ingest_normalized_delivery(key, run_id=run_id)
    print(json.dumps(result, default=str))
    return 0


def op_raw_delivery_ids(args: list[str]) -> int:
    """`raw-delivery-ids`."""
    # Read-only: deliberately outside the lakehouse_write pool, like
    # completeness/reproducibility in
    # monitoring/spark_ops.py. See
    # docs/AIRFLOW-ORCHESTRATION.md#reconciliation-scale.
    from reporting_platform.common.context import feed as get_feed, spark_session
    from reporting_platform.ingest.ingest_feed import raw_delivered_ids

    fd = get_feed(args[0])
    spark = spark_session(f"raw-delivery-ids-{fd.name}", ref="main")
    try:
        ids = raw_delivered_ids(spark, fd)
    finally:
        spark.stop()
    print(json.dumps({"feed": fd.name, "delivery_ids": sorted(ids)}))
    return 0


def op_reconcile_committed(args: list[str]) -> int:
    """`reconcile-committed`."""
    # BACKFILL, not a build. A Delivery Raw already holds committed just
    # as durably before `registry.delivery_committed` existed; this
    # recovers that fact from Raw itself rather than re-ingesting.
    # Idempotent (`record_committed` is ON CONFLICT DO NOTHING), so
    # re-running it costs a no-op per already-known Delivery.
    from reporting_platform.common.context import feed as get_feed, spark_session
    from reporting_platform.ingest.ingest_feed import committed_rows_from_raw
    from reporting_platform.registry import deliveries as registry_deliveries

    fd = get_feed(args[0])
    spark = spark_session(f"reconcile-committed-{fd.name}", ref="main")
    try:
        rows = committed_rows_from_raw(spark, fd)
    finally:
        spark.stop()
    for row in rows:
        registry_deliveries.record_committed(
            fd.name, row["delivery_id"], row["cob_date"],
            row["source_system"], row["rows"],
            "backfill:reconcile-committed", file_version=row["file_version"])
    print(json.dumps({"feed": fd.name, "rows_seen": len(rows)}, default=str))
    return 0


def op_migrate_raw(args: list[str]) -> int:
    """`migrate-raw`."""
    from reporting_platform.ingest.migrate_raw import migrate

    dry = len(args) > 0 and args[0] == "dry"
    print(json.dumps(migrate(dry_run=dry), default=str))
    return 0
