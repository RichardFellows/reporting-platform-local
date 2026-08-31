"""Ingest a batch of (feed, key) pairs within one process/JVM and ONE session.

Invoked by bulk_ingest.py once per CHUNK_SIZE files -- never call this
directly for the full backlog. See bulk_ingest.py's module docstring for
why chunking exists.

ONE SPARK SESSION FOR THE WHOLE CHUNK. This shared the JVM but not the
session: `ingest()` built and stopped its own, so a chunk of ten files
registered ten Spark applications, each paying ~22s of executor acquisition
and catalog initialisation before a few seconds of real work. Measured on a
full load: 127 applications for 183 files, mean 15.7s each, against a
48m52s ingest.

The branch is now addressed per statement (ingest_feed._at_branch) instead of
by binding the session to it, so per-file branch isolation is unchanged --
every file still gets its own branch and merges to main on success.
"""
from __future__ import annotations

import sys

from reporting_platform.common.context import spark_session
from reporting_platform.ingest.ingest_feed import ingest


def main() -> int:
    feed_name, keys = sys.argv[1], sys.argv[2:]
    # Bound to main; each file names its own branch (see _at_branch).
    spark = spark_session(f"ingest-chunk-{feed_name}", ref="main")
    try:
        return _run(feed_name, keys, spark)
    finally:
        spark.stop()


def _run(feed_name: str, keys: list[str], spark) -> int:
    for key in keys:
        result = ingest(feed_name, key, spark=spark)
        drift = ""
        if result.get("missing_columns") or result.get("extra_columns"):
            drift = f"  DRIFT missing={result['missing_columns']} extra={result['extra_columns']}"
        print(f"  {result['business_date']}  v{result['file_version']}  "
              f"{result['rows']} rows  {key}{drift}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
