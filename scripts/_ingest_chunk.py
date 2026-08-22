"""Ingest a batch of (feed, key) pairs within one process/JVM.

Invoked by bulk_ingest.py once per CHUNK_SIZE files -- never call this
directly for the full backlog. See bulk_ingest.py's module docstring for
why chunking exists.
"""
from __future__ import annotations

import sys

from reporting_platform.ingest.ingest_feed import ingest


def main() -> int:
    feed_name, keys = sys.argv[1], sys.argv[2:]
    for key in keys:
        result = ingest(feed_name, key)
        drift = ""
        if result.get("missing_columns") or result.get("extra_columns"):
            drift = f"  DRIFT missing={result['missing_columns']} extra={result['extra_columns']}"
        print(f"  {result['business_date']}  v{result['file_version']}  "
              f"{result['rows']} rows  {key}{drift}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
