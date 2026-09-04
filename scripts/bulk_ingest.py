"""One-off bulk ingest: ingest every pending landed file, across all feeds.

`ingest()` in ingest_feed.py stops its SparkSession at the end of every call,
so calling it in a tight loop in-process does NOT reuse one JVM the way it looks
like it should -- it rebuilds the SparkContext and reloads the jars through a
fresh URLClassLoader each time, and eventually dies with
`java.lang.OutOfMemoryError: Java heap space`.
See docs/DECISIONS.md#one-session-per-chunk (plus recurring "Unclosed
S3FileIO instance" warnings pointing at the same kind of per-call
resource that was never being released).

The real fix is to stop restarting the JVM 48+ times in one process at
all. Each CHUNK_SIZE-sized batch of files runs in its own subprocess, so
the OS fully reclaims the JVM's memory when that subprocess exits --
nothing can accumulate across chunk boundaries. Ivy dependency resolution
only pays a real cost on the very first run of the whole platform; after
that the resolved jars are cached under ~/.ivy2, so per-subprocess
startup is fast.

Safe to re-run after a crash or interruption: `find_pending` checks each
feed's raw table for `_source_file` values already committed to `main`,
so already-ingested files are skipped automatically.

It now ingests MANIFEST keys under `ready/`, not landing keys. `find_pending`
reconciles `ready/` from `landing/` first, so a file pushed straight into the
bucket by an upstream agent -- which runs no code of ours -- still gets picked
up here. See reporting_platform/ingest/normalize.py.
"""
from __future__ import annotations

import subprocess
import sys

from reporting_platform.common.context import feeds
from reporting_platform.ingest.arrival import find_pending

CHUNK_SIZE = 10


def chunked(seq: list, n: int) -> list[list]:
    return [seq[i:i + n] for i in range(0, len(seq), n)]


def main() -> int:
    total = 0
    for name, fd in feeds().items():
        pending = find_pending(fd)
        print(f"=== {name}: {len(pending)} pending ===")
        for chunk in chunked(pending, CHUNK_SIZE):
            print(f"  -- chunk of {len(chunk)} files (fresh JVM) --")
            subprocess.run(
                [sys.executable, "-m", "scripts._ingest_chunk", name, *chunk],
                check=True,
            )
            total += len(chunk)
    print(f"\ningested {total} files total")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
