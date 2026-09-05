"""Bring every raw table up to the current provenance schema, in one pass.

WHY THIS EXISTS, AND IT IS NOT A CONVENIENCE. `ensure_raw_columns` runs inside
`ingest()`, on the branch, for the ONE feed being ingested -- which is the
right place for it, because that is where a table is guaranteed to exist and
where a schema change can be abandoned with the rest of a failed load. But it
means the migration is LAZY: a feed that has not delivered since the columns
were added still has the old schema.

That is invisible until the next dbt build, and then it is not subtle. A build
builds every prepared model, and every prepared model now selects
`_delivery_id`; one feed ingesting migrates one raw table, so the first ingest
after the change makes its own model work and leaves the others failing with

    [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with
    name `_delivery_id` cannot be resolved.

-- observed, on three of four models, which is how this module came to exist.
Run it once when deploying a provenance column, BEFORE the next ingest.

Idempotent and safe to re-run: a table already carrying the columns is
untouched and no commit is made for it. A feed whose raw table does not exist
yet is skipped rather than created -- the first ingest creates it with the
current schema anyway, and creating an empty table here would put a namespace
and a table on `main` for a feed that has never delivered.

ON A BRANCH AND MERGED, like every other write. A schema change is a commit,
and the one thing this must not do is put a half-finished migration on `main`.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from reporting_platform.common.context import (
    Nessie, branch_name, feeds, new_run_id, spark_session,
)
from reporting_platform.ingest.ingest_feed import (
    _PROVENANCE_COLUMNS, _at_branch, ensure_raw_columns,
)

log = logging.getLogger("migrate_raw")


def _exists(spark, table: str) -> bool:
    try:
        spark.sql(f"SELECT * FROM {table} LIMIT 0")
        return True
    except Exception:                                          # noqa: BLE001
        return False


def migrate(dry_run: bool = False) -> dict:
    """Add any missing provenance column to every feed's raw table."""
    from datetime import date

    run_id = new_run_id()
    report: dict = {"run_id": run_id, "dry_run": dry_run, "feeds": [],
                    "migrated": 0, "already_current": 0, "absent": 0}
    wanted = [n for n, _ in _PROVENANCE_COLUMNS]

    nessie = Nessie()
    spark = spark_session(f"migrate-raw-{run_id}", ref="main")
    try:
        for fd in feeds().values():
            if not _exists(spark, fd.raw_table):
                log.info("%s: no raw table yet, skipping", fd.name)
                report["feeds"].append({"feed": fd.name, "status": "absent"})
                report["absent"] += 1
                continue

            have = {c.lower() for c in
                    spark.sql(f"SELECT * FROM {fd.raw_table} LIMIT 0").columns}
            missing = [c for c in wanted if c.lower() not in have]
            if not missing:
                report["feeds"].append({"feed": fd.name,
                                        "status": "already_current"})
                report["already_current"] += 1
                continue

            if dry_run:
                log.info("%s: would add %s", fd.name, ", ".join(missing))
                report["feeds"].append({"feed": fd.name, "status": "would_add",
                                        "columns": missing})
                continue

            branch = branch_name("migrate", fd.name, date.today(), run_id)
            nessie.create_branch(branch)
            try:
                added = ensure_raw_columns(spark, _at_branch(fd.raw_table,
                                                             branch))
                nessie.merge(branch, into="main")
                nessie.delete_reference(branch)
            except Exception:
                # The branch is LEFT for inspection, exactly as a failed
                # ingest leaves its own. `main` never saw the change.
                log.error("%s: migration failed, branch %s left in place",
                          fd.name, branch)
                raise
            log.info("%s: added %s", fd.name, ", ".join(added))
            report["feeds"].append({"feed": fd.name, "status": "migrated",
                                    "columns": added})
            report["migrated"] += 1
    finally:
        spark.stop()
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be altered, changing nothing")
    a = p.parse_args(argv)
    print(json.dumps(migrate(a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
