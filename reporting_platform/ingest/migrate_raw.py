"""Bring every raw table up to its feed's current column contract, in one pass.

WHY THIS EXISTS, AND IT IS NOT A CONVENIENCE. `ensure_raw_schema` runs inside
`ingest()`, on the branch, for the ONE feed being ingested -- the right place,
because that is where a table is guaranteed to exist and where a schema change
can be abandoned with a failed load. But it makes the migration LAZY: a feed
that has not delivered since the columns were added still has the old schema.

That is invisible until the next dbt build, and then it is not subtle. Every
prepared model selects `_delivery_id`, so the first ingest after the change
makes its own model work and leaves the others failing with
`[UNRESOLVED_COLUMN.WITH_SUGGESTION]` -- observed on three of four models,
which is how this module came to exist.

IT COVERS THE FEEDS' OWN COLUMNS TOO, not just the provenance four, and that is
what it gets run for most. Adding a column to an existing feed changes
`feeds.yml` and the prepared model together, and the raw table in between has
to be told. Run this after deploying that change and BEFORE the next ingest or
build: config, migrate, build.

A column a feed no longer declares is REPORTED AND NEVER DROPPED, here as in
`ingest()`. See `plan_raw_schema` for why the two directions differ.

Idempotent and safe to re-run. A feed whose raw table does not exist yet is
skipped rather than created -- the first ingest creates it with the current
schema anyway, and creating an empty table here would put a namespace and table
on `main` for a feed that has never delivered.

ON A BRANCH AND MERGED, like every other write: a schema change is a commit,
and this must not put a half-finished migration on `main`.
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
    _at_branch, ensure_raw_schema, plan_raw_schema,
)

log = logging.getLogger("migrate_raw")


def _exists(spark, table: str) -> bool:
    try:
        spark.sql(f"SELECT * FROM {table} LIMIT 0")
        return True
    except Exception:                                          # noqa: BLE001
        return False


def migrate(dry_run: bool = False) -> dict:
    """Add every column a feed declares that its raw table does not have.

    Orphans -- a column the table has and the feed no longer declares -- are
    counted and named on every feed that has one, whether or not anything was
    added, because this is the tool somebody runs to ask what the tables and
    the config disagree about. Nothing here ever drops one.
    """
    from datetime import date

    run_id = new_run_id()
    report: dict = {"run_id": run_id, "dry_run": dry_run, "feeds": [],
                    "migrated": 0, "already_current": 0, "absent": 0,
                    "orphaned": 0}

    nessie = Nessie()
    spark = spark_session(f"migrate-raw-{run_id}", ref="main")
    try:
        for fd in feeds().values():
            if not _exists(spark, fd.raw_table):
                log.info("%s: no raw table yet, skipping", fd.name)
                report["feeds"].append({"feed": fd.name, "status": "absent"})
                report["absent"] += 1
                continue

            plan = plan_raw_schema(
                fd, spark.sql(f"SELECT * FROM {fd.raw_table} LIMIT 0").dtypes)
            missing = [name for name, _ in plan["add"]]
            orphaned = [name for name, _ in plan["orphaned"]]
            entry: dict = {"feed": fd.name}
            if orphaned:
                # Reported on the feed's row rather than raised: dropping a
                # column to match a config edit is the one thing this must
                # not do on its own. See plan_raw_schema.
                log.warning("%s: %s in the table, not declared by the feed",
                            fd.name, ", ".join(orphaned))
                entry["orphaned"] = orphaned
                report["orphaned"] += 1

            if not missing:
                entry["status"] = "already_current"
                report["feeds"].append(entry)
                report["already_current"] += 1
                continue

            if dry_run:
                log.info("%s: would add %s", fd.name, ", ".join(missing))
                report["feeds"].append({**entry, "status": "would_add",
                                        "columns": missing})
                continue

            branch = branch_name("migrate", fd.name, date.today(), run_id)
            nessie.create_branch(branch)
            try:
                result = ensure_raw_schema(
                    spark, _at_branch(fd.raw_table, branch), fd)
                nessie.merge(branch, into="main")
                nessie.delete_reference(branch)
            except Exception:
                # The branch is LEFT for inspection, exactly as a failed
                # ingest leaves its own. `main` never saw the change.
                log.error("%s: migration failed, branch %s left in place",
                          fd.name, branch)
                raise
            log.info("%s: added %s", fd.name, ", ".join(result["added"]))
            report["feeds"].append({**entry, "status": "migrated",
                                    "columns": result["added"]})
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
