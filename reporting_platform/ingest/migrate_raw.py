"""Bring every raw table up to its feed's current column contract, in one pass
-- creating the ones that do not exist yet.

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

Idempotent and safe to re-run.

A DECLARED FEED GETS ITS RAW TABLE HERE, BEFORE ITS FIRST DELIVERY. This used
to skip a feed with no table, on the reasoning that the first ingest creates
it anyway. It does -- but `prepared_build` builds every prepared model on
every run, the model of a feed with no raw table fails with
`TABLE_OR_VIEW_NOT_FOUND`, and write-audit-publish then keeps the branch: so
from the moment a feed and its model were declared until its first delivery,
NO feed published. The table is created from the same contract
`ensure_raw_table` uses, empty, and the model builds against zero rows.

That turns "no table" into "an empty table" for a feed that has never
delivered, on purpose, which is exactly the READ-vs-EMPTY confusion CLAUDE.md
warns about. So nothing may infer "never delivered" from the table any more:
the registry says it (`registry.delivery` has no row for the feed), and
`monitoring/completeness.py` asks it. See
docs/DECISIONS.md#a-declared-feed-has-a-raw-table-before-it-delivers

Run at deploy time (`airflow-init`, the chart's `platform-init` hook, the
standalone runner's `setup`) and first in `platform_housekeeping` every night,
which is what covers a feed saved from the console between deploys.

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
    _at_branch, _bootstrap_main_if_empty, ensure_raw_namespace,
    ensure_raw_schema, ensure_raw_table, plan_raw_schema,
)

log = logging.getLogger("migrate_raw")


def _exists(spark, table: str) -> bool:
    """Whether `table` exists -- and a RAISE when that cannot be told.

    Only Spark's own "not found" is False. It used to be any exception, which
    was harmless while an absent table was merely skipped; now absent means
    CREATE, and a catalog that is down must not read as a feed to set up.
    """
    try:
        spark.sql(f"SELECT * FROM {table} LIMIT 0")
        return True
    except Exception as exc:                                   # noqa: BLE001
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            return False
        raise


def create_raw_table(spark, nessie, fd, run_id: str) -> str:
    """Create `fd`'s empty raw table on a branch and merge it. Returns how.

    The namespace goes on `main` first, as in `ingest()`: a namespace cannot
    be addressed on a branch (docs/DECISIONS.md#namespace-before-branch). A
    catalog whose `main` has no commits cannot take a branch+merge at all, so
    there `_bootstrap_main_if_empty` creates namespace and table on `main`
    directly, once -- the same one-time path the first ingest takes.
    """
    from datetime import date

    _bootstrap_main_if_empty(nessie, fd, spark)
    if _exists(spark, fd.raw_table):
        return "bootstrapped"
    ensure_raw_namespace(spark, fd)
    branch = branch_name("migrate", fd.name, date.today(), run_id)
    nessie.create_branch(branch)
    try:
        ensure_raw_table(spark, fd, _at_branch(fd.raw_table, branch))
        nessie.merge(branch, into="main")
        nessie.delete_reference(branch)
    except Exception:
        log.error("%s: creating the raw table failed, branch %s left in place",
                  fd.name, branch)
        raise
    return "created"


def migrate(dry_run: bool = False) -> dict:
    """Create every declared feed's raw table that does not exist, and add
    every column a feed declares that its raw table does not have.

    Orphans -- a column the table has and the feed no longer declares -- are
    counted and named on every feed that has one, whether or not anything was
    added, because this is the tool somebody runs to ask what the tables and
    the config disagree about. Nothing here ever drops one.
    """
    from datetime import date

    run_id = new_run_id()
    report: dict = {"run_id": run_id, "dry_run": dry_run, "feeds": [],
                    "created": 0, "migrated": 0, "already_current": 0,
                    "orphaned": 0}

    nessie = Nessie()
    spark = spark_session(f"migrate-raw-{run_id}", ref="main")
    try:
        for fd in feeds().values():
            if not _exists(spark, fd.raw_table):
                # Created with the CURRENT contract, so there is nothing to
                # migrate afterwards: the columns loop below is for tables
                # that predate a change.
                if dry_run:
                    log.info("%s: no raw table, would create it", fd.name)
                    report["feeds"].append({"feed": fd.name,
                                            "status": "would_create"})
                    continue
                how = create_raw_table(spark, nessie, fd, run_id)
                log.info("%s: raw table created (%s), empty until its first "
                         "delivery", fd.name, how)
                report["feeds"].append({"feed": fd.name, "status": "created"})
                report["created"] += 1
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
