"""Ingest one feed delivery: landing CSV -> raw Iceberg table.

Design rules this job enforces:

* Every source column lands as STRING. Casting happens in dbt, in `prepared`,
  where it is testable. A load must never fail because a value was unparseable
  — it must land, and then fail a *test*.
* The write happens on a Nessie branch, never on main. Publication is a merge.
* Re-delivery of a business date does not overwrite: it lands as a new
  `_file_version`. `prepared` picks the latest version; retention removes the
  superseded ones later. This preserves the ability to answer "what did the
  file we originally received say?" for at least the grace period.
* Schema drift is recorded, not fatal. New upstream columns land in an
  `_extra_columns` map; missing columns land as NULL and are reported.

Usage:
    python -m reporting_platform.ingest.ingest_feed \
        --feed fo_trade --object landing/fo_trade/TRADE_20260811.csv \
        --run-id 20260811T060000-a1b2c3
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone

from reporting_platform.common.context import (
    CATALOG, Nessie, branch_name, feed as get_feed, new_run_id, spark_session,
)

log = logging.getLogger("ingest")


def _at_branch(table: str, branch: str | None) -> str:
    """Address `table` on a Nessie branch WITHOUT rebinding the session.

    `lakehouse.raw.fo_trade` on branch `ingest/trade/...` becomes
    `lakehouse.raw.`trade@ingest/trade/...``.

    THIS IS WHAT LETS ONE SPARK SESSION SERVE A WHOLE CHUNK OF FILES: naming the
    branch per statement rather than per session. Per-file branch isolation is
    unchanged. See docs/DECISIONS.md#branch-in-the-table-name

    Backticks are required: branch names contain `/` and `-`.
    """
    if not branch or branch == "main":
        return table
    catalog, namespace, name = table.split(".")
    return f"{catalog}.{namespace}.`{name}@{branch}`"


def _landing_uri(object_key: str) -> str:
    bucket = os.environ.get("REPORTING_LANDING", "s3a://lakehouse/landing")
    root = bucket.rsplit("/", 1)[0] if bucket.endswith("/landing") else bucket
    return f"{root}/{object_key}" if not object_key.startswith("s3a://") else object_key


def ensure_raw_namespace(spark, fd) -> None:
    """Create the feed's raw namespace on `main`, idempotently.

    Separate from ensure_raw_table because it has to happen at a different
    REFERENCE and a different moment -- see the call site in ingest().
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{fd.raw_namespace}")


def ensure_raw_table(spark, fd, table: str | None = None) -> None:
    """Create the raw table if absent.

    business_date is the LEADING partition field on every table. This is not a
    performance choice, it is a retention choice: if it is not the partition
    field, retention deletes become full-table rewrites every night rather than
    metadata operations. See docs/RETENTION.md.
    """
    cols = ",\n        ".join(f"`{c}` STRING" for c in fd.columns)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table or fd.raw_table} (
        {cols},
        _extra_columns MAP<STRING, STRING>,
        _business_date DATE,
        _ingest_ts     TIMESTAMP,
        _source_file   STRING,
        _file_version  INT,
        _row_number    BIGINT,
        _batch_id      STRING
        )
        USING iceberg
        PARTITIONED BY (days(_business_date))
        TBLPROPERTIES (
          'write.format.default'          = 'parquet',
          'write.parquet.compression-codec' = 'zstd',
          'write.target-file-size-bytes'  = '268435456',
          'format-version'                = '2',
          'write.metadata.delete-after-commit.enabled' = 'true',
          'write.metadata.previous-versions-max'       = '20'
        )
        """
    )


def read_landing(spark, fd, uri: str):
    """Read the CSV with every column as string, permissively."""
    return (
        spark.read.option("header", str(fd.header).lower())
        .option("sep", fd.delimiter)
        .option("quote", fd.quote_char)
        .option("encoding", fd.file_encoding)
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "false")
        .csv(uri)
    )


def reconcile_schema(df, fd) -> tuple:
    """Align the arrival to the declared schema, rename to platform names, and
    surface drift.

    THIS IS WHERE SOURCE COLUMN NAMES STOP EXISTING. A delivery may head its
    columns `Trade Id` or `Notional (USD)`; `feeds.yml` maps those to
    identifiers, and everything from the raw table onwards sees only the
    identifier. Doing it here rather than in each prepared model means the
    awkward name lives in one place instead of in every macro call that
    touches it. See docs/DECISIONS.md#source-column-names

    Drift is reported in SOURCE names, because drift is a statement about the
    file: "the delivery did not have `Cpty Ref`" is actionable with the
    upstream, and the platform name it would have become is not.
    """
    from pyspark.sql import functions as F

    arrived = set(df.columns)
    # (platform name, name in the file), in declared order.
    declared = [(c, fd.source_column(c)) for c in fd.columns]
    expected = {source for _, source in declared}
    missing = [source for _, source in declared if source not in arrived]
    extra = sorted(arrived - expected)

    for source in missing:
        df = df.withColumn(source, F.lit(None).cast("string"))

    if extra:
        pairs = []
        for c in extra:
            pairs += [F.lit(c), F.col(f"`{c}`").cast("string")]
        df = df.withColumn("_extra_columns", F.create_map(*pairs))
    else:
        df = df.withColumn(
            "_extra_columns",
            F.create_map().cast("map<string,string>"),
        )

    df = df.select(*[F.col(f"`{source}`").cast("string").alias(name)
                     for name, source in declared],
                   "_extra_columns")
    return df, {"missing_columns": missing, "extra_columns": extra}


def next_file_version(spark, fd, business_date: date,
                      table: str | None = None) -> int:
    try:
        row = spark.sql(
            f"SELECT COALESCE(MAX(_file_version), 0) AS v FROM {table or fd.raw_table} "
            f"WHERE _business_date = DATE '{business_date:%Y-%m-%d}'"
        ).collect()[0]
        return int(row["v"]) + 1
    except Exception:
        return 1


def _bootstrap_main_if_empty(nessie: Nessie, fd, spark=None) -> None:
    """Give `main` one real commit before the first branch+merge ever runs.

    Nessie's "no ancestor" hash is a boundary marker (no logEntry of its own),
    not a real commit object. Merging a branch into a target still pinned at
    that sentinel fails server-side with "No common ancestor in parents of
    <sentinel> and <source-hash>" -- even though the source branch's history
    genuinely descends from it -- and merging WITHOUT pinning an expected
    hash is rejected too ("Expected hash must be provided"). So the first
    write of a fresh catalog can never merge cleanly via branch+merge.

    Workaround: if `main` has no commits yet, run the (idempotent, IF NOT
    EXISTS) namespace/table setup directly against `main`, bypassing
    branch+merge just this once. Every subsequent ingest merges against a
    real commit hash and hits the normal, working path.
    """
    history = nessie._req("GET", "/trees/main/history")
    if history.get("logEntries"):
        return
    log.info("main has no commits yet; bootstrapping directly (one-time)")
    # REUSE THE CALLER'S SESSION IF THERE IS ONE, and never stop what we did
    # not start. This unconditionally built its own session and called
    # spark.stop() in a finally -- which stops the whole SparkContext, not just
    # this handle, so it silently killed the shared session that
    # `_ingest_chunk` had passed into ingest(). Every subsequent read failed
    # with a Py4JJavaError on the next spark.read.csv.
    #
    # It only fires when `main` has no commits, so a warm stack never reaches
    # it: the failure appeared exactly once, on the first ingest of a cold
    # rebuild, which is the one path a session-reuse change most needed to be
    # tested against.
    owns = spark is None
    if owns:
        spark = spark_session(f"bootstrap-main-{fd.name}", ref="main")
    try:
        # Namespace first here too: this path writes straight to main, and
        # ensure_raw_table no longer creates the namespace.
        ensure_raw_namespace(spark, fd)
        ensure_raw_table(spark, fd)
    finally:
        if owns:
            spark.stop()


def ingest(feed_name: str, object_key: str, run_id: str | None = None,
           business_date: date | None = None, dry_run: bool = False,
           spark=None) -> dict:
    """Land one delivery into `raw` on its own Nessie branch, then merge.

    `spark` is an OPTIONAL session to reuse. Pass one when ingesting several
    files in a row -- `scripts/_ingest_chunk.py` does -- and the caller owns
    stopping it. Without it, one is created and stopped here as before, which
    is what the single-file CLI path still does.

    Reusing it is worth real time: a Spark application costs roughly 22s of
    executor acquisition and catalog initialisation before it does any work,
    and the per-file work here is a few seconds. The branch is addressed with
    _at_branch() rather than by binding the session, so nothing about the
    isolation changes -- every file still gets its own branch, and `main` is
    still only touched by the merge.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    fd = get_feed(feed_name)
    run_id = run_id or new_run_id()
    filename = object_key.rsplit("/", 1)[-1]

    parsed = fd.parse_filename(filename)
    if parsed is None and business_date is None:
        raise ValueError(
            f"Filename {filename!r} does not match feed {feed_name!r} pattern "
            f"{fd.filename_pattern!r} and no --business-date was supplied."
        )
    bdate = business_date or parsed[0]

    nessie = Nessie()
    _bootstrap_main_if_empty(nessie, fd, spark)

    # The session is bound to `main` and the BRANCH is named per statement, so
    # one session can serve many files.
    owns_session = spark is None
    if owns_session:
        spark = spark_session(f"ingest-{fd.name}-{run_id}", ref="main")

    # ON MAIN, AND BEFORE THE BRANCH IS CUT. A namespace cannot be created on a
    # branch: Nessie's `@branch` suffix applies to a TABLE identifier, and
    # using it on a namespace does not fail -- it creates a namespace literally
    # named "`raw_x@ingest/...`" on main. Verified against the live catalog.
    #
    # So the namespace must exist on main first and the branch inherits it.
    # Creating it after the branch was cut is what broke the first ingest into
    # a new raw_<source> namespace:
    #   NoSuchNamespaceException: Namespace does not exist: raw
    # -- CREATE NAMESPACE landed on main while CREATE TABLE addressed the
    # branch, cut a moment earlier. It hides for as long as the namespace
    # already exists, which on a warm stack it always does; it appears on a
    # catalog where it does not, which is where it matters most.
    ensure_raw_namespace(spark, fd)

    branch = branch_name("ingest", fd.name, bdate, run_id)
    nessie.create_branch(branch)
    log.info("created branch %s", branch)

    raw_at_branch = _at_branch(fd.raw_table, branch)
    try:
        ensure_raw_table(spark, fd, raw_at_branch)
        version = next_file_version(spark, fd, bdate, raw_at_branch)

        df = read_landing(spark, fd, _landing_uri(object_key))
        df, drift = reconcile_schema(df, fd)

        # `schema_drift: fail` was documented in feeds.yml and listed in the
        # documented as "the fail branch is unexecuted code". It was worse than
        # unexecuted: NOTHING read fd.schema_drift anywhere in the codebase,
        # so setting it to `fail` was silently identical to `warn` and a feed
        # configured to abort on drift would have loaded regardless
        #.
        #
        # It fires on extra AND missing columns. An extra column is the
        # obvious case, but a missing declared column is the quieter one:
        # reconcile_schema fills it with nulls, so the load succeeds and the
        # column reads as "no value" rather than "never arrived" from then on.
        # A typo here would silently mean "warn", which is exactly how this
        # setting managed to do nothing for so long. Reject anything unknown.
        if fd.schema_drift not in ("warn", "fail"):
            raise ValueError(
                f"{fd.name}: schema_drift must be 'warn' or 'fail', got "
                f"{fd.schema_drift!r}"
            )
        if fd.schema_drift == "fail" and (drift["missing_columns"]
                                          or drift["extra_columns"]):
            raise ValueError(
                f"{fd.name} {bdate}: schema drift with schema_drift=fail — "
                f"missing {drift['missing_columns']}, "
                f"extra {drift['extra_columns']}. Branch {branch} left for "
                f"inspection; main is untouched."
            )

        row_win = Window.orderBy(F.monotonically_increasing_id())
        df = (
            df.withColumn("_business_date", F.lit(bdate.isoformat()).cast("date"))
              .withColumn("_ingest_ts", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
              .withColumn("_source_file", F.lit(object_key))
              .withColumn("_file_version", F.lit(version).cast("int"))
              .withColumn("_row_number", F.row_number().over(row_win).cast("bigint"))
              .withColumn("_batch_id", F.lit(run_id))
        )

        row_count = df.count()
        if row_count < fd.expected_min_rows:
            # Abandon the branch: main is untouched, nothing to roll back.
            raise ValueError(
                f"{fd.name} {bdate}: {row_count} rows, below expected minimum "
                f"{fd.expected_min_rows}. Branch {branch} left for inspection."
            )

        if dry_run:
            log.info("dry run: would append %s rows to %s", row_count, raw_at_branch)
        else:
            df.writeTo(raw_at_branch).append()
            nessie.merge(branch, into="main")
            nessie.delete_reference(branch)
            log.info("merged %s into main and deleted branch", branch)

        result = {
            "feed": fd.name,
            "business_date": bdate.isoformat(),
            "file_version": version,
            "rows": row_count,
            "run_id": run_id,
            "branch": branch,
            "source_file": object_key,
            "asset_uri": fd.asset_uri,
            **drift,
        }
        if drift["missing_columns"] or drift["extra_columns"]:
            log.warning("schema drift on %s: %s", fd.name, json.dumps(drift))
        return result
    finally:
        # Only if we made it. A caller that passed one in owns its lifetime.
        if owns_session:
            spark.stop()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--feed", required=True)
    p.add_argument("--object", required=True, help="object key under the landing prefix")
    p.add_argument("--run-id")
    p.add_argument("--business-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    print(json.dumps(ingest(a.feed, a.object, a.run_id, a.business_date, a.dry_run), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
