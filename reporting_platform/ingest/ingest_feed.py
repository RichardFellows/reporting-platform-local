"""Ingest one feed delivery: landing CSV -> raw Iceberg table.

Design rules this job enforces:

* Every source column lands as STRING. Casting happens in dbt, in `prepared`,
  where it is testable. A load must never fail because a value was unparseable
  — it must land, and then fail a *test*.
* The write happens on a Nessie branch, never on main. Publication is a merge.
* Re-delivery of a COB date does not overwrite: it lands as a new
  `_file_version`. `prepared` picks the latest version; retention removes the
  superseded ones later. This preserves the ability to answer "what did the
  file we originally received say?" for at least the grace period.
* Schema drift is recorded, not fatal. New upstream columns land in an
  `_extra_columns` map; missing columns land as NULL and are reported.
* A change to the feed's DECLARED columns is a different event from drift, and
  is applied to the raw table on the branch by `ensure_raw_schema`: declaring a
  new column on an existing feed is the ordinary way a feed changes, so it
  migrates itself. Undeclaring one never drops it. See `plan_raw_schema`.

Usage:
    python -m reporting_platform.ingest.ingest_feed \
        --feed fo_trade --object landing/fo_trade/TRADE_20260811.csv \
        --run-id 20260811T060000-a1b2c3
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import replace
from datetime import date, datetime, timezone
from reporting_platform.common.parsing import feed_format

from reporting_platform.common import settings
from reporting_platform.common.context import (
    CATALOG, Nessie, branch_name, feed as get_feed, new_run_id, spark_session,
)

log = logging.getLogger("ingest")
_DELIVERY_ID = re.compile(r"dlv_[0-9a-f]{32}")


def _record_raw_check(*, control_id: str, fd, manifest: dict, run_id: str,
                      attempt_key: str, severity: str, outcome: str,
                      expected_value, observed_value,
                      message: str | None = None) -> None:
    """Durable evidence for one Raw ingestion control (Phase 7).

    Called for BOTH the passing and the failing case, right next to the check
    itself -- so a PASS is exactly as durable as a FAIL, and the values
    compared are the ones this delivery actually had, not today's config.
    Best-effort: a registry outage must not turn a successful, already-decided
    check into a failed ingest. See `registry/validation.py`.
    """
    from reporting_platform.registry import validation

    validation.record_quietly(
        layer="raw", control_id=control_id, control_name=control_id,
        outcome=outcome, severity=severity, attempt_key=attempt_key,
        run_id=None, execution_ref=run_id, feed=fd.name,
        delivery_id=manifest.get("delivery_id"),
        evidence_ref=manifest.get("source_object"),
        expected_value=expected_value, observed_value=observed_value,
        message=message,
    )


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


def _checksum_objects(manifest: dict) -> list[str]:
    """The objects a declared md5 covers, in the order they are hashed.

    THE NORMALIZER DECIDES, not this module: for a plain delivery the sender
    hashed the object that landed, and for an archive it hashed the CONTAINER,
    while the parts are members this platform extracted and no sender ever
    checksummed. Making that the manifest's answer is what keeps one code path
    here -- ingest reads `parts` to load rows and this to verify bytes, and
    never asks what kind of delivery it is holding.

    Falls back to `parts` for a manifest written before the key existed. Every
    delivery that could carry a declared md5 then was single-part, and its one
    part IS its source object, so the fallback is the same bytes -- not a
    guess, an identity that happened to hold.
    """
    keys = manifest.get("checksum_objects")
    if keys:
        return list(keys)
    return [p["object_key"] for p in manifest["parts"]]


def _delivery_md5(manifest: dict) -> str:
    """md5 of the delivery's bytes, as landed.

    Read through boto3 rather than Spark: this is a checksum of the OBJECT,
    which is what the sender hashed, not of the rows Spark parsed out of it.
    Going through the dataframe would hash a re-serialisation and never match.
    """
    import hashlib

    from reporting_platform.ingest.arrival import _bucket, _client

    digest = hashlib.md5()
    s3, bucket = _client(), _bucket()
    for key in _checksum_objects(manifest):
        digest.update(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    return digest.hexdigest()


def _landing_uri(object_key: str) -> str:
    bucket = settings.landing()
    root = bucket.rsplit("/", 1)[0] if bucket.endswith("/landing") else bucket
    return f"{root}/{object_key}" if not object_key.startswith("s3a://") else object_key


def ensure_raw_namespace(spark, fd) -> None:
    """Create the feed's raw namespace on `main`, idempotently.

    Separate from ensure_raw_table because it has to happen at a different
    REFERENCE and a different moment -- see the call site in ingest(), and
    docs/DECISIONS.md#namespace-before-branch for why it is `main`.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{fd.raw_namespace}")


# PROVENANCE COLUMNS, added together and listed once (REQ-101, REQ-303,
# REQ-304). Each answers a question about the DELIVERY the row came from
# rather than about the row:
#
#   _delivery_id     which delivery, joinable to `registry.delivery` on
#                    (feed, delivery_id). Not `_source_file`, which is the
#                    PART -- for an archive those differ. Legacy v1 pending
#                    detection uses that part; v2 ingestion is ledgered by
#                    this DeliveryID instead.
#   _received_at     when it arrived, which is not `_ingest_ts`: a delivery
#                    landed Friday and ingested Monday has two timestamps.
#   _schema_version  the declared column contract it was read against.
#   _source_system   which upstream, without a join to config that moves.
#
# Four at once rather than one per requirement: `ALTER TABLE ... ADD COLUMNS`
# is cheap in Iceberg but still a commit on every raw table, and prepared has
# to be edited to carry each of them.
_PROVENANCE_COLUMNS = (
    ("_delivery_id", "STRING"),
    ("_received_at", "TIMESTAMP"),
    ("_schema_version", "STRING"),
    ("_source_system", "STRING"),
)


def ensure_raw_table(spark, fd, table: str | None = None) -> None:
    """Create the raw table if absent.

    cob_date is the LEADING partition field on every table. This is not a
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
        _cob_date DATE,
        _ingest_ts     TIMESTAMP,
        _source_file   STRING,
        _file_version  INT,
        _row_number    BIGINT,
        _batch_id      STRING,
        _delivery_id   STRING,
        _received_at   TIMESTAMP,
        _schema_version STRING,
        _source_system STRING
        )
        USING iceberg
        PARTITIONED BY (days(_cob_date))
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


def plan_raw_schema(fd, have) -> dict[str, list]:
    """What an existing raw table is missing, and what it carries that the
    feed no longer declares. PURE -- no Spark, no catalog.

    `have` is the table's columns as (name, type) pairs, exactly what
    `DataFrame.dtypes` gives. Everything this migration turns on is decided
    here, out of `feeds.yml` alone, so it is testable without a stack.

    THE TWO DIRECTIONS ARE NOT SYMMETRICAL, and that is the whole design:

      add       a column `feeds.yml` declares that the table does not have.
                APPLIED automatically, on the branch. An upstream extending
                its extract is the ordinary event in the life of a feed and
                must not need a hand-written migration.

      orphaned  a column the table has that `feeds.yml` no longer declares.
                NEVER APPLIED. Dropping it would delete history to satisfy a
                config edit, and a rename is indistinguishable from a drop
                plus an add -- so the destructive reading of an ambiguous edit
                is the one this refuses. Reported, and filled with NULL so the
                append still resolves.

    WHICH IS WHICH IS DERIVED, NOT LISTED, by the same rule
    `lineage/columns.py:ingest_columns` uses: a raw table is the feed's
    declared columns plus the platform's own, the platform's all begin with
    `_`, so a column that is neither is one `feeds.yml` used to declare. An
    orphan here is the same column `lineage --columns` reports as
    `unresolved`.
    """
    present = {name.lower() for name, _ in have}
    declared = [(c, "STRING") for c in fd.columns]
    add = [(name, ddl) for name, ddl in declared + list(_PROVENANCE_COLUMNS)
           if name.lower() not in present]
    keep = {c.lower() for c in fd.columns}
    orphaned = [(name, ddl) for name, ddl in have
                if not name.startswith("_") and name.lower() not in keep]
    return {"add": add, "orphaned": orphaned}


def ensure_raw_schema(spark, table: str, fd) -> dict[str, list]:
    """Bring an existing raw table up to the feed's current column contract.

    THE MIGRATION FOR TABLES THAT ALREADY EXIST. `ensure_raw_table` is
    `CREATE TABLE IF NOT EXISTS`, so a column added to `feeds.yml` -- or to
    `_PROVENANCE_COLUMNS` -- reaches new tables only; every raw table already
    in the catalog would keep the old schema forever.

    THIS USED TO COVER THE PROVENANCE COLUMNS AND NOTHING ELSE, which left the
    common case unhandled. Declaring a new column on an existing feed passed
    every check in this module -- the file has it, the contract has it, drift
    is empty -- and then failed inside `df.writeTo().append()` on every
    delivery from then on, with
    `[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]`.

    Note what that error says and does not say: an ARITY mismatch, naming
    neither the column nor `feeds.yml`. The append checks the COUNT first and
    only then resolves BY NAME (verified: a same-arity frame written in
    reversed column order reads back correctly), which is what makes the NULL
    fill for an orphan safe to append at the end of the frame.

    ADDED, NOT BACKFILLED, like the provenance columns before them. Iceberg
    adds a column as metadata -- no data files are touched -- so earlier rows
    read NULL. That is the honest answer for a column the upstream was not
    sending, and where a delivery DID carry it undeclared the value is in
    `_extra_columns`.

    For a PROVENANCE column that has a consequence worth restating: an as-of
    query cannot use `_delivery_id` to reach past the change and must fall back
    to `_source_file`. The alternative was rewriting every partition of every
    raw table -- new data files under live published tags, interacting with
    snapshot expiry and the pins retention keeps.

    Reads the current column list rather than relying on `ADD COLUMNS IF NOT
    EXISTS`, which Iceberg's Spark extensions do not accept: an unconditional
    ADD of an existing column fails the whole statement, and this runs on every
    ingest.

    New columns land at the END of the schema rather than in declared order.
    Nothing reads raw positionally, so reordering would be a commit on every
    raw table to change a `DESCRIBE`.
    """
    plan = plan_raw_schema(fd, spark.sql(f"SELECT * FROM {table} LIMIT 0").dtypes)
    if plan["add"]:
        cols = ", ".join(f"`{name}` {ddl}" for name, ddl in plan["add"])
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({cols})")
        log.info("added columns to %s: %s", table,
                 ", ".join(name for name, _ in plan["add"]))
    if plan["orphaned"]:
        # Not an error, and deliberately not fatal -- see plan_raw_schema.
        # Loud, though: this is a table and a contract disagreeing, which
        # `lineage --columns` also reports and which somebody has to settle.
        log.warning(
            "%s carries %s, which %s no longer declares. Kept and written as "
            "NULL; drop it deliberately or re-declare it.", table,
            ", ".join(name for name, _ in plan["orphaned"]), fd.name)
    return {"added": [name for name, _ in plan["add"]],
            "orphaned": plan["orphaned"]}


def read_landing(spark, fmt: dict, uri: str):
    """Read strings with the recorded contract; old manifests keep their defaults.

    `fmt` comes from the delivery's MANIFEST, not from feeds.yml, so an ingest
    is reproducible: what delimiter a delivery was actually read with is
    recorded next to it rather than inferred from whatever the config says
    today. See ingest/normalize.py.
    """
    from reporting_platform.common.parsing import csv_rows, spark_encoding
    if fmt.get("parser_contract", 1) not in (1, 2):
        raise ValueError(f"unsupported CSV parser contract {fmt['parser_contract']!r}")
    if fmt.get("parser_contract", 1) >= 2:
        # Java's CSV decoder can replace bad bytes even in FAILFAST mode.
        # Validate the original bytes before giving them to that reader.
        from urllib.parse import urlsplit
        from pathlib import Path
        from reporting_platform.ingest.arrival import _client
        location = urlsplit(uri)
        if location.scheme in ("s3", "s3a"):
            body = _client().get_object(Bucket=location.netloc,
                                        Key=location.path.lstrip("/"))["Body"]
            try:
                data = body.read()
            finally:
                body.close()
        elif location.scheme in ("", "file"):
            data = Path(location.path if location.scheme else uri).read_bytes()
        else:
            raise ValueError(f"strict CSV validation does not support {location.scheme!r}")
        for _ in csv_rows(data, fmt, source=uri):
            pass
    reader = (
        spark.read.option("header", str(fmt["header"]).lower())
        .option("sep", fmt["delimiter"])
        .option("quote", fmt["quote_char"])
        .option("encoding", spark_encoding(fmt["encoding"]))
        .option("escape", fmt.get("escape_char", "\\"))
        .option("multiLine", str(fmt.get("multiline", False)).lower())
        .option("mode", "FAILFAST" if fmt.get("parser_contract", 1) >= 2 else "PERMISSIVE")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .option("emptyValue", "")
    )
    if not fmt["header"] and fmt.get("columns"):
        from pyspark.sql.types import StringType, StructField, StructType
        reader = reader.schema(StructType([StructField(name, StringType(), True)
                                          for name in fmt["columns"]]))
    return reader.csv(uri)


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


def next_file_version(spark, fd, cob_date: date,
                      table: str | None = None) -> int:
    try:
        row = spark.sql(
            f"SELECT COALESCE(MAX(_file_version), 0) AS v FROM {table or fd.raw_table} "
            f"WHERE _cob_date = DATE '{cob_date:%Y-%m-%d}'"
        ).collect()[0]
        return int(row["v"]) + 1
    except Exception:
        return 1


def already_ingested_delivery(spark, fd, delivery_id: str,
                                table: str | None = None) -> bool:
    """Whether Raw on ``main`` contains the accepted Delivery.

    This is the v2 ledger.  It deliberately queries ``_delivery_id`` rather
    than any physical part in ``_source_file``.  A failed branch write is not
    visible on ``main`` and therefore remains eligible for retry.

    Legacy Ready v1 continues to use :func:`arrival.already_ingested`, whose
    part-key semantics are unchanged.
    """
    if not isinstance(delivery_id, str) or not _DELIVERY_ID.fullmatch(delivery_id):
        raise ValueError(f"invalid opaque DeliveryID {delivery_id!r}")
    target = table or fd.raw_table
    if not spark.catalog.tableExists(target):
        return False
    # A chunked caller deliberately reuses one SparkSession across branch
    # merges.  Spark can retain the pre-merge Iceberg metadata in that
    # session; refresh before asking Raw to act as the ledger or an immediate
    # retry can miss the commit it just made and append the Delivery twice.
    spark.catalog.refreshTable(target)
    columns = {name.lower() for name, _ in
               spark.sql(f"SELECT * FROM {target} LIMIT 0").dtypes}
    if "_delivery_id" not in columns:
        return False
    escaped = delivery_id.replace("'", "''")
    rows = spark.sql(
        f"SELECT 1 AS present FROM {target} "
        f"WHERE _delivery_id = '{escaped}' LIMIT 1"
    ).collect()
    return bool(rows)


def raw_delivered_ids(spark, fd, table: str | None = None) -> set[str]:
    """Every ``_delivery_id`` Raw ``main`` already holds for this Feed.

    A bulk sibling of :func:`already_ingested_delivery`, for a caller that
    needs to know the state of MANY Deliveries -- Phase 6 reconciliation,
    scanning candidates found by walking ``deliveries/`` in object storage.
    One query per Feed rather than one per Delivery is what keeps that walk
    from costing a Spark application per candidate: Raw is the only ledger
    for v2 ingestion state (`docs/RAW-INGESTION-CONTRACT.md`), so there is no
    cheaper index to ask instead.
    """
    target = table or fd.raw_table
    if not spark.catalog.tableExists(target):
        return set()
    spark.catalog.refreshTable(target)
    columns = {name.lower() for name, _ in
               spark.sql(f"SELECT * FROM {target} LIMIT 0").dtypes}
    if "_delivery_id" not in columns:
        return set()
    rows = spark.sql(
        f"SELECT DISTINCT _delivery_id FROM {target} "
        f"WHERE _delivery_id IS NOT NULL"
    ).collect()
    return {row["_delivery_id"] for row in rows}


def committed_rows_from_raw(spark, fd, table: str | None = None) -> list[dict]:
    """Every (delivery_id, cob_date, source_system, file_version, rows) Raw
    `main` already holds for this Feed -- the BACKFILL source for
    `registry.delivery_committed` (registry/db.py's header). A Delivery
    ingested before that table existed committed just as durably; this is
    how `python -m scripts._spark_task reconcile-committed <feed>` recovers
    that fact without re-ingesting anything. One bulk query, the same shape
    as `raw_delivered_ids`.
    """
    target = table or fd.raw_table
    if not spark.catalog.tableExists(target):
        return []
    spark.catalog.refreshTable(target)
    columns = {name.lower() for name, _ in
              spark.sql(f"SELECT * FROM {target} LIMIT 0").dtypes}
    if "_delivery_id" not in columns:
        return []
    rows = spark.sql(
        f"SELECT _delivery_id AS delivery_id, _cob_date AS cob_date, "
        f"       _source_system AS source_system, "
        f"       MAX(_file_version) AS file_version, COUNT(*) AS rows "
        f"FROM {target} WHERE _delivery_id IS NOT NULL "
        f"GROUP BY _delivery_id, _cob_date, _source_system"
    ).collect()
    return [{"delivery_id": r["delivery_id"], "cob_date": r["cob_date"],
             "source_system": r["source_system"],
             "file_version": r["file_version"], "rows": r["rows"]}
            for r in rows]


def _v2_feed_contract(fd, manifest: dict):
    """Feed-shaped historical read contract captured in Normalization v2.

    Table naming still comes from the registered Feed.  Values that decide
    how the Delivery is parsed and validated come from immutable evidence.
    The ``get`` fallbacks are solely for Phase-2/early-Phase-3 manifests that
    predate the additive source/control snapshot; new manifests carry them.
    """
    contract = manifest.get("normalization_contract")
    if not isinstance(contract, dict):
        raise ValueError("NormalizationManifest v2 has no normalization_contract")
    columns = contract.get("columns")
    source_columns = contract.get("source_columns")
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        raise ValueError("NormalizationManifest v2 contract has malformed columns")
    if not isinstance(source_columns, dict):
        raise ValueError("NormalizationManifest v2 contract has malformed source_columns")
    if not all(isinstance(k, str) and isinstance(v, str)
               for k, v in source_columns.items()):
        raise ValueError("NormalizationManifest v2 contract has malformed source_columns")
    source_system = manifest.get("source_system", contract.get("source_system"))
    if source_system is None:
        source_system = fd.source_system
    if not isinstance(source_system, str) or not source_system:
        raise ValueError("NormalizationManifest v2 contract has malformed source_system")
    expected_min_rows = contract.get("expected_min_rows", fd.expected_min_rows)
    if type(expected_min_rows) is not int or expected_min_rows < 0:
        raise ValueError(
            "NormalizationManifest v2 contract has malformed expected_min_rows")
    expected_max_rows = contract.get("expected_max_rows", fd.expected_max_rows)
    if expected_max_rows is not None and (
            type(expected_max_rows) is not int or expected_max_rows < 0):
        raise ValueError(
            "NormalizationManifest v2 contract has malformed expected_max_rows")
    schema_drift = contract.get("schema_drift", fd.schema_drift)
    if schema_drift not in ("warn", "fail"):
        raise ValueError("NormalizationManifest v2 contract has malformed schema_drift")
    if manifest.get("format") != contract.get("format"):
        raise ValueError(
            "NormalizationManifest v2 format conflicts with its frozen contract")
    return replace(
        fd,
        columns=list(columns),
        source_columns=dict(source_columns),
        source_system=str(source_system),
        expected_min_rows=expected_min_rows,
        expected_max_rows=expected_max_rows,
        schema_drift=schema_drift,
    )


def _canonical_v2_manifest(manifest: dict, key: str) -> dict:
    """Validate and adapt explicit v2 names to the shared Raw machinery."""
    if manifest.get("normalization_manifest_version") != 2:
        raise ValueError(f"{key}: expected NormalizationManifest v2")
    required = ("delivery_id", "feed", "business_date", "received_at",
                "schema_version", "format", "parts", "normalization_contract")
    missing = [name for name in required if name not in manifest]
    if missing:
        raise ValueError(f"{key}: missing v2 fields: {', '.join(missing)}")
    if (not isinstance(manifest["delivery_id"], str)
            or not _DELIVERY_ID.fullmatch(manifest["delivery_id"])):
        raise ValueError(f"{key}: v2 delivery_id is not opaque")
    if not isinstance(manifest["feed"], str) or not manifest["feed"]:
        raise ValueError(f"{key}: v2 feed is malformed")
    if not isinstance(manifest["schema_version"], str) or not manifest["schema_version"]:
        raise ValueError(f"{key}: v2 schema_version is malformed")
    if not isinstance(manifest["parts"], list):
        raise ValueError(f"{key}: v2 parts are malformed")
    if not all(isinstance(part, dict)
               and isinstance(part.get("object_key"), str)
               and part["object_key"] for part in manifest["parts"]):
        raise ValueError(f"{key}: v2 part object_key is malformed")
    return {**manifest, "cob_date": manifest["business_date"]}


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
    # spark.stop() in a finally -- which stops the whole SparkContext, so it
    # silently killed the shared session `_ingest_chunk` had passed into
    # ingest(), and every subsequent read failed with a Py4JJavaError.
    #
    # It only fires when `main` has no commits, so a warm stack never reaches
    # it: the failure appeared exactly once, on the first ingest of a cold
    # rebuild.
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


def resolve_delivery(fd, key: str, cob_date: date | None = None) -> dict:
    """The manifest for `key`, whether it names a manifest or a landing object.

    Ingest consumes MANIFESTS (see ingest/normalize.py). A landing key is
    still accepted and normalized on the fly WITHOUT being written to
    `ready/`, because `--object landing/...` is what every runbook, the README
    walkthrough and docs/ADDING-A-FEED.md tell you to type, and a one-off
    manual ingest should not leave a queue entry behind.

    `cob_date` overrides whatever the delivery says, which is how a file
    whose name carries no parsable date gets ingested at all.
    """
    from reporting_platform.ingest import normalize as norm

    if norm.is_manifest_key(fd, key):
        manifest = norm.read_manifest(key)
    else:
        try:
            manifest = norm.normalize(fd, key, write=False)
        except ValueError:
            if cob_date is None:
                raise
            # No parsable date in the name, but the caller supplied one.
            # Build the manifest by hand rather than refusing: this is the
            # documented escape hatch for a delivery the pattern cannot route.
            manifest = {
                "manifest_version": norm.MANIFEST_VERSION,
                "feed": fd.name, "cob_date": cob_date.isoformat(),
                "delivery_id": key.rsplit("/", 1)[-1], "received_at": None,
                "source_object": key,
                "parts": [{"object_key": key, "bytes": None}],
                "format": feed_format(fd),
                "control_object": None, "declared_row_count": None,
                "normalizer": "manual/v1",
            }
    if cob_date is not None:
        manifest = {**manifest, "cob_date": cob_date.isoformat()}
    return manifest


def ingest(feed_name: str, object_key: str, run_id: str | None = None,
           cob_date: date | None = None, dry_run: bool = False,
           spark=None) -> dict:
    """Legacy Ready v1/Landing ingestion, retained unchanged in Phase 4."""
    fd = get_feed(feed_name)
    manifest = resolve_delivery(fd, object_key, cob_date)
    return _ingest_manifest(
        fd, fd, manifest, object_key, run_id=run_id, dry_run=dry_run,
        spark=spark, ledger="legacy_source_file")


def ingest_normalized_delivery(normalization_manifest_key: str,
                               run_id: str | None = None,
                               dry_run: bool = False, spark=None) -> dict:
    """Ingest one NormalizationManifest v2 without a Landing dependency.

    The key is the complete caller contract: feed, DeliveryID, frozen business
    identity, parser/schema contract, source system and physical parts are read
    from the manifest.  Raw itself is the ingestion ledger, keyed by the opaque
    DeliveryID.
    """
    from reporting_platform.ingest import arrival
    from reporting_platform.ingest.normalization import read_normalization_manifest

    manifest = read_normalization_manifest(
        normalization_manifest_key, client=arrival._client(),  # noqa: SLF001
        bucket=arrival._bucket())  # noqa: SLF001
    manifest = _canonical_v2_manifest(manifest, normalization_manifest_key)
    fd = get_feed(manifest["feed"])
    contract_fd = _v2_feed_contract(fd, manifest)
    return _ingest_manifest(
        fd, contract_fd, manifest, normalization_manifest_key,
        run_id=run_id, dry_run=dry_run, spark=spark, ledger="delivery_id")


def _ingest_manifest(fd, contract_fd, manifest: dict, object_key: str,
                     *, run_id: str | None, dry_run: bool, spark,
                     ledger: str) -> dict:
    """Shared branch/write/validate/merge implementation for explicit v1/v2.

    `object_key` is a MANIFEST key under `ready/`, or a landing object key --
    for v1 see `resolve_delivery`.  The v2 caller supplies an already-read
    NormalizationManifest and never enters Landing.

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

    run_id = run_id or new_run_id()
    bdate = date.fromisoformat(manifest["cob_date"])
    parts = manifest["parts"]
    if not parts:
        raise ValueError(
            f"{fd.name}: manifest {object_key} lists no parts. Nothing to "
            f"ingest; `ready/` is a cache, so delete it and re-normalize.")

    nessie = Nessie()
    _bootstrap_main_if_empty(nessie, fd, spark)

    # The session is bound to `main` and the BRANCH is named per statement, so
    # one session can serve many files.
    owns_session = spark is None
    if owns_session:
        spark = spark_session(f"ingest-{fd.name}-{run_id}", ref="main")

    # ON MAIN, AND BEFORE THE BRANCH IS CUT. A namespace cannot be created on
    # a branch: Nessie's `@branch` suffix applies to a TABLE identifier, and
    # using it on a namespace does not fail -- it creates a namespace literally
    # named "`raw_x@ingest/...`" on main. Verified against the live catalog.
    #
    # So the namespace must exist on main first and the branch inherits it.
    # Creating it after the branch was cut broke the first ingest into a new
    # raw_<source> namespace with `NoSuchNamespaceException: raw` -- CREATE
    # NAMESPACE landed on main while CREATE TABLE addressed the branch. It
    # hides for as long as the namespace already exists, which on a warm stack
    # it always does.
    ensure_raw_namespace(spark, fd)

    # The v2 no-op guard is inside the domain operation, not merely in a
    # discovery queue.  It runs against main before a branch is cut, so an
    # explicit retry cannot append the same Delivery twice.  A failed branch
    # is invisible here and remains retryable.
    if ledger == "delivery_id" and already_ingested_delivery(
            spark, fd, manifest["delivery_id"]):
        result = {
            "feed": fd.name,
            "cob_date": bdate.isoformat(),
            "file_version": None,
            "rows": 0,
            "run_id": run_id,
            "branch": None,
            "source_file": parts[0]["object_key"],
            "manifest": object_key,
            "parts": len(parts),
            "delivery_id": manifest["delivery_id"],
            "schema_version": manifest["schema_version"],
            "source_system": contract_fd.source_system,
            "already_ingested": True,
            "columns_added": [],
            "columns_orphaned": [],
            "missing_columns": [],
            "extra_columns": [],
            "asset_uri": fd.asset_uri,
        }
        # This IS a commit -- `already_ingested_delivery` only returns True
        # for a Delivery Raw already holds -- so the fact belongs here too,
        # not only on the fresh-write path below. `rows` is left unknown
        # rather than re-counted: nothing downstream needs it enough to
        # justify a Spark action on an idempotent no-op.
        from reporting_platform.registry import deliveries as registry_deliveries
        registry_deliveries.record_committed_quietly(
            fd.name, manifest["delivery_id"], bdate, contract_fd.source_system,
            None, run_id)
        if owns_session:
            spark.stop()
        return result

    branch = branch_name("ingest", fd.name, bdate, run_id)
    nessie.create_branch(branch)
    log.info("created branch %s", branch)

    raw_at_branch = _at_branch(fd.raw_table, branch)
    try:
        ensure_raw_table(spark, contract_fd, raw_at_branch)
        # ON THE BRANCH, like the CREATE above: a schema change is a commit,
        # and a commit against `main` outside the merge is what
        # write-audit-publish exists to prevent. If the ingest then fails the
        # branch is abandoned and the column was never added to main either.
        schema = ensure_raw_schema(spark, raw_at_branch, contract_fd)
        version = next_file_version(spark, fd, bdate, raw_at_branch)

        # ONE DATAFRAME PER PART, each tagged with its OWN object key, then
        # unioned. `_source_file` is physical provenance in both paths. Legacy
        # pending detection also uses it as its ledger; v2 does not -- v2 uses
        # the shared DeliveryID carried by every one of these part frames.
        frames, drift = [], {"missing_columns": [], "extra_columns": []}
        for part in parts:
            part_df = read_landing(spark, manifest["format"],
                                   _landing_uri(part["object_key"]))
            part_df, part_drift = reconcile_schema(part_df, contract_fd)
            frames.append(
                part_df.withColumn("_source_file", F.lit(part["object_key"])))
            for k in drift:
                drift[k] += [c for c in part_drift[k] if c not in drift[k]]

        df = frames[0]
        for extra in frames[1:]:
            df = df.unionByName(extra)

        # `schema_drift: fail` was documented in feeds.yml and read by
        # NOTHING: setting it to `fail` was silently identical to `warn`, so a
        # feed configured to abort on drift loaded regardless.
        #
        # It fires on extra AND missing columns. An extra column is the obvious
        # case; a missing declared column is the quieter one, because
        # reconcile_schema fills it with nulls, so the load succeeds and the
        # column reads as "no value" rather than "never arrived" from then on.
        # A typo here would silently mean "warn", which is how this setting
        # managed to do nothing for so long. Reject anything unknown.
        if contract_fd.schema_drift not in ("warn", "fail"):
            raise ValueError(
                f"{fd.name}: schema_drift must be 'warn' or 'fail', got "
                f"{contract_fd.schema_drift!r}"
            )
        drifted = bool(drift["missing_columns"] or drift["extra_columns"])
        attempt_key = f"{run_id}:{manifest['delivery_id']}"
        _record_raw_check(
            control_id="schema_drift", fd=fd, manifest=manifest,
            run_id=run_id, attempt_key=attempt_key,
            severity="blocking" if contract_fd.schema_drift == "fail" else "warn",
            outcome=("FAIL" if drifted and contract_fd.schema_drift == "fail"
                     else "WARN" if drifted else "PASS"),
            expected_value="no drift",
            observed_value=(f"missing={drift['missing_columns']} "
                            f"extra={drift['extra_columns']}") if drifted else "none",
            message=None if not drifted else
            f"missing {drift['missing_columns']}, extra {drift['extra_columns']}",
        )
        if contract_fd.schema_drift == "fail" and drifted:
            raise ValueError(
                f"{fd.name} {bdate}: schema drift with schema_drift=fail — "
                f"missing {drift['missing_columns']}, "
                f"extra {drift['extra_columns']}. Branch {branch} left for "
                f"inspection; main is untouched."
            )

        row_win = Window.orderBy(F.monotonically_increasing_id())
        df = (
            df.withColumn("_cob_date", F.lit(bdate.isoformat()).cast("date"))
              .withColumn("_ingest_ts", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
              .withColumn("_file_version", F.lit(version).cast("int"))
              .withColumn("_row_number", F.row_number().over(row_win).cast("bigint"))
              .withColumn("_batch_id", F.lit(run_id))
              # Provenance, from the MANIFEST and the feed's declared
              # contract, not from the filename. `received_at` is the
              # delivery's arrival time -- the landing object's LastModified --
              # and is null only for the hand-built manifest of the
              # `--cob-date` escape hatch, which has no landed object.
              .withColumn("_delivery_id", F.lit(manifest["delivery_id"]))
              .withColumn("_received_at",
                          F.lit(manifest.get("received_at")).cast("timestamp"))
              .withColumn("_schema_version", F.lit(
                  manifest.get("schema_version", contract_fd.schema_version)))
              .withColumn("_source_system", F.lit(contract_fd.source_system))
        )

        # A column the table still has and `feeds.yml` no longer declares.
        # `writeTo().append()` resolves BY NAME and wants a value for every
        # column the table has, so an orphan has to be written -- and NULL is
        # the true one: the contract this delivery was read against did not ask
        # for it. Dropping it would delete history to satisfy a config edit.
        for orphan, dtype in schema["orphaned"]:
            df = df.withColumn(orphan, F.lit(None).cast(dtype))

        row_count = df.count()
        below_floor = row_count < contract_fd.expected_min_rows
        _record_raw_check(
            control_id="expected_min_rows", fd=fd, manifest=manifest,
            run_id=run_id, attempt_key=attempt_key, severity="blocking",
            outcome="FAIL" if below_floor else "PASS",
            expected_value=f">= {contract_fd.expected_min_rows}",
            observed_value=row_count,
        )
        if below_floor:
            # Abandon the branch: main is untouched, nothing to roll back.
            raise ValueError(
                f"{fd.name} {bdate}: {row_count} rows, below expected minimum "
                f"{contract_fd.expected_min_rows}. Branch {branch} left for inspection."
            )

        # The CEILING next to the floor. A platform expectation about the
        # feed as a whole -- distinct from `declared_row_count` below, which
        # is what THIS delivery's producer asserted. See docs/VALIDATION.md.
        if contract_fd.expected_max_rows is not None:
            above_ceiling = row_count > contract_fd.expected_max_rows
            _record_raw_check(
                control_id="expected_max_rows", fd=fd, manifest=manifest,
                run_id=run_id, attempt_key=attempt_key, severity="blocking",
                outcome="FAIL" if above_ceiling else "PASS",
                expected_value=f"<= {contract_fd.expected_max_rows}",
                observed_value=row_count,
            )
            if above_ceiling:
                raise ValueError(
                    f"{fd.name} {bdate}: {row_count} rows, above expected "
                    f"maximum {contract_fd.expected_max_rows}. Branch {branch} "
                    f"left for inspection."
                )

        # The EXACT count next to the floor above. expected_min_rows catches a
        # truncated file; a control file states what the sender counted, so
        # this is an equality check, not another floor. Only a feed with
        # `delivery.control.row_count` and a matching control file has one.
        declared = manifest.get("declared_row_count")
        if declared is not None:
            mismatched = row_count != declared
            _record_raw_check(
                control_id="declared_row_count", fd=fd, manifest=manifest,
                run_id=run_id, attempt_key=attempt_key, severity="blocking",
                outcome="FAIL" if mismatched else "PASS",
                expected_value=declared, observed_value=row_count,
            )
            if mismatched:
                raise ValueError(
                    f"{fd.name} {bdate}: {row_count} rows read, control file "
                    f"{manifest.get('control_object')} declared {declared}. "
                    f"Branch {branch} left for inspection; main is untouched."
                )

        # The checksum, next to the count. It catches what the count cannot: a
        # delivery truncated or re-encoded in transit that still holds the
        # right NUMBER of rows. Hashed from the parts as landed, which for a
        # delivery the inbox promoted is byte-identical to what the upstream
        # sent -- the gate renames, it never rewrites.
        #
        # THIS RUNS FOR EVERY DELIVERY, whichever way it arrived. That is the
        # point of checking here rather than at the door: an approved sender
        # gets the same verification as a legacy feed, from one implementation.
        # See docs/DECISIONS.md#the-inbox-is-the-conformance-gate
        # Lower-cased on BOTH sides. `normalize._declared` already does this
        # when it writes the manifest, but hex case is a property of the
        # sender's file and not of our reader: an older manifest still in
        # `ready/`, or one written by hand, would fail here on a checksum that
        # matches, and the message this raises sends the reader looking for
        # corruption that did not happen.
        declared_md5 = (manifest.get("declared_md5") or "").strip().lower() or None
        if declared_md5 is not None:
            hashed = _checksum_objects(manifest)
            actual_md5 = _delivery_md5(manifest)
            checksum_mismatch = actual_md5 != declared_md5
            _record_raw_check(
                control_id="declared_md5", fd=fd, manifest=manifest,
                run_id=run_id, attempt_key=attempt_key, severity="blocking",
                outcome="FAIL" if checksum_mismatch else "PASS",
                expected_value=declared_md5, observed_value=actual_md5,
            )
            if checksum_mismatch:
                raise ValueError(
                    f"{fd.name} {bdate}: {', '.join(hashed)} hashes to "
                    f"{actual_md5}, control file "
                    f"{manifest.get('control_object')} declared "
                    f"{declared_md5}. The file is not the file the sender "
                    f"checksummed -- truncated, re-encoded in transit, or a "
                    f"different file altogether. Branch {branch} left for "
                    f"inspection; main is untouched."
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
            "cob_date": bdate.isoformat(),
            "file_version": version,
            "rows": row_count,
            "run_id": run_id,
            "branch": branch,
            # The object the ROWS came from, which is what `_source_file`
            # holds and what every existing caller prints. For `kind: file`
            # that is the landing key -- not the manifest key that may have
            # been passed in.
            "source_file": parts[0]["object_key"],
            "manifest": (object_key if object_key != parts[0]["object_key"]
                         else None),
            "parts": len(parts),
            "delivery_id": manifest["delivery_id"],
            "schema_version": manifest.get("schema_version", contract_fd.schema_version),
            "source_system": contract_fd.source_system,
            "already_ingested": False,
            # What this ingest did to the TABLE, a different event from what
            # it found in the FILE (`missing_columns` / `extra_columns`
            # below). A contract change shows up here on the first delivery
            # after the deploy and never again; file drift shows up per
            # delivery.
            "columns_added": schema["added"],
            "columns_orphaned": [name for name, _ in schema["orphaned"]],
            "asset_uri": fd.asset_uri,
            **drift,
        }
        if drift["missing_columns"] or drift["extra_columns"]:
            log.warning("schema drift on %s: %s", fd.name, json.dumps(drift))
        # NOT under `dry_run`: a dry run never merged, so there is nothing to
        # record as committed -- see registry/db.py's header on this table.
        if not dry_run:
            from reporting_platform.registry import deliveries as registry_deliveries
            registry_deliveries.record_committed_quietly(
                fd.name, manifest["delivery_id"], bdate, contract_fd.source_system,
                row_count, run_id, file_version=version)
        return result
    finally:
        # Only if we made it. A caller that passed one in owns its lifetime.
        if owns_session:
            spark.stop()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--feed", required=True)
    p.add_argument("--object", required=True,
                   help="a manifest key under ready/, or a landing object key "
                        "(normalized on the fly, not written to ready/)")
    p.add_argument("--run-id")
    p.add_argument("--cob-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    print(json.dumps(ingest(a.feed, a.object, a.run_id, a.cob_date, a.dry_run), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
