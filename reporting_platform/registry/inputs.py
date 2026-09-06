"""Which deliveries a build actually read, asked of the build itself.

THE ONE SPARK-USING MODULE IN THIS PACKAGE, and it is separate for that
reason. Everything else in `registry/` is boto3, json and psycopg2, so it runs
in an Airflow task process directly; this opens a SparkSession and therefore
has to run through `scripts/_spark_task.py` like every other Spark caller here
(docs/DECISIONS.md#spark-in-a-subprocess).

REQ-400. A run's input set is DERIVED FROM WHAT IT BUILT, not declared before
it: the distinct `delivery_id` in each prepared model on the run's own branch.
That column is `delivery_ref()` -- the recorded `_delivery_id`, or the
basename of `_source_file` for rows that predate provenance -- so the answer
covers the whole history of the table rather than the part of it that happens
to carry the newer column.

SO IT IS "THE DELIVERIES WHOSE ROWS ARE IN WHAT WAS PUBLISHED", NOT "THE
DELIVERIES THE RUN SCANNED", and the two genuinely differ for an SCD2 model.
`ref_counterparty` keeps one row per VERSION, so a delivery that restates an
unchanged entity contributes nothing and does not appear: measured on this
stack, 10 of its 40 ingested deliveries, against 40 of 40 for the per-date
`fo_trade` and 36 of 36 for `ref_rating`, whose ratings change on nearly every
delivery.

That is the right set for the question REQ-602 asks -- re-deriving the
published tables needs exactly the deliveries whose data is in them, and an
SCD2 table re-derives identically from the versions that survived. It is the
WRONG set for "what did this run read", and nothing here should be read as
answering that. Written down because the number looks like an error when you
first see it and is not one.

READ ON THE BRANCH, BEFORE THE MERGE. The branch is the audited state that is
about to become main; reading main afterwards would answer a slightly
different question (what main holds now, including anything else merged in
between) and would make the record depend on the order two builds happened to
publish in.

THE FEED NAME COMES FROM THE MODEL NAME. `prepared.fo_trade` is built from
`raw.fo_trade`, which is the feed `fo_trade`: model filename == table name ==
feed name is the platform's naming rule, the same one `managed_tables()`
already derives from (docs/DECISIONS.md#table-naming-no-layer-prefix). A
prepared model that is not a feed is reported rather than guessed at.
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("registry.inputs")


def collect(branch: str) -> dict[str, Any]:
    """Every (feed, delivery_id) behind the prepared tables on `branch`.

    A model that is absent at the branch is not an error: the platform gains
    models over time and a branch cut before one existed cannot contain it --
    the same rule `monitoring/reproducibility.check_tag` follows. A model that
    exists and cannot be READ is an error, and is reported as one.
    """
    from reporting_platform.common.context import (
        CATALOG, feeds, models_in, spark_session,
    )

    known_feeds = set(feeds())
    out: dict[str, Any] = {"branch": branch, "models": [], "inputs": [],
                           "absent": [], "unreadable": [],
                           "max_business_date": None, "not_a_feed": []}
    spark = spark_session("run-inputs", ref=branch)
    try:
        for model in models_in("prepared"):
            table = f"{CATALOG}.prepared.{model}"
            try:
                columns = {c.lower() for c in
                           spark.sql(f"SELECT * FROM {table} LIMIT 0").columns}
            except Exception as exc:                            # noqa: BLE001
                text = f"{type(exc).__name__}: {str(exc)[:300]}"
                if "NoSuchTable" in text or "TABLE_OR_VIEW_NOT_FOUND" in text:
                    out["absent"].append(model)
                else:
                    out["unreadable"].append({"model": model, "error": text})
                continue

            if "delivery_id" not in columns:
                # The migration guard in _prepared.yml is the loud version of
                # this; here it would otherwise be a silently empty input set,
                # which is the failure mode that matters -- a run claiming to
                # have read nothing looks exactly like a run nobody checked.
                out["unreadable"].append({
                    "model": model,
                    "error": ("no `delivery_id` column: this table predates "
                              "source_provenance() and has not been rebuilt "
                              "since. Its input set cannot be enumerated.")})
                continue

            rows = spark.sql(
                f"SELECT DISTINCT delivery_id FROM {table} "
                f"WHERE delivery_id IS NOT NULL").collect()
            deliveries = sorted(r["delivery_id"] for r in rows)
            if model not in known_feeds:
                out["not_a_feed"].append(model)
            out["models"].append({"model": model,
                                  "deliveries": len(deliveries)})
            out["inputs"].extend([model, d] for d in deliveries)

            # The latest business date this table reflects. `business_date` on
            # a per-date table, `effective_from` on an SCD2 one, which drops
            # business_date entirely -- see scd2_columns().
            date_column = ("business_date" if "business_date" in columns
                           else "effective_from" if "effective_from" in columns
                           else None)
            if date_column:
                value = spark.sql(
                    f"SELECT MAX({date_column}) AS d FROM {table}"
                ).collect()[0]["d"]
                if value is not None:
                    seen = value.isoformat()
                    if out["max_business_date"] is None or seen > out["max_business_date"]:
                        out["max_business_date"] = seen
    finally:
        spark.stop()

    if out["unreadable"]:
        log.error("run inputs incomplete on %s: %s", branch, out["unreadable"])
    if out["not_a_feed"]:
        log.warning("prepared model(s) %s are not feeds; their deliveries are "
                    "recorded under the model name", out["not_a_feed"])
    log.info("run inputs on %s: %d delivery(ies) across %d model(s)",
             branch, len(out["inputs"]), len(out["models"]))
    return out
