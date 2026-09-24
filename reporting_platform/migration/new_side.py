"""Reading the new platform's own checkpoint (Raw/prepared/reporting), in Spark.

SPARK-NATIVE, PER SECTION 10's PREFERENCE: aggregation (count, per-key hash,
control totals) happens IN Spark SQL, and only the small aggregated result
-- never the underlying rows -- is collected to the driver. This is the same
boundary `ingest_feed.py` and `registry/inputs.py` already keep, and it is
why this module, like them, is meant to run through
`scripts/_spark_task.py`'s subprocess, never in an Airflow task's own
process (`docs/DECISIONS.md#spark-in-a-subprocess`).

Table resolution:
  * `raw`       -> `<CATALOG>.raw.<feed>`, filtered on `_cob_date`, and on
                   `_delivery_id` when one is supplied (v2 Deliveries only --
                   see `docs/RAW-INGESTION-CONTRACT.md`).
  * `prepared`  -> `<CATALOG>.prepared.<feed>`, filtered on `cob_date`.
  * `reporting` -> a REPORT is not named the same as a Feed
                   (`docs/ARCHITECTURE.md`'s reporting marts join several
                   feeds), so this checkpoint requires an explicit
                   `compare.table` in the feed's `migration:` block naming
                   the reporting table to read.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from reporting_platform.migration.comparators import (
    canonical_row_hash, drop_technical_columns,
)


def _table_name(catalog: str, feed_name: str, checkpoint: str,
               table_override: str | None) -> str:
    if table_override:
        return f"{catalog}.{checkpoint}.{table_override}"
    if checkpoint == "reporting":
        raise ValueError(
            f"migration.compare.checkpoint: reporting for feed {feed_name!r} "
            f"needs an explicit `compare.table` -- a reporting mart is not "
            f"named after any one feed (docs/ARCHITECTURE.md)")
    return f"{catalog}.{checkpoint}.{feed_name}"


def checkpoint_summary(spark, *, catalog: str, feed_name: str,
                       checkpoint: str, business_date: date,
                       key: list[str], columns: list[str],
                       aggregates: list[dict[str, Any]],
                       delivery_id: str | None = None,
                       table_override: str | None = None) -> dict[str, Any]:
    """Row count, {key_tuple_as_list: hash} pairs, and aggregate values for
    one Feed's checkpoint table on one business date.

    Returns `None` fields (`row_count=None`) rather than raising when the
    table does not exist yet -- a feed that has not built this checkpoint
    for this date is "not yet comparable", exactly the RAW-INGESTION
    completeness distinction this platform already makes
    (`docs/DECISIONS.md`'s "no data / no table / unreadable").
    """
    from pyspark.sql.utils import AnalysisException

    table = _table_name(catalog, feed_name, checkpoint, table_override)
    date_column = "_cob_date" if checkpoint == "raw" else "cob_date"
    try:
        df = spark.table(table)
    except AnalysisException:
        return {"table": table, "exists": False, "row_count": None,
               "row_hashes": {}, "aggregates": {}}

    df = df.where(f"{date_column} = '{business_date.isoformat()}'")
    if delivery_id and checkpoint == "raw" and "_delivery_id" in df.columns:
        df = df.where(df["_delivery_id"] == delivery_id)

    row_count = df.count()

    row_hashes: dict[str, str] = {}
    if key and columns:
        wanted = list(dict.fromkeys([*key, *columns]))
        present = [c for c in wanted if c in df.columns]
        pdf_rows = [r.asDict() for r in df.select(*present).collect()]
        for row in pdf_rows:
            clean = drop_technical_columns(row)
            k = "\x1f".join(str(clean.get(c)) for c in key)
            row_hashes[k] = canonical_row_hash(clean, columns)

    agg_values: dict[str, float] = {}
    for agg in aggregates or []:
        column, function = agg["column"], agg["function"]
        if column not in df.columns:
            continue
        # Raw is deliberately all-string (docs/ARCHITECTURE.md: "raw is 1:1
        # and untyped"), so a numeric aggregate needs an explicit CAST at
        # this checkpoint -- Spark's implicit string->double coercion in
        # sum()/min()/max() is not guaranteed across versions/ANSI modes,
        # and a NULL from a failed implicit cast would silently zero out a
        # comparison rather than raising. `prepared`/`reporting` are already
        # typed, so the CAST is a no-op there beyond re-affirming the type.
        cast_col = f"CAST({column} AS DOUBLE)"
        if function == "sum":
            value = df.selectExpr(f"sum({cast_col}) as v").first()["v"]
        elif function == "count":
            value = df.selectExpr(f"count({column}) as v").first()["v"]
        elif function == "count_distinct":
            value = df.selectExpr(f"count(distinct {column}) as v").first()["v"]
        elif function == "min":
            value = df.selectExpr(f"min({cast_col}) as v").first()["v"]
        elif function == "max":
            value = df.selectExpr(f"max({cast_col}) as v").first()["v"]
        else:
            continue
        agg_values[f"{function}:{column}"] = float(value) if value is not None else None

    return {"table": table, "exists": True, "row_count": row_count,
           "row_hashes": row_hashes, "aggregates": agg_values}
