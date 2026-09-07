"""Column-level lineage, parsed out of the COMPILED dbt SQL.

WHY COMPILED AND NOT THE MODEL FILE. The models are Jinja: `clean_string()`,
`safe_cast()`, `dedupe_rank()`, `source_provenance()`. The template says
nothing about which columns a macro reads -- that is decided when dbt renders
it -- so the parseable artefact is `target/compiled/**/<model>.sql`, which dbt
writes on every build and which persists at DBT_TARGET_PATH.

WHY sqlglot AND NOT THE PARSER ALREADY IN THE IMAGE. `openlineage-sql` ships
with the provider and produces column lineage, but not usable column lineage
here, measured: of 59 fields it emitted, 26 resolved to a real table and the
rest named the CTE the column came through (`trades`, `deduped`). The prepared
layer -- which is the layer that does the renaming and casting anybody wants to
see -- produced ZERO, because those models open with `select *` and no parser
can expand a star without knowing the table's columns. sqlglot resolves CTEs
and takes a schema, and THE PLATFORM ALREADY READS THAT SCHEMA (`schemas.py`,
off the catalog). With it, 116 of 136 columns trace to a source column; the
20 that do not are `dbt_invocation_id`, `nessie_ref`, `dbt_updated_at` and a
`count(*)` -- columns genuinely derived from no input column. So this is not a
partial answer that looks complete, which is the thing `schemas.py` refuses to
emit: every column that HAS a source gets one.

THE TRANSFORMATION IS REPORTED, not just the dependency. A rename shows as an
input field with a different name (`_business_date` -> `business_date`) and no
description; a computation carries the SQL that performs it, e.g.
`TRY_CAST(NULLIF(NULLIF(NULLIF(TRIM(deduped.notional), ''), 'NULL'), 'N/A') AS
DECIMAL(28,4))`. That is the deepest non-trivial expression on the path from
the output column to its source, which is where the work actually happens --
the outermost one is always a passthrough from the final CTE.

COST. 0.02s to parse a model and 0.17s to trace all its columns, once per task
process, on top of the schema read `schemas.py` already does.

TOTAL, like everything else in this package: this runs inside an OpenLineage
extractor, where an exception costs the datasets of whatever was being
extracted, so every failure returns no column lineage and none of them raise.
"""
from __future__ import annotations

import os
from pathlib import Path

from reporting_platform.common.context import feeds, models_in
from reporting_platform.lineage import graph, schemas

# (dataset, field) an output column was computed from.
Source = tuple[tuple[str, str], str]

_SCHEMA: dict[str, dict[str, dict[str, str]]] | None = None


def model_columns(layer: str, model: str) -> dict[str, tuple[list[Source], str]]:
    """{output column: ([(dataset, field)], transformation or "")}.

    Empty when the compiled SQL is absent -- which is the normal state before
    the model's first build, and after a `dbt clean`.
    """
    try:
        sql = _compiled_sql(layer, model)
        if not sql:
            return {}
        return _trace(layer, model, sql)
    except Exception:                                        # noqa: BLE001
        return {}


def ingest_columns(feed_name: str) -> dict[str, tuple[list[Source], str]]:
    """Raw's columns, mapped back to the names the FILE used.

    Not parsed from anything: ingest performs a declared rename rather than a
    query, so the mapping IS `Feed.source_column()`. The platform's own added
    columns (`_business_date`, the provenance four, and the rest) come from no
    file column and are correctly absent.
    """
    try:
        feed = feeds()[feed_name]
        landing = graph.landing(feed_name)
        out = {}
        for column in feed.columns:
            out[column] = ([(landing, feed.source_column(column))], "")
        return out
    except Exception:                                        # noqa: BLE001
        return {}


# ---------------------------------------------------------------- internals
def _compiled_sql(layer: str, model: str) -> str:
    """The compiled SQL dbt last wrote for this model.

    Globbed rather than built from the project name: the path is
    `<target>/compiled/<project>/models/<layer>/<model>.sql`, and the project
    name is a thing in dbt_project.yml that nothing else here needs to know.
    """
    target = Path(os.environ.get("DBT_TARGET_PATH", "/opt/platform/run/dbt/target"))
    matches = sorted((target / "compiled").glob(f"*/models/{layer}/{model}.sql"))
    return matches[0].read_text(encoding="utf-8") if matches else ""


def _trace(layer: str, model: str, sql: str) -> dict[str, tuple[list[Source], str]]:
    import sqlglot
    from sqlglot import exp
    from sqlglot.lineage import lineage

    tree = sqlglot.parse_one(sql, dialect="spark")
    out: dict[str, tuple[list[Source], str]] = {}
    for column, _type in schemas.table_columns(layer, model):
        try:
            node = lineage(column, tree, schema=_schema(), dialect="spark")
        except Exception:                                    # noqa: BLE001
            # sqlglot raises for a column the query does not produce, which
            # happens whenever the TABLE has a column the current SQL does not
            # -- a column added by a later model version, or dropped from it.
            continue
        sources = _sources(node, exp)
        if sources:
            out[column] = (sources, _transformation(node, exp))
    return out


def _sources(node, exp) -> list[Source]:
    found: list[Source] = []
    for item in node.walk():
        if not isinstance(item.source, exp.Table):
            continue
        table = item.source
        dataset = graph.table(table.db, table.name) if table.db else None
        # `item.name` is `<alias>.<column>`; the column is what OpenLineage
        # wants, and the alias is the CTE or table alias it arrived through.
        field = item.name.rsplit(".", 1)[-1]
        if dataset and (dataset, field) not in found:
            found.append((dataset, field))
    return found


def _transformation(node, exp) -> str:
    """The deepest expression that actually computes something.

    Walking outermost-first, each layer is usually `typed.notional AS
    notional` -- a passthrough between CTEs. The last non-passthrough before
    the table is the cast, the trim or the case expression, which is the one
    worth showing.
    """
    deepest = ""
    for item in node.walk():
        if isinstance(item.source, exp.Table):
            continue
        expression = item.expression
        if not isinstance(expression, exp.Expression):
            continue
        inner = expression.this if isinstance(expression, exp.Alias) else expression
        if isinstance(inner, (exp.Column, exp.Star)):
            continue
        deepest = " ".join(inner.sql(dialect="spark").split())
    return deepest


def _schema() -> dict[str, dict[str, dict[str, str]]]:
    """Every managed table's columns, in the shape sqlglot resolves `*` with.

    Per process, and built from `schemas.py` -- so the star expansion and the
    schema facet are the same read of the same catalog.
    """
    global _SCHEMA
    if _SCHEMA is None:
        built: dict[str, dict[str, dict[str, str]]] = {}
        for feed in feeds().values():
            built.setdefault(feed.raw_namespace, {})[feed.name] = dict(
                schemas.table_columns(feed.raw_namespace, feed.name))
        for layer in ("prepared", "reporting"):
            for model in models_in(layer):
                built.setdefault(layer, {})[model] = dict(
                    schemas.table_columns(layer, model))
        _SCHEMA = built
    return _SCHEMA
