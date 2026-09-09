"""Column-level lineage, parsed out of the COMPILED dbt SQL.

WHY COMPILED AND NOT THE MODEL FILE. The models are Jinja: `clean_string()`,
`safe_cast()`, `dedupe_rank()`, `source_provenance()`. The template says
nothing about which columns a macro reads -- that is decided when dbt renders
it -- so the parseable artefact is `target/compiled/**/<model>.sql`.

WHY sqlglot AND NOT THE PARSER ALREADY IN THE IMAGE. `openlineage-sql` ships
with the provider and produces column lineage, but not usable lineage here,
measured: of 59 fields it emitted, 26 resolved to a real table and the rest
named the CTE the column came through. The prepared layer -- the one that does
the renaming and casting anybody wants to see -- produced ZERO, because those
models open with `select *` and no parser can expand a star without knowing the
table's columns. sqlglot resolves CTEs and takes a schema, and THE PLATFORM
ALREADY READS THAT SCHEMA (`schemas.py`).

EVERY COLUMN IS CLASSIFIED, AND THAT IS THE POINT OF THIS MODULE (R-LIN-8).
Reporting only the columns that trace makes a column's ABSENCE ambiguous: a
literal, an aggregate over rows, and a parser failure nobody noticed all look
identical -- like nothing. Those are three different facts and an auditor asks
which. So every column gets a `ColumnLineage` carrying its class:

    sourced          computed from >=1 upstream table column, which is named
    row_aggregate    an aggregate over ROWS, not columns: `count(*)`
    build_metadata   a property of the build: `current_timestamp()`
    literal          a constant the build injected: dbt_invocation_id, nessie_ref
    ingest_added     ingest's own column, sourceless by construction
    unresolved       THE DEFECT CLASS -- see below

Measured on the shipped project: 136 columns, 116 `sourced`, 20 sourceless, 0
`unresolved`.

`unresolved` IS A DEFECT AND IT DOES NOT FAIL A BUILD. It is reachable and
detectable -- sqlglot raises whenever the TABLE has a column the current SQL
does not produce -- and it means the export is describing something it could
not read. But it is reported, not enforced, for two reasons: this package runs
inside an OpenLineage extractor where an exception costs the DATASETS of
whatever was being extracted, so nothing here may raise; and an export is not
an authority, so a DESCRIPTION of the pipeline must never be able to stop the
pipeline. The condition is also legitimately transient -- compiled SQL on disk
is from the last build while the schema is read from `main`, so mid-change the
two disagree by construction. The seam is therefore CI: `python -m
reporting_platform.lineage --columns` exits non-zero on any unresolved column.

THE TRANSFORMATION IS REPORTED, not just the dependency. A rename shows as an
input field with a different name (`_cob_date` -> `cob_date`) and no
description; a computation carries the SQL that performs it. That is the
deepest non-trivial expression on the path from the output column to its
source, which is where the work actually happens -- the outermost is always a
passthrough from the final CTE -- and it is also what the classifier reads, so
the two can never describe different nodes.

COST. 0.02s to parse a model and 0.17s to trace all its columns, once per task
process, on top of the schema read `schemas.py` already does.

TOTAL, like everything else in this package: an exception here costs the
datasets of whatever was being extracted, so every failure returns no column
lineage and none of them raise.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from reporting_platform.common.context import feeds, models_in
from reporting_platform.lineage import graph, schemas

# (dataset, field) an output column was computed from.
Source = tuple[tuple[str, str], str]

# The classes. Strings rather than an Enum because they are emitted straight
# into an OpenLineage facet and read back out of Marquez as JSON.
SOURCED = "sourced"
ROW_AGGREGATE = "row_aggregate"
BUILD_METADATA = "build_metadata"
LITERAL = "literal"
# NOT "platform_column". Facet VALUES are redacted through Airflow's
# SecretsMasker on the way out, and this deployment's Postgres password is the
# word `platform` -- so that name reached Marquez as `***_column`. Found by
# reading the emitted facet back rather than by reasoning about it. Any value
# this package emits must be checked against the masker, not just chosen.
INGEST_ADDED = "ingest_added"
UNRESOLVED = "unresolved"

_SCHEMA: dict[str, dict[str, dict[str, str]]] | None = None


@dataclass(frozen=True)
class ColumnLineage:
    """What one output column is, where it came from, and how it got there.

    `sources` is empty for every class but `sourced`, and that emptiness is
    now a STATEMENT rather than an absence -- the class says which kind of
    emptiness it is. `detail` is populated only for `unresolved`, where the
    reason is the whole of the information.
    """

    classification: str
    sources: list[Source] = field(default_factory=list)
    transformation: str = ""
    detail: str = ""

    @property
    def is_defect(self) -> bool:
        return self.classification == UNRESOLVED


def model_columns(layer: str, model: str) -> dict[str, ColumnLineage]:
    """{output column: ColumnLineage} for every column of the built table.

    Empty when the compiled SQL is absent -- which is the normal state before
    the model's first build, and after a `dbt clean` -- and empty when the
    table is not published, because there is then no column list to classify
    against. Empty means "nothing is known", which is a different claim from
    "every column is unresolved" and must not be confused with it.
    """
    try:
        sql = _compiled_sql(layer, model)
        if not sql:
            return {}
        return classify_sql(sql, [c for c, _ in schemas.table_columns(layer, model)],
                            _schema())
    except Exception:                                        # noqa: BLE001
        return {}


def ingest_columns(feed_name: str) -> dict[str, ColumnLineage]:
    """Raw's columns, mapped back to the names the FILE used.

    Not parsed from anything: ingest performs a declared rename rather than a
    query, so the mapping IS `Feed.source_column()`.

    THE PLATFORM'S OWN COLUMNS ARE CLASSIFIED, NOT OMITTED (R-LIN-8). Raw
    carries `_cob_date`, `_ingest_ts`, `_source_file`, the provenance
    four and the rest; they come from no file column, and dropping them made
    them indistinguishable from a column whose mapping had gone missing. They
    are `ingest_added`: sourceless by construction.

    WHICH IS WHICH IS DERIVED, NOT LISTED. A raw table is the feed's declared
    columns plus the platform's own, so a column the feed does not declare is
    the platform's -- and re-listing ingest's DDL here would be the second
    list this repo keeps refusing. The `_` prefix is the tiebreak, and it is
    only ever consulted for a column the feed did NOT declare: a raw column
    that is neither declared nor underscore-prefixed is drift between the
    table and `feeds.yml`, which is precisely the thing `unresolved` exists to
    make visible.
    """
    try:
        feed = feeds()[feed_name]
        landing = graph.landing(feed_name)
        declared = list(feed.columns)
        # The table when it can be read, the contract when it cannot -- these
        # tests take no catalog, and a feed's declared columns are knowable
        # without one.
        names = [c for c, _ in schemas.table_columns(feed.raw_namespace,
                                                     feed.name)] or declared
        out: dict[str, ColumnLineage] = {}
        for column in names:
            if column in declared:
                out[column] = ColumnLineage(
                    SOURCED, [(landing, feed.source_column(column))])
            elif column.startswith("_"):
                out[column] = ColumnLineage(INGEST_ADDED)
            else:
                out[column] = ColumnLineage(
                    UNRESOLVED,
                    detail=f"{column} is in raw but not declared by the feed")
        return out
    except Exception:                                        # noqa: BLE001
        return {}


def unresolved_columns(traced: dict[str, ColumnLineage]) -> list[str]:
    """The defect list for one table, for the CLI and the tests."""
    return sorted(c for c, lineage in traced.items() if lineage.is_defect)


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


def classify_sql(sql: str, columns: list[str],
                 schema: dict[str, dict[str, dict[str, str]]],
                 ) -> dict[str, ColumnLineage]:
    """Classify every one of `columns` against `sql`. Pure -- no I/O.

    Separated from `model_columns` so the classifier can be driven from a test
    with a hand-written query and schema: the catalog and the compiled SQL are
    the two things a config-level test run does not have, and a classifier
    that could only be exercised through them would be a classifier whose
    `unresolved` branch was asserted to be empty without ever being reached.
    """
    import sqlglot
    from sqlglot import exp
    from sqlglot.lineage import lineage

    tree = sqlglot.parse_one(sql, dialect="spark")
    out: dict[str, ColumnLineage] = {}
    for column in columns:
        try:
            node = lineage(column, tree, schema=schema, dialect="spark")
        except Exception as err:                             # noqa: BLE001
            # sqlglot raises for a column the query does not produce, which
            # happens whenever the TABLE has a column the current SQL does not
            # -- added by a later model version, or dropped from it. It used to
            # be skipped, which reported it as though it did not exist.
            out[column] = ColumnLineage(
                UNRESOLVED, detail=" ".join(str(err).split())[:200])
            continue
        out[column] = _classify(node, exp)
    return out


def _classify(node, exp) -> ColumnLineage:
    """One column's class, from its sources and its deepest expression.

    A column with sources is `sourced` and needs no further reading. A column
    WITHOUT them is identified by the node type of the expression that
    computes it, which is the same deepest-expression walk that produces the
    transformation description -- so the classification and the SQL shown for
    it can never describe two different nodes.
    """
    sources = _sources(node, exp)
    deepest = _deepest(node, exp)
    transformation = (" ".join(deepest.sql(dialect="spark").split())
                      if deepest is not None else "")
    if sources:
        return ColumnLineage(SOURCED, sources, transformation)
    if deepest is None:
        return ColumnLineage(
            UNRESOLVED, transformation=transformation,
            detail="no source column and no expression computing it")
    if isinstance(deepest, exp.AggFunc) and isinstance(deepest.this, exp.Star):
        # `count(*)` counts ROWS. It reads no column, so it has no input
        # field, and inventing one would be a fabricated edge.
        return ColumnLineage(ROW_AGGREGATE, transformation=transformation)
    if isinstance(deepest, (exp.CurrentTimestamp, exp.CurrentDate,
                            exp.CurrentDatetime, exp.CurrentUser)):
        return ColumnLineage(BUILD_METADATA, transformation=transformation)
    if _is_literal(deepest, exp):
        return ColumnLineage(LITERAL, transformation=transformation)
    return ColumnLineage(
        UNRESOLVED, transformation=transformation,
        detail=f"sourceless {type(deepest).__name__} expression")


def _is_literal(expression, exp) -> bool:
    """A constant, however many casts dbt wrapped it in.

    `dbt_invocation_id` and `nessie_ref` arrive as `CAST('...' AS STRING)`,
    so the literal is one level down; unwrapping the casts rather than
    matching the exact shape keeps this true if a macro adds another.
    """
    while isinstance(expression, (exp.Cast, exp.TryCast)):
        expression = expression.this
    return isinstance(expression, (exp.Literal, exp.Null))


def _sources(node, exp) -> list[Source]:
    found: list[Source] = []
    for item in node.walk():
        if not isinstance(item.source, exp.Table):
            continue
        table = item.source
        dataset = graph.table(table.db, table.name) if table.db else None
        # `item.name` is `<alias>.<column>`; the column is what OpenLineage
        # wants, and the alias is the CTE or table alias it arrived through.
        field_name = item.name.rsplit(".", 1)[-1]
        if dataset and (dataset, field_name) not in found:
            found.append((dataset, field_name))
    return found


def _deepest(node, exp):
    """The deepest expression that actually computes something.

    Walking outermost-first, each layer is usually `typed.notional AS
    notional` -- a passthrough between CTEs. The last non-passthrough before
    the table is the cast, the trim or the case expression, which is the one
    worth showing and the one whose node type says what kind of column this
    is. Returns the EXPRESSION rather than its SQL, because the classifier
    needs the type and the description needs the text.
    """
    deepest = None
    for item in node.walk():
        if isinstance(item.source, exp.Table):
            continue
        expression = item.expression
        if not isinstance(expression, exp.Expression):
            continue
        inner = expression.this if isinstance(expression, exp.Alias) else expression
        if isinstance(inner, (exp.Column, exp.Star)):
            continue
        deepest = inner
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
