"""The columns of each dataset in the graph, read from the thing itself.

WHY THIS IS NOT DERIVED FROM THE dbt PROJECT, when the edges are. The dbt
schema YAML documents the columns somebody wrote a TEST or a description for:
`_sources.yml` names two columns of `raw.fo_trade` and the table has twenty.
Emitting that as the schema would not be an incomplete answer, it would be a
WRONG one -- a reader seeing eight of twenty-four columns in Marquez has no
way to know the list is partial, and would reasonably conclude the other
sixteen do not exist. Empty is honest; partial is not. So the columns come
from the table.

READ THROUGH DuckDB, which is the platform's established no-Spark read path
(`scripts/duckdb_console.py`): `DESCRIBE` against the Iceberg REST catalog,
measured at 0.59s for all eleven tables plus 0.31s to attach. Spark here would
mean a JVM in the task process, which is the one thing
docs/DECISIONS.md#spark-in-a-subprocess forbids.

IT IS THE PUBLISHED SCHEMA, AND THAT IS THE RIGHT ONE. DuckDB can only address
the catalog's default branch, so what Marquez is told is the shape of the table
on `main` -- what a reader can actually query -- and not what the branch this
run is building might merge in a minute. A table that has never been published
has no schema here and gets none: it appears after the first run that merges
it, which is also when it becomes true. Marquez is a consumer; it reports what
exists.

A LANDING PREFIX IS NOT A TABLE, and its columns are the ones in the FILE --
`Feed.source_column()`, the upstream's own names, before ingest renames them.
So the graph shows the rename this platform performs: `Trade Id` at landing,
`trade_id` from raw onward. See CLAUDE.md on source column names.

TOTAL, AND CACHED PER PROCESS. Every path returns "no columns" rather than
raising: this runs inside an OpenLineage extractor, where the cost of an
exception is the DATASETS of whatever was being extracted, so a catalog that
is briefly unreachable must cost the schema and never the edge.
"""
from __future__ import annotations

from reporting_platform.common.context import feeds

# Per-process, and deliberately not invalidated. A task process is
# short-lived, and a schema that changed mid-task is not a thing worth
# reporting twice.
_TABLES: dict[str, list[tuple[str, str]]] | None = None


def table_columns(layer: str, name: str) -> list[tuple[str, str]]:
    """[(column, type)] for a published table, or [] if it cannot be read."""
    tables = _published()
    return tables.get(f"{layer}.{name}", [])


def landing_columns(feed_name: str) -> list[tuple[str, str]]:
    """[(column, type)] for a feed's delivered files.

    The names the FILE uses, not the platform's, and every one a string: a CSV
    has no types, and raw lands 1:1 as strings by design.

    `Feed.file_header` is that list already -- it is what schema drift is
    measured against and what the sample-data generator writes -- so this
    renames nothing itself.
    """
    try:
        feed = feeds()[feed_name]
    except Exception:                                        # noqa: BLE001
        return []
    return [(column, "string") for column in feed.file_header]


def columns_for(dataset: tuple[str, str]) -> list[tuple[str, str]]:
    """[(column, type)] for any dataset `graph.py` can name.

    Dispatches on the namespace the graph itself built, rather than on the
    shape of the name: an Iceberg table is `<layer>.<table>` in the catalog
    namespace, and everything else is a landing prefix, identified by matching
    the dataset a feed produces rather than by parsing the key apart.
    """
    from reporting_platform.lineage import graph

    namespace, name = dataset
    if namespace == graph.CATALOG_NAMESPACE:
        layer, _, table = name.partition(".")
        return table_columns(layer, table) if table else []
    try:
        feed_name = {graph.landing(f): f for f in feeds()}.get(dataset)
    except Exception:                                        # noqa: BLE001
        return []
    return landing_columns(feed_name) if feed_name else []


def _published() -> dict[str, list[tuple[str, str]]]:
    """Every published table's columns, described once per process."""
    global _TABLES
    if _TABLES is None:
        try:
            _TABLES = _describe_all()
        except Exception:                                    # noqa: BLE001
            # Cached as empty on purpose: if the catalog cannot be reached,
            # every later call in this process would fail the same way, and
            # retrying it per dataset would add the connection timeout to
            # every task rather than to one.
            _TABLES = {}
    return _TABLES


def _describe_all() -> dict[str, list[tuple[str, str]]]:
    from scripts.duckdb_console import ALIAS, connect

    con = connect()
    try:
        tables = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_catalog = ? ORDER BY 1, 2", [ALIAS]).fetchall()
        out = {}
        for schema, table in tables:
            # DESCRIBE, not information_schema.columns: the Iceberg attach
            # answers the latter with a single placeholder column per table.
            rows = con.execute(f"DESCRIBE {ALIAS}.{schema}.{table}").fetchall()
            out[f"{schema}.{table}"] = [(r[0], r[1]) for r in rows]
        return out
    finally:
        con.close()
