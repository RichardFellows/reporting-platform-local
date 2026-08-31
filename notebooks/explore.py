"""Explore the lakehouse: landing files and every Iceberg layer, in one place.

Run it with the stack up:

    docker compose up -d notebook      # http://localhost:8083

WHY THIS IS READ-ONLY, AND WHY THAT IS NOT A LIMITATION. It uses
`scripts/duckdb_console.connect()`, the same read-only DuckDB session the CLI
console uses -- imported rather than restated, so there is one definition of
how this platform is queried. Two consequences worth knowing before you
wonder why something is missing:

  * You are always looking at `main`. DuckDB takes the Nessie ref from the
    catalog's /v1/config response and its ATTACH exposes no override, so a
    `build/*` branch is invisible here. To inspect a failed build's branch you
    need Spark and the `nessie_ref` var -- see docs/ARCHITECTURE.md.
  * Nothing you type can write. The attach is READ_ONLY, so this cannot
    disturb a build in flight, which is what makes it safe to leave open.

Marimo is REACTIVE: change a cell and everything downstream of it re-runs.
That is why the connection lives in its own cell -- it is built once and every
query cell depends on it.
"""

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys

    sys.path.insert(0, "/opt/platform")
    return mo, sys


@app.cell
def _(mo):
    mo.md(
        """
        # Lakehouse explorer

        Landing CSVs and the `raw` / `prepared` / `reporting` Iceberg layers,
        through one read-only DuckDB connection.

        **You are looking at published `main`.** Build branches are not visible
        here — that is a property of the Iceberg REST catalog, not a setting.
        """
    )
    return


@app.cell
def _():
    # ONE connection, reused by every cell below. Imported from the CLI console
    # rather than rebuilt here: the catalog wiring (httpfs, iceberg extension,
    # AUTHORIZATION_TYPE none, the READ_ONLY attach) has enough traps in it
    # that a second copy would drift. See scripts/duckdb_console.py.
    from scripts.duckdb_console import connect, ALIAS

    con = connect()
    return ALIAS, con


@app.cell
def _(ALIAS, con, mo):
    # What is actually here, with row counts. Cheap enough to run on every
    # reload -- these are metadata reads plus a count per table.
    _tables = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = ? ORDER BY 1, 2",
        [ALIAS],
    ).fetchall()

    _rows = []
    for _schema, _name in _tables:
        _n = con.execute(f"SELECT count(*) FROM {ALIAS}.{_schema}.{_name}").fetchone()[0]
        _rows.append({"layer": _schema, "table": f"{_schema}.{_name}", "rows": _n})

    catalog = mo.ui.table(_rows, selection=None, label="Published tables on `main`")
    catalog
    return (catalog,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Scratch SQL

        Anything DuckDB understands. Tables are `lakehouse.<layer>.<table>`;
        landing files are `read_csv_auto('s3://lakehouse/landing/<feed>/*.csv')`.
        """
    )
    return


@app.cell
def _(mo):
    query = mo.ui.text_area(
        value=(
            "select business_date, count(*) as rows\n"
            "from lakehouse.reporting.exposure_change\n"
            "group by 1 order by 1 desc limit 10"
        ),
        label="SQL",
        rows=8,
        full_width=True,
    )
    query
    return (query,)


@app.cell
def _(con, mo, query):
    # A failed query must not blank the notebook -- marimo re-runs this cell on
    # every keystroke-committed edit, and a half-typed statement is the normal
    # case rather than an exceptional one.
    try:
        result = mo.ui.table(con.execute(query.value).df(), selection=None)
    except Exception as exc:
        result = mo.md(f"```\n{exc}\n```").callout(kind="warn")
    result
    return (result,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Landing files — what actually arrived

        The immutable evidence copy, read straight from object storage. This is
        the only layer that is not Iceberg, and the only one where you can see
        a delivery that was landed but deliberately **not** ingested (a date
        outside the retention keep-set).
        """
    )
    return


@app.cell
def _(con, mo):
    from reporting_platform.common.context import feeds

    _feed_names = sorted(feeds())
    feed_pick = mo.ui.dropdown(
        options=_feed_names, value=_feed_names[0], label="feed"
    )
    feed_pick
    return feed_pick, feeds


@app.cell
def _(con, feed_pick, mo):
    _uri = f"s3://lakehouse/landing/{feed_pick.value}/*.csv"
    try:
        _df = con.execute(
            f"select * from read_csv_auto('{_uri}', filename=true) limit 200"
        ).df()
        landing = mo.vstack(
            [mo.md(f"`{_uri}` — first 200 rows"), mo.ui.table(_df, selection=None)]
        )
    except Exception as exc:
        landing = mo.md(f"```\n{exc}\n```").callout(kind="warn")
    landing
    return (landing,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Joining across layers

        The example below is the one most likely to catch you out.
        `counterparty`, `rating` and `primary_limits` are **SCD2**: one row per
        version, not per business date, with `effective_from` / `effective_to`.

        An equality join against them does not fail and does not return
        nothing — it returns a **plausible-looking subset**. Joining
        `c.effective_from = t.business_date` on this data gives 463 rows where
        the correct join gives the lot: only the dates a version happened to
        start on. Numbers that look reasonable and are silently incomplete are
        worse than an error.

        Join point-in-time instead — the same thing `as_of()` expands to in
        the dbt models.
        """
    )
    return


@app.cell
def _(con, mo):
    _sql = """
    select
        t.business_date,
        c.legal_name,
        c.country_code,
        count(*)                as trades,
        round(sum(t.notional))  as notional
    from lakehouse.prepared.trade t
    join lakehouse.prepared.counterparty c
      on  c.counterparty_id = t.counterparty_id
      -- POINT-IN-TIME, not c.business_date = t.business_date. effective_to is
      -- DATE '9999-12-31' on the open version, so no null handling is needed.
      and t.business_date between c.effective_from and c.effective_to
    group by 1, 2, 3
    order by t.business_date desc, notional desc
    limit 15
    """
    scd2_example = mo.vstack(
        [
            mo.md(f"```sql{_sql}```"),
            mo.ui.table(con.execute(_sql).df(), selection=None),
        ]
    )
    scd2_example
    return (scd2_example,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Reconciling a layer against the one below it

        The question worth asking of any change: does `prepared` still account
        for what `raw` received? Raw is all strings and holds every delivered
        version; prepared keeps the latest `_file_version` per business date.
        A gap here is a dedupe or an incremental-window problem.
        """
    )
    return


@app.cell
def _(con, mo):
    _sql = """
    with raw_latest as (
        select _business_date as business_date, count(distinct trade_id) as ids
        from lakehouse.raw.trade
        group by 1
    ),
    prep as (
        select business_date, count(distinct trade_id) as ids
        from lakehouse.prepared.trade
        group by 1
    )
    select
        coalesce(r.business_date, p.business_date) as business_date,
        r.ids as raw_ids,
        p.ids as prepared_ids,
        coalesce(p.ids, 0) - coalesce(r.ids, 0) as difference
    from raw_latest r
    full outer join prep p on p.business_date = r.business_date
    where coalesce(p.ids, 0) <> coalesce(r.ids, 0)
    order by 1 desc
    """
    recon = mo.vstack(
        [
            mo.md("Business dates where raw and prepared disagree on trade count "
                  "— **empty is the healthy answer**:"),
            mo.ui.table(con.execute(_sql).df(), selection=None),
        ]
    )
    recon
    return (recon,)


if __name__ == "__main__":
    app.run()
