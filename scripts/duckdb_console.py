"""Read-only DuckDB session against the published lakehouse.

WHAT THIS IS FOR. Analyst and developer queries against `main` -- "what does
this table actually contain", "do these numbers reconcile" -- without starting
a SparkSession. DuckDB answers those in about a second where Spark takes tens,
and it reads the same Iceberg tables through the same catalog.

WHAT IT IS NOT FOR, and why this is a script rather than a dbt target. It
cannot build anything, deliberately three times over:

  * DuckDB cannot address a NON-DEFAULT BRANCH unaided, and fails at it
    silently. The Nessie ref travels in the Iceberg REST request prefix;
    DuckDB takes that prefix from /v1/config and then appends it to THE
    ENDPOINT IT WAS GIVEN, so putting the ref in the endpoint path puts it in
    the URL twice (`/iceberg/etl_x/v1/etl_x%7Cwarehouse/...`), which 404s --
    and the 404 renders as an EMPTY CATALOG, not an error. ATTACH succeeds and
    every table is missing. There is no PREFIX or WAREHOUSE option to override
    it. A rewriting proxy in front of Nessie does bridge it; whether that is a
    bridge worth owning is `spike/duckdb-wap/` (measured, with the request
    trace). No branch means no write-audit-publish, so a DuckDB build here
    would write straight to `main`.
  * dbt-duckdb silently ignores `partition_by`, so anything it created would be
    unpartitioned -- and `cob_date` partitioning is what makes retention's
    expiry a metadata delete rather than a full rewrite.
  * DuckDB refuses INSERT and UPDATE on a partitioned table by DEFAULT. There
    is an override (`ignore_target_file_size_for_partitioned_tables`) and with
    it INSERT genuinely works -- but the same flag turns UPDATE's refusal into
    `INTERNAL Error: IcebergDelete multi_file_list is NULL`, which invalidates
    the whole database: every later statement on that connection returns a
    FATAL error. Note DELETE is allowed with or without the override: the
    destructive operation is the one needing no opt-in. Measured on DuckDB
    1.5.5 -- see `spike/duckdb-wap/README.md`.

The attach is therefore READ_ONLY, verified rather than assumed -- CREATE fails
with "Cannot execute statement of type CREATE ... attached in read-only mode".

It also speaks Iceberg REST at NESSIE_ICEBERG_URI, not the Nessie API at
NESSIE_URI. DuckDB's iceberg extension cannot speak the latter; whatever
endpoint it is handed, it appends /v1/config?warehouse= to it. Nessie serves
both -- one version store, two front doors.

    # a one-off query
    docker compose exec -T airflow python -m scripts.duckdb_console \
        "select cob_date, count(*) from lakehouse.prepared.fo_trade
         group by 1 order by 1 desc limit 5"

    # what is in there
    docker compose exec -T airflow python -m scripts.duckdb_console --tables

    # a query too long for a shell argument
    docker compose exec -T airflow python -m scripts.duckdb_console - < q.sql
"""
from __future__ import annotations

import argparse
import os
import sys

WAREHOUSE = os.environ.get("NESSIE_WAREHOUSE", "warehouse")
ALIAS = os.environ.get("REPORTING_CATALOG", "lakehouse")


def connect():
    """A read-only DuckDB connection with the lakehouse attached under REPORTING_CATALOG."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")

    # AUTHORIZATION_TYPE none because this Nessie is unauthenticated. Without
    # it the extension defaults to oauth2 and refuses to attach at all, with
    # "AUTHORIZATION_TYPE is 'oauth2', yet no 'secret' was provided".
    #
    # No S3 secret is created here. The catalog supplies the client what it
    # needs for the object store -- tested, and an earlier version of the dbt
    # profile wrongly claimed a secret was required for reads. Set
    # REPORTING_DUCKDB_S3_SECRET=1 where the catalog does not vend credentials.
    endpoint = os.environ.get("NESSIE_ICEBERG_URI",
                              "http://nessie:19120/iceberg")
    if os.environ.get("REPORTING_DUCKDB_S3_SECRET"):
        con.execute(
            "CREATE SECRET (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT ?, "
            "URL_STYLE 'path', USE_SSL false, REGION ?)",
            [os.environ.get("AWS_ACCESS_KEY_ID", ""),
             os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
             os.environ.get("S3_ENDPOINT_HOST", "minio:9000"),
             os.environ.get("AWS_REGION", "us-east-1")],
        )

    con.execute(
        f"ATTACH '{WAREHOUSE}' AS {ALIAS} "
        f"(TYPE ICEBERG, ENDPOINT '{endpoint}', AUTHORIZATION_TYPE 'none', "
        f"READ_ONLY)"
    )
    return con


def list_tables(con) -> None:
    rows = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = ? ORDER BY 1, 2", [ALIAS]).fetchall()
    width = max((len(s) for s, _ in rows), default=6)
    for schema, table in rows:
        print(f"{schema:<{width}}  {ALIAS}.{schema}.{table}")
    print(f"\n{len(rows)} table(s) on the default branch of {ALIAS}.")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Read-only DuckDB session against published `main`.")
    p.add_argument("sql", nargs="?",
                   help="SQL to run. '-' reads it from stdin. Omit for --tables.")
    p.add_argument("--tables", action="store_true",
                   help="list the tables visible on the default branch")
    a = p.parse_args(argv)

    con = connect()
    if a.tables or not a.sql:
        list_tables(con)
        return 0

    sql = sys.stdin.read() if a.sql == "-" else a.sql
    try:
        # .show() renders a table with column names and types, which is what
        # makes this usable interactively; fetchall() prints bare tuples.
        con.sql(sql).show(max_rows=200)
    except Exception as e:                                   # noqa: BLE001
        # A traceback is the wrong output for an analyst tool, and the
        # read-only refusal in particular is a correct answer rather than a
        # crash -- say what to do instead of dumping frames.
        if "read-only" in str(e):
            print("This console is read-only by design: builds run on Spark, "
                  "on a Nessie branch, through write-audit-publish. See the "
                  "module docstring for why DuckDB cannot do that here.",
                  file=sys.stderr)
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
