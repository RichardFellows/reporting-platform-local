"""The read-only DuckDB connection to the published lakehouse.

Moved out of `scripts/duckdb_console.py`, which is still the CLI and still
where WHY THIS IS READ-ONLY is written down -- read its header before
changing anything here. It lives in core because the lineage export
(`lineage/schemas.py`) describes published tables through it, and `scripts/`
ships in no component (docs/PACKAGING.md).
"""
from __future__ import annotations

import os

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
