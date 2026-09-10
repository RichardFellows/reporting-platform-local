"""Undo everything the spike leaves behind. Idempotent.

A spike that writes to the platform's own catalog has to be able to take it
back out, or the next person reads `wap_poc` in `information_schema` and has
to work out whether anything depends on it. Removes:

  * every `etl_*` / `probe_*` Nessie branch the scripts create,
  * the `wap_poc` namespace and its tables on `main`,
  * the fixture parquet under `s3://lakehouse/spike/duckdb-wap/`.

It does NOT remove the table's data files from the warehouse. Dropping the
table makes them orphans, and reclaiming an orphan is
`retention/orphan_storage.py`'s job -- doing it here would be a second
implementation of the thing this estate keeps only one of.

    docker compose cp spike/duckdb-wap/cleanup.py airflow:/tmp/
    docker compose exec -T airflow python /tmp/cleanup.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PROXY = "http://localhost:18998/iceberg"
DIRECT = "http://nessie:19120/iceberg"
PREFIXES = ("etl_", "probe_")


def main() -> int:
    import duckdb
    from run import Nessie

    nessie = Nessie()
    for ref in nessie._req("GET", "/trees").get("references", []):
        name = ref["name"]
        if name != "main" and name.startswith(PREFIXES):
            try:
                nessie.drop(name)
                print(f"dropped branch {name}")
            except Exception as exc:                        # noqa: BLE001
                print(f"branch {name}: {str(exc)[:120]}")

    # `main` is reachable without the proxy -- the default branch is the one
    # case DuckDB can address on its own.
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute("CREATE SECRET (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT 'minio:9000', "
                "URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')",
                [os.environ.get("AWS_ACCESS_KEY_ID", ""),
                 os.environ.get("AWS_SECRET_ACCESS_KEY", "")])
    con.execute(f"ATTACH 'warehouse' AS m (TYPE ICEBERG, ENDPOINT '{DIRECT}', "
                f"AUTHORIZATION_TYPE 'none')")
    for table in ("trades", "other"):
        try:
            con.execute(f"DROP TABLE IF EXISTS m.wap_poc.{table}")
            print(f"dropped table wap_poc.{table}")
        except Exception as exc:                            # noqa: BLE001
            print(f"table {table}: {str(exc)[:120]}")
    try:
        con.execute("DROP SCHEMA IF EXISTS m.wap_poc")
        print("dropped schema wap_poc")
    except Exception as exc:                                # noqa: BLE001
        print(f"schema: {str(exc)[:120]}")

    import boto3
    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT",
                                                        "http://minio:9000"),
                      aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                      region_name=os.environ.get("AWS_REGION", "us-east-1"))
    removed = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket="lakehouse", Prefix="spike/duckdb-wap/"):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket="lakehouse", Key=obj["Key"])
            removed += 1
    print(f"removed {removed} fixture object(s)")
    print("\nleft alone: the dropped tables' data files, now orphans -- "
          "reclaiming those is retention/orphan_storage.py's job.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
