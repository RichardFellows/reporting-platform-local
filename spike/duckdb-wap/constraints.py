"""The probes behind README.md's findings, so every claim in it is re-runnable.

`poc.sql` demonstrates the happy path and the failure path. This asks the
questions whose ANSWERS are the point of a spike -- what breaks, how it
breaks, and whether it says so:

  1. the branch attach WITHOUT the proxy (the seam itself)
  2. whether DuckDB exposes any prefix override, so the proxy is really needed
  3. UPDATE and DELETE, unpartitioned and partitioned
  4. two branches merging into a moving `main` -- the Airflow scenario
  5. the stale-metadata trap the spec predicted

Each prints what happened rather than asserting: a spike's output is evidence,
and a passing test would hide the error text that is the actual finding.

    docker compose cp spike/duckdb-wap/constraints.py airflow:/tmp/
    docker compose exec -T airflow python /tmp/constraints.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

NESSIE = "http://nessie:19120/iceberg"          # straight at the server
PROXY = "http://localhost:18998/iceberg"        # through nessie_ref_proxy.py


def _nessie():
    from run import Nessie
    return Nessie()


def attach(alias: str, endpoint: str, extra: str = ""):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute("CREATE SECRET (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT 'minio:9000', "
                "URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')",
                [os.environ.get("AWS_ACCESS_KEY_ID", ""),
                 os.environ.get("AWS_SECRET_ACCESS_KEY", "")])
    con.execute(f"ATTACH 'warehouse' AS {alias} (TYPE ICEBERG, ENDPOINT '{endpoint}', "
                f"AUTHORIZATION_TYPE 'none'{extra})")
    return con


def tables(con, alias):
    return [r[0] for r in con.execute(
        "SELECT table_schema||'.'||table_name FROM information_schema.tables "
        f"WHERE table_catalog='{alias}' ORDER BY 1").fetchall()]


def probe_1_the_seam(n) -> None:
    print("\n=== 1. The branch attach, straight at Nessie and through the proxy")
    n.branch("probe_seam")
    for label, endpoint in (("direct ", f"{NESSIE}/probe_seam"),
                            ("proxied", f"{PROXY}/probe_seam")):
        try:
            con = attach("b", endpoint)
            found = tables(con, "b")
            print(f"  {label}: ATTACH ok, tables={found or '[]  <- SILENTLY EMPTY'}")
            con.close()
        except Exception as exc:                            # noqa: BLE001
            print(f"  {label}: {str(exc)[:160]}")
    # And the URL that makes it empty, asked for directly.
    import urllib.error
    import urllib.request
    for url in (f"{NESSIE}/probe_seam/v1/config?warehouse=warehouse",
                f"{NESSIE}/probe_seam/v1/probe_seam%7Cwarehouse/namespaces",
                f"{NESSIE}/v1/probe_seam%7Cwarehouse/namespaces"):
        try:
            with urllib.request.urlopen(url) as r:
                body = r.read()[:90].decode(errors="replace").replace("\n", " ")
            print(f"  GET {url.split('/iceberg')[1]:52s} -> {r.status} {body}")
        except urllib.error.HTTPError as exc:
            print(f"  GET {url.split('/iceberg')[1]:52s} -> {exc.code}")
    n.drop("probe_seam")


def probe_2_overrides() -> None:
    print("\n=== 2. Does ATTACH expose a prefix override, or is the proxy required?")
    import duckdb
    for opt in ("PREFIX 'x|warehouse'", "WAREHOUSE 'x|warehouse'",
                "MAX_TABLE_STALENESS '0s'", "ACCESS_DELEGATION_MODE 'NONE'"):
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
        try:
            con.execute(f"ATTACH 'warehouse' AS c (TYPE ICEBERG, ENDPOINT '{NESSIE}', "
                        f"AUTHORIZATION_TYPE 'none', {opt})")
            print(f"  {opt:32s} accepted")
        except Exception as exc:                            # noqa: BLE001
            print(f"  {opt:32s} {str(exc)[:90]}")
        con.close()


def probe_3_mutations(n) -> None:
    print("\n=== 3. UPDATE and DELETE, unpartitioned then partitioned")
    n.branch("probe_mutate")
    con = attach("b", f"{PROXY}/probe_mutate")
    print("  -- wap_poc.trades (unpartitioned)")
    for label, sql in (("UPDATE", "UPDATE b.wap_poc.trades SET book='X' WHERE trade_id=20"),
                       ("DELETE", "DELETE FROM b.wap_poc.trades WHERE trade_id=21")):
        try:
            con.execute(sql)
            print(f"    {label}: ok")
        except Exception as exc:                            # noqa: BLE001
            print(f"    {label}: {str(exc)[:150]}")
    con.close()
    print("  -- raw.fo_trade (partitioned by _cob_date, a real platform table)")
    print("     `override` is SET ignore_target_file_size_for_partitioned_tables")
    # A CONNECTION EACH, and the liveness check afterwards, because one of
    # these does not merely fail: it invalidates the DATABASE, and every later
    # statement on that connection reports the previous fatal error instead of
    # its own result. Sharing one connection hides which statement did it.
    insert = "INSERT INTO b.raw.fo_trade SELECT * FROM b.raw.fo_trade LIMIT 1"
    update = "UPDATE b.raw.fo_trade SET _source_system='X' WHERE false"
    delete = "DELETE FROM b.raw.fo_trade WHERE false"
    for label, override, sql in (("INSERT", False, insert), ("INSERT", True, insert),
                                 ("UPDATE", False, update), ("UPDATE", True, update),
                                 ("DELETE", False, delete), ("DELETE", True, delete)):
        c = attach("b", f"{PROXY}/probe_mutate")
        if override:
            c.execute("SET ignore_target_file_size_for_partitioned_tables=true")
        tag = f"{label}, override {'ON ' if override else 'OFF'}"
        try:
            c.execute(sql)
            print(f"    {tag}: ok")
        except Exception as exc:                            # noqa: BLE001
            print(f"    {tag}: {str(exc).splitlines()[0][:130]}")
        try:
            c.execute("SELECT 1").fetchone()
        except Exception as exc:                            # noqa: BLE001
            print(f"    {'':22s}  CONNECTION DEAD -- {str(exc).splitlines()[0][:80]}")
        try:
            c.close()
        except Exception:                                   # noqa: BLE001
            pass
    n.drop("probe_mutate")


def probe_4_concurrency(n) -> None:
    print("\n=== 4. Two branches, one main -- the Airflow scenario")
    base = attach("m", PROXY)
    base.execute("CREATE TABLE IF NOT EXISTS m.wap_poc.other (id BIGINT)")
    base.close()
    for name in ("probe_x", "probe_y", "probe_z"):
        n.branch(name)
    print("  three branches cut from the same main hash")
    for name, sql in (("probe_x", "INSERT INTO b.wap_poc.trades VALUES (901,'X',1.00)"),
                      ("probe_y", "INSERT INTO b.wap_poc.trades VALUES (902,'Y',2.00)"),
                      ("probe_z", "INSERT INTO b.wap_poc.other VALUES (1)")):
        con = attach("b", f"{PROXY}/{name}")
        con.execute(sql)
        con.close()
    for name, note in (("probe_x", "first in"),
                       ("probe_y", "same table as probe_x"),
                       ("probe_z", "a different table")):
        try:
            n.merge(name)
            print(f"  merge {name} ({note}): ok")
        except Exception as exc:                            # noqa: BLE001
            msg = " ".join(str(exc).split())
            print(f"  merge {name} ({note}): {msg[:150]}")
    for name in ("probe_x", "probe_y", "probe_z"):
        try:
            n.drop(name)
        except Exception:                                   # noqa: BLE001
            pass


def probe_5_staleness(n) -> None:
    print("\n=== 5. The stale-metadata trap after a merge")
    n.branch("probe_stale")
    before = attach("m", PROXY)                     # attached BEFORE the merge
    start = before.execute("select count(*) from m.wap_poc.trades").fetchone()[0]
    con = attach("b", f"{PROXY}/probe_stale")
    con.execute("INSERT INTO b.wap_poc.trades VALUES (999,'STALE',9.00)")
    con.close()
    n.merge("probe_stale")
    same = before.execute("select count(*) from m.wap_poc.trades").fetchone()[0]
    fresh = attach("m", PROXY).execute("select count(*) from m.wap_poc.trades").fetchone()[0]
    print(f"  rows before merge={start}  same connection after={same}  fresh connection={fresh}")
    print("  -> " + ("STALE: the open connection did not see the merge"
                     if same != fresh else
                     "no staleness observed: the open connection saw the merge"))
    n.drop("probe_stale")


def main() -> int:
    n = _nessie()
    probe_1_the_seam(n)
    probe_2_overrides()
    probe_3_mutations(n)
    probe_4_concurrency(n)
    probe_5_staleness(n)
    print("\nprobes complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
