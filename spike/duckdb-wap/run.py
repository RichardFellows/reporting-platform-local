"""Run `poc.sql` against the live stack, phase by phase.

WHY A RUNNER AND NOT `duckdb -f poc.sql`. There is no duckdb BINARY in this
estate -- the platform uses the Python module (`scripts/duckdb_console.py`),
and putting a CLI in a 2.7GB image shared by six services to run one spike is
not the trade. So this reads the same file a human would paste, statement by
statement, and prints what each phase answered.

It also performs the `-- @nessie` directives, which are the one thing DuckDB
cannot do: branch, merge and drop are the CATALOG's operations. That split is
not an artefact of the runner -- it is the shape any orchestration of this
would have, and worth seeing in the file.

    docker compose exec -T airflow python - < spike/duckdb-wap/run.py

`nessie_ref_proxy.py` must already be running in the same container:

    docker compose cp spike/duckdb-wap/nessie_ref_proxy.py airflow:/tmp/
    docker compose exec -d airflow python /tmp/nessie_ref_proxy.py
"""
from __future__ import annotations

import os
import re
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


def load_sql() -> str:
    """The .sql beside this file, or POC_SQL_TEXT for a piped run."""
    text = os.environ.get("POC_SQL_TEXT")
    if text:
        return text
    for candidate in (os.path.join(HERE, "poc.sql"),
                      "/opt/platform/spike/duckdb-wap/poc.sql",
                      "/tmp/poc.sql"):
        if os.path.exists(candidate):
            return open(candidate).read()
    raise SystemExit("poc.sql not found; set POC_SQL_TEXT or copy it to /tmp")


def statements(sql: str):
    """Yield (directives, statement) in file order.

    Directives are the `-- @...` comment lines that PRECEDE a statement, plus
    the standalone ones (`@nessie`, `@phase`) that have no statement of their
    own. Split on `;` at end of line, which is all this file needs -- there is
    no string literal in it containing one.
    """
    pending: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- @"):
            pending.append(stripped[4:])
            continue
        if stripped.startswith("--") or not stripped:
            continue
        buf.append(line)
        if stripped.endswith(";"):
            yield pending, "\n".join(buf).rstrip().rstrip(";")
            pending, buf = [], []
    if pending:
        yield pending, ""


class Nessie:
    """Only the three calls this needs, so the spike does not depend on the
    platform package -- it is a spike, and should still run if `context.py`
    moves under it again."""

    def __init__(self, uri=None):
        self.uri = (uri or os.environ.get("NESSIE_URI",
                                          "http://nessie:19120/api/v2")).rstrip("/")

    def _req(self, method, path, body=None):
        import json
        import urllib.error
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.uri + path, data=data, method=method)
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as r:
                raw = r.read()
        except urllib.error.HTTPError as exc:
            # THE BODY IS THE MESSAGE. Nessie puts status/reason/message/
            # errorCode in it, and a bare "HTTP Error 409: Conflict" hides the
            # one thing a merge conflict has to say: which key conflicted.
            detail = exc.read().decode(errors="replace")
            raise RuntimeError(f"{exc.code} {exc.reason}: {detail}") from None
        return json.loads(raw) if raw else {}

    def ref(self, name):
        return self._req("GET", f"/trees/{name}")["reference"]

    def branch(self, name, frm="main"):
        try:
            return self.ref(name)
        except Exception:
            pass
        src = self.ref(frm)
        return self._req("POST", f"/trees?name={name}&type=BRANCH",
                         {"type": src["type"], "name": src["name"],
                          "hash": src["hash"]})

    def merge(self, name, into="main"):
        src, tgt = self.ref(name), self.ref(into)
        return self._req("POST", f"/trees/{into}@{tgt['hash']}/history/merge",
                         {"fromRefName": name, "fromHash": src["hash"]})

    def drop(self, name):
        r = self.ref(name)
        return self._req("DELETE", f"/trees/{name}@{r['hash']}?type={r['type']}")


def main() -> int:
    import duckdb

    sql = load_sql()
    sql = re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), sql)
    con = duckdb.connect()
    nessie = Nessie()
    failed_gate = False

    for directives, stmt in statements(sql):
        for d in directives:
            if d.startswith("phase "):
                print(f"\n=== {d[6:]}")
            elif d.startswith("nessie "):
                _, op, name = d.split()
                if op == "merge" and failed_gate:
                    # The gate is the point. A runner that merges anyway would
                    # be demonstrating the happy path with extra steps.
                    print(f"    nessie: REFUSING to merge {name} -- the last "
                          f"audit failed")
                    return 1
                try:
                    getattr(nessie, {"branch": "branch", "merge": "merge",
                                     "drop": "drop"}[op])(name)
                    print(f"    nessie: {op} {name} -> ok")
                except Exception as exc:               # noqa: BLE001
                    print(f"    nessie: {op} {name} -> {str(exc)[:200]}")
                    return 1
        if not stmt:
            continue
        show = [d[5:] for d in directives if d.startswith("show ")]
        gate = any(d == "gate" for d in directives)
        try:
            if stmt.lstrip().upper().startswith("SELECT"):
                cols = [c[0] for c in con.execute(stmt).description]
                row = con.fetchone()
                answer = ", ".join(f"{c}={v}" for c, v in zip(cols, row))
                label = " ".join(show) or "result"
                print(f"    {label}: {answer}")
                if gate:
                    failed_gate = any(v for v in row)
                    print(f"    GATE: {'FAIL' if failed_gate else 'PASS'}"
                          f" -> {'discard' if failed_gate else 'publish'}")
            else:
                con.execute(stmt)
        except Exception as exc:                        # noqa: BLE001
            first = " ".join(stmt.split())[:80]
            print(f"    FAILED: {first}\n      {str(exc)[:300]}")
            return 1
    print("\nall phases ran.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
