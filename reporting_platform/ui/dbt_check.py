"""Ask dbt whether it can actually read the project.

WHAT THIS CLOSES. `scaffold.py` renders the prepared model as text and writes
it; nothing between that and `prepared_build` running, minutes later, ever
hands the file to dbt. So four green `written` steps meant "four files exist",
not "dbt accepts them" -- a bad `ref()`, a Jinja typo, or schema YAML dbt
rejects would all sit there looking finished until the first build failed.

`dbt parse` is the cheap half of that answer: it builds the manifest, which
means it resolves every `ref()` and `source()`, renders every model's Jinja,
and validates the schema YAML -- in about ten seconds, with no Spark session,
no cluster and no warehouse connection.

WHAT IT STILL DOES NOT PROVE. Parsing is structural. It does not compile SQL
against the catalog, so a column that does not exist in raw, a type that will
not cast, or a test that will fail on real data are all invisible here and
show up in `prepared_build`. This narrows the gap; it does not close it, and
the UI says as much rather than presenting a parse as a build.

The parse writes to ITS OWN target directory. dbt keeps the manifest and the
partial-parse cache under `target/`, and the Airflow builds use the same
project dir -- a console parse sharing that directory could interleave with a
running build's artifacts. A separate `--target-path` keeps the two apart and
still lets the console's own partial-parse cache make repeat runs fast.
"""
from __future__ import annotations

import os
import re
import subprocess
import time

DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "spark_local")
TARGET_PATH = os.environ.get("FEED_UI_DBT_TARGET_PATH", "/tmp/dbt-console-target")
TIMEOUT_SECONDS = 240

# dbt prefixes its real problems with one of these. Everything else in the
# output is progress logging and the standing project warnings, which are not
# this feature's business to report.
_PROBLEM = re.compile(
    r"(Compilation Error|Parsing Error|Database Error|Runtime Error|"
    r"Validation Error|Invalid .* Error|Encountered an error)", re.I)


def parse() -> dict:
    """Run `dbt parse`. Returns a result the UI can show verbatim.

    Never raises for a dbt failure -- a project that does not parse is a
    normal outcome here and the message is the whole point of asking.
    """
    args = ["dbt", "parse",
            "--project-dir", DBT_DIR,
            "--profiles-dir", DBT_DIR,
            "--target", DBT_TARGET,
            "--target-path", TARGET_PATH]
    started = time.monotonic()
    try:
        proc = subprocess.run(args, capture_output=True, text=True,
                              timeout=TIMEOUT_SECONDS)
    except FileNotFoundError:
        return {"ok": False, "seconds": 0.0, "ran": False,
                "summary": "dbt is not installed in this container",
                "detail": "The console image is built from Dockerfile.airflow, "
                          "which installs dbt-core. Rebuild it."}
    except subprocess.TimeoutExpired:
        return {"ok": False, "seconds": float(TIMEOUT_SECONDS), "ran": False,
                "summary": f"dbt parse did not finish within {TIMEOUT_SECONDS}s",
                "detail": ""}

    seconds = round(time.monotonic() - started, 1)
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode == 0:
        return {"ok": True, "ran": True, "seconds": seconds,
                "summary": "dbt parses the project — every ref(), source() and "
                           "schema entry resolves",
                "detail": ""}
    return {"ok": False, "ran": True, "seconds": seconds,
            "summary": f"dbt parse failed (exit {proc.returncode})",
            "detail": _problem_text(output)}


def _problem_text(output: str, limit: int = 2500) -> str:
    """The part of dbt's output that says what is wrong.

    dbt puts the useful lines in the middle of its log, not at the end, so a
    plain tail can cut the error message off and leave only the summary count
    -- the same trap `_spark_subprocess` in feed_ingest.py documents from the
    other direction. Anchor on the first problem line instead, and fall back
    to the tail only when nothing matches.
    """
    lines = output.splitlines()
    for i, line in enumerate(lines):
        if _PROBLEM.search(line):
            return "\n".join(lines[i:])[:limit]
    return "\n".join(lines[-25:])[:limit]
