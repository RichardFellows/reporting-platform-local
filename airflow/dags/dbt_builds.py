"""dbt builds for the prepared and reporting layers.

Triggering is asset-based, not cron-based. `prepared_build` fires as soon as
ANY raw asset updates — no feed waits for another feed. dbt's own selection
keeps the rebuild proportionate: only models downstream of the changed source
are run.

Both builds run on a Nessie branch and merge on success (write-audit-publish).
A failing test therefore leaves `main` untouched, and consumers never see a
half-built mart.
"""
from __future__ import annotations

import os
import re
from datetime import timedelta

import pendulum

from reporting_platform.common.context import feeds

try:                                    # Airflow 3
    from airflow.sdk import Asset, dag, task
except ImportError:                     # Airflow 2.x
    from airflow.datasets import Dataset as Asset  # type: ignore
    from airflow.decorators import dag, task       # type: ignore

CATALOG = os.environ.get("REPORTING_CATALOG", "lakehouse")
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")
# Falls back to spark_local, and the fallback matters more than it looks.
# It used to be duckdb_local, which was harmless only because duckdb_local was
# BROKEN: an unset DBT_TARGET crashed loudly. D7 fixed duckdb_local,
# which turned that loud failure into a silent one -- DuckDB can only address
# the catalog's default branch, so it ignores the `nessie_ref` var this DAG
# passes and writes straight to `main`. The run would go green and
# write-audit-publish would have been bypassed with nothing red anywhere.
DBT_TARGET = os.environ.get("DBT_TARGET", "spark_local")

RAW_ASSETS = [Asset(f.asset_uri) for f in feeds().values()]


def any_of(assets):
    """OR the assets together, so ANY one updating fires the consumer.

    A bare list is **AND** in Airflow: `schedule=[a, b, c]` waits until every
    one of them has a new event since the last run. That is the opposite of
    what this platform documents and needs -- `docs/ARCHITECTURE.md` says
    "triggered by ANY upstream asset", "No feed waits for any other feed to
    arrive", and "a feed that is late does not block the ones that arrived".
    With a list, one late feed silently holds up every build, which is exactly
    the batch window the per-feed design exists to remove.

    Verified against the live scheduler: with `schedule=[trade, cpty, rating]`,
    an ingest of trade alone emitted its dataset event and `prepared_build`
    never fired.

    `|` yields DatasetAny/AssetAny on Airflow 2.9+ and 3.x. If that is
    unavailable the list is returned unchanged and a warning logged, because
    degrading to AND silently is how this was missed in the first place.
    """
    import functools
    import logging
    import operator

    try:
        return functools.reduce(operator.or_, assets)
    except TypeError:
        logging.getLogger(__name__).warning(
            "this Airflow does not support OR-ing assets; prepared_build will "
            "wait for ALL raw feeds, not any. See docs/ARCHITECTURE.md.")
        return assets
PREPARED_ASSET = Asset(f"iceberg://{CATALOG}/prepared/all")
REPORTING_ASSET = Asset(f"iceberg://{CATALOG}/reporting/all")

DEFAULT_ARGS = {"owner": "data-platform", "retries": 1,
                "retry_delay": timedelta(minutes=5)}


def _dbt(command: str, select: str | None = None, branch: str | None = None) -> dict:
    """Invoke dbt and return a structured result.

    The Nessie branch is passed through as a dbt var; profiles.yml threads it
    into the catalog `ref` so the whole build lands on the branch. That thread
    is the whole of write-audit-publish, and it is why the target is checked
    below rather than trusted.
    """
    import json
    import subprocess

    # REFUSE A NON-SPARK TARGET, because the failure it prevents is silent.
    # `nessie_ref` below is passed to dbt as a var and honoured only by the
    # Spark profiles; an engine that cannot address a Nessie branch ignores it
    # and writes to the catalog's default branch instead. The build would then
    # SUCCEED, having written to `main` with no branch, no audit and nothing
    # red anywhere. Every target in profiles.yml is Spark today, so this can
    # only fire on a misconfigured DBT_TARGET -- which is exactly the
    # circumstance it exists for (a stale DBT_TARGET in this same
    # code path, and it was the DAG that read it).
    if not DBT_TARGET.startswith("spark"):
        raise RuntimeError(
            f"DBT_TARGET is {DBT_TARGET!r}, which is not a Spark target. "
            f"Builds must run on Spark: the branch this task opened is passed "
            f"as the `nessie_ref` var, and a non-Spark engine ignores it and "
            f"writes to the default branch, bypassing write-audit-publish "
            f"without failing.1'."
        )

    args = ["dbt", command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR,
            "--target", DBT_TARGET]
    if select:
        args += ["--select", select]
    if branch:
        args += ["--vars", json.dumps({"nessie_ref": branch})]

    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"dbt {command} failed (exit {proc.returncode})\n"
            f"--- stdout ---\n{proc.stdout[-4000:]}\n"
            f"--- stderr ---\n{proc.stderr[-2000:]}"
        )
    return {"command": command, "select": select, "target": DBT_TARGET,
            "tail": proc.stdout[-2000:]}


def build_dag(dag_id: str, schedule, select: str, outlet, purpose: str):

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["reporting-platform", "dbt", purpose],
    )
    def _dag():

        @task
        def open_branch(**context) -> str:
            from reporting_platform.common.context import Nessie

            # Slugify rather than slice. `run_id[-24:]` cut mid-token and
            # produced branch names like `-08-21T101555.6748970000` and tags
            # like `published/2026-08-20/l__2026-08-21T1002500000` -- the "l__"
            # being the tail of "dataset_triggered__". Unreadable, and it
            # discards exactly the part that identifies the run.
            raw = context["run_id"]
            run_id = re.sub(r"[^A-Za-z0-9]+", "-", raw).strip("-")[:40]
            branch = f"build/{purpose}/{pendulum.now('UTC'):%Y-%m-%d}/{run_id}"
            # exist_ok: a retry must not be blocked by the branch its own
            # previous attempt left behind. keep_failed_branch deliberately
            # retains it, so without this every retry 409s and `retries` is a
            # trap rather than a safety net.
            Nessie().create_branch(branch, from_ref="main", exist_ok=True)
            return branch

        @task(pool="lakehouse_write")
        def dbt_run(branch: str) -> dict:
            return _dbt("run", select=select, branch=branch)

        @task
        def dbt_test(branch: str) -> dict:
            """Tests run on the branch, before publication.

            This is the whole value of write-audit-publish: a failed test means
            main is never updated, rather than a bad figure being visible until
            someone notices.
            """
            return _dbt("test", select=select, branch=branch)

        @task(outlets=[outlet])
        def publish(branch: str) -> dict:
            from reporting_platform.common.context import Nessie

            n = Nessie()
            n.merge(branch, into="main")
            n.delete_reference(branch)
            return {"merged": branch}

        @task(trigger_rule="one_failed")
        def keep_failed_branch(branch: str) -> str:
            """On failure the branch is deliberately NOT deleted.

            It holds the exact bad data for diagnosis. The retention job sweeps
            abandoned working branches after 48h.
            """
            import logging
            logging.getLogger("airflow.task").error(
                "build failed; branch %s retained for inspection", branch)
            return branch

        b = open_branch()
        run = dbt_run(b)
        test = dbt_test(b)
        run >> test >> publish(b)
        test >> keep_failed_branch(b)

    return _dag()


prepared_build = build_dag(
    dag_id="prepared_build",
    # Fires when ANY raw asset updates. No feed blocks another.
    # any_of() is load-bearing -- a bare list would mean ALL.
    schedule=any_of(RAW_ASSETS),
    select="path:models/prepared",
    outlet=PREPARED_ASSET,
    purpose="prepared",
)

reporting_build = build_dag(
    dag_id="reporting_build",
    schedule=[PREPARED_ASSET],
    select="path:models/reporting",
    outlet=REPORTING_ASSET,
    purpose="reporting",
)
