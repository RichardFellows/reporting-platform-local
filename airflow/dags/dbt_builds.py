"""dbt builds for the prepared and reporting layers, rendered by Astronomer Cosmos.

Triggering is asset-based, not cron-based. `prepared_build` fires as soon as ANY
raw asset updates -- no feed waits for another feed. dbt's own selection keeps
the rebuild proportionate: only models downstream of the changed source are run.

Both builds run on a Nessie branch and merge on success (write-audit-publish).
A failing test leaves `main` untouched, and consumers never see a half-built
mart.

`DbtTaskGroup` emits one Airflow task per model, in the models' own `ref()`
order, plus a test task. Adding a model requires no edit here: the graph is
derived from the dbt project on every DAG parse.

FOUR SETTINGS BELOW ARE LOAD-BEARING -- InvocationMode.SUBPROCESS, the
lakehouse_write pool on every rendered task, LoadMode.DBT_LS, and
TestBehavior.AFTER_ALL. Each is commented at its site.
See docs/DECISIONS.md#cosmos-rendered-builds and #cosmos-load-bearing-settings
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from reporting_platform.common.context import feeds

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    InvocationMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)

try:                                    # Airflow 3
    from airflow.sdk import Asset, dag, task
except ImportError:                     # Airflow 2.x
    from airflow.datasets import Dataset as Asset  # type: ignore
    from airflow.decorators import dag, task       # type: ignore

CATALOG = os.environ.get("REPORTING_CATALOG", "lakehouse")
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt")
# The fallback matters more than it looks: a non-Spark default would ignore
# `nessie_ref` and write straight to main, green.
# See docs/DECISIONS.md#dbt-target-guard
DBT_TARGET = os.environ.get("DBT_TARGET", "spark_local")

# REFUSE A NON-SPARK TARGET AT PARSE TIME, because the failure it prevents is
# silent: the build would SUCCEED, having written to main with no branch and no
# audit. At import time, so it surfaces as a DAG import error in the UI.
# See docs/DECISIONS.md#dbt-target-guard
if not DBT_TARGET.startswith("spark"):
    raise RuntimeError(
        f"DBT_TARGET is {DBT_TARGET!r}, which is not a Spark target. "
        f"Builds must run on Spark: the branch each build opens is passed to "
        f"dbt as the `nessie_ref` var, and a non-Spark engine ignores it and "
        f"writes to the default branch, bypassing write-audit-publish without "
        f"failing."
    )

RAW_ASSETS = [Asset(f.asset_uri) for f in feeds().values()]


def any_of(assets):
    """OR the assets together, so ANY one updating fires the consumer.

    A bare list is **AND** in Airflow, which would let one late feed hold up
    every build. Falls back to the list with a warning rather than degrading to
    AND silently. See docs/DECISIONS.md#assets-are-or-not-and
    """
    import functools
    import logging
    import operator

    if not assets:
        # No feeds yet -- an empty project, not a capability problem. Without
        # this the reduce below raises TypeError and the handler reports "this
        # Airflow does not support OR-ing assets", which is false and is
        # printed on every DAG parse. A message that reads as a diagnosis and
        # is not one is worse than no message.
        return []

    try:
        return functools.reduce(operator.or_, assets)
    except TypeError:
        logging.getLogger(__name__).warning(
            "this Airflow does not support OR-ing assets; prepared_build will "
            "wait for ALL raw feeds, not any. See docs/ARCHITECTURE.md.")
        return assets


PREPARED_ASSET = Asset(f"iceberg://{CATALOG}/prepared/all")
REPORTING_ASSET = Asset(f"iceberg://{CATALOG}/reporting/all")

# Retry delay: SECONDS, not the five minutes this used to be.
# Env-var'd so a cluster deployment can put a production number back without a
# code change. See docs/DECISIONS.md#retry-delay
RETRY_DELAY = timedelta(seconds=int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))

DEFAULT_ARGS = {"owner": "data-platform", "retries": 1,
                "retry_delay": RETRY_DELAY}

# ------------------------------------------------------------------- cosmos
# The committed dbt/profiles.yml, used as-is -- NOT a profile synthesised from
# an Airflow connection. See docs/DECISIONS.md#cosmos-profile-config
PROFILE_CONFIG = ProfileConfig(
    profile_name="reporting_platform",
    target_name=DBT_TARGET,
    profiles_yml_filepath=f"{DBT_DIR}/profiles.yml",
)

PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_DIR,
    # Packages are installed ONCE by airflow-init, not per task, and live at an
    # absolute path outside the project directory, so there is nothing to copy.
    # See docs/DECISIONS.md#cosmos-packages
    install_dbt_deps=False,
    copy_dbt_packages=False,
    # Render-time vars only. The vars that reach the RUNNING dbt come from
    # operator_args below, where the per-run Nessie branch is injected.
    dbt_vars={"nessie_ref": "main"},
)

EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    # LOAD-BEARING: DBT_RUNNER would leave a JVM inside the task process and the
    # scheduler would zombie-reap it.
    # See docs/DECISIONS.md#cosmos-load-bearing-settings
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_EXECUTABLE,
    # NOT dbt_project_path -- Cosmos rejects the project path being set on more
    # than one of the three configs, and ProjectConfig is the one that has it.
)

# LOAD-BEARING: AFTER_EACH is 51 JVM starts, and BUILD drops or misorders
# cross-model `relationships` tests. Overridable so a developer can flip to
# AFTER_EACH while chasing one failing test.
# See docs/DECISIONS.md#cosmos-load-bearing-settings
TEST_BEHAVIOR = TestBehavior(os.environ.get("COSMOS_TEST_BEHAVIOR", "after_all"))


def _render_config(select: str) -> RenderConfig:
    return RenderConfig(
        # LOAD-BEARING: Cosmos's own CUSTOM parser double-emits every test and
        # misses model-level ones.
        # See docs/DECISIONS.md#cosmos-load-bearing-settings
        load_method=LoadMode.DBT_LS,
        # Keep dbt out of the DAG-processor process, as above.
        invocation_mode=InvocationMode.SUBPROCESS,
        select=[select],
        # Exposures are documentation and build nothing; Cosmos has no converter
        # and warns per exposure on every parse.
        # See docs/DECISIONS.md#cosmos-exclude-exposures
        exclude=["resource_type:exposure"],
        test_behavior=TEST_BEHAVIOR,
        # The cascade is layer-grained: per-model datasets would fire on a
        # branch, before the audit and before the merge.
        # See docs/DECISIONS.md#cosmos-emit-datasets
        emit_datasets=False,
        dbt_deps=False,
        dbt_executable_path=DBT_EXECUTABLE,
    )


def _operator_args(branch_task_id: str) -> dict:
    return {
        # THE WHOLE OF WRITE-AUDIT-PUBLISH IS THIS ONE LINE. `vars` is a Cosmos
        # template field, so this renders per run: profiles.yml threads
        # `var('nessie_ref')` into spark.sql.catalog.lakehouse.ref, and the
        # entire build lands on the branch `open_branch` just created.
        "vars": {"nessie_ref": f"{{{{ ti.xcom_pull(task_ids='{branch_task_id}') }}}}"},
        # LOAD-BEARING: one dbt invocation is one Spark app, and standalone mode
        # holds cores until the session stops.
        # See docs/DECISIONS.md#cosmos-load-bearing-settings
        "pool": "lakehouse_write",
        # profiles.yml reads NESSIE_URI, S3_ENDPOINT, REPORTING_WAREHOUSE and
        # SPARK_MASTER through env_var(). They are set on the airflow service in
        # docker-compose.yml, so the subprocess must inherit this process's
        # environment or dbt fails to resolve the profile.
        "append_env": True,
        # Same reasoning as PROJECT_CONFIG above.
        # See docs/DECISIONS.md#cosmos-packages
        "install_deps": False,
        "copy_dbt_packages": False,
    }


def build_dag(dag_id: str, schedule, select: str, outlet, purpose: str):

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["reporting-platform", "dbt", "cosmos", purpose],
    )
    def _dag():
        import re

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

        @task(outlets=[outlet])
        def publish(branch: str) -> dict:
            """Merge the audited branch into main.

            Reached only if every model task AND the test task succeeded --
            that is the "publish" in write-audit-publish, and the reason a
            failed test can never move `main`.
            """
            from reporting_platform.common.context import Nessie

            n = Nessie()
            n.merge(branch, into="main")
            n.delete_reference(branch)
            return {"merged": branch}

        @task(trigger_rule="all_done")
        def keep_failed_branch(branch: str, **context) -> str:
            """On failure the branch is deliberately NOT deleted.

            It holds the exact bad data for diagnosis. The retention job sweeps
            abandoned `build/*` branches after 120h -- deliberately longer than
            the 48h it gives `ingest/*`, so a build that fails at 22:00 on a
            Friday is still there on Monday morning. See
            retention.yml -> references.working_branches. (This docstring said
            48h for a long time, which was the global value before that split.)

            `all_done` rather than `one_failed`, and that is a Cosmos-shaped
            change. With a single upstream `dbt_test` task, `one_failed` was
            exact. With a rendered graph, a model failing in the MIDDLE leaves
            every task after it `upstream_failed` -- which `one_failed` does not
            count as a failure, so this task would never fire on precisely the
            case it exists for. So it always runs, inspects the run, and skips
            itself when there is nothing to report.
            """
            import logging

            from airflow.exceptions import AirflowSkipException

            me = context["ti"].task_id
            bad = sorted(
                ti.task_id for ti in context["ti"].get_dagrun().get_task_instances()
                if ti.task_id != me and ti.state in ("failed", "upstream_failed")
            )
            if not bad:
                raise AirflowSkipException("build succeeded; nothing to retain")
            logging.getLogger("airflow.task").error(
                "build failed (%s); branch %s retained for inspection. "
                "Query it with the nessie_ref var, or delete it by hand once "
                "you are done: it is swept automatically after 48h.",
                ", ".join(bad), branch)
            return branch

        b = open_branch()

        # The rendered dbt graph. Everything between the branch and the merge.
        models = DbtTaskGroup(
            group_id="dbt",
            project_config=PROJECT_CONFIG,
            profile_config=PROFILE_CONFIG,
            execution_config=EXECUTION_CONFIG,
            render_config=_render_config(select),
            operator_args=_operator_args("open_branch"),
            default_args=DEFAULT_ARGS,
        )

        b >> models
        models >> publish(b)
        models >> keep_failed_branch(b)

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
