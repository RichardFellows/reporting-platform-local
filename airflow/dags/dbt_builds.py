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
from reporting_platform.transform import wap
from reporting_platform.transform.dbt import archive_invocation, target as dbt_target

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
# REFUSE A NON-SPARK TARGET AT PARSE TIME, because the failure it prevents is
# silent: the build would SUCCEED, having written to main with no branch and no
# audit. At import time, so it surfaces as a DAG import error in the UI. The
# check is `transform.dbt.target()`, shared with the hand-run build.
# See docs/DECISIONS.md#dbt-target-guard
DBT_TARGET = dbt_target()

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
    # LOCAL IN BOTH EXECUTION MODES, deliberately. With PLATFORM_EXECUTION=
    # kubernetes the dbt child process is still the driver, here in the task's
    # pod; spark_ocp gives it a k8s:// master, so only the EXECUTORS move to
    # pods. Cosmos's KUBERNETES mode would run dbt in another pod, where
    # `_archive_dbt_artifacts` below cannot read its target/ -- and `publish`
    # refuses to merge a build whose artifacts it cannot verify.
    # See docs/DECISIONS.md#execution-mode-is-configuration
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


def _archive_dbt_artifacts(context) -> None:
    """Retain this Cosmos task's artifacts before the next task overwrites them.

    Runs on BOTH `on_success_callback` and `on_failure_callback`, so a failing
    test task's evidence is captured exactly like a passing one. The work is
    `transform.dbt.archive_invocation`, which the hand-run build calls too.
    """
    ti = context["ti"]
    branch = ti.xcom_pull(task_ids="open_branch")
    if not branch:
        raise RuntimeError(
            f"{ti.task_id}: cannot associate dbt artifacts without open_branch")
    archive_invocation(wap.run_key(branch), ti.task_id, ti.try_number)


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
        # Cosmos invokes dbt once per task and every invocation overwrites the
        # shared target directory. Archive while the task still owns it; the
        # publish task verifies all successful invocations before merging.
        "on_success_callback": _archive_dbt_artifacts,
        "on_failure_callback": _archive_dbt_artifacts,
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
        # REQ-405. The change this build is made under -- a Jira key, a change
        # number, whatever the process uses. Carried onto the Nessie merge
        # commit and the run record, so "why did this figure change" has an
        # answer in the catalog. Empty for a scheduled build, which is honest:
        # nothing authorised it beyond the schedule.
        params={"change_ref": ""},
    )
    def _dag():
        # THE BRANCH, THE PUBLICATION AND THE FAILURE ARE `transform.wap`,
        # the same functions `python -m reporting_platform.transform` runs.
        # What only Airflow knows -- its run id, the task attempts that
        # succeeded -- is passed in from the context.

        @task
        def open_branch(**context) -> str:
            return wap.open_build(
                purpose, context["run_id"],
                change_ref=(context["params"].get("change_ref") or None),
                dag_id=dag_id, airflow_run_id=context["run_id"])

        # THE POOL, because this task runs Spark (reading the input set off
        # the branch is one more Spark application), and standalone mode
        # holds every free core until the session stops. Safe to take here:
        # every rendered dbt task upstream has released its slot.
        @task(outlets=[outlet], pool="lakehouse_write")
        def publish(branch: str, **context) -> dict:
            """Reached only if every model task AND the test task succeeded --
            the "publish" in write-audit-publish. See `wap.publish`.

            A callback exception alone does not reliably change the completed
            task's state in every supported Airflow version, so the attempts
            are read here and `wap.publish` verifies each one's artifacts
            before anything is merged.
            """
            dbt_attempts = [
                (ti.task_id, ti.try_number)
                for ti in context["ti"].get_dagrun().get_task_instances()
                if ti.task_id.startswith("dbt.") and ti.state == "success"
            ]
            return wap.publish(
                branch, dbt_attempts=dbt_attempts,
                change_ref=context["params"].get("change_ref"))

        @task(trigger_rule="all_done")
        def keep_failed_branch(branch: str, **context) -> str:
            """On failure the branch is deliberately NOT deleted -- see
            `wap.fail_build`.

            `all_done` rather than `one_failed`, and that is a Cosmos-shaped
            choice. With a rendered graph, a model failing in the MIDDLE
            leaves every task after it `upstream_failed` -- which `one_failed`
            does not count as a failure, so this task would never fire on
            precisely the case it exists for. So it always runs, inspects the
            run, and skips itself when there is nothing to report.
            """
            from airflow.exceptions import AirflowSkipException

            me = context["ti"].task_id
            bad = sorted(
                ti.task_id for ti in context["ti"].get_dagrun().get_task_instances()
                if ti.task_id != me and ti.state in ("failed", "upstream_failed")
            )
            if not bad:
                raise AirflowSkipException("build succeeded; nothing to retain")
            return wap.fail_build(branch, f"failed tasks: {', '.join(bad)}")

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
