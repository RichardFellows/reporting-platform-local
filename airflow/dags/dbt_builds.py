"""dbt builds for the prepared and reporting layers, rendered by Astronomer Cosmos.

Triggering is asset-based, not cron-based. `prepared_build` fires as soon as
ANY raw asset updates — no feed waits for another feed. dbt's own selection
keeps the rebuild proportionate: only models downstream of the changed source
are run.

Both builds run on a Nessie branch and merge on success (write-audit-publish).
A failing test therefore leaves `main` untouched, and consumers never see a
half-built mart.

WHAT COSMOS CHANGED
-------------------
The build tasks are no longer two hand-written `dbt run` / `dbt test`
subprocess calls. `DbtTaskGroup` reads the dbt project and emits **one Airflow
task per model**, wired in the models' own `ref()` order, plus a test task —
so a broken model is a red task carrying that model's name rather than a
4000-character log tail to read, and a clear-and-retry restarts from the model
that failed instead of from the top of the layer.

Nothing about the *shape* of the build changed: branch → build → test →
merge-only-if-clean, with the branch retained on failure. Cosmos supplies the
middle; `open_branch` and `publish` are the same tasks they always were.

**Adding a model requires no edit here.** The graph is derived from the dbt
project on every DAG parse, so a new `.sql` under `models/prepared/` appears as
a new task in `prepared_build` by itself, the same way a new entry in
`feeds.yml` appears as a new ingest DAG. That symmetry is the point.

THREE SETTINGS BELOW ARE LOAD-BEARING
-------------------------------------
1. **`InvocationMode.SUBPROCESS`.** Cosmos defaults to `DBT_RUNNER`, which
   invokes dbt *in the calling process*. Our dbt target is
   `method: session` — dbt builds a SparkSession — so DBT_RUNNER would leave a
   JVM with non-daemon threads inside the Airflow task process, heartbeats
   would stop, and the scheduler would zombie-reap the task ~300s after the
   work had already succeeded. This is the same constraint that puts every
   other Spark call behind `scripts/_spark_task.py`; see CLAUDE.md.

2. **`pool="lakehouse_write"` on every rendered task.** One dbt invocation is
   one Spark application, and each caps itself at 2 cores against a 6-core
   worker. Per-model tasks mean Airflow would otherwise start several at once
   and the cluster would hand out cores until nothing could get a full share —
   standalone mode grants free cores on request and holds them until the
   session stops, so the losers wait forever rather than failing. The single
   pool slot serialises them exactly as the old monolithic `dbt run` did by
   holding that slot for its whole duration.

3. **`LoadMode.DBT_LS`.** `LoadMode.CUSTOM` (Cosmos's own parser, no dbt
   invocation) looked attractive because it is fast and touches no adapter —
   but on this project it emits **every test twice**, once under a bare id and
   once under a `test.dbt.` one, which would collide as Airflow task ids, and
   it misses model-level tests entirely: the
   `dbt_utils.unique_combination_of_columns` blocks that prove `dedupe_rank`
   works never appear. Verified by loading the graph both ways. `DBT_LS` shells
   out to real dbt, finds all 51 tests, and does not connect to Spark —
   `dbt ls` resolves the profile without opening a session. It costs ~5s per
   DAG parse, which Cosmos caches against a hash of the project files, so it
   is paid again only when a model actually changes.
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
# Falls back to spark_local, and the fallback matters more than it looks.
# It used to be duckdb_local, which was harmless only because duckdb_local was
# BROKEN: an unset DBT_TARGET crashed loudly. Fixing duckdb_local turned that
# loud failure into a silent one -- DuckDB can only address the catalog's
# default branch, so it ignores the `nessie_ref` var this DAG passes and writes
# straight to `main`. The run would go green and write-audit-publish would have
# been bypassed with nothing red anywhere.
DBT_TARGET = os.environ.get("DBT_TARGET", "spark_local")

# REFUSE A NON-SPARK TARGET AT PARSE TIME, because the failure it prevents is
# silent. `nessie_ref` below is passed to dbt as a var and honoured only by the
# Spark profiles; an engine that cannot address a Nessie branch ignores it and
# writes to the catalog's default branch instead. The build would then SUCCEED,
# having written to `main` with no branch, no audit and nothing red anywhere.
# Every target in profiles.yml is Spark today, so this can only fire on a
# misconfigured DBT_TARGET -- which is exactly the circumstance it exists for.
#
# This used to live inside the task that shelled out to dbt. Cosmos builds the
# dbt command itself, so there is no longer a single call site to guard; the
# check moves to import time, where a bad DBT_TARGET becomes a DAG import error
# visible in the UI rather than a green run that published to main.
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

# Retry delay: SECONDS, not the five minutes this used to be.
#
# Five minutes is a sensible production number -- it waits out a transient
# cluster or catalog blip without hammering it. On a laptop it is simply dead
# time: the whole prepared build is about three minutes, so one retried task
# doubled the wall clock of the thing you were watching, and a mid-graph
# failure left the rest of the graph parked behind the pool for longer than the
# build itself takes.
#
# Env-var'd rather than hard-coded so the OpenShift deployment can put its own
# number back without a code change; the default is the local-stack one,
# because this repo IS the local stack.
RETRY_DELAY = timedelta(seconds=int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))

DEFAULT_ARGS = {"owner": "data-platform", "retries": 1,
                "retry_delay": RETRY_DELAY}

# ------------------------------------------------------------------- cosmos
# One ProfileConfig for everything: the committed dbt/profiles.yml, used as-is.
# Cosmos can also SYNTHESISE a profile from an Airflow connection
# (`profile_mapping`), and that is deliberately not used here -- profiles.yml
# carries about thirty `server_side_parameters` lines of Iceberg/Nessie/S3A
# wiring with comments explaining each one, and a second, generated copy of
# that in the Airflow connections table is a forked definition that drifts.
# There is one profile, it is in git, and dbt on the command line and dbt under
# Cosmos read the same file.
PROFILE_CONFIG = ProfileConfig(
    profile_name="reporting_platform",
    target_name=DBT_TARGET,
    profiles_yml_filepath=f"{DBT_DIR}/profiles.yml",
)

PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_DIR,
    # dbt packages are installed ONCE by `airflow-init`, not per task:
    # install_dbt_deps here would make every rendered task run `dbt deps`
    # against the network before doing any work.
    install_dbt_deps=False,
    # copy_dbt_packages was True while packages lived in the project directory,
    # to carry them into the temporary project Cosmos builds for each task --
    # without them that directory has no dbt_utils and every dbt_utils test
    # fails to compile. It is False now because dbt_project.yml's
    # `packages-install-path` is ABSOLUTE (/opt/platform/run/dbt/dbt_packages,
    # off the bind mount -- see the comment there). Cosmos resolves that key
    # against the project folder to find what to copy, and joining a folder
    # with an absolute path yields the absolute path itself, so the copy would
    # have the same source and destination. Nothing needs copying: the path is
    # absolute and identical inside every process in this container, so the dbt
    # subprocess in the temporary project resolves it directly.
    copy_dbt_packages=False,
    # Render-time vars only. The vars that reach the RUNNING dbt come from
    # operator_args below, which is where the per-run Nessie branch is injected;
    # `dbt ls` has no opinion about which branch it is describing.
    dbt_vars={"nessie_ref": "main"},
)

EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    # See the module docstring, point 1. Not negotiable while the dbt target is
    # `method: session`.
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_EXECUTABLE,
    # NOT dbt_project_path -- Cosmos rejects the project path being set on more
    # than one of the three configs, and ProjectConfig is the one that has it.
)

# AFTER_ALL, not the AFTER_EACH default, and not BUILD.
#
# Every rendered task is a separate dbt invocation and therefore a separate
# Spark application with its own ~30s session startup. AFTER_EACH would render
# one task per *test* -- 51 of them here -- and the layer would spend most of an
# hour starting and stopping JVMs. AFTER_ALL renders one `dbt test` covering the
# layer, which is exactly what the previous hand-written `dbt_test` task did.
#
# BUILD (model and its tests in one `dbt build` per node) is tempting and is
# wrong here for a second reason: under eager indirect selection a
# `relationships` test is pulled in with the model it is declared on, but its
# OTHER parent may not have been built yet -- `primary_limits`' relationship to
# `counterparty` is not a dependency of the *model*, so Cosmos has no reason to
# order them. Under cautious selection that test is silently dropped instead,
# which is worse. Testing the whole layer once, after it is whole, has neither
# problem.
#
# Overridable so a developer can flip to AFTER_EACH while chasing one failing
# test without editing the DAG.
TEST_BEHAVIOR = TestBehavior(os.environ.get("COSMOS_TEST_BEHAVIOR", "after_all"))


def _render_config(select: str) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_LS,
        # Same subprocess reasoning as the execution config: keep dbt out of
        # the DAG-processor process.
        invocation_mode=InvocationMode.SUBPROCESS,
        select=[select],
        # dbt `exposures` are documentation -- they declare who CONSUMES a mart
        # and build nothing. Cosmos has no converter for them and logs
        # "Unavailable conversion function for <DbtResourceType.EXPOSURE>" on
        # every DAG parse for each one. Dropping them at selection time is
        # honest about what they are and keeps the parse log readable; they are
        # still rendered in `dbt docs`, which is where they belong.
        exclude=["resource_type:exposure"],
        test_behavior=TEST_BEHAVIOR,
        # Cosmos would otherwise attach a Dataset outlet to every model task.
        # The cascade in this platform is deliberately layer-grained -- the
        # `prepared` asset means "the whole prepared layer is published and
        # merged to main", which is emitted by `publish` below and is the only
        # thing `reporting_build` should react to. Per-model datasets would fire
        # on a branch, before any audit, and before the merge.
        emit_datasets=False,
        # dbt_packages is installed by airflow-init; see PROJECT_CONFIG.
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
        # See the module docstring, point 2.
        "pool": "lakehouse_write",
        # profiles.yml reads NESSIE_URI, S3_ENDPOINT, REPORTING_WAREHOUSE and
        # SPARK_MASTER through env_var(). They are set on the airflow service in
        # docker-compose.yml, so the subprocess must inherit this process's
        # environment or dbt fails to resolve the profile.
        "append_env": True,
        "install_deps": False,
        # False for the same reason as ProjectConfig above: the packages live
        # at an absolute path outside the project directory, so there is
        # nothing to copy and a copy would be source-onto-itself.
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
