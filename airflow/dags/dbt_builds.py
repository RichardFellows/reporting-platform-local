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


def _spark_run(*args: str) -> dict:
    """One Spark operation in a child process. See scripts/_spark_task.run.

    The build DAG needs Spark exactly once -- to read its own input set off
    the branch -- and it needs it the same way everything else here does: in a
    subprocess, or the JVM keeps the task process alive and the scheduler
    zombie-reaps it. See docs/DECISIONS.md#spark-in-a-subprocess.
    """
    from scripts._spark_task import run

    return run(*args)


def _run_key(branch: str) -> str:
    """The registry run id for a build branch.

    Derived from the branch rather than recomputed from the Airflow run id, so
    `open_branch` and `publish` cannot slugify differently and end up writing
    two rows for one run. Prefixed with the purpose because the prepared and
    reporting builds slugify the SAME dataset-triggered run id, and their two
    runs would otherwise collide on the primary key.
    """
    return f"{branch.split('/')[1]}-{branch.rsplit('/', 1)[-1]}"


def build_dag(dag_id: str, schedule, select: str, outlet, purpose: str):

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["reporting-platform", "dbt", "cosmos", purpose],
        # REQ-405. The change this build is being made under -- a Jira key, a
        # change number, whatever the process uses. Carried onto the Nessie
        # merge commit and onto the run record, so "why did this figure
        # change" has an answer in the catalog rather than only in somebody's
        # inbox. Empty for a scheduled build, which is the honest answer:
        # nothing authorised it beyond the schedule.
        params={"change_ref": ""},
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
            _open_run(_run_key(branch), branch, context)
            return branch

        def _open_run(run_id: str, branch: str, context) -> None:
            """REQ-400/REQ-404. Record the run BEFORE it builds anything.

            Opened here rather than at publish because a run that fails is the
            one most worth having a record of -- and because `code_ref` and
            `dbt_manifest_ref` describe the code that is about to run, which
            is only knowable now. Best-effort in the same sense
            `normalize`'s registry write is: the registry is a record over
            work that happens whether or not the row lands, and taking a build
            down to protect its own audit row would be the wrong way round.
            The publish step re-opens the run if this failed, so a lost row
            here costs the start time and nothing else.
            """
            import logging

            from reporting_platform.common.context import (
                ENV, code_ref, dbt_manifest_ref,
            )
            from reporting_platform.registry import runs

            try:
                ref, kind = code_ref()
                runs.open_run(
                    run_id, purpose, branch,
                    environment=ENV, code_ref=ref, code_ref_kind=kind,
                    dbt_manifest_ref=dbt_manifest_ref(),
                    dag_id=dag_id, airflow_run_id=context["run_id"],
                    change_ref=(context["params"].get("change_ref") or None))
            except Exception as exc:                            # noqa: BLE001
                logging.getLogger("airflow.task").warning(
                    "could not open the run record for %s: %s", run_id,
                    f"{type(exc).__name__}: {exc}")

        # THE POOL, because this task now runs Spark. It did not before: it was
        # a Nessie merge and nothing else. Reading the input set off the branch
        # is one more Spark application, and standalone mode hands out every
        # free core and holds them until the session stops -- so a publish
        # outside the pool would contend with the next build's model tasks for
        # exactly the reason `lakehouse_write` exists. Safe to take here: every
        # rendered dbt task upstream has finished and released its slot.
        @task(outlets=[outlet], pool="lakehouse_write")
        def publish(branch: str, **context) -> dict:
            """Merge the audited branch into main, and RECORD THE PUBLICATION.

            Reached only if every model task AND the test task succeeded --
            that is the "publish" in write-audit-publish, and the reason a
            failed test can never move `main`.

            Four things happen here that did not before, and the order
            matters:

              1. THE INPUT SET IS READ OFF THE BRANCH, before the merge, while
                 it is still exactly what was audited. REQ-400.
              1b. THE AS-AT LIFECYCLE GATE runs between them, because it is
                 the only check here that must be able to stop the
                 publication -- and after the merge it could not. REQ-500/502.
              2. The merge carries a COMMIT MESSAGE naming the purpose, the
                 business date and the change reference. REQ-405. Nessie
                 otherwise synthesises "Merge <hash> into main", which names
                 two hashes and no reason.
              3. A REPORTING build cuts one tag PER REPORT --
                 `published/<report>/<bd>/<run_id>` -- and allocates that
                 report's next version for the as-at date. REQ-401/REQ-403.
                 `references.published_tags.per_report` has been waiting for a
                 publication that knows which report it is for; this is it.
              4. The run is closed with the merged hash.

            A prepared build publishes no report and cuts no tag: it is a run,
            it is recorded as one, and calling it a publication is the mistake
            the ingest DAG used to make.
            """
            import logging

            from reporting_platform.common.context import (
                Nessie, published_tag, reports,
            )
            from reporting_platform.registry import runs

            log = logging.getLogger("airflow.task")
            run_id = _run_key(branch)
            change_ref = (context["params"].get("change_ref") or "").strip() or None

            # 1. What this build read, from the branch it built on.
            inputs, business_date = {}, None
            try:
                inputs = _spark_run("run-inputs", branch)
                business_date = inputs.get("max_business_date")
                if inputs.get("unreadable"):
                    # NOT fatal, and deliberately so: the tables are audited
                    # and correct, and refusing to publish them because their
                    # provenance could not be enumerated would take the
                    # platform down to protect a record of it. Loud, though --
                    # this is the state the _prepared.yml migration guard
                    # exists to make impossible.
                    log.error("run %s: input set INCOMPLETE -- %s", run_id,
                              inputs["unreadable"])
                runs.record_inputs(
                    run_id, [(feed, delivery) for feed, delivery in
                             inputs.get("inputs", [])])
            except Exception as exc:                            # noqa: BLE001
                log.error("run %s: could not record the input set: %s", run_id,
                          f"{type(exc).__name__}: {exc}")

            # 1b. THE AS-AT LIFECYCLE GATE, and it must be HERE -- after the
            # input set (which is what makes the as-at date knowable) and
            # BEFORE the merge. Placed with the versioning below it would fire
            # after `main` had already moved, so the refusal would be a
            # complaint about a publication that had happened. Refusing here
            # fails the task with main untouched and the branch retained by
            # keep_failed_branch, which is write-audit-publish doing its job.
            #
            # REQ-500/502. A LifecycleRefused is deliberately NOT caught: it is
            # the one error in this task that must stop the publication. Every
            # other failure here is best-effort because the tables are audited
            # and correct; this one says the tables must not become main's.
            carried_forward: list[dict] = []
            if purpose == "reporting" and business_date:
                from datetime import date as _date

                from reporting_platform.registry import lifecycle

                candidate = {(feed, delivery) for feed, delivery
                             in inputs.get("inputs", [])}
                for report in sorted(reports()):
                    verdict = lifecycle.check_publishable(
                        report, _date.fromisoformat(business_date), candidate)
                    if verdict["carried_forward"]:
                        # The policy said carry forward, so the publication
                        # proceeds -- but "we knowingly did not restate this"
                        # belongs on the run record, not in a task log that is
                        # swept with the rest of them.
                        carried_forward.append(verdict)

            # 2. The merge, with something written on it.
            n = Nessie()
            message = (f"publish({purpose}): {business_date or 'no business date'}"
                       f" run {run_id}"
                       + (f" [{change_ref}]" if change_ref else ""))
            n.merge(branch, into="main", message=message,
                    properties={"purpose": purpose, "run_id": run_id,
                                "change_ref": change_ref or "",
                                "business_date": business_date or ""})
            merged = n.get_reference("main")["reference"]["hash"]
            n.delete_reference(branch)

            # 3. The pins, one per report, and the version each one is.
            published: list[dict] = []
            version_errors: list[str] = []
            if purpose == "reporting" and business_date:
                from datetime import date as _date

                as_at = _date.fromisoformat(business_date)
                for report in sorted(reports()):
                    tag = published_tag(report, as_at, run_id)
                    try:
                        n.create_tag(tag, from_ref="main")
                    except Exception as exc:                    # noqa: BLE001
                        # A rerun of this task finds its own tag already
                        # there. allocate_version is idempotent on the tag, so
                        # the version is not minted twice either.
                        log.warning("tag %s: %s", tag, str(exc)[:200])
                    try:
                        version = runs.allocate_version(report, as_at, run_id, tag)
                    except Exception as exc:                    # noqa: BLE001
                        # NOT fatal -- the merge has already happened and the
                        # tag is cut, so the publication is real whether or not
                        # it was numbered. But a tag with no version row is an
                        # inconsistency somebody has to explain later, so it is
                        # carried onto the run record rather than living only
                        # in a task log. Observed once, for real: the allocator
                        # used SELECT ... FOR UPDATE over an aggregate, which
                        # Postgres refuses, and nothing found out until the
                        # first reporting build published.
                        detail = f"{type(exc).__name__}: {exc}"
                        log.error("could not allocate a version for %s %s: %s",
                                  report, as_at, detail)
                        version_errors.append(f"{report}: {detail}")
                        version = None
                    published.append({"report": report, "tag": tag,
                                      "version": version})
            elif purpose == "reporting":
                log.error(
                    "reporting build on %s published no report: the input set "
                    "yielded no business date, so there is nothing to pin an "
                    "as-at date to. See the run-inputs error above.", branch)

            # 4. Close the run.
            try:
                from datetime import date as _date

                notes = list(version_errors)
                notes += [
                    f"carried forward: {v['report']} {v['as_at_date']} is "
                    f"{v['state']} with {len(v.get('added') or [])} added and "
                    f"{len(v.get('removed') or [])} removed delivery(ies); "
                    f"v{v.get('published_version')} stands"
                    for v in carried_forward]
                runs.finish_run(
                    run_id, runs.PUBLISHED, merged_hash=merged,
                    business_date=_date.fromisoformat(business_date)
                    if business_date else None,
                    error="; ".join(notes) or None)
            except Exception as exc:                            # noqa: BLE001
                log.warning("could not close run %s: %s", run_id,
                            f"{type(exc).__name__}: {exc}")

            return {"merged": branch, "run_id": run_id, "hash": merged,
                    "business_date": business_date,
                    "inputs": len(inputs.get("inputs", [])),
                    "carried_forward": carried_forward,
                    "published": published}

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
            # THE RUN RECORD HAS TO SAY IT FAILED, or the only rows in it are
            # the successes and "how often does this build fail" is a question
            # about a table that only records wins.
            try:
                from reporting_platform.registry import runs

                runs.finish_run(_run_key(branch), runs.FAILED,
                                error=f"failed tasks: {', '.join(bad)}")
            except Exception as exc:                            # noqa: BLE001
                logging.getLogger("airflow.task").warning(
                    "could not close the failed run: %s",
                    f"{type(exc).__name__}: {exc}")
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
