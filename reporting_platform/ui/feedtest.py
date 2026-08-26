"""Build and test ONE feed's models, on a throwaway Nessie branch.

WHAT THIS IS FOR. `dbt parse` proves dbt can read the project; it says nothing
about whether the model produces correct rows. The failures that actually cost
a dev an afternoon are the ones only a build shows: a column named in the
model that raw does not have, a `safe_cast` that quietly nulls every value
because the source format was misread, a `relationships` test failing because
the reference feed has no data on that business date.

Today the only way to see those is the full `prepared_build` DAG -- every
model in the layer, through Airflow. This runs `dbt build` (run **and** test)
for one feed, on its own branch, and streams the output back.

**IT NEVER MERGES.** The Airflow builds open a branch, build, test and merge
to `main` on success -- that is publication. This is a dev proving a
definition, and publishing as a side effect of pressing "test" is exactly the
kind of surprise write-audit-publish exists to prevent. On success the branch
is deleted; on failure it is deliberately kept, holding the exact bad data for
inspection, the way `keep_failed_branch` does in the DAG.

It runs OUTSIDE Airflow's `lakehouse_write` pool, so it is not serialised
against the DAG builds by that pool -- `jobs.py` allows only one at a time for
the same reason, and the 2-core cap in `spark_session` is what keeps the two
from starving each other.
"""
from __future__ import annotations

import json
import os

from reporting_platform.common.context import Feed, Nessie, new_run_id

from . import jobs
from .dbt_check import DBT_DIR, DBT_TARGET, TARGET_PATH

KIND = "feed-test"


def selector(feed: Feed, downstream: bool = False) -> str:
    """What to build.

    The feed's prepared model by name -- which is also the table name, since
    no `alias` is configured anywhere. `dbt build` runs the model and then its
    tests, so this one selector is the whole audit.

    `downstream` appends `+`, pulling in the reporting models that consume it.
    Off by default: a brand-new feed usually has no reporting model yet, and
    for one that does, a failure downstream is a different question from
    "is my feed definition right".
    """
    return f"{feed.name}+" if downstream else feed.name


def start(feed: Feed, downstream: bool = False) -> jobs.Job:
    # Named out here, not inside the thread, so the caller is told the branch
    # in the same response that starts the job -- a failed build leaves that
    # branch behind for inspection, and being handed it only after the failure
    # is the wrong moment.
    branch = f"build/feed-test/{feed.name}/{new_run_id()}"

    def _run(job: jobs.Job) -> None:
        nessie = Nessie()
        job.log(f"opening branch {branch} from main")
        nessie.create_branch(branch, from_ref="main", exist_ok=True)
        # update, never reassign: `start` below seeds `branch` into this dict
        # before returning, and this thread is already running by then.
        job.result.update({"branch": branch, "feed": feed.name, "merged": False})

        args = [
            "dbt", "build",
            "--project-dir", DBT_DIR,
            "--profiles-dir", DBT_DIR,
            "--target", DBT_TARGET,
            # Its own target dir, so a console build and a concurrent Airflow
            # build do not overwrite each other's manifest and run_results.
            "--target-path", TARGET_PATH,
            "--select", selector(feed, downstream),
            # THE THREAD THAT MAKES THIS A BRANCH BUILD. profiles.yml reads
            # this var into the catalog `ref`; without it dbt writes to main.
            "--vars", json.dumps({"nessie_ref": branch}),
        ]
        code = jobs.stream(job, args)

        if code == 0:
            job.log(f"\nbuild and tests passed on {branch}")
            # Nothing to publish -- this was a test. Delete the branch rather
            # than leaving one behind per press.
            try:
                nessie.delete_reference(branch)
                job.log(f"branch {branch} deleted (nothing published; "
                        f"testing never merges to main)")
                job.result["branch_deleted"] = True
            except Exception as exc:                           # noqa: BLE001
                job.log(f"could not delete {branch}: {exc}")
                job.result["branch_deleted"] = False
            return

        job.log(f"\nbuild FAILED (exit {code}). Branch {branch} kept for "
                f"inspection -- it holds exactly what the build produced. "
                f"`main` is untouched.")
        job.result["branch_deleted"] = False
        raise RuntimeError(f"dbt build failed (exit {code}) — see the log above")

    label = f"dbt build --select {selector(feed, downstream)}"
    job = jobs.start(KIND, label, _run)
    job.result.setdefault("branch", branch)
    return job


def environment_note() -> str:
    """What the caller should know before pressing the button."""
    return (f"Runs on {os.environ.get('SPARK_MASTER', 'the Spark cluster')} "
            f"with target {DBT_TARGET}. Minutes, not seconds.")
