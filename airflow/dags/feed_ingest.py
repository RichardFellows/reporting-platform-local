"""One ingest DAG per feed, generated from reporting_platform/config/feeds.yml.

Requirement being satisfied: *each feed must be processed as soon as it is
received* — there is no single batch run that processes all feeds together.

Adding a feed means adding a block to feeds.yml. No DAG file is edited.

Each DAG:
  resolve_arrival -> normalize -> ingest (Spark) -> snapshot (Nessie merge + tag)
and emits an Asset on completion, which is what triggers the dbt builds.

`normalize` is the stage that turns whatever the upstream actually delivered
into the one shape ingest understands -- a manifest in `ready/` naming the
business date, the objects holding the rows, and how to read them. It costs
nothing for a plain CSV (a small JSON object, no copy) and it is where zips,
control files and everything else in docs/DELIVERY-SHAPES.md will be handled,
so `ingest` keeps exactly one code path.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pendulum

from reporting_platform.common.context import feeds

# ---------------------------------------------------------------- AF2/AF3 shim
# The estate is mid Airflow 2->3 migration; keep DAG code portable across both.
try:                                    # Airflow 3
    from airflow.sdk import Asset, dag, task
    _AF3 = True
except ImportError:                     # Airflow 2.x
    from airflow.datasets import Dataset as Asset  # type: ignore
    from airflow.decorators import dag, task       # type: ignore
    _AF3 = False

# Env-var'd so a cluster deployment can put a production number back without a
# code change. See docs/DECISIONS.md#retry-delay
RETRY_DELAY = timedelta(seconds=int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": RETRY_DELAY,
    "email_on_failure": False,
}


def _spark_subprocess(*args: str) -> dict:
    """Run a Spark-using operation in a child process and parse its JSON.

    An in-process SparkSession makes the task hang after it returns and the
    scheduler zombie-reaps it. See docs/DECISIONS.md#spark-in-a-subprocess.
    In OpenShift this becomes a
    KubernetesPodOperator issuing spark-submit -- same module, same arguments,
    a different execution wrapper, and the same process-isolation property.

    The implementation moved to `scripts/_spark_task.run` when the build DAG
    needed the same launcher: the argument list and the dispatch table it
    feeds belong in one file, or two copies of this eventually parse the same
    output differently.
    """
    from scripts._spark_task import run

    return run(*args)


def build_feed_dag(feed):
    asset = Asset(feed.asset_uri)

    @dag(
        dag_id=f"ingest_{feed.name}",
        description=f"Ingest {feed.name}: {feed.description}",
        # No schedule. A run is triggered per arrival -- by the inbox watcher
        # when a file is dropped (reporting_platform/ingest/inbox.py), by the
        # feed console, or by hand. A cron would reintroduce the batch window
        # the per-feed design exists to remove.
        # See docs/DECISIONS.md#inbox-is-polled
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,          # serialise re-deliveries of the same feed
        default_args=DEFAULT_ARGS,
        tags=["reporting-platform", "ingest", feed.source_system],
        params={"object_key": "", "business_date": ""},
    )
    def _dag():

        @task(task_id="resolve_arrival")
        def resolve_arrival(**context) -> dict:
            """Determine which delivery to ingest.

            Triggered runs carry the object key in dag_run.conf -- a LANDING
            key, because that is what the inbox watcher and the console have
            in hand. A manual run with no conf falls back to the oldest
            unprocessed delivery, which by then is a MANIFEST key. Both are
            handed to `normalize` below, which resolves either into a
            manifest.
            """
            conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
            key = conf.get("object_key") or context["params"].get("object_key")
            if key:
                return {"object_key": key,
                        "business_date": conf.get("business_date") or None}
            pending = _spark_subprocess("pending", feed.name)["pending"]
            if not pending:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException(f"no pending arrivals for {feed.name}")
            return {"object_key": pending[0], "business_date": None}

        @task(task_id="normalize")
        def normalize_task(arrival: dict) -> dict:
            """Landing object -> a manifest in `ready/`. No Spark.

            Plain Python -- boto3 and json -- so it runs in the task process
            rather than through `scripts/_spark_task.py`. The moment a
            normalizer needs Spark it must move there, for the reason that
            module's header gives.

            Idempotent: a delivery that already has a manifest is left alone,
            and a manifest key arriving here passes straight through. That is
            what makes a retry safe and what lets the same task serve both the
            triggered path (a landing key from the inbox) and the manual one
            (a manifest key from `pending`).

            A delivery waiting on its control file (`delivery.control`, step 4
            of docs/DELIVERY-SHAPES.md) SKIPS rather than fails. `DEFAULT_ARGS`
            retries twice at `RETRY_DELAY` -- seconds, tuned for a transient
            infra hiccup -- which would turn "the control file has not landed
            yet" into a hard failure in well under a minute, for the
            ordinary case this whole mechanism exists to handle gracefully.
            Nothing declares when a delivery is actually late -- there is no
            timeout here, by decision, see
            docs/DECISIONS.md#no-arrival-timeout. Skipping leaves this run
            asking nothing further; the safety-net poll path
            (`resolve_arrival`'s `find_pending` fallback above, or
            `scripts.bulk_ingest`) is what picks the delivery up once the
            control file lands, same as for a landing object nobody triggered
            a run for at all.
            """
            from airflow.exceptions import AirflowSkipException

            from reporting_platform.common.context import feed as get_feed
            from reporting_platform.ingest import normalize as norm

            fd = get_feed(feed.name)
            key = arrival["object_key"]
            if norm.is_manifest_key(fd, key):
                return {**arrival, "normalized": False}
            try:
                manifest = norm.normalize(fd, key)
            except norm.NotReady as exc:
                raise AirflowSkipException(str(exc)) from exc
            return {"object_key": norm.manifest_key(fd, key),
                    "business_date": arrival.get("business_date"),
                    "normalized": True,
                    "landed_object": key,
                    "delivery_business_date": manifest["business_date"]}

        @task(task_id="ingest", outlets=[asset], pool="lakehouse_write")
        def ingest_task(arrival: dict, **context) -> dict:
            """Land the CSV into raw Iceberg on a Nessie branch, then merge.

            The Spark work runs on the spark-master/spark-worker cluster;
            only the DRIVER lives in the child process this task spawns. In
            OpenShift this becomes a KubernetesPodOperator issuing
            spark-submit — same module, same arguments, different execution
            wrapper.
            """
            return _spark_subprocess(
                "ingest",
                feed.name,
                arrival["object_key"],
                context["run_id"].replace(":", "").replace("+", "")[-24:],
                arrival.get("business_date") or "",
            )

        @task(task_id="record_snapshot")
        def record_snapshot(result: dict) -> dict:
            """Pin the state this ingest left, so it stays addressable.

            THIS TASK USED TO BE CALLED `record_publication` AND CUT
            `published/<bd>/<run_id>`, AND AN INGEST IS NOT A PUBLICATION.
            The consequences were not cosmetic: every check that read
            `published/` -- reproducibility, evidence, the per-report
            retention window -- was reading ingests, N feeds landing one
            business date cut N tags that carried no feed name to tell them
            apart, and `references.published_tags.per_report` could never match
            anything because no ingest knows which report it serves.

            It is still worth pinning. Raw is where retention deletes business
            dates, so the state each ingest left is exactly what someone may
            need to read back. It is simply a different object with a
            different lifetime: `snapshot/<feed>/<bd>/<run_id>`, kept by
            `references.snapshot_tags` in retention.yml. A REPORT publication
            is cut by the reporting build, which knows what it published.
            """
            from datetime import date as _date

            from reporting_platform.common.context import Nessie, snapshot_tag

            tag = snapshot_tag(feed.name,
                               _date.fromisoformat(result["business_date"]),
                               result["run_id"])
            try:
                Nessie().create_tag(tag, from_ref="main")
            except Exception as exc:      # tag already exists on a rerun
                return {**result, "tag": tag, "tag_error": str(exc)}
            return {**result, "tag": tag}

        @task(task_id="report_drift")
        def report_drift(result: dict) -> dict:
            """Schema drift is reported, never fatal.

            A new upstream column must not stop the pipeline; it must show up
            as a warning and in _extra_columns, so the dbt model can be
            extended deliberately rather than under incident pressure.

            TWO DIFFERENT EVENTS ARE REPORTED HERE and they are logged apart
            on purpose. DRIFT is a statement about one delivery: the file did
            not match the contract, and it can happen on any run. A CONTRACT
            CHANGE is a statement about the deployment: `feeds.yml` changed
            and this was the first ingest to carry it into the raw table, so
            it appears once and then never again for that column. Reading the
            second as the first sends somebody to the upstream about a change
            that was made here.
            """
            import logging

            log = logging.getLogger("airflow.task")
            if result.get("missing_columns") or result.get("extra_columns"):
                log.warning(
                    "SCHEMA DRIFT %s %s: missing=%s extra=%s",
                    feed.name, result["business_date"],
                    result["missing_columns"], result["extra_columns"])
            if result.get("columns_added"):
                log.warning(
                    "CONTRACT CHANGE %s %s: added %s to the raw table. "
                    "History reads NULL for it -- added, never backfilled.",
                    feed.name, result["business_date"],
                    result["columns_added"])
            if result.get("columns_orphaned"):
                log.warning(
                    "CONTRACT CHANGE %s %s: %s is in the raw table and the "
                    "feed no longer declares it. Written as NULL, never "
                    "dropped; settle it deliberately.",
                    feed.name, result["business_date"],
                    result["columns_orphaned"])
            return result

        arrival = resolve_arrival()
        ready = normalize_task(arrival)
        ingested = ingest_task(ready)
        report_drift(ingested) >> record_snapshot(ingested)

    return _dag()


for _feed in feeds().values():
    globals()[f"ingest_{_feed.name}"] = build_feed_dag(_feed)
