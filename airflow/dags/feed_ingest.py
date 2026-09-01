"""One ingest DAG per feed, generated from reporting_platform/config/feeds.yml.

Requirement being satisfied: *each feed must be processed as soon as it is
received* — there is no single batch run that processes all feeds together.

Adding a feed means adding a block to feeds.yml. No DAG file is edited.

Each DAG:
  wait_for_arrival  ->  ingest (Spark)  ->  publish (Nessie merge + tag)
and emits an Asset on completion, which is what triggers the dbt builds.
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
    """
    import json
    import subprocess
    import sys

    proc = subprocess.run(
        [sys.executable, "-m", "scripts._spark_task", *args],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        # Head of the last traceback as well as the tail: a Py4JJavaError's Java
        # stack pushes the exception MESSAGE off the front of a tail-only
        # budget. See docs/DECISIONS.md#log-tail-plus-head
        err = proc.stderr or ""
        cut = err.rfind("Traceback (most recent call last)")
        head = err[cut:cut + 2500] if cut >= 0 else ""
        tail = ((proc.stdout or "")[-1500:] + "\n" + head
                + "\n...\n" + err[-2000:])
        raise RuntimeError(
            f"spark task {args!r} failed (exit {proc.returncode})\n{tail}"
        )
    for line in reversed((proc.stdout or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError(
        f"spark task {args!r} produced no JSON:\n{proc.stdout[-1500:]}")


def build_feed_dag(feed):
    asset = Asset(feed.asset_uri)

    @dag(
        dag_id=f"ingest_{feed.name}",
        description=f"Ingest {feed.name}: {feed.description}",
        # No schedule. The DAG is started by the arrival sensor DAG or by an
        # object-created event; a cron would reintroduce the batch window we
        # are trying to remove.
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
            """Determine which object to ingest.

            Triggered runs carry the object key in dag_run.conf. A manual run
            with no conf falls back to the newest unprocessed object for the
            feed, which is what you want when re-running by hand.
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

        @task(task_id="record_publication")
        def record_publication(result: dict) -> dict:
            """Tag main so this publication is addressable for time travel.

            Tag retention is governed by retention.yml -> references.
            published_tags, because a tag pins every data file its commit
            referenced. See docs/RETENTION.md.
            """
            from datetime import date as _date

            from reporting_platform.common.context import Nessie, published_tag

            tag = published_tag(_date.fromisoformat(result["business_date"]),
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
            """
            if result.get("missing_columns") or result.get("extra_columns"):
                import logging
                logging.getLogger("airflow.task").warning(
                    "SCHEMA DRIFT %s %s: missing=%s extra=%s",
                    feed.name, result["business_date"],
                    result["missing_columns"], result["extra_columns"])
            return result

        arrival = resolve_arrival()
        ingested = ingest_task(arrival)
        report_drift(ingested) >> record_publication(ingested)

    return _dag()


for _feed in feeds().values():
    globals()[f"ingest_{_feed.name}"] = build_feed_dag(_feed)
