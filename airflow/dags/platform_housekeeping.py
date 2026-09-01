"""Nightly platform housekeeping: maintenance then retention.

ORDER IS THE POINT. See docs/MAINTENANCE.md and docs/RETENTION.md.

    collect metrics
        -> compact / rewrite manifests / rewrite deletes   (maintenance)
        -> delete abandoned working branches
        -> expire published tags                           <-- tags pin files
        -> nessie gc sweep                                 <-- IDENTIFIES files
        -> deferred deletes past their window              <-- REMOVES them
        -> delete expired rows (partition-level)
        -> expire_snapshots                                <-- dead under Nessie
        -> remove_orphan_files (older_than >= 3 days)
        -> sweep orphan table prefixes

Expiring snapshots before expiring tags reclaims nothing while appearing to
succeed. Note also that identification and removal are two different steps a
deferral window apart: the sweep records what is collectable, and
a later run deletes it. So reclamation is lagged by design, and `storage_report`
cannot assert that bytes fell tonight — see its docstring.

CONCURRENCY: every task here that touches table files holds the SAME
`lakehouse_write` pool as ingest and the dbt builds. That one shared slot is
what prevents `remove_orphan_files` running underneath an in-flight write, which
corrupts the table. It must stay ONE pool -- a second one-slot pool does not
exclude anything. See docs/DECISIONS.md#one-shared-write-pool

Strictly, only `remove_orphan_files` corrupts on a concurrent write; compaction
and snapshot expiry are safe under Iceberg's optimistic concurrency. Splitting
just the orphan sweep into its own task would block far less, but it means
restructuring retention.run()'s ordering and is only correct if that
concurrency claim holds in this exact Iceberg/Nessie setup -- untested. Do it
only if maintenance is observed to actually delay arrivals.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from reporting_platform.common.context import managed_tables

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task  # type: ignore

# 3x the base delay (30s locally, the longest of the three DAGs): these are the
# slow destructive tasks, and retrying one straight away is more likely to
# collide with whatever it collided with. See docs/DECISIONS.md#retry-delay
RETRY_DELAY = timedelta(
    seconds=3 * int(os.environ.get("AIRFLOW_RETRY_DELAY_SECONDS", "10")))

DEFAULT_ARGS = {"owner": "data-platform", "retries": 1,
                "retry_delay": RETRY_DELAY}



def _spark_subprocess(*args: str) -> dict:
    """Run a Spark-using operation in a child process and parse its JSON.

    Identical reasoning to feed_ingest.py: an in-process SparkSession keeps the
    JVM's non-daemon threads alive after the task callable returns, heartbeats
    stop, and the scheduler reaps the task as a zombie even though the work
    succeeded. Both tasks below run Spark, so both need it.
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
            f"spark task {args!r} failed (exit {proc.returncode})\n{tail}")
    for line in reversed((proc.stdout or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError(f"spark task {args!r} produced no JSON")


@dag(
    dag_id="platform_housekeeping",
    description="Iceberg maintenance + retention enforcement",
    # After the last publication of the day. Deliberately not overlapping the
    # arrival window: feeds land through the day, maintenance does not.
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reporting-platform", "platform", "housekeeping"],
    params={"dry_run": False, "force_compaction": False,
            "fail_on_gap": False},
)
def platform_housekeeping():

    @task
    def collect_metrics() -> dict:
        """Emit metrics whether or not we act, so degradation shows as a trend."""
        specs = [f"{t}:{layer}" for t, layer in managed_tables()]
        return _spark_subprocess("maintain-metrics", *specs)

    @task(pool="lakehouse_write")
    def maintain(**context) -> dict:
        p = context["params"]
        specs = [f"{t}:{layer}" for t, layer in managed_tables()]
        if p.get("dry_run"):
            return _spark_subprocess("maintain-metrics", *specs)
        force = "force" if p.get("force_compaction") else "noforce"
        return _spark_subprocess("maintain", force, *specs)

    @task(pool="lakehouse_write")
    def enforce_retention(**context) -> dict:
        """Branches -> tags -> gc -> rows -> snapshots -> orphan prefixes."""
        specs = [f"{t}:{layer}" for t, layer in managed_tables()]
        mode = "dry" if context["params"].get("dry_run") else "real"
        return _spark_subprocess("retention", mode, *specs)

    @task
    def completeness_check(**context) -> dict:
        """Business dates a feed is missing that other feeds prove existed.

        WHY THIS EXISTS. `dbt source freshness` measures the age of the newest
        `_ingest_ts`, so it catches a feed that has STOPPED arriving and is
        blind to a hole in the middle of a history that later resumed — the
        seed's absent counterparty day raises no freshness warning at all. A
        late feed is visibly missing; a gap is a report that runs, returns
        numbers, and is quietly wrong for one date forever.

        WARNS BY DEFAULT RATHER THAN FAILING, deliberately. A red run here
        would be indistinguishable, to the watchdog, from housekeeping being
        down — and the watchdog exists to tell whether reclamation is
        happening, not whether an upstream missed a Tuesday. Conflating the
        two would make the alert that matters less trustworthy. The default
        seed also ships a deliberate gap, so failing would mean permanently
        red. Set `fail_on_gap` where a gap really should stop the night.

        It runs independently of the maintenance chain: a data gap must not
        block reclamation, and reclamation failing must not hide a data gap.
        """
        import logging

        from airflow.exceptions import AirflowException

        log = logging.getLogger("airflow.task")
        report = _spark_subprocess("completeness")
        for f in report.get("feeds", []):
            if f.get("missing"):
                log.warning(
                    "feed %s (%s) is missing %d period(s) that other feeds "
                    "delivered on: %s", f["feed"], f.get("cadence"),
                    len(f["missing"]), ", ".join(f["missing"]))
        if report.get("total_missing") and context["params"].get("fail_on_gap"):
            raise AirflowException(
                f"{report['total_missing']} business date(s) are missing; "
                "see the per-feed detail above")
        return report

    @task
    def storage_report(before: dict, after: dict) -> dict:
        """Prove the reclamation actually happened.

        WHAT THIS CAN AND CANNOT ASSERT ON. The obvious tripwire -- "dates
        expired but no files deleted" -- was wired to the per-table
        `files_deleted` counter, which `apply_table_retention` only ever sets
        from `expire_snapshots`, and it was established that
        `expire_snapshots` can never run under Nessie (`gc.enabled=false`), so
        that counter is permanently absent and the assertion was unsatisfiable
        by construction: every real run failed the moment any date expired
.

        Reclamation here comes from three catalog-wide steps, not per-table
        ones: Nessie GC's sweep, the deferred-delete pass that executes what
        earlier sweeps recorded, and the orphan-prefix sweep.

        WHAT D4 CHANGED, AND WHAT IT DID NOT. Deferred deletes are automated
        now, so "expired dates, zero bytes reclaimed" is no longer the
        permanent state it was. It is still not a failure condition, and
        asserting on it would repeat that defect's mistake in a new costume:
        reclamation is deliberately LAGGED by `deferred_delete_after_hours`,
        so tonight's run deletes what a sweep several nights ago identified.
        A night on which nothing had become collectable back then reclaims
        nothing tonight, correctly.

        The satisfiable assertions are about the machinery, not the bytes:
        GC must have swept, and the deferred-delete pass must have run without
        error. Whether reclamation is actually keeping up is a question about
        a backlog over time, which is the watchdog's `deferred_backlog` check
        -- it can see files sitting past their window, which is the thing that
        genuinely means nothing is being reclaimed.
        """
        import logging

        from airflow.exceptions import AirflowException

        log = logging.getLogger("airflow.task")
        # Read the flag off the retention report, not params: the report
        # records what the subprocess actually did, and params arrive from the
        # CLI as the string "True". A dry run deletes nothing by design, so
        # none of the assertions below apply to it.
        dry = bool(after.get("dry_run"))

        gc = after.get("nessie_gc") or {}
        gc_swept = "sweep" in gc
        gc_deferred = bool(gc.get("deferred"))
        orphans = after.get("orphan_prefixes") or {}
        deferred = after.get("deferred_deletes") or {}

        before_mb = {t["table"]: t.get("metrics", {}).get("total_size_mb", 0)
                     for t in before.get("tables", [])}
        expiring_tables, rows = [], []
        for t in after.get("tables", []):
            name = t.get("table")
            expiring = t.get("expiring_dates", 0)
            rows.append({"table": name,
                         "size_mb_before": before_mb.get(name, 0),
                         "dates_expired": expiring,
                         "retained_dates": t.get("retained_dates"),
                         "oldest_retained": t.get("oldest_retained")})
            if expiring:
                expiring_tables.append(name)

        # Everything that actually removed an object this run. The
        # deferred-delete pass is the one that reclaims under Nessie; the
        # per-table counter is here for a non-Nessie catalog and is always 0
        # under this one.
        objects_deleted = (
            sum(t.get("files_deleted") or 0 for t in after.get("tables", []))
            + (orphans.get("objects_deleted") or 0)
            + (deferred.get("files_deleted") or 0)
        )
        summary = {"dry_run": dry, "summary": rows,
                   "expiring_tables": expiring_tables,
                   "objects_deleted": objects_deleted,
                   "gc_swept": gc_swept, "gc_deferred": gc_deferred,
                   "deferred_deleted": deferred.get("files_deleted"),
                   "deferred_pending": deferred.get("files_pending")}

        # A MACHINERY assertion, not a deletion one, which is why it comes
        # before the dry-run return: retention swallows a deferred-delete
        # failure so the chain completes, so nothing else would notice this
        # breaking. See docs/DECISIONS.md#gc-lag-and-assertions
        if deferred.get("error"):
            raise AirflowException(
                "the deferred-delete pass failed: " + str(deferred["error"])
                + ". This is the ONLY thing that reclaims storage unattended "
                ". Left broken, the "
                "warehouse grows without bound."
            )

        if dry:
            log.info("dry_run: reclamation assertions skipped — nothing is "
                     "deleted on a dry run, so zero reclaimed is correct")
            return summary

        if not expiring_tables:
            log.info("no dates expired; nothing to reclaim")
            return summary

        if not gc_swept:
            raise AirflowException(
                "dates expired on " + ", ".join(expiring_tables)
                + f" but Nessie GC did not sweep (nessie_gc={gc}). Under "
                "Nessie GC is the ONLY thing that reclaims — expire_snapshots "
                "cannot. Storage cannot fall in this state."
            )

        if objects_deleted:
            log.info("reclaimed %d objects across %d expiring tables",
                     objects_deleted, len(expiring_tables))
            return summary

        if gc_deferred:
            # Correct and expected: reclamation is lagged by the deferral
            # window, so a night whose eligible live-sets held nothing removes
            # nothing. INFO, not WARNING -- a standing warning that means
            # "working" trains people to ignore it.
            # See docs/DECISIONS.md#gc-lag-and-assertions
            log.info(
                "nothing removed this run: reclamation is deferred by %sh, so "
                "tonight's pass actions sweeps from that long ago and those "
                "held no files. %s files are queued inside the window. A "
                "backlog that outlives the window is what would be wrong, and "
                "the watchdog's deferred_backlog check alerts on exactly that.",
                deferred.get("window_hours"), deferred.get("files_pending"))
            return summary

        raise AirflowException(
            "expired dates reclaimed nothing on: " + ", ".join(expiring_tables)
            + ". GC swept with deferral OFF, so files should have been "
            "deleted — something is still pinning them, almost always a "
            "published tag or an open branch. Do not ignore this."
        )

    metrics = collect_metrics()
    maintained = maintain()
    retained = enforce_retention()
    metrics >> maintained >> retained
    storage_report(metrics, retained)
    # No dependency on the chain above, on purpose: a data gap must not block
    # reclamation, and reclamation failing must not hide a data gap.
    completeness_check()


platform_housekeeping()
