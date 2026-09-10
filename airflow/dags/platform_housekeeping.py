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
succeed. Identification and removal are also two steps a deferral window
apart, so reclamation is lagged by design and `storage_report` cannot assert
that bytes fell tonight.

CONCURRENCY: every task here that touches table files holds the SAME
`lakehouse_write` pool as ingest and the dbt builds. That one shared slot is
what prevents `remove_orphan_files` running underneath an in-flight write,
which corrupts the table. It must stay ONE pool -- a second one-slot pool does
not exclude anything. See docs/DECISIONS.md#one-shared-write-pool

Strictly, only `remove_orphan_files` corrupts on a concurrent write; compaction
and snapshot expiry are safe under Iceberg's optimistic concurrency. Splitting
just the orphan sweep out would block far less, but it means restructuring
retention.run()'s ordering and is only correct if that concurrency claim holds
in this exact Iceberg/Nessie setup -- untested.
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
            "fail_on_gap": False, "fail_on_late": False},
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
    def reproducibility_check(**context) -> dict:
        """Can a published run still be read at its own pin? REQ-702.

        AFTER THE MAINTENANCE CHAIN, and that ordering is the whole test.
        `maintain` rewrites data files; `enforce_retention` then expires
        published tags and COB dates, runs Nessie GC and executes the deferred
        deletes an earlier sweep queued. Every one can break a pin while
        leaving a catalog that looks healthy. Running the check first would
        exercise yesterday's state and pass on the night the damage was done.

        FAILS THE RUN, unlike `completeness_check` beside it, and the
        difference is what the red means. A completeness gap is an upstream
        missing a Tuesday -- real, but not the platform's own doing, and a red
        run there would be indistinguishable to the watchdog from housekeeping
        being down. A pin that no longer resolves IS the platform destroying
        its own evidence, in the step that just ran, and it is unrecoverable.

        `not_yet_meaningful` DOES NOT FAIL, and it is no longer an age. The
        check selects the oldest pin holding a data file `main` no longer
        references, because a pin whose every file `main` keeps alive would
        read successfully whether pinning worked or not. This used to be
        approximated by the pin being older than `recent_partition_days`,
        which is backwards -- compaction is scoped to the RECENT partitions,
        so a pin ages out of that window rather than into it.
        """
        import logging

        from airflow.exceptions import AirflowException

        log = logging.getLogger("airflow.task")
        report = _spark_subprocess("reproducibility")
        status = report.get("status")
        if status == "not_yet_meaningful":
            log.info("reproducibility: %s", report.get("detail"))
            return report
        if status == "no_published_tags":
            log.info("reproducibility: nothing has been published yet")
            return report
        if not report.get("ok", False):
            raise AirflowException(
                f"REPRODUCIBILITY BROKEN at {report.get('tag')}: "
                f"{len(report.get('unreadable', []))} table(s) can no longer "
                f"be read at a published pin. The maintenance chain that just "
                f"ran destroyed evidence a published run depends on. Do not "
                f"ignore this — every further night makes it less diagnosable."
            )
        # The divergence count is the half that says the green MEANS something:
        # it is how many data files this pin keeps alive that main does not.
        # A green with zero there would be the old age gate's meaningless one.
        div = report.get("divergence") or {}
        log.info("reproducibility: %s reproduced, %d table(s) read, %d absent "
                 "at that pin; selected after scanning %s pin(s), holding %s "
                 "data file(s) main no longer references", report.get("tag"),
                 len(report.get("tables", [])), len(report.get("absent", [])),
                 div.get("pins_scanned"), div.get("exclusive_files"))
        return report

    @task(pool="lakehouse_write")
    def migrate_raw_schema(**context) -> dict:
        """Give every raw table the current provenance columns. Idempotent.

        FIRST IN THE CHAIN, and it is not maintenance -- it is the guard
        against a lazy migration. `ingest` adds a missing provenance column on
        the branch it is already writing, for the one feed it is ingesting, so
        a feed that has not delivered since the column was added keeps the old
        schema. Every prepared model selects those columns, so the next build
        fails for every feed that has not happened to deliver -- observed, on
        three of four models, which is why this task exists.

        Costs one schema read per feed and commits nothing when they are all
        current, which is every night after the first.
        """
        import logging

        log = logging.getLogger("airflow.task")
        mode = "dry" if context["params"].get("dry_run") else "real"
        report = _spark_subprocess("migrate-raw", mode)
        if report.get("migrated"):
            log.warning("migrated %d raw table(s) to the current provenance "
                        "schema: %s", report["migrated"],
                        [f["feed"] for f in report["feeds"]
                         if f["status"] == "migrated"])
        return report

    @task
    def registry_reconcile(**context) -> dict:
        """Register every delivery object storage holds that has no row yet.

        NO SPARK, so it runs in the task process rather than through
        `scripts/_spark_task.py` -- boto3, json and psycopg2 only.

        BEFORE the evidence check below and after retention, and both halves
        matter. After, because the landing sweep may have removed deliveries
        and this must describe what is left; before, because a delivery with no
        registry row looks to the evidence check exactly like a pinned date
        whose evidence is gone.

        Ingest already registers each delivery inline as it normalizes it, so
        on an ordinary night this finds nothing. It exists for the nights it
        does: a registry write that failed while Postgres was restarting, or a
        file an upstream agent pushed straight into the bucket.

        `dry_run` NARROWS THIS TASK, IT DOES NOT SKIP IT, and the distinction
        is which side effect matters. Registry rows are an index: the write is
        an upsert, nothing that deletes reads it, and skipping the poll is
        what the rule above forbids. Manifest objects are different -- they are
        the input to the `ready/` sweep two tasks back, so a "dry" run that
        creates one changes what the real run does. Observed: a run triggered
        with `{"dry_run": true}` wrote 116 manifests and forecast a sweep of 42
        that then removed 157.
        """
        import logging

        from reporting_platform.registry.deliveries import reconcile_all

        log = logging.getLogger("airflow.task")
        dry = bool(context["params"].get("dry_run"))
        report = reconcile_all(normalize_first=not dry)
        log.info("registry: %d newly registered, %d failed",
                 report["registered"], report["failed"])
        if dry:
            log.info("dry_run: no manifests written; %d landed object(s) have "
                     "none and would be normalized by a real run",
                     report.get("would_normalize", 0))
        return report

    @task
    def evidence_check(**context) -> dict:
        """Does every published pin still have its deliveries? REQ-602.

        The per-delivery half of the interlock.
        `retention.check_reproducibility_window` compares two windows and
        refuses the sweep if they disagree; that runs first and cannot see a
        single delivery missing inside a coherent window. This can, because the
        registry says what was received and object storage says what is left.

        FAILS THE RUN when a pinned delivery's landing object is gone, for the
        same reason `reproducibility_check` fails: the platform has destroyed
        its own evidence, and it does not get better by itself.

        Resolves each pin's input set from its RUN RECORD where there is one
        (exact) and from the tag's COB date where there is not (approximate,
        and generous only in the direction of missing something). The counts
        are logged separately.

        WARNS, and does not fail, when a pinned tag has no registered
        deliveries at all. That is what an unreconciled registry looks like --
        indistinguishable from the real thing on the evidence available here --
        and failing on it would make the first red the one everybody ignores.
        """
        import logging

        from airflow.exceptions import AirflowException

        from reporting_platform.monitoring import evidence

        log = logging.getLogger("airflow.task")
        report = evidence.run()
        if report.get("status") == "no_published_tags":
            log.info("evidence: nothing has been published yet")
            return report
        if not report.get("ok", False):
            raise AirflowException(
                f"EVIDENCE MISSING behind a published pin: "
                f"{len(report.get('missing_evidence', []))} delivery(ies) "
                f"registered as received are no longer in landing/. The pin "
                f"still resolves, so the tables read -- what the upstream "
                f"actually sent does not. REQ-602.")
        if report.get("unregistered_inputs"):
            log.warning("evidence: %d input delivery(ies) named by a run are "
                        "not in the registry: %s. reconcile is the rebuild "
                        "path.", len(report["unregistered_inputs"]),
                        report["unregistered_inputs"][:5])
            return report
        if report.get("unbacked_tags"):
            log.warning("evidence: %d pinned tag(s) have no input "
                        "deliveries: %s", len(report["unbacked_tags"]),
                        report["unbacked_tags"][:5])
            return report
        # EXACT vs APPROXIMATE is worth saying out loud every night: a green
        # from a tag resolved through its run record means the deliveries that
        # run actually read; a green from a tag resolved by COB date means the
        # deliveries that happened to arrive that day. The second is weaker,
        # and a log line that did not distinguish them would let it pass for
        # the stronger.
        log.info("evidence: %d delivery(ies) behind %d pinned tag(s), all "
                 "still landed (%d exact from run records, %d approximated "
                 "from the tag's COB date)",
                 report.get("deliveries_checked"), report.get("tags_checked"),
                 report.get("exact"), report.get("approximate"))
        return report

    @task
    def completeness_check(**context) -> dict:
        """COB dates a feed is missing that other feeds prove existed.

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
        # A FEED THAT COULD NOT BE READ IS NOT A FEED WITH NO GAPS. It carries
        # no missing periods, so the loop above says nothing about it and
        # `total_missing` counts nothing for it: without this the check goes
        # green on a table it never opened.
        unreadable = report.get("unreadable_feeds") or []
        for name in unreadable:
            log.error("feed %s was NOT CHECKED: its raw table could not be "
                      "read, so nothing here describes its completeness", name)
        if (report.get("total_missing") or unreadable) \
                and context["params"].get("fail_on_gap"):
            raise AirflowException(
                f"{report.get('total_missing', 0)} COB date(s) are missing and "
                f"{len(unreadable)} feed(s) could not be read; "
                "see the per-feed detail above")
        return report

    @task
    def lateness_check(**context) -> dict:
        """Deliveries that arrived after the time the feed promised. REQ-201.

        BESIDE `completeness_check`, NOT INSIDE IT, and the split is the
        point: that one asks which COB dates are MISSING, this asks which
        of the deliveries that arrived were late. A date with nothing
        registered is a gap and is not judged here, so an outage is reported
        once rather than by both checks in different words.

        WARNS RATHER THAN FAILING, for `completeness_check`'s reason exactly:
        a red here would be indistinguishable, to the watchdog, from
        housekeeping being down, and an upstream missing a deadline is not
        reclamation failing. `fail_on_late` is there for a platform where it
        should stop the night.

        No Spark: the arrival time is `registry.delivery.received_at`, so this
        runs in the task process rather than through `_spark_task`.
        """
        import logging

        from airflow.exceptions import AirflowException

        from reporting_platform.monitoring import lateness

        log = logging.getLogger("airflow.task")
        report = lateness.run()
        for f in report.get("feeds", []):
            for entry in f.get("late", []):
                log.warning("feed %s: %s arrived %.1fh after its %s deadline",
                            f["feed"], entry["cob_date"],
                            entry["hours_late"], f["expected_by"])
        if report.get("total_late") and context["params"].get("fail_on_late"):
            raise AirflowException(
                f"{report['total_late']} delivery(ies) arrived late; see the "
                "per-feed detail above")
        return report

    @task
    def storage_report(before: dict, after: dict) -> dict:
        """Prove the reclamation actually happened.

        WHAT THIS CAN AND CANNOT ASSERT ON. The obvious tripwire -- "dates
        expired but no files deleted" -- was wired to the per-table
        `files_deleted` counter, which `apply_table_retention` only ever sets
        from `expire_snapshots`, which can never run under Nessie
        (`gc.enabled=false`). So that counter is permanently absent and the
        assertion was unsatisfiable by construction: every real run failed the
        moment any date expired.

        Reclamation here comes from three catalog-wide steps, not per-table
        ones: Nessie GC's sweep, the deferred-delete pass that executes what
        earlier sweeps recorded, and the orphan-prefix sweep.

        Deferred deletes are automated now, so "expired dates, zero bytes
        reclaimed" is no longer the permanent state it was. It is still not a
        failure condition, and asserting on it would repeat that defect's
        mistake in a new costume: reclamation is deliberately LAGGED by
        `deferred_delete_after_hours`, so tonight's run deletes what a sweep
        several nights ago identified.

        The satisfiable assertions are about the machinery, not the bytes: GC
        must have swept, and the deferred-delete pass must have run without
        error. Whether reclamation is keeping up is a question about a backlog
        over time, which is the watchdog's `deferred_backlog` check.
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

    # BEFORE the maintenance chain, and before anything reads a raw table:
    # a raw table missing a provenance column breaks every prepared build,
    # not just its own feed's model.
    migrated = migrate_raw_schema()
    metrics = collect_metrics()
    maintained = maintain()
    retained = enforce_retention()
    migrated >> metrics >> maintained >> retained
    storage_report(metrics, retained)
    # AFTER retention, and depending on it: this asks whether the chain that
    # just ran destroyed a published pin, so it has to observe the state that
    # chain left behind.
    retained >> reproducibility_check()
    # Same reasoning, different question: reproducibility asks whether the
    # TABLES can still be read at the pin, this asks whether the DELIVERIES
    # behind it are still in landing. Both observe the state the retention
    # chain left, and the registry must be current before the second can tell
    # "evidence gone" from "never recorded".
    retained >> registry_reconcile() >> evidence_check()
    # No dependency on the chain above, on purpose: a data gap must not block
    # reclamation, and reclamation failing must not hide a data gap. The
    # lateness check is beside it and independent of it for the same reason,
    # and of it: a feed can be complete and late, or on time and full of holes.
    completeness_check()
    lateness_check()


platform_housekeeping()
