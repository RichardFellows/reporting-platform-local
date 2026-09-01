"""External watchdog for platform housekeeping.

WHY THIS EXISTS, AND WHY IT IS NOT AN AIRFLOW TASK.

`storage_report` is the tripwire for reclamation not happening -- and it is a
task *inside* `platform_housekeeping`, the DAG whose absence it would need to
detect. A monitor inside the thing it monitors cannot report that thing being
down. Pause the DAG, break its import, leave a stale run wedging
`max_active_runs=1`, and the tripwire simply never fires. Nothing complains,
because the thing that would complain is the thing that stopped.

That is not hypothetical here. A storage leak that stranded
table directories permanently -- went unnoticed for exactly this reason, and
the stale working branches found on first inspection existed because
housekeeping had never run at all and nothing said so.

So this process depends on **none** of Airflow's runtime. It reads:

  * the Airflow *metadata database* directly (Postgres), which is the source of
    truth for run state and stays up when the scheduler does not;
  * Nessie's REST API, for reference counts;
  * the object store, for warehouse size.

If the scheduler is dead, runs stop appearing and the freshness check fires. If
the DAG is paused or deleted, that is read straight off the `dag` table and
fires immediately. If the *database* is unreachable, that is itself an alert --
silence is never treated as health.

Deliberately not using Airflow's REST API: it requires the webserver, which is
another Airflow process that can be down for reasons unrelated to scheduling,
and its absence would then be indistinguishable from a real outage.

Deliberately not importing `airflow` at all. This runs in the same image for
convenience -- it is the image with the code and the drivers -- but in its own
container, started by its own compose service that does not depend on any
Airflow service. Importing the library would reintroduce the coupling this
exists to remove.

ALERTING. On a laptop the "alert channel" is the process exit code and stderr:
non-zero means at least one ALERT-severity finding. In OpenShift this is the
shape a CronJob or a Prometheus textfile exporter wants -- see
docs/OPENSHIFT-MAPPING.md. It writes each evaluation to a JSONL history file so
warehouse-size trend can be read across runs; a single sample cannot show a
trend, and the flatlining storage graph docs/MAINTENANCE.md warns about is
precisely a trend failure.

Usage:
    python -m reporting_platform.monitoring.watchdog             # one evaluation
    python -m reporting_platform.monitoring.watchdog --loop 300  # every 5 minutes
    python -m reporting_platform.monitoring.watchdog --json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("watchdog")

# The DAG whose health is the point of the exercise.
WATCHED_DAG = os.environ.get("REPORTING_WATCHDOG_DAG", "platform_housekeeping")

# How stale a last-success may get before it is an alert. The DAG is scheduled
# daily at 22:00, so 48h is two missed runs -- one missed run can be a laptop
# that was closed, two is a pattern.
MAX_AGE_HOURS = float(os.environ.get("REPORTING_WATCHDOG_MAX_AGE_HOURS", "48"))

# Working branches are created per ingest and per build and swept by retention.
# A rising count is the earliest visible symptom of housekeeping not running,
# and it is what Phase 0 found by hand.
MAX_WORKING_BRANCHES = int(os.environ.get("REPORTING_WATCHDOG_MAX_BRANCHES", "12"))

# A run non-terminal for longer than this is wedged, not working. Housekeeping
# tasks are minutes, not hours, so 6h is generous by an order of magnitude.
STALE_RUN_HOURS = float(os.environ.get("REPORTING_WATCHDOG_STALE_RUN_HOURS", "6"))

# How long the warehouse may sit at exactly the same byte count before that is
# worth remarking on. MUST exceed the reclamation cadence -- housekeeping is
# nightly, so anything under 24h flags a healthy platform between runs, which
# is precisely the bug this constant exists to prevent recurring.
FLAT_WAREHOUSE_HOURS = float(
    os.environ.get("REPORTING_WATCHDOG_FLAT_HOURS", "36"))

HISTORY_PATH = os.environ.get(
    "REPORTING_WATCHDOG_HISTORY", "/var/lib/reporting-watchdog/history.jsonl")

ALERT, WARN, OK = "ALERT", "WARN", "OK"


class Finding(dict):
    def __init__(self, check: str, severity: str, message: str, **detail):
        super().__init__(check=check, severity=severity, message=message,
                         **detail)


# ------------------------------------------------------------ metadata DB
def _pg_dsn() -> str:
    """Postgres DSN for the Airflow metadata DB, from Airflow's own setting.

    Parsed rather than reconstructed so there is exactly one place the
    connection is configured. `postgresql+psycopg2://` is SQLAlchemy's dialect
    form; psycopg2 wants plain `postgresql://`.
    """
    url = os.environ.get(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "postgresql+psycopg2://platform:platform@postgres:5432/airflow")
    return url.replace("postgresql+psycopg2://", "postgresql://", 1)


def check_orchestrator(now: datetime) -> tuple[list[Finding], dict]:
    """Is the watched DAG present, unpaused, and succeeding?

    Every branch below is an alert, including the ones that look like operator
    intent. A paused housekeeping DAG is indistinguishable, from the storage's
    point of view, from a broken one: nothing reclaims either way. If pausing
    is deliberate, silence the watchdog deliberately too.
    """
    import psycopg2

    facts: dict = {}
    try:
        conn = psycopg2.connect(_pg_dsn(), connect_timeout=10)
    except Exception as exc:
        # Unreachable metadata DB means we cannot say anything about the DAG.
        # Report that as an alert; an unanswerable question is not a pass.
        return [Finding("metadata_db", ALERT,
                        f"cannot reach the Airflow metadata DB: {exc}"[:300])], facts

    findings: list[Finding] = []
    try:
        with conn, conn.cursor() as cur:
            try:
                cur.execute(
                    "SELECT is_paused, is_active FROM dag WHERE dag_id = %s",
                    (WATCHED_DAG,))
            except psycopg2.errors.UndefinedTable:
                # THE STACK IS STILL COMING UP. This container deliberately has
                # no depends_on airflow-init -- a monitor that shares the
                # lifecycle of the thing it monitors cannot report that thing
                # being down -- so on a cold start it wins the race against
                # `airflow db migrate` and there is no `dag` table to read yet.
                #
                # That is not "housekeeping is broken", and calling it an ALERT
                # meant every single fresh clone opened with a red watchdog
                # before anything had had a chance to go wrong. A monitor that
                # cries wolf on first boot is a monitor people learn to skip,
                # which costs more than the check is worth. WARN, with a
                # message that says which of the two it is; it clears on the
                # next pass once airflow-init has finished.
                return [Finding(
                    "metadata_db", WARN,
                    "the Airflow metadata schema does not exist yet — "
                    "airflow-init has not finished `db migrate`. Normal for "
                    "the first minute of a cold start; if it persists, check "
                    "`docker compose logs airflow-init`.")], facts
            row = cur.fetchone()
            if row is None:
                findings.append(Finding(
                    "dag_present", ALERT,
                    f"{WATCHED_DAG} is not in the metadata DB at all — the DAG "
                    "file is missing or failed to import"))
                return findings, facts

            is_paused, is_active = row
            facts["is_paused"] = bool(is_paused)
            facts["is_active"] = bool(is_active)
            if is_paused:
                findings.append(Finding(
                    "dag_paused", ALERT,
                    f"{WATCHED_DAG} is PAUSED — it will not run, so nothing "
                    "maintains or reclaims. Retention, GC and the orphan "
                    "sweep are all stopped."))
            if is_active is False:
                findings.append(Finding(
                    "dag_active", ALERT,
                    f"{WATCHED_DAG} is marked inactive — the scheduler is no "
                    "longer parsing its file"))

            cur.execute(
                "SELECT run_id, end_date FROM dag_run "
                "WHERE dag_id = %s AND state = 'success' "
                "ORDER BY end_date DESC NULLS LAST LIMIT 1",
                (WATCHED_DAG,))
            last = cur.fetchone()
            if last is None or last[1] is None:
                findings.append(Finding(
                    "housekeeping_freshness", ALERT,
                    f"{WATCHED_DAG} has never completed successfully"))
            else:
                run_id, end = last
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                age_h = (now - end).total_seconds() / 3600
                facts["last_success_run_id"] = run_id
                facts["last_success_age_hours"] = round(age_h, 2)
                # The absolute instant, not just the age: check_deferred_backlog
                # needs to compare it against when each live-set became
                # eligible, and an age is only meaningful against the same
                # `now`.
                facts["last_success_at"] = end.isoformat()
                if age_h > MAX_AGE_HOURS:
                    findings.append(Finding(
                        "housekeeping_freshness", ALERT,
                        f"{WATCHED_DAG} last succeeded {age_h:.1f}h ago "
                        f"({run_id}), over the {MAX_AGE_HOURS:.0f}h limit",
                        age_hours=round(age_h, 2)))

            # A run stuck non-terminal holds max_active_runs=1 and makes the
            # scheduler spin on it, starving every other DAG. That failure
            # mode has bitten this stack repeatedly (CLAUDE.md), and from
            # inside Airflow it looks like nothing happening at all.
            # COALESCE, and it is the whole point of this query. A run that
            # never started has start_date NULL, and "queued with start=None"
            # is precisely the symptom CLAUDE.md tells you to look for -- so
            # keying on start_date alone made the check blind to the exact
            # failure it exists for. Worse, `ORDER BY start_date` sorts NULLs
            # last in Postgres, so such a run was not even the row examined
            # unless it was the only one, and when it was, `start_date is
            # None` skipped the check in silence.
            #
            # queued_at is when the scheduler put it in the queue, which is
            # the right clock for a run that never got further; execution_date
            # is a last resort and is NOT NULL by schema.
            cur.execute(
                "SELECT run_id, "
                "       COALESCE(start_date, queued_at, execution_date) "
                "FROM dag_run "
                "WHERE dag_id = %s AND state IN ('running', 'queued') "
                "ORDER BY COALESCE(start_date, queued_at, execution_date) "
                "LIMIT 1",
                (WATCHED_DAG,))
            stuck = cur.fetchone()
            if stuck and stuck[1] is not None:
                start = stuck[1]
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
                stuck_h = (now - start).total_seconds() / 3600
                if stuck_h > STALE_RUN_HOURS:
                    findings.append(Finding(
                        "stale_run", ALERT,
                        f"{WATCHED_DAG} run {stuck[0]} has been non-terminal "
                        f"for {stuck_h:.1f}h — it holds max_active_runs=1 and "
                        "blocks every later run",
                        age_hours=round(stuck_h, 2)))
    finally:
        conn.close()
    return findings, facts


# ------------------------------------------------------------------ Nessie
def check_nessie() -> tuple[list[Finding], dict]:
    """Reference counts. Rising working branches == housekeeping not sweeping."""
    from reporting_platform.common.context import Nessie

    try:
        refs = Nessie().list_references()
    except Exception as exc:
        return [Finding("nessie", ALERT,
                        f"cannot reach Nessie: {exc}"[:300])], {}

    working = [r["name"] for r in refs
               if r.get("type") == "BRANCH" and r.get("name") != "main"
               and r["name"].split("/", 1)[0] in ("ingest", "build")]
    facts = {"references": len(refs), "working_branches": len(working),
             "tags": sum(1 for r in refs if r.get("type") == "TAG")}
    findings = []
    if len(working) > MAX_WORKING_BRANCHES:
        findings.append(Finding(
            "working_branches", WARN,
            f"{len(working)} working branches are open (limit "
            f"{MAX_WORKING_BRANCHES}). A live branch keeps its content "
            "reachable, so Nessie GC cannot collect it — this suppresses "
            "reclamation as well as being untidy.",
            branches=working[:20]))
    return findings, facts


# --------------------------------------------------------------- warehouse
def check_warehouse(history: list[dict], now: datetime) -> tuple[list[Finding], dict]:
    """Warehouse size, and whether it has moved at all across recent runs.

    THE WINDOW IS WALL-CLOCK, NOT SAMPLES, and the current sample is part of it.
    A sample-counted window does not match the nightly cadence of the thing that
    clears it. See docs/DECISIONS.md#watchdog-wall-clock-window

    Stays a WARN, because flat is legitimate: a quiet weekend produces no
    garbage. The condition that genuinely means reclamation is broken is
    `check_deferred_backlog` below, and that one alerts.
    """
    try:
        from reporting_platform.retention.orphan_storage import warehouse_table_prefixes

        prefixes = warehouse_table_prefixes()
    except Exception as exc:
        return [Finding("warehouse", ALERT,
                        f"cannot read the warehouse: {exc}"[:300])], {}

    total = sum(r["bytes"] for r in prefixes.values())
    facts = {"warehouse_bytes": total,
             "warehouse_mb": round(total / 1048576, 2),
             "table_prefixes": len(prefixes)}

    cutoff = now.timestamp() - FLAT_WAREHOUSE_HOURS * 3600
    sizes = [total]
    for h in history:
        b = h.get("warehouse_bytes")
        if not isinstance(b, int):
            continue
        try:
            ts = datetime.fromisoformat(h["checked_at"]).timestamp()
        except Exception:
            continue
        if ts >= cutoff:
            sizes.append(b)

    findings = []
    # `len(sizes) - 1` because the current sample is not from history: the
    # window is only genuinely covered once history reaches back across it.
    covered = len(sizes) - 1 >= 2 and _spans(history, cutoff)
    if covered and max(sizes) == min(sizes):
        findings.append(Finding(
            "warehouse_trend", WARN,
            f"warehouse has been exactly {facts['warehouse_mb']} MB for "
            f"{FLAT_WAREHOUSE_HOURS:g}h across {len(sizes)} samples — nothing "
            "has been reclaimed in that time. Legitimate on a quiet platform "
            "with no garbage to collect; check `deferred_backlog` to tell the "
            "two apart.",
            samples=len(sizes), window_hours=FLAT_WAREHOUSE_HOURS))
    return findings, facts


def _spans(history: list[dict], cutoff: float) -> bool:
    """True if history reaches back past `cutoff`.

    Without this a watchdog restarted ten minutes ago -- empty history, two
    quick samples -- would declare the warehouse flat for 36 hours on the
    strength of ten minutes of evidence.
    """
    for h in history:
        try:
            if datetime.fromisoformat(h["checked_at"]).timestamp() < cutoff:
                return True
        except Exception:
            continue
    return False


# ------------------------------------------------------- deferred backlog
def check_deferred_backlog(
    now: datetime, last_success_at: str | None = None
) -> tuple[list[Finding], dict]:
    """Has a housekeeping run left GC-identified files undeleted past their window?

    This is the one assertion in this file that is both satisfiable and
    meaningful. Nessie GC's sweep records files rather than deleting them;
    `retention.deferred_deletes()` executes those records once they are older
    than `deferred_delete_after_hours`. If that pass stops working -- the JAR is
    gone, the GC database is unreachable, the step throws and is swallowed --
    the records pile up and the warehouse silently stops falling.

    ELIGIBLE IS NOT THE SAME AS OVERDUE, and conflating them makes this check
    fire almost continuously on a healthy platform.
    See docs/DECISIONS.md#watchdog-eligible-vs-overdue

    So the condition is not "time has passed". It is **a housekeeping run
    completed after these files became eligible, and they are still here** --
    which is the actual statement "the deferred-delete pass ran and did not
    work". Files that are eligible but have not yet met a run are `waiting`,
    which is the normal state for most of any given day.

    If housekeeping has never succeeded, or has not succeeded since anything
    became eligible, this check stays silent on purpose: "the job is not
    running" is `housekeeping_freshness`'s and `dag_paused`'s job. One check
    per failure, so the alert that fires tells you which one it is.

    Deliberately read straight out of the GC database rather than by shelling
    out to the nessie-gc JVM every five minutes: this is a monitor, it should
    be cheap, and it must not depend on the tooling whose absence is one of
    the faults it detects.
    """
    import psycopg2

    from reporting_platform.common.context import gc_window_hours, nessie_gc_config

    cfg = nessie_gc_config()
    if not cfg.get("enabled", False) or not cfg.get("defer_deletes", True):
        # Nothing defers, so a backlog cannot exist. Saying so beats a silent
        # pass that looks identical to the check never running.
        return [], {"deferred_backlog": "n/a (deferral off)"}

    window_h = gc_window_hours("deferred_delete_after_hours", 48)
    dsn = _gc_dsn(cfg)
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10)
    except Exception as exc:
        return [Finding("deferred_backlog", ALERT,
                        "cannot reach the Nessie GC database, so whether "
                        f"anything is being reclaimed is unknowable: {exc}"[:300])], {}

    try:
        with conn, conn.cursor() as cur:
            try:
                cur.execute(
                    "SELECT s.live_set_id, s.identify_started, COUNT(*) "
                    "FROM gc_live_sets s "
                    "JOIN gc_file_deletions d ON d.live_set_id = s.live_set_id "
                    "WHERE s.set_status = 'EXPIRY_SUCCESS' "
                    "GROUP BY s.live_set_id, s.identify_started"
                )
            except psycopg2.errors.UndefinedTable:
                # `nessie_gc` is created empty by scripts/init-postgres.sql;
                # the TABLES in it are created by the nessie-gc tool the first
                # time housekeeping runs. So on any stack where housekeeping
                # has never run, these tables are legitimately absent and the
                # honest answer is "nothing has been deferred yet" -- not an
                # alert. This fired on every fresh clone.
                #
                # But absent tables AFTER a successful housekeeping run is a
                # real fault: the GC step did not do what the DAG says it did.
                # So the same missing table is a fact or an alert depending on
                # whether anything should have created it yet, which is exactly
                # the distinction the rest of this check is built around --
                # eligible is not the same as overdue.
                if last_success_at is None:
                    return [], {"deferred_backlog":
                                "n/a (GC has never run; no tables yet)"}
                return [Finding(
                    "deferred_backlog", ALERT,
                    "the Nessie GC tables do not exist even though "
                    f"{WATCHED_DAG} last succeeded at {last_success_at} — the "
                    "GC step cannot have run, so nothing is being "
                    "reclaimed")], {}
            rows = cur.fetchall()
    finally:
        conn.close()

    swept_at = None
    if last_success_at:
        try:
            swept_at = datetime.fromisoformat(last_success_at)
            if swept_at.tzinfo is None:
                swept_at = swept_at.replace(tzinfo=timezone.utc)
        except ValueError:
            swept_at = None

    overdue_files = held_files = waiting_files = 0
    oldest_h = 0.0
    for _lsid, started, count in rows:
        # The GC schema stores naive timestamps; they are UTC.
        started = started.replace(tzinfo=timezone.utc)
        age_h = (now - started).total_seconds() / 3600
        if age_h <= window_h:
            # Inside the review window. Exactly what deferral is for.
            held_files += count
        elif swept_at is not None and swept_at > started + timedelta(hours=window_h):
            # Eligible, and a housekeeping run has COMPLETED since it became
            # eligible without clearing it. That is the pass failing.
            overdue_files += count
            oldest_h = max(oldest_h, age_h)
        else:
            # Eligible but no run has met it yet. The normal state for most of
            # the day, and not this check's problem: if runs have stopped
            # altogether, housekeeping_freshness says so.
            waiting_files += count

    facts = {"deferred_overdue_files": overdue_files,
             "deferred_held_files": held_files,
             "deferred_waiting_files": waiting_files,
             "deferred_window_hours": window_h}
    if not overdue_files:
        return [], facts
    return [Finding(
        "deferred_backlog", ALERT,
        f"{overdue_files} files became collectable over {window_h}h ago "
        f"(oldest {oldest_h:.1f}h) and are STILL present after a "
        f"{WATCHED_DAG} run completed at {swept_at:%Y-%m-%d %H:%M}Z. The run "
        "succeeded, so the deferred-delete pass inside it is failing rather "
        "than absent — check the retention task log for a swallowed error. "
        "Nothing is being reclaimed.",
        overdue_files=overdue_files, oldest_age_hours=round(oldest_h, 1),
        since_run_completed=swept_at.isoformat())], facts


def _gc_dsn(cfg: dict) -> str:
    """psycopg2 DSN from the JDBC URL in retention.yml, so it is configured once."""
    url = cfg["jdbc_url"].replace("jdbc:postgresql://", "", 1)
    return (f"postgresql://{cfg['jdbc_user']}:{cfg['jdbc_password']}@{url}")


# ------------------------------------------------------------------ driver
def _load_history(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
    except FileNotFoundError:
        return []
    except Exception as exc:            # a corrupt history must not stop checks
        log.warning("could not read history %s: %s", path, exc)
        return []


def _append_history(path: str, record: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:            # history is useful, not load-bearing
        log.warning("could not write history %s: %s", path, exc)


def evaluate(history_path: str = HISTORY_PATH) -> dict:
    now = datetime.now(timezone.utc)
    history = _load_history(history_path)

    findings: list[Finding] = []
    facts: dict = {}
    for check in (lambda: check_orchestrator(now),
                  check_nessie,
                  lambda: check_warehouse(history, now),
                  # ORDER MATTERS, and the lambda is what makes it work:
                  # `facts` is read when this is CALLED, after
                  # check_orchestrator has already put last_success_at in it.
                  # check_deferred_backlog needs to know whether a
                  # housekeeping run has completed since files became
                  # eligible -- without that it cannot tell "the pass failed"
                  # from "the pass has not run yet".
                  lambda: check_deferred_backlog(
                      now, facts.get("last_success_at"))):
        # One check failing outright must not hide the others. A watchdog that
        # dies on its first problem reports less the worse things get.
        try:
            found, got = check()
        except Exception as exc:
            log.exception("check raised")
            found, got = [Finding("watchdog", ALERT,
                                  f"check raised: {exc}"[:300])], {}
        findings.extend(found)
        facts.update(got)

    severity = (ALERT if any(f["severity"] == ALERT for f in findings)
                else WARN if findings else OK)
    record = {"checked_at": now.isoformat(), "severity": severity,
              "dag": WATCHED_DAG, **facts,
              "findings": [dict(f) for f in findings]}
    _append_history(history_path, record)
    return record


def _emit(record: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(record, default=str))
        return
    stream = sys.stderr if record["severity"] == ALERT else sys.stdout
    print(f"[{record['severity']}] {record['dag']} @ {record['checked_at']}",
          file=stream)
    for f in record["findings"]:
        print(f"  {f['severity']:5} {f['check']}: {f['message']}", file=stream)
    if not record["findings"]:
        age = record.get("last_success_age_hours")
        print(f"  OK    last success {age}h ago, "
              f"{record.get('working_branches')} working branches, "
              f"{record.get('warehouse_mb')} MB", file=stream)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--loop", type=int, default=0,
                   help="seconds between evaluations; 0 = evaluate once")
    p.add_argument("--json", action="store_true", help="emit JSON")
    p.add_argument("--history", default=HISTORY_PATH)
    a = p.parse_args(argv)

    if not a.loop:
        record = evaluate(a.history)
        _emit(record, a.json)
        return 1 if record["severity"] == ALERT else 0

    # Looping mode never exits on a finding: the watchdog going away because it
    # found something is the failure mode it exists to prevent.
    while True:
        _emit(evaluate(a.history), a.json)
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(a.loop)


if __name__ == "__main__":
    sys.exit(main())
