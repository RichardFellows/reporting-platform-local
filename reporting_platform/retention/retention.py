"""Retention enforcement.

Reproduces the legacy nightly partition-switch archive job (10 working days
plus 80 month-end dates) on Iceberg + Nessie.

The important thing this job encodes is ORDER. See docs/RETENTION.md:

    1. delete merged / abandoned working branches
    2. delete expired published/* tags        <-- tags pin files
    3. delete expired rows (partition-level)
    4. nessie gc                              <-- THE reclamation step here
    5. expire_snapshots / remove_orphan_files <-- SKIPPED under Nessie
    6. sweep orphan table prefixes            <-- what GC structurally cannot see
    7. sweep the landing prefix               <-- the evidence copy, flat 8y

Ordering matters: run GC before tags are expired and the tags still pin the
content, so it collects nothing while appearing to succeed. That is the failure
mode to watch for -- the storage graph flatlines and nobody notices for a
quarter.

A correction the docs got wrong for a long time: under a Nessie catalog,
`expire_snapshots` is NOT the step that reclaims storage. NessieCatalog sets
`gc.enabled=false` on every table, so Iceberg refuses to delete files at all --
correctly, because data files are shared across references and no single table
pointer knows what another branch still needs. Nessie GC is the only mechanism
that can decide safely. Steps 5 are kept, guarded, so the code still works
against a non-Nessie catalog.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

from reporting_platform.common.calendar_rules import expire_set, keep_set
from reporting_platform.common.context import (
    CATALOG, ENV, Nessie, all_snapshot_retention_days, gc_window_hours,
    maintenance_config, managed_tables, nessie_gc_config, reference_policy,
    retention_policy, spark_session,
)

log = logging.getLogger("retention")

HELPTEXT = ("every table the platform manages, from managed_tables() -- the "
            "same list platform_housekeeping uses")

TAG_RE = re.compile(r"^published/(?P<bd>\d{4}-\d{2}-\d{2})/(?P<run>.+)$")
WORKING_RE = re.compile(r"^(ingest|build)/")


# ----------------------------------------------------------- 1. branch hygiene
def _abandoned_after_hours(policy: dict) -> dict[str, int]:
    """Sweep window per working-branch prefix.

    One global window was serving two needs and serving the important one
    badly: a build failing on a Friday night was swept before Monday, which is
    the one case `keep_failed_branch` exists for. A scalar is still accepted
    and applied to every prefix.

    Windows longer than the shortest snapshot_retention_days put branch
    retention and snapshot retention in tension -- the branch outlives the
    snapshots its commits reference. Warn rather than refuse: unlike the GC
    cutoff interlock this costs storage, not correctness.
    """
    raw = policy["abandoned_after_hours"]
    prefixes = ("ingest", "build")
    if isinstance(raw, dict):
        missing = [p for p in prefixes if p not in raw]
        if missing:
            raise KeyError(
                f"retention.yml: working_branches.abandoned_after_hours has no "
                f"entry for {missing}. Every prefix WORKING_RE matches needs "
                f"one, or the branches it misses are never swept."
            )
        windows = {p: int(raw[p]) for p in prefixes}
    else:
        windows = {p: int(raw) for p in prefixes}

    longest_hold = max(windows.values())
    shortest_snapshot_h = min(all_snapshot_retention_days() or [0]) * 24
    if shortest_snapshot_h and longest_hold > shortest_snapshot_h:
        log.warning(
            "abandoned_after_hours %dh exceeds the shortest "
            "snapshot_retention_days (%dh). A working branch will outlive the "
            "snapshots its commits reference, pinning files GC would collect.",
            longest_hold, shortest_snapshot_h)
    return windows


def clean_working_branches(nessie: Nessie, dry_run: bool = False) -> list[str]:
    policy = reference_policy("working_branches")
    windows = _abandoned_after_hours(policy)
    now = datetime.now(timezone.utc)
    removed = []
    # fetch_all=True is REQUIRED: without it Nessie returns no metadata block,
    # so commitTime below is always None. That made the age check dead code and
    # this function deleted every working branch on sight -- including branches
    # an ingest or dbt build was actively writing to.
    for ref in nessie.list_references(fetch_all=True):
        if ref.get("type") != "BRANCH":
            continue
        m = WORKING_RE.match(ref["name"])
        # Anything outside ingest/ and build/ is not swept, which is what
        # makes the hold/ convention work: renaming a branch under
        # investigation to hold/<name> exempts it by construction, with no
        # special case here to forget or get wrong.
        if not m:
            continue
        cutoff = now - timedelta(hours=windows[m.group(1)])
        meta = ref.get("metadata") or {}

        # A branch with no commits of its own POINTS AT ITS BASE's commit, so
        # commitMetaOfHEAD describes main, not this branch, and the age below
        # would be main's age. On a quiet main -- a long weekend, a platform
        # that publishes weekly -- a branch opened seconds ago reads as days
        # old and the next sweep deletes it while a build is writing to it.
        # Nessie exposes no branch-creation timestamp, so the age is genuinely
        # unknowable; fall back to the same fail-safe as a missing commit time.
        # The cost is that an empty abandoned branch is never swept and keeps
        # pinning the commit it forked from; the watchdog's working-branch
        # count is what catches those accumulating.
        if meta.get("numCommitsAhead") == 0:
            log.info("working branch %s has no commits of its own; its age is "
                     "unknowable, leaving it", ref["name"])
            continue

        committed = (meta.get("commitMetaOfHEAD") or {}).get("commitTime")
        ts = None
        if committed:
            try:
                ts = datetime.fromisoformat(committed.replace("Z", "+00:00"))
            except ValueError:
                ts = None
        if ts is None:
            # Age unknown. Deleting a branch is destructive and unrecoverable,
            # so fail safe and leave it: a branch kept one night too long costs
            # storage, one deleted mid-write costs the run.
            log.warning("no commit time for %s; leaving it alone", ref["name"])
            continue
        age_h = (now - ts).total_seconds() / 3600
        # Log the decision, not just the deletions. With per-prefix windows
        # (D5) "why is that branch still there" is now a question with two
        # possible answers, and a log that only records removals cannot
        # distinguish "inside its window" from "the sweep never saw it".
        log.info("working branch %s: %.1fh old, %s window %dh -> %s",
                 ref["name"], age_h, m.group(1), windows[m.group(1)],
                 "keep" if ts > cutoff else "sweep")
        if ts > cutoff:
            continue  # still recent; may be an in-flight run
        if not dry_run:
            nessie.delete_reference(ref["name"])
        removed.append(ref["name"])
    log.info("working branches removed: %d", len(removed))
    return removed


# --------------------------------------------------------------- 2. tag expiry
def expire_tags(nessie: Nessie, dry_run: bool = False) -> list[str]:
    """Tag retention IS data retention — a tag pins every file its commit used."""
    policy = reference_policy("published_tags")
    # fetch_all=True for the same reason clean_working_branches needs it: the
    # commit time only appears in the metadata block, and without it "newest"
    # below silently degrades to a string sort.
    tags: dict[date, list[tuple[str, str]]] = {}
    for ref in nessie.list_references("published/", fetch_all=True):
        if ref.get("type") != "TAG":
            continue
        m = TAG_RE.match(ref["name"])
        if not m:
            continue
        bd = datetime.strptime(m.group("bd"), "%Y-%m-%d").date()
        meta = ref.get("metadata") or {}
        committed = (meta.get("commitMetaOfHEAD") or {}).get("commitTime") or ""
        tags.setdefault(bd, []).append((ref["name"], committed))

    if not tags:
        return []

    keep = keep_set(
        sorted(tags),
        keep_business_days=policy.get("keep_business_days", 0),
        keep_month_ends=policy.get("keep_month_ends", 0),
    )
    removed = []
    for bd, entries in sorted(tags.items()):
        if bd in keep:
            # Keep only the newest tag for a retained date; earlier reruns of
            # the same date pin files for no benefit.
            #
            # "Newest" must mean newest by COMMIT TIME. Sorting the tag names
            # instead ranks an arbitrary run_id suffix lexicographically, and
            # that deletes the wrong tag the moment two run_id formats coexist
            # -- observed doing exactly that: a real
            # `published/<date>/20260821T001056-904f03` was dropped in favour
            # of a `published/<date>/synthetic000` because '2' sorts before
            # 's'. The tag you actually care about is the one destroyed.
            # Secondary key on name keeps the outcome deterministic when two
            # tags point at the SAME commit -- which can happen if a date is
            # re-tagged without rebuilding. That case is immaterial for
            # retention: identical commit means identical pinned files, so
            # whichever survives reclaims exactly the same bytes. It is only
            # the audit trail's name that differs.
            if all(c for _, c in entries):
                ordered = [n for n, _ in sorted(entries, key=lambda e: (e[1], e[0]))]
            else:
                log.warning(
                    "%s: missing commit time on some tags; falling back to "
                    "name order, which may keep the wrong one", bd)
                ordered = sorted(n for n, _ in entries)
            for name in ordered[:-1]:
                if not dry_run:
                    nessie.delete_reference(name)
                removed.append(name)
        else:
            for name, _ in entries:
                if not dry_run:
                    nessie.delete_reference(name)
                removed.append(name)
    log.info("published tags removed: %d", len(removed))
    return removed


# ------------------------------------------------------------ 2b. nessie gc
NESSIE_GC_JAR = os.environ.get("NESSIE_GC_JAR", "/opt/platform/lib/nessie-gc.jar")

# Durations the cutoff interlock understands. A commit-count or ISO-instant
# cutoff cannot be compared against a retention window in days, so those are
# allowed through with a warning rather than silently trusted.
_ISO_DAYS_RE = re.compile(r"^P(?:(?P<weeks>\d+)W|(?P<days>\d+)D)$", re.IGNORECASE)


def _cutoff_days(policy: str) -> int | None:
    """Days represented by a cutoff policy, or None if not day-expressible."""
    if policy is None:
        return None
    p = str(policy).strip()
    if p.upper() == "NONE":
        # Everything live: collects nothing, so it can never be too short.
        return 10**6
    m = _ISO_DAYS_RE.match(p)
    if not m:
        return None
    if m.group("weeks"):
        return int(m.group("weeks")) * 7
    return int(m.group("days"))


def _gc_base() -> list[str]:
    if not os.path.exists(NESSIE_GC_JAR):
        raise FileNotFoundError(
            f"nessie-gc tool not found at {NESSIE_GC_JAR}. It is downloaded by "
            f"Dockerfile.airflow; rebuild the image, or set NESSIE_GC_JAR."
        )
    return ["java", "-jar", NESSIE_GC_JAR]


def _gc_jdbc(cfg: dict) -> list[str]:
    """Live-set storage. Every subcommand except `gc` itself needs it.

    `--jdbc-schema=CREATE_IF_NOT_EXISTS` is what makes `create-sql-schema`
    below idempotent; without it that command exits 1 with `relation
    "gc_live_sets" already exists`. On the other subcommands it is inert --
    notably it does NOT create the schema on `mark-live`, which was the first
    guess and was wrong.
    """
    return [
        "--jdbc-url", cfg["jdbc_url"],
        "--jdbc-user", cfg["jdbc_user"],
        "--jdbc-password", cfg["jdbc_password"],
        "--jdbc-schema=CREATE_IF_NOT_EXISTS",
    ]


_GC_SCHEMA_READY = False


def _ensure_gc_schema(cfg: dict) -> None:
    """Create the GC live-set tables if this database has never had them.

    WHY THIS EXISTS. `scripts/init-postgres.sql` creates the `nessie_gc`
    DATABASE but no tables, and the GC tool does not create them on demand --
    it has a dedicated `create-sql-schema` command that nothing in this repo
    ever ran. The tables then survive in the Postgres volume forever, which is
    why four sessions of GC work never noticed. Rebuilding from `docker
    compose down -v` to verify the quick-start put a genuinely empty database
    underneath it and the first mark-live died with:

        ERROR: relation "gc_live_sets" does not exist

    That is not a soft failure. `nessie_gc()` is deliberately not wrapped in a
    try/except, because Nessie GC is the ONLY thing that reclaims storage here
 and a silent skip would be worse than a red run -- so the first
    `platform_housekeeping` of every new install went red.

    Runs once per process and tolerates the already-created case; the
    `--jdbc-schema=CREATE_IF_NOT_EXISTS` in _gc_jdbc() is what makes the
    command return 0 rather than 1 on a database that already has the tables.
    """
    global _GC_SCHEMA_READY
    if _GC_SCHEMA_READY:
        return
    rc, out = _gc_exec(_gc_base() + ["create-sql-schema"] + _gc_jdbc(cfg),
                       "create-sql-schema")
    if rc != 0 and "already exists" not in out:
        raise RuntimeError(
            f"nessie-gc create-sql-schema failed (exit {rc})\n{out[-2000:]}")
    _GC_SCHEMA_READY = True


def _gc_fileio() -> list[str]:
    """FileIO settings for the phases that actually touch the object store.

    `sweep` and `deferred-deletes` delete files and need this; `mark-live`
    only walks the commit graph and does not.
    """
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    return [
        "-I", "io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "-I", f"s3.endpoint={endpoint}",
        "-I", f"s3.access-key-id={os.environ.get('AWS_ACCESS_KEY_ID', '')}",
        "-I", f"s3.secret-access-key={os.environ.get('AWS_SECRET_ACCESS_KEY', '')}",
        "-I", "s3.path-style-access=true",
    ]


def nessie_gc(dry_run: bool = False) -> dict:
    """Identify (and optionally sweep) content unreachable from any live ref.

    THE GAP THIS FILLS. Iceberg's `expire_snapshots` only knows about snapshots
    reachable from the single table pointer it is handed. Nessie holds many
    pointers -- every branch, every tag -- so a file can be unreferenced from
    `main` yet still live because some abandoned build branch's commit contains
    it. Nessie GC is the only thing that reasons across all references.
    `docs/MAINTENANCE.md` has documented this as step 3 of the nightly chain
    since the beginning; until now nothing implemented it.

    There is no server-side GC endpoint and no REST call for this: it runs via
    the external `nessie-gc` tool, baked into the image by Dockerfile.airflow.

    Safety model, which is the tool's own three-stage one:

      dry_run=True   -> `mark-live` only. Walks the commit graph and records a
                        live-content-set. Deletes nothing, ever.
      dry_run=False  -> `mark-live`, then `sweep`. With `defer_deletes: true`
                        (the default) the sweep RECORDS files to delete rather
                        than deleting them, so a human can review with
                        `nessie-gc list-deferred` and then run
                        `nessie-gc deferred-deletes`.

    So even a non-dry run removes nothing by default. That is deliberate: this
    is the one step in the retention chain with no undo.
    """
    cfg = nessie_gc_config()
    if not cfg.get("enabled", False):
        log.info("nessie gc: disabled in retention.yml, skipping")
        return {"skipped": "disabled"}

    cutoff = cfg.get("default_cutoff", "NONE")

    # INTERLOCK: a cutoff shorter than the longest snapshot retention would
    # collect files that still-valid snapshots reference -- breaking time
    # travel and the extracts snapshot_retention_days exists to protect. This
    # is a refuse-to-run condition, not a warning: the damage is unrecoverable.
    longest = max(all_snapshot_retention_days() or [0])
    days = _cutoff_days(cutoff)
    if days is None:
        log.warning(
            "nessie gc: cutoff %r is not expressed in days, so it cannot be "
            "checked against the longest snapshot_retention_days (%d). "
            "Proceeding on the operator's word.", cutoff, longest)
    elif days < longest:
        raise ValueError(
            f"nessie gc cutoff {cutoff!r} ({days}d) is shorter than the longest "
            f"snapshot_retention_days ({longest}d). GC would collect files that "
            f"surviving snapshots still reference. Raise default_cutoff to at "
            f"least P{longest}D, or lower snapshot_retention_days -- but do not "
            f"run GC in this state."
        )

    base = _gc_base()
    _ensure_gc_schema(cfg)
    jdbc = _gc_jdbc(cfg)
    # `--uri` is a mark-live option ONLY. `sweep` does not accept it and exits
    # 2 if given it -- it works from the stored live-set, not from Nessie.
    nessie_uri = ["--uri", os.environ.get("NESSIE_URI",
                                          "http://nessie:19120/api/v2")]
    fileio = _gc_fileio()

    result: dict = {"cutoff": cutoff, "dry_run": dry_run}

    mark = base + ["mark-live"] + nessie_uri + jdbc + ["--default-cutoff", str(cutoff)]
    out = _run_gc(mark, "mark-live")
    result["identify"] = out
    live_set = out.get("live_set_id")

    if dry_run:
        log.info("nessie gc: identify-only (dry run), live-set %s", live_set)
        return result

    if not live_set:
        raise RuntimeError("nessie gc: mark-live did not report a live-set id")

    sweep = base + ["sweep"] + jdbc + fileio + ["--live-set-id", live_set]
    if cfg.get("defer_deletes", True):
        sweep.append("--defer-deletes")
    result["sweep"] = _run_gc(sweep, "sweep")
    result["deferred"] = bool(cfg.get("defer_deletes", True))
    if result["deferred"]:
        log.info(
            "nessie gc: files recorded as DEFERRED deletes, nothing removed. "
            "Review with `nessie-gc list-deferred`, then `nessie-gc "
            "deferred-deletes` to actually delete.")
    return result


_LIVE_SET_RE = re.compile(r"live-content-set ID is ([0-9a-f-]{36})")


def _gc_exec(cmd: list[str], phase: str) -> tuple[int, str]:
    """Run the nessie-gc CLI, returning (exit code, combined output).

    The tool prints a logback bootstrap banner before anything useful, and to
    stdout, not stderr -- so callers must match on content, never slice by
    line position.
    """
    import subprocess

    log.info("nessie gc: running %s phase", phase)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _run_gc(cmd: list[str], phase: str) -> dict:
    """Invoke the nessie-gc CLI, surfacing its output on failure."""
    rc, combined = _gc_exec(cmd, phase)
    if rc != 0:
        raise RuntimeError(
            f"nessie-gc {phase} failed (exit {rc})\n"
            f"{combined[-4000:]}"
        )
    out: dict = {"phase": phase, "exit": rc}
    m = _LIVE_SET_RE.search(combined)
    if m:
        out["live_set_id"] = m.group(1)
    for marker in ("IDENTIFY_SUCCESS", "EXPIRY_SUCCESS", "SUCCESS"):
        if marker in combined:
            out["status"] = marker
            break
    return out


# ------------------------------------------- 2c. deferred deletes
# `nessie-gc list` prints a fixed-width table after its logback banner. Anchor
# on the shape of a row -- uuid, status, ISO timestamp -- rather than on column
# offsets or line numbers, both of which the banner and an Error column shift.
_LIVE_SET_ROW = re.compile(
    r"^\s*(?P<id>[0-9a-f]{8}-[0-9a-f-]{27})\s+"
    r"(?P<status>[A-Z_]+)\s+"
    r"(?P<created>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)",
    re.MULTILINE,
)
# A deferred-delete row from `list-deferred`. NOTE the path is printed as TWO
# columns -- the content's base location, then the file relative to it -- so a
# naive `s3://\S+` match captures only the directory and silently looks like a
# file that does not exist. Match both halves.
_DEFERRED_FILE_RE = re.compile(
    r"^\s*\S+\s+(?P<base>s3[an]?://\S+)\s+(?P<path>\S+)\s*$", re.MULTILINE)


def list_live_sets() -> list[dict]:
    """Every live-content-set the GC database knows about, newest first.

    Timestamps are requested in UTC explicitly: the CLI otherwise formats them
    in the JVM's default zone, which is the container's, which nothing in this
    stack guarantees.
    """
    cfg = nessie_gc_config()
    _ensure_gc_schema(cfg)
    cmd = _gc_base() + ["list"] + _gc_jdbc(cfg) + ["--time-zone=UTC"]
    rc, out = _gc_exec(cmd, "list")
    if rc != 0:
        raise RuntimeError(f"nessie-gc list failed (exit {rc})\n{out[-4000:]}")
    sets = []
    for m in _LIVE_SET_ROW.finditer(out):
        created = datetime.fromisoformat(m.group("created")).replace(
            tzinfo=timezone.utc)
        sets.append({"id": m.group("id"), "status": m.group("status"),
                     "created": created})
    return sorted(sets, key=lambda s: s["created"], reverse=True)


def _deferred_file_count(cfg: dict, live_set: str) -> int:
    cmd = (_gc_base() + ["list-deferred"] + _gc_jdbc(cfg)
           + ["--time-zone=UTC", "-l", live_set])
    rc, out = _gc_exec(cmd, "list-deferred")
    if rc != 0:
        raise RuntimeError(
            f"nessie-gc list-deferred failed (exit {rc})\n{out[-4000:]}")
    return len(_DEFERRED_FILE_RE.findall(out))


def deferred_deletes(dry_run: bool = False, after_hours: int | None = None) -> dict:
    """Execute the deferred deletes recorded by sweeps older than the window.

    WHY THIS MATTERS. `nessie_gc()` above runs `sweep
    --defer-deletes`, which RECORDS the files it would remove against the
    live-set and deletes nothing. Until this function existed, the second step
    was `nessie-gc deferred-deletes` typed by a human -- which meant, in
    practice, that nothing was ever reclaimed unattended. The warehouse only
    shrank when somebody remembered, and two separate guards built to detect
    reclamation failure (`storage_report`'s tripwire and the watchdog's
    warehouse trend) had to be held at warn-only because a flat warehouse was
    the correct, expected state.

    The deferral period is kept -- it is a genuine safety feature -- but it
    becomes a POLICY VALUE (`deferred_delete_after_hours`) instead of a manual
    step. A sweep never deletes what it just recorded; a later run does, once
    the window has passed. Review time is preserved and reclamation happens on
    its own.

    Note the `deferred-deletes` subcommand takes a live-set id and nothing
    else: there is no "--older-than" flag in the tool. The window is enforced
    here, by choosing which live-sets to hand it.

    THE RISK THE WINDOW CREATES. A file recorded as unreachable is deleted
    later, so anything that makes it reachable again in between -- reassigning
    a Nessie ref backwards, or branching from a pre-sweep commit -- is
    resurrecting content this pass will then delete. That is inherent in
    deferral and was true of the manual step too; a longer window widens it.
    Do not point a ref at an old hash while deferred deletes are outstanding.

    Live-sets are dropped once acted on (nothing is left to act on, and
    leaving them means re-running `deferred-deletes` against already-deleted
    files every night forever). Sets that only ever completed mark-live -- one
    per dry run -- are pruned on age alone.

    CRASHING BETWEEN THE DELETES AND THE DROP is survivable, and that is now
    tested rather than assumed. The live-set is left
    EXPIRY_SUCCESS and past its window, `list-deferred` still reports its
    files -- the tool lists what was RECORDED, not what is still present --
    and the next run hands it to `deferred-deletes` again. The tool exits 0 on
    files that are already gone, so the pass completes and drops the row.

    What that costs is counter accuracy: `files_deleted` is the count
    `list-deferred` reported, so a live-set actioned twice is counted twice
. The tool offers
    no "files removed" figure to read instead. Treat this number as "files
    actioned", and treat the watchdog's deferred_backlog check -- which reads
    the object store, not this counter -- as the authority on whether
    reclamation is really happening.
    """
    cfg = nessie_gc_config()
    if not cfg.get("enabled", False):
        return {"skipped": "gc disabled"}
    if not cfg.get("defer_deletes", True):
        # Nothing defers, so there is nothing to catch up on.
        return {"skipped": "defer_deletes is off; sweep deletes inline"}

    _ensure_gc_schema(cfg)
    window_h = (int(after_hours) if after_hours is not None
                else gc_window_hours("deferred_delete_after_hours", 48))
    prune_h = gc_window_hours("live_set_retention_hours", 168)
    now = datetime.now(timezone.utc)
    delete_before = now - timedelta(hours=window_h)
    prune_before = now - timedelta(hours=prune_h)

    result: dict = {"dry_run": dry_run, "window_hours": window_h,
                    "live_sets": [], "files_deleted": 0, "files_pending": 0,
                    "live_sets_pruned": [], "failed_live_sets": []}

    for s in list_live_sets():
        age_h = round((now - s["created"]).total_seconds() / 3600, 2)
        if s["status"] != "EXPIRY_SUCCESS":
            # mark-live only, or a failed phase. Prune the successful-identify
            # leftovers; leave failures alone, they are evidence.
            #
            # But REPORT the failures. Left merely un-pruned they accumulate
            # in silence, and a sweep that fails every night looks from the
            # outside exactly like a platform with nothing to collect — which
            # is the failure mode this whole subsystem keeps producing.
            if s["status"] not in ("IDENTIFY_SUCCESS", "IDENTIFY_IN_PROGRESS",
                                   "EXPIRY_IN_PROGRESS"):
                result["failed_live_sets"].append(
                    {"live_set": s["id"], "status": s["status"],
                     "age_hours": age_h})
            if s["status"] == "IDENTIFY_SUCCESS" and s["created"] < prune_before:
                if not dry_run:
                    _run_gc(_gc_base() + ["delete"] + _gc_jdbc(cfg)
                            + ["-l", s["id"]], "delete")
                result["live_sets_pruned"].append(s["id"])
            continue

        files = _deferred_file_count(cfg, s["id"])
        entry = {"live_set": s["id"], "created": s["created"].isoformat(),
                 "age_hours": age_h, "deferred_files": files}

        if s["created"] >= delete_before:
            # Inside the review window. This is the normal state of the sweep
            # this very run just recorded.
            entry["action"] = "held"
            result["files_pending"] += files
        elif not files:
            entry["action"] = "empty, dropped"
            if not dry_run:
                _run_gc(_gc_base() + ["delete"] + _gc_jdbc(cfg)
                        + ["-l", s["id"]], "delete")
            result["live_sets_pruned"].append(s["id"])
        elif dry_run:
            entry["action"] = "would delete"
            result["files_pending"] += files
        else:
            _run_gc(_gc_base() + ["deferred-deletes"] + _gc_jdbc(cfg)
                    + _gc_fileio() + ["--time-zone=UTC", "-l", s["id"]],
                    "deferred-deletes")
            entry["action"] = "deleted"
            result["files_deleted"] += files
            # Acted on and now empty: the row is pure bookkeeping.
            _run_gc(_gc_base() + ["delete"] + _gc_jdbc(cfg) + ["-l", s["id"]],
                    "delete")
            result["live_sets_pruned"].append(s["id"])

        result["live_sets"].append(entry)

    log.info("deferred deletes: %d files removed, %d still inside the %dh "
             "window", result["files_deleted"], result["files_pending"],
             window_h)
    if result["failed_live_sets"]:
        log.warning(
            "%d GC live-set(s) are in a failed state and are being left as "
            "evidence: %s. Nothing they identified will ever be reclaimed "
            "until the underlying sweep failure is fixed.",
            len(result["failed_live_sets"]), result["failed_live_sets"])
    return result


# --------------------------------------------------- 3+4. table row retention
def observed_dates(spark, table: str, column: str) -> list[date]:
    rows = spark.sql(
        f"SELECT DISTINCT {column} AS bd FROM {table} WHERE {column} IS NOT NULL"
    ).collect()
    return sorted(r["bd"] for r in rows)


def iceberg_gc_enabled(spark, table: str) -> bool:
    """Whether Iceberg-level file deletion is permitted on this table.

    NessieCatalog sets `gc.enabled=false` (and `nessie.gc.no-warning=true`) on
    every table it creates, and it is right to. Under Nessie many references
    can share the same data files, so deleting files based on ONE table
    pointer's snapshot view can corrupt what another branch or tag sees.
    Iceberg therefore refuses:

        Cannot expire snapshots: GC is disabled (deleting files may corrupt
        other tables)

    Do NOT "fix" this by forcing gc.enabled=true. That disables the guard, not
    the hazard. Under Nessie, physical reclamation is Nessie GC's job -- see
    nessie_gc() above and docs/MAINTENANCE.md.
    """
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        props = {r[0]: r[1] for r in rows}
        return str(props.get("gc.enabled", "true")).lower() == "true"
    except Exception:
        return True


def is_scd2(spark, table: str) -> bool:
    """Does this table store one row per VERSION rather than per business date?

    DETECTED, not configured. The alternative was a per-table flag in
    retention.yml, and this file already carries the scar of a hand-maintained
    table list that drifted -- five tables against the DAG's nine, silently
    leaving four unretained. A list of which tables are SCD2 would drift the
    same way, and the failure would be worse: retention would run the WRONG
    delete against a table it believed was a snapshot.

    Reading the columns asks the table what it actually is. All three columns
    are required so an unrelated table with a `effective_from` cannot be mistaken
    for one of these. Which mode was chosen is reported in the result JSON, so
    the detection is visible rather than silent.
    """
    try:
        cols = {f.name for f in spark.table(table).schema.fields}
    except Exception:
        return False
    return {"effective_from", "effective_to", "is_current"} <= cols


def apply_scd2_retention(spark, table: str, layer: str,
                         dry_run: bool = False) -> dict:
    """Expire CLOSED versions that no retained business date falls inside.

    Two rules, and the first is absolute:

      * A CURRENT version is never expired, however old. It is the answer to
        "what is this counterparty now", and its effective_from can predate the
        keep-set by years -- 60 of the 70 rows here start on 2024-02-29. A
        cutoff that dropped them would empty the dimension and every
        point-in-time join with it.
      * A closed version is expired only once the whole of its effective range
        sits before the oldest retained business date. Conservative on
        purpose: a version straddling the cutoff is still in force for a
        retained date and must stay.

    AND THIS IS NOT A PARTITION DROP. The snapshot path below deletes whole
    `business_date` partitions as an Iceberg metadata operation. There is no
    business_date here, so this is a row-level delete producing delete files,
    and reclaiming them is `rewrite_data_files` in the maintenance job. That is
    the real cost of SCD2 and it belongs in the open -- see docs/RETENTION.md.

    In exchange the problem is much smaller: this table holds 70 rows where the
    snapshot held 2,400, so row retention on an SCD2 dimension is nearly moot.
    It exists to bound history, not volume.
    """
    policy = retention_policy(layer)
    observed = sorted({r["bd"] for r in spark.sql(
        f"SELECT DISTINCT effective_from AS bd FROM {table} WHERE effective_from IS NOT NULL"
    ).collect()})
    if not observed:
        return {"table": table, "skipped": "no data"}

    retained = sorted(set(observed) - expire_set(observed, policy))
    if not retained:
        return {"table": table, "layer": layer, "mode": "scd2",
                "skipped": "keep-set covers nothing; refusing to empty the table"}
    cutoff = min(retained)

    counts = spark.sql(f"""
        SELECT
          SUM(CASE WHEN is_current THEN 1 ELSE 0 END)                      AS current_rows,
          SUM(CASE WHEN NOT is_current AND effective_to < DATE '{cutoff:%Y-%m-%d}'
                   THEN 1 ELSE 0 END)                                      AS expiring_rows,
          COUNT(*)                                                         AS total_rows
        FROM {table}""").collect()[0]

    result = {
        "table": table,
        "layer": layer,
        "mode": "scd2",
        "total_rows": counts["total_rows"],
        "current_rows_never_expired": counts["current_rows"],
        "expiring_rows": counts["expiring_rows"],
        "cutoff": cutoff,
        "delete_style": "row-level (not a partition drop) -- reclaimed by "
                        "rewrite_data_files in maintenance",
    }

    if counts["expiring_rows"] and not dry_run:
        spark.sql(f"DELETE FROM {table} "
                  f"WHERE NOT is_current AND effective_to < DATE '{cutoff:%Y-%m-%d}'")
        log.info("%s: expired %d closed version(s) ending before %s",
                 table, counts["expiring_rows"], cutoff)
    return result


def apply_table_retention(spark, table: str, layer: str, date_column: str,
                          dry_run: bool = False) -> dict:
    # An SCD2 dimension has no business_date to delete by; see is_scd2().
    if is_scd2(spark, table):
        return apply_scd2_retention(spark, table, layer, dry_run=dry_run)

    policy = retention_policy(layer)
    observed = observed_dates(spark, table, date_column)
    if not observed:
        return {"table": table, "skipped": "no data"}

    to_expire = sorted(expire_set(observed, policy))
    result = {
        "table": table,
        "layer": layer,
        "observed_dates": len(observed),
        "expiring_dates": len(to_expire),
        "retained_dates": len(observed) - len(to_expire),
        "oldest_retained": min(set(observed) - set(to_expire)).isoformat(),
    }

    if to_expire and not dry_run:
        # business_date is the partition column, so this resolves to a
        # partition-level metadata delete — the Iceberg analogue of partition
        # switching. It does NOT rewrite data files.
        in_list = ", ".join(f"DATE '{d:%Y-%m-%d}'" for d in to_expire)
        spark.sql(f"DELETE FROM {table} WHERE {date_column} IN ({in_list})")
        log.info("%s: logically expired %d dates", table, len(to_expire))

    # Physical reclamation. Must run AFTER tag expiry.
    #
    # ... except that under Nessie it does not run here at all. NessieCatalog
    # sets gc.enabled=false on its tables, so expire_snapshots raises rather
    # than deleting anything. That is correct: files are shared across refs and
    # only Nessie GC can safely decide what is dead. See iceberg_gc_enabled().
    if not dry_run and not iceberg_gc_enabled(spark, table):
        result["snapshot_expiry"] = "skipped: gc.enabled=false (Nessie-managed)"
        log.info(
            "%s: skipping expire_snapshots -- gc.enabled=false. Under Nessie, "
            "reclamation is nessie_gc()'s job, not Iceberg's.", table)
    elif not dry_run:
        older_than = datetime.now(timezone.utc) - timedelta(
            days=policy["snapshot_retention_days"]
        )
        expired = spark.sql(
            f"CALL {CATALOG}.system.expire_snapshots("
            f"  table => '{_short(table)}',"
            f"  older_than => TIMESTAMP '{older_than:%Y-%m-%d %H:%M:%S}',"
            f"  retain_last => {policy['snapshot_retain_last']})"
        ).collect()
        if expired:
            row = expired[0].asDict()
            result["files_deleted"] = sum(
                v for k, v in row.items() if "deleted" in k.lower() and isinstance(v, int)
            )
    return result


# -------------------------------------------------------- 5. orphan file sweep
def sweep_orphans(spark, table: str, dry_run: bool = False) -> dict:
    cfg = maintenance_config()["orphan_files"]
    if not cfg.get("enabled", True):
        return {"table": table, "orphan_sweep": "disabled"}

    min_age = max(int(cfg["min_age_days"]), 3)  # hard floor: never below 3 days
    older_than = datetime.now(timezone.utc) - timedelta(days=min_age)
    if dry_run:
        return {"table": table, "orphan_sweep": "dry-run",
                "older_than": older_than.isoformat()}

    # Same Nessie guard as snapshot expiry: remove_orphan_files also deletes
    # files, so gc.enabled=false blocks it, and for the same good reason -- an
    # "orphan" from main's point of view may be live on another ref.
    if not iceberg_gc_enabled(spark, table):
        return {"table": table,
                "orphan_sweep": "skipped: gc.enabled=false (Nessie-managed)"}

    rows = spark.sql(
        f"CALL {CATALOG}.system.remove_orphan_files("
        f"  table => '{_short(table)}',"
        f"  older_than => TIMESTAMP '{older_than:%Y-%m-%d %H:%M:%S}')"
    ).collect()
    return {"table": table, "orphans_removed": len(rows)}


def _short(fqn: str) -> str:
    """Iceberg CALL procedures take namespace.table, without the catalog."""
    parts = fqn.split(".")
    return ".".join(parts[1:]) if parts[0] == CATALOG else fqn


# ----------------------------------------------------------------------- main
class RetentionPartialFailure(RuntimeError):
    """Retention applied to some tables and failed on others.

    Carries the report rather than only a message so a caller -- the
    `enforce_retention` task, or a human reading the log -- can see exactly
    which tables were retained before the failure. See run() for why the
    recovery is simply to run it again.
    """

    def __init__(self, failed: list[str], applied: list[str], report: dict):
        self.failed = failed
        self.applied = applied
        self.report = report
        super().__init__(
            f"retention failed on {len(failed)} of "
            f"{len(failed) + len(applied)} table(s): {', '.join(failed)}. "
            f"Applied to: {', '.join(applied) or 'none'}. Re-run the chain to "
            f"finish; steps already applied are no-ops."
        )


def run(tables: list[tuple[str, str]], date_column: str = "business_date",
        dry_run: bool = False) -> dict:
    """tables: list of (fully_qualified_table, layer)."""
    nessie = Nessie()
    report: dict = {"env": ENV, "dry_run": dry_run,
                    "started": datetime.now(timezone.utc).isoformat()}

    report["working_branches_removed"] = clean_working_branches(nessie, dry_run)
    report["tags_removed"] = expire_tags(nessie, dry_run)
    # Step 3 of the documented chain, and it MUST land here: after tag expiry
    # (a tag pins every file its commit referenced) and before expire_snapshots.
    report["nessie_gc"] = nessie_gc(dry_run)
    # Step 3b: execute the deletes that PREVIOUS sweeps deferred and whose
    # review window has now passed. Deliberately after the sweep above -- the
    # set that sweep just recorded is the newest one and cannot be eligible, so
    # this reads as "yesterday's findings, actioned today". Never fail the
    # nightly chain over it: the rest of retention is still correct without it.
    # See docs/DECISIONS.md#gc-lag-and-assertions
    try:
        report["deferred_deletes"] = deferred_deletes(dry_run)
    except Exception as e:
        log.warning("deferred deletes failed: %s", str(e)[:400])
        report["deferred_deletes"] = {"error": str(e)[:400]}

    spark = spark_session("retention", ref="main")
    try:
        report["tables"] = []
        for table, layer in tables:
            col = "_business_date" if layer == "raw" else date_column
            # PER TABLE, not one try around the loop. Tables are
            # independent -- each one's row DELETE is its own Nessie commit --
            # so one table failing is no reason the rest go unretained. Before
            # this, a failure on table 2 of 9 meant tables 3-9 were silently
            # skipped for the night and the only evidence was a traceback.
            # The run still FAILS, at the end, naming what was applied and
            # what was not; it just does the work it can do first.
            try:
                r = apply_table_retention(spark, table, layer, col, dry_run)
                r.update(sweep_orphans(spark, table, dry_run))
            except Exception as e:                       # noqa: BLE001
                log.exception("retention failed on %s", table)
                r = {"table": table, "layer": layer,
                     "error": f"{type(e).__name__}: {str(e)[:400]}"}
            report["tables"].append(r)
    finally:
        spark.stop()

    # Step 0-ish, and it runs LAST because it is independent of everything
    # else: landing is flat object storage, touched by no Nessie ref and no
    # Iceberg snapshot. Never fail the chain over it -- the table layers are
    # still correctly retained without it.
    try:
        from reporting_platform.retention.landing import sweep_landing

        report["landing"] = sweep_landing(dry_run=dry_run)
    except Exception as e:                     # never fail retention over this
        log.warning("landing sweep failed: %s", str(e)[:200])
        report["landing"] = {"error": str(e)[:200]}

    # Step 6: prefixes no reference points at. Runs LAST, after GC has had its
    # chance -- GC can only collect files of content it enumerates from live
    # refs, so anything left after it is either live or genuinely stranded.
    # See orphan_storage for why neither GC nor remove_orphan_files covers this.
    try:
        from reporting_platform.retention.orphan_storage import sweep_orphan_prefixes

        report["orphan_prefixes"] = sweep_orphan_prefixes(dry_run=dry_run)
    except Exception as e:                     # never fail retention over this
        log.warning("orphan-prefix sweep failed: %s", str(e)[:200])
        report["orphan_prefixes"] = {"error": str(e)[:200]}

    report["finished"] = datetime.now(timezone.utc).isoformat()

    # A half-applied run is the failure shape this chain actually produces, so
    # it must report which tables were applied rather than throwing that away
    # with the exception. Re-running is safe -- every step recomputes what is
    # left to do rather than replaying what it did -- but that is only useful to
    # someone who knows what state they are re-running from.
    # See docs/DECISIONS.md#retention-partial-failure-report
    failed = [t["table"] for t in report["tables"] if t.get("error")]
    if failed:
        applied = [t["table"] for t in report["tables"] if not t.get("error")]
        report["partial"] = {"failed_tables": failed, "applied_tables": applied}
        log.error("PARTIAL RETENTION RUN. Applied: %s. Failed: %s. "
                  "Re-running the whole chain is safe and is the supported "
                  "recovery: each step recomputes from current state, so "
                  "tables already retained become no-ops.",
                  applied or "none", failed)
        raise RetentionPartialFailure(failed, applied, report)
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--table", action="append", default=[],
                   help="fqn:layer, e.g. lakehouse.raw.trade:raw (repeatable)")
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be expired without changing anything")
    p.add_argument("--deferred-only", action="store_true",
                   help="run ONLY the deferred-delete pass and "
                        "exit; touches no table and needs no Spark")
    p.add_argument("--deferred-after-hours", type=int, default=None,
                   help="override the configured deferral window, in hours")
    p.add_argument("--all-managed", action="store_true",
                   help="%s" % HELPTEXT)
    a = p.parse_args(argv)
    if a.deferred_only:
        print(json.dumps(deferred_deletes(a.dry_run, a.deferred_after_hours),
                         indent=2, default=str))
        return 0
    # Derived, never hand-listed. A hardcoded subset in the Makefile had
    # already drifted to five tables against the DAG's nine, silently leaving
    # four unretained.
    tables = [tuple(t.split(":", 1)) for t in a.table]
    if a.all_managed:
        tables = managed_tables() + tables
    if not tables:
        p.error("give --table, or --all-managed for everything the platform "
                "manages (or --deferred-only, which needs no tables)")
    try:
        print(json.dumps(run(tables, dry_run=a.dry_run), indent=2, default=str))
    except RetentionPartialFailure as e:
        # Print the report BEFORE failing: a half-applied run must say what it
        # applied, and a traceback alone is not enough to decide anything.
        # See docs/DECISIONS.md#retention-partial-failure-report
        print(json.dumps(e.report, indent=2, default=str))
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
