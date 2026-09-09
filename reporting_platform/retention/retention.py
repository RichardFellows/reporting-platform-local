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

Run GC before tags are expired and the tags still pin the content, so it
collects nothing while appearing to succeed -- the storage graph flatlines and
nobody notices for a quarter.

Under a Nessie catalog `expire_snapshots` is NOT the step that reclaims
storage: NessieCatalog sets `gc.enabled=false` on every table, correctly,
because data files are shared across references and no single table pointer
knows what another branch still needs. Step 5 is kept, guarded, so the code
still works against a non-Nessie catalog.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

from reporting_platform.common.calendar_rules import expire_set
from reporting_platform.common.context import (
    CATALOG, ENV, Nessie, all_snapshot_retention_days, class_keep_years,
    feeds, feeds_behind_report, gc_window_hours, maintenance_config,
    managed_tables, nessie_gc_config, reference_policy, reports,
    retention_policy, spark_session, tag_retention_years,
)

log = logging.getLogger("retention")

HELPTEXT = ("every table the platform manages, from managed_tables() -- the "
            "same list platform_housekeeping uses")

# TWO SHAPES, and only one is still written. `published/<report>/<bd>/<run>` is
# what `dbt_builds.publish` cuts per report, which is what makes
# `references.published_tags.per_report` mean something. `published/<bd>/<run>`
# is what the INGEST DAG cut before a publication knew its report; nothing
# writes it now, and it is still matched because tags cut under it are real
# pins. A report name may not contain "/", so the two cannot both match.
TAG_RE = re.compile(
    r"^published/"
    r"(?:(?P<report>[^/]+)/)?"
    r"(?P<bd>\d{4}-\d{2}-\d{2})/(?P<run>.+)$")

# `snapshot/<feed>/<bd>/<run>` -- what an INGEST pins now. A different object
# from a publication and kept for a different reason: it pins the state one
# feed's ingest left, so a raw COB date stays readable after retention deleted
# it from the live table. Its window (`references.snapshot_tags`) may be much
# shorter than a published tag's, because nothing is reproduced FROM it.
SNAPSHOT_RE = re.compile(
    r"^snapshot/(?P<feed>[^/]+)/(?P<bd>\d{4}-\d{2}-\d{2})/(?P<run>.+)$")

# Matches landing.py, and for the same reason: a whole number of years turned
# into a timedelta avoids the 29 February case that calendar arithmetic hits.
DAYS_PER_YEAR = 365.25
WORKING_RE = re.compile(r"^(ingest|build)/")


def check_reproducibility_window() -> dict:
    """REFUSE to sweep if landing expires before the published pins do.

    A published tag pins the TABLES. Reproducing a published run means showing
    its inputs too, and `landing/` is the only copy of what the upstream sent
    -- so a pin outliving its landing evidence cannot be fully honoured.

    PER FEED SINCE RETENTION CLASSES (REQ-600/601). It used to be two global
    numbers; with classes there is no single landing window, so the question
    is asked per report: does every feed BEHIND it keep its evidence at least
    as long as that report's pin? `feeds_behind_report` answers it by walking
    the project's `ref()`s, the same derivation `managed_tables()` uses.

    THIS IS A NARROWER REFUSAL THAN THE OLD ONE, deliberately. A feed behind
    no published report is no longer bound by any pin, because nothing
    published is reproduced from it -- without that, no class could ever be
    shorter than the longest pin and the feature would be decoration. The cost
    is that a window is only as protected as the lineage derivation is
    correct, which is why `feeds_behind_report` raises on an unresolvable ref.

    A `per_report` ENTRY NAMING NO LIVE EXPOSURE binds every feed: the tags it
    already cut still resolve their window by name, and the lineage that would
    say which feeds were behind it is gone.

    STILL NOT BOUND TO `snapshot_tags`, and that must not creep back in.
    Nothing is reproduced from a snapshot tag; binding it here would silently
    impose the published window on every feed again.

    THIS ONE REFUSES, where `landing.keep_years()`'s interlock warns, because
    of what the failure costs: landing running short of the raw window
    degrades `find_pending` gradually and is recoverable, deleting evidence a
    live pin depends on is not, and this sweep runs nightly and unattended.

    NOT REQ-602 IN FULL. This compares WINDOWS, so it catches the
    configuration that guarantees the loss and cannot catch a specific
    delivery expiring early inside a coherent one. `monitoring/evidence.py` is
    the per-delivery half, and it runs after this chain because it has to
    observe what the sweep left.
    """
    known = reports()
    every_feed = feeds()

    # A `per_report` entry naming no live exposure is NOT ignored. A report
    # removed from the project keeps the tags it already cut -- `expire_tags`
    # resolves their window by the report name in the tag -- and its lineage is
    # gone, so which feeds were behind it can no longer be derived. It
    # therefore binds EVERY feed.
    orphans = sorted(set(
        (reference_policy("published_tags").get("per_report") or {})) - set(known))

    checked: list[dict] = []
    problems: list[str] = []
    for name in sorted(known) + orphans:
        tag_years = tag_retention_years(name)
        behind = (feeds_behind_report(name) if name in known
                  else sorted(every_feed))
        for feed_name in behind:
            spec = every_feed[feed_name]
            landing_years = class_keep_years("landing", spec.retention_class)
            checked.append({"report": name, "feed": feed_name,
                            "live_exposure": name in known,
                            "retention_class": spec.retention_class,
                            "landing_keep_years": landing_years,
                            "tag_keep_years": tag_years})
            if landing_years < tag_years:
                where = ("" if name in known else
                         " (no live exposure, so every feed binds)")
                problems.append(
                    f"{name} pins for up to {tag_years} years{where} but "
                    f"{feed_name} (class {spec.retention_class}) keeps its "
                    f"landing evidence {landing_years} years")

    if problems:
        raise ValueError(
            f"retention.yml (env {ENV!r}): a published run would stay pinned "
            f"and reproducible for longer than the landing evidence it was "
            f"built from is kept -- " + "; ".join(problems) + ". Raise the "
            f"landing window for those retention classes, or lower "
            f"references.published_tags for those reports -- both are per "
            f"environment. Refusing to sweep in this state.")

    unbound = sorted(set(feeds()) - {c["feed"] for c in checked})
    if unbound:
        # NOT a problem, and said out loud so it is not read as one. These
        # feeds reach no exposure, so no published figure is reproduced from
        # them. It is also the list to look at first if a report was expected
        # to depend on one of them.
        log.info("landing evidence for %s is bound by no published tag: those "
                 "feeds are behind no report", ", ".join(unbound))
    return {"checked": checked, "unbound_feeds": unbound,
            "default_landing_keep_years":
                int(retention_policy("landing")["keep_years"])}


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
    # this function deleted every working branch on sight.
    for ref in nessie.list_references(fetch_all=True):
        if ref.get("type") != "BRANCH":
            continue
        m = WORKING_RE.match(ref["name"])
        # Anything outside ingest/ and build/ is not swept, which is what
        # makes the hold/ convention work: renaming a branch under
        # investigation to hold/<name> exempts it by construction.
        if not m:
            continue
        cutoff = now - timedelta(hours=windows[m.group(1)])
        meta = ref.get("metadata") or {}

        # A branch with no commits of its own POINTS AT ITS BASE's commit, so
        # commitMetaOfHEAD describes main and the age below would be main's. On
        # a quiet main a branch opened seconds ago reads as days old and the
        # next sweep deletes it mid-build. Nessie exposes no branch-creation
        # timestamp, so the age is genuinely unknowable; fail safe as for a
        # missing commit time. The cost is that an empty abandoned branch is
        # never swept; the watchdog's branch count catches those.
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
        # "why is that branch still there" has two possible answers, and a log
        # of removals alone cannot distinguish them.
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


def _tag_age(ref: dict, cob_date: date) -> tuple[str, date, date | None]:
    """(what it was judged on, the date to judge it by, the commit date).

    THE AGE IS THE COMMIT TIME where there is one -- when the record was made
    -- with the COB DATE as a conservative fallback: a tag cannot be cut
    before the date it is about, so the COB date is never later than the
    commit time and can only ever keep a tag the commit time would have kept
    too. Deleting a tag is unrecoverable, so everything ambiguous keeps.

    Shared by the published and snapshot sweeps: they answer to different
    windows, but "how old is this tag" is one question and had better have one
    answer.
    """
    meta = ref.get("metadata") or {}
    committed = (meta.get("commitMetaOfHEAD") or {}).get("commitTime")
    commit_date = None
    if committed:
        try:
            commit_date = datetime.fromisoformat(
                committed.replace("Z", "+00:00")).date()
        except ValueError:
            commit_date = None
    if committed and commit_date is None:
        log.warning("tag %s has an unreadable commit time %r; judging it on "
                    "its COB date alone", ref.get("name"), committed)
    if commit_date is not None:
        return "commit time", commit_date, commit_date
    return "COB date", cob_date, None


# --------------------------------------------------------------- 2. tag expiry
def expire_tags(nessie: Nessie, dry_run: bool = False) -> list[str]:
    """Published-tag retention. TAG RETENTION IS DATA RETENTION.

    A tag pins every data file its commit referenced, and `expire_snapshots`
    cannot reclaim a pinned file. So this sweep decides how long a published
    run stays REPRODUCIBLE -- a different question from how much history the
    tables serve, and it used to be answered with the tables' own keep-set.

    Two things were wrong with that, and both were live:

      * an ordinary daily publication lost its pin once ten more COB dates had
        been published -- about a fortnight. The evidence expired on a table
        schedule.
      * within a RETAINED date, only the newest tag survived. The tag name
        carried no feed, and `record_publication` ran in every per-feed ingest
        DAG, so N feeds publishing one COB date cut N tags and N-1 were
        deleted the same night. Observed live: three tags for 2026-08-01, two
        scheduled for deletion as "earlier reruns"; they were other feeds'
        publications.

    Now: FLAT AGE, per report, every tag judged on its own.

    THE AGE IS THE COMMIT TIME -- when the publication was MADE -- not the COB
    date it is about. A retention period runs from the creation of the record,
    and a restatement published today for an old COB date must survive its own
    full window. Measuring from the COB date would expire it on arrival.

    The COB date is the FALLBACK, used only when a tag carries no readable
    commit time. Conservative by construction: a publication cannot precede
    the date it reports on.

    Deleting a tag is unrecoverable, so everything ambiguous here keeps.
    """
    now = datetime.now(timezone.utc)
    removed: list[str] = []
    kept = 0

    for ref in nessie.list_references("published/", fetch_all=True):
        if ref.get("type") != "TAG":
            continue
        name = ref["name"]
        m = TAG_RE.match(name)
        if not m:
            # Under `published/` but not shaped like a publication. Never
            # swept: this sweep only removes what it positively recognises,
            # the same rule `clean_working_branches` follows for hold/.
            log.info("tag %s does not match the published shape; leaving it",
                     name)
            continue

        report = m.group("report")
        try:
            years = tag_retention_years(report)
        except ValueError:
            # A window that cannot be resolved must not fall back to a guess:
            # the fallback would authorise deletion. Re-raised so the whole
            # sweep refuses, matching the GC cutoff interlock.
            log.error("cannot resolve the retention window for %s; refusing "
                      "to sweep published tags", name)
            raise
        cutoff = (now - timedelta(days=years * DAYS_PER_YEAR)).date()

        bd = datetime.strptime(m.group("bd"), "%Y-%m-%d").date()
        judged_on, age_date, commit_date = _tag_age(ref, bd)
        keep = age_date >= cutoff
        log.info("published tag %s: COB date %s, published %s, window "
                 "%dy (cutoff %s), judged on %s%s -> %s",
                 name, bd, commit_date or "unknown", years, cutoff, judged_on,
                 f", report {report!r}" if report else "",
                 "keep" if keep else "expire")
        if keep:
            kept += 1
            continue
        if not dry_run:
            nessie.delete_reference(name)
        removed.append(name)

    log.info("published tags: %d kept, %d removed", kept, len(removed))
    return removed



def expire_snapshot_tags(nessie: Nessie, dry_run: bool = False) -> list[str]:
    """`snapshot/<feed>/<bd>/<run>` retention. NOT the same thing as a pin.

    An ingest pins the state it left so a raw COB date stays readable after
    retention removed it from the live table. That is a convenience with a
    cost -- a pinned file cannot be reclaimed -- and NOTHING IS REPRODUCED
    FROM IT, so its window is a storage decision rather than an evidence one
    and may be far shorter than a published tag's.

    This is why the ingest DAG's tag was renamed out of `published/`. There it
    was judged by the published window -- ten years of pinned raw files per
    feed per COB date -- and counted by every check asking whether a
    PUBLICATION can still be read.

    Judged the same way a published tag is (`_tag_age`), keeping everything it
    does not positively recognise.
    """
    policy = reference_policy("snapshot_tags")
    years = policy.get("keep_years")
    if isinstance(years, dict):
        if ENV not in years:
            raise ValueError(
                f"retention.yml: references.snapshot_tags.keep_years has no "
                f"entry for env {ENV!r} (has {sorted(years)}). Add one, or use "
                f"a bare number of years.")
        years = years[ENV]
    if isinstance(years, bool) or not isinstance(years, int) or years <= 0:
        raise ValueError(
            f"retention.yml: references.snapshot_tags.keep_years is {years!r}, "
            f"which is not a positive whole number of years. Refusing to sweep "
            f"rather than guessing a window that authorises deletion.")

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=years * DAYS_PER_YEAR)).date()
    removed: list[str] = []
    kept = 0
    for ref in nessie.list_references("snapshot/", fetch_all=True):
        if ref.get("type") != "TAG":
            continue
        name = ref["name"]
        m = SNAPSHOT_RE.match(name)
        if not m:
            log.info("tag %s does not match the snapshot shape; leaving it",
                     name)
            continue
        bd = datetime.strptime(m.group("bd"), "%Y-%m-%d").date()
        judged_on, age_date, _commit = _tag_age(ref, bd)
        keep = age_date >= cutoff
        log.info("snapshot tag %s: feed %s, COB date %s, window %dy "
                 "(cutoff %s), judged on %s -> %s", name, m.group("feed"), bd,
                 years, cutoff, judged_on, "keep" if keep else "expire")
        if keep:
            kept += 1
            continue
        if not dry_run:
            nessie.delete_reference(name)
        removed.append(name)
    log.info("snapshot tags: %d kept, %d removed", kept, len(removed))
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

    `scripts/init-postgres.sql` creates the `nessie_gc` DATABASE but no
    tables, and the GC tool does not create them on demand -- it has a
    `create-sql-schema` command nothing in this repo ever ran. The tables then
    survive in the Postgres volume forever, which is why four sessions of GC
    work never noticed. A rebuild from `docker compose down -v` put a
    genuinely empty database underneath and the first mark-live died with
    `relation "gc_live_sets" does not exist`.

    Not a soft failure: `nessie_gc()` is deliberately not wrapped in a
    try/except, because Nessie GC is the ONLY thing that reclaims storage here
    and a silent skip would be worse than a red run.

    Runs once per process and tolerates the already-created case;
    `--jdbc-schema=CREATE_IF_NOT_EXISTS` in _gc_jdbc() is what makes the
    command return 0 on a database that already has the tables.
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

    THE GAP THIS FILLS. Iceberg's `expire_snapshots` only knows about
    snapshots reachable from the single table pointer it is handed. Nessie
    holds many pointers -- every branch, every tag -- so a file can be
    unreferenced from `main` yet still live because some abandoned build
    branch's commit contains it. Nessie GC is the only thing that reasons
    across all references.

    There is no server-side GC endpoint: it runs via the external `nessie-gc`
    tool, baked into the image by Dockerfile.airflow.

    Safety model, which is the tool's own three-stage one:

      dry_run=True   -> `mark-live` only. Records a live-content-set and
                        deletes nothing, ever.
      dry_run=False  -> `mark-live`, then `sweep`. With `defer_deletes: true`
                        (the default) the sweep RECORDS files to delete rather
                        than deleting them, for review with
                        `nessie-gc list-deferred`.

    So even a non-dry run removes nothing by default -- this is the one step
    in the retention chain with no undo.
    """
    cfg = nessie_gc_config()
    if not cfg.get("enabled", False):
        log.info("nessie gc: disabled in retention.yml, skipping")
        return {"skipped": "disabled"}

    cutoff = cfg.get("default_cutoff", "NONE")

    # INTERLOCK: a cutoff shorter than the longest snapshot retention would
    # collect files still-valid snapshots reference, breaking time travel. A
    # refuse-to-run condition, not a warning: the damage is unrecoverable.
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
# offsets or line numbers, which the banner and an Error column shift.
_LIVE_SET_ROW = re.compile(
    r"^\s*(?P<id>[0-9a-f]{8}-[0-9a-f-]{27})\s+"
    r"(?P<status>[A-Z_]+)\s+"
    r"(?P<created>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)",
    re.MULTILINE,
)
# A deferred-delete row from `list-deferred`. NOTE the path is printed as TWO
# columns -- the content's base location, then the file relative to it -- so a
# naive `s3://\S+` match captures only the directory. Match both halves.
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

    `nessie_gc()` runs `sweep --defer-deletes`, which RECORDS the files it
    would remove and deletes nothing. Until this function existed the second
    step was `nessie-gc deferred-deletes` typed by a human -- so in practice
    nothing was ever reclaimed unattended. The warehouse only shrank when
    somebody remembered, and two guards built to detect reclamation failure
    had to be held at warn-only because a flat warehouse was the expected
    state.

    The deferral period is kept -- it is a genuine safety feature -- but
    becomes a POLICY VALUE (`deferred_delete_after_hours`) instead of a manual
    step. A sweep never deletes what it just recorded; a later run does.

    The `deferred-deletes` subcommand takes a live-set id and nothing else:
    there is no "--older-than" flag, so the window is enforced here, by
    choosing which live-sets to hand it.

    THE RISK THE WINDOW CREATES. A file recorded as unreachable is deleted
    later, so anything making it reachable again in between -- reassigning a
    ref backwards, branching from a pre-sweep commit -- is resurrecting
    content this pass will then delete. Inherent in deferral, and a longer
    window widens it. Do not point a ref at an old hash while deferred deletes
    are outstanding.

    Live-sets are dropped once acted on, or re-running would hand
    `deferred-deletes` already-deleted files every night forever. Sets that
    only completed mark-live are pruned on age alone.

    CRASHING BETWEEN THE DELETES AND THE DROP is survivable, and tested rather
    than assumed: the live-set is left EXPIRY_SUCCESS and past its window,
    `list-deferred` still reports its files (the tool lists what was RECORDED,
    not what is still present), and the next run hands it over again. The tool
    exits 0 on files already gone.

    What that costs is counter accuracy: `files_deleted` is what
    `list-deferred` reported, so a live-set actioned twice is counted twice.
    Treat it as "files actioned", and the watchdog's deferred_backlog check --
    which reads the object store -- as the authority on whether reclamation is
    really happening.
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
            # But REPORT the failures: left merely un-pruned they accumulate in
            # silence, and a sweep failing every night looks from the outside
            # exactly like a platform with nothing to collect.
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

    NessieCatalog sets `gc.enabled=false` on every table it creates, and it is
    right to. Under Nessie many references can share the same data files, so
    deleting files based on ONE table pointer's snapshot view can corrupt what
    another branch or tag sees. Iceberg therefore refuses with "Cannot expire
    snapshots: GC is disabled".

    Do NOT "fix" this by forcing gc.enabled=true. That disables the guard, not
    the hazard. Under Nessie, physical reclamation is Nessie GC's job.
    """
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        props = {r[0]: r[1] for r in rows}
        return str(props.get("gc.enabled", "true")).lower() == "true"
    except Exception:
        return True


def is_scd2(spark, table: str) -> bool:
    """Does this table store one row per VERSION rather than per COB date?

    DETECTED, not configured. This file already carries the scar of a
    hand-maintained table list that drifted -- five tables against the DAG's
    nine, silently leaving four unretained. A list of which tables are SCD2
    would drift the same way, and worse: retention would run the WRONG delete
    against a table it believed was a snapshot.

    All three columns are required so an unrelated table with an
    `effective_from` cannot be mistaken for one of these. Which mode was
    chosen is reported in the result JSON, so the detection is visible.
    """
    try:
        cols = {f.name for f in spark.table(table).schema.fields}
    except Exception:
        return False
    return {"effective_from", "effective_to", "is_current"} <= cols


def apply_scd2_retention(spark, table: str, layer: str,
                         dry_run: bool = False) -> dict:
    """Expire CLOSED versions that no retained COB date falls inside.

    Two rules, and the first is absolute:

      * A CURRENT version is never expired, however old. It is the answer to
        "what is this counterparty now", and its effective_from can predate the
        keep-set by years -- 60 of the 70 rows here start on 2024-02-29. A
        cutoff that dropped them would empty the dimension and every
        point-in-time join with it.
      * A closed version is expired only once the whole of its effective range
        sits before the oldest retained COB date. Conservative on
        purpose: a version straddling the cutoff is still in force for a
        retained date and must stay.

    AND THIS IS NOT A PARTITION DROP. The snapshot path below deletes whole
    `cob_date` partitions as an Iceberg metadata operation. There is no
    cob_date here, so this is a row-level delete producing delete files,
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
    # An SCD2 dimension has no cob_date to delete by; see is_scd2().
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
        # cob_date is the partition column, so this resolves to a
        # partition-level metadata delete — the Iceberg analogue of partition
        # switching. It does NOT rewrite data files.
        in_list = ", ".join(f"DATE '{d:%Y-%m-%d}'" for d in to_expire)
        spark.sql(f"DELETE FROM {table} WHERE {date_column} IN ({in_list})")
        log.info("%s: logically expired %d dates", table, len(to_expire))

    # Physical reclamation, which must run AFTER tag expiry -- except that
    # under Nessie it does not run here at all. NessieCatalog sets
    # gc.enabled=false on its tables, so expire_snapshots raises. That is
    # correct: files are shared across refs. See iceberg_gc_enabled().
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


def run(tables: list[tuple[str, str]], date_column: str = "cob_date",
        dry_run: bool = False) -> dict:
    """tables: list of (fully_qualified_table, layer)."""
    nessie = Nessie()
    report: dict = {"env": ENV, "dry_run": dry_run,
                    "started": datetime.now(timezone.utc).isoformat()}

    # FIRST, and before a dry-run check, because this refuses on CONFIGURATION
    # rather than on anything observed -- a dry run that passes here while the
    # real run would refuse teaches the wrong thing. Raising aborts the chain:
    # every step below this line deletes something.
    report["reproducibility_window"] = check_reproducibility_window()

    report["working_branches_removed"] = clean_working_branches(nessie, dry_run)
    report["tags_removed"] = expire_tags(nessie, dry_run)
    # Beside the published sweep and before the GC, for the same reason: both
    # kinds of tag pin data files, and the GC must run after everything that
    # can release one.
    report["snapshot_tags_removed"] = expire_snapshot_tags(nessie, dry_run)
    # Step 3 of the documented chain, and it MUST land here: after tag expiry
    # (a tag pins every file its commit referenced) and before expire_snapshots.
    report["nessie_gc"] = nessie_gc(dry_run)
    # Step 3b: execute the deletes PREVIOUS sweeps deferred and whose review
    # window has passed. After the sweep above, so this reads as "yesterday's
    # findings, actioned today". Never fail the nightly chain over it.
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
            col = "_cob_date" if layer == "raw" else date_column
            # PER TABLE, not one try around the loop. Each table's row DELETE
            # is its own Nessie commit, so one failing is no reason the rest go
            # unretained -- a failure on table 2 of 9 used to skip 3-9 silently.
            # The run still FAILS at the end, naming what was applied; it just
            # does the work it can do first.
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

    # Runs LAST because it is independent of everything else: landing is flat
    # object storage, touched by no Nessie ref and no Iceberg snapshot. Never
    # fail the chain over it -- the table layers are still correctly retained.
    try:
        from reporting_platform.retention.landing import sweep_landing

        report["landing"] = sweep_landing(dry_run=dry_run)
    except Exception as e:                     # never fail retention over this
        log.warning("landing sweep failed: %s", str(e)[:200])
        report["landing"] = {"error": str(e)[:200]}

    # Same shape, same reasoning, different prefix: `ready/` is flat object
    # storage too, and a CACHE -- everything in it is rebuildable by
    # re-normalizing from landing.
    try:
        from reporting_platform.retention.ready import sweep_ready

        report["ready"] = sweep_ready(dry_run=dry_run)
    except Exception as e:                     # never fail retention over this
        log.warning("ready sweep failed: %s", str(e)[:200])
        report["ready"] = {"error": str(e)[:200]}

    # And again for refused deliveries. Same prefix-sweep shape; the window
    # is its own key in retention.yml because nothing is reproduced from a
    # delivery that never landed, so this one has no correctness floor.
    try:
        from reporting_platform.retention.quarantine import sweep_quarantine

        report["quarantine"] = sweep_quarantine(dry_run=dry_run)
    except Exception as e:                     # never fail retention over this
        log.warning("quarantine sweep failed: %s", str(e)[:200])
        report["quarantine"] = {"error": str(e)[:200]}

    # Step 6: prefixes no reference points at. Runs LAST, after GC has had its
    # chance -- GC only collects files it enumerates from live refs, so
    # anything left is either live or genuinely stranded. See orphan_storage.
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
    # left -- but only useful to someone who knows what state they are in.
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
                   help="fqn:layer, e.g. lakehouse.raw.fo_trade:raw (repeatable)")
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
