"""What was published: runs, report versions and submissions (REQ-400..405).

THIS IS THE PART OF THE REGISTRY THAT CANNOT BE REBUILT. `deliveries.py` is an
index over objects that exist in storage, so `reconcile()` can reconstruct
every row of it from `landing/`. Nothing in this module can be reconstructed
from anything: that a build ran at 03:14, from that commit of the code, merged
that hash and published version 3 of a report is an EVENT, and the only record
of it is the one written while it happened. `registry/db.py`'s header says what
follows from that -- no foreign key from `run_input` to `delivery`, and a
mutable status on a run where a delivery may not have one.

THE INPUT SET IS DERIVED, NOT DECLARED. A run does not say which deliveries
are behind it; it is asked, after it has built them, by selecting the distinct
`delivery_id` out of the prepared models on its own branch. Precisely: the
deliveries whose rows are PRESENT in what it published, which for an SCD2
model is narrower than the deliveries it scanned -- see
`registry/inputs.py`. That column is
`delivery_ref()` -- `_delivery_id` where the row has one, the basename of
`_source_file` where it predates provenance -- so the answer reaches back past
the provenance change rather than stopping at it. A declared input set would be
a second statement of something the rows already carry, and the two would
disagree the first time a model changed which sources it reads.

WHY THE PREPARED LAYER AND NOT THE REPORTING ONE. Both would answer, but only
prepared knows WHICH FEED a delivery belongs to: a prepared model is one feed
by the platform's naming rule (model filename == table name == feed name, see
docs/DECISIONS.md#table-naming-no-layer-prefix), where a reporting model joins
several and keeps no column saying which delivery came from where. The
reporting layer's input set is its prepared tables' input set, so asking
prepared is both exact and more informative.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from reporting_platform.registry import db, lifecycle

log = logging.getLogger("registry.runs")

RUNNING, PUBLISHED, FAILED = "running", "published", "failed"
PURPOSES = ("prepared", "reporting")


def open_run(run_id: str, purpose: str, branch: str, *,
             environment: str, code_ref: str, code_ref_kind: str,
             dbt_manifest_ref: str, dag_id: str | None = None,
             airflow_run_id: str | None = None,
             change_ref: str | None = None,
             provenance: dict[str, str] | None = None) -> dict[str, Any]:
    """Record that a build has started. Idempotent on `run_id`.

    OPENED BEFORE THE BUILD, not after it, because a run that fails is the one
    most worth having a record of -- and a row written only on success would
    describe a platform that has never had a bad night. A retry of the same
    Airflow task reuses the row rather than raising: `keep_failed_branch`
    deliberately leaves the branch behind for a retry to reuse, and the run is
    the same logical run.

    `provenance` is `context.deployment_provenance()` -- the standing change,
    the pipeline and the declared project version, all of which are properties
    of the DEPLOYMENT and identical across every run of it. `change_ref` is the
    per-run exception and stays separate; see the column comments in db.py.
    """
    if purpose not in PURPOSES:
        raise ValueError(f"run purpose must be one of {PURPOSES}, got {purpose!r}")
    prov = provenance or {}
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO registry.run (run_id, purpose, status, environment,
                                      dag_id, airflow_run_id, branch,
                                      code_ref, code_ref_kind,
                                      dbt_manifest_ref, change_ref,
                                      dbt_project_ref, deployment_change_ref,
                                      deployment_pipeline_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                -- A retry re-reads the code and the project, and they can
                -- legitimately have changed between attempts -- that is worth
                -- recording, and recording the LATEST attempt's identity is
                -- what makes the row describe what actually produced the
                -- publication. status returns to running: the row is being
                -- reopened, and leaving 'failed' there would make a
                -- subsequent success invisible.
                status = EXCLUDED.status,
                branch = EXCLUDED.branch,
                code_ref = EXCLUDED.code_ref,
                code_ref_kind = EXCLUDED.code_ref_kind,
                dbt_manifest_ref = EXCLUDED.dbt_manifest_ref,
                change_ref = COALESCE(EXCLUDED.change_ref, registry.run.change_ref),
                -- Overwritten, not coalesced, unlike change_ref: a retry runs
                -- under whatever is deployed NOW, and if that changed between
                -- attempts then the new deployment is what produced the
                -- publication. Keeping the first attempt's would attribute the
                -- result to a version that did not produce it.
                dbt_project_ref = EXCLUDED.dbt_project_ref,
                deployment_change_ref = EXCLUDED.deployment_change_ref,
                deployment_pipeline_ref = EXCLUDED.deployment_pipeline_ref,
                error = NULL
            """,
            (run_id, purpose, RUNNING, environment, dag_id, airflow_run_id,
             branch, code_ref, code_ref_kind, dbt_manifest_ref, change_ref,
             prov.get("dbt_project_ref") or None,
             prov.get("deployment_change_ref") or None,
             prov.get("deployment_pipeline_ref") or None))
    log.info("run %s opened (%s) on %s, code %s/%s", run_id, purpose, branch,
             code_ref_kind, code_ref)
    return {"run_id": run_id, "purpose": purpose, "status": RUNNING}


def finish_run(run_id: str, status: str, *, merged_hash: str | None = None,
               cob_date: date | None = None,
               error: str | None = None) -> None:
    """Close a run. `status` is PUBLISHED or FAILED."""
    if status not in (PUBLISHED, FAILED):
        raise ValueError(f"a run finishes {PUBLISHED} or {FAILED}, got {status!r}")
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE registry.run SET status = %s, finished_at = now(), "
            "merged_hash = COALESCE(%s, merged_hash), "
            "cob_date = COALESCE(%s, cob_date), "
            "error = %s WHERE run_id = %s",
            (status, merged_hash, cob_date, (error or None)[:2000]
             if error else None, run_id))
        if cur.rowcount == 0:
            # Not fatal, but it means the run was never opened -- which is a
            # gap in the record, not a cosmetic one.
            log.warning("no run row for %s to finish; it was never opened",
                        run_id)


def record_inputs(run_id: str, inputs: list[tuple[str, str]]) -> int:
    """Attach the deliveries a run read. Replaces whatever was there.

    Wholesale replacement rather than an upsert, for `deliveries.write`'s
    reason one level up: a re-run of the publish step against a rebuilt branch
    legitimately has a different input set, and merging would leave the
    deliveries it no longer reads behind as rows claiming it did.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM registry.run_input WHERE run_id = %s", (run_id,))
        for feed, delivery_id in sorted(set(inputs)):
            cur.execute(
                "INSERT INTO registry.run_input (run_id, feed, delivery_id) "
                "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (run_id, feed, delivery_id))
    log.info("run %s: %d input delivery(ies) recorded", run_id, len(set(inputs)))
    return len(set(inputs))


def allocate_version(report: str, as_at_date: date, run_id: str,
                     tag: str) -> int:
    """The next version number for (report, as-at date). REQ-401, decision 5.

    ALLOCATED UNDER A TRANSACTION-SCOPED ADVISORY LOCK, so two publications of
    the same report and date cannot both read the same maximum and both write
    it. That read-then-write is what `sequence_no` avoided by being a
    BIGSERIAL, and a plain sequence will not do here because the number
    restarts per (report, as-at date) rather than running globally.

    IT WAS `SELECT MAX(...) ... FOR UPDATE`, WHICH POSTGRES REFUSES:

        FeatureNotSupported: FOR UPDATE is not allowed with aggregate functions

    -- and the refusal only appeared when a publication actually reached this
    code, which is the first time a reporting build published. The row lock
    was the wrong instrument anyway: there is no row to lock until the first
    version of a report exists, so it could not have serialised the case that
    matters. `pg_advisory_xact_lock` locks the (report, date) PAIR whether or
    not a row exists yet, and is released when the transaction ends however it
    ends. The primary key stays as the backstop.

    Idempotent on the TAG: republishing the same tag returns the version it
    already has rather than minting a second one, because a retried publish
    task is the same publication.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_no FROM registry.report_version WHERE tag = %s",
                    (tag,))
        row = cur.fetchone()
        if row:
            log.info("report %s %s already published as v%d (tag %s)",
                     report, as_at_date, row[0], tag)
            return int(row[0])
        # Two-argument form: two int4 keys rather than one int8, so the two
        # halves of the identity stay visible in pg_locks. A hash collision
        # serialises two unrelated (report, date) pairs for the length of one
        # insert, which costs nothing.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    (report, as_at_date.isoformat()))
        cur.execute(
            "SELECT COALESCE(MAX(version_no), 0) FROM registry.report_version "
            "WHERE report = %s AND as_at_date = %s",
            (report, as_at_date))
        version_no = int(cur.fetchone()[0]) + 1
        cur.execute(
            "INSERT INTO registry.report_version "
            "(report, as_at_date, version_no, run_id, tag) "
            "VALUES (%s, %s, %s, %s, %s)",
            (report, as_at_date, version_no, run_id, tag))
    log.info("report %s %s published as v%d (tag %s)", report, as_at_date,
             version_no, tag)
    return version_no


def record_submission(destination: str, submitted_by: str,
                      items: list[tuple[str, date, int]], *,
                      family: str | None = None, note: str | None = None,
                      submission_id: str | None = None) -> dict[str, Any]:
    """Record that published versions were SENT somewhere. REQ-402.

    The platform does not submit anything and this does not pretend to: it is
    the record that a human or another system did, written at the time rather
    than reconstructed from email afterwards. `family` is decision 5's other
    half -- reports submitted together as one return are grouped here, on the
    submission, rather than by sharing a version sequence.
    """
    import uuid

    sid = submission_id or f"sub-{uuid.uuid4().hex[:12]}"
    if not items:
        raise ValueError("a submission with no versions in it records nothing")
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO registry.submission "
            "(submission_id, family, destination, submitted_by, note) "
            "VALUES (%s, %s, %s, %s, %s)",
            (sid, family, destination, submitted_by, note))
        for report, as_at_date, version_no in items:
            # The foreign key is the check: submitting a version that was
            # never published fails here rather than being recorded as though
            # it had been.
            cur.execute(
                "INSERT INTO registry.submission_item "
                "(submission_id, report, as_at_date, version_no) "
                "VALUES (%s, %s, %s, %s)",
                (sid, report, as_at_date, version_no))

    # REQ-500. Submitting CLOSES the as-at date, and it is written here rather
    # than left to whoever submitted to remember. A date whose figure has left
    # the building and is still `open` is the exact gap the lifecycle exists to
    # close, and it would be created by omission every time.
    #
    # Written after the submission commits, in its own transaction: a
    # lifecycle write that failed must not roll back the record that the
    # submission happened. The submission is the event; the state is derived
    # from it and can be re-derived by locking the date by hand.
    for report, as_at_date, version_no in items:
        try:
            lifecycle.transition(
                report, as_at_date, lifecycle.SUBMITTED,
                actor=submitted_by,
                reason=f"submitted v{version_no} to {destination}",
                submission_id=sid)
        except Exception as exc:                                # noqa: BLE001
            log.error("submission %s recorded, but %s %s could not be moved "
                      "to %s: %s -- lock it by hand", sid, report, as_at_date,
                      lifecycle.SUBMITTED, f"{type(exc).__name__}: {exc}")
    return {"submission_id": sid, "family": family, "destination": destination,
            "items": len(items)}


def diff(report: str, as_at_date: date, from_version: int | None = None,
         to_version: int | None = None) -> dict[str, Any]:
    """What changed between two published versions of one report and date.

    §11. NEARLY FREE, because phase 5 made a run enumerate the deliveries
    behind what it published: a v14 -> v15 comparison is the set difference of
    two `run_input` sets, and the `change_ref` on each run says why. Nothing
    here computes a diff of the DATA -- the published tables are pinned by
    their tags and can be compared directly with `nessie_ref`; what was missing
    was the ability to say which INPUTS differ, which no query over the tables
    can answer.

    THE DELIVERY JOIN IS A LEFT JOIN, because `run_input` deliberately has no
    foreign key to `delivery` (see registry/db.py). A delivery the registry
    cannot describe is reported with nulls rather than dropped -- dropping it
    would remove it from both sides equally and make a real difference look
    like agreement.

    Defaults are "the last two versions", which is the comparison anybody
    actually wants and the one that is tedious to type.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT version_no, run_id, tag, created_at "
            "FROM registry.report_version "
            "WHERE report = %s AND as_at_date = %s ORDER BY version_no",
            (report, as_at_date))
        versions_here = [dict(zip(("version_no", "run_id", "tag", "created_at"), r))
                         for r in cur.fetchall()]
        if len(versions_here) < 2 and (from_version is None or to_version is None):
            raise ValueError(
                f"{report} {as_at_date} has "
                f"{len(versions_here)} published version(s); a diff needs two. "
                f"Name them explicitly if you meant a different pair.")

        by_no = {v["version_no"]: v for v in versions_here}
        lo = by_no.get(from_version if from_version is not None
                       else versions_here[-2]["version_no"])
        hi = by_no.get(to_version if to_version is not None
                       else versions_here[-1]["version_no"])
        for want, got in ((from_version, lo), (to_version, hi)):
            if got is None:
                raise ValueError(
                    f"{report} {as_at_date} has no version {want}. Published: "
                    f"{', '.join(str(v['version_no']) for v in versions_here)}")

        def _inputs(run_id: str) -> dict[tuple[str, str], dict[str, Any]]:
            cur.execute(
                "SELECT i.feed, i.delivery_id, d.cob_date, d.received_at, "
                "       d.md5, d.source_filename "
                "FROM registry.run_input i "
                "LEFT JOIN registry.delivery d "
                "  ON d.feed = i.feed AND d.delivery_id = i.delivery_id "
                "WHERE i.run_id = %s", (run_id,))
            cols = [c[0] for c in cur.description]
            return {(r[0], r[1]): dict(zip(cols, r)) for r in cur.fetchall()}

        old_inputs, new_inputs = _inputs(lo["run_id"]), _inputs(hi["run_id"])

        cur.execute(
            "SELECT run_id, code_ref, code_ref_kind, dbt_manifest_ref, "
            "       change_ref, merged_hash, status, finished_at "
            "FROM registry.run WHERE run_id = ANY(%s)",
            ([lo["run_id"], hi["run_id"]],))
        cols = [c[0] for c in cur.description]
        run_rows = {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}

    lo_run = run_rows.get(lo["run_id"], {})
    hi_run = run_rows.get(hi["run_id"], {})
    added = sorted(new_inputs.keys() - old_inputs.keys())
    removed = sorted(old_inputs.keys() - new_inputs.keys())
    return {
        "report": report, "as_at_date": as_at_date,
        "from": {**lo, "run": lo_run}, "to": {**hi, "run": hi_run},
        "deliveries": {
            "added": [new_inputs[k] for k in added],
            "removed": [old_inputs[k] for k in removed],
            "unchanged": len(new_inputs.keys() & old_inputs.keys()),
        },
        "code": {
            # Named rather than left to be compared by eye. A version pair
            # whose deliveries are identical and whose code_ref differs is a
            # rebuild, not a restatement, and that is the commonest real diff.
            "code_ref_changed":
                lo_run.get("code_ref") != hi_run.get("code_ref"),
            "dbt_project_changed":
                lo_run.get("dbt_manifest_ref") != hi_run.get("dbt_manifest_ref"),
            "from_code_ref": lo_run.get("code_ref"),
            "to_code_ref": hi_run.get("code_ref"),
            "from_change_ref": lo_run.get("change_ref"),
            "to_change_ref": hi_run.get("change_ref"),
        },
    }


# ------------------------------------------------------------------ reading
def run_for_tag(tag: str) -> dict[str, Any] | None:
    """The run behind a published tag, or None if nothing recorded one.

    None is the ORDINARY answer for a tag cut before this existed, and callers
    are expected to fall back rather than fail -- see monitoring/evidence.py.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT r.run_id, r.purpose, r.status, r.cob_date, "
            "       r.code_ref, r.code_ref_kind, r.dbt_manifest_ref, "
            "       r.change_ref, r.merged_hash, r.finished_at, "
            "       v.report, v.as_at_date, v.version_no "
            "FROM registry.report_version v "
            "JOIN registry.run r ON r.run_id = v.run_id "
            "WHERE v.tag = %s", (tag,))
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip([d[0] for d in cur.description], row))


def inputs_for_run(run_id: str) -> list[dict[str, str]]:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT feed, delivery_id FROM registry.run_input "
                    "WHERE run_id = %s ORDER BY feed, delivery_id", (run_id,))
        return [{"feed": f, "delivery_id": d} for f, d in cur.fetchall()]


def recent(limit: int = 20, purpose: str | None = None) -> list[dict[str, Any]]:
    sql = ("SELECT run_id, purpose, status, environment, branch, cob_date, "
           "       started_at, finished_at, code_ref, code_ref_kind, "
           "       dbt_manifest_ref, change_ref, dbt_project_ref, "
           "       deployment_change_ref, deployment_pipeline_ref, "
           "       merged_hash, error "
           "FROM registry.run")
    args: list[Any] = []
    if purpose:
        sql += " WHERE purpose = %s"
        args.append(purpose)
    sql += " ORDER BY started_at DESC LIMIT %s"
    args.append(limit)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def versions(report: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    sql = ("SELECT v.report, v.as_at_date, v.version_no, v.tag, v.run_id, "
           "       v.created_at, r.status, r.code_ref, r.change_ref "
           "FROM registry.report_version v "
           "LEFT JOIN registry.run r ON r.run_id = v.run_id")
    args: list[Any] = []
    if report:
        sql += " WHERE v.report = %s"
        args.append(report)
    sql += " ORDER BY v.created_at DESC LIMIT %s"
    args.append(limit)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def submissions(limit: int = 50) -> list[dict[str, Any]]:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT s.submission_id, s.family, s.destination, s.submitted_at, "
            "       s.submitted_by, s.note, i.report, i.as_at_date, i.version_no "
            "FROM registry.submission s "
            "LEFT JOIN registry.submission_item i "
            "  ON i.submission_id = s.submission_id "
            "ORDER BY s.submitted_at DESC, i.report LIMIT %s", (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
