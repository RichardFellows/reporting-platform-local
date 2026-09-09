"""The as-at date lifecycle: what may still be published, and on whose say-so.

REQ-500..503. One state machine per (report, as-at date), keyed on exactly that
pair because that is what a version number is keyed on. A lifecycle keyed on
the run would move when an unrelated report was rebuilt; one keyed on a
submission family would move when a sibling was restated.

THE REPORT IS A dbt EXPOSURE, derived by `context.reports()` -- not a new list
that would disagree with that one the first time a model moved. So is the
restatement policy (`meta: restatement:`) and the owner (`owner: name:`), so
the lifecycle adds NO new registry of reports.

  open       no row. An as-at date nobody has locked -- the ordinary state.
  locked     closed to routine republication. A publish whose input set for
             that date has MOVED is refused; one that has not is allowed, so
             an unrelated rebuild of a locked date is not an incident.
  submitted  a version was sent somewhere. Everything `locked` means, plus a
             stronger reopening requirement. Written by `record_submission`.
  reopened   somebody deliberately reopened it. The next publish is allowed
             and takes the next version number.

WHAT THIS PLATFORM CAN AND CANNOT ENFORCE. There is no identity provider here:
nothing can verify that whoever typed `--actor jane` is Jane, and a module
claiming to enforce authority would be theatre. What it CAN do:

  * refuse any transition that does not name an ACTOR and a REASON, so the
    record is never "somebody unlocked this at some point";
  * require, when reopening a SUBMITTED date, an `approved_by` matching the
    report's declared owner -- a real check, because the owner comes from the
    exposure and cannot be supplied by the person reopening.

Authority is RECORDED and checked against the project's own declaration, not
enforced against an identity the platform does not have.

WHERE THE GATE RUNS, and it is not where it looks like it should. The publish
task merges the audited branch into `main` and only THEN cuts tags and
allocates versions, so a check sitting with the versioning would fire after
`main` had moved. `check_publishable()` is called between reading the input set
(where the as-at date first becomes known) and the merge. A refusal there fails
the task with `main` untouched and the branch retained.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from reporting_platform.registry import db

log = logging.getLogger("registry.lifecycle")

OPEN, LOCKED, SUBMITTED, REOPENED = "open", "locked", "submitted", "reopened"

# States that CLOSE a date to routine republication. `reopened` is deliberately
# not one: reopening exists precisely to leave that set.
CLOSED = (LOCKED, SUBMITTED)

# What may be written. `open` is absent on purpose -- it is the absence of a
# row, so there is nothing to write and no way to "set" it except by reopening.
WRITABLE = (LOCKED, SUBMITTED, REOPENED)


class LifecycleRefused(Exception):
    """A transition or a publication the lifecycle will not allow.

    Its own type rather than ValueError so the publish path can distinguish
    "this report may not be published onto this date" -- a policy answer, with
    a named owner who can change it -- from a bug in the code that asked.
    """


# ------------------------------------------------------------------ reading
def state(report: str, as_at_date: date) -> dict[str, Any]:
    """The current state of one (report, as-at date). Never None.

    `open` is synthesised from the absence of any row rather than stored, so a
    report's first publication needs no lifecycle set-up and a date nobody has
    touched has no row to go stale.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT state, occurred_at, actor, reason, approved_by, "
            "       submission_id, transition_id "
            "FROM registry.as_at_transition "
            "WHERE report = %s AND as_at_date = %s "
            "ORDER BY occurred_at DESC, transition_id DESC LIMIT 1",
            (report, as_at_date))
        row = cur.fetchone()
    if not row:
        return {"report": report, "as_at_date": as_at_date, "state": OPEN,
                "since": None, "actor": None, "reason": None,
                "approved_by": None, "submission_id": None}
    cols = ("state", "since", "actor", "reason", "approved_by",
            "submission_id", "transition_id")
    out = dict(zip(cols, row))
    out.update(report=report, as_at_date=as_at_date)
    return out


def history(report: str | None = None, as_at_date: date | None = None,
            limit: int = 100) -> list[dict[str, Any]]:
    """Every transition, newest first. The audit this table exists for."""
    sql = ("SELECT report, as_at_date, state, occurred_at, actor, reason, "
           "       approved_by, submission_id "
           "FROM registry.as_at_transition")
    where, args = [], []
    if report:
        where.append("report = %s")
        args.append(report)
    if as_at_date:
        where.append("as_at_date = %s")
        args.append(as_at_date)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY occurred_at DESC, transition_id DESC LIMIT %s"
    args.append(limit)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def open_dates(report: str | None = None) -> list[dict[str, Any]]:
    """The current state of every (report, as-at date) that has one.

    Dates in `open` have no row and so are absent -- which is correct: the
    question this answers is "what has been locked, submitted or reopened",
    and listing every date the platform has ever published as `open` would
    bury that in the ordinary case.
    """
    sql = ("SELECT DISTINCT ON (report, as_at_date) report, as_at_date, "
           "       state, occurred_at, actor, reason, approved_by "
           "FROM registry.as_at_transition")
    args: list[Any] = []
    if report:
        sql += " WHERE report = %s"
        args.append(report)
    sql += (" ORDER BY report, as_at_date DESC, occurred_at DESC, "
            "transition_id DESC")
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ------------------------------------------------------------- transitions
def transition(report: str, as_at_date: date, to: str, *, actor: str,
               reason: str, approved_by: str | None = None,
               submission_id: str | None = None) -> dict[str, Any]:
    """Move a (report, as-at date) to `to`. Appends; never updates.

    THE REPORT MUST EXIST as an exposure. Locking a report name with a typo in
    it would record a lock nothing ever reads and leave the real date open --
    a control that silently protects nothing, which is worse than no control.

    THE APPROVER RULE, and it is the whole of the enforcement:

      * reopening a SUBMITTED date requires `approved_by` equal to the
        report's exposure owner. That is checkable because the owner comes
        from the project, not from the caller.
      * reopening a merely LOCKED date requires an actor and a reason and no
        approver. A lock is an internal control; a submission has left the
        building, and the two should not cost the same to undo.
      * locking requires neither, because locking is the safe direction.
    """
    from reporting_platform.common.context import report as get_report
    from reporting_platform.common.context import report_owner

    if to not in WRITABLE:
        raise LifecycleRefused(
            f"{to!r} is not a state that can be written. Valid: "
            f"{', '.join(WRITABLE)}. `{OPEN}` is the ABSENCE of a transition, "
            f"so a date returns to it by being reopened, not by being set.")
    get_report(report)                          # refuses an unknown report
    actor = (actor or "").strip()
    reason = (reason or "").strip()
    if not actor or not reason:
        raise LifecycleRefused(
            "a lifecycle transition must name an ACTOR and a REASON. This "
            "platform has no identity provider and cannot verify who you are, "
            "so the record of who decided and why is the only thing it has -- "
            "and an unattributed lock is one nobody can ask about later.")

    current = state(report, as_at_date)
    if to == REOPENED and current["state"] not in CLOSED:
        raise LifecycleRefused(
            f"{report} {as_at_date} is {current['state']}, not "
            f"{' or '.join(CLOSED)}. There is nothing to reopen: an open date "
            f"already accepts a publication.")
    if to == REOPENED and current["state"] == SUBMITTED:
        owner = report_owner(report)
        if (approved_by or "").strip().lower() != owner.lower():
            raise LifecycleRefused(
                f"reopening {report} {as_at_date} needs `approved_by` to be "
                f"{owner!r}, the owner declared on the report's dbt exposure "
                f"(REQ-501). A version for this date has already been "
                f"submitted, so restating it changes a figure somebody else "
                f"is holding. Got {approved_by!r}.")

    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO registry.as_at_transition "
            "(report, as_at_date, state, actor, reason, approved_by, "
            " submission_id) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "RETURNING transition_id, occurred_at",
            (report, as_at_date, to, actor, reason,
             (approved_by or "").strip() or None, submission_id))
        transition_id, occurred_at = cur.fetchone()
    log.info("%s %s: %s -> %s by %s (%s)", report, as_at_date,
             current["state"], to, actor, reason)
    return {"report": report, "as_at_date": as_at_date, "state": to,
            "from": current["state"], "transition_id": transition_id,
            "occurred_at": occurred_at, "actor": actor, "reason": reason,
            "approved_by": approved_by, "submission_id": submission_id}


def lock(report: str, as_at_date: date, *, actor: str, reason: str):
    return transition(report, as_at_date, LOCKED, actor=actor, reason=reason)


def reopen(report: str, as_at_date: date, *, actor: str, reason: str,
           approved_by: str | None = None):
    return transition(report, as_at_date, REOPENED, actor=actor, reason=reason,
                      approved_by=approved_by)


# ------------------------------------------------------------------ the gate
def inputs_changed(report: str, as_at_date: date,
                   candidate: set[tuple[str, str]]) -> dict[str, Any]:
    """Whether `candidate` differs from the input set of the version in force.

    THE COMPARISON IS RESTRICTED TO DELIVERIES FOR THIS AS-AT DATE. A run's
    input set spans every COB date its tables hold, so comparing the sets whole
    would report a change on every ordinary daily build. What the lifecycle
    asks is narrower and is the question REQ-502 poses: has what we published
    FOR THIS DATE moved?

    The COB date comes from `registry.delivery`, joined without a foreign key
    (see registry/db.py) -- so a delivery the registry has never seen is
    reported as `unregistered` rather than silently dropped from both sides,
    where it would look like agreement.

    `unregistered` MEANS UNKNOWN TO THE REGISTRY, NOT "FOR ANOTHER DATE", and
    the difference is most of the input set. A run's inputs are what it
    PUBLISHED: `ref_counterparty` is SCD2, so one reporting build reads ten of
    its forty deliveries and thirty-odd for other COB dates are in `candidate`
    perfectly legitimately. Verified against a real 126-delivery input set
    where 85 are for other dates -- an earlier version called all 85
    `unregistered`, sending somebody to look for a registry gap that is not
    there. They are counted as `off_date` instead.

    AN `unregistered` DELIVERY DOES NOT FLIP `changed`. It cannot be attributed
    to a COB date, so it cannot be said to have moved this date's input set;
    blocking a publication on it would refuse exactly when the registry is
    behind, which is when it is least useful. It is reported instead, and
    `deliveries.reconcile()` is what resolves it.

    Returns `{"changed": bool, ...}` with the sets, so a refusal can say what
    moved rather than that something did.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT v.version_no, v.run_id FROM registry.report_version v "
            "WHERE v.report = %s AND v.as_at_date = %s "
            "ORDER BY v.version_no DESC LIMIT 1", (report, as_at_date))
        row = cur.fetchone()
        if not row:
            # Nothing has been published for this date, so nothing can have
            # moved. A locked date with no version is a date somebody closed
            # before it was ever produced, which is legitimate.
            return {"changed": False, "published_version": None,
                    "added": [], "removed": [], "unregistered": [],
                    "off_date": 0}
        version_no, run_id = row

        cur.execute(
            "SELECT i.feed, i.delivery_id "
            "FROM registry.run_input i "
            "JOIN registry.delivery d "
            "  ON d.feed = i.feed AND d.delivery_id = i.delivery_id "
            "WHERE i.run_id = %s AND d.cob_date = %s",
            (run_id, as_at_date))
        published = {(f, d) for f, d in cur.fetchall()}

        # The candidate set, resolved by the same authority -- but asked about
        # THE PAIRS THEMSELVES rather than about the date. Selecting every
        # delivery for this COB date and calling the remainder unknown
        # conflates "the registry has never seen this" with "this belongs to
        # another date", and the second is the ordinary case.
        on_date: set[tuple[str, str]] = set()
        seen: set[tuple[str, str]] = set()
        if candidate:
            cur.execute(
                "SELECT feed, delivery_id, cob_date FROM registry.delivery "
                "WHERE (feed, delivery_id) IN %s", (tuple(sorted(candidate)),))
            for feed_name, delivery_id, cob_date in cur.fetchall():
                seen.add((feed_name, delivery_id))
                if cob_date == as_at_date:
                    on_date.add((feed_name, delivery_id))

    known = {p for p in candidate if p in on_date}
    unregistered = sorted(p for p in candidate if p not in seen)
    off_date = len(candidate) - len(known) - len(unregistered)
    added = sorted(known - published)
    removed = sorted(published - known)
    return {"changed": bool(added or removed), "published_version": version_no,
            "published_run": run_id, "added": added, "removed": removed,
            "unregistered": unregistered, "off_date": off_date}


def check_publishable(report: str, as_at_date: date,
                      candidate: set[tuple[str, str]]) -> dict[str, Any]:
    """May `report` be published for `as_at_date`? Raises if not. REQ-502.

    THE POLICY ONLY BITES ON A CLOSED DATE WHOSE INPUTS HAVE MOVED, and every
    word of that is load-bearing:

      * an OPEN or REOPENED date publishes, whatever changed. That is the
        ordinary daily path and it must not acquire a lifecycle cost;
      * a CLOSED date whose input set for that date is UNCHANGED publishes
        too. Rebuilding a locked date after a code change is not a
        restatement of the data and refusing it would make `lock` mean "this
        report may never be rebuilt", which is not what a lock is for;
      * a CLOSED date whose inputs HAVE moved is the case REQ-502 is about,
        and the report's declared policy answers it.

    `restate`       -- REFUSED. The figure must be republished deliberately,
                       so somebody reopens the date and the restatement is a
                       decision with a name on it. The error says what moved
                       and who may authorise it.
    `carry_forward` -- ALLOWED, and recorded. The published version stands and
                       the change surfaces at the current date; the returned
                       dict carries `carried_forward` so the publish step can
                       put it on the run rather than leave it in a task log.

    There is NO platform-wide default -- `restatement_policy()` refuses a
    report that has not declared one, which is open decision 2 settled as
    `fail`. REQ-503 falls out of there being no branch here on what KIND of
    report this is: a daily internal dashboard and a quarterly regulatory
    return traverse this function identically, and `tests/test_lifecycle.py`
    asserts it rather than a comment claiming it.
    """
    from reporting_platform.common.context import report_owner, restatement_policy

    current = state(report, as_at_date)
    policy = restatement_policy(report)          # refuses an undeclared one
    out = {"report": report, "as_at_date": as_at_date,
           "state": current["state"], "restatement": policy,
           "carried_forward": False, "changed": False}

    if current["state"] not in CLOSED:
        return out

    delta = inputs_changed(report, as_at_date, candidate)
    out.update({k: v for k, v in delta.items() if k != "changed"})
    out["changed"] = delta["changed"]
    if not delta["changed"]:
        return out

    what = (f"{len(delta['added'])} delivery(ies) added, "
            f"{len(delta['removed'])} removed")
    if policy == "carry_forward":
        out["carried_forward"] = True
        log.warning(
            "%s %s is %s and its inputs have moved (%s), but its restatement "
            "policy is carry_forward: v%s stands and the change surfaces at "
            "the current date. Owner: %s", report, as_at_date,
            current["state"], what, delta["published_version"],
            report_owner(report))
        return out

    raise LifecycleRefused(
        f"{report} {as_at_date} is {current['state']} and its input set has "
        f"changed since v{delta['published_version']} ({what}: "
        f"added {delta['added'][:5]}, removed {delta['removed'][:5]}). Its "
        f"restatement policy is `restate`, so this is a RESTATEMENT and not a "
        f"routine rebuild -- it needs somebody to reopen the date rather than "
        f"a build to do it quietly. Reopen it with:\n"
        f"    python -m reporting_platform.registry reopen "
        f"--report {report} --as-at {as_at_date} --actor YOU "
        f"--reason '...'"
        + (f" --approved-by '{report_owner(report)}'"
           if current["state"] == SUBMITTED else ""))
