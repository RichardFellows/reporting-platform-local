"""What arrived, what the gate made of it, and what happened next.

THE JOURNEY OF ONE FILE, assembled from what already records each leg of it.
The console had every stage of the platform on screen except the first one:
a file was dropped into `inbox/`, and the next thing anybody saw was either a
raw table with more rows in it or nothing at all. "Did it arrive?", "was it
recognised?", "did its control file agree with it?" and "did an ingest
actually start?" were four different places to look -- a container's log, a
Postgres table, an object prefix and Airflow's UI -- and only the first of
them said anything at all about a file that never got past the door.

NOTHING HERE IS A NEW RECORD. There is no arrivals table, no status column and
nothing written; every field below is read from something that already holds
it, and that is deliberate:

  * `registry.delivery` -- what was accepted, under what name, out of what the
    upstream called it, and what its control file declared.
  * `registry.rejection` -- what was refused, and which CLASS of refusal.
  * `inbox/` itself, through `inbox.route()` -- what is sitting at the door
    right now, and which feed (if any) claims it. The gate's own function, not
    a second reading of the patterns.
  * Airflow -- the ingest run that followed, by the key it was told to ingest.

Adding a fifth place that recorded the same journey would make the console the
authority on it, and the console is the one component here that must never be
that (see `ui/__init__.py`). It would also be the first thing to drift: a row
written when a file arrives cannot be rebuilt from object storage, which is
precisely the property `registry/deliveries.py` was designed around.

WHAT THIS CANNOT TELL YOU, and says so rather than implying otherwise:

  * **Whether the rows reached raw.** That is derived from `_source_file` in
    the raw table and costs a Spark job; the ingest run's state is what is
    shown instead, and a delivery Airflow no longer remembers a run for reads
    `no run recorded`, never `not ingested`.
  * **Whether the row count matched.** Only Spark counts the rows, at ingest.
    The DECLARED count is shown here, with the ingest run beside it as the
    verdict -- an ingest that failed on the count failed the task.
  * **Whether a checksum matched, before an ingest has run.** The comparison
    below is between two things the registry recorded -- what the control file
    declared and what the landed object hashes to -- which is the same
    comparison `ingest_feed` makes and NOT a substitute for it having been
    made. See `checks()`.
"""
from __future__ import annotations

import ast
import logging
from datetime import datetime, timezone
from typing import Any

from reporting_platform.registry import deliveries, rejections

from . import orchestration

log = logging.getLogger("ui.arrivals")

# The task whose XCom names the delivery a run without conf resolved for
# itself. See `_ingested_key`.
RESOLVE_TASK = "resolve_arrival"


# --------------------------------------------------------------- at the door
def _classification(fd, filename: str, is_control: bool) -> str:
    """WHICH of this feed's patterns claimed the name.

    `route()` has already decided WHICH FEED, over every feed in `feeds.yml`;
    this only names the pattern, and it asks the feed the same questions in
    the same order `route` asks them in. That order cannot produce a different
    answer here: `route` tries `filename_pattern` across every feed before any
    `arrival.source_pattern`, so a feed it returned for a source pattern is a
    feed whose own `parse_filename` did not match.

      conformant  the name landing's contract already accepts -- uploaded
                  under its own name, no rename, no control file needed
      gated       the name the upstream sends -- goes through the conformance
                  gate and is promoted under a name `filename_pattern`
                  describes
      control     a control file: it names no COB date of its own, it says
                  something about the delivery that does
    """
    if is_control:
        return "control"
    if fd.parse_filename(filename) is not None:
        return "conformant"
    return "gated"


def inbox_state(processed_limit: int = 20) -> dict[str, Any]:
    """What is at the door right now, and what recently went through it.

    THE WATCHER'S OWN VIEW, not a second one. `route()` is the function the
    sweep calls to decide where a file goes, so a file shown here as claimed
    by no feed is a file the next sweep will quarantine -- which is the
    question this answers that nothing else did: a name nobody has onboarded
    looks exactly like a name that is about to be picked up, right up until
    it is not.

    A file listed under `waiting` has NOT necessarily been seen yet. The
    watcher requires two consecutive polls with an unchanged size and mtime
    before it touches anything (`inbox.STABLE_POLLS`), so a file written
    seconds ago is legitimately still sitting there. `modified_at` is what
    tells those apart, and it is why the age is shown rather than a
    "processing" spinner that would be a guess.
    """
    from reporting_platform.ingest import inbox as ib

    out: dict[str, Any] = {"path": str(ib.INBOX), "present": ib.INBOX.is_dir(),
                           "stable_polls": ib.STABLE_POLLS,
                           "waiting": [], "processed": []}
    if not out["present"]:
        return out

    for path in sorted(ib.INBOX.iterdir()):
        if ib._skip(path):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:       # picked up between listing and stat
            continue
        fd, reason, is_control = ib.route(path.name)
        out["waiting"].append({
            "filename": path.name,
            "bytes": stat.st_size,
            "modified_at": _iso(stat.st_mtime),
            "feed": fd.name if fd else None,
            # `ambiguous` is a configuration error and `unroutable` usually is
            # not, which is why the gate counts them apart and so does this.
            "classification": (_classification(fd, path.name, is_control)
                               if fd else
                               "ambiguous" if "more than one" in (reason or "")
                               else "unroutable"),
            "reason": reason,
        })

    # `.processed/<feed>/` is the gate's record of what it moved, and it is
    # moved BEFORE the trigger (see inbox.py) -- so a file here whose delivery
    # has no registry row means the upload succeeded and something after it
    # did not, which is a different failure from the file never having landed.
    processed = ib.INBOX / ib.PROCESSED
    if processed.is_dir():
        rows = []
        for feed_dir in processed.iterdir():
            if not feed_dir.is_dir():
                continue
            for path in feed_dir.iterdir():
                if ib._skip(path):
                    continue
                rows.append({"filename": path.name, "feed": feed_dir.name,
                             "bytes": path.stat().st_size,
                             "processed_at": _iso(path.stat().st_mtime)})
        rows.sort(key=lambda r: r["processed_at"], reverse=True)
        out["processed"] = rows[:processed_limit]
    return out


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


# ------------------------------------------------------------- what arrived
def checks(row: dict[str, Any]) -> dict[str, Any]:
    """The integrity pair for one delivery, as far as the registry can answer.

    TWO DIFFERENT KINDS OF ANSWER, and running them together would be the
    misleading thing to do:

    **The checksum is answerable here.** `declared_md5` is what the control
    file said; `md5` is what the landed object hashes to, measured once when
    the delivery was registered. `ingest_feed._parts_md5` hashes the same
    bytes -- every feed that can carry a `delivery.control` block is
    single-part by construction (`kind: archive` is refused a control block at
    load), so the delivery's one part IS its source object. Comparing them is
    therefore the same comparison ingest makes rather than an approximation of
    it. A delivery with more than one part is reported `not_comparable` rather
    than guessed at: that combination cannot arise today, and inventing an
    answer for it is how it would be wrong the day it does.

    **The row count is not.** Nothing counts rows without reading the file,
    and reading the file is a Spark job. `declared` is what the control file
    stated and the verdict is `at_ingest` -- the ingest task compares it and
    fails the run on a mismatch, so the run's state IS the verdict and is
    shown beside it.

    NEITHER IS A CLAIM THAT THE CHECK HAS BEEN MADE. A delivery can sit in
    landing for days with a checksum that agrees perfectly and never be
    ingested. `verdict: ok` says the two recorded values match, and the ingest
    run beside it says whether anything acted on that.
    """
    declared_md5 = (row.get("declared_md5") or "").strip().lower()
    landed_md5 = (row.get("md5") or "").strip().lower()
    parts = row.get("parts")
    parts = len(parts) if isinstance(parts, list) else (parts or 1)

    if not declared_md5:
        md5_verdict = "not_declared"
    elif parts > 1:
        md5_verdict = "not_comparable"
    elif declared_md5 == landed_md5:
        md5_verdict = "ok"
    else:
        md5_verdict = "mismatch"

    declared_rows = row.get("declared_row_count")
    return {
        "row_count": {"declared": declared_rows,
                      "verdict": "at_ingest" if declared_rows is not None
                                 else "not_declared"},
        "md5": {"declared": row.get("declared_md5"), "landed": row.get("md5"),
                "verdict": md5_verdict},
        "control_object": row.get("control_object"),
    }


def _accepted(row: dict[str, Any]) -> dict[str, Any]:
    """One registered delivery, in the arrivals shape."""
    source_name = row.get("source_filename") or row["source_object"].rsplit("/", 1)[-1]
    return {
        "kind": "delivery",
        # The delivery's own arrival time, which is what both halves of this
        # list are sorted on -- a rejection's `received_at` is the same
        # measurement of the same event.
        "at": _stamp(row.get("received_at")),
        "first_seen_at": _stamp(row.get("first_seen_at")),
        "feed": row["feed"],
        "source_system": row.get("source_system"),
        "outcome": "landed",
        "source_name": source_name,
        "delivery_id": row["delivery_id"],
        # TWO FILENAMES AND THEY ARE DIFFERENT STRINGS for exactly the feeds
        # that went through the gate. Saying which is which matters: the
        # upstream will only ever know the first one.
        "renamed": source_name != row["delivery_id"],
        "source_container": row.get("source_container"),
        "cob_date": _stamp(row.get("cob_date")),
        "origin": row.get("origin"),
        "normalizer": row.get("normalizer"),
        "bytes": row.get("bytes"),
        # ALWAYS A COUNT here, whichever read produced the row: `recent`
        # returns the number and `by_id` the objects themselves, and one field
        # name holding two shapes is how a caller comes to render `[object
        # Object]`. The objects travel as `part_objects`, set by `detail`.
        "parts": (len(row["parts"]) if isinstance(row.get("parts"), list)
                  else row.get("parts")),
        "source_object": row.get("source_object"),
        "manifest_key": row.get("manifest_key"),
        "schema_version": row.get("schema_version"),
        "producer_run_id": row.get("producer_run_id"),
        "checks": checks(row),
    }


def _refused(row: dict[str, Any]) -> dict[str, Any]:
    """One rejection, in the same shape.

    A REFUSED DELIVERY IS AN ARRIVAL, and putting it in a separate list is how
    the console would come to show a feed as having received nothing on a
    morning when it received something unreadable. `feed` is null when nothing
    claimed the name -- which is a rejection the registry can still describe,
    and the commonest one.
    """
    return {
        "kind": "rejection",
        "at": _stamp(row.get("received_at")),
        "rejected_at": _stamp(row.get("rejected_at")),
        "feed": row.get("feed"),
        "outcome": "quarantined",
        "source_name": row.get("source_filename"),
        "delivery_id": None,
        "renamed": False,
        "reason_class": row.get("reason_class"),
        "reason": row.get("reason"),
        "bytes": row.get("bytes"),
        "md5": row.get("md5"),
        "quarantine_key": row.get("quarantine_key"),
    }


def _stamp(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else value


def recent(limit: int = 50, feed: str | None = None) -> dict[str, Any]:
    """Accepted and refused arrivals in one list, newest first.

    Both halves are read at `limit` and the merge is trimmed back to it, so a
    morning of rejections cannot push every delivery off the page or the other
    way round.
    """
    rows = [_accepted(r) for r in deliveries.recent(limit, feed)]
    rows += [_refused(r) for r in rejections.recent(limit, feed)]
    # `at` is an ISO string in UTC on both sides, so a lexical sort is a
    # chronological one. A row with no timestamp sorts last rather than
    # raising -- every column in the registry that feeds it is NOT NULL, so
    # this is defence against a row from a future schema, not an expected case.
    rows.sort(key=lambda r: r["at"] or "", reverse=True)
    # Trimmed BEFORE counting, so the summary describes the rows on screen.
    # Counting the merge would report four arrivals above a list of three, and
    # a header that disagrees with the table under it is worse than no header.
    rows = rows[:limit]
    return {"arrivals": rows, "limit": limit, "feed": feed,
            "counts": _counts(rows)}


def _counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    out = {"landed": 0, "quarantined": 0, "md5_mismatch": 0, "declared": 0}
    for r in rows:
        out[r["outcome"]] = out.get(r["outcome"], 0) + 1
        verdict = ((r.get("checks") or {}).get("md5") or {}).get("verdict")
        if verdict == "mismatch":
            out["md5_mismatch"] += 1
        if verdict in ("ok", "mismatch"):
            out["declared"] += 1
    return out


# ------------------------------------------------------- and what happened
def _ingested_key(dag_id: str, run: dict[str, Any]) -> str | None:
    """The object key this ingest run was told to work on.

    ONE RULE, READ WHEREVER IT ENDED UP. A run triggered by the inbox watcher
    or by this console carries the key in `dag_run.conf`, because both have it
    in hand when they trigger. A run triggered with no conf resolves its own
    delivery -- `resolve_arrival` takes the oldest pending one -- and the key
    it chose is in that task's XCom. Both are the same fact: what this run
    ingested. The cheap one is tried first, and the second call is made only
    for the runs the first cannot answer.

    Airflow 2.10's XCom endpoint returns the value as a Python REPR, not as
    JSON, so it is read with `literal_eval` -- which evaluates literals and
    nothing else. A value it cannot parse gives None, and the run is then
    shown as an ingest of this feed that names no delivery, which is exactly
    what is known about it.
    """
    key = (run.get("conf") or {}).get("object_key")
    if key:
        return key
    try:
        value = orchestration.xcom(dag_id, run["run_id"], RESOLVE_TASK)
    except orchestration.AirflowError:
        return None
    if not isinstance(value, str):
        return (value or {}).get("object_key") if isinstance(value, dict) else None
    try:
        parsed = ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return None
    return parsed.get("object_key") if isinstance(parsed, dict) else None


def ingest_runs(feed_name: str, limit: int = 15) -> dict[str, Any]:
    """Recent ingest runs for one feed, each with the key it ingested.

    Returns `{"available": False}` rather than raising when Airflow is
    unreachable: the arrivals list is worth reading without it -- what arrived
    and what its control file declared are facts about object storage and
    Postgres -- and a page that failed as a whole because the scheduler is
    down would hide them.
    """
    dag_id = f"ingest_{feed_name}"
    try:
        runs = orchestration.recent_runs(dag_id, limit)
    except orchestration.AirflowError as exc:
        log.info("no ingest runs for %s: %s", feed_name, str(exc)[:200])
        return {"dag_id": dag_id, "available": False, "dag_known": None,
                "runs": [], "error": str(exc)[:300]}
    # NO RUNS AND NO DAG ARE DIFFERENT ANSWERS, and `recent_runs` returns the
    # same empty list for both -- it swallows the 404 so a feed Airflow has not
    # parsed yet does not take a page down. The registry outlives `feeds.yml`,
    # so "this feed has no ingest DAG any more" is a state the arrivals list
    # genuinely reaches, and reading it as "nothing has ever ingested this"
    # sends somebody looking for a lost run. Asked only when there are no runs,
    # which is when it is the cheap call rather than an extra one.
    dag_known = True
    if not runs:
        try:
            dag_known = orchestration.get_dag(dag_id) is not None
        except orchestration.AirflowError:
            dag_known = None
    for run in runs:
        run["object_key"] = _ingested_key(dag_id, run)
    return {"dag_id": dag_id, "available": True, "dag_known": dag_known,
            "runs": runs}


def _matches(run: dict[str, Any], delivery: dict[str, Any]) -> bool:
    """Is this run an ingest of this delivery?

    The key a run was given is a LANDING key when something triggered it with
    one and a MANIFEST key when it resolved its own, and `normalize` accepts
    either -- so both are the delivery's, and so is any of its parts (an
    archive member's key is what a re-run of a single part would name).
    """
    key = run.get("object_key")
    if not key:
        return False
    parts = delivery.get("parts")
    part_keys = ({p["object_key"] for p in parts}
                 if isinstance(parts, list) else set())
    return key in ({delivery.get("source_object"),
                    delivery.get("manifest_key")} | part_keys)


def with_runs(limit: int = 50, feed: str | None = None) -> dict[str, Any]:
    """`recent`, with each delivery's ingest run attached.

    ONE AIRFLOW CALL PER FEED IN THE LIST, not one per delivery. The runs of a
    feed's ingest DAG are fetched once and matched against every delivery of
    that feed, because the alternative -- asking Airflow about each row -- is
    a page that gets slower the more history the registry has, which is the
    wrong way round for a list whose whole job is to grow.
    """
    out = recent(limit, feed)
    wanted = {r["feed"] for r in out["arrivals"]
              if r["kind"] == "delivery" and r["feed"]}
    by_feed = {name: ingest_runs(name) for name in sorted(wanted)}
    for row in out["arrivals"]:
        if row["kind"] != "delivery":
            continue
        activity = by_feed.get(row["feed"], {})
        matched = [r for r in activity.get("runs", []) if _matches(r, row)]
        row["ingest"] = {
            "dag_id": activity.get("dag_id"),
            "available": activity.get("available", False),
            "dag_known": activity.get("dag_known"),
            # Newest first, as `recent_runs` returns them: a re-run after a
            # failure is the state anybody asking is asking about.
            "runs": matched,
            "state": matched[0]["state"] if matched else None,
        }
    out["airflow"] = {name: a.get("available", False)
                      for name, a in by_feed.items()}
    return out


def detail(feed_name: str, delivery_id: str) -> dict[str, Any] | None:
    """One delivery, its parts, its checks and every ingest run naming it."""
    row = deliveries.by_id(feed_name, delivery_id)
    if row is None:
        return None
    out = _accepted(row)
    out["part_objects"] = row["parts"]
    out["origin_uri"] = row.get("origin_uri")
    activity = ingest_runs(feed_name)
    matched = [r for r in activity.get("runs", []) if _matches(r, row)]
    out["ingest"] = {"dag_id": activity.get("dag_id"),
                     "available": activity.get("available", False),
                     "dag_known": activity.get("dag_known"),
                     "runs": matched,
                     "state": matched[0]["state"] if matched else None,
                     "error": activity.get("error")}
    return out
