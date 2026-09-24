"""Reads of `registry.delivery`: pure SQL, no object storage, no ingest code.

SPLIT OUT OF `deliveries.py` SO CORE CAN READ WHAT INGEST WRITES. The table's
DDL is `registry/db.py`'s, which is core; writing and reconciling a row needs
the ingest contracts (`arrival`, `normalize`, `transport`), which is why
`deliveries.py` ships with ingest. A published run's trace
(`runs.trace_version`) and the evidence monitor read the rows, and neither
should need an ingest install to do it. `deliveries.py` re-exports every name
here, so `deliveries.recent(...)` and friends still work.
See docs/PACKAGING.md.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from reporting_platform.registry import db


# ------------------------------------------------------------------ reading
# THE ARRIVALS READS. What `deliveries_on` and `deliveries_by_id` are for a
# date and for a run's input set, these are for "what has arrived lately" --
# the question the feed console's arrivals view asks. They live here rather
# than in the console because a read of the registry is the registry's shape
# to state, and a SELECT written in `ui/` would be the second place that knows
# what a delivery row holds.
_ARRIVAL_COLUMNS = (
    "feed, delivery_id, sequence_no, source_system, cob_date, received_at, "
    "first_seen_at, source_object, manifest_key, normalizer, bytes, md5, "
    "schema_version, origin, origin_uri, source_filename, source_container, "
    "control_object, declared_row_count, declared_md5, producer_run_id")


def recent(limit: int = 50, feed: str | None = None) -> list[dict[str, Any]]:
    """The most recent deliveries, newest first, with their part count.

    ORDERED BY `received_at`, NOT `first_seen_at`. The delivery's own arrival
    time is what "recent" means to somebody asking what has arrived; the
    registry's clock is when it got round to writing the row, so a reconcile
    back-filling a year of history in one pass would order that year by the
    minute it ran and put 2019 at the top. `sequence_no` breaks the tie, which
    for two deliveries with the same LastModified is registration order.

    The part count comes back rather than the parts: a list of 200 deliveries
    that each carried their members would be mostly archive members, and the
    one caller that wants them (`by_id`) is looking at one delivery.
    """
    sql = (f"SELECT {_ARRIVAL_COLUMNS}, "
           "  (SELECT count(*) FROM registry.delivery_part p "
           "    WHERE p.feed = d.feed AND p.delivery_id = d.delivery_id) "
           "  AS parts "
           "FROM registry.delivery d")
    args: list[Any] = []
    if feed:
        sql += " WHERE feed = %s"
        args.append(feed)
    sql += " ORDER BY received_at DESC, sequence_no DESC LIMIT %s"
    args.append(limit)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def by_id(feed: str, delivery_id: str) -> dict[str, Any] | None:
    """One delivery and the objects that hold its rows, or None.

    The parts are the join back to raw: `_source_file` is the PART's key, so
    this is what turns a row in the raw table into the delivery it arrived in.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_ARRIVAL_COLUMNS} FROM registry.delivery "
                    "WHERE feed = %s AND delivery_id = %s",
                    (feed, delivery_id))
        row = cur.fetchone()
        if row is None:
            return None
        out = dict(zip([c[0] for c in cur.description], row))
        cur.execute("SELECT part_no, object_key, bytes "
                    "FROM registry.delivery_part "
                    "WHERE feed = %s AND delivery_id = %s ORDER BY part_no",
                    (feed, delivery_id))
        out["parts"] = [{"part_no": r[0], "object_key": r[1], "bytes": r[2]}
                        for r in cur.fetchall()]
    return out


def deliveries_on(cob_date: date, feed: str | None = None
                  ) -> list[dict[str, Any]]:
    """Every registered delivery for one COB date. Used by REQ-602."""
    sql = ("SELECT feed, delivery_id, source_object, cob_date, "
           "       received_at, md5, bytes "
           "FROM registry.delivery WHERE cob_date = %s")
    args: list[Any] = [cob_date]
    if feed:
        sql += " AND feed = %s"
        args.append(feed)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql + " ORDER BY feed, delivery_id", args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def delivered_dates(feed: str, start: date, end: date) -> set[date]:
    """COB dates `feed` has a registered delivery for, within [start, end].

    The `weekly` cadence's only use of this table: "has this ISO week already
    seen a delivery" (monitoring/feed_status.py), the same corroboration
    `completeness.find_gaps` already applies at Raw scale, done here at
    registry scale so a status page needs no Spark.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT cob_date FROM registry.delivery "
            "WHERE feed = %s AND cob_date BETWEEN %s AND %s", (feed, start, end))
        return {r[0] for r in cur.fetchall()}


def deliveries_by_id(pairs: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """The registered deliveries named by (feed, delivery_id). REQ-602/REQ-400.

    What `deliveries_on` is for a COB date, this is for a RUN's recorded
    input set -- the exact deliveries a published run read, rather than every
    delivery that happened to arrive for the date its tag names.

    A pair with no row is NOT dropped silently: it comes back with
    `registered: False`, because `run_input` carries no foreign key to
    `delivery` (see registry/db.py) and a run naming a delivery the registry
    cannot describe is exactly the finding this is asked for.
    """
    if not pairs:
        return []
    wanted = sorted(set(pairs))
    sql = ("SELECT feed, delivery_id, source_object, cob_date, "
           "       received_at, md5, bytes, manifest_key, origin, origin_uri, "
           "       source_filename, source_container, control_object "
           "FROM registry.delivery WHERE (feed, delivery_id) IN %s")
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (tuple(wanted),))
        cols = [d[0] for d in cur.description]
        found = {(r[0], r[1]): dict(zip(cols, r)) for r in cur.fetchall()}
    out = []
    for feed, delivery_id in wanted:
        row = found.get((feed, delivery_id))
        if row:
            out.append({**row, "registered": True})
        else:
            out.append({"feed": feed, "delivery_id": delivery_id,
                        "source_object": None, "cob_date": None,
                        "received_at": None, "md5": None, "bytes": None,
                        "manifest_key": None, "origin": None,
                        "origin_uri": None, "source_filename": None,
                        "source_container": None, "control_object": None,
                        "registered": False})
    return out
