"""What arrived and was refused: quarantine and the rejection record (REQ-106).

WHAT WAS HERE BEFORE. A delivery the conformance gate could not accept was
moved to `.rejected/` -- a directory on the inbox container's bind mount --
and a line was written to that container's log. Nothing recorded what the file
was, what was wrong with it, or that it had ever arrived; the evidence lived on
one host, in a folder with no retention policy, and `docker compose down -v`
took it with it. A rejected delivery is evidence exactly as much as an accepted
one is: it is the proof that the upstream sent something wrong, which is the
first thing anyone asks about when the numbers are short.

So the bytes now go to `quarantine/` in object storage, beside `landing/` and
under a retention policy of its own, and this module writes the row that says
what they were.

`.rejected/` STAYS, and is written after the quarantine copy. It is what the
feed console's unclaimed-deliveries queue reads and what the sniffer offers to
onboard, and both of those want a local file to open. It is now a working
copy of something durable rather than the only copy.

THE KEY CARRIES THE DATE, and that is deliberate. A quarantined file has no
COB date -- not being nameable is frequently why it was rejected -- so
there is nothing in it for a retention sweep to date it by. Putting the
rejection date in the key means `retention/quarantine.py` dates every object
from its own name with no lookup and no guess, which is the same property
`<delivery>.meta.json` was given for the same reason.
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone

from reporting_platform.common.context import Feed
from reporting_platform.registry import db

log = logging.getLogger("registry.rejections")

QUARANTINE_PREFIX = "quarantine"

# What the gate can refuse a file for, as a CLASS rather than as prose, so the
# common ones can be counted without parsing the message beside them:
#
#   unroutable  no feed claims this filename
#   ambiguous   more than one feed claims it -- rejected, never guessed
#   identity    a feed claims it and it cannot be NAMED for landing: no COB
#               date, an unparsable control file, a version that will not
#               render
#   member      an archive member that could not be conformed, where the
#               container itself was fine
#
# An INTEGRITY failure is deliberately not in this list. A delivery whose row
# count or checksum does not match LANDS and fails at ingest -- landing is the
# evidence copy -- so it is never quarantined.
# See docs/DECISIONS.md#the-inbox-is-the-conformance-gate.
REASON_CLASSES = ("unroutable", "ambiguous", "identity", "member")

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def quarantine_key(feed: Feed | None, filename: str,
                   when: datetime | None = None) -> str:
    """Where a rejected file's bytes go.

    `quarantine/<feed>/<yyyy>/<mm>/<timestamp>_<filename>`, with `_unclaimed`
    standing in for a file no feed claimed. The timestamp makes each ATTEMPT
    its own object: an upstream that resends the same broken file every
    morning is producing a new event each time, and collapsing those onto one
    key would leave only the last one and hide the pattern.

    The filename is sanitised, not trusted. It reaches here straight from a
    directory anyone can write to, and a name containing `/` or `..` would
    place the object outside this prefix -- the same traversal
    `normalize._safe_member_name` refuses for archive members, in the one
    other place a caller-supplied name becomes part of a key.
    """
    when = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
    safe = _UNSAFE.sub("_", filename) or "unnamed"
    return (f"{QUARANTINE_PREFIX}/{feed.name if feed else '_unclaimed'}/"
            f"{when:%Y}/{when:%m}/{when:%Y%m%dT%H%M%SZ}_{safe}")


def quarantine(feed: Feed | None, filename: str, content: bytes, *,
               reason_class: str, reason: str,
               received_at: datetime | None = None,
               when: datetime | None = None) -> dict:
    """Copy the refused bytes to object storage and record the rejection.

    Returns the row. Raises if either half fails -- unlike a delivery
    registration, which is best-effort because the delivery itself is already
    safe in `landing/`. Here the object storage copy IS the evidence, and
    quietly failing to make it would leave a file in `.rejected/` on one
    container and no record anywhere that it had ever arrived.
    """
    if reason_class not in REASON_CLASSES:
        raise ValueError(
            f"unknown rejection class {reason_class!r}; expected one of "
            f"{', '.join(REASON_CLASSES)}")

    from reporting_platform.ingest import arrival

    when = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
    key = quarantine_key(feed, filename, when)
    arrival._client().put_object(Bucket=arrival._bucket(), Key=key,
                                 Body=content)

    row = {
        "quarantine_key": key,
        "feed": feed.name if feed else None,
        "source_filename": filename,
        "received_at": (received_at or when).astimezone(timezone.utc),
        "rejected_at": when,
        "reason_class": reason_class,
        "reason": reason[:2000],
        "bytes": len(content),
        "md5": hashlib.md5(content).hexdigest(),
    }
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO registry.rejection (quarantine_key, feed, "
            "  source_filename, received_at, rejected_at, reason_class, "
            "  reason, bytes, md5) "
            "VALUES (%(quarantine_key)s, %(feed)s, %(source_filename)s, "
            "  %(received_at)s, %(rejected_at)s, %(reason_class)s, "
            "  %(reason)s, %(bytes)s, %(md5)s) "
            "ON CONFLICT (quarantine_key) DO NOTHING", row)
    log.info("quarantined %s as %s (%s)", filename, key, reason_class)
    return row


def quarantine_quietly(feed: Feed | None, filename: str, content: bytes, *,
                       reason_class: str, reason: str,
                       received_at: datetime | None = None) -> dict | None:
    """`quarantine`, but a failure does not change what the gate does next.

    The gate's job is to keep a bad file out of `landing/` and move it aside;
    that must still happen if object storage or Postgres is unreachable. The
    failure is logged loudly because, unlike a missed delivery registration,
    nothing reconciles this one later -- the bytes exist only in `.rejected/`
    until somebody re-drops them.
    """
    try:
        return quarantine(feed, filename, content, reason_class=reason_class,
                          reason=reason, received_at=received_at)
    except Exception as exc:                                    # noqa: BLE001
        log.warning("could not quarantine %s: %s -- the file is in .rejected/ "
                    "and nowhere else", filename, f"{type(exc).__name__}: {exc}")
        return None


def recent(limit: int = 50, feed: str | None = None) -> list[dict]:
    """The most recent rejections, newest ARRIVAL first.

    ORDERED BY `received_at`, LIKE `deliveries.recent`, because the one caller
    that reads both (`ui/arrivals.recent`) merges them and re-sorts on that
    field. Ordered on `rejected_at` instead, the two halves disagree about
    what "recent" means: a file re-examined long after it arrived takes a slot
    in this top-N and then sorts to the bottom of the merged page, displacing
    an arrival that genuinely belonged on it. `rejected_at` breaks the tie,
    which for a gate that refuses on sight is registration order.
    """
    sql = ("SELECT quarantine_key, feed, source_filename, received_at, "
           "       rejected_at, reason_class, reason, bytes, md5 "
           "FROM registry.rejection")
    args: list = []
    if feed:
        sql += " WHERE feed = %s"
        args.append(feed)
    sql += " ORDER BY received_at DESC, rejected_at DESC LIMIT %s"
    args.append(limit)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
