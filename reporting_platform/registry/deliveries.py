"""What arrived: the delivery half of the registry (REQ-100, REQ-101, REQ-104).

ONE PROJECTION, TWO CALLERS. `observations()` turns (feed, manifest, sidecar,
md5) into a row and touches nothing -- no S3, no database -- which is what
makes it testable without the stack and what keeps the two write paths from
drifting:

  * `normalize.normalize()` registers a delivery the moment it produces a
    manifest for it. Best-effort: a registry write that fails is logged and
    counted, never fatal. `landing/` is the evidence and the raw table is the
    ledger; this is an index over both, and taking ingestion down to protect
    an index would be the wrong way round.
  * `reconcile()` walks object storage and registers everything missing. This
    is the authority, and it is the same rule `arrival.py` already states for
    arrival detection: *events are an optimisation, the poll is the
    correctness guarantee.*

Because the second path exists and is the same code, "rebuildable from object
storage" is true by construction rather than by a second implementation that
would rot. `coverage()` is the check that says whether it is still true.

WHERE THE md5 COMES FROM, AND WHY IT IS NOT COMPUTED TWICE. A landing object
is immutable, so its hash is measured once and never again. Three sources, in
this order:

  1. The `.meta.json` sidecar, when the delivery came through the inbox gate.
     Measured at the door on the bytes the upstream sent.
  2. The object's ETag. MinIO and S3 both return the content md5 as the ETag
     for a single-part upload, so the ordinary case costs nothing beyond the
     listing already in hand. Verified against this stack's MinIO.
  3. Reading the object and hashing it -- only for a multipart upload, whose
     ETag ends in `-<partcount>` and is a hash of hashes, not of the content.

A row already registered is never re-hashed at all: `register()` leaves
`md5`, `bytes` and `sequence_no` alone on conflict.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from reporting_platform.common.context import Feed, feeds
from reporting_platform.registry import db

log = logging.getLogger("registry.deliveries")


# ------------------------------------------------------------- what arrived
def _sidecar(feed: Feed, delivery_id: str, present: set[str]) -> dict | None:
    """The `.meta.json` beside this delivery, if the gate wrote one.

    Absent for every feed with no `arrival:` block -- which is every feed
    shipped today -- so this is the uncommon case, not the default. Returns
    None rather than raising on unreadable metadata, for the reason
    `arrival._md5_of_landed` gives: bad provenance is not a reason to refuse
    to record that the delivery exists.
    """
    from reporting_platform.ingest import arrival, conform

    key = f"{feed.landing_prefix}/{feed.name}/{delivery_id}{conform.METADATA_SUFFIX}"
    if key not in present:
        return None
    try:
        body = arrival._client().get_object(
            Bucket=arrival._bucket(), Key=key)["Body"].read()
        return json.loads(body)
    except Exception as exc:                                    # noqa: BLE001
        log.warning("cannot read %s: %s", key, str(exc)[:200])
        return None


def _md5_of(object_key: str, etag: str | None) -> str:
    """The content md5 of a landing object. See the module header."""
    import hashlib

    from reporting_platform.ingest import arrival

    if etag:
        clean = etag.strip('"')
        # A multipart ETag is `<hex>-<parts>` and is a hash of part hashes.
        if "-" not in clean and len(clean) == 32:
            return clean
    body = arrival._client().get_object(
        Bucket=arrival._bucket(), Key=object_key)["Body"].read()
    return hashlib.md5(body).hexdigest()


def observations(feed: Feed, manifest: dict[str, Any],
                 sidecar: dict | None, md5: str, bucket: str) -> dict[str, Any]:
    """One delivery's row. Pure: computes from its arguments and nothing else.

    OBSERVATIONS ONLY. Everything here is a fact about an object that exists
    in `landing/` right now, or about the control file that arrived with it.
    Nothing here says whether the delivery was ingested, whether it superseded
    another, or whether anybody thinks it is good -- see `registry/db.py` on
    why that boundary is the whole difference between this and a load-control
    table.
    """
    parts = manifest["parts"]
    sidecar = sidecar or {}
    declared = sidecar.get("declared") or {}
    return {
        "feed": feed.name,
        "delivery_id": manifest["delivery_id"],
        "source_system": feed.source_system,
        "business_date": date.fromisoformat(manifest["business_date"]),
        "received_at": manifest["received_at"],
        "source_object": manifest["source_object"],
        "normalizer": manifest["normalizer"],
        # The delivery's size is the sum of the objects holding its rows, so
        # an archive's size is its members' and not the container's. The
        # container's own size is in the sidecar where it belongs.
        "bytes": sum(p.get("bytes") or 0 for p in parts),
        "md5": md5,
        "schema_version": feed.schema_version,
        # The gate stamps `promoted_by`; anything without a sidecar was
        # written into landing/ by an approved sender, which is the other of
        # the two ways in that `landing/`'s contract allows.
        "origin": "inbox" if sidecar.get("promoted_by") else "direct",
        # WHERE IT CAME FROM, as far back as anything recorded. For a gated
        # delivery that is the name in the inbox; for a direct one the landing
        # object is the earliest thing that exists, and saying so is more
        # honest than leaving the column null and implying it is unknown.
        # `bucket` is passed in rather than read here so this stays pure --
        # and it is not optional: `source_object` is a KEY, so the bucket has
        # to come from somewhere or the URI names a bucket called "landing".
        "origin_uri": (f"inbox:{sidecar['source_filename']}"
                       if sidecar.get("source_filename")
                       else f"s3://{bucket}/{manifest['source_object']}"),
        "source_filename": sidecar.get("source_filename"),
        "source_container": sidecar.get("source_container"),
        "control_object": manifest.get("control_object"),
        "declared_row_count": manifest.get("declared_row_count"),
        "declared_md5": manifest.get("declared_md5"),
        "producer_run_id": declared.get("producer_run_id"),
        "parts": [{"part_no": i, "object_key": p["object_key"],
                   "bytes": p.get("bytes")} for i, p in enumerate(parts)],
    }


# ------------------------------------------------------------------- writing
_COLUMNS = ("feed", "delivery_id", "source_system", "business_date",
            "received_at", "source_object", "manifest_key", "normalizer",
            "bytes", "md5", "schema_version", "origin", "origin_uri",
            "source_filename", "source_container", "control_object",
            "declared_row_count", "declared_md5", "producer_run_id")

# On conflict, update only what is a pure function of the delivery's own
# objects. NOT `sequence_no` (the arrival order does not change because the
# row was seen again), NOT `first_seen_at`, NOT `md5` or `bytes` (the object
# is immutable, so re-measuring it can only introduce disagreement), and NOT
# `received_at` (the landing object's LastModified, which likewise does not
# move). What CAN legitimately change is what config says about the delivery
# -- a corrected `source_columns` map changes `schema_version`, a control file
# arriving late gives it a `control_object` -- and a re-registration is how
# that is picked up.
_UPDATABLE = ("manifest_key", "normalizer", "schema_version", "control_object",
              "declared_row_count", "declared_md5", "producer_run_id",
              "origin", "origin_uri", "source_filename", "source_container")


def write(conn, row: dict[str, Any], manifest_key: str | None) -> None:
    """Insert or refresh one delivery and its parts."""
    values = {**{c: row.get(c) for c in _COLUMNS}, "manifest_key": manifest_key}
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATABLE)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO registry.delivery ({', '.join(_COLUMNS)}) "
            f"VALUES ({', '.join('%(' + c + ')s' for c in _COLUMNS)}) "
            f"ON CONFLICT (feed, delivery_id) DO UPDATE SET {sets}",
            values)
        # Replaced wholesale rather than upserted part by part: an archive
        # re-normalized against a changed `member_pattern` legitimately has a
        # different set of parts, and a merge would leave the members it no
        # longer extracts behind as rows pointing at objects that are gone.
        cur.execute("DELETE FROM registry.delivery_part "
                    "WHERE feed = %s AND delivery_id = %s",
                    (row["feed"], row["delivery_id"]))
        for part in row["parts"]:
            cur.execute(
                "INSERT INTO registry.delivery_part "
                "(feed, delivery_id, part_no, object_key, bytes) "
                "VALUES (%s, %s, %s, %s, %s)",
                (row["feed"], row["delivery_id"], part["part_no"],
                 part["object_key"], part["bytes"]))


def register(feed: Feed, manifest: dict[str, Any],
             manifest_key: str | None = None, *, conn=None,
             present: set[str] | None = None,
             etags: dict[str, str] | None = None) -> dict[str, Any]:
    """Record one delivery. Returns the row that was written.

    `present` and `etags` are the caller's existing listing of the feed's
    landing prefix, so a reconcile over two hundred deliveries costs the one
    LIST it was already doing rather than a HEAD each.
    """
    from reporting_platform.ingest import arrival

    if present is None:
        present = set(arrival.list_landing(feed))
    sidecar = _sidecar(feed, manifest["delivery_id"], present)
    md5 = sidecar.get("md5") if sidecar else None
    if not md5:
        source = manifest["source_object"]
        md5 = _md5_of(source, (etags or {}).get(source))
    row = observations(feed, manifest, sidecar, md5, arrival._bucket())

    if conn is not None:
        write(conn, row, manifest_key)
    else:
        with db.connect() as own:
            write(own, row, manifest_key)
    return row


def register_quietly(feed: Feed, manifest: dict[str, Any],
                     manifest_key: str | None = None) -> None:
    """`register`, but a failure is logged rather than raised.

    The inline path, called from `normalize`. See the module header: the
    registry is an index over evidence that exists whether or not this row
    does, and `reconcile()` is what makes the omission temporary. A registry
    write must not be able to stop a delivery being normalized and ingested.

    The log line is deliberately at WARNING and names the delivery, because
    the failure it most wants to catch is a slow drift -- Postgres unreachable
    for a week, nobody noticing, the registry quietly a week behind. That is
    also what `coverage()` measures, so the condition is both logged and
    checkable.
    """
    try:
        register(feed, manifest, manifest_key)
    except Exception as exc:                                    # noqa: BLE001
        log.warning("registry: could not record %s/%s: %s -- reconcile will "
                    "pick it up", feed.name, manifest.get("delivery_id"),
                    f"{type(exc).__name__}: {exc}")


# --------------------------------------------------------------- reconciling
def reconcile(feed: Feed, *, normalize_first: bool = True) -> dict[str, Any]:
    """Register every delivery in object storage that has no row yet.

    THE REBUILD PATH, and the ordinary one. Registering from here rather than
    only inline is what lets the whole registry be dropped and reconstructed
    from `landing/` and `ready/`, which is the property that makes it an index
    rather than a second source of truth.

    `normalize_first` runs `normalize.reconcile` so a delivery pushed straight
    into the bucket has a manifest before this looks for one. The registry
    follows manifests, not landing objects, because a manifest is the platform
    having ACCEPTED the delivery as readable -- a file still waiting on its
    control file has landed but is not yet a delivery anything can describe,
    and giving it a row would mean inventing a business date the platform has
    not yet been told.
    """
    from reporting_platform.ingest import arrival
    from reporting_platform.ingest import normalize as norm

    out: dict[str, Any] = {"feed": feed.name, "registered": [], "failed": []}
    if normalize_first:
        out["normalized"] = norm.reconcile(feed)["created"]

    listing = _landing_listing(feed)
    present, etags = set(listing), listing
    entries = norm.manifests_for(feed)

    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT delivery_id FROM registry.delivery "
                        "WHERE feed = %s", (feed.name,))
            known = {r[0] for r in cur.fetchall()}
        # Oldest first, so `sequence_no` reproduces arrival order on a rebuild.
        # See registry/db.py: the order is what a rebuild preserves, not the
        # integers, and it only comes out right if they are inserted in it.
        entries.sort(key=lambda kv: (kv[1].get("received_at") or "",
                                     kv[1]["delivery_id"]))
        for key, manifest in entries:
            if manifest["delivery_id"] in known:
                continue
            try:
                register(feed, manifest, key, conn=conn,
                         present=present, etags=etags)
                out["registered"].append(manifest["delivery_id"])
            except Exception as exc:                            # noqa: BLE001
                # One delivery that cannot be described must not stop the
                # rest, exactly as in `normalize.reconcile`.
                conn.rollback()
                out["failed"].append({"delivery": manifest["delivery_id"],
                                      "error": f"{type(exc).__name__}: {exc}"})
    out["known"] = len(known)
    if out["failed"]:
        log.warning("%s: %d delivery(ies) could not be registered: %s",
                    feed.name, len(out["failed"]), out["failed"][:3])
    log.info("registry %s: %d new, %d already known", feed.name,
             len(out["registered"]), out["known"])
    return out


def _landing_listing(feed: Feed) -> dict[str, str]:
    """`object key -> ETag` for the feed's landing prefix, in one LIST."""
    from reporting_platform.ingest import arrival

    prefix = f"{feed.landing_prefix}/{feed.name}/"
    paginator = arrival._client().get_paginator("list_objects_v2")
    out: dict[str, str] = {}
    for page in paginator.paginate(Bucket=arrival._bucket(), Prefix=prefix):
        for obj in page.get("Contents", []):
            out[obj["Key"]] = obj.get("ETag", "")
    return out


def coverage(feed: Feed) -> dict[str, Any]:
    """Manifests in `ready/` versus rows in the registry, for one feed.

    THE CHECK THAT CAN ACTUALLY FIRE. `register_quietly` swallows a failed
    write on purpose, so the registry can fall behind silently; this is what
    makes that condition visible, and it is written against the mechanism that
    exists rather than against a status column that does not. A non-empty
    `missing` means reconcile has not run since something failed, not that
    anything is wrong with the deliveries themselves.
    """
    from reporting_platform.ingest import normalize as norm

    manifests = {m["delivery_id"] for _, m in norm.manifests_for(feed)}
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT delivery_id FROM registry.delivery WHERE feed = %s",
                    (feed.name,))
        rows = {r[0] for r in cur.fetchall()}
    return {"feed": feed.name, "manifests": len(manifests),
            "registered": len(rows),
            "missing": sorted(manifests - rows),
            # A row whose manifest is gone is NOT an error: `ready/` is a
            # days-long cache and `landing/` keeps deliveries for years, so
            # the registry outliving the manifest is the designed steady
            # state. Reported for completeness, never warned on.
            "manifest_expired": len(rows - manifests)}


def deliveries_on(business_date: date, feed: str | None = None
                  ) -> list[dict[str, Any]]:
    """Every registered delivery for one business date. Used by REQ-602."""
    sql = ("SELECT feed, delivery_id, source_object, business_date, "
           "       received_at, md5, bytes "
           "FROM registry.delivery WHERE business_date = %s")
    args: list[Any] = [business_date]
    if feed:
        sql += " AND feed = %s"
        args.append(feed)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql + " ORDER BY feed, delivery_id", args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def reconcile_all() -> dict[str, Any]:
    out = {"feeds": [], "registered": 0, "failed": 0}
    for fd in feeds().values():
        one = reconcile(fd)
        out["feeds"].append(one)
        out["registered"] += len(one["registered"])
        out["failed"] += len(one["failed"])
    return out
