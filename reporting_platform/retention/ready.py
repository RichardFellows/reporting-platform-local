"""`ready/` retention: the work queue is a cache, and expires in days.

NOT THE LANDING RULE, and the difference is the whole reason the two prefixes
were split. `landing/` is the evidence copy -- what the upstream actually
sent, kept for `keep_years` and never deleted on a guess. `ready/` holds
manifests and any parts a normalizer derived from them, all of which can be
reconstructed by re-normalizing. It is deletable precisely because it is
derived, and that property is worth protecting: the moment something in
`ready/` cannot be rebuilt from `landing/`, it has quietly become a third copy
of the data.

THE FLOOR IS OPERATIONAL, NOT POLICY. `landing:` must be >= the raw window
because `find_pending` computes its retention keep-set from the dates present
in landing. Nothing computes anything from `ready/`, so `keep_days` has no
correctness floor at all -- the rule below is what stops a delivery being
swept between being normalized and being ingested, and it holds at any age.
Seven days is a convenience: it keeps a recently extracted archive member
around while something is being diagnosed, and re-normalizing with `--force`
is what rebuilds an older one.

WHAT `keep_days` GOVERNS IS NARROWER THAN IT LOOKS, and the next section is
the correction that made it so. It bounds the DERIVED PARTS. It does not bound
the manifests, which live as long as the landing objects they describe.

WHAT IS ACTUALLY RECLAIMABLE, AND WHAT ONLY LOOKED IT. This swept manifests
past `keep_days` whose parts were all ingested, and reported them deleted. It
was deleting nothing: `normalize.reconcile` creates a manifest for every
landing object that lacks one, which after the sweep is all of them, so the
two subsystems undid each other every night. Measured inside one housekeeping
run, three tasks apart -- `enforce_retention` deleted 157 manifests and
`registry_reconcile` re-created all 157, newly registering 0. Nothing was
corrupted, and both logged success.

THE SWEEP WAS THE ONE IN THE WRONG. Making `normalize.reconcile` skip landing
objects that are already ingested would stop the churn and break something
load-bearing: the registry follows MANIFESTS, so `deliveries.reconcile` --
the path that makes the registry rebuildable from object storage rather than a
second source of truth -- can only see a delivery that has one. Sweep the
manifests and teach reconcile not to rebuild them, and dropping the registry
would re-register only the deliveries of the last `keep_days`. The manifest is
not merely a queue entry; it is the delivery's description of record in object
storage, and it is 1KB against a landing object it outlives nothing of.

So two things go, and neither of them comes back:

  * DERIVED PARTS -- a zip member `_normalize_archive` extracted -- of a
    manifest past `keep_days` whose parts are all ingested. This is the real
    duplication: a second copy of data landing already holds. The manifest
    stays, so `normalize.reconcile` skips the landing object and nothing
    re-extracts them; `normalize --force` is what rebuilds them if anything
    ever needs to.
  * ORPHANED MANIFESTS -- whose `source_object` is no longer in `landing/`,
    at any age. Nothing recreates one, because `reconcile` walks landing.
    These are what the landing sweep leaves behind, which is why it runs
    immediately before this one in `retention.run()`.

A manifest whose landing object is still there is KEPT, at any age. That is
the honest answer to "what does the `ready:` window reclaim": the parts, and
the manifests landing has already let go of.

THE INGEST RULE STILL HOLDS, and it is still the negative one that matters. A
manifest whose parts are not yet in the raw table's `_source_file` values has
its parts left alone at any age. Deleting them is not data loss -- landing
still holds the object -- but nothing would re-normalize it on its own, so it
is a SILENT drop, and a silent drop is worse than a loud one. Note this is a
read of `already_ingested`, not a status flag in the manifest: the manifest
never records derived state, for the reason ingest/normalize.py's header
gives. It costs a Spark session, so it is taken LAZILY -- on a queue of plain
CSV manifests, where every part points back into `landing/` and there is
nothing derived to sweep, it is never taken at all.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from reporting_platform.common.context import Feed, feeds, retention_policy
from reporting_platform.ingest import normalize as norm
from reporting_platform.ingest import arrival
from reporting_platform.ingest.arrival import _bucket, _client, already_ingested

log = logging.getLogger("retention.ready")

DEFAULT_KEEP_DAYS = 7


def keep_days() -> int:
    """`ready:` from retention.yml, or the default if the block is absent.

    Tolerant on purpose, and only here. A missing `ready:` block means the
    queue is kept a week, which is safe; making it fatal would take the whole
    retention chain down over a cache policy. The table layers stay strict --
    a missing `raw:` block must not silently mean "keep nothing".
    """
    try:
        policy = retention_policy("ready") or {}
    except KeyError:
        log.warning("no `ready:` retention block for this environment; "
                    "defaulting to %d days", DEFAULT_KEEP_DAYS)
        return DEFAULT_KEEP_DAYS
    return int(policy.get("keep_days", DEFAULT_KEEP_DAYS))


def sweep_feed(feed: Feed, cutoff, dry_run: bool = True) -> dict:
    entries = norm.manifests_for(feed)
    empty = {"feed": feed.name, "parts_deleted": 0, "manifests_deleted": 0,
             "retained": 0, "held_uningested": 0, "orphans": 0}
    if not entries:
        return empty

    landed = set(arrival.list_landing(feed))
    orphans: list[tuple[str, dict]] = []
    candidates: list[tuple[str, dict, list[str]]] = []
    retained = 0
    for key, manifest in entries:
        source = manifest.get("source_object")
        if source is not None and source not in landed:
            # Landing has let this delivery go. Nothing will recreate the
            # manifest, so it is the one thing here that reclaims for good.
            orphans.append((key, manifest))
            continue
        derived = [p["object_key"] for p in manifest["parts"]
                   if p["object_key"].startswith(f"{feed.ready_prefix}/")]
        if not derived or norm.business_date_of(manifest) >= cutoff:
            retained += 1
            continue
        candidates.append((key, manifest, derived))

    # LAZY, and that is the point of computing it here rather than at the top:
    # `already_ingested` opens a Spark session per feed, and on a queue with
    # no derived parts and no orphans there is nothing to decide.
    done: set[str] | None = None
    if candidates or orphans:
        done = already_ingested(feed)

    expired, held = [], 0
    for key, manifest, derived in candidates:
        if not all(p["object_key"] in done for p in manifest["parts"]):
            # Old enough, but never ingested. Leave it: see the header.
            held += 1
            continue
        expired.append(derived)

    stranded = [(k, m) for k, m in orphans
                if not all(p["object_key"] in done for p in m["parts"])]

    if not dry_run:
        s3 = _client()
        for derived in expired:
            for pk in derived:
                s3.delete_object(Bucket=_bucket(), Key=pk)
        for key, manifest in orphans:
            for part in manifest["parts"]:
                pk = part["object_key"]
                if pk.startswith(f"{feed.ready_prefix}/"):
                    s3.delete_object(Bucket=_bucket(), Key=pk)
            s3.delete_object(Bucket=_bucket(), Key=key)

    if held:
        log.info("ready %s: %d manifest(s) past the window but not yet "
                 "ingested, derived parts left alone", feed.name, held)
    if stranded:
        # Loud, because it is the one case where something is genuinely lost:
        # a delivery that landed, was never ingested, and has now aged out of
        # `landing/` entirely. The manifest goes because nothing can act on it.
        log.warning("ready %s: %d manifest(s) whose landing object is gone "
                    "were never ingested: %s", feed.name, len(stranded),
                    [k for k, _ in stranded][:3])
    return {"feed": feed.name,
            "parts_deleted": sum(len(d) for d in expired),
            "manifests_deleted": len(orphans),
            "retained": retained, "held_uningested": held,
            "orphans": len(orphans), "orphans_uningested": len(stranded)}


def sweep_ready(dry_run: bool = True) -> dict:
    """Every feed. The counters are what was REMOVED, not what was considered.

    `manifests_deleted` counts orphans only. A manifest whose landing object
    is still present is never swept -- see the header -- so a number here that
    tracked "past the window" would be describing work the next
    `normalize.reconcile` undoes, which is what it used to do.
    """
    days = keep_days()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date()
    result = {"keep_days": days, "cutoff": cutoff.isoformat(),
              "dry_run": dry_run, "feeds": [], "manifests_deleted": 0,
              "parts_deleted": 0}
    for feed in feeds().values():
        entry = sweep_feed(feed, cutoff, dry_run)
        result["feeds"].append(entry)
        if not dry_run:
            result["manifests_deleted"] += entry["manifests_deleted"]
            result["parts_deleted"] += entry["parts_deleted"]
        log.info("ready %s: %d derived part(s) and %d orphaned manifest(s) "
                 "removed (parts before %s), %d manifest(s) retained%s",
                 feed.name, entry["parts_deleted"], entry["manifests_deleted"],
                 cutoff, entry["retained"], " [dry run]" if dry_run else "")
    return result


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--apply", dest="dry_run", action="store_false")
    a = p.parse_args(argv)
    print(json.dumps(sweep_ready(a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
