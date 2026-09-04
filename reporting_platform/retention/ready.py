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
in landing. Nothing computes anything from `ready/`, so its only constraint is
that it comfortably exceeds `arrival_timeout_hours` (26h, deliberately longer
than a day) -- otherwise a delivery can be swept between being normalized and
being ingested by a late run.

ONE RULE, AND IT USES THE DERIVED LEDGER. A manifest is only swept once every
one of its parts appears in the raw table's `_source_file` values. Sweeping an
un-ingested delivery is not data loss -- landing still holds the object -- but
nothing would re-normalize it on its own, so it is a SILENT drop, and a silent
drop is worse than a loud one. Note this is a read of `already_ingested`, not
a status flag written into the manifest: the manifest never records derived
state, for the reason ingest/normalize.py's header gives.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from reporting_platform.common.context import Feed, feeds, retention_policy
from reporting_platform.ingest import normalize as norm
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
    if not entries:
        return {"feed": feed.name, "expired": 0, "retained": 0,
                "held_uningested": 0}

    done = already_ingested(feed)
    expired, retained, held = [], 0, 0
    for key, manifest in entries:
        if norm.business_date_of(manifest) >= cutoff:
            retained += 1
            continue
        if not all(p["object_key"] in done for p in manifest["parts"]):
            # Old enough, but never ingested. Leave it: see the header.
            held += 1
            continue
        expired.append((key, manifest))

    if not dry_run:
        s3 = _client()
        for key, manifest in expired:
            # Derived parts go with the manifest; a part that points back into
            # `landing/` is the evidence copy and is NOT this job's to delete.
            for part in manifest["parts"]:
                pk = part["object_key"]
                if pk.startswith(f"{feed.ready_prefix}/"):
                    s3.delete_object(Bucket=_bucket(), Key=pk)
            s3.delete_object(Bucket=_bucket(), Key=key)

    if held:
        log.info("ready %s: %d manifest(s) past the window but not yet "
                 "ingested, left alone", feed.name, held)
    return {"feed": feed.name, "expired": len(expired), "retained": retained,
            "held_uningested": held}


def sweep_ready(dry_run: bool = True) -> dict:
    days = keep_days()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date()
    result = {"keep_days": days, "cutoff": cutoff.isoformat(),
              "dry_run": dry_run, "feeds": [], "manifests_deleted": 0}
    for feed in feeds().values():
        entry = sweep_feed(feed, cutoff, dry_run)
        result["feeds"].append(entry)
        if not dry_run:
            result["manifests_deleted"] += entry["expired"]
        log.info("ready %s: %d expired (before %s), %d retained%s",
                 feed.name, entry["expired"], cutoff, entry["retained"],
                 " [dry run]" if dry_run else "")
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
