"""Reclaim table directories that no Nessie reference points at.

THE GAP THIS FILLS. Two mechanisms are supposed to reclaim
storage, and neither can touch this case:

  * **Nessie GC** identifies live content by walking live references, then for
    each content in that live set deletes files which are not live. A table
    whose content entry existed ONLY on a branch that has since been deleted is
    in no live reference, so it never enters the live set and its files are
    never visited. GC cannot collect what it never enumerates.
  * **`remove_orphan_files`** would catch exactly this by diffing the object
    store against table metadata -- but it is disabled under Nessie by
    `gc.enabled=false`, and rightly so: files are shared across
    references and no single table pointer knows what another branch needs.

So a failed build whose branch is later swept by `clean_working_branches`
leaves its entire table output in object storage permanently. That is
unbounded, and invisible to `storage_report`, which only checks that expiry
produced deletions -- not that the warehouse holds prefixes nothing points at.

WHAT THIS DOES. Resolve the set of table locations live across *every*
reference (branches and tags), list the table-level prefixes actually present
in the warehouse, and delete those in neither -- subject to an age floor.

TWO SAFETY PROPERTIES, both deliberate:

  * **Age floor.** A prefix whose newest object is younger than
    `min_age_days` is never touched, because it may belong to a write that is
    still in flight. Same reasoning and the same floor as
    `remove_orphan_files`; it also absorbs clock skew between the writer and
    the object store.
  * **Every reference, not just main.** Reading only `main` would delete the
    working output of every open build branch. The set must be the union
    across all references, which is precisely the reasoning that makes Nessie
    GC the right owner of the file lifecycle in the first place.

ORDERING TRAP. Deleting a branch *before* GC runs guarantees its files can
never be collected by GC -- which is how the stranded directories appeared.
This sweep is the backstop for that, but any automated branch cleanup should
still run GC first.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone

from reporting_platform.common.context import Nessie, maintenance_config

log = logging.getLogger("orphan-storage")

# s3a://bucket/warehouse/<namespace>/<table>_<uuid>/metadata/xxxx.metadata.json
#                        ^--------- the table prefix we care about ---------^
_METADATA_RE = re.compile(r"^[a-z0-9]+://(?P<bucket>[^/]+)/(?P<prefix>.+?)/metadata/[^/]+$")


def _client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _warehouse() -> tuple[str, str]:
    """(bucket, root prefix) of the Iceberg warehouse."""
    wh = os.environ.get("REPORTING_WAREHOUSE", "s3a://lakehouse/warehouse")
    body = wh.split("://", 1)[-1]
    bucket, _, prefix = body.partition("/")
    return bucket, prefix.strip("/")


def live_table_prefixes(nessie: Nessie | None = None) -> set[str]:
    """Table prefixes referenced by ANY branch or tag."""
    nessie = nessie or Nessie()
    live: set[str] = set()
    for ref in nessie.list_references():
        name = ref.get("name")
        if not name:
            continue
        try:
            entries = nessie.list_entries(name)
        except Exception as e:                      # a ref can vanish mid-sweep
            log.warning("could not read entries for %s: %s", name, str(e)[:120])
            continue
        for e in entries:
            content = e.get("content") or {}
            loc = content.get("metadataLocation")
            if not loc:
                continue
            m = _METADATA_RE.match(loc)
            if m:
                live.add(m.group("prefix").strip("/"))
    return live


def warehouse_table_prefixes() -> dict[str, dict]:
    """Table-level prefixes present in object storage, with size and newest mtime."""
    bucket, root = _warehouse()
    s3 = _client()
    found: dict[str, dict] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{root}/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            # root / namespace / table_uuid / ...
            if len(parts) < 4:
                continue
            prefix = "/".join(parts[:3])
            rec = found.setdefault(prefix, {"objects": 0, "bytes": 0, "newest": None})
            rec["objects"] += 1
            rec["bytes"] += obj["Size"]
            ts = obj["LastModified"]
            if rec["newest"] is None or ts > rec["newest"]:
                rec["newest"] = ts
    return found


def sweep_orphan_prefixes(dry_run: bool = True) -> dict:
    """Delete warehouse table prefixes that no reference points at."""
    cfg = maintenance_config().get("orphan_files", {})
    min_age = max(int(cfg.get("min_age_days", 3)), 3)   # hard floor, as elsewhere
    cutoff = datetime.now(timezone.utc) - timedelta(days=min_age)

    nessie = Nessie()
    live = live_table_prefixes(nessie)
    present = warehouse_table_prefixes()

    orphans, too_new = [], []
    for prefix, rec in sorted(present.items()):
        if prefix in live:
            continue
        if rec["newest"] is not None and rec["newest"] > cutoff:
            too_new.append(prefix)
            continue
        orphans.append((prefix, rec))

    report = {
        "live_prefixes": len(live),
        "warehouse_prefixes": len(present),
        "orphans": [p for p, _ in orphans],
        "orphan_bytes": sum(r["bytes"] for _, r in orphans),
        "orphan_objects": sum(r["objects"] for _, r in orphans),
        "skipped_too_new": too_new,
        "min_age_days": min_age,
        "dry_run": dry_run,
    }

    if dry_run or not orphans:
        if orphans:
            log.info("would delete %d orphan prefixes (%.2f MB)",
                     len(orphans), report["orphan_bytes"] / 1048576)
        return report

    bucket, _ = _warehouse()
    s3 = _client()
    deleted = 0
    for prefix, _rec in orphans:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                # One object at a time, deliberately. Batched delete_objects is
                # faster but MinIO rejects it without a Content-MD5 header,
                # which current botocore does not send:
                #   MissingContentMD5: Missing required header for this
                #   request: Content-Md5
                # Per-object DELETE has no such requirement and behaves the
                # same on MinIO and real S3. For table-sized prefixes the
                # difference does not matter, and being portable matters more
                # than being quick in a destructive path.
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
                deleted += 1
        log.info("deleted orphan prefix %s", prefix)
    report["objects_deleted"] = deleted
    return report


def main(argv=None) -> int:
    import argparse
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--dry-run", action="store_true", default=False,
                   help="report what would be deleted, change nothing")
    a = p.parse_args(argv)
    print(json.dumps(sweep_orphan_prefixes(dry_run=a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
