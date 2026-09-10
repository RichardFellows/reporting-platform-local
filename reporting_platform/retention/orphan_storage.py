"""Reclaim table directories that no Nessie reference points at.

THE GAP THIS FILLS. Two mechanisms are supposed to reclaim storage, and
neither can touch this case:

  * **Nessie GC** identifies live content by walking live references, then
    deletes files not live for each content in that set. A table whose content
    entry existed ONLY on a branch since deleted is in no live reference, so it
    never enters the live set. GC cannot collect what it never enumerates.
  * **`remove_orphan_files`** would catch exactly this by diffing the object
    store against table metadata -- but it is disabled under Nessie by
    `gc.enabled=false`, and rightly so: files are shared across references.

So a failed build whose branch is later swept by `clean_working_branches`
leaves its entire table output in object storage permanently. That is
unbounded, and invisible to `storage_report`, which only checks that expiry
produced deletions.

WHAT THIS DOES. Resolve the set of table locations live across *every*
reference (branches and tags), list the table-level prefixes present in the
warehouse, and delete those in neither -- subject to an age floor.

THREE SAFETY PROPERTIES, all deliberate:

  * **Age floor.** A prefix whose newest object is younger than `min_age_days`
    is never touched, because it may belong to a write still in flight. Same
    reasoning and floor as `remove_orphan_files`; it also absorbs clock skew.
  * **Every reference, not just main.** Reading only `main` would delete the
    working output of every open build branch.
  * **A COMPLETE LIVE SET, OR NOTHING.** This sweep's input is a set of things
    NOT to delete, so every gap in it is a deletion. A Nessie that is down,
    slow or answering 401 therefore reads as "nothing is live" and the sweep
    would delete the entire warehouse -- unattended, from the nightly chain.
    So an unreadable reference REFUSES the sweep rather than shrinking the
    keep-set, and an empty live set refuses too: a warehouse holding objects
    while the catalog claims no tables is not a state to act destructively on.
    Only a 404 is tolerated, because a ref that vanished mid-sweep really has
    taken its tables with it. See docs/DECISIONS.md#an-incomplete-keep-set-refuses

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


class IncompleteLiveSet(RuntimeError):
    """The set of live prefixes could not be established in full.

    Raised rather than returned, because the caller of `live_table_prefixes`
    is about to delete everything NOT in the answer: a short set and a
    complete one are the same type and read the same way, and the difference
    between them is the whole warehouse.
    """


def _ref_is_gone(exc: Exception) -> bool:
    """Did this reference 404, as opposed to being unreadable?

    A branch deleted between `list_references` and `list_entries` is benign --
    its tables genuinely have no live reference now, which is exactly what this
    sweep exists to reclaim. Every other failure (connection refused, 401, 500,
    a read timeout) means the ref may hold live tables we simply did not see.
    """
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None) == 404


def live_table_prefixes(nessie: Nessie | None = None) -> set[str]:
    """Table prefixes referenced by ANY branch or tag.

    Raises `IncompleteLiveSet` if any reference could not be read. See this
    module's third safety property: a partial keep-set is a deletion order.
    """
    nessie = nessie or Nessie()
    live: set[str] = set()
    unreadable: list[str] = []
    try:
        references = nessie.list_references()
    except Exception as e:
        # The same refusal as an unreadable ref, and the commoner one: a
        # Nessie that is down fails HERE, and an empty reference list would
        # otherwise mean "nothing is live" -- i.e. delete the warehouse.
        raise IncompleteLiveSet(
            f"could not list the catalog's references, so nothing is known "
            f"to be live: {str(e)[:200]}") from e
    for ref in references:
        name = ref.get("name")
        if not name:
            continue
        try:
            entries = nessie.list_entries(name)
        except Exception as e:
            if _ref_is_gone(e):                     # a ref can vanish mid-sweep
                log.warning("reference %s vanished mid-sweep", name)
                continue
            log.error("could not read entries for %s: %s", name, str(e)[:120])
            unreadable.append(f"{name}: {str(e)[:120]}")
            continue
        for e in entries:
            content = e.get("content") or {}
            loc = content.get("metadataLocation")
            if not loc:
                continue
            m = _METADATA_RE.match(loc)
            if m:
                live.add(m.group("prefix").strip("/"))
    if unreadable:
        raise IncompleteLiveSet(
            "could not read %d of the catalog's references, so the live set is "
            "incomplete and anything missing from it would be deleted: %s"
            % (len(unreadable), "; ".join(unreadable[:5])))
    return live


def warehouse_table_prefixes() -> dict[str, dict]:
    """Table-level prefixes present in object storage, with size and newest mtime."""
    bucket, root = _warehouse()
    # root / namespace / table_uuid / ... -- and the ROOT IS CONFIGURABLE, so
    # its depth is derived, never assumed. `_METADATA_RE` captures the whole
    # root into a live prefix; a hardcoded depth of 3 against a nested root
    # (`s3a://bucket/a/warehouse`) yields `a/warehouse/<namespace>`, which can
    # never equal a live prefix -- so every namespace reads as an orphan.
    root_parts = [p for p in root.split("/") if p]
    depth = len(root_parts) + 2
    s3 = _client()
    found: dict[str, dict] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{root}/" if root else ""):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) <= depth:
                continue
            prefix = "/".join(parts[:depth])
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
    try:
        live = live_table_prefixes(nessie)
    except IncompleteLiveSet as e:
        log.error("REFUSING the orphan sweep: %s", e)
        return {"refused": str(e), "orphans": [], "dry_run": dry_run}
    present = warehouse_table_prefixes()

    if present and not live:
        # Not a crash and not an empty warehouse: the catalog answered, and
        # answered that it holds no tables at all, while object storage holds
        # some. Every prefix would qualify. Refuse and say so.
        msg = ("no live table prefixes across any reference, but %d prefixes "
               "are present in the warehouse -- every one of them would be "
               "deleted. Refusing: check the catalog before sweeping."
               % len(present))
        log.error("REFUSING the orphan sweep: %s", msg)
        return {"refused": msg, "orphans": [], "warehouse_prefixes": len(present),
                "dry_run": dry_run}

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
                # One object at a time, deliberately: batched delete_objects
                # needs a Content-MD5 header botocore does not send, and MinIO
                # rejects it. See docs/DECISIONS.md#minio-per-object-delete
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
                deleted += 1
        log.info("deleted orphan prefix %s", prefix)
    report["objects_deleted"] = deleted
    return report


def main(argv=None) -> int:
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--dry-run", action="store_true", default=False,
                   help="report what would be deleted, change nothing")
    a = p.parse_args(argv)
    report = sweep_orphan_prefixes(dry_run=a.dry_run)
    print(json.dumps(report, indent=2, default=str))
    # A REFUSAL IS NOT A SUCCESS. Nothing was deleted, but nothing was checked
    # either, and this runs unattended often enough that a zero exit would be
    # read as "no orphans".
    return 1 if report.get("refused") else 0


if __name__ == "__main__":
    raise SystemExit(main())
