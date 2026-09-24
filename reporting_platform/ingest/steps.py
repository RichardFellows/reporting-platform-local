"""Land and ingest, as steps a person or any scheduler can call.

    land(paths)            files -> the inbox gate -> landing/ (+ control files)
    ingest(feed)           every pending delivery of a feed -> raw, each on its
                           own branch, merged to main; a snapshot tag per ingest

The same functions the `ingest_<feed>` DAGs reach: `record_snapshot` and
`drift_warnings` were the bodies of two of its tasks, and the ingest itself is
the `ingest-batch` Spark op, launched by `spark_task.run` like every other.
What the DAG adds is a trigger per arrival; what this adds is a way to run the
same thing with nothing listening. CLI: `python -m reporting_platform.ingest`.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

# Deliveries per Spark application. One session per delivery costs ~16s of
# start-up each; one for a whole backlog grows the heap until it dies
# (docs/DECISIONS.md#one-session-per-chunk). scripts/bulk_ingest.py's number.
CHUNK_SIZE = 10


def land(paths: list[Path]) -> list[dict]:
    """Offer files to the inbox gate: conform, promote control files,
    quarantine what cannot be named. Triggers nothing. See `inbox.land_files`.
    """
    from reporting_platform.ingest.inbox import land_files

    missing = [str(p) for p in paths if not Path(p).is_file()]
    if missing:
        raise FileNotFoundError(f"not files: {missing}")
    return land_files([Path(p) for p in paths])


def pending(feed_name: str) -> list[str]:
    """Manifest keys of the deliveries landed and not yet in raw.

    Reconciles `ready/` from `landing/` first, so a file put straight into
    the bucket by anything at all is found too. Spark, because "not yet in
    raw" is read from raw.
    """
    from reporting_platform.common.spark_task import run

    return run("pending", feed_name)["pending"]


def ingest(feed_name: str, keys: list[str] | None = None) -> list[dict]:
    """Ingest `keys` (default: everything pending), then pin each result.

    Returns one dict per delivery: the ingest's own result plus `tag`, or
    `error` for one that failed. One failure does not stop the others --
    each delivery has its own branch -- but the caller must look.
    """
    from reporting_platform.common.spark_task import run

    keys = pending(feed_name) if keys is None else keys
    out: list[dict] = []
    for i in range(0, len(keys), CHUNK_SIZE):
        chunk = keys[i:i + CHUNK_SIZE]
        log.info("%s: ingesting %d delivery(ies) in one Spark session",
                 feed_name, len(chunk))
        for result in run("ingest-batch", feed_name, *chunk)["results"]:
            if "error" in result:
                log.error("%s: %s failed: %s", feed_name,
                          result["object_key"], result["error"][:500])
                out.append(result)
                continue
            for message in drift_warnings(feed_name, result):
                log.warning("%s", message)
            out.append(record_snapshot(feed_name, result))
    return out


def record_snapshot(feed_name: str, result: dict) -> dict:
    """Pin the state this ingest left, so it stays addressable.

    AN INGEST IS NOT A PUBLICATION, so this is `snapshot/<feed>/<bd>/<run_id>`,
    kept by `references.snapshot_tags` in retention.yml -- never
    `published/...`, which the reporting build cuts per report. It is still
    worth pinning: raw is where retention deletes COB dates, so the state each
    ingest left is exactly what someone may need to read back.
    See docs/DECISIONS.md#an-ingest-is-not-a-publication
    """
    from reporting_platform.common.context import Nessie, snapshot_tag

    tag = snapshot_tag(feed_name, date.fromisoformat(str(result["cob_date"])),
                       result["run_id"])
    try:
        Nessie().create_tag(tag, from_ref="main")
    except Exception as exc:          # tag already exists on a rerun
        return {**result, "tag": tag, "tag_error": str(exc)}
    return {**result, "tag": tag}


def drift_warnings(feed_name: str, result: dict) -> list[str]:
    """Schema drift is reported, never fatal.

    TWO DIFFERENT EVENTS, worded apart on purpose. DRIFT is about one
    delivery: the file did not match the contract. A CONTRACT CHANGE is about
    the deployment: `feeds.yml` changed and this was the first ingest to carry
    it into the raw table. Reading the second as the first sends somebody to
    the upstream about a change that was made here.
    """
    out = []
    if result.get("missing_columns") or result.get("extra_columns"):
        out.append(
            f"SCHEMA DRIFT {feed_name} {result['cob_date']}: "
            f"missing={result['missing_columns']} extra={result['extra_columns']}")
    if result.get("columns_added"):
        out.append(
            f"CONTRACT CHANGE {feed_name} {result['cob_date']}: added "
            f"{result['columns_added']} to the raw table. History reads NULL "
            f"for it -- added, never backfilled.")
    if result.get("columns_orphaned"):
        out.append(
            f"CONTRACT CHANGE {feed_name} {result['cob_date']}: "
            f"{result['columns_orphaned']} is in the raw table and the feed no "
            f"longer declares it. Written as NULL, never dropped; settle it "
            f"deliberately.")
    return out
