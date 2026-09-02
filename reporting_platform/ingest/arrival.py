"""Feed arrival detection.

This is the one component whose local implementation genuinely differs from
the cluster (see docs/OPENSHIFT-MAPPING.md). Locally we poll a landing prefix
in MinIO. In OpenShift the preferred shape is a push: an agent on the existing
Windows host — which already holds the AD context for the DFS share — does an
S3 PutObject into the landing prefix, and the object-created event triggers the
DAG. That inverts the trust direction and keeps cross-domain Kerberos out of
the cluster entirely.

`find_pending` is the fallback poll path, used locally and as a safety net in
the cluster in case an event is missed. Events are an optimisation; the poll is
the correctness guarantee.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Iterable

import boto3

from reporting_platform.common.context import Feed

log = logging.getLogger("arrival")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _bucket() -> str:
    warehouse = os.environ.get("REPORTING_WAREHOUSE", "s3a://lakehouse/warehouse")
    return warehouse.replace("s3a://", "").replace("s3://", "").split("/")[0]


def list_landing(feed: Feed) -> list[str]:
    """All object keys under the feed's landing prefix, oldest first."""
    prefix = f"{feed.landing_prefix}/{feed.name}/"
    paginator = _client().get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix):
        objs.extend(page.get("Contents", []))
    return [o["Key"] for o in sorted(objs, key=lambda o: o["LastModified"])]


def matching(feed: Feed, keys: Iterable[str]) -> list[str]:
    """Keys whose filename matches the feed's declared pattern."""
    out = []
    for key in keys:
        if feed.parse_filename(key.rsplit("/", 1)[-1]) is not None:
            out.append(key)
    return out


def already_ingested(feed: Feed) -> set[str]:
    """Source files already present in the raw table.

    Reading `_source_file` from the table itself, rather than keeping a
    separate control table, means the ledger cannot drift from reality —
    which is exactly the failure mode the legacy `stg` load-control tables
    had. Slightly more expensive per run; far cheaper per incident.
    """
    from reporting_platform.common.context import spark_session

    spark = spark_session(f"arrival-check-{feed.name}", ref="main")
    try:
        rows = spark.sql(
            f"SELECT DISTINCT _source_file FROM {feed.raw_table}"
        ).collect()
        return {r["_source_file"] for r in rows}
    except Exception:
        return set()          # table not created yet: nothing ingested
    finally:
        spark.stop()


def retention_keep_dates(feed: Feed, observed: list[date]) -> set[date]:
    """Business dates the raw retention policy would keep, given `observed`.

    Deliberately computed from the dates seen in LANDING, not from the table.
    The table has already lost the expired ones -- that is the whole problem
    find_pending() has to work around.
    """
    from reporting_platform.common.calendar_rules import keep_set
    from reporting_platform.common.context import retention_policy

    policy = retention_policy(feed.raw_namespace if feed.raw_namespace in
                             ("raw",) else "raw")
    return set(keep_set(
        sorted(set(observed)),
        keep_business_days=policy.get("keep_business_days", 0),
        keep_month_ends=policy.get("keep_month_ends", 0),
    ))


def control_key(feed: Feed, data_key: str) -> str | None:
    """The landing key of the control file describing this delivery, if any."""
    from reporting_platform.ingest import control

    parsed = feed.parse_filename(data_key.rsplit("/", 1)[-1])
    if parsed is None or not control.spec(feed):
        return None
    name = control.control_name(feed, parsed[0], parsed[1])
    return f"{feed.landing_prefix}/{feed.name}/{name}" if name else None


def _with_control(feed: Feed, candidates: list[str], landed: list[str]) -> list[str]:
    """Drop candidates whose required control file has not landed yet."""
    from reporting_platform.ingest import control

    if not control.required(feed):
        return candidates
    present = set(landed)
    ready, waiting = [], []
    for key in candidates:
        expected = control_key(feed, key)
        (ready if expected and expected in present else waiting).append(key)
    if waiting:
        log.info("%s: %d file(s) waiting for a control file: %s", feed.name,
                 len(waiting), ", ".join(k.rsplit("/", 1)[-1] for k in waiting[:3]))
    return ready


def find_pending(feed: Feed, skip_ingested_check: bool = False) -> list[str]:
    """Object keys that have arrived but not yet been ingested.

    Two filters, and the second is not optional.

    `already_ingested()` derives its ledger from the raw table's own
    `_source_file` values, which cannot drift from reality -- but it also
    cannot distinguish "never ingested" from "ingested, then expired by
    retention". Retention deletes whole business dates, taking their
    `_source_file` rows with them, so without the second filter every
    retention-expired file reappears as pending and gets re-ingested. That is
    a silent loop: ingest -> expire -> re-ingest -> expire, resurrecting data
    the retention policy deliberately removed and quietly undoing the policy.

    So the second filter recomputes the retention keep-set from the business
    dates seen in LANDING -- which still has every date, expired or not -- and
    treats a candidate outside that keep-set as expired rather than new. Note
    a floor check is not enough: the policy keeps "10 recent business days
    PLUS 80 month-ends", so expired dates are gaps in the middle of the range,
    not everything below some cutoff.
    """
    landed = list_landing(feed)
    candidates = matching(feed, landed)
    # A delivery whose control file has not arrived yet is NOT pending. Without
    # this, the file becomes pending the moment it lands and the ingest fails
    # on a control file that is simply seconds behind it -- a spurious failure
    # on every feed whose sender writes the sidecar second, which is most of
    # them. See docs/DECISIONS.md#control-files-abort-the-ingest
    candidates = _with_control(feed, candidates, landed)
    if skip_ingested_check:
        return candidates

    done = already_ingested(feed)
    fresh = [k for k in candidates if k not in done]
    if not fresh:
        return []

    # Dates seen across ALL landed objects, expired or not.
    dates = []
    for k in candidates:
        parsed = feed.parse_filename(k.rsplit("/", 1)[-1])
        if parsed is not None:
            dates.append(parsed[0])
    if not dates:
        return fresh
    keep = retention_keep_dates(feed, dates)

    pending, expired = [], []
    for k in fresh:
        parsed = feed.parse_filename(k.rsplit("/", 1)[-1])
        if parsed is not None and parsed[0] not in keep:
            expired.append(k)
        else:
            pending.append(k)
    if expired:
        log.info(
            "%s: ignoring %d landed object(s) outside the retention keep-set "
            "(expired, not new): %s",
            feed.name, len(expired),
            ", ".join(sorted(expired)[:3]) + ("..." if len(expired) > 3 else ""))
    return pending


def read_object(key: str) -> bytes:
    """Fetch one landing object whole.

    Used for control files and for digesting a delivery. Whole rather than
    streamed on purpose: a control file is a few hundred bytes, and a digest
    has to see every byte anyway.
    """
    return _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()


def put_landing(feed: Feed, local_path: str, filename: str | None = None) -> str:
    """Upload a local file into the feed's landing prefix.

    Used by the sample-data generator and by the manual walkthrough. In the
    cluster this is what the Windows-side push agent does.
    """
    name = filename or os.path.basename(local_path)
    key = f"{feed.landing_prefix}/{feed.name}/{name}"
    _client().upload_file(local_path, _bucket(), key)
    return key
