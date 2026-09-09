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

from reporting_platform.common.context import Feed

log = logging.getLogger("arrival")


def _client():
    # Imported here, not at module scope, matching retention/landing.py. The
    # routing helpers in this module -- `matching`, `parse_filename` -- are
    # pure and are imported by things that never touch object storage.
    import boto3

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


def landed_md5_lookup(feed: Feed, keys: Iterable[str]):
    """`landing_filename -> md5 of the delivery landed under it, or None`.

    What lets the conformance gate tell an unchanged RESEND from a genuine
    correction. `taken` says the name is used; only the bytes say by what.
    See `conform._free_name` and `conform.DuplicateDelivery`.

    TWO SOURCES, IN THIS ORDER, and the second is not redundant. The metadata
    sibling already records `md5` measured at the door, so the ordinary case
    costs one small GET. But `inbox._promote` writes the data file BEFORE the
    metadata and treats a failed metadata write as the survivable failure --
    "the delivery is complete and will ingest with its provenance missing" --
    and an object an approved sender wrote straight into `landing/` has no
    sibling at all. Falling back to hashing the landed object covers both,
    and covers everything landed before this function existed.

    **None means UNKNOWN, and unknown must not be read as "different".** The
    caller versions on None, which is the fail-open direction: an unnecessary
    `_v2` costs one object, and a suppressed restatement loses evidence.

    `keys` is the feed's landing listing, which the caller already has -- so a
    name with nothing landed under it costs no request at all. Results are
    cached: a resend walks the same candidate more than once.
    """
    from reporting_platform.ingest import conform

    prefix = f"{feed.landing_prefix}/{feed.name}/"
    present = set(keys)
    cache: dict[str, str | None] = {}

    def lookup(filename: str) -> str | None:
        if filename in cache:
            return cache[filename]
        cache[filename] = md5 = _md5_of_landed(prefix, filename, present,
                                               conform.METADATA_SUFFIX)
        return md5

    return lookup


def _md5_of_landed(prefix: str, filename: str, present: set[str],
                   metadata_suffix: str) -> str | None:
    import hashlib
    import json

    client = _client()
    sidecar = f"{prefix}{filename}{metadata_suffix}"
    if sidecar in present:
        try:
            body = client.get_object(Bucket=_bucket(), Key=sidecar)["Body"].read()
            recorded = json.loads(body).get("md5")
            if recorded:
                return str(recorded)
        except Exception as exc:                            # noqa: BLE001
            # Unreadable or malformed metadata is not a reason to refuse the
            # delivery -- fall through to the object itself.
            log.warning("cannot read %s: %s", sidecar, str(exc)[:200])

    key = f"{prefix}{filename}"
    if key not in present:
        return None
    try:
        body = client.get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except Exception as exc:                                # noqa: BLE001
        log.warning("cannot read %s: %s", key, str(exc)[:200])
        return None
    return hashlib.md5(body).hexdigest()


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
    """COB dates the raw retention policy would keep, given `observed`.

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


def find_pending(feed: Feed, skip_ingested_check: bool = False,
                 reconcile: bool = True) -> list[str]:
    """MANIFEST keys for deliveries that have arrived but not been ingested.

    Returns keys under `ready/`, not `landing/`. What ingest consumes is a
    manifest, and the COB date comes from inside it rather than a regex re-run
    here.

    `reconcile=True` first gives every landed object a manifest -- a cheap,
    idempotent, Spark-free pass that makes `ready/` a DERIVED INDEX of landing
    rather than a queue that can be left unfilled, because the production
    arrival path is an agent doing a PutObject that runs no code of ours.

    Two filters after that, and the second is not optional.

    `already_ingested()` derives its ledger from the raw table's own
    `_source_file` values, which cannot drift from reality -- but it also
    cannot distinguish "never ingested" from "ingested, then expired by
    retention". Retention deletes whole COB dates, taking their `_source_file`
    rows with them, so without the second filter every retention-expired file
    reappears as pending: ingest -> expire -> re-ingest -> expire, silently
    undoing the policy.

    So the second filter recomputes the retention keep-set from the COB dates
    seen in LANDING -- which still has every date -- and treats a candidate
    outside it as expired rather than new. A floor check is not enough: the
    policy keeps "10 recent business days PLUS 80 month-ends", so expired dates
    are gaps in the middle of the range, not everything below a cutoff.

    THE KEEP-SET COMES FROM `landing/`, NOT FROM THE MANIFESTS, and that is
    load-bearing. `retention.yml`'s `landing:` window must be >= the raw
    layer's precisely because this needs a prefix that still holds every date.
    `ready/` is a days-long cache; computing the keep-set from it would
    silently narrow the window and start reporting live COB dates as expired.
    Giving manifests an eight-year lifetime instead is the load-control-table
    trap in a different hat.
    """
    from reporting_platform.ingest import normalize as norm

    if reconcile:
        norm.reconcile(feed)

    entries = norm.manifests_for(feed)
    if skip_ingested_check:
        return [key for key, _ in entries]

    done = already_ingested(feed)
    fresh = [(key, m) for key, m in entries
             if not any(p["object_key"] in done for p in m["parts"])]
    if not fresh:
        return []

    # Dates seen across ALL landed objects, expired or not.
    dates = []
    for k in matching(feed, list_landing(feed)):
        parsed = feed.parse_filename(k.rsplit("/", 1)[-1])
        if parsed is not None:
            dates.append(parsed[0])
    if not dates:
        return [key for key, _ in fresh]
    keep = retention_keep_dates(feed, dates)

    pending, expired = [], []
    for key, m in fresh:
        if norm.cob_date_of(m) not in keep:
            expired.append(key)
        else:
            pending.append(key)
    if expired:
        log.info(
            "%s: ignoring %d landed object(s) outside the retention keep-set "
            "(expired, not new): %s",
            feed.name, len(expired),
            ", ".join(sorted(expired)[:3]) + ("..." if len(expired) > 3 else ""))
    return pending


def put_landing(feed: Feed, local_path: str, filename: str | None = None) -> str:
    """Upload a local file into the feed's landing prefix.

    Used by the sample-data generator and by the manual walkthrough. In the
    cluster this is what the Windows-side push agent does.
    """
    name = filename or os.path.basename(local_path)
    key = f"{feed.landing_prefix}/{feed.name}/{name}"
    _client().upload_file(local_path, _bucket(), key)
    return key


def put_landing_bytes(feed: Feed, filename: str, body: bytes,
                      content_type: str = "") -> str:
    """Write bytes into the feed's landing prefix under a chosen name.

    The conformance gate needs this rather than `put_landing`: it uploads
    content it already holds, under a name it DERIVED, and it also writes a
    metadata sibling that exists only in memory. `put_landing` takes a local
    path and defaults the key to that file's own basename, which is exactly
    the coupling the gate has to break.
    """
    key = f"{feed.landing_prefix}/{feed.name}/{filename}"
    kwargs = {"ContentType": content_type} if content_type else {}
    _client().put_object(Bucket=_bucket(), Key=key, Body=body, **kwargs)
    return key
