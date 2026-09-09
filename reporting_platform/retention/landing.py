"""Landing-prefix retention: the evidence copy, kept for a flat number of years.

WHAT THIS IS AND WHY IT IS NOT THE TABLE RULE. `landing/` holds an immutable
copy of every CSV every feed has ever delivered -- the answer to "what did the
file we actually received say?", normally asked after a restatement, about a
COB date the table layers have long since expired. So this sweep keeps
**everything** for `keep_years` and then removes it, rather than sampling by
the "10 business days plus 80 month-ends" keep-set the tables use. Superseded
re-deliveries are kept too, because the interesting question is usually about
the first one.

That is a policy decision, not an implementation detail: it costs the cheapest
bytes in the estate to preserve the only non-reconstructable artefact the
platform holds.

THIS DID NOT USED TO EXIST. `retention.yml` carried a `landing:` block with
four keys, `docs/RETENTION.md` described its behaviour in the present tense,
and no code read any of it -- so the landing prefix grew without bound and "we
keep every delivery forever" was being decided by omission.

AGE MEANS COB DATE, NOT UPLOAD TIME. A file re-delivered late carries an old
COB date and a recent `LastModified`; the data in it is still ten years old and
the retention question is about the data. Parsing also means a key whose name
this platform does not recognise is never deleted -- a deliberate skip rather
than a fallback to object age. Deleting something we cannot identify is not a
risk worth taking against a prefix whose entire purpose is evidence.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

from reporting_platform.common.context import (
    Feed, class_keep_years, feeds, retention_policy,
)
from reporting_platform.ingest import conform

log = logging.getLogger("retention.landing")

# Average days per year including leap years. Landing retention is a coarse
# "about ten years" policy, not a calendar computation -- being a day out on
# a ten-year boundary changes nothing anyone cares about, whereas pretending
# to calendar precision invites someone to depend on it.
DAYS_PER_YEAR = 365.25


def _client():
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


def keep_years(feed: Feed | None = None) -> int:
    """The landing window for `feed`, with the interlock against raw's window.

    PER FEED SINCE RETENTION CLASSES (REQ-600/601). `feed.retention_class`
    names the obligation and retention.yml gives it a window; a feed in the
    default `standard` class resolves to exactly the `landing.keep_years` this
    used to return for everything. `None` asks for the prefix default, which is
    what a caller with no feed in hand wants.

    THE INTERLOCK IS NOW PER FEED TOO, and it had to move with the window.
    `find_pending` computes the retention keep-set from the COB dates it can
    see in LANDING, per feed, precisely so a date expired from the table is
    recognised as expired rather than re-ingested. That only works while a
    feed's landing prefix still holds every date its raw table might. Left
    global, this check would have compared the DEFAULT class against the raw
    window and gone quiet about exactly the feed a short class was applied to.

    Warn rather than refuse: unlike the GC cutoff interlock the failure is
    gradual and recoverable, and an operator deliberately shortening a class in
    a sandbox should not be blocked.
    `retention.check_reproducibility_window` is the one that refuses.
    """
    years = (class_keep_years("landing", feed.retention_class) if feed
             else int(retention_policy("landing")["keep_years"]))
    raw_months = int(retention_policy("raw").get("keep_month_ends", 0))
    raw_years = raw_months / 12
    if raw_years and years < raw_years:
        log.warning(
            "landing keep_years=%d for %s is shorter than the raw layer's "
            "window (%d month-ends = %.1f years). find_pending derives its "
            "keep-set from the dates present in that feed's landing prefix, so "
            "live COB dates will start being treated as expired.",
            years, f"feed {feed.name} (class {feed.retention_class})" if feed
            else "the default class", raw_months, raw_years)
    return years


def _cob_date(feed: Feed, filename: str,
                   siblings: set[str] | None = None) -> date | None:
    """The COB date of one landed object: delivery, metadata or control.

    THREE KINDS OF OBJECT LIVE IN A LANDING PREFIX NOW, and this sweep deletes
    things, so each has to be dated correctly or not at all.

    A DELIVERY dates from its own name -- `parse_filename`, unchanged.

    A METADATA sibling is named `<delivery>.meta.json`, so its date is inside
    its own name. That is why the suffix convention was chosen over a parallel
    prefix: an orphaned metadata object still expires rather than accumulating.

    A CONTROL file cannot do either. `TRADE_20260801.ctl` matches no
    `filename_pattern`, and stripping a suffix does not help because the data
    file is `TRADE_20260801.csv` and the extension is not guessable. It is
    dated from the SIBLING it gates: the delivery in the same prefix whose stem
    its `delivery.control.pattern` matches, found with no extra S3 call.

    Searching the control filename for an 8-digit run would be simpler and is
    deliberately not done: this function authorises DELETION from the evidence
    copy, and a name with two 8-digit runs would pick the wrong one. No sibling
    means None, which means keep.
    """
    if conform.is_metadata_key(filename):
        filename = conform.delivery_of_metadata(filename)
    parsed = feed.parse_filename(filename)
    if parsed:
        return parsed[0]

    from reporting_platform.ingest import normalize as norm

    if siblings and norm.is_control_file(feed, filename):
        pattern = (feed.delivery.get("control") or {}).get("pattern", "")
        for candidate in sorted(siblings):
            dated = feed.parse_filename(candidate)
            if not dated:
                continue
            stem = candidate[:candidate.rindex(".")] if "." in candidate \
                else candidate
            if re.fullmatch(pattern.format(stem=re.escape(stem)), filename):
                return dated[0]
    return None


def _expiry(feed: Feed, key: str, cutoff: date,
            siblings: set[str] | None = None) -> date | None:
    """COB date of `key` if it is past `cutoff`, else None.

    Returns None for anything unparseable, which means "leave it alone". The
    landing prefix is the evidence copy; an object whose name this platform
    does not recognise is exactly the object not to delete on a guess.
    """
    bd = _cob_date(feed, key.rsplit("/", 1)[-1], siblings)
    if bd is None:
        return None
    return bd if bd < cutoff else None


def sweep_landing(dry_run: bool = True) -> dict:
    """Remove landed objects whose COB date is older than the window.

    ONE CUTOFF PER FEED, not one for the sweep. A feed's retention class
    decides how long its evidence is kept, so the cutoff is resolved inside the
    loop and reported per feed.

    THE TOP-LEVEL WINDOW IS NAMED `default_*`, and the rename is the point.
    They were `keep_years`/`cutoff`, which read as the window this sweep
    applied -- and after classes they are only the DEFAULT class's. Verified on
    this stack, where the summary said `keep_years: 10, cutoff: 2016-09-05`
    while `ref_collateral` was swept at 7 years against a 2019 cutoff: a reader
    of the summary would have concluded nothing after 2016 could have been
    deleted. `classes_applied` is the honest one-line answer, and the per-feed
    entries carry the rest. Nothing outside this module read the old keys.
    """
    years = keep_years()
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=years * DAYS_PER_YEAR)).date()
    s3, bucket = _client(), _bucket()
    paginator = s3.get_paginator("list_objects_v2")

    result: dict = {"default_keep_years": years,
                    "default_cutoff": cutoff.isoformat(),
                    "classes_applied": {}, "dry_run": dry_run, "feeds": [],
                    "objects_deleted": 0, "bytes_deleted": 0,
                    "unrecognised": 0}

    for feed in feeds().values():
        feed_years = keep_years(feed)
        feed_cutoff = (datetime.now(timezone.utc)
                       - timedelta(days=feed_years * DAYS_PER_YEAR)).date()
        prefix = f"{feed.landing_prefix}/{feed.name}/"
        expired, kept, unknown, freed = [], 0, 0, 0

        # THE WHOLE PREFIX IS COLLECTED BEFORE ANYTHING IS DATED, because a
        # control file is dated from the delivery it gates and that sibling
        # may be on any page. Costs one list of keys already being paged
        # through, and no extra request.
        objects = [obj for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
                   for obj in page.get("Contents", [])]
        siblings = {o["Key"].rsplit("/", 1)[-1] for o in objects}

        for obj in objects:
            bd = _expiry(feed, obj["Key"], feed_cutoff, siblings)
            if bd is None:
                # `_cob_date`, not `parse_filename`: a metadata sibling
                # dates through its delivery's name and a control file
                # through the delivery it gates, and counting either as
                # `unrecognised` would report a configuration error for every
                # conformed delivery in the prefix.
                if _cob_date(feed, obj["Key"].rsplit("/", 1)[-1],
                                  siblings) is None:
                    unknown += 1
                else:
                    kept += 1
                continue
            expired.append((obj["Key"], obj["Size"], bd))
            freed += obj["Size"]

        for key, _size, _bd in expired:
            if not dry_run:
                # One at a time, matching orphan_storage: a batched
                # delete_objects reports partial failure in a way that is easy
                # to ignore, and this prefix is the evidence copy.
                s3.delete_object(Bucket=bucket, Key=key)

        entry = {"feed": feed.name, "retention_class": feed.retention_class,
                 "keep_years": feed_years, "cutoff": feed_cutoff.isoformat(),
                 "expired": len(expired), "retained": kept,
                 "unrecognised": unknown, "bytes": freed}
        if expired:
            entry["oldest"] = min(bd for _, _, bd in expired).isoformat()
            entry["newest_expired"] = max(bd for _, _, bd in expired).isoformat()
        result["feeds"].append(entry)
        # Which windows this sweep ACTUALLY applied, and to how many feeds.
        # One number cannot describe a per-feed sweep, so this is the smallest
        # true summary: {"standard": {"keep_years": 10, "feeds": 3}, ...}.
        klass = result["classes_applied"].setdefault(
            feed.retention_class, {"keep_years": feed_years, "feeds": 0,
                                   "cutoff": feed_cutoff.isoformat()})
        klass["feeds"] += 1
        result["objects_deleted"] += len(expired) if not dry_run else 0
        result["bytes_deleted"] += freed if not dry_run else 0
        result["unrecognised"] += unknown
        log.info("landing %s [%s, %dy]: %d expired (before %s), %d retained, "
                 "%d unrecognised%s", feed.name, feed.retention_class,
                 feed_years, len(expired), feed_cutoff, kept, unknown,
                 " [dry run]" if dry_run else "")

    if result["unrecognised"]:
        log.warning(
            "%d landed object(s) do not match any feed's filename pattern and "
            "were left alone. They are never ingested either, so they are dead "
            "weight -- but deleting an unidentifiable object out of the "
            "evidence prefix is not this job's call.", result["unrecognised"])
    return result


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be removed, changing nothing")
    a = p.parse_args(argv)
    print(json.dumps(sweep_landing(a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
