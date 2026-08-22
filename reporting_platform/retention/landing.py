"""Landing-prefix retention: the evidence copy, kept for a flat number of years.

WHAT THIS IS AND WHY IT IS NOT THE TABLE RULE.

`landing/` holds an immutable copy of every CSV every feed has ever delivered.
It is the answer to "what did the file we actually received say?" — a question
normally asked after a restatement, about a business date the table layers have
long since expired. So this sweep keeps **everything** for `keep_years` and
then removes it, rather than sampling by the "10 business days plus 80
month-ends" keep-set the tables use. Superseded re-deliveries are kept too:
`TRADE_20260813.csv` and `TRADE_20260813_v2.csv` both survive their eight
years, because the interesting question is usually about the first one.

That is a policy decision, not an implementation detail. It costs the cheapest
bytes in the estate (flat CSV on object storage) to preserve the only
non-reconstructable artefact the platform holds.

UNTIL SESSION 5 THIS DID NOT EXIST. `retention.yml` carried a `landing:` block
with four keys, `docs/RETENTION.md` described its behaviour in the present
tense, and no code read any of it — so the landing prefix grew without bound
and "we keep every delivery forever" was being decided by omission.

AGE MEANS BUSINESS DATE, NOT UPLOAD TIME. A file re-delivered late carries an
old business date and a recent `LastModified`; the data in it is still eight
years old and the retention question is about the data. Parsing also means a
key whose name this platform does not recognise is never deleted — see
`_expiry` below, where that is a deliberate skip rather than a fallback to
object age. Deleting something we cannot identify is not a risk worth taking
against a prefix whose entire purpose is evidence.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

from reporting_platform.common.context import Feed, feeds, retention_policy

log = logging.getLogger("retention.landing")

# Average days per year including leap years. Landing retention is a coarse
# "about eight years" policy, not a calendar computation -- being a day out on
# an eight-year boundary changes nothing anyone cares about, whereas pretending
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


def keep_years() -> int:
    """The landing window, with the interlock against the raw layer's window.

    `find_pending` computes the retention keep-set from the business dates it
    can see in LANDING, precisely so a date expired from the table is
    recognised as expired rather than re-ingested. That only
    works while landing still holds every date the tables might hold. Set
    landing shorter than the raw window and the keep-set is computed from a
    truncated history, so genuinely live month-ends start looking expired.

    Warn rather than refuse: unlike the GC cutoff interlock, the failure is
    gradual and recoverable, and an operator deliberately shortening landing in
    a sandbox should not be blocked.
    """
    years = int(retention_policy("landing")["keep_years"])
    raw_months = int(retention_policy("raw").get("keep_month_ends", 0))
    raw_years = raw_months / 12
    if raw_years and years < raw_years:
        log.warning(
            "landing keep_years=%d is shorter than the raw layer's window "
            "(%d month-ends = %.1f years). find_pending derives its keep-set "
            "from the dates present in landing, so live business dates will "
            "start being treated as expired.", years, raw_months, raw_years)
    return years


def _expiry(feed: Feed, key: str, cutoff: date) -> date | None:
    """Business date of `key` if it is past `cutoff`, else None.

    Returns None for anything unparseable, which means "leave it alone". The
    landing prefix is the evidence copy; an object whose name this platform
    does not recognise is exactly the object not to delete on a guess.
    """
    parsed = feed.parse_filename(key.rsplit("/", 1)[-1])
    if parsed is None:
        return None
    bd = parsed[0]
    return bd if bd < cutoff else None


def sweep_landing(dry_run: bool = True) -> dict:
    """Remove landed objects whose business date is older than the window."""
    years = keep_years()
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=years * DAYS_PER_YEAR)).date()
    s3, bucket = _client(), _bucket()
    paginator = s3.get_paginator("list_objects_v2")

    result: dict = {"keep_years": years, "cutoff": cutoff.isoformat(),
                    "dry_run": dry_run, "feeds": [], "objects_deleted": 0,
                    "bytes_deleted": 0, "unrecognised": 0}

    for feed in feeds().values():
        prefix = f"{feed.landing_prefix}/{feed.name}/"
        expired, kept, unknown, freed = [], 0, 0, 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                bd = _expiry(feed, obj["Key"], cutoff)
                if bd is None:
                    if feed.parse_filename(obj["Key"].rsplit("/", 1)[-1]) is None:
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

        entry = {"feed": feed.name, "expired": len(expired), "retained": kept,
                 "unrecognised": unknown, "bytes": freed}
        if expired:
            entry["oldest"] = min(bd for _, _, bd in expired).isoformat()
            entry["newest_expired"] = max(bd for _, _, bd in expired).isoformat()
        result["feeds"].append(entry)
        result["objects_deleted"] += len(expired) if not dry_run else 0
        result["bytes_deleted"] += freed if not dry_run else 0
        result["unrecognised"] += unknown
        log.info("landing %s: %d expired (before %s), %d retained, %d "
                 "unrecognised%s", feed.name, len(expired), cutoff, kept,
                 unknown, " [dry run]" if dry_run else "")

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
