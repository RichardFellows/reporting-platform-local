"""Quarantine retention: refused deliveries, kept by flat age (REQ-106).

The counterpart to `retention/landing.py`, and shaped the same way for the
same reason: `quarantine/` holds what an upstream actually sent, so it is kept
whole for `keep_years` rather than sampled by the keep-set the tables use.
`config/retention.yml` says why the two windows are separate keys.

DATED FROM THE KEY, NOT FROM THE FILE. `landing.py` dates an object by parsing
its filename, and refuses to delete anything it cannot parse -- correct there,
because everything in `landing/` is conformant by contract. Nothing in
`quarantine/` is: *not being nameable* is one of the commonest reasons a file
is here, so filename parsing would decline to delete almost the entire prefix
and the sweep would do nothing forever.

So the rejection date is put INTO the key when the object is written --
`quarantine/<feed>/<yyyy>/<mm>/<timestamp>_<name>` -- and read back out of it
here. The property that matters is preserved: the date is in the object's own
name, needs no lookup, and an object whose key this cannot parse is still left
alone. What changes is that the platform, not the upstream, chose the name, so
"cannot parse" now means a hand-written key rather than an ordinary delivery.

THE ROW OUTLIVES THE BYTES, deliberately. `registry.rejection` is small and is
not swept here: the fact that a delivery was refused on a date, and why, is
worth keeping after the bytes are not. Sweeping both would make the registry
unable to answer "has this upstream sent us something broken before?", which
is most of the value of having recorded it.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

from reporting_platform.common.context import retention_policy
from reporting_platform.registry.rejections import QUARANTINE_PREFIX
from reporting_platform.retention.landing import DAYS_PER_YEAR, _bucket, _client

log = logging.getLogger("retention.quarantine")

# quarantine/<feed>/<yyyy>/<mm>/<yyyymmdd>T<hhmmss>Z_<name>
KEY_RE = re.compile(
    rf"^{QUARANTINE_PREFIX}/[^/]+/(?P<year>\d{{4}})/(?P<month>\d{{2}})/"
    rf"(?P<stamp>\d{{8}})T\d{{6}}Z_.+$")


def keep_years() -> int:
    return int(retention_policy("quarantine")["keep_years"])


def rejected_on(key: str) -> date | None:
    """The rejection date in a quarantine key, or None if it has none.

    The day comes from the TIMESTAMP in the filename, not from the
    `<yyyy>/<mm>` folders. Those exist so the prefix can be listed by month
    without a full scan and are checked against the timestamp: a key whose
    folders and stamp disagree was not written by this platform, and the rule
    for anything this does not recognise is to leave it alone.
    """
    m = KEY_RE.match(key)
    if not m:
        return None
    try:
        when = datetime.strptime(m.group("stamp"), "%Y%m%d").date()
    except ValueError:
        return None
    if (when.year, when.month) != (int(m.group("year")), int(m.group("month"))):
        return None
    return when


def sweep_quarantine(dry_run: bool = True) -> dict:
    """Remove quarantined objects older than the window."""
    years = keep_years()
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=years * DAYS_PER_YEAR)).date()
    s3, bucket = _client(), _bucket()
    paginator = s3.get_paginator("list_objects_v2")

    expired, kept, unknown, freed = [], 0, 0, 0
    for page in paginator.paginate(Bucket=bucket,
                                   Prefix=f"{QUARANTINE_PREFIX}/"):
        for obj in page.get("Contents", []):
            when = rejected_on(obj["Key"])
            if when is None:
                unknown += 1
                continue
            if when >= cutoff:
                kept += 1
                continue
            expired.append(obj["Key"])
            freed += obj["Size"]

    if not dry_run:
        for key in expired:
            # One at a time, matching landing.py and orphan_storage: a batched
            # delete reports partial failure in a way that is easy to ignore.
            s3.delete_object(Bucket=bucket, Key=key)

    if unknown:
        log.warning(
            "%d object(s) under %s/ do not match the key shape this sweep "
            "writes and were left alone. Nothing here is deleted on a guess.",
            unknown, QUARANTINE_PREFIX)
    log.info("quarantine: %d expired (before %s), %d retained, %d "
             "unrecognised%s", len(expired), cutoff, kept, unknown,
             " [dry run]" if dry_run else "")
    return {"keep_years": years, "cutoff": cutoff.isoformat(),
            "dry_run": dry_run, "expired": len(expired), "retained": kept,
            "unrecognised": unknown,
            "objects_deleted": 0 if dry_run else len(expired),
            "bytes_deleted": 0 if dry_run else freed,
            "bytes": freed}


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be removed, changing nothing")
    a = p.parse_args(argv)
    print(json.dumps(sweep_quarantine(a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
