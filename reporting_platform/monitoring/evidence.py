"""Does every published pin still have the deliveries behind it? REQ-602.

WHAT PHASE 1 BUILT AND WHY IT IS NOT ENOUGH.
`retention.check_reproducibility_window()` compares two numbers -- the landing
window against the longest published-tag window -- and refuses the whole sweep
if landing is shorter. That catches the CONFIGURATION that guarantees evidence
loss, which was the live defect, and it is cheap enough to run before every
delete. What it structurally cannot catch is one delivery going missing inside
an otherwise coherent window: a landing object deleted by hand, a sweep that
ran against a shorter window last month, an upload that was never actually
made. The numbers still agree; the evidence is still gone.

So this is the other half, and it is per delivery rather than per window. For
each live published tag it takes the business date the tag names, asks the
registry which deliveries were received for that date, and checks that each
one's landing object is still there. A pin whose inputs cannot be produced is
reported as unbacked whether or not the arithmetic in retention.yml is sound.

WHY THE REGISTRY AND NOT LANDING DIRECTLY. Listing landing for a date would
answer "is there anything there", not "is everything there that arrived". The
difference is exactly the registry's job: it is the record of what was
received, and comparing it against what still exists is the only way to see a
gap. That also makes this a check on the registry itself -- a delivery
registered and then deleted from landing is what it is looking for, and the
two cannot both be wrong in the same direction without somebody having tried.

WHAT IT STILL CANNOT TELL YOU, and this is the honest limit until phase 5.
A tag names one business date, because `record_publication` cuts it from the
date of the ingest that triggered the build. A published run reads more than
that date -- reference data from earlier dates, a month-end window, whatever
the model selects -- so "the deliveries for the tag's business date" is an
approximation of the run's real input set, and a generous one in one direction
only: it can miss a delivery from another date that the run depended on. It
cannot produce a false alarm, because everything it does check genuinely was
received for that date. The exact set needs the run record enumerating its
deliveries, which is REQ-400 and phase 5. Stated here rather than left to be
discovered from the code.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone

from reporting_platform.common.context import Nessie
from reporting_platform.monitoring.reproducibility import published_tags

log = logging.getLogger("evidence")


def _existing(keys: set[str]) -> set[str]:
    """Which of `keys` are objects in the warehouse bucket right now.

    One LIST per landing prefix rather than a HEAD per key: a decade of
    published tags against a daily feed is thousands of keys, and the prefixes
    are few.
    """
    from reporting_platform.ingest import arrival

    prefixes = sorted({k.rsplit("/", 1)[0] + "/" for k in keys})
    client, bucket = arrival._client(), arrival._bucket()
    paginator = client.get_paginator("list_objects_v2")
    present: set[str] = set()
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"] in keys:
                    present.add(obj["Key"])
    return present


def check(business_dates: dict[date, list[str]]) -> dict:
    """For each business date, are all its registered deliveries still landed?

    `business_dates` maps a date to the tags that name it, so one date pinned
    by five tags -- which is what five feeds publishing one day produces -- is
    checked once and reported against all five.
    """
    from reporting_platform.registry import deliveries as reg

    rows: list[dict] = []
    for bd in sorted(business_dates):
        for row in reg.deliveries_on(bd):
            rows.append(row)

    keys = {r["source_object"] for r in rows}
    present = _existing(keys) if keys else set()
    missing = [r for r in rows if r["source_object"] not in present]

    dated: dict[str, dict] = {}
    for bd, tags in sorted(business_dates.items()):
        for_date = [r for r in rows if r["business_date"] == bd]
        gone = [r for r in for_date if r["source_object"] not in present]
        dated[bd.isoformat()] = {
            "tags": sorted(tags),
            "deliveries": len(for_date),
            "missing": sorted(r["source_object"] for r in gone),
            # NO REGISTERED DELIVERY AT ALL for a pinned date. Reported apart
            # from a missing object because the two have different causes: an
            # empty result usually means the registry has not been reconciled
            # since this date was ingested, and a missing object means the
            # evidence really is gone. Both are worth seeing; only one is an
            # emergency, and guessing which would make the check useless.
            "unbacked": not for_date,
        }

    unbacked = sorted(d for d, v in dated.items() if v["unbacked"])
    return {
        "business_dates": dated,
        "dates_checked": len(dated),
        "deliveries_checked": len(rows),
        "missing_evidence": sorted(r["source_object"] for r in missing),
        "unbacked_dates": unbacked,
        "ok": not missing,
    }


def run() -> dict:
    """Every live published tag, checked against the registry and landing."""
    report: dict = {"checked_at": datetime.now(timezone.utc).isoformat()}
    tags = published_tags(Nessie())
    report["published_tags"] = len(tags)
    if not tags:
        report.update({"ok": True, "status": "no_published_tags"})
        return report

    dates: dict[date, list[str]] = {}
    for tag in tags:
        try:
            bd = date.fromisoformat(tag["business_date"])
        except (TypeError, ValueError):
            continue
        dates.setdefault(bd, []).append(tag["tag"])

    report.update(check(dates))
    if not report["ok"]:
        log.error(
            "EVIDENCE MISSING for %d delivery(ies) behind a published pin: "
            "%s. The tables can still be read at the pin; what the upstream "
            "actually sent cannot. See REQ-602.",
            len(report["missing_evidence"]), report["missing_evidence"][:5])
        report["status"] = "BROKEN"
    elif report["unbacked_dates"]:
        # Not a failure by itself: an unreconciled registry looks exactly like
        # this, and failing the nightly chain the first time a tag is cut
        # before reconcile has run would train people to ignore it.
        log.warning(
            "%d pinned business date(s) have no registered deliveries: %s. "
            "Either the registry has not been reconciled since they were "
            "ingested, or their landing evidence predates the registry. "
            "`python -m reporting_platform.registry reconcile` answers which.",
            len(report["unbacked_dates"]), report["unbacked_dates"][:5])
        report["status"] = "unbacked_dates"
    else:
        report["status"] = "backed"
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fail-on-missing", action="store_true",
                   help="exit non-zero if a pinned delivery is gone")
    a = p.parse_args(argv)
    report = run()
    print(json.dumps(report, indent=2, default=str))
    if a.fail_on_missing and not report.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
