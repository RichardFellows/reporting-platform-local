"""Can a published run still be reproduced? REQ-702.

WHY A TEST AND NOT A COMMENT. Everything that makes a published run
reproducible is a *pin* — `record_publication` cuts
`published/<business_date>/<run_id>` against main, and retention keeps that tag
for `references.published_tags`. Nothing about that arrangement announces its
own failure. A tag deleted too early, a GC cutoff that collected a file the tag
still referenced, a maintenance step that rewrote data and then expired the
snapshot the tag pointed at: every one of those leaves a catalog that looks
healthy and a pin that no longer resolves, and the first person to find out is
whoever was asked to reproduce a figure from three years ago.

So the pin is EXERCISED, on a schedule, against the real catalog. Reading a
table at a tag is the whole check: it resolves the reference, opens the
metadata the commit pointed at, and reads the data files that metadata names.
If any link in that chain has been collected, the read raises rather than
returning wrong numbers.

WHY OLDER THAN `recent_partition_days`. A tag cut this morning proves almost
nothing: its files are the ones main is still using, so it would resolve even
if pinning did not work at all. Maintenance is what makes the question real.
`maintain.py` compacts partitions older than `recent_partition_days`, which
REWRITES their data files; main moves to the new ones and the tag keeps
referencing the old, which is exactly the state in which a too-eager GC or
snapshot expiry destroys reproducibility. A run older than that cutoff has
been through at least one such cycle, so it is the youngest run whose survival
means anything. Below the cutoff the check reports `not_yet_meaningful` rather
than passing, because a green result there is worse than no result.

WHAT THIS CANNOT TELL YOU. That the numbers are the same as those published —
it asserts the tables are READABLE at the pin and reports their row counts, not
that they match a report nobody has recorded yet. Comparing against what was
actually published needs the run record: which deliveries, which code, which
report version. Until that exists this is a liveness check on the pin, which is
the failure mode that actually occurs and the one that is silent.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from reporting_platform.common.context import (
    Nessie, managed_tables, maintenance_config, spark_session,
)

log = logging.getLogger("reproducibility")


def _recent_partition_days() -> int:
    return int((maintenance_config().get("defaults") or {})
               .get("recent_partition_days", 7))


def published_tags(nessie: Nessie) -> list[dict]:
    """Every published tag with its commit time, oldest first.

    Oldest first because the oldest pin is the one most has happened to: the
    most compaction cycles, the most snapshot expiries, the most GC sweeps.
    If reproducibility is broken anywhere it breaks there first.
    """
    from reporting_platform.retention.retention import TAG_RE

    out = []
    for ref in nessie.list_references("published/", fetch_all=True):
        if ref.get("type") != "TAG":
            continue
        m = TAG_RE.match(ref["name"])
        if not m:
            continue
        committed = ((ref.get("metadata") or {})
                     .get("commitMetaOfHEAD") or {}).get("commitTime")
        when = None
        if committed:
            try:
                when = datetime.fromisoformat(committed.replace("Z", "+00:00"))
            except ValueError:
                when = None
        out.append({"tag": ref["name"], "report": m.group("report"),
                    "business_date": m.group("bd"), "committed": when})
    # A tag with no readable commit time sorts last rather than first: its age
    # is unknown, and treating unknown as ancient would pick it as the probe
    # and report a misleading "oldest".
    return sorted(out, key=lambda t: (t["committed"] is None,
                                      t["committed"] or datetime.max.replace(
                                          tzinfo=timezone.utc)))


def check_tag(tag: str, tables: list[str]) -> dict:
    """Read every managed table at `tag`. One Spark session, one session stop.

    A table missing AT THE PIN is not a failure: the platform gains tables over
    time, and a report published before `reporting.exposure_by_country` existed
    cannot be expected to contain it. A table that exists and cannot be READ is
    the failure this looks for.
    """
    result: dict = {"tag": tag, "tables": [], "unreadable": [], "absent": []}
    spark = spark_session("reproducibility", ref=tag)
    try:
        for table in tables:
            try:
                rows = spark.sql(f"SELECT COUNT(*) c FROM {table}").collect()
                result["tables"].append({"table": table,
                                         "rows": rows[0]["c"]})
            except Exception as exc:                     # noqa: BLE001
                text = f"{type(exc).__name__}: {str(exc)[:300]}"
                # "table not found" at an old pin is history, not damage.
                if "NoSuchTable" in text or "TABLE_OR_VIEW_NOT_FOUND" in text:
                    result["absent"].append({"table": table, "error": text})
                else:
                    log.error("cannot read %s at %s: %s", table, tag, text)
                    result["unreadable"].append({"table": table,
                                                 "error": text})
    finally:
        spark.stop()
    result["ok"] = not result["unreadable"]
    return result


def run(tag: str | None = None) -> dict:
    """Exercise the oldest meaningful published pin, or a named one."""
    days = _recent_partition_days()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    tables = [t for t, _layer in managed_tables()]
    report: dict = {"recent_partition_days": days,
                    "cutoff": cutoff.isoformat(),
                    "checked_at": datetime.now(timezone.utc).isoformat()}

    if tag is not None:
        report.update(check_tag(tag, tables))
        report["selected"] = "explicit"
        return report

    tags = published_tags(Nessie())
    report["published_tags"] = len(tags)
    if not tags:
        report.update({"ok": True, "status": "no_published_tags"})
        return report

    eligible = [t for t in tags
                if t["committed"] is not None and t["committed"] < cutoff]
    if not eligible:
        # Deliberately NOT a pass. Every pin is younger than one compaction
        # cycle, so its files are still the ones main uses and the read would
        # succeed whether pinning worked or not.
        newest = max(t["committed"] for t in tags if t["committed"]) \
            if any(t["committed"] for t in tags) else None
        report.update({
            "ok": True,
            "status": "not_yet_meaningful",
            "detail": (
                f"no published tag is older than recent_partition_days "
                f"({days}d). Every pin still shares its files with main, so a "
                f"successful read would not prove the pin holds."),
            "newest_tag_committed": newest.isoformat() if newest else None,
        })
        return report

    chosen = eligible[0]
    report.update(check_tag(chosen["tag"], tables))
    report["selected"] = "oldest_eligible"
    report["business_date"] = chosen["business_date"]
    report["committed"] = chosen["committed"].isoformat()
    report["eligible_tags"] = len(eligible)
    report["status"] = "reproduced" if report["ok"] else "BROKEN"
    if not report["ok"]:
        log.error("REPRODUCIBILITY BROKEN at %s: %d table(s) unreadable. A "
                  "published run can no longer be read at its own pin.",
                  chosen["tag"], len(report["unreadable"]))
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--tag", default=None,
                   help="check this published tag instead of the oldest "
                        "eligible one")
    p.add_argument("--fail-on-break", action="store_true",
                   help="exit non-zero if a pin no longer resolves")
    a = p.parse_args(argv)

    report = run(a.tag)
    print(json.dumps(report, indent=2, default=str))
    if a.fail_on_break and not report.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
