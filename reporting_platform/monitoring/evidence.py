"""Does every published pin still have the deliveries behind it? REQ-602.

WHAT THE WINDOW CHECK CANNOT DO.
`retention.check_reproducibility_window()` compares two numbers -- the landing
window against the longest published-tag window -- and refuses the whole sweep
if landing is shorter. That catches the CONFIGURATION that guarantees evidence
loss and is cheap enough to run before every delete. What it structurally
cannot catch is one delivery going missing inside an otherwise coherent window:
a landing object deleted by hand, a sweep that ran against a shorter window
last month, an upload never actually made. The numbers still agree; the
evidence is still gone.

So this is the other half, per delivery rather than per window. For each live
published tag it takes the COB date the tag names, asks the registry which
deliveries were received for that date, and checks each one's landing object is
still there.

WHY THE REGISTRY AND NOT LANDING DIRECTLY. Listing landing for a date would
answer "is there anything there", not "is everything there that arrived". That
difference is exactly the registry's job. It also makes this a check on the
registry itself -- a delivery registered and then deleted from landing is what
it is looking for.

TWO WAYS THE INPUT SET IS RESOLVED, and the difference is REQ-400 arriving.

  * EXACT, where the tag has a run record. A published run enumerates the
    deliveries it read -- taken off the tables it built, not declared -- so the
    check asks the registry about exactly those.
  * APPROXIMATE, where it does not. A tag cut before run records existed
    carries only a COB date, and the best answer is "every delivery received
    for that date". Generous in one direction only: a published run reads more
    than one COB date, so it can MISS a delivery the run depended on. It cannot
    raise a false alarm.

Each tag's result says which of the two it got, because a green from the second
kind means less than a green from the first.
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


def check(tag_inputs: dict[str, dict]) -> dict:
    """For each pinned tag, are all its input deliveries still landed?

    `tag_inputs` maps a tag to `{"rows": [...], "resolution": "run"|
    "cob_date"}` -- see `resolve_inputs`. The landing check itself is one
    LIST per prefix over the union of every tag's rows, so a hundred tags
    sharing one feed's deliveries cost one listing, not a hundred.
    """
    rows = [r for v in tag_inputs.values() for r in v["rows"]]
    keys = {r["source_object"] for r in rows if r.get("source_object")}
    present = _existing(keys) if keys else set()

    per_tag: dict[str, dict] = {}
    for tag, value in sorted(tag_inputs.items()):
        mine = value["rows"]
        gone = [r for r in mine
                if r.get("source_object") and r["source_object"] not in present]
        # A delivery a RUN named that the registry cannot describe. Only
        # possible on the run path -- `run_input` carries no foreign key, on
        # purpose -- and a finding rather than an impossibility: the run read
        # it, so it existed, and the registry not knowing it means the registry
        # has lost something or was never reconciled.
        unregistered = [f"{r['feed']}/{r['delivery_id']}" for r in mine
                        if r.get("registered") is False]
        per_tag[tag] = {
            "resolution": value["resolution"],
            "cob_date": value.get("cob_date"),
            "deliveries": len(mine),
            "missing": sorted(r["source_object"] for r in gone),
            "unregistered": sorted(unregistered),
            # NO INPUT AT ALL for a pinned tag. Reported apart from a missing
            # object because the causes differ: an empty result usually means
            # the registry has not been reconciled since the date was ingested,
            # a missing object means the evidence really is gone. Both are
            # worth seeing; only one is an emergency.
            "unbacked": not mine,
        }

    missing = sorted({r["source_object"] for v in tag_inputs.values()
                      for r in v["rows"]
                      if r.get("source_object")
                      and r["source_object"] not in present})
    return {
        "tags": per_tag,
        "tags_checked": len(per_tag),
        "exact": sum(1 for v in per_tag.values() if v["resolution"] == "run"),
        "approximate": sum(1 for v in per_tag.values()
                           if v["resolution"] != "run"),
        "deliveries_checked": len(rows),
        "missing_evidence": missing,
        "unbacked_tags": sorted(t for t, v in per_tag.items() if v["unbacked"]),
        "unregistered_inputs": sorted(
            {u for v in per_tag.values() for u in v["unregistered"]}),
        "ok": not missing,
    }


def resolve_inputs(tags: list[dict]) -> dict[str, dict]:
    """Tag -> the deliveries behind it, by run record where there is one.

    THE RUN RECORD IS PREFERRED AND THE DATE IS THE FALLBACK, never the other
    way round: the run says what it actually read, the date says what happened
    to arrive. Where a run exists but recorded no inputs the date fallback is
    NOT used -- an empty input set on a real run is a finding about that run,
    and quietly substituting a broader answer would hide it.
    """
    from reporting_platform.registry import deliveries as reg
    from reporting_platform.registry import runs as reg_runs

    # One query per COB date, shared by every tag naming it -- which is
    # what N feeds publishing one day produces.
    by_date: dict[date, list[dict]] = {}
    out: dict[str, dict] = {}
    for tag in tags:
        name = tag["tag"]
        run = None
        try:
            run = reg_runs.run_for_tag(name)
        except Exception as exc:                                # noqa: BLE001
            log.warning("cannot read the run record for %s: %s", name,
                        f"{type(exc).__name__}: {exc}")
        if run:
            pairs = [(i["feed"], i["delivery_id"])
                     for i in reg_runs.inputs_for_run(run["run_id"])]
            out[name] = {"rows": reg.deliveries_by_id(pairs),
                         "resolution": "run",
                         "run_id": run["run_id"],
                         "report": run.get("report"),
                         "version": run.get("version_no"),
                         "cob_date": str(run.get("cob_date") or
                                              tag["cob_date"])}
            continue
        try:
            bd = date.fromisoformat(tag["cob_date"])
        except (TypeError, ValueError):
            continue
        if bd not in by_date:
            by_date[bd] = [{**r, "registered": True}
                           for r in reg.deliveries_on(bd)]
        out[name] = {"rows": by_date[bd], "resolution": "cob_date",
                     "cob_date": bd.isoformat()}
    return out


def run() -> dict:
    """Every live published tag, checked against the registry and landing."""
    report: dict = {"checked_at": datetime.now(timezone.utc).isoformat()}
    tags = published_tags(Nessie())
    report["published_tags"] = len(tags)
    if not tags:
        report.update({"ok": True, "status": "no_published_tags"})
        return report

    report.update(check(resolve_inputs(tags)))
    if not report["ok"]:
        log.error(
            "EVIDENCE MISSING for %d delivery(ies) behind a published pin: "
            "%s. The tables can still be read at the pin; what the upstream "
            "actually sent cannot. See REQ-602.",
            len(report["missing_evidence"]), report["missing_evidence"][:5])
        report["status"] = "BROKEN"
    elif report["unregistered_inputs"]:
        # A run named a delivery the registry has no row for. Not evidence
        # loss by itself -- the landing object may well be there -- but the
        # registry is what turns "is it there" into "is everything there", so
        # a gap in it degrades every future run of this check.
        log.warning(
            "%d input delivery(ies) named by a run are not in the registry: "
            "%s. `python -m reporting_platform.registry reconcile` is the "
            "rebuild path.",
            len(report["unregistered_inputs"]), report["unregistered_inputs"][:5])
        report["status"] = "unregistered_inputs"
    elif report["unbacked_tags"]:
        # Not a failure by itself: an unreconciled registry looks exactly like
        # this, and failing the nightly chain the first time a tag is cut
        # before reconcile has run would train people to ignore it.
        log.warning(
            "%d pinned tag(s) have no input deliveries: %s. Either the "
            "registry has not been reconciled since they were ingested, or "
            "their landing evidence predates the registry. "
            "`python -m reporting_platform.registry reconcile` answers which.",
            len(report["unbacked_tags"]), report["unbacked_tags"][:5])
        report["status"] = "unbacked_tags"
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
