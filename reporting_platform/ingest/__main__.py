"""Land files and ingest them into raw, without an orchestrator.

    python -m reporting_platform.ingest land inbox/fo_trade_20260801.csv ...
    python -m reporting_platform.ingest pending fo_trade
    python -m reporting_platform.ingest ingest fo_trade [ref_rating ...]
    python -m reporting_platform.ingest ingest --all
    python -m reporting_platform.ingest transport MARKER_KEY...
    python -m reporting_platform.ingest transport --pending [--window N | --cob-date D... | --all]

`land` is the inbox gate over the files named (control files included):
what cannot be named is quarantined, nothing is triggered. `ingest` takes
every pending delivery of each feed into raw -- each on its own Nessie branch,
merged on success -- and cuts a `snapshot/` tag per ingest. Exits 1 if
anything was refused or failed. See `reporting_platform.ingest.steps`.

`transport` is the other way in: a Transport DCM (or `reporting_transport
publish`) completed under `received/`, carried to raw by the same four steps
the `transport_ingest` DAG runs (`reporting_platform.ingest.transport_steps`).
Name the markers, or `--pending` to take every one not yet in raw -- found
the way `transport_reconcile` finds them. `--list` only says which.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# What `land` reports that is not a failure. Anything else -- rejected,
# upload failed, not landed -- is something the person has to look at.
LANDED = {"landed", "conformed", "duplicate", "held (control file)"}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m reporting_platform.ingest",
                                description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    la = sub.add_parser("land", help="files -> inbox gate -> landing/")
    la.add_argument("files", nargs="+", type=Path)
    pe = sub.add_parser("pending", help="manifest keys landed, not yet in raw")
    pe.add_argument("feed")
    ig = sub.add_parser("ingest", help="pending deliveries -> raw -> main")
    ig.add_argument("feeds", nargs="*")
    ig.add_argument("--all", action="store_true", help="every configured feed")
    tr = sub.add_parser("transport", help="completed Transports -> raw -> main")
    tr.add_argument("markers", nargs="*", help="_COMPLETE.json object keys")
    tr.add_argument("--pending", action="store_true",
                    help="every completed Transport not yet in raw")
    scope = tr.add_mutually_exclusive_group()
    scope.add_argument("--window", type=int, metavar="DAYS",
                       help="--pending over the last DAYS COB dates "
                            "(default 7, transport_reconcile's)")
    scope.add_argument("--cob-date", action="append", metavar="YYYY-MM-DD",
                       help="--pending over these COB dates")
    scope.add_argument("--all", action="store_true", dest="full_sweep",
                       help="--pending over all of received/, v1 included")
    tr.add_argument("--list", action="store_true",
                    help="with --pending: print what is pending, ingest nothing")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from reporting_platform.ingest import steps

    if a.cmd == "land":
        results = steps.land(a.files)
        print(json.dumps(results, indent=2, default=str))
        return 0 if all(r["status"] in LANDED for r in results) else 1
    if a.cmd == "transport":
        return _transport(p, a)
    if a.cmd == "pending":
        print(json.dumps(steps.pending(a.feed), indent=2))
        return 0

    if a.all:
        from reporting_platform.common.context import feeds

        names = sorted(feeds())
    elif a.feeds:
        names = a.feeds
    else:
        p.error("name the feeds to ingest, or --all")
    results = {name: steps.ingest(name) for name in names}
    print(json.dumps(results, indent=2, default=str))
    failed = [r for rs in results.values() for r in rs if "error" in r]
    return 1 if failed else 0


def _transport(p, a) -> int:
    from reporting_platform.ingest import transport_steps

    if a.markers and a.pending:
        p.error("name markers, or --pending -- not both")
    if not a.markers and not a.pending:
        p.error("name the markers to ingest, or --pending")
    if not a.pending and (a.window or a.cob_date or a.full_sweep or a.list):
        p.error("--window/--cob-date/--all/--list go with --pending")

    unreadable: list = []
    markers = a.markers
    if a.pending:
        cob_dates = (None if a.full_sweep else a.cob_date
                     or transport_steps.window_cob_dates(a.window or 7))
        found = transport_steps.pending(cob_dates)
        markers, unreadable = found["marker_keys"], found["unreadable"]
        if a.list:
            print(json.dumps(found, indent=2))
            return 1 if unreadable else 0
    results = [transport_steps.ingest_transport(m) for m in markers]
    print(json.dumps({"results": results, "unreadable": unreadable},
                     indent=2, default=str))
    # An unreadable marker is not "nothing pending" -- it is something this
    # could not look at, and the person has to.
    return 1 if unreadable or any("error" in r for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
