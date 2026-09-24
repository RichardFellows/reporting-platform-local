"""Land files and ingest them into raw, without an orchestrator.

    python -m reporting_platform.ingest land inbox/fo_trade_20260801.csv ...
    python -m reporting_platform.ingest pending fo_trade
    python -m reporting_platform.ingest ingest fo_trade [ref_rating ...]
    python -m reporting_platform.ingest ingest --all

`land` is the inbox gate over the files named (control files included):
what cannot be named is quarantined, nothing is triggered. `ingest` takes
every pending delivery of each feed into raw -- each on its own Nessie branch,
merged on success -- and cuts a `snapshot/` tag per ingest. Exits 1 if
anything was refused or failed. See `reporting_platform.ingest.steps`.
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
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from reporting_platform.ingest import steps

    if a.cmd == "land":
        results = steps.land(a.files)
        print(json.dumps(results, indent=2, default=str))
        return 0 if all(r["status"] in LANDED for r in results) else 1
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


if __name__ == "__main__":
    raise SystemExit(main())
