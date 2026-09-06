"""Registry CLI: reconcile, check coverage, look at what is recorded.

    python -m reporting_platform.registry reconcile [--feed fo_trade]
    python -m reporting_platform.registry coverage
    python -m reporting_platform.registry schema
    python -m reporting_platform.registry deliveries --business-date 2026-08-01
    python -m reporting_platform.registry rejections
    python -m reporting_platform.registry runs [--purpose reporting]
    python -m reporting_platform.registry versions [--report NAME]
    python -m reporting_platform.registry inputs --run-id RUN
    python -m reporting_platform.registry submissions
    python -m reporting_platform.registry submit --destination D --by WHO \
        --version report:2026-08-01:3 [--version ...] [--family NAME]

No Spark in this CLI: everything it reaches is boto3, json and psycopg2, so it
runs in the task process rather than through `scripts/_spark_task.py`. The one
Spark-using module in the package, `registry/inputs.py`, is deliberately not
wired in here -- it is invoked as `scripts._spark_task run-inputs <branch>`,
like every other Spark caller in this repo.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from reporting_platform.common.context import feed as get_feed
from reporting_platform.common.context import feeds
from reporting_platform.registry import db, deliveries, rejections, runs


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="reporting_platform.registry")
    sub = p.add_subparsers(dest="command", required=True)

    rec = sub.add_parser("reconcile", help="register everything not yet known")
    rec.add_argument("--feed")
    rec.add_argument("--no-normalize", action="store_true",
                     help="do not create missing manifests first")

    cov = sub.add_parser("coverage", help="manifests vs registered rows")
    cov.add_argument("--feed")

    sub.add_parser("schema", help="create the registry schema if absent")

    dl = sub.add_parser("deliveries", help="registered deliveries for a date")
    dl.add_argument("--business-date", required=True,
                    type=lambda s: date.fromisoformat(s))
    dl.add_argument("--feed")

    rj = sub.add_parser("rejections", help="most recent rejections")
    rj.add_argument("--feed")
    rj.add_argument("--limit", type=int, default=50)

    rn = sub.add_parser("runs", help="build runs, newest first")
    rn.add_argument("--purpose", choices=runs.PURPOSES)
    rn.add_argument("--limit", type=int, default=20)

    vs = sub.add_parser("versions", help="published report versions")
    vs.add_argument("--report")
    vs.add_argument("--limit", type=int, default=50)

    ip = sub.add_parser("inputs", help="the deliveries one run read")
    ip.add_argument("--run-id", required=True)

    sub.add_parser("submissions", help="recorded submissions")

    sb = sub.add_parser("submit", help="record that versions were submitted")
    sb.add_argument("--destination", required=True)
    sb.add_argument("--by", required=True, dest="submitted_by")
    sb.add_argument("--family",
                    help="a group of reports submitted together as one return")
    sb.add_argument("--note")
    sb.add_argument("--version", action="append", required=True,
                    dest="versions", metavar="REPORT:AS_AT_DATE:VERSION",
                    help="repeatable; the version must already be published")

    a = p.parse_args(argv)

    # The run-side commands are not per feed, so they are answered before the
    # feed list is resolved -- `chosen` below would otherwise load every feed
    # to answer a question about a report.
    if a.command == "runs":
        print(json.dumps(runs.recent(a.limit, a.purpose), indent=2, default=str))
        return 0
    if a.command == "versions":
        print(json.dumps(runs.versions(a.report, a.limit), indent=2, default=str))
        return 0
    if a.command == "inputs":
        print(json.dumps(runs.inputs_for_run(a.run_id), indent=2, default=str))
        return 0
    if a.command == "submissions":
        print(json.dumps(runs.submissions(), indent=2, default=str))
        return 0
    if a.command == "submit":
        items = []
        for spec in a.versions:
            try:
                report, as_at, version = spec.rsplit(":", 2)
                items.append((report, date.fromisoformat(as_at), int(version)))
            except ValueError:
                p.error(f"--version {spec!r} is not REPORT:YYYY-MM-DD:VERSION")
        print(json.dumps(runs.record_submission(
            a.destination, a.submitted_by, items, family=a.family,
            note=a.note), indent=2, default=str))
        return 0

    chosen = [get_feed(a.feed)] if getattr(a, "feed", None) else list(feeds().values())

    if a.command == "reconcile":
        out = [deliveries.reconcile(f, normalize_first=not a.no_normalize)
               for f in chosen]
    elif a.command == "coverage":
        out = [deliveries.coverage(f) for f in chosen]
    elif a.command == "schema":
        db.ensure_schema()
        out = {"schema": "ensured"}
    elif a.command == "deliveries":
        out = deliveries.deliveries_on(a.business_date, a.feed)
    else:
        out = rejections.recent(a.limit, a.feed)

    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
