"""Registry CLI: reconcile, check coverage, look at what is recorded.

    python -m reporting_platform.registry reconcile [--feed fo_trade]
    python -m reporting_platform.registry coverage
    python -m reporting_platform.registry schema
    python -m reporting_platform.registry deliveries --business-date 2026-08-01
    python -m reporting_platform.registry rejections

No Spark anywhere in here: the registry is boto3, json and psycopg2, so it
runs in the task process rather than through `scripts/_spark_task.py`.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from reporting_platform.common.context import feed as get_feed
from reporting_platform.common.context import feeds
from reporting_platform.registry import db, deliveries, rejections


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

    a = p.parse_args(argv)
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
