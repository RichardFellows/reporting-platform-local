"""Migration CLI: cutover readiness and comparison evidence (Phase 8).

    python -m reporting_platform.migration overview
    python -m reporting_platform.migration status <feed>
    python -m reporting_platform.migration compare show <comparison-id>
    python -m reporting_platform.migration compare recent <feed> [--limit N]

Same shape as `reporting_platform.registry`'s CLI: no Spark here (everything
this reaches is psycopg2/boto3/json), every command prints JSON.
"""
from __future__ import annotations

import argparse
import json
import sys

from reporting_platform.common.context import feed as get_feed
from reporting_platform.common.context import feeds
from reporting_platform.migration import acceptance, evidence


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="reporting_platform.migration")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("overview", help="mode/streak/readiness for every feed")

    st = sub.add_parser("status", help="migration status for one feed")
    st.add_argument("feed")

    cmp_p = sub.add_parser("compare", help="comparison evidence")
    cmp_sub = cmp_p.add_subparsers(dest="compare_command", required=True)
    show = cmp_sub.add_parser("show", help="one comparison, by id")
    show.add_argument("comparison_id")
    recent = cmp_sub.add_parser("recent", help="recent comparisons for a feed")
    recent.add_argument("feed")
    recent.add_argument("--limit", type=int, default=20)

    a = p.parse_args(argv)

    if a.command == "overview":
        print(json.dumps(acceptance.overview(list(feeds().values())),
                         indent=2, default=str))
        return 0
    if a.command == "status":
        print(json.dumps(acceptance.evaluate_acceptance(get_feed(a.feed)),
                         indent=2, default=str))
        return 0
    if a.command == "compare":
        if a.compare_command == "show":
            row = evidence.get(a.comparison_id)
            if row is None:
                print(f"no comparison {a.comparison_id!r} recorded",
                     file=sys.stderr)
                return 2
            print(json.dumps(row, indent=2, default=str))
            return 0
        print(json.dumps(evidence.for_feed(a.feed, a.limit), indent=2,
                         default=str))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
