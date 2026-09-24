"""Transform raw -> prepared -> reporting, without an orchestrator.

    python -m reporting_platform.transform build prepared      # all three below
    python -m reporting_platform.transform build reporting --change-ref CHG-1

    # ...or one step at a time, which is what a build IS:
    branch=$(python -m reporting_platform.transform open prepared)
    python -m reporting_platform.transform dbt "$branch"
    python -m reporting_platform.transform publish "$branch"   # or: fail "$branch"

WRITE-AUDIT-PUBLISH, ALWAYS: `open` branches off main, `dbt` builds and tests
ON that branch, and only `publish` moves main -- and it refuses unless the
archived dbt results for the branch say every node passed. A failed build
keeps its branch for diagnosis. Same code as the Airflow `prepared_build` /
`reporting_build` DAGs (`transform.wap`, `transform.dbt`); what Airflow adds
is the trigger and one task per model.

Spark runs wherever PLATFORM_EXECUTION says: on the cluster (`local`), or in
this process with no cluster at all (`embedded`, SPARK_MASTER=local[N]).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from reporting_platform.transform import dbt, wap


def _label(given: str | None) -> str:
    from reporting_platform.common.context import new_run_id

    return given or f"cli-{new_run_id()}"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m reporting_platform.transform",
        description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    o = sub.add_parser("open", help="branch off main and open the run record; "
                                    "prints ONLY the branch name")
    o.add_argument("purpose", choices=wap.PURPOSES)
    o.add_argument("--label", help="names the branch (default: a fresh run id)")
    o.add_argument("--change-ref", help="the ticket authorising this build")

    d = sub.add_parser("dbt", help="dbt build + test ON the branch; never merges")
    d.add_argument("branch")
    d.add_argument("--select", help="default: the branch's layer")
    d.add_argument("--full-refresh", action="store_true")

    pb = sub.add_parser("publish", help="merge an audited branch into main, "
                                        "tag and version its reports")
    pb.add_argument("branch")
    pb.add_argument("--change-ref")

    f = sub.add_parser("fail", help="close the run as failed; keep the branch")
    f.add_argument("branch")
    f.add_argument("--reason", default="failed by hand")

    b = sub.add_parser("build", help="open + dbt + publish (or fail)")
    b.add_argument("purpose", choices=wap.PURPOSES)
    b.add_argument("--label")
    b.add_argument("--change-ref")
    b.add_argument("--select")
    b.add_argument("--full-refresh", action="store_true")
    b.add_argument("--no-publish", action="store_true",
                   help="build and test on the branch, leave main alone")
    a = p.parse_args(argv)

    # stderr, so `open`'s stdout stays capturable as just the branch name.
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    dbt.target()        # refuse a non-Spark target before touching anything

    if a.cmd == "open":
        print(wap.open_build(a.purpose, _label(a.label), change_ref=a.change_ref))
        return 0
    if a.cmd == "dbt":
        result = dbt.build(a.branch, select=a.select, full_refresh=a.full_refresh)
        print(json.dumps(result))
        return 0 if result["ok"] else 1
    if a.cmd == "publish":
        attempt = dbt.verified_attempt(a.branch)
        print(json.dumps(wap.publish(a.branch, dbt_attempts=[attempt],
                                     change_ref=a.change_ref), default=str))
        return 0
    if a.cmd == "fail":
        print(wap.fail_build(a.branch, a.reason))
        return 0

    result = dbt.build_layer(
        a.purpose, _label(a.label), change_ref=a.change_ref, select=a.select,
        full_refresh=a.full_refresh, publish=not a.no_publish)
    print(json.dumps(result, default=str))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
