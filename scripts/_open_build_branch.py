"""One-off: open a build branch from main for a manual dbt-build test.

Prints ONLY the branch name to stdout (nothing else), so callers can capture
it directly, e.g. in PowerShell:

    $branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()

This exists because running `dbt build` straight against `main` would
materialize models directly onto main before tests even run -- dbt runs
models, then tests, and a failing test does not undo an already-written
model. The real prepared_build/reporting_build Airflow DAGs (see
airflow/dags/dbt_builds.py) always build on a throwaway Nessie branch first
and only merge into main if tests pass; this script gives you that same
branch for a manual walkthrough of the same pattern.
"""
from __future__ import annotations

from reporting_platform.common.context import Nessie, new_run_id


def main() -> int:
    branch = f"build/prepared/manual-test/{new_run_id()}"
    Nessie().create_branch(branch, from_ref="main")
    print(branch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
