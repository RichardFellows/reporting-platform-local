#!/usr/bin/env bash
# Wraps CLAUDE.md's two-command build-branch recipe into one command, because
# a two-command incantation is exactly the kind of thing that gets skipped
# under time pressure -- and the shortcut is `dbt build` with no --vars,
# which lands straight on Nessie `main` (see the Makefile's *-on-main note).
#
# NEVER MERGES: publication is the Airflow builds' job (prepared_build /
# reporting_build in airflow/dags/dbt_builds.py), which merge only when the
# test task on the branch passes. This script is for a manual build you
# inspect yourself -- see CLAUDE.md's "Build on a throwaway branch, never
# main" for the merge step.
set -uo pipefail

if [ $# -eq 0 ]; then
  echo "usage: build_branch.sh <dbt select expr> [<dbt select expr> ...]" >&2
  exit 1
fi

echo "Opening a throwaway Nessie branch off main..." >&2
BRANCH=$(docker compose exec -T airflow python -m scripts._open_build_branch | tr -d '\r')

# _open_build_branch prints exactly one line on success and nothing on
# failure (it raises instead) -- an empty capture here means the exec itself
# failed (stack not up, container not built), not a dbt problem, so fail
# before ever calling dbt with an empty --vars nessie_ref.
if [ -z "$BRANCH" ]; then
  echo "ERROR: no branch name came back from _open_build_branch -- is the stack up (docker compose ps)?" >&2
  exit 1
fi

echo "Branch: $BRANCH" >&2
echo >&2

docker compose exec -T airflow dbt build \
  --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt \
  --target spark_local --select "$@" --vars "{nessie_ref: $BRANCH}"
STATUS=$?

# Nessie's v2 diff endpoint takes the ref as ONE path segment, so a branch
# name with slashes (every build branch has them, e.g. build/prepared/...)
# 404s unless they are percent-encoded -- verified against the live stack,
# not assumed from the API shape.
ENCODED_BRANCH=${BRANCH//\//%2F}

echo >&2
echo "Built on branch: $BRANCH (main is untouched -- this script never merges)" >&2
echo "See what it changed vs main:" >&2
echo "  curl -s http://localhost:19120/api/v2/trees/main/diff/$ENCODED_BRANCH" >&2
echo "Merge only if you mean to publish it -- see CLAUDE.md, \"Build on a" \
  "throwaway branch, never main\", for the merge step. This script does not" \
  "do it for you." >&2

exit $STATUS
