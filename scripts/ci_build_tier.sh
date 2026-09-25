#!/usr/bin/env bash
# The BUILD TIER: the one check that builds the dbt project against a real
# catalog. `config.yml` (~10s) and `parse.yml` (~3 min) cannot: `dbt parse`
# accepts an unknown generic test and an unknown column key, and
# `lineage --columns` without a catalog reads 7 of 11 tables as not derivable
# and exits 0. This runs the pipeline end to end on a throwaway stack instead.
#
#   scripts/ci_build_tier.sh            # what .github/workflows/build.yml runs
#   CI_MONTHS=2 scripts/ci_build_tier.sh
#
# Runs as its own compose project with no host ports
# (.github/compose.ci.yml), so it can run beside a developer's stack. It
# leaves the project up for inspection; `CI_DOWN=1` removes it and its
# volumes at the end.
#
# THE SEQUENCE IS WRITE-AUDIT-PUBLISH, as the Airflow build does it: build on
# a throwaway Nessie branch, merge to `main` only if the build is clean, and
# only then run the lineage gate -- which reads schemas through DuckDB, and
# DuckDB can address only the catalog's default branch.
# See docs/DECISIONS.md#the-build-tier
set -euo pipefail
cd "$(dirname "$0")/.."

project=${CI_PROJECT:-rp-ci}
months=${CI_MONTHS:-3}
C=(docker compose -p "$project" -f docker-compose.yml -f .github/compose.ci.yml)

step() { printf '\n=== %s  [%s]\n' "$1" "$(date +%H:%M:%S)"; }

cleanup() {
  code=$?
  if [ "$code" -ne 0 ]; then
    step "FAILED (exit $code) -- last logs"
    "${C[@]}" logs --tail 40 spark-worker nessie 2>&1 | tail -60 || true
  fi
  if [ "${CI_DOWN:-0}" = "1" ]; then
    "${C[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$code"
}
trap cleanup EXIT

# A runner has no .env; the defaults in docker-compose.yml are the validated
# set. AIRFLOW_UID is the runner's own, so the bind mounts are writable.
[ -f .env ] || cp .env.example .env
export AIRFLOW_UID=${AIRFLOW_UID:-$(id -u)}

step "build images"
"${C[@]}" build airflow airflow-init spark-master spark-worker minio minio-init

step "stores and Spark"
"${C[@]}" up -d --wait minio postgres nessie spark-master spark-worker
"${C[@]}" up minio-init --exit-code-from minio-init

step "airflow-init (db, pool, registry schema, raw tables, dbt deps)"
"${C[@]}" run --rm airflow-init

# One container for everything after this: dbt's compiled SQL, which the
# lineage gate reads, lives in that container's DBT_TARGET_PATH.
step "seed ($months months, --clean) -> land -> ingest -> build -> merge -> lineage"
"${C[@]}" run --rm -e CI_MONTHS="$months" airflow bash -euo pipefail -c '
  t() { printf "\n--- %s  [%s]\n" "$1" "$(date +%H:%M:%S)"; }

  t "generate --clean"
  python /opt/platform/scripts/generate_feeds.py \
    --months "$CI_MONTHS" --end 2026-08-19 --out /tmp/seed --clean

  t "land"
  python -m scripts.land_feeds --source /tmp/seed

  # The two qa_ feeds are not generated: their fixtures are the onboarding
  # examples, and each carries a control file, which only the inbox gate
  # promotes (land_feeds lands data files only). --no-trigger because this
  # stack has no Airflow webserver; bulk_ingest below ingests them.
  t "inbox gate: the qa_ fixtures"
  mkdir -p /tmp/inbox
  cp /opt/platform/tests/fixtures/happy_path/qa_*.csv \
     /opt/platform/tests/fixtures/happy_path/qa_*.ctl /tmp/inbox/
  REPORTING_INBOX=/tmp/inbox python -m reporting_platform.ingest.inbox --no-trigger

  t "ingest"
  python -m scripts.bulk_ingest

  t "dbt build on a throwaway branch"
  branch=$(python -m scripts._open_build_branch)
  echo "branch: $branch"
  dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt \
    --target spark_local --select path:models/prepared path:models/reporting \
    --vars "{nessie_ref: $branch}"

  t "merge $branch -> main (the build was clean)"
  python -c "from reporting_platform.common.context import Nessie; Nessie().merge(\"$branch\", \"main\"); print(\"merged\")"

  t "lineage --columns --require-derivable"
  python -m reporting_platform.lineage --columns --require-derivable
'
step "PASSED"
