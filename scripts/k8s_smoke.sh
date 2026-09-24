#!/usr/bin/env bash
# The local-Kubernetes smoke test (`make k8s-smoke`): the whole platform in a
# kind cluster, from the chart, the way a cluster runs it -- and the first
# place PLATFORM_EXECUTION=kubernetes, the chart and the release image run
# together at all.
#
#   scripts/k8s_smoke.sh                  # create or reuse kind cluster rp-smoke
#   K8S_SMOKE_DOWN=1 scripts/k8s_smoke.sh # delete the cluster at the end
#
# What it proves, in order: the chart installs with --wait; the platform-init
# hook ran; every DAG imports from the baked image; bulk ingest runs Spark
# with executors as pods; the inbox gate lands the qa_ fixtures and TRIGGERS
# their ingest DAGs through Airflow's API; an `ingest_fo_trade` run launches
# its driver as a pod (`_spark_task.run`); `prepared_build` builds, audits and
# MERGES into Nessie `main`; and `registry runs` records the run as published.
#
# Waits on specific run ids, never `airflow dags test` (CLAUDE.md).
# Needs docker, kind, kubectl, helm. See docs/DECISIONS.md#the-local-k8s-smoke
set -euo pipefail
cd "$(dirname "$0")/.."

cluster=${K8S_SMOKE_CLUSTER:-rp-smoke}
ns=rp
release=rp
chart=deploy/helm/reporting-platform
ctx="kind-$cluster"
K=(kubectl --context "$ctx" -n "$ns")
stamp=$(date +%Y%m%dT%H%M%S)

step() { printf '\n=== %s  [%s]\n' "$1" "$(date +%H:%M:%S)"; }
ex() { "${K[@]}" exec deploy/rp-scheduler -c scheduler -- "$@"; }
af() { ex airflow "$@"; }

# Wait for one DAG run to finish; fail unless it succeeded.
wait_run() {
  local dag=$1 run=$2 state=""
  for _ in $(seq 1 180); do
    state=$(af dags list-runs -d "$dag" -o plain 2>/dev/null \
            | awk -v r="$run" '$2==r {print $3}')
    [[ "$state" == success || "$state" == failed ]] && break
    sleep 10
  done
  echo "$dag $run: ${state:-not found}"
  [[ "$state" == success ]]
}

cleanup() {
  code=$?
  if [ "$code" -ne 0 ]; then
    step "FAILED (exit $code)"
    "${K[@]}" get pods 2>&1 | tail -20 || true
  fi
  if [ "${K8S_SMOKE_DOWN:-0}" = "1" ]; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  exit "$code"
}
trap cleanup EXIT

step "cluster"
kind get clusters 2>/dev/null | grep -qx "$cluster" \
  || kind create cluster --name "$cluster" --wait 180s

step "images (release + spark + minio), loaded into kind"
nessie_server=$(grep -E '^NESSIE_SERVER_VERSION=' .env.example | cut -d= -f2)
docker build -q -f Dockerfile.airflow --target release \
  --build-arg "NESSIE_SERVER_VERSION=$nessie_server" \
  -t reporting-platform-airflow:k8s-smoke .
docker build -q -f Dockerfile.spark -t reporting-platform-spark:k8s-smoke .
# MinIO too: no registry serves its images anonymously any more.
# See docs/DECISIONS.md#minio-is-built-from-source
docker build -q -f Dockerfile.minio -t reporting-platform-minio:k8s-smoke .
kind load docker-image --name "$cluster" \
  reporting-platform-airflow:k8s-smoke reporting-platform-spark:k8s-smoke \
  reporting-platform-minio:k8s-smoke

step "helm install (--wait)"
helm dependency build "$chart" >/dev/null
reinstall=$(helm --kube-context "$ctx" -n "$ns" status "$release" >/dev/null 2>&1 && echo 1 || echo 0)
helm upgrade --install "$release" "$chart" --kube-context "$ctx" -n "$ns" \
  --create-namespace -f "$chart/values-local-k8s.yaml" --wait --timeout 15m
if [ "$reinstall" = 1 ]; then
  # Same tag, new image: nothing rolls on its own.
  "${K[@]}" rollout restart deploy/rp-scheduler deploy/rp-webserver statefulset/rp-triggerer
  "${K[@]}" rollout status deploy/rp-scheduler --timeout 300s
  "${K[@]}" wait --for=condition=Ready pod -l component=scheduler --timeout 300s
fi

step "platform-init: pool, registry schema, DAGs, settings"
af pools get lakehouse_write -o plain | tail -1
ex python -m reporting_platform.config check
imports=$(af dags list-import-errors -o plain 2>&1 | tail -1)
[[ "$imports" == "No data found" ]] || { echo "DAG import errors: $imports"; exit 1; }

step "seed (1 month, --clean): reference feeds by bulk ingest"
ex bash -euc '
  python /opt/platform/scripts/generate_feeds.py --months 1 --end 2026-08-19 \
    --out /tmp/seed --clean >/dev/null
  for f in ref_counterparty ref_collateral ref_rating; do
    python -m scripts.land_feeds --source /tmp/seed --feed "$f" | tail -1
  done
  python -m scripts.bulk_ingest 2>&1 | grep -E "^===|ingested"'

step "qa_ fixtures through the inbox gate, which triggers their DAGs"
pod=$("${K[@]}" get pods -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
ex mkdir -p /tmp/inbox
for f in tests/fixtures/happy_path/qa_*; do
  "${K[@]}" cp "$f" "$pod:/tmp/inbox/$(basename "$f")" -c scheduler
done
ex bash -c 'REPORTING_INBOX=/tmp/inbox timeout 30 python -m reporting_platform.ingest.inbox --loop 3 2>&1 \
            | grep -E "landed|conformed" || true'
for dag in ingest_qa_happy_position ingest_qa_headerless_position; do
  run=$(af dags list-runs -d "$dag" -o plain 2>/dev/null | awk 'NR>1 {print $2; exit}')
  [ -n "$run" ] || { echo "the gate triggered no run of $dag"; exit 1; }
  wait_run "$dag" "$run"
done

step "ingest_fo_trade: a DAG run, its driver a pod"
ex bash -c 'python -m scripts.land_feeds --source /tmp/seed --feed fo_trade | tail -1'
af dags unpause ingest_fo_trade -o plain >/dev/null
af dags trigger ingest_fo_trade -r "smoke-$stamp" -o plain >/dev/null
wait_run ingest_fo_trade "smoke-$stamp"

step "prepared_build: build on a branch, audit, merge"
af dags unpause prepared_build -o plain >/dev/null
af dags trigger prepared_build -r "smoke-$stamp" -o plain >/dev/null
wait_run prepared_build "smoke-$stamp"

step "published: Nessie main and the registry"
ex python -c "
import os, requests
head = requests.get(os.environ['NESSIE_URI'] + '/trees/main/history?max-records=1').json()
msg = head['logEntries'][0]['commitMeta']['message']
print('main head:', msg)
assert 'smoke-$stamp' in msg and msg.startswith('publish(prepared)'), msg
"
ex python -m reporting_platform.registry runs 2>/dev/null \
  | python3 -c "
import json, sys
runs = {r['run_id']: r for r in json.load(sys.stdin)}
r = runs.get('prepared-smoke-$stamp')
assert r, 'no registry run prepared-smoke-$stamp'
print('registry run:', r['run_id'], r['status'], r['code_ref'], r['code_ref_kind'])
assert r['status'] == 'published', r['status']
"
step "PASSED"
