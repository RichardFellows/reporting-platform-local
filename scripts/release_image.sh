#!/usr/bin/env bash
# Build the release image and print the provenance a deployment of it carries.
#
#   scripts/release_image.sh <image-ref> [--push] > release.env
#
# Writes two lines, KEY=value, for the chart's values:
#
#   PLATFORM_CODE_REF   the image DIGEST, never the tag -- a tag can be
#                       re-pushed and the evidence changes under a run that
#                       has already been published. With --push this is the
#                       registry's repo digest; without, the local one.
#   DBT_PROJECT_DIGEST  `registry provenance` run INSIDE the built image, so
#                       it is computed by the platform's own code over the
#                       project actually shipped, not reimplemented here.
#
# See docs/OPENSHIFT-MAPPING.md and
# docs/DECISIONS.md#the-release-image-carries-the-code
set -euo pipefail

image=${1:?usage: scripts/release_image.sh <image-ref> [--push]}
push=${2:-}
cd "$(dirname "$0")/.."

nessie_server=$(grep -E '^NESSIE_SERVER_VERSION=' .env.example | cut -d= -f2)
docker build -f Dockerfile.airflow --target release \
  --build-arg "NESSIE_SERVER_VERSION=${NESSIE_SERVER_VERSION:-$nessie_server}" \
  -t "$image" . >&2

if [ "$push" = "--push" ]; then
  docker push "$image" >&2
fi

code_ref=$(docker image inspect "$image" \
  --format '{{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}{{.Id}}{{end}}')

project_digest=$(docker run --rm "$image" \
  python -m reporting_platform.registry provenance \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["dbt_project_digest"])')

echo "PLATFORM_CODE_REF=$code_ref"
echo "DBT_PROJECT_DIGEST=$project_digest"
