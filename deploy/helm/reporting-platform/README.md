# reporting-platform Helm chart

Deploys the reporting platform onto Kubernetes/OpenShift: Airflow (the
official chart, as a dependency) running the platform's release image with
the KubernetesExecutor, Spark drivers and executors as pods, and the
platform's own configuration, secrets, RBAC and init job.

`values.yaml` is the schema and the defaults, with a comment on every key.
Read it (and `docs/OPENSHIFT-MAPPING.md`, which links here rather than
restating it) before changing anything. **This chart is the ONLY place a
deployment's settings are written** -- one ConfigMap and one Secret, read by
every platform pod through `envFrom`, so no two pods can disagree about a
setting.

## Why KubernetesExecutor

dbt's driver stays in the Airflow task in every execution mode
(`docs/DECISIONS.md#execution-mode-is-configuration`: Cosmos stays
`ExecutionMode.LOCAL`, because its artifact archive and the publish gate read
`target/` off the task's own filesystem). That driver is only isolated from
other tasks -- and from the pool accounting `lakehouse_write` depends on
(`docs/DECISIONS.md#one-shared-write-pool`) -- if the Airflow *task* is its
own pod. LocalExecutor forks tasks from one scheduler process; only
KubernetesExecutor gives each task its own pod.

## Install per environment

```
# local-k8s (kind/k3d, make k8s-smoke): fully self-contained, nothing to --set
helm dependency update deploy/helm/reporting-platform
helm install t deploy/helm/reporting-platform -f deploy/helm/reporting-platform/values-local-k8s.yaml

# dev: the deploy pipeline supplies endpoints, image digests and provenance
helm install reporting-platform-dev deploy/helm/reporting-platform \
  -f deploy/helm/reporting-platform/values-dev.yaml \
  --set images.platform.repository=... --set images.platform.digest=sha256:... \
  --set images.spark.repository=...    --set images.spark.digest=sha256:... \
  --set endpoints.s3=... --set endpoints.nessie=... \
  --set endpoints.warehouse=s3a://.../warehouse --set endpoints.landing=s3a://.../landing \
  --set provenance.dbtProjectRef=... --set provenance.dbtProjectDigest=... \
  --set provenance.deploymentChangeRef=... --set provenance.deploymentPipelineRef=...

# uat / prod: as dev, with values-uat.yaml / values-prod.yaml. Both refuse a
# tag with no digest and refuse feedConsole.enabled -- see below.
```

## What the deploying pipeline must `--set` (or bake into a generated values file)

Every value `values.yaml` leaves empty is `required` at render time, so a
missing one fails `helm template`/`helm install` naming it rather than
failing later, quietly, in whichever pod reads it first:

- `images.platform.{repository,digest}`, `images.spark.{repository,digest}` --
  `make release-image` prints the platform digest and
  `provenance.dbtProjectDigest`.
- `endpoints.{s3,nessie,warehouse,landing}` -- where the shared stores are.
- `provenance.{dbtProjectRef,dbtProjectDigest,deploymentChangeRef,deploymentPipelineRef}`
  -- the deployment's change identity, recorded by every run
  (`docs/DECISIONS.md#a-change-is-a-deployment-event-not-a-run-event`).
  **Required in `uat` and `prod` only.** `dev` deliberately declares no
  project ref, because the console edits the project there, and empty is
  what a run then records.
- `secrets.existingSecret` (or `secrets.create: true` with `secrets.values.*`
  for a throwaway cluster) -- see "Secrets" below.
- `airflow.data.metadataSecretName` -- a Secret with a `connection` key, for
  Airflow's own metadata database.

## Secrets

The chart creates ONE Secret, `<release>-platform-secrets`, only when
`secrets.create` is true (a throwaway cluster). Above that,
`secrets.existingSecret` names a Secret created outside the chart -- Vault,
sealed-secrets, the platform team's own process -- with these keys:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `REGISTRY_DSN` -- the `platform` registry database connection string
- `DBT_ENV_SECRET_NESSIE_AUTH_TOKEN` -- only if `nessie.authType` is `BEARER`

**`airflow.extraEnvFrom` must name the SAME secret.** Helm evaluates that
value in the AIRFLOW SUBCHART's own template scope, which cannot see this
chart's `secrets.existingSecret` -- there is no template hook that lets a
parent chart hand a subchart a value computed from the parent's own values
(see `templates/_helpers.tpl`'s `reporting-platform.envSecret` comment for
the full explanation). Concretely: whenever a values file sets
`secrets.existingSecret`, it must ALSO override `airflow.extraEnvFrom` to
name that same secret literally -- `values-dev.yaml`, `values-uat.yaml` and
`values-prod.yaml` all do this; copy the pattern for a new environment file.
`tests/test_chart.py` checks that the base `values.yaml` file's two names
agree; a per-environment override is checked by `helm template` actually
resolving both to the same string.

## Environments

- `dev` -- the feed console and the inbox both run. Not controlled: a
  project digest mismatch is not enforced.
- `uat` / `prod` -- CONTROLLED. The chart refuses to render if
  `feedConsole.enabled` is true (`docs/OPENSHIFT-MAPPING.md`, "the feed
  console is not deployed above dev") or if a platform/Spark image has a
  `.tag` but no `.digest` (a tag can be re-pushed under a run already
  published -- `docs/DECISIONS.md#the-release-image-carries-the-code`).
- `values-local-k8s.yaml` -- a throwaway kind/k3d cluster (`make k8s-smoke`):
  MinIO, Nessie and Postgres as in-release Deployments, `secrets.create:
  true`, images loaded by tag. Never point this at a shared cluster.

## A new platform image rolls the inbox pod

Deploying a new platform image (a new `images.platform.digest`) must recreate
the inbox Deployment's pod, not just leave the old one running the new
ConfigMap/Secret values. `reporting_platform/ingest/inbox.py` is a
long-running watcher that imports `reporting_platform.ingest`/`common` ONCE at
process start and never re-imports them (CLAUDE.md: "editing a module a
long-running process already imported ... the inbox watcher holds its
imports"). `helm upgrade` with a new image digest already changes the pod
spec (a new `image:` value), which is what forces Kubernetes to roll the
Deployment -- so this is automatic as long as the digest actually changes
between releases, which is exactly why `images.platform.digest` (not `.tag`)
is what `PLATFORM_CODE_REF` and this chart both key on above dev.

## Rendering / linting locally

```
helm dependency update deploy/helm/reporting-platform
helm lint deploy/helm/reporting-platform -f deploy/helm/reporting-platform/values-local-k8s.yaml
helm template t deploy/helm/reporting-platform -f deploy/helm/reporting-platform/values-local-k8s.yaml
```

`charts/*.tgz` (the fetched airflow dependency) is gitignored; `Chart.lock` is
committed so the exact dependency version is pinned.
