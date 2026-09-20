# Deploying to OpenShift with Helm

The plan for turning the local compose stack into a Helm-installed OpenShift
deployment, on the two constraints this deployment is given:

- **No MinIO.** A managed, S3-compatible service supplies the object store.
- **No credentials in the chart.** A Vault service injects the S3 credentials
  into environment variables; the chart never holds one.

Read `docs/OPENSHIFT-MAPPING.md` first — it is the contract that says promotion
changes configuration and not code, and this document is the work needed to
make that true. Where the two disagree, the mapping is the older document and
this one is the correction.

**Nothing here has run.** The `spark_ocp` dbt target has never been exercised by
a cluster, and neither has anything else below. Every section that asserts a
behaviour rather than describing one says so at the point it does it. The one
habit that matters applies to this document more than any other in the repo:
get the actual error text.

---

## 1. What is actually being changed

Of the fourteen compose services, the deployment is not a fourteen-way
translation. Sorted by what happens to each:

| Compose service | In OpenShift | Why |
|---|---|---|
| `minio`, `minio-init` | **deleted** | managed S3. Nothing replaces `minio-init`: the bucket is provisioned by the storage team, and `#no-folder-markers` means no prefix needs creating — a prefix exists when an object under it does |
| `postgres` | **deleted** | managed RDBMS, three logical databases (§5) |
| `nessie` | Deployment + Service (+ Route) | unchanged image, JDBC store points at the managed RDBMS |
| `spark-master`, `spark-worker` | Deployment + Service, **phase 1 only** | see §6 — kept standalone deliberately, then retired |
| `airflow-init` | Helm hook `Job` | §4 |
| `airflow` (scheduler) | Deployment | executor changes, §4 |
| `airflow-webserver` | Deployment + Service + Route | §4 |
| `airflow-triggerer` | Deployment | `transport_watch`'s deferrable sensor lives here |
| `watchdog` | Deployment + small PVC | §7 |
| `feed-ui` | Deployment, **dev only, refused above dev by the chart** | §8 |
| `notebook` | Deployment, **dev only** | §8 |
| `inbox` | **not deployed** | a polling filesystem watcher has no cluster role; arrival is the S3 push agent plus `transport_watch` (`OPENSHIFT-MAPPING.md` §1) |
| `marquez-api`, `marquez-web` | Deployment + Service (+ Route), off by default | §9 |

Four things are genuinely new work rather than translation, and they are the
sections worth arguing about before anything is written: **the S3 client
assumptions (§3)**, **credential rotation against processes that read their
environment once (§3.3)**, **Spark's execution model (§6)** and **the base
image question (§10.1)**.

---

## 2. Chart shape

```
deploy/helm/reporting-platform/
  Chart.yaml
  values.yaml                  defaults: everything off that is not prod-safe
  values-dev.yaml
  values-uat.yaml
  values-prod.yaml
  templates/
    _helpers.tpl               the platform env block  <- the load-bearing part
    configmap-platform.yaml    every non-secret variable, one object
    secret-refs.yaml           ExternalSecret / SecretProviderClass (§3.3)
    nessie/{deployment,service,route}.yaml
    airflow/{scheduler,webserver,triggerer}-deployment.yaml
    airflow/{webserver-service,route}.yaml
    airflow/pod-template.yaml  KubernetesExecutor worker template
    airflow/rbac.yaml          SA + Role + RoleBinding (pod create/delete)
    jobs/db-migrate.yaml       helm.sh/hook: pre-install,pre-upgrade
    jobs/platform-bootstrap.yaml
    spark/{master,worker}.yaml gated on .Values.spark.mode == "standalone"
    watchdog/{deployment,pvc}.yaml
    dev/{feed-ui,notebook}.yaml
    lineage/{marquez-api,marquez-web}.yaml
    networkpolicy.yaml
    tests/smoke.yaml           helm.sh/hook: test
```

**One chart, not an umbrella of per-service charts,** and the reason is
`docker-compose.yml`'s `x-s3-env` anchor. Fourteen services share one
environment block and the file's own comments record what happened the two
times a service redefined `environment:` and silently inherited nothing from
it. A subchart per service reintroduces exactly that: each one owning its own
copy of `NESSIE_URI`, `REPORTING_WAREHOUSE` and `S3_ENDPOINT`, free to drift.

The anchor's Helm equivalent is a named template in `_helpers.tpl` plus a
single ConfigMap, consumed with **`envFrom`, never a per-container `env:`
list**:

```yaml
envFrom:
  - configMapRef: {name: {{ include "rp.fullname" . }}-platform}
  - secretRef:    {name: {{ .Values.secrets.s3SecretName }}}
```

`envFrom` merges the whole object; a per-container `env:` list is the shallow
merge that bit compose. Per-container `env:` is then reserved for the handful
of values that genuinely differ per workload (`FEED_UI_PORT`, the downward-API
`POD_IP` in §6), which makes the exceptions visible.

---

## 3. Object storage: the managed S3 service

### 3.1 What the chart supplies

| Variable | Value | Notes |
|---|---|---|
| `S3_ENDPOINT` | `https://<managed endpoint>` | **https**, not http — every local default is `http://minio:9000` |
| `AWS_REGION` | the service's region | already threaded everywhere |
| `REPORTING_WAREHOUSE` | `s3a://<bucket>/warehouse` | bucket name is derived from this string by `_bucket()`; it is the single place the bucket is written down |
| `REPORTING_LANDING` | `s3a://<bucket>/landing` | |
| `REPORTING_RECEIVED_PREFIX` | `received` | DCM transport evidence |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | **from Vault, never from values** | §3.3 |

The chart creates no bucket and no prefix. If the managed service is AWS S3
proper rather than an on-prem S3-compatible store, `S3_ENDPOINT` should be
**unset** rather than set to an AWS hostname, so botocore and the AWS SDK do
their own endpoint resolution — which means `_client()` and its three siblings
must stop defaulting to `http://minio:9000` when the variable is absent. See
below; this is the same change.

### 3.2 The MinIO assumptions are hardcoded, and they are wrong for managed S3

This is the largest code change the deployment needs, and it is small. Four
call sites hardcode two MinIO facts — path-style addressing and TLS off:

| File | What is hardcoded |
|---|---|
| `reporting_platform/common/spark.py` | `s3.path-style-access=true`, `fs.s3a.path.style.access=true`, `fs.s3a.connection.ssl.enabled=false` |
| `dbt/profiles.yml` (`spark_local`) | the same three |
| `reporting_platform/retention/retention.py` (`_gc_fileio`) | `s3.path-style-access=true`, passed as `-I` flags to `nessie-gc` |
| `scripts/duckdb_console.py` | `URL_STYLE 'path', USE_SSL false` |

and one more outside Python, in the Nessie server's own configuration:
`nessie.catalog.service.s3.default-options.path-style-access: "true"`.

Plus five `os.environ.get("S3_ENDPOINT", "http://minio:9000")` defaults
(`arrival.py`, `retention/landing.py`, `retention/orphan_storage.py`,
`registry/artifacts.py`, `common/spark.py`) and `ui/feeddata.py`, which passes
`endpoint_url=os.environ.get("S3_ENDPOINT")` with no default and is therefore
already correct.

**The change:** one helper that answers both questions from the environment,
used by all of them.

- `S3_PATH_STYLE` — `true` locally (MinIO cannot do virtual-host addressing
  without wildcard DNS), whatever the managed service requires in the cluster.
  Most on-prem S3-compatible stores want path-style; AWS S3 wants virtual-host.
  **This must be established against the actual service, not assumed.**
- TLS follows the endpoint scheme rather than being a second setting: an
  `https://` endpoint means `fs.s3a.connection.ssl.enabled=true`. A separate
  flag is a way for the two to disagree.

The failure if this is skipped is worth naming, because it is not obvious:
`path-style-access=true` against AWS S3 works for a while (AWS still serves
path-style on many paths) and then does not, and a wrong
`connection.ssl.enabled=false` against an HTTPS endpoint fails at the S3A layer
only — Iceberg's `S3FileIO` uses the AWS SDK and honours the scheme, so
**Iceberg writes succeed and the landing-CSV read fails**, which looks like a
feed problem rather than a configuration one.

`tests/test_value_checks.py` is the right home for a check that the resolved
combination is coherent (an `http://` endpoint with TLS demanded, or the
reverse), on the same principle as every other value in that file: the console
may not be the only thing that validates a value.

### 3.3 Vault, and the fact that these processes read their environment once

`reporting_platform/common/settings.py` says it in its docstring, and
`CLAUDE.md` states it as a rule: **environment is read at process start**, which
is why editing `.env` needs a container recreated rather than restarted. Vault
injecting credentials into environment variables inherits that property
exactly.

So: **a credential rotation is a pod rollout, not a reload.** For each
long-lived workload — scheduler, webserver, triggerer, watchdog, nessie,
feed-ui — a rotated credential that does not trigger a rollout produces a pod
holding a dead key. The symptom is `403 SignatureDoesNotMatch` on the next S3
call, which surfaces as an ingest failure, a retention sweep refusing, or — the
bad one — `orphan_storage` reading an unreadable bucket. That last case is
already guarded: an unreadable reference refuses the sweep and an empty live
set against a non-empty warehouse refuses too
(`#an-incomplete-keep-set-refuses`). The guard exists; this is the first
deployment where the thing it guards against is routine.

**Recommended shape.** Vault → a Kubernetes `Secret`, via External Secrets
Operator or the Vault CSI provider with `secretObjects`, and the chart consumes
that Secret with `envFrom.secretRef`. Then:

1. Every Deployment carries
   `checksum/s3-secret: {{ .Values.secrets.s3Revision | quote }}` or, better,
   is watched by a reloader that restarts on Secret change. A rotation becomes
   a rolling restart, automatically.
2. **Rotation must not land inside a build.** A rolling restart of the
   scheduler during the nightly chain kills in-flight tasks. Airflow retries
   them, and the write-audit-publish shape means a killed build leaves a
   branch behind and `main` untouched — so the blast radius is bounded — but
   the rotation window should still be set outside the nightly chain, and that
   is a scheduling agreement with whoever owns Vault, not a chart setting.
3. **KubernetesExecutor worker pods get the credential fresh at pod start**,
   because they are created per task. That is the one place rotation is free,
   and it is an argument for the executor change in §4 beyond elasticity.

If Vault's dynamic S3 credentials have a TTL shorter than a pod's lifetime —
hours, not days — environment-variable injection is the wrong mechanism and the
alternative is a file-based credential source plus a credential provider that
re-reads it (`spark.hadoop.fs.s3a.aws.credentials.provider` and a
boto3 `RefreshableCredentials` shim). **That is a materially larger change**
and the plan assumes it is not needed. It is the first question in §12.

### 3.4 Do not let the credential become part of a connection string

`docker-compose.yml` sets `AIRFLOW_CONN_AWS_DEFAULT` to a JSON document with
the access key and secret inline, because locally there is no secret worth
managing. In the cluster this would put the credential into a second place,
inside a string the chart templates.

**Set it with no `login`/`password`:**

```
AIRFLOW_CONN_AWS_DEFAULT={"conn_type":"aws","extra":{"endpoint_url":"...","region_name":"..."}}
```

botocore then falls through to the environment variables Vault already
supplies, which is the same credential by the same path as every other S3
client in the platform. The connection carries configuration only, and it can
live in the ConfigMap rather than the Secret.

Two related settings:

- `AIRFLOW__WEBSERVER__EXPOSE_CONFIG` is `true` locally and must be **`false`**
  in uat and prod. It renders the running configuration, and every secret this
  deployment has travels through an `AIRFLOW__*` variable.
- `AIRFLOW__CORE__FERNET_KEY` is **not set anywhere in the compose stack**, and
  it must be in the cluster: it encrypts connections and variables in the
  metadata DB. It must come from Vault and be **stable across releases** — a
  regenerated Fernet key makes every stored connection unreadable, with an
  error that talks about decryption and not about deployment.
- `AIRFLOW__WEBSERVER__SECRET_KEY` is `local-dev-not-a-real-secret` in compose.
  From Vault, and stable and identical across webserver replicas, or log
  fetching fails intermittently between them.

One footnote that is specific to this repo: `#openlineage-masker` records that
Airflow's `SecretsMasker` corrupted a lineage facet because this estate's
Postgres password is the word `platform`, so `platform_column` was rewritten to
`***_column`. The masker redacts **any string containing a configured secret's
value**. In the cluster, choose high-entropy credentials — not for strength, for
the fact that a short or dictionary-word secret silently corrupts unrelated
output. The test that pins this stays useful.

---

## 4. Airflow

### 4.1 Executor

`LocalExecutor` → **`KubernetesExecutor`**. The scheduler stops being the thing
that runs tasks, and each task gets a pod from `templates/airflow/pod-template.yaml`
built from the same platform image, the same `envFrom`, and the service account
in §6.2.

What survives the change unaltered, and should be checked rather than assumed:

- **The `lakehouse_write` pool, at one slot.** Pools live in the metadata
  database and are executor-independent. The mapping doc is right that the
  constraint in the cluster is writer exclusion rather than core starvation —
  keep it at 1 until there is a measured reason, and revisit alongside §6.
- **`max_active_runs=1`** on the ingest DAGs, and with it the rule that a run
  left non-terminal starves every other run.
- **`scripts/_spark_task.py`'s subprocess.** Still required. The failure it
  prevents is that a task callable which builds a SparkSession in-process never
  exits, because the JVM's non-daemon threads keep it alive; heartbeats stop and
  the scheduler zombie-reaps it ~300s later, after the work has succeeded. Under
  KubernetesExecutor the task process is the pod's process and the symptom is
  identical — a pod that will not exit. `OPENSHIFT-MAPPING.md` already says the
  driver lives in that process so this holds on a cluster too.
- **Cosmos's four load-bearing settings.** `LoadMode.DBT_LS` renders the same
  graph wherever it runs, so the task graph in OpenShift is the laptop's graph.
  `InvocationMode.SUBPROCESS` stays while `ExecutionMode.LOCAL` does; if and
  when `ExecutionMode.KUBERNETES` is adopted, the pod boundary supplies the
  same isolation and the setting stops applying — but that is a later change
  and not part of this one.

**Task logs must go to S3.** Worker pods are ephemeral, so the `airflow-logs`
volume has no analogue: `AIRFLOW__LOGGING__REMOTE_LOGGING=true`,
`REMOTE_BASE_LOG_FOLDER=s3://<bucket>/airflow-logs`,
`REMOTE_LOG_CONN_ID=aws_default` — the connection from §3.4. Without it, a
failed task's log is gone the moment its pod is reaped, which is precisely when
it is wanted. Note the interaction with the Arrivals page: `no run recorded`
already means Airflow trimmed its history, never "not ingested"
(`#the-arrivals-view-is-a-join-not-a-record`), and remote logging does not
change that — the metadata DB's retention does.

### 4.2 `airflow-init` becomes two Helm hook Jobs

`airflow-init` does five things and CLAUDE.md records that each one broke the
platform silently when skipped. They do not all belong in one hook:

**Job A — `db-migrate`,** `helm.sh/hook: pre-install,pre-upgrade`,
`hook-weight: -10`, `hook-delete-policy: before-hook-creation`:

- `airflow db migrate`

It must complete before any scheduler or webserver pod starts, which is what
the hook weight and `--wait` buy. It must **not** run concurrently with a
running scheduler, so the upgrade path is: hook runs, then the Deployments roll.

**Job B — `platform-bootstrap`,** `post-install,post-upgrade`, after the
metadata DB exists:

- `airflow users create` — **dev only.** Above dev the webserver authenticates
  against the estate's OIDC/LDAP and a local admin is a standing credential
  nobody rotates. Gate on `.Values.airflow.createAdminUser`.
- `airflow pools set lakehouse_write 1 ...`
- `python -m reporting_platform.registry schema` — idempotent, and it also runs
  on first connect in `registry/db.py`, which is not redundant: the workloads
  that share the image do not share this Job's ordering.

**`dbt deps` does not run at deploy time.** It reaches the internet, and there
is no egress. Vendor `dbt_packages` **into the image at build time**, where the
internal mirror is configured, set `DBT_PACKAGES_INSTALL_PATH` at the baked
path, and drop the `dbt-packages` volume. This is load-bearing and its failure
is not subtle: without packages, `dbt ls` cannot compile a `dbt_utils` test and
**both build DAGs fail to import**. `Dockerfile.airflow` should assert it the
way it already asserts `dbt --version` after the `--no-deps` cosmos install —
run `dbt deps` then `dbt ls` in the build, so a broken package set fails the
image rather than the release.

### 4.3 Everything the DAGs read is baked into the image

`REPORTING_CONFIG_DIR` defaults to the package directory, so `feeds/*.yml`,
`retention.yml` and `maintenance.yml` ship inside the image, as does the dbt
project and `airflow/dags`. **Keep it that way. Do not mount the feed registry
from a ConfigMap.**

The reason is the one `OPENSHIFT-MAPPING.md` gives for not deploying the feed
console above dev: a feed definition that can change without an image change is
drift, and `check_project_drift()` covers the **dbt project only** — there is no
equivalent digest over the feed registry. A feed change reaching uat as a
`helm upgrade --set` is a change to what gets ingested with no commit behind it.
Adding a feed is a git change, an image build, and a release.

One consequence worth stating so nobody reports it as a bug: `feeds()` caches on
mtime so a new feed reaches the DAG processor, and with a read-only baked
config that cache never invalidates. That is correct here and inert, not broken.

### 4.4 Probes

| Workload | Readiness | Liveness |
|---|---|---|
| webserver | `GET /health` | `GET /health` |
| scheduler | — | `airflow jobs check --job-type SchedulerJob --hostname $(hostname)` |
| triggerer | — | `airflow jobs check --job-type TriggererJob --hostname $(hostname)` |
| nessie | `GET /api/v2/config` (the compose healthcheck) | same |
| feed-ui | `GET /` | — |

---

## 5. Postgres: three logical databases on a managed RDBMS

`scripts/init-postgres.sql` creates what the compose Postgres needs. In the
cluster the chart has no `CREATE DATABASE` rights and should not ask for them.
The provisioning request is:

| Database | Owner | Used by | Notes |
|---|---|---|---|
| `airflow` | platform role | Airflow metadata | migrated by Job A |
| `platform` | platform role | `registry.*` | schema created by Job B, and by `registry/db.py` on first connect |
| `nessie` | platform role | Nessie version store | Nessie migrates its own store on start |
| `marquez` | marquez role | Marquez, if §9 is enabled | upstream's `marquez.dev.yml` **hardcodes** database, user and password to `marquez`; that is why it is a separate role locally and it stays one |

`REGISTRY_DSN` and `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` both carry a password,
so both are Secret values, not ConfigMap values. `registry/db.py` refuses to run
without `REGISTRY_DSN` rather than defaulting, which is the behaviour that makes
a misconfigured Secret fail loudly at start instead of at the first delivery.

---

## 6. Spark

### 6.1 Phase 1 keeps standalone, deliberately

The instinct is to go straight to `k8s://` with dynamic executor pods, and the
end state is that. The plan does not start there, for one reason: **no cluster
has ever run any of this**, and a first deployment that changes the object
store, the credential source, the secret mechanism, the executor, the log
destination, the base platform *and* the Spark execution model has no bisection
available when it fails.

So phase 1 deploys `spark-master` and `spark-worker` as ordinary Deployments
with the configuration this project has actually proven, and changes only the
endpoint and the credentials. `SPARK_MASTER` stays `spark://spark-master:7077`
— now a Service DNS name instead of a compose one — and `spark.master` in
`dbt/profiles.yml` stays equal to it, which is the invariant
`#spark-master-single-source` exists to protect and the one thing that must not
be quietly broken by the chart templating one of them and not the other.

**Exit criterion for phase 1:** a full ingest → prepared → reporting chain
builds on a Nessie branch, passes its tests, merges to `main` and cuts a
`published/` tag, against the managed S3 service. That is the point at which
everything except Spark's topology is known good.

### 6.2 Phase 2 swaps to Kubernetes-native executors

`spark_session()` refuses a `local` master and accepts anything else, so
`k8s://https://kubernetes.default.svc` passes the guard as written — checked,
not assumed. What phase 2 then needs:

- **Client mode**, because the driver is the task process
  (`scripts/_spark_task.py`'s child) inside the KubernetesExecutor worker pod.
  Executors are pods that driver creates. That means `spark.driver.host` from
  the downward API (`POD_IP`), `spark.driver.bindAddress=0.0.0.0`, and fixed
  `spark.driver.port` / `spark.driver.blockManager.port` so a NetworkPolicy can
  name them. `spark.py`'s current comment — that the driver host is left at its
  default because Docker's embedded DNS resolves the container hostname — stops
  being true, and that comment must change with the code.
- **RBAC**: a service account with `pods` create/get/list/watch/delete,
  `services` and `configmaps` in the namespace, bound to the worker pod
  template. `spark.kubernetes.authenticate.driver.serviceAccountName` names it.
- **`spark.kubernetes.container.image`** = the Spark image, by digest.
- **`spark.jars.packages` must go.** Today the driver resolves five Maven
  coordinates via Ivy at session start, because the pip `pyspark` in the Airflow
  image has none of the jars. In the cluster there is no egress to Maven Central
  and `#spark-jars-prebaked` is already the standing rule. The fix is to **bake
  the same jar set into `Dockerfile.airflow`** using the same build args, and
  make `spark.jars.packages` conditional on a non-empty `SPARK_JARS_PACKAGES`.

  **The chart must not name a jar version.** `tests/test_versions.py` pins
  `ICEBERG_VERSION` across five declaration sites and the whole point of that
  test is that a bump touches one of them. A `values.yaml` entry would be a
  sixth site, in a file that test cannot see meaningfully, and the failure it
  guards against — `NoSuchMethodError` on the first write, naming no version —
  is exactly the one nobody diagnoses quickly. The chart pins the **image
  digest** and the versions stay a property of the image. Adding the jars to
  `Dockerfile.airflow` does add one declaration site, and `test_versions.py`
  must gain it in the same change.

- **`conf/spark-defaults.conf`** becomes a ConfigMap rendered from the same
  values as the platform ConfigMap, so the endpoint and warehouse cannot
  disagree between the file and `spark_session()`. In client mode the file
  mostly does not decide anything — `spark_session()` sets every key explicitly
  — but leaving a stale file that reads as working is the failure mode this repo
  keeps rejecting.

- **Sizing** starts from `OPENSHIFT-MAPPING.md`'s table, which is explicitly
  unmeasured, and is replaced with observed figures after the first week.
  `spark.cores.max=2` exists to stop one app taking a standalone cluster's every
  free core; on Kubernetes that constraint does not exist and the cap should be
  re-derived rather than carried over.

`ExecutionMode.KUBERNETES` for Cosmos is a **third** phase and a separate
decision. Phase 2 already gives each dbt model task its own pod through the
executor; one pod per model through Cosmos as well is a different tradeoff and
should be priced when there is a measured reason.

---

## 7. Watchdog

Deploys as a single-replica Deployment. Its one complication is that it keeps
history in a file: `REPORTING_WATCHDOG_HISTORY`, default
`/var/lib/reporting-watchdog/history.jsonl`, a bind mount locally.

Two options, and the second is better:

1. A small RWO PVC. Works, single replica only, pins the pod to a zone.
2. Point `REPORTING_WATCHDOG_HISTORY` at the registry or at S3 and delete the
   volume.

Phase 1 takes option 1 because it changes no code. Option 2 is the right end
state and should be a tracked item, not a forgotten one — a PVC for a
append-only JSONL file is a stateful dependency bought for nothing.

The watchdog is deliberately not an Airflow service and is in no `depends_on`
with one (`#watchdog-independent`); in Helm that means it has no init
container waiting on Airflow and no readiness gate tied to it. Preserve that.

---

## 8. The two dev-only services, and refusing them above dev

`feed-ui` and `notebook` deploy in dev and **must not deploy in uat or prod**:

- The feed console writes `_sources.yml` and a scaffolded model into the dbt
  project, which is exactly the drift `check_project_drift()` exists to catch.
  Its changes reach uat and prod through git and the pipeline like anything
  else.
- The marimo notebook runs `--headless --no-token`. That is an unauthenticated
  arbitrary-code-execution endpoint with the platform's S3 credentials in its
  environment. It is fine behind a dev cluster's access controls and it must
  never have a Route in uat or prod.

**Enforce it in the template, not in the values file.** A values file can be
overridden on the command line; a `fail` cannot:

```
{{- if and .Values.feedUi.enabled (has .Values.reportingEnv (list "uat" "prod")) }}
{{- fail "feed-ui is not deployed above dev: it writes into the dbt project. See docs/OPENSHIFT-MAPPING.md" }}
{{- end }}
```

This is the same habit as the rest of the platform: the console may not be the
only thing that validates a value, `dbt_builds.py` refuses a non-Spark target at
parse time, `spark_session()` refuses a local master. A rule that only lives in
a document is a rule that is one `--set` away from not existing.

Note also that if `feed-ui` is not deployed, `AIRFLOW__API__AUTH_BACKENDS` no
longer needs `basic_auth` above dev — it is there so the console's API calls do
not 401. Whether the push agent (§1, arrival) triggers DAGs through the same API
decides this; if it does, basic_auth stays and gets its own service account
credential from Vault rather than reusing `admin`.

---

## 9. Lineage

`marquez-api` and `marquez-web` sit behind `.Values.lineage.enabled`, default
**false**, matching `OPENLINEAGE_DISABLED=true`.

**One values key drives both halves**, because that is the interlock
`.env.example` documents: the OpenLineage provider does not fail a task when its
transport refuses a connection, it logs and moves on, so Airflow emitting to a
Marquez that is not deployed produces several hundred failed POSTs a night —
standing noise that teaches people to stop reading task logs. Enabling the
export without the consumer must not be expressible.

Two mechanical points:

- `AIRFLOW__OPENLINEAGE__*` is read at process start, so flipping the flag must
  roll the three Airflow Deployments. A `checksum/config` annotation over the
  platform ConfigMap does this automatically and is the direct Helm analogue of
  "recreate, not restart".
- `AIRFLOW__OPENLINEAGE__EXTRACTORS` (**not** `__CUSTOM_EXTRACTORS`, which is
  read by nothing and warns about nothing) must carry
  `reporting_platform.lineage.extractor.PlatformLineageExtractor`. Without it
  Airflow emits jobs and no datasets at all. It belongs in the ConfigMap as a
  fixed value, not a tunable.
- Both Marquez images are built here on UBI from Marquez's own source, so
  `MARQUEZ_VERSION` is a build input and the chart references the built image by
  digest like every other.

`SEARCH_ENABLED=false` stays: upstream's config defaults it true against an
OpenSearch that is not deployed.

---

## 10. OpenShift specifics

### 10.1 The base image question — resolve this before building anything

`#marquez-on-ubi` says both Marquez images are built here, from source, onto UBI
bases, because "upstream ships Ubuntu and Alpine, and nothing in this estate may
be."

**The platform's own two images do not meet that standard.**
`Dockerfile.airflow` is `FROM apache/airflow:2.10.5-python3.11` and
`Dockerfile.spark` is `FROM apache/spark:3.5.3-python3`; both are Debian.

Either the policy applies only to images this estate builds from source (in
which case Marquez was rebased because it was being built anyway, and the two
platform images are fine as vendor images), or it applies to everything, and
**rebasing Airflow 2.10.5 and Spark 3.5.3 onto UBI is a significant piece of
work** that must be in the plan rather than discovered during a security review.

This is a question for whoever owns the estate's image policy, and it is first
in §12 alongside the Vault TTL question because both can invalidate a sprint.

### 10.2 Arbitrary UID / `restricted-v2`

OpenShift assigns a random UID with GID 0. Consequences:

- **`Dockerfile.airflow` is already correct** and for a different reason.
  Its last layer chowns `/opt/platform/run` to `airflow:0` with `g+rwX`, and
  the image's own writable paths (`/home/airflow`, `/opt/airflow/logs`) follow
  the same arrangement, which the file's comment correctly identifies as
  "precisely the arrangement that lets an ARBITRARY uid run this image". That
  was done for host bind mounts on a laptop; it happens to be the SCC answer.
  **Do not set `runAsUser` and do not set `AIRFLOW_UID`** — let OpenShift assign.
- **`Dockerfile.spark` is not.** It ends `USER spark` (uid 185) and nothing
  makes `/opt/spark` group-writable. Under an assigned UID, anything Spark
  writes there fails: the Ivy cache at `/opt/spark/.ivy2` (a named volume in
  compose), `$SPARK_HOME/work`, and the standalone daemon logs under
  `$SPARK_HOME/logs`. It needs the equivalent final layer, and the writable
  paths should be redirected at an `emptyDir` rather than the image.
- **Verify the entrypoint can name the assigned UID.** Spark's own launch
  scripts call `whoami`, which fails when the UID has no `/etc/passwd` entry —
  "cannot find name for user ID". Upstream's Kubernetes entrypoint appends the
  UID to `/etc/passwd` when that file is group-writable; whether this image's
  does is a thing to test, not to assume, and the fallback is
  `SPARK_IDENT_STRING` set explicitly or an nss_wrapper shim.
- `/opt/platform/run` gets an `emptyDir` (dbt's log and target paths). It must
  not be a PVC: two pods writing one dbt target directory is the problem
  `#dbt-working-directories` already solved once.

### 10.3 Routes and network

- Routes with edge TLS for the Airflow webserver, plus feed-ui / notebook /
  marquez-web in dev only. `AIRFLOW__WEBSERVER__BASE_URL` must match the Route
  hostname or generated links point at localhost.
- The feed console renders host-side URLs for the browser
  (`MINIO_CONSOLE_URL`, `AIRFLOW_UI_URL`, `SPARK_UI_URL`, `NESSIE_UI_URL`).
  These become Route URLs, and `MINIO_CONSOLE_URL` becomes either the managed
  service's console or nothing — the console template must tolerate its absence
  rather than rendering a dead link.
- NetworkPolicy: default-deny, then allow Airflow → Nessie, Airflow → Postgres,
  Nessie → Postgres, driver ↔ executor on the fixed Spark ports (§6.2), and
  **egress to the managed S3 endpoint**, which on many estates means an explicit
  egress rule or a proxy exception. If the S3 endpoint is reached through a
  proxy, `HTTPS_PROXY`/`NO_PROXY` belong in the platform ConfigMap and
  `NO_PROXY` must include the in-cluster service names or Nessie calls go out
  through the proxy and fail.
- The managed S3 service's TLS chain must be trusted. If it is signed by an
  internal CA, mount the estate's CA bundle and set `AWS_CA_BUNDLE` for botocore
  plus a JVM truststore for Spark. Two mechanisms, one fact — and the JVM half
  is the one people forget, so its symptom is worth knowing:
  `PKIX path building failed` inside a Spark task, with the boto3 clients
  working perfectly.

### 10.4 Provenance from the chart

The four variables `OPENSHIFT-MAPPING.md` requires, plus the image digest, set
at deployment and constant for its life:

| Variable | Source |
|---|---|
| `DBT_PROJECT_REF` | the commit the pipeline built from |
| `DBT_PROJECT_DIGEST` | `python -m reporting_platform.registry provenance` run by the pipeline over the project being deployed — **computed with the platform's own command, never reimplemented** |
| `DEPLOYMENT_CHANGE_REF` | the approved change ticket |
| `DEPLOYMENT_PIPELINE_REF` | the deploying pipeline/job id |
| `PLATFORM_CODE_REF` | the image **digest**, not a tag — a tag can be re-pushed, and then the evidence changes underneath an already-published run |

`check_project_drift()` compares digest to digest and refuses in `uat`/`prod`.
Worth confirming while wiring it: `dbt_manifest_ref()` hashes
`models/`, `macros/` and `tests/` only, so **vendoring `dbt_packages` into the
image (§4.2) does not move the digest** — checked, because if it did, every
package bump would read as project drift.

---

## 11. Phases, with exit criteria

| Phase | Scope | Done when |
|---|---|---|
| **0** | Answer §12's questions. Rebase images if §10.1 says so. Provision the bucket, the three databases, the Vault path. | The S3 flavour, path-style answer, credential TTL and image policy are written down, not assumed |
| **1** | Chart + Nessie + Airflow (KubernetesExecutor) + standalone Spark + watchdog, dev only. S3 code change from §3.2. | One feed ingests, `prepared_build` and `reporting_build` run on a branch, tests pass, the merge happens, a `published/` tag is cut. `registry runs`/`inputs` resolve |
| **2** | Spark to `k8s://`, jars baked into the Airflow image, `spark.jars.packages` dropped, RBAC. | The same chain as phase 1, with executor pods, and `test_versions.py` extended to the new declaration site |
| **3** | uat: no feed-ui, no notebook, `expose_config: false`, OIDC, `check_project_drift()` refusing on real values, retention and housekeeping DAGs on. | `python -m reporting_platform.monitoring.reproducibility` answers, and a rotation of the S3 credential completes with no manual step |
| **4** | prod. Sizing replaced with measured figures. Lineage if wanted. | — |

**The post-build CI tier finally becomes possible.** `lineage --columns
--require-derivable` is the gate `#a-gate-that-cannot-fail` describes as needing
a built catalog, which no tier has. A deployed non-prod environment with a real
build is where it can run — a `helm test` hook, or a pipeline stage after the
phase-1 chain. That is a genuine gain from this work and should be claimed
rather than left as a coincidence.

Existing CI tiers are unaffected and stay as they are. One cheap addition, in
its own workflow so `config.yml` stays ~10s: `helm lint` plus `helm template`
against each values file piped through `kubeconform`. It catches a bad template
and it proves the `fail` guards in §8 actually fire, which is the part worth
testing.

---

## 12. Open questions

Ordered by how much they change the plan.

1. **Vault's credential TTL.** If rotation is more frequent than a pod's
   lifetime, environment-variable injection is the wrong mechanism and §3.3's
   file-based alternative is a materially larger change. What is the TTL, and
   what is the rotation mechanism — ESO, Vault CSI, or the agent injector?
2. **Image base policy (§10.1).** Does "nothing in this estate may be Ubuntu or
   Alpine" apply to vendor images, or only to what we build from source?
   Rebasing Airflow and Spark onto UBI is weeks, not days.
3. **Which S3?** AWS S3 proper, or an on-prem S3-compatible store? It decides
   path-style vs virtual-host addressing, whether `S3_ENDPOINT` is set at all,
   and whether the TLS chain needs an internal CA bundle.
4. **Managed Postgres, or a Postgres in the cluster?** The plan assumes managed
   and that the chart has no `CREATE DATABASE` rights.
5. **Is the arrival push agent in scope for this work** (`OPENSHIFT-MAPPING.md`
   §1 — the Windows-side S3 `PutObject` agent), or does phase 1 land files by
   hand? If it is in scope it needs its own credential path and its own
   monitoring, and it is not a Helm chart.
6. **Lineage in or out?** It is two more workloads and a fourth database.
7. **Does anything trigger Airflow through its REST API above dev?** It decides
   whether `basic_auth` stays in `AIRFLOW__API__AUTH_BACKENDS` and whether a
   service account credential is needed (§8).
