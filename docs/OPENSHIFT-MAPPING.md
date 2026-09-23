# Local → OpenShift Mapping

The point of the local stack is that promotion changes configuration, not code.
This table is the contract that keeps that true.

| Concern | Local | OpenShift target | What changes |
|---|---|---|---|
| Object storage | MinIO container | on-prem S3-compatible store | endpoint URL, credentials |
| S3 credentials | `.env` static keys | Kubernetes Secret → env vars | secret source only |
| Catalog | Nessie container | Nessie Deployment + Service | `NESSIE_URI` |
| Nessie version store | Postgres container | existing Postgres/CockroachDB | JDBC URL |
| Orchestration | Airflow LocalExecutor | Airflow KubernetesExecutor, via the official chart (`deploy/helm/reporting-platform`'s `airflow` dependency, pinned to 1.16.0 — the last line whose default is Airflow 2.10.5) | executor config, pod template, chart values |
| Spark | standalone master+worker | a driver pod per `_spark_task` call (`k8s://` master), executors as pods — not the Spark Operator | `--master`, image ref |
| dbt | Cosmos `ExecutionMode.LOCAL` + `InvocationMode.SUBPROCESS`, one task per model in the Airflow container | THE SAME — `ExecutionMode.LOCAL` + `InvocationMode.SUBPROCESS` in every environment; only the dbt TARGET changes (`spark_local` → `spark_ocp`), which points dbt's executors at pods through a `k8s://` master while its driver stays put | `DBT_TARGET`, from the chart's ConfigMap. See `docs/DECISIONS.md#execution-mode-is-configuration` |
| Serving DB | Postgres container | an enterprise RDBMS | dbt/export target profile |
| Secrets | `.env` | OpenShift Secrets / Vault | injection mechanism |
| DAG deployment | bind mount | Forge CI → image → Helm | packaging only |
| Change provenance | nothing declared; content digests | set by the chart, constant for the deployment's life | env vars only — see `values.yaml`'s `provenance.*` keys |
| Feed arrival (current, Transport path) | DCM/producer `PutObject` into `received/<TransportID>/`, picked up by `transport_watch`'s S3 sensor | S3-compatible store, same `PutObject` boundary — DCM (or its S3-writing bridge) writes directly | none — this boundary is S3-native in both places |
| Feed arrival (legacy compatibility path) | poll of the MinIO landing prefix | S3 event / SFTP landing prefix poll, or a DFS-to-S3 push agent (see below) | sensor implementation |

## What the chart must supply

The chart (`deploy/helm/reporting-platform`) is now the single place this is
written down — see `values.yaml`'s own banner comment, its per-environment
`values-{dev,uat,prod,local-k8s}.yaml` files, and
`docs/DECISIONS.md#the-chart-is-the-only-place-settings-are-written` for how
one ConfigMap and one Secret reach every platform pod. This section used to
restate that schema in prose; read `values.yaml` instead; the two are
guaranteed to drift; a comment here cannot.

**`DBT_PROJECT_DIGEST` is computed with the platform's own command**, not
reimplemented in the pipeline (`make release-image` prints it, from
`registry provenance` run inside the release image). A run recomputes it and
compares; a mismatch means the project was modified after deployment, and in
`uat`/`prod` the build refuses rather than attributing a publication to a
commit that did not produce it. See
`docs/DECISIONS.md#a-change-is-a-deployment-event-not-a-run-event`.

**The feed console is not deployed above `dev`.** It writes `_sources.yml` and
a scaffolded model into the project, which is exactly the drift the check
exists to catch — so its changes reach `uat` and `prod` the same way any other
change does, through git and the pipeline. The chart enforces this itself now:
`feedConsole.enabled: true` in `uat`/`prod` fails `helm template` rather than
deploying something that would only be caught later, at the next build.

## The three things that genuinely differ

Everything above is configuration. These three are real design work that the
local stack can only approximate:

### 1. Feed arrival

**The Transport path's boundary is already S3-native and needs no DFS/SMB/
Kerberos design — Phase 1-6 deliberately removed that requirement from the
future-state boundary.** DCM (or another approved producer) writes source
objects and `_COMPLETE.json` directly into `received/<TransportID>/` via
`PutObject`; `transport_watch`'s deferrable `S3KeySensor` (and
`transport_reconcile`'s recovery sweep) trigger off that arrival, in the
cluster exactly as locally — only the endpoint and credentials change, per
the table above. No Airflow pod needs Windows-share access on this path. See
[TRANSPORT-CONTRACT.md](TRANSPORT-CONTRACT.md) for what a producer must hand
off.

**The legacy compatibility path is where the Windows/DFS design question
below still applies**, for any Feed still onboarded onto `landing/` rather
than Transport. Locally we poll a directory. In the cluster, files would
arrive from SFTP or — the sticking point this design work was for — a
Windows DFS share reached with Windows auth via a privileged system AD
account.

The recommendation for that legacy path is to **stop trying to make the pod
reach DFS**. Cross-domain Kerberos from an OpenShift container to a Windows
DFS namespace is a poor dependency to build a strategic platform on: it needs
a keytab in the cluster, a working `krb5.conf` for the trust path, DFS
referral handling in the client, and it ties the new stack to exactly the
licensed Windows infrastructure the programme is trying to decouple from.

The lower-risk shape is a **push, not a pull**: a small agent on the existing
Windows host (which already has the share mounted and the AD context) does an
S3 `PutObject` into the landing prefix, and the pipeline triggers off the
object arriving. That inverts the trust direction, removes Kerberos from the
cluster entirely, and is a component that can be retired the day the Feed
onboards onto the Transport path above instead.

It is not free — it keeps a Windows footprint alive for longer and needs its own
monitoring — so it is a trade, not an obvious win. But it is a smaller and more
contained trade than in-cluster cross-domain Kerberos, and it only applies to
Feeds still on the legacy path.

### 1b. dbt execution mode

The build DAGs are rendered by Astronomer Cosmos, and **the render is
deployment-independent**: `LoadMode.DBT_LS` reads the dbt project the same way
wherever it runs, so the task graph in OpenShift is the same graph as on a
laptop.

**Settled, not a live choice: `ExecutionConfig` stays `ExecutionMode.LOCAL` +
`InvocationMode.SUBPROCESS` in every environment.** An earlier draft of this
section framed `ExecutionMode.KUBERNETES` (a pod per model) as an option to
decide between here, the same substitution the ingest tasks make when
`_spark_task.run` moves a Spark driver into its own pod. It is not available
for dbt: `_archive_dbt_artifacts` reads dbt's `target/` off the TASK's own
filesystem to archive build artifacts and capture validation evidence, and
`publish` refuses to merge a build it cannot verify that way — so
`ExecutionMode.KUBERNETES` would run dbt in a pod `target/` is never read
back from, and the switch would never publish. See
`docs/DECISIONS.md#execution-mode-is-configuration` for the full reasoning
and what DOES move: only the dbt TARGET (`spark_local` → `spark_ocp`), which
points dbt's own executors at pods through a `k8s://` master while dbt's
driver stays a subprocess in the Airflow task's pod.

Two things carry over as a result, not because they were re-decided:

- **`InvocationMode.SUBPROCESS` stays.** It is not a laptop concession: the
  dbt target is `method: session`, so dbt builds a SparkSession in-process,
  and Cosmos's default `DBT_RUNNER` would leave that JVM inside the task
  process to be zombie-reaped. Cosmos's `KUBERNETES` execution mode would
  supply pod-boundary isolation instead, if it were reachable for dbt here.
- **The `lakehouse_write` pool is a writer-exclusion guard**, not a capacity
  one: maintenance must not run alongside a write. Its slot count is
  `LAKEHOUSE_WRITE_SLOTS` (default 1, set by the chart), and above 1 it
  excludes nothing, so keep it at 1 on the cluster too. See
  `DECISIONS.md#one-shared-write-pool`.

**Built, not yet run on a real cluster.** `deploy/helm/reporting-platform`
renders `spark_ocp` under dbt (`check_dag_imports` gives the same DAG count in
both modes), but `helm template` is not a cluster: the first `spark_ocp` dbt
build actually executing is `make k8s-smoke`'s job, not this document's.

### 2. Spark execution model

Locally, one standalone master with one worker. In the cluster, each Spark job
is a driver pod plus N executor pods. This section used to weigh two options:

- **Spark Operator (`SparkApplication` CRD)** — declarative, good observability,
  but another operator to install and keep patched.
- **A driver pod launched directly, no operator** — fewer moving parts, but
  you own the RBAC and the pod templates.

**Settled and built, not still open: the second option, WITHOUT even a
KubernetesPodOperator.** `_spark_task.run` (`PLATFORM_EXECUTION=kubernetes`)
launches the driver as a plain Pod through the Kubernetes Python client
directly from inside the calling Airflow task — the same module, the same
arguments, the same JSON-on-stdout contract as the local subprocess path, so
no DAG changed shape for it (`docs/DECISIONS.md#execution-mode-is-configuration`).
The RBAC that decision requires (a Role letting the Spark and Airflow-worker
ServiceAccounts create/watch/delete pods, and Spark's own executor
pods/services/configmaps) is `deploy/helm/reporting-platform/templates/rbac-spark.yaml`.
Revisit if Spark job count grows past ~20 distinct jobs.

Either way: the paired infra/worker namespace topology already established for
an earlier Airflow deployment applies unchanged.

### 3. Non-prod data

Locally, generated sample data. In the cluster today, non-prod carries
production data with access restricted to prod-authorised users. The pipeline
code is identical either way, but the *retention configuration* and eventually
the *masking step* differ by environment.

Design for this now by keeping every environment-varying value in
`reporting_platform/config/*.yml` selected by `REPORTING_ENV`, rather than discovering later
that masking needs to be threaded through twelve DAGs.

## Resource sizing starting points

Not measured — these are starting points to be replaced with observed figures.

| Workload | Driver | Executors | Notes |
|---|---|---|---|
| Ingest (per feed) | 1 CPU / 2 GB | 2 × (1 CPU / 4 GB) | most feeds are small |
| dbt prepared build | 1 CPU / 2 GB | 2 × (2 CPU / 8 GB) | Spark only — see below |
| dbt reporting build | 1 CPU / 2 GB | 2 × (2 CPU / 8 GB) | |
| Maintenance | 2 CPU / 4 GB | 4 × (2 CPU / 8 GB) | bursty, off-peak |
| Retention | 1 CPU / 2 GB | 2 × (1 CPU / 4 GB) | metadata-heavy, not data-heavy |

### DuckDB: demonstrated, and priced against the wrong workload

This section used to say the DuckDB row above priced an option that had never
been demonstrated, and to recommend establishing that a version bump made
`duckdb_local` work before leaning on it. That has since been done. The bump works — and it settled the capacity question the other way.

**What works.** `duckdb==1.5.5` + `dbt-duckdb==1.9.6`, attached to the Iceberg
REST catalog Nessie serves at `/iceberg` (enabled by `nessie.catalog.*` in
`docker-compose.yml`). `dbt build --target duckdb_local` completes, reads and
writes real Iceberg tables, and what it writes is immediately readable by
Spark through the Nessie API. The cheap-engine intuition is sound: it built in
under a second what Spark takes tens of seconds to build.

**What does not, and cannot without an upstream change.** DuckDB can only
target the **default branch**. The Nessie ref travels in the Iceberg REST
request prefix, DuckDB takes that prefix from the catalog's `/v1/config`
response, and its `ATTACH` exposes no way to override it. So there is no
branch to build on, and therefore **no write-audit-publish**: every DuckDB
build would write straight to `main`. That is the safety property the whole
architecture is built around (`ARCHITECTURE.md`, "write-audit-publish"), and
no amount of extra CPU saving buys it back.

**So the sizing row above is Spark-only for the transformation builds.** Where
DuckDB does belong in a capacity plan is on the read side, against published
`main` — ad-hoc analysis, extract generation, a serving pod. That is the
`serving_export` shape, and a single 2 CPU / 8 GB pod is a
realistic starting point for it. Price it there, not here.

One caveat to carry into any read-side sizing: `duckdb-iceberg` issue #969
(open) makes a REST-attached read follow the newest snapshot across all
**Iceberg table-level** refs rather than `current-snapshot-id`. Every table
here carries only `main` at that level — Nessie does the branching, not
Iceberg — so it cannot bite as things stand. It would the moment anyone used
`ALTER TABLE ... CREATE BRANCH`, and it would be silent.

Detail, including the exact errors at each step and the ATTACH option list
checked against the current DuckDB documentation, is in the module docstring of
`scripts/duckdb_console.py`.

## Phase 8: migration_reconcile

`migration_reconcile` is one more Airflow DAG and needs no new component in
this mapping: it runs in the same Airflow deployment, reads the same
Postgres `platform` database (one new table, `registry.migration_comparison`)
and the same object store (`migration-diffs/` alongside `dbt-artifacts/`).
A production legacy adapter (`docs/MIGRATION.md#legacy-adapter`) would be the
one new network dependency this phase's design anticipates -- read-only
credentials to the legacy estate, configured the same way every other
connection in this mapping is, never hard-coded.
