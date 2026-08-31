# Local → OpenShift Mapping

The point of the local stack is that promotion changes configuration, not code.
This table is the contract that keeps that true.

| Concern | Local | OpenShift target | What changes |
|---|---|---|---|
| Object storage | MinIO container | on-prem S3-compatible store | endpoint URL, credentials |
| S3 credentials | `.env` static keys | Kubernetes Secret → env vars | secret source only |
| Catalog | Nessie container | Nessie Deployment + Service | `NESSIE_URI` |
| Nessie version store | Postgres container | existing Postgres/CockroachDB | JDBC URL |
| Orchestration | Airflow LocalExecutor | Airflow KubernetesExecutor | executor config, pod template |
| Spark | standalone master+worker | `spark-submit` in-cluster, or Spark Operator | `--master`, image ref |
| dbt | Cosmos `ExecutionMode.LOCAL`, one task per model in the Airflow container | Cosmos `ExecutionMode.KUBERNETES`, same image, one pod per model | `ExecutionConfig` in `dbt_builds.py`; the rendered graph is unchanged |
| Serving DB | Postgres container | an enterprise RDBMS | dbt/export target profile |
| Secrets | `.env` | OpenShift Secrets / Vault | injection mechanism |
| DAG deployment | bind mount | Forge CI → image → Helm | packaging only |
| Feed arrival | poll of the MinIO landing prefix | S3 event / SFTP landing prefix poll | sensor implementation |

## The three things that genuinely differ

Everything above is configuration. These three are real design work that the
local stack can only approximate:

### 1. Feed arrival

Locally we poll a directory. In the cluster, files arrive from SFTP or — the
current sticking point — a Windows DFS share reached with Windows auth via a
privileged system AD account.

The recommendation is to **stop trying to make the pod reach DFS**. Cross-domain
Kerberos from an OpenShift container to a Windows DFS namespace is a poor
dependency to build a strategic platform on: it needs a keytab in the cluster, a
working `krb5.conf` for the trust path, DFS referral handling in the client, and
it ties the new stack to exactly the licensed Windows infrastructure the
programme is trying to decouple from.

The lower-risk shape is a **push, not a pull**: a small agent on the existing
Windows host (which already has the share mounted and the AD context) does an
S3 `PutObject` into the landing prefix, and the pipeline triggers off the object
arriving. That inverts the trust direction, removes Kerberos from the cluster
entirely, and is a component that can be retired the day upstream can write to
S3 directly.

It is not free — it keeps a Windows footprint alive for longer and needs its own
monitoring — so it is a trade, not an obvious win. But it is a smaller and more
contained trade than in-cluster cross-domain Kerberos.

### 1b. dbt execution mode

The build DAGs are rendered by Astronomer Cosmos, and **the render is
deployment-independent**: `LoadMode.DBT_LS` reads the dbt project the same way
wherever it runs, so the task graph in OpenShift is the same graph as on a
laptop. What changes is one field.

`ExecutionConfig(execution_mode=ExecutionMode.LOCAL)` runs each model as a dbt
subprocess inside the Airflow container. `ExecutionMode.KUBERNETES` runs each
one as a pod instead, which is the same substitution the ingest tasks make when
`_spark_task.py` becomes a KubernetesPodOperator issuing `spark-submit` — same
module, same arguments, a different execution wrapper.

Two things carry over rather than being re-decided:

- **`InvocationMode.SUBPROCESS` stays**, for the local mode. It is not a
  laptop concession: the dbt target is `method: session`, so dbt builds a
  SparkSession in-process, and Cosmos's default `DBT_RUNNER` would leave that
  JVM inside the task process to be zombie-reaped. Under
  `ExecutionMode.KUBERNETES` the pod boundary supplies the same isolation and
  the setting stops applying.
- **The `lakehouse_write` pool is a local capacity guard**, sized against one
  6-core standalone worker. In the cluster the constraint is different — the
  serialisation that still matters is *writer* exclusion (maintenance must not
  run alongside a write), not core starvation, so keep the pool but revisit
  the slot count alongside the Spark execution model below.

**UNPROVEN: no cluster has ever run this.** The same caveat as the `spark_ocp`
dbt target it would use.

### 2. Spark execution model

Locally, one standalone master with one worker. In the cluster, each Spark job
is a driver pod plus N executor pods, and you need to decide:

- **Spark Operator (`SparkApplication` CRD)** — declarative, good observability,
  but another operator to install and keep patched.
- **`spark-submit` in cluster mode from a KubernetesPodOperator** — no operator,
  fewer moving parts, but you own the RBAC and the pod templates.

Given the existing Forge/Helm deployment pattern and the team's capacity
position, `spark-submit` from a KubernetesPodOperator is the lower-overhead
starting point. Revisit if Spark job count grows past ~20 distinct jobs.

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
