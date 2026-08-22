# Reporting Platform — Local Approximation: Architecture

## Purpose

This repo is a **local, laptop-runnable approximation** of the target Reporting Platform
stack. It is deliberately shaped so that every component has a 1:1 counterpart
in the on-prem OpenShift target (see `OPENSHIFT-MAPPING.md`), and so that the
*code you write locally is the code that runs in the cluster* — only
configuration changes.

It replaces the legacy pattern:

```
Upstream CSV  →  the legacy ETL tool package  →  the legacy RDBMS stg tables
              →  stored procedures  →  reporting schema
              →  the legacy report server / the BI tool
              →  nightly partition-switch archive job
```

with:

```
Upstream CSV  →  landing (object storage, immutable)
              →  raw     (Iceberg, 1:1, typed)
              →  prepared (Iceberg, dbt, conformed)
              →  reporting (Iceberg, dbt, shared-lineage marts)
              →  serving  (optional RDBMS export / BI direct-read)
```

with Nessie providing commit-level version control over the whole lakehouse,
and an explicit maintenance + retention subsystem replacing the implicit
behaviours the legacy RDBMS gave us for free.

---

## Layer model

| Layer | Format | Catalog | Written by | Grain |
|---|---|---|---|---|
| `landing` | CSV as received, uncompressed | none (object keys) | landing task | one object per feed arrival |
| `raw` | Iceberg | Nessie `lakehouse.raw` | ingest task (Spark) | 1:1 with source rows, all columns as-received + ingest metadata |
| `prepared` | Iceberg | Nessie `lakehouse.prepared` | dbt | conformed, typed, deduplicated, one row per business key per business date |
| `reporting` | Iceberg | Nessie `lakehouse.reporting` | dbt | report-shaped marts, shared lineage |
| `serving` | Postgres / an enterprise RDBMS | n/a | export task | latest business date only, BI-tool-specific |

### Why `landing` is separate from `raw`

The legacy `stg` schema conflated "what arrived" with "what we parsed". Keeping
the byte-exact original in object storage means:

- reprocessing is possible without asking upstream to resend;
- schema drift is detectable by diffing arrivals rather than by a load failure;
- the retention policy for the immutable evidence copy can differ from the
  retention policy for the queryable copy (it does — see `RETENTION.md`).

### Why `raw` is 1:1 and untyped-ish

`raw` is a mechanical projection of the CSV. Every column lands as `string`
except the ingest metadata. All parsing, casting, trimming and null-normalising
happens in `prepared`, in dbt, where it is testable and version-controlled.
A load must never fail because a value was unparseable — it must land, and then
fail a *test*.

Ingest metadata columns added to every `raw` table:

| Column | Type | Meaning |
|---|---|---|
| `_business_date` | date | the date the data describes (from filename or feed config) |
| `_ingest_ts` | timestamp | when we ingested it |
| `_source_file` | string | full object key in `landing` |
| `_file_version` | int | 1, 2, 3… for re-deliveries of the same business date |
| `_row_number` | bigint | position in the source file, for diagnosis |
| `_batch_id` | string | ingest run identifier, = Nessie branch name suffix |

### Namespace naming

The three Iceberg layers are namespaces in the Nessie catalog: `lakehouse.raw`,
`lakehouse.prepared`, `lakehouse.reporting`. Note two things that are easy to get wrong,
both of which were live defects at one point:

- **dbt does not produce these names by default.** Its default
  `generate_schema_name` concatenates the profile's schema with the per-layer
  `+schema:`, which yielded `lakehouse_prepared` and `lakehouse_reporting` — sitting
  inconsistently beside the `raw` that `ingest_feed.py` creates literally.
  `dbt/macros/naming.sql` overrides it to use the layer name verbatim.
- **The catalog is not addressable as a third name part from dbt.** dbt-spark
  supports only a two-level `schema.table`, so `lakehouse` comes from
  `spark.sql.defaultCatalog` in `profiles.yml` rather than from a `database:`
  on the source.

Table names carry their dbt model prefix, so the full names are
`lakehouse.prepared.prep_trade`, `lakehouse.reporting.rpt_counterparty_exposure`, and so
on. Only `raw` tables are bare (`lakehouse.raw.trade`), because ingest creates them
rather than dbt.

### Why `prepared` and `reporting` are both dbt

Shared lineage is the whole point. Multiple legacy apps consume the same
counterparty and trade datasets. If each app builds its own copy, the estate
recreates the legacy estate's problem in a new stack. `prepared` is the single conformed
representation; `reporting` marts are thin, and every mart declares its
`prepared` inputs via `ref()` so lineage is machine-readable — this is the
metadata catalog gap called out in the PoC review, closed by dbt's manifest.

---

## Per-feed processing, not a nightly batch

Requirement: *each feed must be processed as soon as it is received.*

The DAG topology is therefore:

```
                     (file arrival)
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
  ingest_trade      ingest_counterparty   ingest_rating      ← one DAG per feed,
        │                  │                  │                generated from feeds.yml
        ▼                  ▼                  ▼
   Asset: raw.trade   Asset: raw.counterparty  Asset: raw.rating
        └──────────────────┼──────────────────┘
                           ▼
                  prepared_build (dbt)          ← triggered by ANY upstream asset
                           │
                  Asset: prepared.*
                           ▼
                  reporting_build (dbt)         ← triggered by prepared assets
                           │
                  Asset: reporting.*
                           ▼
                     serving_export
```

Airflow **assets** (datasets in Airflow 2) do the coupling. No feed waits for
any other feed to arrive. `prepared_build` is scheduled on the asset condition
and runs as soon as anything upstream changes; dbt's own `state:modified+` and
model selection keep the rebuild proportionate.

A feed that is late does not block the ones that arrived; the reporting layer
simply carries forward the last good version of that dimension and the
freshness test flags it. This is a deliberate behavioural change from the
legacy scheduler's gated model and needs to be signed off by the report owners.

**Two different failures hide behind "late", and only one of them is caught by
freshness.** `dbt source freshness` measures the age of the newest
`_ingest_ts`, so it detects a feed that has *stopped arriving*. It cannot
detect a hole in the middle of a history that later resumed, because a newer
delivery resets the clock — the seed's deliberately absent counterparty date
raises no freshness warning at all. That second shape is the more dangerous
one: a late feed is visibly missing and the report is visibly incomplete,
whereas a gap is a report that runs, returns numbers, and is quietly wrong for
one date forever.

`reporting_platform/monitoring/completeness.py` covers the gap case. It infers the
business calendar from the platform's own data — a business date is one on
which at least one feed delivered — so no holiday calendar is needed and a
holiday can never be reported as a gap. Its blind spot is a day on which every
feed missed; that is the orchestrator's and the watchdog's territory.

---

## Nessie: write-audit-publish

Every ingest and every dbt build runs on a **branch**, not on `main`.

```
main ────────────●────────────────────●──────────►
                 ▲                    ▲
                 │ merge (if tests    │ merge
                 │        pass)       │
  ingest/trade/2026-08-11/r7 ──●      │
                                      │
  build/prepared/2026-08-11/r7 ───●───●
```

Branch naming: `<purpose>/<scope>/<business_date>/<run_id>`.

This gives us three things the legacy RDBMS never did cheaply:

1. **Atomic multi-table publication.** A reporting refresh that touches nine
   tables becomes one Nessie merge. Consumers never see a half-built mart.
2. **Rollback.** A bad publication is reverted by resetting `main` to the prior
   commit; the data files are still there.
3. **Reproducibility.** A report can be re-run "as at" a Nessie commit hash, so
   a regulator question about a figure published on a given date is answerable.

Tags are cut on `main` after each successful publication:
`published/<business_date>/<run_id>`. Retention of *tags* is what determines
how far back you can time-travel, and is a separate policy from row retention.

---

## Engine strategy: Spark runs the pipeline, DuckDB serves people

**Spark is the only build engine, and that is a constraint rather than a
preference.** Two independent things force it:

- **Every build must land on a Nessie branch.** Write-audit-publish is the
  safety model this whole document is built around, and a branch is something
  the engine has to be able to *address*. Only the Spark path can: dbt-spark
  takes `spark.sql.catalog.lakehouse.ref` directly, which is how `nessie_ref`
  threads from the DAG into the build.
- **Iceberg maintenance is Spark-only.** `rewrite_data_files`,
  `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files` and
  `rewrite_position_delete_files` are Iceberg *stored procedures* invoked via
  `CALL`, and only the Spark runtime implements them.

This was tested rather than assumed. Session 5 got DuckDB
building Iceberg tables into Nessie for the first time — `duckdb 1.5.5` +
`dbt-duckdb 1.9.6` against the Iceberg REST catalog Nessie serves at
`/iceberg`, with `dbt build` completing and Spark reading the result — and
then removed the target anyway, because a *completing build* turned out to
prove less than it looked:

| | |
|---|---|
| Cannot address a Nessie branch | the ref rides in the Iceberg REST prefix, which DuckDB takes from `/v1/config` and cannot override — so no write-audit-publish |
| Silently drops `partition_by` | produces unpartitioned tables, and `business_date` partitioning is what makes retention's expiry a metadata delete rather than a full rewrite |
| Cannot `INSERT`/`UPDATE` a partitioned table | so it cannot write to the tables the platform already has, without an explicit override |

Any one of those is disqualifying.

**DuckDB's role is reading published `main`** — analyst queries, a developer
checking what actually landed, the kind of question that should not need a
SparkSession. `scripts/duckdb_console.py` is that entry point: a read-only
attach, one command, sub-second answers against the same Iceberg tables
through the same catalog.

```
docker compose exec -T airflow python -m scripts.duckdb_console --tables
docker compose exec -T airflow python -m scripts.duckdb_console \
    "select business_date, count(*) from lakehouse.prepared.prep_trade group by 1"
```

It is a script rather than a dbt target deliberately. The engine macros are
Spark-only now, so a DuckDB target could not compile the models anyway, and a
target that builds some models and not others is a trap. The read-only attach
is verified, not assumed — `CREATE` fails with *"Cannot execute statement of
type CREATE on database ... attached in read-only mode"*.

### What happened to the portability layer

This section used to promise that models were engine-portable: ANSI SQL only,
divergences fixed in `dbt/macros/engine.sql`, "the project ships tests that
must pass on both engines". **That promise is withdrawn**, because nothing
verified it and an untested guarantee in this codebase has a poor record. The
DuckDB branches are gone from the macros.

`engine.sql` remains, and its real value was never the second engine: it is
that engine-specific constructs live in **one place**. Bug #8 was a bare
`CAST(x AS VARCHAR)` that Spark rejects outright, and all three
reporting models having copy-pasted their own inline version, so fixing the
macro never reached them. Centralisation is what prevents that, with one
engine or five.

`profiles.yml` carries `spark_local` and `spark_ocp`, both pointed at the same
Nessie catalog and the same S3 warehouse; `DBT_TARGET` selects between them at
deployment time. `airflow/dags/dbt_builds.py` **refuses** a target that is not
a Spark one — because the failure it prevents is silent, not loud: a
non-Spark engine ignores the `nessie_ref` var, writes to the default branch,
and the DAG goes green.

---

## Component inventory

| Component | Local | Purpose |
|---|---|---|
| Object storage | MinIO | stand-in for the on-prem S3-compatible store |
| Catalog | Nessie (Postgres-backed) | Iceberg catalog + git-like versioning |
| Orchestration | Airflow **2.10.5**, LocalExecutor | DAGs, assets, sensors |
| Compute | Spark 3.5 standalone | ingest, maintenance, **and every dbt build** |
| Transformation | dbt-core + dbt-spark | prepared + reporting |
| Query (people) | DuckDB, read-only on `main` | `scripts/duckdb_console.py` — analyst and dev queries |
| Serving | Postgres | stand-in for an enterprise RDBMS export target |
| Metadata | dbt manifest + Nessie commit log | lineage, catalog |

Airflow is **2.10.5 deliberately, not 3.x**. Under Airflow 3 no DAG run here
could ever complete — tasks ran, logged, returned values and pushed xcom, and
the scheduler never recorded them. Airflow 2's
LocalExecutor writes the result straight to the metadata DB. Read
`Dockerfile.airflow`'s header before changing it.

Nessie is deliberately Postgres-backed rather than in-memory so that the local
stack exercises the same version-store code path as the cluster, and so
restarting the stack does not silently reset the catalog.
