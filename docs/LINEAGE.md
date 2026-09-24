# Lineage

The authoritative operational chain for a published result is:

```text
Report Version
  -> registry.run
  -> registry.run_input
  -> registry.delivery observation
  -> DeliveryManifest
  -> Transport completion evidence
  -> original received source object(s)
```

Use `python -m reporting_platform.registry trace --report NAME --as-at DATE
--version N` to read that chain. A missing `registry.delivery` observation is
reported as an evidence-coverage gap; it does not remove the DeliveryID from
the authoritative run history. Reconciliation can rebuild the observation
from the immutable DeliveryManifest later.

## Delivery provenance through transformation

Raw has two deliberately different fields:

| Field | Meaning |
|---|---|
| `_delivery_id` | accepted Delivery identity; new v2 rows carry the opaque `dlv_...` value |
| `_source_file` | physical object or normalized part that Spark read |

`delivery_ref()` maps Raw `_delivery_id` to the prepared layer's canonical
`delivery_id`. It uses the basename of `_source_file` only when `_delivery_id`
is null on a historical row written before Delivery provenance existed. New
v2 identity is never reconstructed from a filename.

`_file_version` remains ordering for Feed + COB-date restatement selection.
It decides which snapshot survives; `delivery_id` identifies the accepted
Delivery that supplied the surviving row. They are not interchangeable.

SCD2 rows keep the Delivery that created each historical version. A later
replay may update that version's build-audit columns when it closes or reopens
the range, but it does not replace the version's `delivery_id`. If Delivery A
creates state X and a later Delivery B repeats X unchanged, B is scanned but
creates no SCD2 version and is therefore absent from the published input set.

## What `run_input` means

**`run_input` contains Deliveries whose rows are present in what the run
published. It does not necessarily contain every Delivery scanned during
execution.**

Both prepared and reporting runs collect the distinct `(feed, delivery_id)`
pairs from prepared tables on their own build branch before merge. Prepared is
the boundary used for both because its model/table name preserves Feed
identity, while arbitrary reporting joins and aggregates may combine several
feeds and several Deliveries. A reporting row keeps a scalar `delivery_id`
only where one Delivery is naturally representable; generic report lineage is
the run-level union in `registry.run_input`, not a comma-separated or JSON
list added to every row.

This yields three separate levels:

- operational Delivery provenance: the authoritative chain above;
- dbt dataset/column lineage: which datasets and columns may contribute;
- optional model-specific value provenance: which value won, only where a
  model already materializes that business fact.

Phase 5 implements the first and preserves the second. It does not introduce a
universal value-level framework.

## dbt execution artifacts

`registry.run.dbt_manifest_ref` remains the content digest of the dbt project
that was built, alongside the declared `dbt_project_ref`. Cosmos runs one dbt
subprocess per rendered task, so there is no honest single invocation manifest.
Each task therefore copies its own `manifest.json` and `run_results.json` to
the immutable object prefix in `registry.run.dbt_artifacts_ref` before another
task can overwrite the shared target directory. `catalog.json` is copied when
generated. Publication refuses to merge if a successful dbt task lacks either
required artifact.

These files are retained for later metadata consumers; they are not a custom
catalogue and Phase 5 does not implement OpenMetadata.

OpenLineage is an **export**. Marquez is a **consumer**. Neither is an
authority, and neither may ever gain the power to stop the pipeline.

Both are **off by default**, behind `OPENLINEAGE_DISABLED` in `.env` and the
`lineage` compose profile.

## Turning it on

```bash
docker compose --profile lineage up -d marquez-api marquez-web
# OPENLINEAGE_DISABLED=false in .env, and then:
docker compose up -d --force-recreate airflow airflow-webserver airflow-triggerer
```

**`--force-recreate`, not `restart`.** The environment is read at process
start, so a restarted container carries the old value and emits nothing, with
no error to say why.

The UI is on <http://localhost:13000>. Ports 5000/5001/3000 are deliberately
remapped — they collide with Grafana and friends on an ordinary developer box.

**Both Marquez images are built here, on UBI, from Marquez's own source**
(`Dockerfile.marquez-api`, `Dockerfile.marquez-web`); upstream ships Ubuntu and
Alpine. `MARQUEZ_VERSION` is the **release tag the builders fetch**, so the
first `--profile lineage up` after changing it runs gradle and npm *with
egress* rather than pulling an image. Nothing else about the deployment
changed. ([`#marquez-on-ubi`](DECISIONS.md#marquez-on-ubi))

## Two traps that cost an afternoon each

**Register the custom extractor under `AIRFLOW__OPENLINEAGE__EXTRACTORS`.**
Not `__CUSTOM_EXTRACTORS`, which is read by nothing and warns about nothing.
Without the right variable, Airflow emits jobs and **no datasets at all** —
because neither built-in path can work here, and both reasons are permanent.

**The OpenLineage provider is already in the image.** Installing it explicitly
under Airflow's constraint file is the cosmos trap exactly: the constraint
resolution downgrades a shared dependency and something unrelated dies at
import, a long way from the change.

## Looking at it without starting anything

```bash
# what Marquez will be told each task reads and writes -- no Airflow, no Spark
docker compose exec -T airflow python -m reporting_platform.lineage
docker compose exec -T airflow python -m reporting_platform.lineage --json

# every column of every managed table, classified. EXITS 1 on any `unresolved`
docker compose exec -T airflow python -m reporting_platform.lineage --columns
```

This answers "what will Marquez be told", which is the question you actually
have when the graph looks wrong — and it answers it **from the same derivation
the extractor uses**, so a missing edge here is a missing edge there.

## Every column is classified

Reporting only the columns that trace would make a literal, a `count(*)` and a
parser failure look identical.

| Class | Means |
|---|---|
| `sourced` | Computed from ≥1 upstream table column, which is named. |
| `row_aggregate` | An aggregate over **rows**, not columns: `count(*)`. |
| `build_metadata` | A property of the build: `current_timestamp()`. |
| `literal` | A constant the build injected: `dbt_invocation_id`, `nessie_ref`. |
| `ingest_added` | Ingest's own column, sourceless by construction. |
| `unresolved` | **The defect class.** The parser could not read it. |

On the shipped project: 136 columns, 116 `sourced`, 20 sourceless, 0
`unresolved`.

**`unresolved` is a defect that does not fail a build.** Nothing in this
package may raise — it runs inside an extractor, and an export must never gain
the power to stop the pipeline. So the refusal has to live somewhere that *is*
allowed to say no, and that somewhere is **CI**: `lineage --columns` exits 1 on
any unresolved column. It needs the catalog and the compiled SQL, which the
edge listing does not, which is why it is a flag rather than the default.

## It is not the record of what a run published

`registry.run_input` is. The two legitimately differ, and **you must not
reconcile them**: an SCD2 dimension may contribute 10 of 40 deliveries to a
published figure while OpenLineage correctly reports all 40 as read. They
answer different questions.

**There is one graph walker, not two.** `context.model_refs()` derives the
lineage graph *and* is what `feeds_behind_report()` uses to size per-feed
retention windows. A second derivation that drifted from the first is the
failure this codebase keeps warning about, so `tests/test_lineage.py` asserts
the two agree.

## A skipped task shows as `RUNNING` forever

Airflow 2.10's listener spec has no skipped hook, and skipping is the ingest
DAGs' idle state. `RUNNING` in Marquez means **"started, did not succeed or
fail"** — not "running now".

**Airflow is the authority on what is running.** Do not build an alert on
Marquez run states. ([`#openlineage-is-an-export-not-a-record`](DECISIONS.md#openlineage-is-an-export-not-a-record))

## No value this package emits may contain a credential word

Facet values pass through Airflow's `SecretsMasker`, and this estate's Postgres
user and password are both `platform`. A class called `platform_column`
therefore reached Marquez as `***_column` — a well-formed facet with corrupted
content, which is worse than a missing one. A test pins this.

## Querying Marquez directly

```bash
curl -s 'http://localhost:15000/api/v1/namespaces/reporting-platform-local/jobs?limit=50'

# the datasets live in their OWN namespaces, not the job's -- a jobs query
# showing no inputs/outputs is NOT evidence that nothing was emitted
curl -s http://localhost:15000/api/v1/namespaces

curl -s -G http://localhost:15000/api/v1/lineage \
  --data-urlencode 'nodeId=dataset:s3://lakehouse:landing/fo_trade' \
  --data-urlencode depth=20

# column-level: one column back to the CSV it came from, with the SQL that
# transformed it at each hop
curl -s -G http://localhost:15000/api/v1/column-lineage --data-urlencode depth=20 \
  --data-urlencode 'nodeId=datasetField:iceberg://lakehouse:reporting.exposure_by_country:total_mtm'
```

## Phase 6: orchestration does not change lineage

`transport_ingest`'s `ingest_raw` task calls the same
`ingest_normalized_delivery()` Phase 4 already used, so Raw rows it writes
carry the identical `_delivery_id`/`_source_file`/`_cob_date`/etc. provenance
columns regardless of whether a Delivery reached Raw through the legacy
per-feed DAG or the new Transport-driven one. `delivery_ref()`, `run_input`,
and `registry trace` all continue to work unchanged --
`docs/AIRFLOW-ORCHESTRATION.md` is about triggering, retries, and
concurrency, not about what gets written or how it is traced.

## Related

- [`ARCHITECTURE.md`](ARCHITECTURE.md#lineage-is-an-export-not-an-authority) — where lineage sits
- [`DECISIONS.md#lineage-is-derived-from-the-dbt-project`](DECISIONS.md#lineage-is-derived-from-the-dbt-project) and [`#a-column-with-no-source-says-so`](DECISIONS.md#a-column-with-no-source-says-so) — **read these before changing `reporting_platform/lineage`**
- [`REGISTRY.md`](REGISTRY.md) — the actual publication record
