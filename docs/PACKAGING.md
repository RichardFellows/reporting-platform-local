# Packaging and deployment

How this one repo becomes several packages, each built, tested, versioned,
published to Nexus and deployed to an existing Airflow estate on its own.
The boundaries are enforced now. Separate builds are the next step, and this
page says which parts exist and which do not.

## The components

`components.yml` is the only statement of which module belongs to which
component. `tests/test_components.py` fails on any import, lazy ones included,
that reaches a component its owner does not declare. The reasoning is in
[`DECISIONS.md#components-are-declared-and-enforced`](DECISIONS.md#components-are-declared-and-enforced).

| Component | What it is | Depends on | DAGs |
|---|---|---|---|
| `transport` | Copies files from a source location to the on-prem S3 bucket and writes the Transport manifest (`reporting_transport/`) | nothing | none yet |
| `core` | Settings, feed registry loading, Nessie/Spark/DuckDB wrappers, the Airflow REST client, the Spark task launcher, the Postgres registry schema and its run/lifecycle records | nothing | none |
| `ingest` | Reads Transport manifests and deliveries, normalizes them, writes raw Iceberg tables; the delivery index | `core`, `transport` | `transport_watch`, `transport_ingest`, `transport_reconcile`, `feed_ingest`, `_transport_trigger` |
| `dbt` | The dbt project (prepared and reporting layers) and the build/test/publish DAGs | `core` | `dbt_builds` |
| `ops` | Monitoring, retention, table maintenance, lineage export, the registry admin CLI | `core`, `transport`, `ingest` | `platform_housekeeping` |
| `dev` | Feed console, feed onboarding (`sniff`), legacy-migration harness. **Never packaged** | anything | `migration_reconcile` |
| *(export)* | Not built yet. Reads a report from its `published/…` Nessie tag and writes it to the target database over JDBC | `core` | — |

The contracts between components:

- **transport → ingest**: the Transport manifest, parsed by
  `reporting_transport.contract` on both sides.
- **ingest → dbt**: the raw tables' schema, which comes from the feed config,
  plus the raw Airflow Datasets.
- **dbt → export**: the `published/<report>/<bd>/<run_id>` tag.
- **The registry**: its Postgres schema, owned by `core`.

## Spark operations

Every DAG launches Spark work through `reporting_platform.common.spark_task.run`
and nothing else. The launcher is in `core`. Each operation's body is in a
`spark_ops.py` in the component that owns it, and `spark_task.OPS` maps the
op's name to it:

```
python -m reporting_platform.common.spark_task pending fo_trade
python -m scripts._spark_task pending fo_trade      # the old name, a shim
```

A driver image built without the owning component refuses the op by name.

## Status

**Done (step 1: boundaries):**
- `components.yml` and `tests/test_components.py`.
- Imports that pointed the wrong way were removed:
  - the inbox's Airflow client moved to `common/airflow_api.py`;
  - delivery-index reads moved to `registry/delivery_reads.py`;
  - the DuckDB connection moved to `common/duckdb_catalog.py`;
  - `_spark_task` moved into core.
- No behaviour change: the full `python -m tests.run` suite passes.

**Next:**
1. A `pyproject.toml` per component, built from these module lists. Then CI
   proves each wheel installs and imports on its own.
2. Images and wheels published to Nexus from CI, one version per component,
   and a per-environment release manifest naming the version of each.
3. Heavy work off the shared Airflow workers: Spark drivers and dbt run in
   versioned images through pods. Cosmos renders from a precompiled
   `manifest.json`, so dbt is not needed where DAG files are parsed.
4. The `export` component.
