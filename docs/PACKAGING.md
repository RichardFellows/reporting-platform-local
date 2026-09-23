# Packaging and deployment

How this one repo becomes several packages, each built, tested, versioned,
published to Nexus and deployed to an existing Airflow estate on its own.
The boundaries are enforced and each component builds as its own wheel. This
page says which parts exist and which do not yet.

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

## Building

```
python -m scripts.build_components                  # every packaged component -> dist/
python -m scripts.build_components ingest --check   # + clean-venv install-and-import
python -m scripts.build_components --version 1.4.0
```

The build stages exactly the files `components.yml` assigns to a component,
writes that component's `pyproject.toml` (hatchling), and runs `uv build`.
The pyproject comes from `components.yml`: module list, `depends_on` (as
sibling distributions), `requires`, `extras`, `scripts`. So there is one
source for all four.

Components share packages. `reporting_platform/registry/` is split between
`core` and `ingest`, so installing both wheels merges their files into one
directory. The config YAML is in no wheel. It is deployment content, read
from `REPORTING_CONFIG_DIR`.

`--check` does, for each component:
1. Creates a fresh Python 3.11 virtualenv outside the checkout.
2. Installs the wheel and its sibling wheels **by file**. It never installs
   them by name: a public index could answer `reporting-core` with someone
   else's package.
3. Installs the third-party `requires` and **not the extras**.
4. Imports every module the component owns.

A module that needs the host Airflow (the OpenLineage extractor) is reported
as skipped, not failed. `.github/workflows/components.yml` runs the check once
per component on every PR that touches code.

**Dependencies are ranges, not pins.** `Dockerfile.airflow` remains the
authority for exact versions in an image. A wheel states what it is
compatible with.
- **`requires`** is installed with the wheel. For core that is only `pyyaml`,
  `ruamel.yaml` and `requests`, so an Airflow worker parsing a DAG needs
  nothing heavier.
- **`extras`** (`reporting-core[spark,registry,s3]`) carry pyspark, duckdb,
  kubernetes and psycopg2 for the images that run them.
- **`host`** names what the Airflow estate itself provides. The wheels never
  install it: pulling Airflow in through a dependency would replace the
  estate's own.

**Sibling components are unpinned** (`reporting-core` with no version) in
this step. How tightly a component pins its siblings is the release
pipeline's decision (next step). Whatever it decides, publish to a Nexus
*hosted* repository with a name prefix nobody else can claim on the proxied
public index.

## Status

**Done (step 1: boundaries and wheels):**
- `components.yml` and `tests/test_components.py`. The tests cover import
  boundaries, the Spark ops table, third-party declarations, parent-package
  ownership, and agreement with the CI matrix.
- `scripts/build_components.py` and `.github/workflows/components.yml`. All
  four wheels install and import on their own.
- Imports that pointed the wrong way were removed:
  - the inbox's Airflow client moved to `common/airflow_api.py`;
  - delivery-index reads moved to `registry/delivery_reads.py`;
  - the DuckDB connection moved to `common/duckdb_catalog.py`;
  - `_spark_task` moved into core.
- No behaviour change: the full `python -m tests.run` suite passes.

**Next:**
1. DAG bundles: each component's `dags` as a versioned artifact beside its
   wheel, and the dbt project (with the feed config it must release with) as
   `reporting-dbt`.
2. Images and wheels published to Nexus from CI, one version per component,
   and a per-environment release manifest naming the version of each.
3. Heavy work off the shared Airflow workers: Spark drivers and dbt run in
   versioned images through pods. Cosmos renders from a precompiled
   `manifest.json`, so dbt is not needed where DAG files are parsed.
4. The `export` component.
