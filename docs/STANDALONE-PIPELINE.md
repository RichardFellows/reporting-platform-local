# The pipeline without Airflow or a Spark cluster

The platform is four steps. Each is its own command, owned by the component
that ships it, and each runs the **same code the Airflow DAGs run**. Airflow
adds triggers, retries, one task per model and a UI around the steps. It does
not add any step. This page is about running the steps without it.

| Step | Command | Component | What it does | Airflow equivalent |
|---|---|---|---|---|
| land | `python -m reporting_platform.ingest land FILE...` | `ingest` | Runs the inbox gate over the files named: route, conform, promote control files, quarantine what cannot be named. Triggers nothing. | the `inbox` watcher |
| ingest | `python -m reporting_platform.ingest ingest FEED...` | `ingest` | Takes every pending delivery of each feed into raw, each on its own Nessie branch and merged on success. Cuts a `snapshot/` tag per ingest. | `ingest_<feed>` |
| prepared | `python -m reporting_platform.transform build prepared` | `dbt` | Opens a build branch, runs `dbt build` on it, then publishes, or fails and keeps the branch. | `prepared_build` |
| reporting | `python -m reporting_platform.transform build reporting` | `dbt` | The same for the marts, plus one `published/<report>/…` tag and version per report. | `reporting_build` |

`python -m reporting_platform.pipeline run FILE...` runs all four in order and
stops at the first that fails. It is glue: it contains no step of its own.

## Where the code is shared

| What | Module | Called by |
|---|---|---|
| Open branch + run record, publish, fail | `transform/wap.py` | `dbt_builds.py` tasks, `transform` CLI |
| Archive a dbt invocation's evidence | `transform/dbt.py:archive_invocation` | the Cosmos callback, `transform dbt` |
| Snapshot tag, drift report | `ingest/steps.py` | `feed_ingest.py` tasks, `ingest` CLI |
| The inbox gate | `ingest/inbox.py:sweep` | the watcher, `ingest land` (`land_files`) |
| Any Spark work | `common/spark_task.run` + an op | every DAG, every CLI |

`tests/test_transform.py` fails if `dbt_builds.py` grows its own merge,
lifecycle check, tag or version allocation again.

## Spark with no cluster: `PLATFORM_EXECUTION=embedded`

`embedded` runs the driver *and* every task in one process, with a
`local[N]` master. It is a **declared mode**, not a fallback. `local` and
`kubernetes` still refuse a `local` master, because in those modes an
in-process session means a job silently skipped the cluster it was deployed
against. `embedded` refuses a cluster master. `SPARK_MASTER` is still the
only master setting, and both drivers read it: `spark_session()` and
`dbt/profiles.yml`.

## Running it: the `runner` service

The runner is the Airflow image with its entrypoint set to
`reporting_platform.pipeline`. It sits behind a compose profile, so `docker
compose up` never starts it. It needs only MinIO (or S3), Nessie and Postgres:

```bash
docker compose --profile standalone run --rm runner check    # resolve + reach every store
docker compose --profile standalone run --rm runner setup    # registry schema, dbt deps

# the whole pipeline over some files (the path is inside the container)
docker compose --profile standalone run --rm runner run \
    /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.csv \
    /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.ctl

# or one step at a time
docker compose --profile standalone run --rm runner ingest land /opt/platform/seed_clean/fo_trade/*.csv
docker compose --profile standalone run --rm runner ingest ingest fo_trade
docker compose --profile standalone run --rm runner transform build prepared --no-publish
docker compose --profile standalone run --rm runner transform publish build/prepared/2026-09-24/cli-...
```

Files from anywhere on the host: add `-v "$PWD/drop:/drop:ro"` and name
`/drop/...`.

**Don't `--entrypoint` around it.** The image's entrypoint is what makes the
host uid a real user with a home directory. Without it, Java resolves
`user.home` to `?` and Spark dies at start-up with `basedir must be absolute:
?/.ivy2/local`. Every step is reachable through the runner's own entrypoint
(`runner ingest ...`, `runner transform ...`), so there is no reason to
override it.

### One transform, step by step

`transform build` is these four commands, and each can be run separately:

```bash
branch=$(python -m reporting_platform.transform open prepared)    # prints only the branch
python -m reporting_platform.transform dbt "$branch"              # build + test ON the branch
python -m reporting_platform.transform publish "$branch"          # or: fail "$branch"
```

`publish` does not accept the caller's word that the build passed. It reads
the newest archived `run_results.json` for the branch and refuses unless every
node is `success`, `pass` or `warn`. A `skipped` node was downstream of a
failure and was never built. Every command prints a single JSON result on
stdout; dbt's own output goes to stderr.

## Pointing it at deployed infrastructure

Only the environment changes. The runner reads each store from a
`RUNNER_*` variable (in the shell or `.env`) and falls back to this compose
file's hosts:

| Variable | Sets | Default |
|---|---|---|
| `RUNNER_S3_ENDPOINT` | `S3_ENDPOINT`. `https://` turns TLS on by itself | `http://minio:9000` |
| `RUNNER_S3_ACCESS_KEY` / `RUNNER_S3_SECRET_KEY` | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | the MinIO pair |
| `RUNNER_WAREHOUSE` / `RUNNER_LANDING` | `REPORTING_WAREHOUSE` / `REPORTING_LANDING` | `s3a://lakehouse/...` |
| `RUNNER_NESSIE_URI` | `NESSIE_URI` | `http://nessie:19120/api/v2` |
| `RUNNER_NESSIE_AUTH_TYPE` / `RUNNER_NESSIE_AUTH_TOKEN` | Nessie auth (`NONE` or `BEARER`) | `NONE` |
| `RUNNER_REGISTRY_DSN` | `REGISTRY_DSN` | the compose Postgres |
| `RUNNER_REPORTING_ENV` | `REPORTING_ENV` | `local` |
| `RUNNER_SPARK_MASTER` | `SPARK_MASTER` | `local[2]` |

```bash
RUNNER_S3_ENDPOINT=https://s3.internal RUNNER_NESSIE_URI=https://nessie.internal/api/v2 \
RUNNER_REGISTRY_DSN=postgresql://... RUNNER_REPORTING_ENV=dev \
  docker compose --profile standalone run --rm --no-deps runner check
```

`--no-deps` keeps the local stores from starting. Outside `local`, an endpoint
left unset is refused by name rather than falling back to a compose host
(`#settings-refuse-outside-local`). `check` shows what was resolved and
whether each store could be reached. A store it could not reach is reported
as `unreachable`, never as empty.

## What has been run

On a cold, isolated compose project (`-p rp-pipe`, the build tier's
no-host-ports override) holding only MinIO, Nessie and Postgres:

- `pipeline run` over a one-month `--clean` seed plus the two `qa_` fixtures:
  126 files landed, 43 deliveries ingested (six feeds, 10 per Spark session,
  about 16s each), prepared 59/59, reporting 26 pass + 1 warn, both reports
  tagged `published/…` as v1. **3m46s end to end**, including generating the
  seed.
- The step commands on a delivery with an orphan counterparty: `transform
  dbt` exits 1 on the `relationships` test, `transform publish` refuses and
  names it, and `transform fail` closes the run `failed` with the branch
  kept and `main` unmoved.
- The refactored `prepared_build` DAG on the same stack, now with Airflow and
  the Spark cluster added: every task green through `wap.publish`, and the
  run record carries its `dag_id`, `airflow_run_id` and `change_ref`.

## What it does not do

- **No `lakehouse_write` pool.** In Airflow, one pool slot serialises every
  writer. Here, each step writes when it runs. Don't point a runner at the
  same catalog as a live Airflow and run both at once. Nessie merges are
  optimistic, so a collision fails a merge rather than corrupting data, but
  one of the two builds then has to be re-run.
- **No trigger.** Nothing ingests a file because it arrived, and nothing
  builds because raw changed. You say when.
- **No housekeeping**, retention or maintenance. Those are
  `platform_housekeeping`, and their own CLIs (`CLAUDE.md`'s quick reference)
  still work.
- **No OpenLineage.** Lineage is emitted by the Airflow listener.
- **One dbt invocation, not one task per model.** The evidence is archived
  under task id `dbt.build`, attempt N (the next free number, so rebuilding
  a branch never overwrites evidence).

## Running on the host, without a container

It should work, but it has not been run. The container has everything the
host would need:
Java 17, `pyspark` 3.5, `dbt-spark[session]`, the dbt packages (`setup`), and
the five driver jars `Dockerfile.airflow` bakes, listed in
`PLATFORM_DRIVER_JARS` at the versions `.env` pins. Set
`PLATFORM_EXECUTION=embedded`, `SPARK_MASTER=local[2]`, the store variables
above without the `RUNNER_` prefix, and `REPORTING_CONFIG_DIR` /
`DBT_PROJECT_DIR` to this checkout. On Windows, Spark local mode also needs
`HADOOP_HOME` with `winutils.exe`.
