# The pipeline without Airflow or a Spark cluster

The platform is four steps. Each is its own command, owned by the component
that ships it, and each runs the **same code the Airflow DAGs run**. Airflow
adds triggers, retries, one task per model and a UI around the steps. It does
not add any step. This page is about running the steps without it.

| Step | Command | Component | What it does | Airflow equivalent |
|---|---|---|---|---|
| land | `python -m reporting_platform.ingest land FILE...` | `ingest` | Runs the inbox gate over the files named: route, conform, promote control files, quarantine what cannot be named. Triggers nothing. | the `inbox` watcher |
| ingest | `python -m reporting_platform.ingest ingest FEED...` | `ingest` | Takes every pending delivery of each feed into raw, each on its own Nessie branch and merged on success. Cuts a `snapshot/` tag per ingest. | `ingest_<feed>` |
| *or* transport | `python -m reporting_platform.ingest transport MARKER...` / `--pending` | `ingest` | Takes completed Transports from `received/` into raw: validate, deliver, normalize, ingest. Replaces land + ingest for a feed DCM delivers. | `transport_ingest` (+ `transport_reconcile` for `--pending`) |
| prepared | `python -m reporting_platform.transform build prepared` | `dbt` | Opens a build branch, runs `dbt build` on it, then publishes, or fails and keeps the branch. | `prepared_build` |
| reporting | `python -m reporting_platform.transform build reporting` | `dbt` | The same for the marts, plus one `published/<report>/…` tag and version per report. | `reporting_build` |

`python -m reporting_platform.pipeline run FILE...` runs all four in order and
stops at the first that fails. It is glue: it contains no step of its own.

**Files, or Transports.** `land` + `ingest` is the inbox way in. The other
is a Transport that DCM has already completed under `received/`
([`TRANSPORT-CONTRACT.md`](TRANSPORT-CONTRACT.md)). `python -m
reporting_platform.ingest transport` carries it to raw with the four steps
the `transport_ingest` DAG runs, and `pipeline run --transport` builds on top.
Both ways stay: a feed with no DCM mapping can only arrive through the inbox.
See [Transports instead of files](#transports-instead-of-files).

## Where the code is shared

| What | Module | Called by |
|---|---|---|
| Open branch + run record, publish, fail | `transform/wap.py` | `dbt_builds.py` tasks, `transform` CLI |
| Archive a dbt invocation's evidence | `transform/dbt.py:archive_invocation` | the Cosmos callback, `transform dbt` |
| Snapshot tag, drift report | `ingest/steps.py` | `feed_ingest.py` tasks, `ingest` CLI |
| A Transport to raw: validate, deliver, normalize, ingest, with receipts and evidence | `ingest/transport_steps.py` | `transport_ingest.py` tasks, `ingest transport` |
| What Transports are pending | `transport_steps.pending` (the `transport_reconcile` walk) | `ingest transport --pending`, `pipeline run --transport` |
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

## Transports instead of files

A Transport needs nothing landed: the producer has already completed it
under `received/`. The runner takes it from there.

```bash
# make one to take in (what DCM runs; the runner's own S3 settings apply)
docker compose --profile standalone run --rm runner transport publish \
    --legacy-feed-id qa-happy-position --producer-run-id run-1 \
    --cob-date 2026-09-14 --source-system QA \
    --source-observed-at 2026-09-24T16:40:00Z \
    --data /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.csv \
    --control /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.ctl

# what is pending, ingest it, or the whole pipeline over it
docker compose --profile standalone run --rm runner ingest transport --pending --window 7 --list
docker compose --profile standalone run --rm runner ingest transport --pending --window 7
docker compose --profile standalone run --rm runner ingest transport received/cob_date=.../_COMPLETE.json
docker compose --profile standalone run --rm runner run --transport --window 7
```

**Pending means what `transport_reconcile` means**: every completed
Transport whose Delivery is not in raw, over the last `--window` COB dates
(default 7, the DAG's), `--cob-date D` (repeatable), or `--all` (all of
`received/`, v1 markers included). A marker the walk could not read is
reported as `unreadable` and exits 1. It is never counted as nothing pending.

**A refused Transport is still pending, every time.** A wrong checksum, a
missing control file or an identity conflict never reaches raw, so the next
`--pending` tries it again and refuses it again. Airflow triggers each
Transport once per run id. Here you see the refusal on every run until the
Transport is corrected or falls out of the window.

**Refused is not failed.** `pipeline run --transport` treats a refused
Transport as `run` treats a quarantined file. It never reached raw, so the
builds go ahead on the ones that did, and the exit is 1. Any other failure
(a store that was down, a Spark error) stops before the builds, as a failed
file ingest does.

**Each run writes the same evidence as the DAG.** The step records its
`registry.transport_receipt` row (with no `airflow_*` values) and its
`registry.validation_result` rows, so the COB Status page shows a Transport
the runner took exactly as one Airflow took.

**A snapshot tag per ingest, as on the file path.** After `ingest_raw`,
`ingest_transport` runs `steps.after_ingest`: the drift report, then
`snapshot/<feed>/<bd>/<run>` at the commit the ingest's merge made. It's the
same function the file path and both DAGs use
(`docs/DECISIONS.md#a-snapshot-tag-names-its-merge-commit`). A Transport raw
already held is not tagged again.

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
- **Transports**, on the development stack with `transport_watch` and
  `transport_reconcile` paused. `runner transport publish` completed one
  Transport, and `ingest transport --pending --cob-date 2026-09-14 --list`
  listed it with the five refused test Transports already in that partition.
  `runner run --transport --cob-date 2026-09-14` ingested it (3 rows), refused
  the five again (four at `ingest_raw` on md5, one at `deliver` for a missing
  control file), and started the prepared build on top. The build failed on
  `TABLE_OR_VIEW_NOT_FOUND` for four raw tables that stack has never had,
  exactly as its Airflow `prepared_build` had since 21 Sep. The build then
  kept its branch, left `main` unmoved and exited 1: 1m45s. Naming the ingested
  Transport again returned `already_ingested`, exit 0. The refactored
  `transport_ingest` DAG, same session: a good Transport merged and a wrong
  md5 failed once as `SparkTaskRefused`, receipts and FAIL row as before. **A
  clean build on top of a Transport has not been run here**, because the
  build tier could not pull MinIO's image on 2026-09-24.

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
