# CLAUDE.md

Orientation for working on this repo with an AI assistant.

Read order: **this file** → `docs/QUICKSTART.md` (get it running) →
`docs/ARCHITECTURE.md` (why it is shaped this way).

## What this is

A laptop-runnable local approximation of a lakehouse platform: MinIO (S3),
Nessie (Iceberg catalog with git-like branching), Postgres, Spark, Airflow and
dbt, replacing a legacy ETL / RDBMS / scheduler chain.

Data flows: daily CSVs land immutably in object storage → `raw` (1:1, all
strings) → `prepared` (conformed, typed, deduplicated) → `reporting` (marts).
Every build happens on a Nessie branch and merges to `main` only if its tests
pass — write-audit-publish.

## The one habit that matters

**Verify against the live stack. Get the actual error text before proposing a
fix.** Docstrings, comments and docs in a system like this drift from
behaviour; a claim is worth what its last execution proved. A subsystem
routinely looks fine until the first time it runs in a *new* configuration, so
if you are about to run something for the first time, expect it to fail and
read what it actually says.

Two corollaries worth holding on to:

- **A guard written against a documented mechanism rather than the working one
  can only ever produce false alarms.** Before trusting an assertion, check
  that the thing it reads can actually be set.
- **A check whose window does not contain the thing it describes** will either
  never fire or never stop. Match the window to the cadence of whatever clears
  it.

## Environment

- Docker Desktop. An agent session can drive the stack directly — `docker
  compose`, `curl`, everything.
- **Use PowerShell (or `MSYS_NO_PATHCONV=1` with bash) for `docker compose
  exec`** on Windows, or Git Bash rewrites container paths like
  `/opt/platform/...` and the exec fails.
- **Airflow is 2.10.5 deliberately.** Under Airflow 3 no DAG run here could
  complete: tasks ran, logged, returned values and pushed xcom, and the
  scheduler never recorded them. Airflow 2's LocalExecutor writes the result
  straight to the metadata DB. Read `Dockerfile.airflow`'s header before
  changing it.
- **Compose builds a separate image per service.** `docker compose build
  airflow` does NOT rebuild `airflow-init`; build them together.
- Editing `.env` or `docker-compose.yml` does nothing until the container is
  **recreated**.
- A DAG run left in a non-terminal state makes the scheduler spin on it and
  starve every other run. If DAGs sit `queued` with `start=None`, look for a
  stale run first.
- **Do not use `airflow dags test`.** It creates a real run that blocks the
  next under `max_active_runs=1`; killing the local `docker compose exec` pipe
  does not kill the process inside the container; and deleting its rows from
  the metadata DB corrupts the record rather than removing it. Use `airflow
  dags trigger` with a distinct `-r` run id and wait on **that run id**.

## How to work on this

- Write-audit-publish is the safety net: branch → build → test → merge only if
  clean. Don't build against `main` — `scripts/_open_build_branch.py` opens a
  throwaway branch.
- **Spark is the only build engine**, and `dbt_builds.py` refuses a non-Spark
  `DBT_TARGET`. A build must land on a Nessie branch and only the Spark path
  can address one. DuckDB is a read-only query tool here
  (`scripts/duckdb_console.py`).
- `dbt/macros/engine.sql` keeps engine-specific SQL constructs in one place —
  but **fixing a macro proves nothing about models that don't call it**. Grep
  for the construct, not the macro.
- `dbt/macros/naming.sql` overrides `generate_schema_name` so layers land in
  `prepared`/`reporting` rather than dbt's default concatenation. Don't remove
  it without moving every table reference.
- **Anything running Spark inside an Airflow task must go through
  `scripts/_spark_task.py`** (a subprocess), or the JVM keeps the task process
  alive, heartbeats stop, and the scheduler zombie-reaps it. This is still
  true on the cluster — the *driver* is what lives in that process.
- **Every Spark job runs on the `spark-master`/`spark-worker` cluster, never
  `local[*]`.** The master comes from `SPARK_MASTER` in two places that must
  not diverge: `spark_session()` in `common/context.py` and `spark.master` in
  `dbt/profiles.yml`. `spark_session()` refuses a `local` master rather than
  quietly running the pipeline in the Airflow container with the cluster idle.
  Each app caps itself at 2 cores/2g so one job cannot hold the whole worker —
  standalone mode otherwise grants every free core until the session stops, and
  the next job waits forever instead of failing. Watch it at
  <http://localhost:8080>. See `docs/ARCHITECTURE.md` § *Where Spark actually
  runs* for the jar-shipping and Python-version constraints.
- **The Iceberg and Nessie jar versions live in `.env`, and there are THREE of
  them.** `ICEBERG_VERSION` has to be identical in the Spark image (baked into
  `/opt/spark/jars`) and in *both* drivers — `spark_session()` in
  `common/context.py` and `spark.jars.packages` in `dbt/profiles.yml` — because
  every process that submits work runs a pip-installed pyspark with no jars of
  its own, and `spark.jars.packages` ships the driver's jars to every executor.
  Diverge and you get two Iceberg versions in one application, surfacing as
  `NoSuchMethodError` on the first write rather than as anything saying
  "version". `NESSIE_SPARK_EXT_VERSION` tracks **Iceberg, not the server** — the
  extensions jar is compiled against a specific Iceberg (0.103.3 against 1.8.1,
  0.108.1 against 1.11.0) and running one built against a *newer* Iceberg than
  you have is the failing direction. `NESSIE_SERVER_VERSION` sets the server
  image and the `nessie-gc` jar, which must be equal to each other, and is
  allowed to be newer than the extensions. `.env.example` has the reasoning and
  a known-good alternative set. `docker compose exec spark-worker env | grep
  VERSION` says what is actually baked into the image you are running.
- **The dbt build DAGs are rendered by Astronomer Cosmos**, one Airflow task
  per model, derived from the dbt project on every parse — so **adding a model
  needs no DAG edit either** — `docs/ADDING-A-MODEL.md` has the two files it
  does touch. Four settings in `dbt_builds.py` are load-bearing
  and the docstring says why each one is there: `InvocationMode.SUBPROCESS`
  (a `method: session` target builds a JVM in-process and the task gets
  zombie-reaped), the `lakehouse_write` pool on *every* rendered task (one dbt
  invocation is one Spark app; standalone mode holds cores until the session
  stops), `LoadMode.DBT_LS` (Cosmos's own CUSTOM parser double-emits every test
  and misses model-level ones), and `TestBehavior.AFTER_ALL` (AFTER_EACH is 51
  JVM starts; BUILD drops or misorders cross-model `relationships` tests).
- **`astronomer-cosmos` is installed `--no-deps`, and that is not an
  optimisation.** Installing it under Airflow's constraint file pins
  `typing_extensions==4.12.2`, dbt's `mashumaro` needs `evaluate_forward_ref`
  from 4.13+, and **every dbt invocation then dies at import** — in dbt, not in
  cosmos, and not until something runs dbt. `Dockerfile.airflow` runs
  `dbt --version` as a build-time smoke check so that cannot ship silently
  again. Re-run `pip install --dry-run` before moving `COSMOS_VERSION`.
- **`airflow-init` now does four things, not two**: db migrate, admin user,
  `airflow pools set lakehouse_write 1`, and `dbt deps`. The last two were
  manual steps that broke the platform silently when skipped — and `dbt deps`
  is no longer optional at all, because Cosmos renders the build DAGs by
  running `dbt ls`, which cannot compile a `dbt_utils` test without the
  package. No installed packages now means those two DAGs do not *import*.
- **dbt's three working directories all live under `/opt/platform/run`, not in
  the `./dbt` bind mount** — `DBT_LOG_PATH` and `DBT_TARGET_PATH` in
  `docker-compose.yml`, `packages-install-path` in `dbt_project.yml`. A bind
  mount takes its ownership from the host, so no `chown` in the image can reach
  it and `dbt deps` fails with `Permission denied` wherever the checkout is not
  owned by uid 50000. Packages are the awkward one and the comments in those
  files say why: they must be **shared** between `airflow-init` and everything
  that reads them, so they cannot be a plain image path (copy-on-write per
  container — the init container's install is discarded when it exits), and the
  named volume that shares them has to be mounted one level **above**
  `dbt_packages`, because `dbt deps` rmtree's that directory and a mount point
  cannot be removed. If packages ever come back missing or root-owned, remove
  the volume — rebuilding the image will not re-seed one that already exists.
- **A control file ABORTS the ingest, it does not fail a test.** "Is this the
  file the sender sent" is a different question from "is this value right", and
  the second is what prepared's tests are for. The delivery is not pending
  until its control file lands — otherwise every feed whose sender writes the
  sidecar second fails spuriously. See
  `docs/DECISIONS.md#control-files-abort-the-ingest`.
- **A feed is named `<source_system>_<feed>`** — `fo_trade`,
  `ref_counterparty`, `treasury_margin_call` — and it is TYPED into feeds.yml,
  not derived. That one string is the raw table, the DAG id, the landing
  prefix, the dbt source table and the prepared model at once, so prefixing the
  name prefixes all five and none of them can drift.
  See `docs/DECISIONS.md#feed-names-carry-the-source`.
- **A column may be named differently in the file than in the platform.**
  `- trade_id: "Trade Id"` in `feeds.yml` renames at ingest, so raw onwards is
  ordinary identifiers and dbt macros never have to quote one. Drift is
  reported in the file's names. See `docs/DECISIONS.md#source-column-names`,
  and `#identifiers-in-macros` for which macros quote and which must not.
- **Adding a feed is five files and no DAG edit** — `docs/ADDING-A-FEED.md`
  has them in order. `generate_feeds.py` is not one of them: it hand-writes
  generators for the four original feeds, whose pathologies are the point, and
  generates every *other* feed in `feeds.yml` from its definition via the
  console's `ui/sampledata.py`. Pass `types=` when calling that directly — from
  the column name alone it re-guesses, and a `decimal` column gets a string
  that `safe_cast` silently nulls. Nothing in it fails silently any more:
  the prepared and reporting tables that maintenance and retention cover are
  derived from the dbt project directory, so the model file IS the
  registration.
- **The feed console (`reporting_platform/ui`, <http://localhost:8082>) writes
  those five files from a form** and drives land → ingest → build. It is a
  front end for the doc above, not a second source of truth: it round-trips
  `feeds.yml` with ruamel so the comments survive, and the change it makes is
  an ordinary reviewable diff, checked with `dbt parse` (~5s, no Spark) so a
  scaffolded model that dbt cannot read is caught then rather than in the
  build. It can also generate a delivery from the definition and run `dbt
  build` for ONE feed on a throwaway branch — **that path never merges**, on
  purpose: publication belongs to the Airflow builds, not to a button labelled
  "test". See `docs/FEED-UI.md`. Because it edits config
  a running Airflow is reading, `feeds()` and `_load()` in `common/context.py`
  are cached on the file's **mtime** — do not put a plain `@lru_cache` back on
  them or a new feed will never reach the DAG processor.
- Retention and GC delete data. `dry_run` first, always. GC defers its deletes
  by design; the deferred-delete pass is the deliberate second step.

## Quick reference

```powershell
# bulk ingest everything pending (safe to re-run)
docker compose exec airflow python -m scripts.bulk_ingest

# build + test both layers on a throwaway branch
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt --target spark_local --select path:models/prepared path:models/reporting --vars "{nessie_ref: $branch}"

# retention / maintenance -- dry run first, --all-managed covers every table
docker compose exec -T airflow python -m reporting_platform.retention.retention --all-managed --dry-run
docker compose exec -T airflow python -m reporting_platform.maintenance.maintain --all-managed --dry-run

# DAGs
docker compose exec -T airflow airflow dags list-runs -d prepared_build -o plain
docker compose exec -T airflow airflow dags trigger ingest_fo_trade

# what Cosmos rendered -- one *_run task per dbt model, plus dbt_test
docker compose exec -T airflow airflow tasks list prepared_build

# unpause everything (derive the list; a written-down one goes stale per feed)
docker compose exec -T airflow airflow dags list -o plain | ForEach-Object {
  ($_ -split '\s+')[0] } | Select-Object -Skip 1 | ForEach-Object {
  docker compose exec -T airflow airflow dags unpause $_ }

# housekeeping -- the conf JSON needs bash, PowerShell mangles the quoting
#   MSYS_NO_PATHCONV=1 docker compose exec -T airflow airflow dags trigger \
#     platform_housekeeping -r run1 -c '{"dry_run": true}'

# out-of-band health check (its own container, no Airflow dependency)
docker compose logs --tail 20 watchdog

# drop a file in ./inbox and it lands, ingests and moves to .processed/<feed>/
docker compose up -d inbox
docker compose exec -T inbox python -m reporting_platform.ingest.inbox --dry-run

# feed console -- add/edit a feed, land it, ingest it, watch the builds
docker compose up -d feed-ui     # http://localhost:8082

# read-only query console against published `main`
docker compose exec -T airflow python -m scripts.duckdb_console --tables

# refs (add ?fetch=ALL for commit metadata)
curl -s http://localhost:19120/api/v2/trees
```

A clean seed that passes its tests — needed for anything that publishes —
comes from `generate_feeds.py --clean`. The default seed injects two
data-quality failures on purpose, so a build against it correctly refuses to
publish.
