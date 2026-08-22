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
  alive, heartbeats stop, and the scheduler zombie-reaps it.
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
docker compose exec -T airflow airflow dags trigger ingest_trade

# housekeeping -- the conf JSON needs bash, PowerShell mangles the quoting
#   MSYS_NO_PATHCONV=1 docker compose exec -T airflow airflow dags trigger \
#     platform_housekeeping -r run1 -c '{"dry_run": true}'

# out-of-band health check (its own container, no Airflow dependency)
docker compose logs --tail 20 watchdog

# read-only query console against published `main`
docker compose exec -T airflow python -m scripts.duckdb_console --tables

# refs (add ?fetch=ALL for commit metadata)
curl -s http://localhost:19120/api/v2/trees
```

A clean seed that passes its tests — needed for anything that publishes —
comes from `generate_feeds.py --clean`. The default seed injects two
data-quality failures on purpose, so a build against it correctly refuses to
publish.
