# Quick start

Clone to a published `reporting` layer, in nine commands.

This is the short path. [`README.md`](../README.md)'s numbered walkthrough is
the long one and explains *why* at each step — read that when you want to
understand the platform rather than start it.

**Budget about 40 minutes for a cold start**, almost all of it the ingest.
Measured end to end from `docker compose down -v` with every gitignored
artefact deleted: stack up **53s**, seed **2s**, land **6s**, ingest
**29min**, then the two builds a few minutes. The ingest is dominated by a
one-off ~200 MB Iceberg/Nessie/S3A jar resolution on the first Spark call and
by starting a fresh JVM per ten-file chunk. Add time on the very first run for
the Airflow image build, which was already cached when this was measured.

---

## 0. Prerequisites

**Docker Desktop with ~8 GB available to it.** That is the whole list.

You do **not** need:

| | |
|---|---|
| Python on the host | The seed generator runs inside a container; `seed/` is a bind mount, so the files land on your disk anyway. |
| `make` | Convenience wrapper only. Stock Windows does not have it, and nothing below uses it. |

**On Windows, use Git Bash for anything with single-quoted JSON in it.**
PowerShell mangles the quoting and the DAG `--conf` flags will not parse. Plain
`docker compose` commands are fine in either shell.

---

## 1. Start the stack

```bash
git clone <repo> reporting-platform && cd reporting-platform
cp .env.example .env
docker compose up -d --build
```

`.env` holds no real credentials — it is copied verbatim from
`.env.example`. Check everything came up:

```bash
docker compose ps
```

Twelve services are defined; **ten stay running**: `minio`, `postgres`,
`nessie`, `spark-master`, `spark-worker`, `airflow` (scheduler *and* task
execution), `airflow-webserver`, `airflow-triggerer`, `feed-ui` and
`watchdog`. (This page said eleven and nine until `feed-ui` was added and the
count was not updated with it.)

The other two are one-shot and **exiting `(0)` is the correct outcome** for
both:

- `minio-init` creates the buckets.
- `airflow-init` migrates the metadata DB, creates the admin user, **creates
  the `lakehouse_write` pool and runs `dbt deps`**. The last two used to be
  manual steps later in this guide; both are done for you now, because
  forgetting either broke the platform without saying so. Worth a glance the
  first time:

  ```bash
  docker compose logs airflow-init | tail -20
  ```

### Where everything lives

| Service | URL | Credentials |
|---|---|---|
| **Airflow** | http://localhost:8081 | `admin` / `admin` |
| **MinIO console** (object storage) | http://localhost:19001 | `minioadmin` / `minioadmin123` |
| **Spark master** | http://localhost:8080 | none |
| **Nessie** (catalog API) | http://localhost:19120/api/v2/config | none |

| Endpoint | Address | Credentials |
|---|---|---|
| MinIO S3 API | `localhost:19000` | `minioadmin` / `minioadmin123` |
| Postgres | `localhost:5432` | `platform` / `platform` |

**Every port above is overridable**, via `*_HOST_PORT` in `.env` —
`AIRFLOW_HOST_PORT`, `SPARK_UI_HOST_PORT`, `POSTGRES_HOST_PORT`,
`NESSIE_HOST_PORT`, `MINIO_API_HOST_PORT`, `MINIO_CONSOLE_HOST_PORT`,
`FEED_UI_HOST_PORT`, `SPARK_MASTER_HOST_PORT`. They are the **host side only**:
containers keep their own fixed ports and address each other by service name,
so nothing in the pipeline is affected by changing them.

MinIO already defaults off the usual 9000/9001, which collide with ZScaler on
a corporate laptop. The rest keep conventional defaults — but 8080 and 5432
are the usual suspects if a container refuses to start, or a URL answers with
something that isn't this stack.

Postgres holds five databases, deliberately separated: `airflow` (metadata),
`nessie` (the catalog's version store), `nessie_gc` (GC live-sets), `serving`
(the future BI export target) and `platform`.

Object storage starts with one bucket, `lakehouse`, containing
`landing/` (immutable arrival copies) and `warehouse/` (Iceberg data and
metadata).

All the S3 credentials come from `S3_ACCESS_KEY` / `S3_SECRET_KEY` in `.env`.
Change them there and everything follows.

---

## 2. Generate sample upstream data

```bash
docker compose exec -T airflow python /opt/platform/scripts/generate_feeds.py \
  --months 30 --end 2026-08-19 --out /opt/platform/seed --clean
```

Three things about that command:

- **`--end` is pinned, not defaulted.** It defaults to today, and every
  filename in the README's walkthrough derives from it.
- **30 months matters.** You cannot test "10 working days plus 80 month-ends"
  against three days of data.
- **`--clean` matters more, and it is the one decision in this guide.** The
  default seed *deliberately* injects two data-quality failures — an orphan
  counterparty reference and an unparseable notional. They make `dbt test`
  fail, which makes the build refuse to publish. That is the platform working
  correctly, and it is the more interesting demo — but it is not the demo you
  want on your first run, because nothing reaches `reporting`. `--clean` omits
  those two. Everything else awkward is kept: a re-delivered date, an absent
  counterparty day, a new upstream column appearing partway through.

Files appear in `seed/` on your host.

---

## 3. Land the files in object storage

```bash
docker compose exec -T airflow python -m scripts.land_feeds --source /opt/platform/seed
```

This copies the CSVs into `s3://lakehouse/landing/`, which is the
immutable evidence copy — the pipeline never reads your local disk again. You
can see them in the MinIO console.

---

## 4. Ingest into the `raw` layer

```bash
docker compose exec -T airflow python -m scripts.bulk_ingest
```

Safe to re-run: it only ingests files that are pending. **This is the slow
step on a cold stack** — the first Spark call resolves the Iceberg, Nessie and
S3A jars from Maven, and the whole load takes roughly half an hour. It batches
into subprocesses on purpose, so the JVM heap cannot accumulate across the
files.

Each file is ingested on its own Nessie branch and merged to `main` only when
the write succeeds.

**Fewer files are ingested than you landed, and that is correct.** Expect a
log line like:

```
trade: ignoring 14 landed object(s) outside the retention keep-set
(expired, not new): landing/trade/TRADE_20260716.csv, ...
```

Retention keeps "10 recent business days plus 80 month-ends". A landed file
whose business date is outside that set is treated as **expired rather than
new** — ingesting it would only be undone by the next retention run, and
re-ingesting it forever is a real bug this once had. So on a
cold load the middle of the dense tail is skipped by design, and
`bulk_ingest` afterwards reports **0 pending** even though `landing/` holds
more files than `raw` does. That is the system agreeing with itself, not data
loss.

From the seed generated above, expect roughly:

| Table | Files |
|---|---|
| `raw.trade` | 41 of 55 landed |
| `raw.counterparty` | 40 of 53 landed |
| `raw.rating` | 36 of 36 landed |
| `raw.primary_limits` | 40 of 53 landed |

157 files in total. (`primary_limits` was missing from this table until the
cold run above counted them.)

---

## 5. Build and publish `prepared` and `reporting`

**Unpause the DAGs.** Airflow pauses every DAG at creation and this repo does
not override that, so on a fresh clone they are all paused and no build will
ever fire. Ask Airflow what exists rather than naming them — the DAG set is
derived from `feeds.yml`, so a hard-coded list here goes stale the first time
anyone adds a feed. It already had: it named six, and `ingest_primary_limits`
made seven.

```bash
docker compose exec -T airflow airflow dags list -o plain | awk 'NR>1 {print $1}' |
  xargs -n1 docker compose exec -T airflow airflow dags unpause
```

The write pool and `dbt deps` were both manual steps here and are now run by
`airflow-init` — see step 1. Nothing else to set up, so trigger the build:

```bash
docker compose exec -T airflow airflow dags trigger prepared_build -r first_build
```

**You only trigger `prepared_build`.** It opens a Nessie branch, builds,
tests on the branch, and merges to `main` only if the tests pass. Merging
updates the `prepared` asset, and `reporting_build` starts **on its own**.
Watch:

```bash
docker compose exec -T airflow airflow dags list-runs -d prepared_build  -o plain
docker compose exec -T airflow airflow dags list-runs -d reporting_build -o plain
```

The evidence is in the run_id. Yours reads `first_build`; the one you did not
trigger reads **`dataset_triggered__…`**. That prefix is the whole per-feed
topology working — if you only ever see `manual__`, the cascade is not firing.

---

## 6. Confirm it published

Query the reporting layer. This is read-only and answers in about a second,
without starting Spark:

```bash
docker compose exec -T airflow python -m scripts.duckdb_console --tables

docker compose exec -T airflow python -m scripts.duckdb_console \
  "select business_date, count(*) as rows
   from lakehouse.reporting.exposure_change
   group by 1 order by 1 desc limit 5"
```

**Eleven** tables across `raw`, `prepared` and `reporting` means the whole
chain landed — four `raw`, four `prepared`, three `reporting`. (This page said
nine until the `primary_limits` feed added one to each of the first two
layers.)

Expected row counts after the build:

| Table | Rows |
|---|---|
| `prepared.trade` | 16,000 |
| `prepared.primary_limits` | 5,755 |
| `reporting.counterparty_exposure` | 2,400 |
| `reporting.exposure_change` | 2,400 |

Check the catalog:

```bash
curl -s http://localhost:19120/api/v2/trees
```

You should see **`main` and no `build/*` branches**. A surviving `build/*`
branch is a build whose tests failed — deliberately kept for inspection, with
`main` untouched.

**You will not see any `published/*` tags yet, and that is expected on this
path.** Those tags are cut by `record_publication`, a task in the *ingest
DAGs* — and step 4 deliberately used the `bulk_ingest` CLI instead, because it
is far quicker for a first load. Everything is published to `main`; nothing has
been *tagged* for time travel. To get tags, run an ingest through Airflow —
[`README.md`](../README.md) section 14 covers it — and see section 13 there for
time travel once a tag exists.

---

## Optional: the nightly housekeeping

Maintenance, retention, Nessie GC and the orphan sweep. **Dry run first — this
is the DAG that deletes things.** The `--conf` JSON needs Git Bash on Windows:

```bash
docker compose exec -T airflow airflow dags trigger \
  platform_housekeeping -r hk_dry -c '{"dry_run": true}'
```

And the out-of-band health check, which runs in its own container and depends
on no Airflow service:

```bash
docker compose logs --tail 20 watchdog
```

Exit code 0 is healthy; non-zero means at least one ALERT.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| Tasks stay `queued` forever | The `lakehouse_write` pool does not exist. `airflow-init` creates it — check `docker compose logs airflow-init`, then `make pools`. |
| Nothing happens when you land a file | The DAGs are paused. Airflow does that at creation; step 5 unpauses them. |
| `dbt found 1 package(s) specified in packages.yml, but only 0 installed` | `dbt deps` did not run. `airflow-init` does it; re-run with `make deps`. |
| `prepared_build` / `reporting_build` are missing, with an import error in the UI | Same cause. Cosmos renders those two DAGs by running `dbt ls`, which cannot compile a `dbt_utils` test without `dbt_packages/`, so the DAG file fails to import rather than the build failing later. `make deps`, then wait one parse interval. |
| One dbt model task is red and a `build/*` branch survived | Working as intended: `main` is untouched and the branch holds the exact bad data. `build/*` is swept after 120h, `ingest/*` after 48h. |
| The DAG `--conf` flag errors on Windows | PowerShell mangles single-quoted JSON. Use Git Bash. |
| `dbt_test` fails and nothing publishes | Expected if you generated **without** `--clean`: two data-quality failures are injected on purpose. The branch is kept for inspection and `main` is correctly untouched. |
| First Spark command takes many minutes | One-off Maven jar resolution. Cached under `~/.ivy2` in the container afterwards. |
| DAGs sit `queued` with `start=None` | A stale non-terminal run is wedging `max_active_runs=1` and the scheduler is spinning on it. Find and clear that run — it has been the cause every time. |
| `make: command not found` | It is optional and not needed by anything in this guide. |
| `bulk_ingest` reports `0 pending` but `landing/` has more files than `raw` | Correct — see step 4. Files outside the retention keep-set are expired, not new. |
| Ctrl-C on a long `docker compose exec` does not stop the work | **It does not.** The process keeps running inside the container. Check with `docker compose exec -T airflow ps -eo pid,etime,cmd \| grep bulk_ingest` before starting anything else, or you will run two ingests at once. |

**Do not use `airflow dags test`.** It creates a real DAG run, which blocks
the next one under `max_active_runs=1`; killing your local `docker compose
exec` does not kill the process inside the container; and deleting its rows
from the metadata DB corrupts the record rather than removing it. Use
`airflow dags trigger` with your own `-r` run id.

### Starting over

```bash
docker compose down -v      # destroys all data and volumes
```

---

## Where to go next

| | |
|---|---|
| [`README.md`](../README.md) | The same journey, step by step, with the reasoning. Section 14 covers the scheduler in more depth. |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Layer model, write-audit-publish, why Spark is the only build engine, how Cosmos renders the builds |
| [`ADDING-A-FEED.md`](ADDING-A-FEED.md) | Onboard a new feed — six files, no DAG edit |
| [`ADDING-A-MODEL.md`](ADDING-A-MODEL.md) | Add a dbt model — two files, no DAG edit |
| [`RETENTION.md`](RETENTION.md) | Why tag retention *is* data retention |
| [`MAINTENANCE.md`](MAINTENANCE.md) | The Iceberg procedures and their ordering |

---

*Executed end to end from `docker compose down -v` with every gitignored
artefact removed, on 2026-08-30 — stack up **53s**, seed **2s**, land **6s**,
ingest **28m56s** (157 files), `prepared_build` **3m18s** and the
asset-triggered `reporting_build` **2m56s**, both green, eleven tables
queryable at the documented row counts. Then a single new delivery landed and
run through `ingest_trade`: it cut `published/2026-08-20/demo2`, and
`prepared_build` and `reporting_build` each fired themselves off the asset
below them, ending with `2026-08-20` in `reporting.exposure_change` and the
catalog holding `main` and that one tag — no surviving `build/*` branch. The
nightly housekeeping DAG was also run as a dry run (5/5 tasks green) and the
watchdog exits 0.*

*Corrections that run forced into this page: the service count (eleven/nine →
twelve/ten, stale since `feed-ui` was added), the table count (nine → eleven,
stale since the `primary_limits` feed), a hard-coded six-DAG unpause list that
had never included `ingest_primary_limits`, and the manual pool/`dbt deps`
steps, which are now done by `airflow-init` because forgetting either failed
silently.*