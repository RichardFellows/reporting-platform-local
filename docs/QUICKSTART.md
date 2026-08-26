# Quick start

Clone to a published `reporting` layer, in nine commands.

This is the short path. [`README.md`](../README.md)'s numbered walkthrough is
the long one and explains *why* at each step — read that when you want to
understand the platform rather than start it.

**Budget an hour for a cold start**, almost all of it downloads. Measured on a
`docker compose down -v` rebuild: stack up in **42s**, seed **2s**, land
**4s**, and the ingest about **half an hour** — that one is dominated by a
one-off ~200 MB Iceberg/Nessie/S3A jar resolution on the first Spark call.
Builds are a couple of minutes. Add time on the very first run for the Airflow
image build, which was already cached when this was measured.

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

Eleven services are defined; **nine stay running**: `minio`, `postgres`,
`nessie`, `spark-master`, `spark-worker`, `airflow` (scheduler *and* task
execution), `airflow-webserver`, `airflow-triggerer` and `watchdog`.

The other two are one-shot and **exiting is the correct outcome** for both:
`minio-init` creates the buckets, and `airflow-init` migrates the metadata DB
and creates the admin user.

### Where everything lives

| Service | URL | Credentials |
|---|---|---|
| **Airflow** | http://localhost:8081 | `admin` / `admin` |
| **MinIO console** (object storage) | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **Spark master** | http://localhost:8080 | none |
| **Nessie** (catalog API) | http://localhost:19120/api/v2/config | none |

| Endpoint | Address | Credentials |
|---|---|---|
| MinIO S3 API | `localhost:9000` | `minioadmin` / `minioadmin123` |
| Postgres | `localhost:5432` | `platform` / `platform` |

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

| Table | Rows | Files |
|---|---|---|
| `raw.trade` | 16,400 | 41 of 55 landed |
| `raw.counterparty` | 2,400 | 40 of 53 landed |
| `raw.rating` | 5,500 | 36 of 36 landed |

---

## 5. Build and publish `prepared` and `reporting`

Two things are needed before Airflow will run anything.

**The write pool** — without it every task sits queued forever:

```bash
docker compose exec -T airflow airflow pools set lakehouse_write 1 \
  "serialise all Iceberg writers, incl. maintenance"
```

**Unpause the DAGs.** Airflow pauses every DAG at creation and this repo does
not override that, so on a fresh clone all six are paused and no build will
ever fire:

```bash
for d in ingest_trade ingest_counterparty ingest_rating \
         prepared_build reporting_build platform_housekeeping; do
  docker compose exec -T airflow airflow dags unpause $d
done
```

Now install the dbt package dependency and trigger a build. `dbt deps` is
mandatory — `dbt/dbt_packages/` is not committed and `packages.yml` requires
`dbt_utils`:

```bash
docker compose exec -T airflow dbt deps \
  --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt

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

Nine tables across `raw`, `prepared` and `reporting` means the whole chain
landed.

Expected row counts after the build:

| Table | Rows |
|---|---|
| `prepared.trade` | 16,000 |
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
| Tasks stay `queued` forever | The `lakehouse_write` pool does not exist — step 5. |
| Nothing happens when you land a file | The DAGs are paused. Airflow does that at creation; step 5 unpauses them. |
| `dbt found 1 package(s) specified in packages.yml, but only 0 installed` | `dbt deps` — step 5. |
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
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Layer model, write-audit-publish, why Spark is the only build engine |
| [`RETENTION.md`](RETENTION.md) | Why tag retention *is* data retention |
| [`MAINTENANCE.md`](MAINTENANCE.md) | The Iceberg procedures and their ordering |

---

*Executed end to end from `docker compose down -v` with every gitignored
artefact removed, on 2026-08-22 — stack up in 42s, seed in 2s, land in 4s,
ingest ~30min cold, build and asset-triggered reporting build both green,
nine tables queryable. Two errors in an earlier draft of this page were found
by that run and corrected: it claimed `published/*` tags would exist (they are
cut by the ingest DAG, which this path bypasses) and it did not explain why
fewer files are ingested than landed.*