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
  **recreated**. Neither does editing a module a LONG-RUNNING process already
  imported: `feeds()` re-reads config on mtime, which covers a feed being
  added, but the `inbox` watcher holds the code it imported at start. Restart
  it after touching `ingest/` or `common/`, or it reports config as broken
  that is valid on disk.
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
- **`landing/` has a contract: everything in it is correctly named and
  classified.** Two ways in — an approved sender that adheres to the contract
  writes there directly, and everything else goes through the **inbox
  conformance gate** (`ingest/conform.py`, driven by `ingest/inbox.py`). A feed
  with an **`arrival:`** block is the second kind.
  **THE INBOX ESTABLISHES IDENTITY; INGESTION VERIFIES INTEGRITY**, and the
  keys are split to enforce it: `arrival.control` carries `business_date` and
  `version` (what the file must be NAMED) and rejects `row_count`/`md5` at
  load; `delivery.control` carries `row_count` and `md5`, is read in landing
  and checked at ingest — once, for every delivery however it arrived, so the
  trusted path is never the less-verified one.
  **The control file is PROMOTED, not consumed**: the delivery is the data
  file and its control file together, so the gate renames both into landing
  and `delivery.control` reads it there. The two blocks are therefore
  *complementary* — a legacy feed needs both, and an `arrival.control` with no
  `delivery.control` is rejected. Such a feed has **two filenames and they are
  different strings**: `Feed.claims_source` for the upstream's,
  `Feed.parse_filename` for landing's; the rename is built FROM
  `filename_pattern` by `common/filenames.render_filename` and fed back through
  `parse_filename`, so a name landing would reject cannot be produced. A
  re-delivery for a date already landed gets `_v2` rather than overwriting the
  evidence. **An identity failure (cannot be named) is QUARANTINED and goes
  to `.rejected/`; an integrity failure LANDS and fails at ingest**, because landing is the
  evidence copy and a bad delivery is exactly what it exists to prove.
  **A zip is unpacked AT THE GATE** (`arrival.archive.member_pattern`) and its
  members land as ordinary deliveries — one inbox file in, N deliveries out,
  container never landed but recorded in each member's metadata. Each member
  must carry its own business date; members that are *parts* of one date are a
  different shape and NOT BUILT here. That leaves `landing/` holding only
  objects Spark can read, and `ready/` holding only manifests.
  See `docs/DECISIONS.md#the-inbox-is-the-conformance-gate` and
  `#unpacking-happens-at-the-gate`.
- **`landing/` is the evidence copy; `ready/` is the work queue.** A
  **normalize** stage between them turns a delivery into a MANIFEST -- one
  JSON object naming the business date, the objects holding the rows, and the
  delimiter/quoting/encoding to read them with. `ingest` consumes manifests;
  `find_pending` returns manifest keys. For a plain CSV nothing is copied (the
  part points back into `landing/`), so `ingest` keeps one code path while
  zips and control files get their own normalizers.
  **`_source_file` must stay the PART's key, never the manifest's** --
  `already_ingested` matches on it, so the manifest key there would re-ingest
  every delivery forever. **`find_pending` computes its keep-set from
  `landing/`, not from the manifests**, because `ready/` is a days-long cache
  and landing is the only prefix still holding every date. The manifest never
  records whether something was ingested; that stays derived from the raw
  table. `ready/` is reconciled from `landing/` on demand, so a file pushed
  straight into the bucket is never stranded — and because the registry
  follows MANIFESTS, that reconcile is also what makes the registry
  rebuildable. So the `ready:` window bounds the **derived parts**, not the
  manifests: a manifest whose landing object is still there is kept at any age,
  because sweeping it only gives the next reconcile something to recreate. The
  two were undoing each other nightly, 157 deleted and 157 remade, both logging
  success.
  See `docs/DECISIONS.md#ready-is-a-derived-index`,
  `#the-ready-window-bounds-the-parts-not-the-manifests` and
  `docs/DELIVERY-SHAPES.md`.
- **A feed is named `<source_system>_<feed>`** — `fo_trade`,
  `ref_counterparty`, `treasury_margin_call` — and it is TYPED into feeds.yml,
  not derived. That one string is the raw table, the DAG id, the landing
  prefix, the dbt source table and the prepared model at once, so prefixing the
  name prefixes all five and none of them can drift.
  See `docs/DECISIONS.md#feed-names-carry-the-source`.
- **`conventions:` in feeds.yml is a middle tier between `defaults:` and a
  feed block** — `defaults -> convention -> feed`, shallow at each layer. The
  variation between feeds is mostly per SOURCE SYSTEM, so a convention is
  onboarded once and its feeds are a name, a key and a column list.
  `context.effective_defaults()` is the ONLY implementation of that ordering,
  and the feed console depends on it: `ui/registry._block` omits any key whose
  value matches what the feed inherits, so a second copy of the merge would
  silently start pinning inherited values into individual feed blocks. Naming
  an undefined convention, using an unknown key inside one, or setting `name`
  or `convention` in one are all errors at LOAD.
  See `docs/DECISIONS.md#feed-conventions`.
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
- **The delivery registry is an INDEX, not a ledger** (`reporting_platform/
  registry/`, Postgres `platform`). One row per delivery -- business date,
  arrival time, size, md5, the name the upstream used, the column contract it
  was read against -- and **no verdicts**: no `ingested`, no `superseded`, no
  `status`. Whether a delivery reached raw stays derived from `_source_file`;
  whether it supersedes another stays `dedupe_rank`'s answer. It is Postgres
  because `sequence_no` needs a serialising authority, and it is **rebuildable
  from object storage by the same code that writes it** -- `normalize()`
  registers inline and best-effort, `deliveries.reconcile()` is the authority,
  `coverage()` is what makes a lag visible. `REGISTRY_DSN` lives on the
  `x-s3-env` anchor, so **a container predating it needs recreating, not
  restarting**. See `docs/DECISIONS.md#the-registry-records-observations-not-verdicts`.
- **A refused delivery is evidence too.** Bytes to `quarantine/` in object
  storage, a row in `registry.rejection`, `.rejected/` kept as the console's
  working copy. The rejection DATE IS IN THE KEY, because a quarantined file
  frequently has no parsable name -- that is the whole reason it is there.
  Only an IDENTITY failure is quarantined; an integrity failure still lands
  and fails at ingest. See `docs/DECISIONS.md#quarantine-is-where-a-refused-delivery-goes`.
- **Raw carries four provenance columns and `prepared` selects them through
  `source_provenance()`** -- `_delivery_id`, `_received_at`, `_schema_version`,
  `_source_system`. **Added, never backfilled**: history reads NULL, and an
  as-of query must fall back to `_source_file` to reach past the change.
  `ensure_raw_columns` runs inside `ingest()` for the ONE feed being ingested,
  so the migration is LAZY -- a feed that has not delivered keeps the old
  schema and **every prepared model then fails, not just its own**. Run
  `python -m reporting_platform.ingest.migrate_raw` when deploying a new
  provenance column, before the next ingest; `platform_housekeeping` runs it
  first every night for the same reason.
  See `docs/DECISIONS.md#provenance-is-added-not-backfilled`.
- Retention and GC delete data. `dry_run` first, always. GC defers its deletes
  by design; the deferred-delete pass is the deliberate second step.
- **A dry run may write to the index; it may not write anything a later step
  reads to decide what to delete.** `registry_reconcile` used to ignore
  `dry_run` entirely, and `deliveries.reconcile()` normalizes first, so
  `{"dry_run": true}` wrote 116 manifests into `ready/` and then mispredicted
  its own sweep. Skipping the task is not the fix — it is the rebuild path, and
  *events are an optimisation, the poll is the correctness guarantee*. It is
  narrowed instead: rows still written, manifests not, and the count reported
  as `would_normalize`.
  See `docs/DECISIONS.md#a-dry-run-may-write-to-the-index-not-to-object-storage`.
- **A published tag is DATA retention, sized in years, not by the table
  keep-set.** `references.published_tags` in `retention.yml` is the
  reproducibility window: a tag pins every data file its commit referenced, so
  its lifetime is how long a published run can still be read. It carried the
  tables' `keep_business_days: 10 / keep_month_ends: 80` and was expiring pins
  after about a fortnight. `landing.keep_years` must be >= the longest tag
  window and `retention.py` **refuses to sweep** if it is not — landing is the
  only copy of what the upstream sent, so a pin outliving its evidence is one
  that cannot be honoured. Verified: a tag's own state stays live at any
  `nessie_gc` cutoff, so the tag's lifetime is the only thing that decides
  reproducibility. See `docs/DECISIONS.md#published-tags-are-the-reproducibility-window`.
- **AN INGEST IS NOT A PUBLICATION, and they cut different tags.** An ingest
  pins `snapshot/<feed>/<bd>/<run_id>` (`references.snapshot_tags`); the
  **reporting build** cuts `published/<report>/<bd>/<run_id>`, one per report,
  when it merges. While the ingest cut `published/`, every check that read that
  prefix was reading ingests, `per_report` could never match anything, and an
  ingest was retained for the full ten-year window. A **report is a dbt
  EXPOSURE**, derived by `context.reports()` from the project — not a new
  config block. See `docs/DECISIONS.md#an-ingest-is-not-a-publication`.
- **A run record is the first thing in the registry that CANNOT be rebuilt.**
  `registry.run` / `run_input` / `report_version` / `submission`: a delivery is
  an observation about an object in storage, a run is an event that happened
  once. So `run_input` carries **no foreign key** to `delivery` (a registry
  rebuild would cascade run history away), and a run **does** have a mutable
  status where a delivery may not. The input set is DERIVED — `publish` reads
  the distinct `delivery_id` out of the **prepared** models on the branch,
  before the merge, because only prepared knows which feed a delivery belongs
  to. Version numbering is per `(report, as-at date)`; a family is grouped on
  the SUBMISSION. See `docs/DECISIONS.md#a-run-is-the-first-thing-the-registry-cannot-rebuild`.
- **AN AS-AT DATE HAS A LIFECYCLE, and `open` is the absence of a row.**
  `registry.as_at_transition` is append-only: `open -> locked -> submitted`,
  and back to `reopened` only deliberately. Storing `open` would need every
  (report, date) pair seeded, and that set is DERIVED — a seeded table is a
  second list of reports. Reopening a **submitted** date needs `approved_by`
  to equal the report exposure's `owner.name`; reopening a merely **locked**
  one needs only an actor and a reason. `actor`/`reason` are NOT NULL because
  there is no identity provider here and that record is the whole of the
  accountability. **The publish gate runs BEFORE the merge** — `publish()`
  merges first and versions second, so a gate placed with the versioning fires
  after `main` has moved. It bites only on a CLOSED date whose inputs for that
  date MOVED; `restate` refuses, `carry_forward` publishes and records it, and
  there is no platform-wide default (an exposure with no `meta.restatement` is
  refused). REQ-503 is that nothing branches on what KIND of report it is, and
  `tests/test_lifecycle.py` greps for that rather than asserting it in a
  comment. See `docs/DECISIONS.md#the-as-at-date-has-a-lifecycle`.
- **A feed's evidence window is its RETENTION CLASS**, named in `feeds.yml`
  and sized in `retention.yml` per environment; an undeclared class is refused
  at LOAD. Classes govern `landing/` and `quarantine/` **only** — table
  keep-sets stay per layer, because a per-feed raw window would fight
  `find_pending`. The reproducibility interlock is now per (report, feed) via
  `feeds_behind_report()`, which walks the exposure's `ref()` closure: a feed
  behind no published report is bound by no pin, which is the entire point —
  under the old global rule no class could ever be shorter than the longest
  pin. `snapshot_tags` stays outside it. The landing sweep's summary is
  `default_keep_years`/`classes_applied`, not `keep_years`, because one number
  cannot describe a per-feed sweep. See
  `docs/DECISIONS.md#retention-classes-name-the-obligation`.
- **`expected_by:` is the ONE lateness concept** — a wall-clock `"HH:MM"`,
  judged on the day AFTER the business date (fixed at +1, forgiving on
  purpose). **Quote it**: YAML 1.1 reads `7:00` as 420, and a leading zero
  hides that until the first unpadded hour. A feed without one is skipped, not
  defaulted to midnight. A backfill — every late date sharing one arrival day —
  is reported as ONE event rather than N missed deadlines, described
  differently but never suppressed. See
  `docs/DECISIONS.md#lateness-is-a-wall-clock-time-not-a-duration`.
- **`supersession:` in feeds.yml declares what `dedupe_rank` always assumed.**
  `full_snapshot` is the only built mode and the default; `delta_append` and
  `correction` raise NOT_BUILT at load. The value is the REFUSAL — a delta feed
  deduped as a snapshot silently loses every key its newest file omits.
  As-of queries are the same models with `--vars '{knowledge_time: ...}'`,
  filtered by `known_as_of()` on `coalesce(_received_at, _ingest_ts)`; it
  compiles to `1 = 1` when unset and **refuses an incremental run**, because
  merging as-of rows into the published table restates it backwards.
  `delivery_ref()` is the `_source_file` fallback and it strips the prefix —
  `_delivery_id` is a basename, `_source_file` is a key. A `not_null` test on
  `prepared.delivery_id` is the migration guard: a macro change reaches only
  the models rebuilt after it. See `docs/DECISIONS.md#supersession-is-declared-not-assumed`,
  `#as-of-is-a-var-not-a-second-model` and `#delivery-ref-is-the-fallback-with-the-prefix-stripped`.

## Quick reference

```powershell
# config-level tests: feeds.yml resolution + the console's write-back.
# No stack, ~1s. Everything else is verified by running it. tests/README.md
python -m tests.run

# bulk ingest everything pending (safe to re-run)
docker compose exec airflow python -m scripts.bulk_ingest

# what normalize produced, and what is pending (manifest keys under ready/)
docker compose exec -T airflow python -m scripts._spark_task pending fo_trade
# ready/ retention -- a cache; never sweeps a manifest that is not yet ingested
docker compose exec -T airflow python -m reporting_platform.retention.ready --dry-run

# build + test both layers on a throwaway branch
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt --target spark_local --select path:models/prepared path:models/reporting --vars "{nessie_ref: $branch}"

# can a published run still be read at its own pin? (REQ-702)
# picks the oldest pin holding a data file main no longer references; reports
# `not_yet_meaningful` when no scanned pin does. `--tag <t>` forces one.
docker compose exec -T airflow python -m reporting_platform.monitoring.reproducibility

# the delivery registry -- reconcile is the rebuild path and is idempotent
docker compose exec -T airflow python -m reporting_platform.registry reconcile
docker compose exec -T airflow python -m reporting_platform.registry coverage
docker compose exec -T airflow python -m reporting_platform.registry rejections

# what was published, and out of which deliveries (REQ-400/401)
docker compose exec -T airflow python -m reporting_platform.registry runs
docker compose exec -T airflow python -m reporting_platform.registry versions
docker compose exec -T airflow python -m reporting_platform.registry inputs --run-id <id>

# the as-at lifecycle (REQ-500..503). `open` is the ABSENCE of a transition.
docker compose exec -T airflow python -m reporting_platform.registry lifecycle
docker compose exec -T airflow python -m reporting_platform.registry state --report <r> --as-at <d>
docker compose exec -T airflow python -m reporting_platform.registry lock --report <r> --as-at <d> --actor WHO --reason WHY
# reopening a SUBMITTED date needs --approved-by == the exposure's owner
docker compose exec -T airflow python -m reporting_platform.registry reopen --report <r> --as-at <d> --actor WHO --reason WHY

# what changed between two versions of one report+date: inputs AND code (§11)
docker compose exec -T airflow python -m reporting_platform.registry diff --report <r> --as-at <d>

# deliveries that arrived after their expected_by (REQ-201). No Spark.
docker compose exec -T airflow python -m reporting_platform.monitoring.lateness

# as of a knowledge time -- the same models, on a throwaway branch, NEVER merged.
# --full-refresh is not optional: known_as_of() refuses an incremental run.
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt --target spark_local --full-refresh --select path:models/prepared --vars "{nessie_ref: $branch, knowledge_time: '2026-08-10'}"

# is every published pin's landing evidence still there? (REQ-602, per delivery)
docker compose exec -T airflow python -m reporting_platform.monitoring.evidence

# give every raw table the current provenance columns (idempotent; needed
# BEFORE the next ingest whenever a provenance column is added)
docker compose exec -T airflow python -m reporting_platform.ingest.migrate_raw --dry-run

# retention / maintenance -- dry run first, --all-managed covers every table
docker compose exec -T airflow python -m reporting_platform.retention.retention --all-managed --dry-run
docker compose exec -T airflow python -m reporting_platform.retention.quarantine --dry-run
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
