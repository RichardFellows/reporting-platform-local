# CLAUDE.md

Orientation for working on this repo with an AI assistant.

Read order: **this file** → `docs/QUICKSTART.md` (get it running) →
`docs/ARCHITECTURE.md` (why it is shaped this way).

This file is the short form: one line per rule, with the symptom that
identifies it. The reasoning and the evidence behind each is in
`docs/DECISIONS.md` under the anchor cited — read that before changing one.

## What this is

A laptop-runnable local approximation of a lakehouse platform: MinIO (S3),
Nessie (Iceberg catalog with git-like branching), Postgres, Spark, Airflow and
dbt, replacing a legacy ETL / RDBMS / scheduler chain.

Daily CSVs land immutably in object storage → `raw` (1:1, all strings) →
`prepared` (conformed, typed, deduplicated) → `reporting` (marts). Every build
happens on a Nessie branch and merges to `main` only if its tests pass —
write-audit-publish.

## The one habit that matters

**Verify against the live stack. Get the actual error text before proposing a
fix.** Docstrings, comments and docs in a system like this drift from
behaviour; a claim is worth what its last execution proved. A subsystem looks
fine until the first time it runs in a *new* configuration, so if you are about
to run something for the first time, expect it to fail and read what it says.

- **A guard written against a documented mechanism rather than the working one
  can only ever produce false alarms.** Check that the thing it reads can
  actually be set.
- **A check whose window does not contain the thing it describes** will either
  never fire or never stop. Match the window to the cadence of whatever clears
  it.
- **A subject it could not READ is not a subject that is EMPTY**, and the two
  are the same value. Reporting the first as the second is how a monitor goes
  green on a table nobody opened and a sweep treats "nothing is live" as
  "delete everything". Say which one it was.
  (`#an-incomplete-keep-set-refuses`)

## Environment

- Docker Desktop. An agent session drives the stack directly — `docker
  compose`, `curl`, everything.
- **Windows: use PowerShell, or `MSYS_NO_PATHCONV=1` with bash**, for `docker
  compose exec` — Git Bash rewrites `/opt/platform/...` and the exec fails.
- **Airflow is 2.10.5 deliberately.** Under Airflow 3 tasks ran and pushed xcom
  and the scheduler never recorded them. See `Dockerfile.airflow`'s header.
  (`#airflow-2-not-3`)
- **Compose builds one image per service**: `build airflow` does not rebuild
  `airflow-init`. Build them together.
- **Editing `.env` or `docker-compose.yml` needs the container recreated**, not
  restarted — including a variable added to the `x-s3-env` anchor, which is how
  `REGISTRY_DSN` reaches a container. So does editing a module a long-running
  process already imported: `feeds()` re-reads config on mtime, but the `inbox`
  watcher holds its imports, so restart it after touching `ingest/` or
  `common/` or it reports config as broken that is valid on disk.
- **A DAG run left non-terminal starves every other run.** DAGs `queued` with
  `start=None` mean a stale run.
- **Never use `airflow dags test`** — it blocks the next run under
  `max_active_runs=1`, killing the exec pipe leaves the process alive, and
  deleting its rows corrupts the record. Use `dags trigger -r <distinct id>`
  and wait on that run id.

## Engine and versions

- **Build on a throwaway branch, never `main`** —
  `scripts/_open_build_branch.py`. Branch → build → test → merge only if clean.
- **Spark is the only build engine** (`dbt_builds.py` refuses a non-Spark
  `DBT_TARGET`): only the Spark path can address a Nessie branch. DuckDB is a
  read-only query tool (`scripts/duckdb_console.py`).
  (`#duckdb-is-not-an-engine`)
- **`spark_session()` is in `common/spark.py` and `Nessie` in
  `common/nessie.py`**, both RE-EXPORTED from `common/context.py` — every
  existing import still works, and reading `feeds.yml` no longer drags an
  engine and an HTTP client in with it. `CONFIG_DIR`/`CATALOG`/`ENV` are in
  `common/settings.py`, which is what lets `spark.py` name the catalog without
  importing `context`.
- **Every Spark job runs on the cluster, never `local[*]`.** `SPARK_MASTER` is
  read in two places that must not diverge — `spark_session()` in
  `common/spark.py` and `spark.master` in `dbt/profiles.yml`;
  `spark_session()` refuses a `local` master rather than run in the Airflow
  container with the cluster idle. Each app caps at 2 cores/2g or standalone
  mode holds every free core until the session stops and the next job waits
  forever instead of failing. <http://localhost:8080>.
  (`#spark-master-single-source`, `#spark-worker-sizing`, ARCHITECTURE
  § *Where Spark actually runs*)
- **Spark inside an Airflow task must go through `scripts/_spark_task.py`** (a
  subprocess), or the JVM keeps the task process alive, heartbeats stop and the
  scheduler zombie-reaps it. The *driver* lives in that process, so this holds
  on a cluster too. (`#spark-in-a-subprocess`)
- **THREE jar versions live in `.env`** and diverging them gives you
  `NoSuchMethodError` on the first write, never anything saying "version".
  `ICEBERG_VERSION` must be identical in the Spark image and in *both* drivers
  (`spark_session()`, `spark.jars.packages` in `dbt/profiles.yml`) — every
  submitting process runs a pip pyspark with no jars of its own.
  `NESSIE_SPARK_EXT_VERSION` tracks **Iceberg, not the server** (0.103.3 ↔
  1.8.1, 0.108.1 ↔ 1.11.0); newer-than-yours is the failing direction.
  `NESSIE_SERVER_VERSION` sets the server image and the `nessie-gc` jar, equal
  to each other, allowed to be newer than the extensions. `.env.example` has a
  known-good set; `docker compose exec spark-worker env | grep VERSION` says
  what is baked in. (`#jar-versions`)

## dbt and the build DAGs

- **Fixing a macro proves nothing about models that don't call it.** Grep for
  the construct, not the macro. (`dbt/macros/engine.sql` holds engine-specific
  SQL; `naming.sql` overrides `generate_schema_name` so layers land in
  `prepared`/`reporting` — removing it means moving every table reference.)
- **Cosmos renders the build DAGs from the dbt project on every parse**, one
  task per model, so **adding a model needs no DAG edit**
  (`docs/ADDING-A-MODEL.md`). Four settings in `dbt_builds.py` are load-bearing
  and its docstring says why: `InvocationMode.SUBPROCESS`, the
  `lakehouse_write` pool on *every* rendered task, `LoadMode.DBT_LS`,
  `TestBehavior.AFTER_ALL`. (`#cosmos-load-bearing-settings`)
- **`astronomer-cosmos` is installed `--no-deps` on purpose.** Under Airflow's
  constraints it pins `typing_extensions==4.12.2`; dbt's `mashumaro` needs
  4.13+ for `evaluate_forward_ref`, and every dbt invocation then dies at
  import — in dbt, not cosmos, and not until something runs dbt.
  `Dockerfile.airflow` smoke-tests `dbt --version`; re-run `pip install
  --dry-run` before moving `COSMOS_VERSION`.
  (`#cosmos-no-deps`)
- **`airflow-init` does four things**: db migrate, admin user, `pools set
  lakehouse_write 1`, `dbt deps`. Without packages, `dbt ls` cannot compile a
  `dbt_utils` test and the two build DAGs do not *import*.
  (`#airflow-init-four-things`)
- **dbt's three working directories live under `/opt/platform/run`, not the
  `./dbt` bind mount** (`DBT_LOG_PATH`, `DBT_TARGET_PATH`,
  `packages-install-path`) — a bind mount keeps host ownership, so no `chown` in
  the image reaches it and `dbt deps` fails with `Permission denied`. Packages
  are shared through a named volume mounted one level **above** `dbt_packages`
  (`dbt deps` rmtree's that directory; a mount point cannot be removed). If
  packages come back missing or root-owned, remove the volume — a rebuild will
  not re-seed an existing one. (`#dbt-working-directories`,
  `#dbt-packages-volume`)

## Arrival: inbox → landing → ready

Shapes and mechanism: `docs/DELIVERY-SHAPES.md`,
`docs/DECISIONS.md#the-inbox-is-the-conformance-gate`,
`#unpacking-happens-at-the-gate`, `#ready-is-a-derived-index`.

- **`landing/` has a contract: everything in it is correctly named and
  classified.** An approved sender writes there directly; everything else goes
  through the **inbox conformance gate** (`ingest/conform.py`, driven by
  `ingest/inbox.py`). A feed with an `arrival:` block is the second kind.
- **THE INBOX ESTABLISHES IDENTITY; INGESTION VERIFIES INTEGRITY.** The keys
  are split to enforce it: `arrival.control` carries `cob_date`/`version` (what
  the file must be NAMED) and rejects `row_count`/`md5` at load;
  `delivery.control` carries `row_count`/`md5`, checked at ingest for every
  delivery however it arrived, so the trusted path is never the less-verified
  one. They are complementary — `arrival.control` without `delivery.control` is
  rejected.
- **The control file is PROMOTED, not consumed**: the delivery is data file
  plus control file, and the gate renames both into landing. Such a feed has
  **two filenames and they are different strings** — `Feed.claims_source` for
  the upstream's, `Feed.parse_filename` for landing's. The rename is generated
  from `filename_pattern` and fed back through `parse_filename`, so a name
  landing would reject cannot be produced; a re-delivery for a landed date gets
  `_v2` rather than overwriting the evidence.
- **An identity failure is QUARANTINED to `.rejected/`; an integrity failure
  LANDS and fails at ingest** — landing is the evidence copy, and a bad
  delivery is what it exists to prove.
- **HOW a control file is read is `control.format`; WHAT is read out of it is
  the fields.** The default is `regex` -- a pattern per field over the whole
  text -- and `delimited` makes every field a COLUMN NAME instead, for a
  pipe-or-whatever table with its own headers. `ingest/control.py` is the ONLY
  parser, for both blocks and both formats. The two blocks read the SAME
  PROMOTED BYTES, so a format declared on each must be identical and load
  refuses otherwise; `delimiter` is required and never inherited from the
  feed's own, because pipes read as commas is not an error, it is one column
  named by the whole header line. (`#control-file-formats`)
- **A zip is unpacked AT THE GATE** (`arrival.archive.member_pattern`): one
  file in, N ordinary deliveries out, container never landed but recorded in
  each member's metadata. Each member carries its own COB date; members that
  are *parts* of one date are NOT BUILT here. So `landing/` holds only objects
  Spark can read.
- **`landing/` is the evidence copy; `ready/` is the work queue.** A
  **normalize** stage turns a delivery into a MANIFEST — COB date, the objects
  holding the rows, delimiter/quoting/encoding — which is what `ingest`
  consumes and `find_pending` returns. A plain CSV copies nothing (the part
  points back into `landing/`), so `ingest` keeps one code path while zips and
  control files get their own normalizers.
- **`_source_file` must stay the PART's key, never the manifest's** —
  `already_ingested` matches on it, so the manifest key re-ingests every
  delivery forever.
- **`find_pending` computes its keep-set from `landing/`, not the manifests**:
  `ready/` is a days-long cache, landing is the only prefix holding every date.
  So the `ready:` window bounds the **derived parts, not the manifests** — a
  manifest whose landing object exists is kept at any age, or the sweep and the
  reconcile undo each other nightly (157 deleted, 157 remade, both logging
  success). Ingestion is never recorded in the manifest; it stays derived from
  the raw table. `ready/` reconciles from `landing/` on demand, so nothing
  pushed straight into the bucket is stranded — and since the registry follows
  manifests, that reconcile is also what makes the registry rebuildable.
  (`#the-ready-window-bounds-the-parts-not-the-manifests`)

## Feeds and columns

The procedures are `docs/ADDING-A-FEED.md` (five files, no DAG edit),
`docs/ADDING-A-COLUMN.md` (three files and one command) and `docs/FEED-UI.md`.

- **A feed is named `<source_system>_<feed>`** (`fo_trade`,
  `ref_counterparty`), TYPED into feeds.yml, not derived. That one string is
  the raw table, DAG id, landing prefix, dbt source table and prepared model at
  once, so none of the five can drift. (`#feed-names-carry-the-source`)
- **`conventions:` is a middle tier**: `defaults -> convention -> feed`,
  shallow at each layer, because variation is mostly per SOURCE SYSTEM.
  `context.effective_defaults()` is the ONLY implementation of that ordering,
  and the console depends on it — `ui/registry._block` omits any key matching
  what the feed inherits, so a second copy of the merge would start pinning
  inherited values into feed blocks. An undefined convention, an unknown key in
  one, or `name`/`convention` in one are errors at LOAD. (`#feed-conventions`)
- **A column may be named differently in the file than in the platform** —
  `- trade_id: "Trade Id"` renames at ingest, so raw onwards is ordinary
  identifiers and macros never quote one; drift is reported in the file's
  names. (`#source-column-names`, `#identifiers-in-macros`)
- **`generate_feeds.py` is not one of those five files**: it hand-writes the
  four original feeds, whose pathologies are the point, and generates the rest
  via `ui/sampledata.py`. Pass `types=` when calling that directly, or it
  re-guesses from the column name and a `decimal` gets a string `safe_cast`
  silently nulls.
- **The feed console (`reporting_platform/ui`, <http://localhost:8082>) writes
  those five files from a form** and drives land → ingest → build. Its
  **Arrivals** page (`ui/arrivals.py`) is the one view that is neither: what
  became of every file offered, joined per request from `registry.delivery`,
  `registry.rejection`, `inbox.route()` and Airflow, and **written nowhere** —
  an arrivals table would hold verdicts the platform derives and could not be
  rebuilt. A declared **md5** is comparable there (same bytes ingest hashes);
  a declared **row_count** is not, and says `at_ingest`. `no run recorded`
  means Airflow trimmed its history, never "not ingested".
  (`#the-arrivals-view-is-a-join-not-a-record`) It is a
  front end for that procedure, not a second source of truth: the change is an
  ordinary reviewable diff, checked with `dbt parse` (~5s, no Spark). Its
  one-feed build **never merges**, on purpose — publication belongs to the
  Airflow builds, not a button labelled "test". That build is EXCLUSIVE, so it
  must be able to end: `jobs.stream` kills its subprocess at
  `FEED_UI_JOB_TIMEOUT` (default 3600s) and `DELETE /api/jobs/<id>` cancels
  one, both by PROCESS GROUP — dbt spawns spark-submit spawns a JVM, and
  signalling the direct child alone orphans the part holding the cores. Because it edits config a
  running Airflow is reading, `feeds()` and `_load()` are cached on **mtime**:
  a plain `@lru_cache` there means a new feed never reaches the DAG
  processor.
- **ADDING A COLUMN TO AN EXISTING FEED IS THE COMMONEST CHANGE A LIVE FEED
  EVER HAS, and the raw table is the part not in the git diff.**
  `ensure_raw_table` is `CREATE TABLE IF NOT EXISTS` and reconciles nothing;
  `ensure_raw_schema` does, on the branch, for the whole declared contract.
  Without it a declared column passes every check in `ingest_feed` and dies at
  the write with `INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS`, naming
  an ARITY and neither the column nor `feeds.yml`. **The directions are NOT
  symmetrical**: a declared column is added, an undeclared one is NEVER dropped
  — a rename is character-for-character a drop plus an add, so the destructive
  reading of an ambiguous edit is the one nothing acts on. An orphan is
  NULL-filled and reported. (`#a-declared-column-migrates-itself`)
- **`on_schema_change: append_new_columns` in `dbt_project.yml`** — dbt's
  `ignore` default leaves a new column in the SELECT and never in the target,
  green build and all. It adds the column but does not populate rows the run
  did not touch, so on an SCD2 dimension every CURRENT row reads NULL until its
  entity next changes; `--full-refresh` is a choice about DATA, not the only
  way to get the COLUMN.
- **Raw's four provenance columns are added, never backfilled** —
  `_delivery_id`, `_received_at`, `_schema_version`, `_source_system`, selected
  through `source_provenance()`. History reads NULL, so an as-of query must
  fall back to `_source_file`. The migration is LAZY, so a feed that has not
  delivered keeps the old schema and **every prepared model then fails, not
  just its own**: run `ingest.migrate_raw` before the next ingest when
  deploying one. (`#provenance-is-added-not-backfilled`)
- **`supersession:` declares what `dedupe_rank` always assumed.**
  `full_snapshot` is the only built mode and the default; `delta_append` and
  `correction` raise NOT_BUILT at load. The value is the REFUSAL — a delta feed
  deduped as a snapshot silently loses every key its newest file omits.
  (`#supersession-is-declared-not-assumed`)
- **As-of is a var, not a second model**: the same models with `--vars
  '{knowledge_time: ...}'`, filtered by `known_as_of()` on
  `coalesce(_received_at, _ingest_ts)`. It compiles to `1 = 1` when unset and
  **refuses an incremental run**, because merging as-of rows into the published
  table restates it backwards. `delivery_ref()` is the `_source_file` fallback
  and strips the prefix (`_delivery_id` is a basename, `_source_file` a key);
  the `not_null` test on `prepared.delivery_id` is the migration guard, since a
  macro change reaches only the models rebuilt after it.
  (`#as-of-is-a-var-not-a-second-model`,
  `#delivery-ref-is-the-fallback-with-the-prefix-stripped`)
- **`expected_by:` is the ONE lateness concept** — a wall-clock `"HH:MM"`
  judged the day AFTER the COB date (fixed at +1, forgiving on purpose).
  **Quote it**: YAML 1.1 reads `7:00` as 420 and a leading zero hides that until
  the first unpadded hour. No `expected_by` means skipped, not midnight. A
  backfill (many late dates, one arrival day) is ONE event, described
  differently but never suppressed.
  (`#lateness-is-a-wall-clock-time-not-a-duration`)

## The registry (Postgres `platform`)

`reporting_platform/registry/`. Shapes and column lists are in
`docs/DECISIONS.md`; these are the rules that fail silently if you work against
them.

- **The delivery registry is an INDEX, not a ledger**: one row per delivery and
  **no verdicts** — no `ingested`, no `superseded`, no `status`. Whether a
  delivery reached raw stays derived from `_source_file`; whether it supersedes
  another stays `dedupe_rank`'s answer. It is **rebuildable from object storage
  by the same code that writes it**, and `deliveries.reconcile()` is the
  authority. (`#the-registry-records-observations-not-verdicts`)
- **A refused delivery is evidence too** — bytes to `quarantine/`, a row in
  `registry.rejection`, `.rejected/` as the console's working copy. The
  rejection DATE IS IN THE KEY, because a quarantined file usually has no
  parsable name; that is why it is there.
  (`#quarantine-is-where-a-refused-delivery-goes`)
- **A run record is the first thing in the registry that CANNOT be rebuilt.** A
  delivery is an observation about stored bytes; a run is an event that
  happened once. So `run_input` carries **no foreign key** to `delivery`, and a
  run **does** have a mutable status where a delivery may not. Its input set is
  DERIVED, read out of the **prepared** models on the branch before the merge.
  (`#a-run-is-the-first-thing-the-registry-cannot-rebuild`,
  `#version-is-per-report-and-as-at-date`)
- **A CHANGE IS A DEPLOYMENT EVENT, NOT A RUN EVENT.** One ticket authorises a
  version and every run until the next deployment inherits it, so the
  deployment refs come from the ENVIRONMENT, not the trigger; `change_ref`
  stays separate for a per-run restatement. `dbt_project_ref` (declared commit)
  and `dbt_manifest_ref` (digest of what is on disk) both exist because they
  diverge whenever the project is writable at run time — which is what the feed
  console does — and `check_project_drift()` therefore compares DIGEST TO
  DIGEST, refusing only in `uat`/`prod`.
  (`#a-change-is-a-deployment-event-not-a-run-event`,
  `#code-identity-is-a-digest-when-it-cannot-be-a-tag`)
- **Adding a registry column needs `MIGRATIONS` as well as `SCHEMA`** — `CREATE
  TABLE IF NOT EXISTS` is a no-op on an existing table, so the column never
  appears and the INSERT fails later, in a task, at publish.
- **AN AS-AT DATE HAS A LIFECYCLE, and `open` is the absence of a row.**
  `registry.as_at_transition` is append-only, `open -> locked -> submitted`,
  back to `reopened` only deliberately and — from **submitted** — only with the
  exposure owner's approval. **The publish gate runs BEFORE the merge**, since
  `publish()` merges first and versions second, so a gate placed with the
  versioning fires after `main` has moved. REQ-503 is that nothing branches on
  what KIND of report it is; `tests/test_lifecycle.py` greps for that.
  (`#the-as-at-date-has-a-lifecycle`)

## Retention, tags and GC

- **Retention and GC delete data. `dry_run` first, always.** GC defers its
  deletes by design; the deferred-delete pass is the deliberate second step.
- **THE ORPHAN SWEEP'S INPUT IS A KEEP-SET, so a short answer is a deletion
  order.** `orphan_storage` deletes every warehouse prefix not live on some
  reference: a Nessie that is down therefore used to read as "nothing is
  live". An unreadable reference (anything but a 404) now REFUSES the sweep,
  an empty live set against a non-empty warehouse refuses too, and a refusal
  exits non-zero — nothing deleted is not the same as nothing to delete. The
  prefix depth is derived from `REPORTING_WAREHOUSE`, never assumed, or a
  nested root makes every namespace an orphan.
  (`#an-incomplete-keep-set-refuses`)
- **The completeness check has THREE answers, not two**: `no data` (empty
  table), `no table` (`TABLE_OR_VIEW_NOT_FOUND` — a feed that has never
  delivered), `unreadable` (anything else, and it fails `--fail-on-gap`).
- **A dry run may write to the index; it may not write anything a later step
  reads to decide what to delete.** `registry_reconcile` ignored `dry_run` and
  `deliveries.reconcile()` normalizes first, so `{"dry_run": true}` wrote 116
  manifests into `ready/` and then mispredicted its own sweep. Skipping the task
  is not the fix — it is the rebuild path, and *events are an optimisation, the
  poll is the correctness guarantee*. Narrowed instead: rows written, manifests
  not, count reported as `would_normalize`.
  (`#a-dry-run-may-write-to-the-index-not-to-object-storage`)
- **A published tag is DATA retention, sized in years, not by the table
  keep-set.** `references.published_tags` is the reproducibility window: a tag
  pins every data file its commit referenced, so its lifetime is how long a
  published run can still be read. It once carried the tables'
  `keep_business_days: 10` and expired pins after a fortnight.
  `landing.keep_years` must be >= the longest tag window and `retention.py`
  **refuses to sweep** otherwise — landing is the only copy of what the
  upstream sent, so a pin outliving its evidence cannot be honoured. (A tag's
  own state stays live at any `nessie_gc` cutoff — verified.)
  (`#published-tags-are-the-reproducibility-window`)
- **AN INGEST IS NOT A PUBLICATION, and they cut different tags.** An ingest
  pins `snapshot/<feed>/<bd>/<run_id>`; the **reporting build** cuts
  `published/<report>/<bd>/<run_id>`, one per report, when it merges. While the
  ingest cut `published/`, every check reading that prefix was reading ingests,
  `per_report` matched nothing, and an ingest was kept for the full ten years.
  A **report is a dbt EXPOSURE**, derived by `context.reports()` — not a new
  config block. (`#an-ingest-is-not-a-publication`)
- **A feed's evidence window is its RETENTION CLASS**, named in `feeds.yml`,
  sized in `retention.yml` per environment, refused at LOAD if undeclared.
  Classes govern `landing/` and `quarantine/` **only** — table keep-sets stay
  per layer, because a per-feed raw window would fight `find_pending`. The
  reproducibility interlock is per (report, feed) via `feeds_behind_report()`,
  which walks the exposure's `ref()` closure: a feed behind no published report
  is bound by no pin, which is the point — under the old global rule no class
  could be shorter than the longest pin. `snapshot_tags` stays outside it. The
  landing sweep reports `default_keep_years`/`classes_applied`, because one
  number cannot describe a per-feed sweep.
  (`#retention-classes-name-the-obligation`)

## Lineage

How the datasets, columns and classes are actually derived is in
`docs/DECISIONS.md#lineage-is-derived-from-the-dbt-project` and
`#a-column-with-no-source-says-so` — read those before changing
`reporting_platform/lineage`. What matters from outside it:

- **OpenLineage is an EXPORT, Marquez a CONSUMER**, both off by default behind
  `OPENLINEAGE_DISABLED`. The provider is ALREADY in the image — installing it
  explicitly under Airflow's constraints is the cosmos trap exactly. Env is
  read at process start, so enabling it needs the airflow containers
  **recreated**.
- **Both Marquez images are BUILT HERE on UBI, from Marquez's own source** —
  `Dockerfile.marquez-api`, `Dockerfile.marquez-web`; upstream ships Ubuntu and
  Alpine. `MARQUEZ_VERSION` is the RELEASE TAG the builders fetch, so the first
  `--profile lineage up` after changing it builds (gradle + npm, with egress)
  rather than pulls. Nothing else about the deployment changed.
  (`#marquez-on-ubi`)
- **It is not an authority, and not the record of what a run published.**
  Lineage is derived from the dbt project by `context.model_refs()` — the SAME
  walker `feeds_behind_report()` sizes retention windows with, so a second
  graph that drifts is the failure this file keeps warning about;
  `tests/test_lineage.py` asserts they agree. `registry.run_input` is the
  publication record, and the two legitimately differ (an SCD2 dimension
  contributes 10 of 40 deliveries; OpenLineage reports all 40 as read). Do not
  reconcile them.
- **Register the custom extractor with `AIRFLOW__OPENLINEAGE__EXTRACTORS`** —
  **not** `__CUSTOM_EXTRACTORS`, which is read by nothing and warns about
  nothing. Without it Airflow emits jobs and no datasets at all, because
  neither built-in path can work here and both reasons are permanent.
- **Every column is classified** — `sourced`, `row_aggregate`,
  `build_metadata`, `literal`, `ingest_added`, `unresolved` — because reporting
  only the columns that trace makes a literal, a `count(*)` and a parser
  failure look identical.
- **`unresolved` is a DEFECT that does not fail a build**: nothing in this
  package may raise, an export must never gain the power to stop the pipeline.
  The seam is CI — `lineage --columns` exits 1 on any.
- **No value this package emits may contain a credential word.** Facet values
  pass through Airflow's SecretsMasker and this estate's Postgres user and
  password are both `platform`, so a class called `platform_column` reached
  Marquez as `***_column` — a well-formed facet with corrupted content. A test
  pins it.
- **A SKIPPED task shows as `RUNNING` in Marquez forever** — Airflow 2.10's
  listener spec has no skipped hook and skipping is the ingest DAGs' idle
  state, so `RUNNING` there means "started, did not succeed or fail". Airflow
  is the authority on what is running.
  (`#openlineage-is-an-export-not-a-record`)

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

# can a published run still be read at its own pin? (REQ-702) Reports
# `not_yet_meaningful` when no scanned pin holds a file main dropped;
# `--tag <t>` forces one
docker compose exec -T airflow python -m reporting_platform.monitoring.reproducibility

# the delivery registry -- reconcile is the rebuild path and is idempotent
docker compose exec -T airflow python -m reporting_platform.registry reconcile
docker compose exec -T airflow python -m reporting_platform.registry coverage
docker compose exec -T airflow python -m reporting_platform.registry rejections

# what was published, and out of which deliveries (REQ-400/401)
docker compose exec -T airflow python -m reporting_platform.registry runs
docker compose exec -T airflow python -m reporting_platform.registry versions
docker compose exec -T airflow python -m reporting_platform.registry inputs --run-id <id>
# code + deployment identity. The DEPLOYMENT PIPELINE uses this to compute the
# DBT_PROJECT_DIGEST it bakes in.
docker compose exec -T airflow python -m reporting_platform.registry provenance
# what changed between two versions of one report+date: inputs AND code (§11)
docker compose exec -T airflow python -m reporting_platform.registry diff --report <r> --as-at <d>

# the as-at lifecycle (REQ-500..503). `open` is the ABSENCE of a transition.
docker compose exec -T airflow python -m reporting_platform.registry lifecycle
docker compose exec -T airflow python -m reporting_platform.registry state --report <r> --as-at <d>
docker compose exec -T airflow python -m reporting_platform.registry lock --report <r> --as-at <d> --actor WHO --reason WHY
# reopening a SUBMITTED date needs --approved-by == the exposure's owner
docker compose exec -T airflow python -m reporting_platform.registry reopen --report <r> --as-at <d> --actor WHO --reason WHY

# deliveries that arrived after their expected_by (REQ-201). No Spark.
docker compose exec -T airflow python -m reporting_platform.monitoring.lateness
# is every published pin's landing evidence still there? (REQ-602, per delivery)
docker compose exec -T airflow python -m reporting_platform.monitoring.evidence

# as of a knowledge time -- same models, throwaway branch, NEVER merged.
# --full-refresh is not optional: known_as_of() refuses an incremental run.
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt --target spark_local --full-refresh --select path:models/prepared --vars "{nessie_ref: $branch, knowledge_time: '2026-08-10'}"

# give every raw table the current provenance columns (idempotent; needed
# BEFORE the next ingest whenever a provenance column is added)
docker compose exec -T airflow python -m reporting_platform.ingest.migrate_raw --dry-run

# retention / maintenance -- dry run first, --all-managed covers every table
docker compose exec -T airflow python -m reporting_platform.retention.retention --all-managed --dry-run
docker compose exec -T airflow python -m reporting_platform.retention.quarantine --dry-run
docker compose exec -T airflow python -m reporting_platform.maintenance.maintain --all-managed --dry-run

# DAGs -- and what Cosmos rendered (one *_run task per model, plus dbt_test)
docker compose exec -T airflow airflow dags list-runs -d prepared_build -o plain
docker compose exec -T airflow airflow dags trigger ingest_fo_trade
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
# what became of every file offered: classification, declared checks, the
# ingest run. A join, written nowhere -- no stack state to reset.
curl -s 'http://localhost:8082/api/arrivals?limit=20'
curl -s http://localhost:8082/api/inbox     # what is at the door, and how it routes

# read-only query console against published `main`
docker compose exec -T airflow python -m scripts.duckdb_console --tables

# refs (add ?fetch=ALL for commit metadata)
curl -s http://localhost:19120/api/v2/trees

# lineage -- opt-in. Needs OPENLINEAGE_DISABLED=false in .env AND the airflow
# containers RECREATED (env is read at process start), or nothing is emitted.
docker compose --profile lineage up -d marquez-api marquez-web
docker compose up -d --force-recreate airflow airflow-webserver airflow-triggerer
# http://localhost:13000 -- 5000/5001/3000 are remapped, they collide with
# grafana and friends on an ordinary developer box
curl -s 'http://localhost:15000/api/v1/namespaces/reporting-platform-local/jobs?limit=50'
# what Marquez will be told each task reads and writes -- no Airflow, no Spark.
# A missing edge here is a missing edge there; both come from one derivation.
docker compose exec -T airflow python -m reporting_platform.lineage
# every column of every managed table, classified. EXITS 1 on any `unresolved`
# -- the CI seam, because the lineage package itself may never refuse.
docker compose exec -T airflow python -m reporting_platform.lineage --columns
# the datasets live in their OWN namespaces, not the job's -- a jobs query
# showing no inputs/outputs is not evidence that nothing was emitted
curl -s http://localhost:15000/api/v1/namespaces
curl -s -G http://localhost:15000/api/v1/lineage \
  --data-urlencode 'nodeId=dataset:s3://lakehouse:landing/fo_trade' --data-urlencode depth=20
# column-level: one column back to the CSV it came from, with the SQL that
# transformed it at each hop
curl -s -G http://localhost:15000/api/v1/column-lineage --data-urlencode depth=20 \
  --data-urlencode 'nodeId=datasetField:iceberg://lakehouse:reporting.exposure_by_country:total_mtm'
```

A clean seed that passes its tests — needed for anything that publishes —
comes from `generate_feeds.py --clean`. The default seed injects two
data-quality failures on purpose, so a build against it correctly refuses to
publish.
