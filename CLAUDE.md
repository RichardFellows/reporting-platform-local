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
  restarted — including a variable added to the `x-s3-env` anchor. So does
  editing a module a long-running process already imported: `feeds()` re-reads
  config on mtime, but the `inbox` watcher holds its imports, so restart it
  after touching `ingest/` or `common/`.
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
- **Endpoints come from `common/settings.py` accessors, and fall back to the
  compose hosts ONLY when `REPORTING_ENV=local`** — `s3_endpoint()`,
  `nessie_uri()`, `warehouse()`, `landing()`, `registry_dsn()`. Anywhere else an
  unset one raises naming the variable, and `config check` exits 1. A direct
  `os.environ` read of one fails `tests/test_settings.py`.
  (`#settings-refuse-outside-local`)
- **`spark_session()` is in `common/spark.py` and `Nessie` in
  `common/nessie.py`**, both RE-EXPORTED from `common/context.py` — every
  existing import still works, and reading `feeds.yml` no longer drags an
  engine and an HTTP client in with it. `CONFIG_DIR`/`CATALOG`/`ENV` are in
  `common/settings.py`, which is what lets `spark.py` name the catalog without
  importing `context`.
- **Every Spark job runs on the cluster, never `local[*]`.** `SPARK_MASTER` is
  read in two places that must not diverge — `spark_session()` and
  `spark.master` in `dbt/profiles.yml` — and `spark_session()` refuses a
  `local` master. Each app caps at 2 cores/2g, or standalone mode holds every
  free core and the next job waits forever instead of failing.
  <http://localhost:8080>. (`#spark-master-single-source`,
  `#spark-worker-sizing`)
- **S3 TLS is derived from `S3_ENDPOINT`'s own scheme, not a second env
  var** — `https://` turns on `fs.s3a.connection.ssl.enabled`, in both
  `spark_session()` and `dbt/profiles.yml`'s `spark_local` target. Pointing
  at a real TLS-terminated S3-compatible store needs only the URL changed,
  never a code change. (`#s3-ssl-follows-the-endpoint-scheme`)
- **`conf/spark-defaults.conf` holds only what is the same everywhere** — the
  cluster image ships it, so a host, TLS switch or credential source there is
  one the cluster inherits. `spark_ocp` reads every per-environment key via
  `env_var()`, like `spark_local`; `tests/test_spark_config.py` enforces
  both. A bare `spark-sql` in spark-master no longer knows the catalog: use
  `scripts/spark-sql`. (`#spark-defaults-hold-only-invariants`)
- **Spark inside an Airflow task must go through `scripts/_spark_task.py`** (a
  subprocess), or the JVM keeps the task process alive, heartbeats stop and the
  scheduler zombie-reaps it. The *driver* lives in that process, so this holds
  on a cluster too. (`#spark-in-a-subprocess`)
- **THREE jar versions live in `.env`**, and diverging them gives
  `NoSuchMethodError` on the first write, never anything saying "version".
  `ICEBERG_VERSION` must be identical in the Spark image and the Airflow
  image, which bakes the jars for *both* drivers — every submitting process
  runs a pip pyspark with no jars of its own. Both drivers read
  `PLATFORM_DRIVER_JARS`; nothing resolves jars through Ivy at runtime.
  (`#driver-jars-are-baked`)
  `NESSIE_SPARK_EXT_VERSION` tracks **Iceberg, not the server**;
  `NESSIE_SERVER_VERSION` sets the server image and the `nessie-gc` jar and may
  be newer than the extensions. `tests/test_versions.py` pins all three.
  (`#jar-versions`)
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
- **`Dockerfile.airflow` has two targets: compose builds `dev` and MOUNTS the
  code; `release` bakes the code and dbt packages in, for a cluster.** A
  directory added to the airflow anchor's mounts must be COPYed into
  `release` too — `tests/test_release_image.py` fails otherwise. `make
  release-image` prints `PLATFORM_CODE_REF` (the image digest) and
  `DBT_PROJECT_DIGEST`. (`#the-release-image-carries-the-code`)
- **`airflow-init` does five things**: db migrate, admin user, `pools set
  lakehouse_write 1`, the registry schema, `dbt deps`. Without packages,
  `dbt ls` cannot compile a `dbt_utils` test and the two build DAGs do not
  *import*. (`#airflow-init-load-bearing-steps`)
- **dbt's three working directories live under `/opt/platform/run`, not the
  `./dbt` bind mount** (`DBT_LOG_PATH`, `DBT_TARGET_PATH`,
  `packages-install-path`): a bind mount keeps host ownership, so `dbt deps`
  fails with `Permission denied`. Packages share a named volume mounted one
  level **above** `dbt_packages`, because `dbt deps` rmtree's that directory.
  (`#dbt-working-directories`, `#dbt-packages-volume`)
- **`landing/` has a contract: everything in it is correctly named and
  classified.** An approved sender writes there directly; everything else goes
  through the **inbox conformance gate** (`ingest/conform.py`, driven by
  `ingest/inbox.py`) — a feed with an `arrival:` block.
- **THE INBOX ESTABLISHES IDENTITY; INGESTION VERIFIES INTEGRITY.**
  `arrival.control` carries `cob_date`/`version` and refuses `row_count`/`md5`
  at load; `delivery.control` carries `row_count`/`md5` and is checked at
  ingest for every delivery however it arrived. Complementary, not
  alternatives: `arrival.control` without `delivery.control` is refused.
- **The control file is PROMOTED, not consumed**, so such a feed has **two
  filenames and they are different strings** — `Feed.claims_source` for the
  upstream's, `Feed.parse_filename` for landing's. The rename is built from
  `filename_pattern` and fed back through `parse_filename`, so a name landing
  would reject cannot be produced.
- **An identity failure is QUARANTINED to `.rejected/`; an integrity failure
  LANDS and fails at ingest** — landing is the evidence copy, and a bad
  delivery is what it exists to prove.
- **`{stem}` IS NOT A WILDCARD: a control file is attributed by its stem.**
  Two feeds may share `.ctl`; two whose data names differ only by EXTENSION
  are refused at LOAD, because at the door nothing can tell their control
  files apart. A `.ctl` matching no feed's data shape is unroutable.
  (`#a-control-file-is-attributed-by-its-stem`)
- **HOW a control file is read is `control.format`; WHAT is read out of it is
  the fields.** `regex` (the default) is a pattern per field over the whole
  text; `delimited` makes every field a COLUMN NAME. `ingest/control.py` is
  the only parser. Both blocks read the same promoted bytes, so a format
  declared on each must be identical, and `delimiter` is never inherited from
  the feed's own. (`#control-file-formats`)
- **THERE ARE TWO ZIP MECHANISMS AND THE COB DATE PICKS ONE.** On each member:
  `arrival.archive`, unpacked AT THE GATE, N deliveries out, container never
  lands. On the container: `delivery.kind: archive`, the zip lands and
  `normalize` explodes it into `ready/` as parts of ONE delivery. Neither
  source, or both, is refused at load.
  (`#unpacking-happens-at-the-gate`, `#archive-normalizer`)
- **A member may carry its own control file inside the container**, which is
  how statically named members get dated; both are promoted and verified at
  ingest. A member's control file MISSING from the container is a refusal, not
  a wait — a container arrives complete.
- **`delivery.control` gates EITHER kind.** For an archive, `row_count` is the
  total across members and `md5` is the CONTAINER's. The manifest's
  `checksum_objects` says which objects a declared md5 covers, so
  `ingest_feed` never branches on the kind — an invariant `ui/arrivals.checks()`
  shares. (`#control-file-gate`)
- **A DELIVERY SHAPE IS A REGISTRY ENTRY, not a branch**:
  `conform.ARRIVAL_SHAPES` at the door (planners returning
  `Planned`/`Refused`/`Duplicate`/`Waiting`, the only four `inbox.py` acts on)
  and `normalize.NORMALIZERS` on the landing side, sharing `normalize._gate`.
  (`#a-delivery-shape-is-a-registry-entry`)
- **`landing/` is the evidence copy; `ready/` is the work queue.** A
  **normalize** stage turns a delivery into a MANIFEST — COB date, the objects
  holding the rows, delimiter/quoting/encoding — which is what `ingest`
  consumes and `find_pending` returns. A plain CSV copies nothing.
- **`_source_file` must stay the PART's key, never the manifest's** —
  `already_ingested` matches on it, so the manifest key re-ingests every
  delivery forever.
- **`find_pending` computes its keep-set from `landing/`, not the manifests**,
  the only prefix holding every date. So `ready:` bounds the **derived parts,
  not the manifests**, or the sweep and the reconcile undo each other nightly.
  `ready/` reconciles from `landing/` on demand, which is also what makes the
  registry rebuildable.
  (`#the-ready-window-bounds-the-parts-not-the-manifests`)
- **A feed is named `<source_system>_<feed>`** (`fo_trade`,
  `ref_counterparty`), TYPED into feeds.yml, not derived. That one string is
  the raw table, DAG id, landing prefix, dbt source table and prepared model at
  once, so none of the five can drift. (`#feed-names-carry-the-source`)
- **`conventions/` is a middle tier**: `_defaults.yml -> convention -> feed`,
  shallow at each layer, chained through `parent:`.
  `context.effective_defaults()` is the ONLY implementation of that ordering
  and the console reads it, or saving a feed pins inherited values into its
  block. An undefined convention or parent, a cycle, an unknown key in one, or
  `name`/`convention` in one are errors at LOAD. (`#feed-conventions`)
- **A column may be named differently in the file than in the platform** —
  `- trade_id: "Trade Id"` renames at ingest, so raw onwards is ordinary
  identifiers and macros never quote one; drift is reported in the file's
  names. (`#source-column-names`, `#identifiers-in-macros`)
- **`generate_feeds.py` is not one of those five files**: it hand-writes the
  four original feeds, whose pathologies are the point, and generates the rest
  via `ui/sampledata.py`. Pass `types=` when calling that directly, or a
  `decimal` gets a string `safe_cast` silently nulls.
- **The feed console (`reporting_platform/ui`, <http://localhost:8082>) writes
  those five files from a form** and drives land → ingest → build. It is a
  front end for the procedure, not a second source of truth. Its one-feed build
  **never merges** — publication belongs to the Airflow builds — and is
  EXCLUSIVE, so it must be able to end: `jobs.stream` kills at
  `FEED_UI_JOB_TIMEOUT` and `DELETE /api/jobs/<id>` cancels, both by PROCESS
  GROUP, or the JVM holding the cores is orphaned. `feeds()` caches on
  **mtime**, or a new feed never reaches the DAG processor.
- **The Arrivals page is a JOIN, written nowhere** (`ui/arrivals.py`): what
  became of every file offered, assembled per request from `registry.delivery`,
  `registry.rejection`, `inbox.route()` and Airflow. A declared **md5** is
  comparable there, a **row_count** is not (`at_ingest`), and `no run recorded`
  means Airflow trimmed its history, never "not ingested".
  (`#the-arrivals-view-is-a-join-not-a-record`)
- **EVERY PREPARED MODEL RANKS THE CLEANED KEY, never the raw one** — clean,
  then `dedupe_rank` in `ranked_rows`, then project
  `prepared_output_columns()` to drop the carried `_cob_date`/`_file_version`/
  `_row_number` (Spark 3.5 has no `SELECT * EXCEPT`). Ranked raw, ` T1` and
  `T1` in one file both survive as one key. The scaffold emits this shape.
- **ADDING A COLUMN TO AN EXISTING FEED IS THE COMMONEST CHANGE A LIVE FEED
  EVER HAS, and the raw table is the part not in the git diff.**
  `ensure_raw_schema` reconciles the declared contract on the branch;
  `ensure_raw_table` is `CREATE TABLE IF NOT EXISTS` and reconciles nothing.
  Without it the write dies with `INSERT_COLUMN_ARITY_MISMATCH`, naming neither
  the column nor `feeds.yml`. A declared column is added; an undeclared one is
  NEVER dropped, because a rename is a drop plus an add.
  (`#a-declared-column-migrates-itself`)
- **`on_schema_change: append_new_columns` in `dbt_project.yml`** — dbt's
  `ignore` default leaves a new column in the SELECT and never in the target,
  green build and all. It does not populate rows the run did not touch, so
  `--full-refresh` is a choice about DATA, not the only way to get the COLUMN.
- **Raw's four provenance columns are added, never backfilled** —
  `_delivery_id`, `_received_at`, `_schema_version`, `_source_system`, through
  `source_provenance()`. History reads NULL, so an as-of query falls back to
  `_source_file`. The migration is LAZY, so a feed that has not delivered
  keeps the old schema and **every prepared model then fails, not just its
  own**: run `ingest.migrate_raw` before the next ingest when deploying one.
  (`#provenance-is-added-not-backfilled`)
- **`supersession:` declares what `dedupe_rank` always assumed.**
  `full_snapshot` is the only built mode and the default; `delta_append` and
  `correction` raise NOT_BUILT at load. The value is the REFUSAL — a delta feed
  deduped as a snapshot silently loses every key its newest file omits.
  (`#supersession-is-declared-not-assumed`)
- **`full_snapshot`: the newest DELIVERY restates its whole COB date, so a key
  it omits is ABSENT.** `dedupe_rank` used to rank per KEY and kept dropped
  keys, uniqueness test green. It gates on the newest `_file_version` per date
  now, and the cob_date-partitioned models are `insert_overwrite`, because a
  MERGE never deletes. That is safe only for a select returning each date
  WHOLE: a query keeping some keys of a date passes `newest_version=`, and
  the SCD2 models stay `merge`. The newest file is load-bearing — a
  truncated re-delivery wipes its date, and `expected_min_rows` and
  `delivery.control` `row_count` are the guards.
  (`#a-snapshot-re-delivery-restates-the-whole-date`)
- **An SCD2 version a replaced delivery began must be RETRACTED, and a MERGE
  cannot delete.** Without it the version stays current and the key's next
  change opens a second one. The SCD2 models emit marker rows
  (`scd2_retractions`, `effective_to = scd2_retracted()`) and set
  `scd2_retractions=true`, which gives them the project's
  `spark__get_merge_sql` in `macros/merge.sql` — delete on the marker, update,
  insert. Every other merge is dbt-spark's. The SCD2 models rank and scope
  the replay AFTER their cleaning — a raw ` B` never matches the target's `B`
  — comparing keys NULL-safely (`scd2_key_match`, merge `on` included, or an
  `'N/A'` key vanishes from incremental builds only), and `scd2_replay` heads
  the replay with the target's version before its start, so retracting the
  start version reopens that one. A key absent from a delivery still
  never CLOSES the version in force; and a date raw no longer holds is never
  a retraction. (`#a-snapshot-re-delivery-restates-the-whole-date`)
- **As-of is a var, not a second model**: the same models with `--vars
  '{knowledge_time: ...}'`, filtered by `known_as_of()`. It compiles to
  `1 = 1` when unset and **refuses an incremental run**, because writing as-of
  rows into the published table restates it backwards.
  (`#as-of-is-a-var-not-a-second-model`,
  `#delivery-ref-is-the-fallback-with-the-prefix-stripped`)
- **`expected_by:` is the ONE lateness concept** — a wall-clock `"HH:MM"`
  judged the day AFTER the COB date. **Quote it**: YAML 1.1 reads `7:00` as
  420, and a leading zero hides that until the first unpadded hour. No
  `expected_by` means skipped, not midnight.
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
  version and every run until the next deployment inherits it, so deployment
  refs come from the ENVIRONMENT, not the trigger. `dbt_project_ref` (declared
  commit) and `dbt_manifest_ref` (digest on disk) diverge whenever the project
  is writable at run time, which the console makes it, so
  `check_project_drift()` compares DIGEST TO DIGEST and refuses only in
  `uat`/`prod`.
- **Adding a registry column needs `MIGRATIONS` as well as `SCHEMA`** — `CREATE
  TABLE IF NOT EXISTS` is a no-op on an existing table, so the column never
  appears and the INSERT fails later, in a task, at publish.
- **AN AS-AT DATE HAS A LIFECYCLE, and `open` is the absence of a row.**
  `registry.as_at_transition` is append-only, `open -> locked -> submitted`,
  reopened only deliberately and — from **submitted** — only with the exposure
  owner's approval. **The publish gate runs BEFORE the merge**, or it fires
  after `main` has moved. Nothing branches on what KIND of report it is
  (REQ-503). (`#the-as-at-date-has-a-lifecycle`)
- **Retention and GC delete data. `dry_run` first, always.** GC defers its
  deletes by design; the deferred-delete pass is the deliberate second step.
- **THE ORPHAN SWEEP'S INPUT IS A KEEP-SET, so a short answer is a deletion
  order.** `orphan_storage` deletes every warehouse prefix not live on some
  reference, so a Nessie that is down reads as "nothing is live". An unreadable
  reference refuses the sweep, an empty live set against a non-empty warehouse
  refuses too, and a refusal exits non-zero. Prefix depth is derived from
  `REPORTING_WAREHOUSE`. (`#an-incomplete-keep-set-refuses`)
- **The completeness check has THREE answers, not two**: `no data` (empty
  table), `no table` (`TABLE_OR_VIEW_NOT_FOUND` — a feed that has never
  delivered), `unreadable` (anything else, and it fails `--fail-on-gap`).
- **A dry run may write to the index; it may not write anything a later step
  reads to decide what to delete.** `registry_reconcile` writes rows but not
  manifests under `dry_run`, and reports `would_normalize` — skipping the task
  is not the fix, because it is the rebuild path.
  (`#a-dry-run-may-write-to-the-index-not-to-object-storage`)
- **A published tag is DATA retention, sized in years, not by the table
  keep-set.** `references.published_tags` is the reproducibility window: a tag
  pins every data file its commit referenced, so its lifetime is how long a
  published run can still be read. `landing.keep_years` must be >= the longest
  tag window and `retention.py` **refuses to sweep** otherwise — a pin
  outliving its evidence cannot be honoured.
  (`#published-tags-are-the-reproducibility-window`)
- **AN INGEST IS NOT A PUBLICATION, and they cut different tags.** An ingest
  pins `snapshot/<feed>/<bd>/<run_id>`; the **reporting build** cuts
  `published/<report>/<bd>/<run_id>`, one per report, when it merges. A
  **report is a dbt EXPOSURE**, derived by `context.reports()` — not a config
  block. (`#an-ingest-is-not-a-publication`)
- **A feed's evidence window is its RETENTION CLASS**, named in `feeds.yml`,
  sized in `retention.yml` per environment, refused at LOAD if undeclared.
  Classes govern `landing/` and `quarantine/` **only** — a per-feed raw window
  would fight `find_pending`. The reproducibility interlock is per (report,
  feed) via `feeds_behind_report()`, so a feed behind no published report is
  bound by no pin. (`#retention-classes-name-the-obligation`)
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
  Lineage is derived by `context.model_refs()` — the SAME walker
  `feeds_behind_report()` sizes retention with, and `tests/test_lineage.py`
  asserts they agree. `registry.run_input` is the publication record, and the
  two legitimately differ: an SCD2 dimension contributes 10 of 40 deliveries
  while OpenLineage reports all 40 as read. Do not reconcile them.
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
  The seam is CI — `lineage --columns --require-derivable` exits 1 on any,
  and only the BUILD tier (`build.yml`) runs it: without a catalog the column
  list falls back to the feed's declared columns, so every column reads
  `sourced` and `unresolved` cannot arise. (`#a-gate-that-cannot-fail`,
  `#the-build-tier`)
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
- **The operational control plane answers "where is this feed right now" —
  Airflow's DAG list does not**, deliberately, under generic ingestion.
  `registry.transport_receipt` is an EVENT record like `run` (a mutable
  status tracking one Transport occurrence through `transport_ingest`);
  `registry.delivery_committed` is an OBSERVATION like `delivery_part` (one
  fact, recorded once, that a Delivery reached Raw — universal across the
  legacy and Transport paths, because `registry.delivery` itself gets a row
  at NORMALIZE time and says nothing about a commit). Neither is a status
  column on `delivery`. `monitoring/feed_status.py` derives COB Feed Status
  from these plus `Feed.expected_by`/`cadence`, fresh on every request — see
  `docs/OPERATIONAL-CONTROL-PLANE.md`.
- **A Delivery ingested before `_delivery_id` existed on its raw table can
  never get a `delivery_committed` row from the backfill** —
  `scripts._spark_task reconcile-committed <feed>` reads that column, and
  raw provenance is added, never backfilled
  (`#a-declared-column-migrates-itself`). Such a Delivery reads PROCESSING
  on the COB Status page until that table is rebuilt or the date ages out.

## Quick reference

- **THE CONSOLE MAY NOT BE THE ONLY THING THAT VALIDATES A VALUE.** `cadence`,
  `schema_drift`, `expected_min_rows`, `delimiter`, `quote_char` and
  `file_encoding` were checked by `ui/registry.validate` and by NOTHING at
  load, so the form refused what a hand edit or a merge could still write.
  They are `check_*` functions in `common/context.py` now, called from both.
  `tests/test_value_checks.py` asserts each from both sides.
- **`.github/workflows/config.yml` is the cheap tier**: `config check` +
  `python -m tests.run` on a bare runner, ~10s.
  Its dependency set (pyyaml, ruamel.yaml, requests, duckdb, jinja2) was
  DERIVED BY RUNNING IT in a clean virtualenv, not read off the imports —
  `test_sniff` imports duckdb and `test_dedupe_rank` jinja2 at module level,
  and either missing aborts the whole run with no summary line.
- **`.github/workflows/parse.yml` is the tier above, and it is SEPARATE so the
  cheap one stays cheap**: the image's pins installed with pip (~2–3 min),
  then `dbt --version`, `dbt deps`, `dbt parse` and
  `python -m scripts.check_dag_imports`. That last is Airflow's own `DagBag`,
  because importing `dbt_builds.py` renders both build DAGs through `dbt ls`
  — so it needs a metadata db (sqlite is enough) and `dbt deps` to have run.
  **An empty DagBag has no import errors either**, so it also asserts every
  file produced a DAG and every feed produced an `ingest_*`.
- **`dbt parse` catches a bad `ref()`, uncompilable Jinja and unloadable YAML
  — and NOT an unknown generic test or an unknown key in a column block**,
  both of which parse green. Measured, not assumed: those resolve when a test
  is BUILT, which is what `build.yml` does.
- **The parse tier's pins are a second copy of `Dockerfile.airflow`'s, and
  `tests/test_ci_pins.py` fails when they diverge** — versions, the provider
  set, the constraint-file URL, `--no-deps` on cosmos and the `dbt --version`
  smoke test after it. It reads the workflow's `run:` blocks as YAML, never
  the file text: matching prose is how its first two versions passed with the
  thing they checked deleted.
- **`lineage --columns` is run by NEITHER cheap tier, and that is not an
  oversight** — only `build.yml`, after a build, runs it. Without compiled SQL and a catalog it reports 7 of 11 tables as
  `not derivable` and exits **0** — and the 4 it does read come from
  `feeds.yml`, every column `sourced`, so it cannot fail. `--require-derivable`
  makes it fail every time instead. `unresolved` is a column it READ and could
  not trace; `not derivable` is a table it could not read, and reporting the
  second as the first is this file's own rule broken by the tool enforcing the
  rest. `dbt parse` writes no compiled SQL, so the parse tier does not change
  this. (`#a-gate-that-cannot-fail`)
- **The jar triple is checked** — `tests/test_value_checks.py`'s sibling
  `tests/test_versions.py` pins `ICEBERG_VERSION` across all **five** files
  that declare it, the extensions across five, the server against the
  `nessie-gc` jar, and the configured pair against a hand-kept list of
  combinations somebody has run. Compatibility itself is upstream's
  build-time fact and is not computed.
- **A declared column that no prepared model selects now fails** —
  `test_lineage.py`, the one drift quadrant nothing covered.
- **Two claims the DOCS make are gated** — `tests/test_doc_claims.py`. A
  paragraph saying something is NOT BUILT must name a value
  `context.NOT_BUILT`/`SUPERSESSION_NOT_BUILT` still lists, or be written as
  history; a `` `"..."` `` quoted error must be one a module still raises.
  Both go false SILENTLY, because building the feature is what falsifies them
  and whoever builds it is reading code, not the docs. Backticked identifiers
  are deliberately NOT checked — too noisy to gate.
- **`.github/workflows/build.yml` is the BUILD tier**, and the only one that
  builds: images, a throwaway stack (`.github/compose.ci.yml`, no host
  ports), a `--clean` seed plus the qa_ fixtures through the inbox gate,
  `bulk_ingest`, `dbt build` on a branch, merge to that stack's `main` if
  clean, then `lineage --columns --require-derivable`. Path-filtered PRs and
  nightly. It is `scripts/ci_build_tier.sh`, so a developer runs exactly what
  CI runs, beside their own stack (`-p rp-ci`). The lineage gate needs the
  MERGE: DuckDB reads only `main`. (`#the-build-tier`)
- **Still ungated**: nothing builds the images in the cheap tiers, and the
  build tier builds them but does not push or scan them. A cluster run is
  the local-k8s smoke test, not CI.

```powershell
# config-level tests: registry resolution + the console's write-back.
# No stack, ~6s. Everything else is verified by running it. tests/README.md
python -m tests.run

# every DAG file imports, and produced the DAGs it should have. What CI's
# parse tier runs; in the container it needs no argument.
docker compose exec -T airflow python -m scripts.check_dag_imports

# what the registry resolves to, and WHICH TIER each value came from.
# No stack. `check` exits 1 if the config will not load -- the CI seam.
python -m reporting_platform.config list
python -m reporting_platform.config show fo_trade --origin
python -m reporting_platform.config check

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

# COB Feed Status: every feed, grouped by source system, derived status for
# one COB date. No Spark, no Airflow calls. Same report the feed console's
# COB Status page renders.
docker compose exec -T airflow python -m reporting_platform.monitoring.feed_status --cob-date 2026-09-21
# one-time per feed after deploying it: recover delivery_committed facts for
# Deliveries ingested before that table existed, from Raw itself.
docker compose exec -T airflow python -m scripts._spark_task reconcile-committed <feed>

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
# ...and 1 on any table it could not READ, which is a different fact and the
# ordinary state of every model before a build. This is the post-build gate;
# without it `unresolved: none` is also what 7 unreadable tables look like.
docker compose exec -T airflow python -m reporting_platform.lineage --columns --require-derivable
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
