# Decisions

Why the code is shaped the way it is.

Almost everything here was learned by running the stack and reading what it
actually said, not by design. That is worth keeping: a constraint you cannot see
is one someone removes, and most of the entries below exist because someone
already tried the obvious thing and it failed in a way that did not name itself.

**The code points here rather than repeating it.** A line like

```yaml
# See docs/DECISIONS.md#dbt-packages-volume
- dbt-packages:/opt/platform/run/packages
```

means the reasoning lives in the matching `##` section below. Anchors are
stable; if you rename one, grep for it first.

Read `CLAUDE.md` for the short version of the rules that bite most often, and
`docs/ARCHITECTURE.md` for how the pieces fit together.

---

## jar-versions

Three version values, in `.env`, reaching six consumers. The split is the
finding, not pedantry.

| Variable | Sets |
|---|---|
| `ICEBERG_VERSION` | the Iceberg runtime: baked into the Spark image's `/opt/spark/jars`, and resolved by both drivers |
| `NESSIE_SPARK_EXT_VERSION` | the Nessie Spark SQL extensions, in the same three places |
| `NESSIE_SERVER_VERSION` | the Nessie server image tag and the `nessie-gc` jar |

`ICEBERG_VERSION` has to be identical in three places — `Dockerfile.spark`, and
*both* drivers (`spark_session()` in `common/context.py`, `spark.jars.packages`
in `dbt/profiles.yml`) — because every process that submits work here runs a
pip-installed pyspark with no jars of its own. `spark.jars.packages` ships the
driver's jars to every executor, so what the executors load is what the driver
resolved. Diverge and you get two Iceberg versions in one application, which
surfaces as `NoSuchMethodError` on the first write rather than as anything
saying "version".

`NESSIE_SPARK_EXT_VERSION` tracks **Iceberg, not the server**. The extensions
jar is compiled against a specific Iceberg — 0.103.3 against 1.8.1, 0.108.1
against 1.11.0 — and running one built against a *newer* Iceberg than you
actually have is the direction that fails. Pick the newest extensions release
built against an Iceberg no newer than yours.

`NESSIE_SERVER_VERSION` is allowed to be newer than the extensions: Nessie's
REST API v2 is stable across that range. That is what makes a
security-mandated server bump possible without moving the Spark jars.

`hadoop-aws` and `aws-java-sdk-bundle` are deliberately not parameterised. They
track Spark 3.5's Hadoop, not Iceberg.

Defaults everywhere repeat the combination the stack was validated against, so
a clone with no `.env` builds what it always built. `docker compose exec
spark-worker env | grep VERSION` reports what is actually baked into the image
you are running — guessing that from a Dockerfile you have not rebuilt is how
versions drift in the first place.

## spark-jars-prebaked

The Spark image bakes its jars rather than resolving `--packages` at submit
time. In the cluster they come from the internal registry; baking them means no
egress at runtime.

The drivers still resolve via Ivy (see [jar-versions](#jar-versions)) because
they run pip-installed pyspark, which has none of these jars — the first
Iceberg SQL statement would fail with `ClassNotFoundException` before it ran.

## nessie-gc-jar

There is no server-side GC endpoint and no REST call for it. Collecting content
unreachable from *any* Nessie reference is only possible with this external CLI,
and `docs/MAINTENANCE.md` has it as step 3 of the nightly chain.

It is published as a GitHub release asset, **not** on Maven Central under this
name. Its version comes from `NESSIE_SERVER_VERSION` so it cannot drift from the
server: a CLI that disagrees with the server about the repository format is the
one component here that can quietly delete the wrong thing. From Nessie 0.107.0
it needs Java 17; the JRE in the image is 17.

At ~128 MB it is most of the Airflow image's size.

## airflow-2-not-3

Airflow is 2.10.5 deliberately. This was 3.0.2, and under 3.0.2 no DAG run on
this stack could ever complete: a task would start, log correctly, return its
value and push xcom, and the scheduler would never record it as succeeded, so
nothing downstream ran. That survived splitting `standalone` into separate
components, wiring `EXECUTION_API_SERVER_URL`, and sharing a JWT secret.

Airflow 3 routes every running task through an execution API served by the
api-server, authenticated with JWT. Airflow 2's LocalExecutor forks the task
in-process and writes the result straight to the metadata DB — far fewer moving
parts between "task finished" and "state recorded", and no component that can
silently fail to acknowledge a completion.

The DAG code was already written to run on both (`airflow.sdk` with an
`airflow.datasets`/`airflow.decorators` fallback, `Asset` aliased to `Dataset`),
so this is an infrastructure change only.

Related: the services are split into real components rather than `airflow
standalone`, which bundles everything into one supervised process and proved too
unreliable to prove scheduling on. The service that runs tasks is still called
`airflow`, because under LocalExecutor tasks execute inside the scheduler
process and every `docker compose exec -T airflow ...` in the README, Makefile
and `CLAUDE.md` expects to land in the container with the code mounted.

## airflow-provider-constraints

Providers install under Airflow's own constraint file so pip cannot quietly drag
in a different Airflow version to satisfy them.

dbt and pyspark are deliberately **not** constrained by that file — their pins
are the ones validated against a live stack and must not move.

The exception is `duckdb`, bumped 1.1.3 → 1.5.5 for the DuckDB console. 1.1.3's
iceberg extension has no catalog `ATTACH` at all:

```
ATTACH ... (TYPE ICEBERG)
Binder Error: Unrecognized storage type "ICEBERG"
```

1.5.5 attaches to an Iceberg REST catalog and reads it, which is what
`scripts/duckdb_console.py` needs.

There is no `dbt-duckdb`, on purpose — see
[duckdb-is-not-an-engine](#duckdb-is-not-an-engine).

## cosmos-no-deps

`astronomer-cosmos` is installed `--no-deps`, and that is not an optimisation.

Installing it the way the providers are installed — under Airflow's constraint
file — **breaks dbt**. The 2.10.5 constraints pin `typing_extensions==4.12.2`;
the dbt layer resolves `mashumaro` 3.22, whose `pack.py` imports
`evaluate_forward_ref` from typing_extensions 4.13+. Cosmos depends on
typing-extensions, so the constrained install downgrades 4.16.0 → 4.12.2 and
every subsequent `dbt` invocation dies at import with

```
ImportError: cannot import name 'evaluate_forward_ref' from 'typing_extensions'
```

— in dbt, not in cosmos, and not until something runs dbt. Verified by building
the image both ways and running `dbt --version`.

So: install cosmos alone and add its two genuinely-new dependencies by hand.
`pip install --dry-run` against the image showed the whole delta was `aenum` +
`deprecation` (plus that typing_extensions downgrade); everything else cosmos
wants — airflow, attrs, packaging, msgpack, pydantic, virtualenv — is already
present at a version it accepts. **Re-run that dry run before moving
`COSMOS_VERSION`.**

The image runs `dbt --version` as a build-time smoke check so this cannot ship
silently again.

Cosmos invokes the `dbt` **executable** (see `ExecutionConfig.dbt_executable_path`
in `airflow/dags/dbt_builds.py`) rather than importing dbt-core, so the validated
`dbt-core==1.8.7` / `dbt-spark==1.8.0` pins stay authoritative.

## marimo-not-jupyter

Marimo rather than Jupyter because its notebooks *are* Python files — they diff,
review and merge like any other source in this repo, where a Jupyter `.ipynb` is
a JSON blob carrying outputs and execution counts that make every save a
conflict.

Marimo itself is pinned but its dependencies are not: `pip install --dry-run`
showed it adds only new packages (jedi, msgspec, narwhals, pyzmq and friends)
and moves nothing already installed — notably **not** typing_extensions, which
is the one that broke dbt when cosmos went in under a constraint file. Re-run
that dry run before moving the pin.

It runs as `python -m marimo`, not `marimo`: the Airflow image's entrypoint
execs a short list of commands directly (python, bash, …) and assumes anything
else is an airflow subcommand, so a bare `marimo` becomes `airflow marimo` and
dies with `invalid choice: 'marimo'`. `feed-ui` runs `python -m
reporting_platform.ui` for the same reason.

## dbt-working-directories

All three of dbt's working directories live under `/opt/platform/run`, outside
the `./dbt` bind mount: `DBT_LOG_PATH` and `DBT_TARGET_PATH` in
`docker-compose.yml`, `packages-install-path` in `dbt_project.yml`.

A bind mount takes its ownership from the **host**, so no `chown` in the image
can reach it. On Docker Desktop for Windows the host side presents 0777 and
everything works by accident; on a host where the checkout is not owned by uid
50000, `dbt deps` in `airflow-init` dies with `Permission denied:
'dbt_packages/dbt_utils'` before anything else can run.

The directories are created in `Dockerfile.airflow` as `airflow:0` with
`g+rwX` — group 0, not uid 50000 alone and not `chmod 777`. The container runs
`uid=50000 gid=0`, and group 0 is what stays writable when the platform assigns
an arbitrary uid, which is exactly what OpenShift does.

`logs` and `target` are per-container: image paths with nothing mounted over
them, so each service gets its own and two dbt processes cannot interleave their
output. `dbt_packages` cannot work that way — see
[dbt-packages-volume](#dbt-packages-volume).

## dbt-packages-volume

`airflow-init` runs `dbt deps` once and the scheduler, webserver, triggerer and
feed console all read the result, so the packages directory must be **shared**.
Two approaches fail, and both look correct until they run:

- **A named volume mounted at `./dbt/dbt_packages`.** `dbt deps` rmtree's the
  packages directory before reinstalling, and a mount point cannot be removed:
  `OSError: [Errno 16] Device or resource busy: 'dbt_packages'`. That fails on
  every `docker compose up`, on every host.
- **A plain image directory**, alongside logs and target. `/opt/platform/run` is
  copy-on-write per container, so `airflow-init` installs into a layer that is
  discarded when it exits, `dbt deps` reports success, and every other service
  then says `dbt found 1 package(s) specified in packages.yml, but only 0
  package(s) installed in ...`.

What works is a named volume mounted **one level above** — on
`/opt/platform/run/packages` — shared by every service, with `dbt_packages` an
ordinary removable subdirectory inside it, named by `packages-install-path`.

The volume is seeded once, at creation, from the image's ownership of
`/opt/platform/run/packages`. An existing volume is **never** re-seeded, so if
it ever comes back root-owned the fix is `docker volume rm
reporting-platform_dbt-packages`, not another rebuild. The contents are
disposable: `airflow-init` reinstalls them on the next `up`.

## image-permissions-layer

The permissions layer is last in `Dockerfile.airflow` on purpose. It changes far
more often than the pip installs above it, and Docker invalidates every layer
after the one that changed — put it higher and editing a directory list costs a
full reinstall of Airflow's providers, dbt, pyspark, cosmos and marimo through
whatever registry mirror is in front of pip. Nothing below it depends on it.

## airflow-init-four-things

`airflow-init` runs once and everything else waits on it *completing*, so no
component races the database into existence. It does four things, each of which
was once a manual step that silently broke the platform when skipped:

1. `db migrate` — the metadata schema. (Unlike Airflow 3, `airflow users` exists
   here.)
2. `users create` — admin/admin for the web UI and the REST API the feed console
   calls.
3. `pools set` — **one** pool, at one slot. Ingest, dbt model tasks and
   maintenance all take this same slot; a second one-slot pool would *not*
   exclude them from each other, which is the bug that once let
   `remove_orphan_files` run alongside a write. Without it every task sits
   `queued` forever with nothing to say why.
4. `dbt deps` — no longer merely "the build fails until you run it". Cosmos
   renders `prepared_build` and `reporting_build` by running `dbt ls`, which
   cannot compile a `dbt_utils` test without the package, so on a fresh clone
   those two DAGs would not **import**. Installing here makes the clone
   self-sufficient.

`|| true` on the user and the pool: both are idempotent in intent but noisy on a
second `docker compose up` against existing volumes, and this container failing
would block the whole stack.

## airflow-api-auth

`session` alone is the Airflow 2.x default and only authenticates a browser that
has logged into the web UI, so an API call from another container gets 401 with
a perfectly healthy webserver. `basic_auth` is added and `session` kept so the
UI still works. Same admin/admin as the web UI: this is the local stack, and
anything shared needs a real identity layer in front of the console regardless.

The webserver secret key is shared across replicas and restarts so sessions
survive. Airflow 2 needs nothing like Airflow 3's execution-API URL or JWT
secret — see [airflow-2-not-3](#airflow-2-not-3).

## spark-master-single-source

Every Spark job runs on the `spark-master`/`spark-worker` cluster, never
`local[*]`. The master comes from `SPARK_MASTER` in two places that must not
diverge: `spark_session()` in `common/context.py` and `spark.master` in
`dbt/profiles.yml`. `spark_session()` refuses a `local` master rather than
quietly running the pipeline inside the Airflow container with the cluster idle.

`feed-ui` sets it explicitly rather than relying on the default, because both
readers default to the same address — which is exactly the silent divergence
worth avoiding.

## spark-worker-sizing

The worker is sized so several applications can hold cores at once. Every Spark
job on this platform is a *client* of this cluster — ingest, dbt builds,
maintenance, retention, arrival checks, completeness — and a standalone
application holds its cores from its first job until the session stops.

Each application caps itself at 2 cores / 2g (`spark.cores.max` in `context.py`
and `dbt/profiles.yml`), so 6 cores / 6g leaves room for three concurrent: the
one `lakehouse_write` slot plus the read-only jobs outside that pool. Without
the cap, standalone mode grants every free core until the session stops and the
next job waits forever instead of failing.

The driver runs in the calling container and does no task work, so it needs far
less heap than the old `local[*]` session — but not the 1g default, which is
tight once Iceberg/Nessie/aws-sdk-bundle classes are loaded and exercised across
repeated catalog operations.

`spark.driver.host` is left at its default: Spark advertises the container's
hostname and Docker's embedded DNS resolves it from `spark-worker`, so executors
can call back. Verified live — a task scheduled on the worker returned its
result to a driver advertising the raw container id.

## minio-host-ports

The published MinIO ports are the **host** side only. The container keeps
9000/9001, which is what `S3_ENDPOINT: http://minio:9000` and every other
service address uses; container-to-container traffic never goes near the host
mapping, so changing them cannot break the pipeline.

They default *away* from 9000/9001 because those collide with things people
actually run: ZScaler on a corporate laptop takes them, and so do a fair number
of local dev servers. A clash shows up as a container that will not start, or
worse, a console that answers with something else entirely.

Every other host port is overridable the same way, via `*_HOST_PORT`. Note the
suffix: `FEED_UI_PORT` already exists and sets the port the console binds
*inside* its container — setting that one would move the listener out from under
the mapping.

## nessie-iceberg-rest

Nessie 0.99 already serves an Iceberg REST catalog at `/iceberg/v1`, but it
answers every request with `Warehouse 'x' is not known` until a warehouse is
declared. It is a **second front door onto the same version store**, not a
second catalog: Spark keeps using the Nessie API at `/api/v2` and sees exactly
the same commits.

It exists for `scripts/duckdb_console.py`, the read-only query tool. DuckDB's
iceberg extension cannot speak the Nessie API; it speaks Iceberg REST. It is not
used by the pipeline, which goes through `NESSIE_URI`.

The credentials are the same MinIO ones the rest of the stack uses, referenced
through `nessie.catalog.secrets` rather than inline because Nessie rejects the
inline form.

The version store is JDBC-backed rather than in-memory so the local stack
exercises the same path as the cluster and survives a restart.

## feed-ui-same-image

The feed console runs from the same image as Airflow because it imports
`common.context`, `ingest.arrival` and the dbt project layout directly — there
is one definition of a feed and the console reads it rather than describing it
again. A separate slimmer image would have to duplicate the platform package and
could then be built against a different version of it.

Its bind mounts are read-write, unlike `spark-master`/`spark-worker`: the whole
point is that it edits `reporting_platform/config/feeds.yml`, the dbt project
and `seed/`. Those edits land in the working tree on the host and show up in
`git diff`, which is what makes a feed added there reviewable as an ordinary
change.

It does not `depends_on` airflow: the console is useful with the scheduler down
(it can still register and scaffold a feed) and says so in its header rather
than refusing to start.

It renders host-side URLs for the **browser**, so those must be the host
mapping, which nothing inside the network otherwise knows. Hardcoding them in
`index.html` meant a changed port sent people to whatever else was listening.

## notebook-service

A developer sandbox for "what is actually in these tables" — landing CSVs, raw,
prepared and reporting, all through one read-only DuckDB connection
(`scripts/duckdb_console.connect`), so a question costs a second rather than a
22s SparkSession.

Same image as Airflow because it imports that `connect()` rather than restating
the catalog wiring, and reads the same `feeds.yml`. It writes **nothing**: the
attach is `READ_ONLY` and DuckDB can only ever see the catalog's default branch,
so it cannot touch a build in flight.

`./notebooks` is mounted read-write on purpose — a developer editing the
notebook is editing a file in the working tree, and that edit shows up in `git
diff` like any other change.

`REPORTING_DUCKDB_S3_SECRET` is set because the Iceberg `ATTACH` vends
credentials for the catalog's own data files but **not** for a direct `s3://`
read, so landing CSVs 403 without it.

## watchdog-independent

Deliberately not an Airflow service, and deliberately not in `depends_on` with
any of them: a monitor that shares the lifecycle of the thing it monitors cannot
report that thing being down, which is exactly why `storage_report` never
noticed housekeeping had not run.

It uses the same image only because that is the image with the code and the
drivers; it imports no airflow module and talks to Postgres, Nessie and MinIO
directly. `docker compose stop airflow` leaves it running and complaining, which
is the whole point.

Its history file is a bind mount, not a named volume: a fresh named volume is
created root-owned and the image runs as `airflow`, so the file could not be
written (`Errno 13`). A bind mount also puts the trend history somewhere a human
can read without entering a container.

## seed-clean

`seed_clean/` is an optional clean restatement of the same history, produced by
`generate_feeds.py --clean --version 2`. It exists to demonstrate a build that
*passes* its tests and can therefore be published to `main`.

The default seed injects two data-quality failures on purpose, so a build
against it correctly refuses to publish. Both are useful; know which one you are
looking at.

## duckdb-is-not-an-engine

Every dbt target is a Spark one, and that is a constraint rather than a
preference. A build must land on a Nessie branch -- write-audit-publish is the
whole safety model -- and only the Spark path can address one. `dbt_builds.py`
refuses a non-Spark `DBT_TARGET` rather than silently writing to `main`.

A `duckdb_local` target briefly existed and got dbt-duckdb building Iceberg
tables into Nessie. It was removed anyway, because DuckDB fails as a build
engine on three independent counts, any one of which is disqualifying:

- it can only ever address the catalog's default branch;
- it silently drops `partition_by`, so it cannot reproduce the partition spec
  retention depends on;
- it cannot INSERT to a partitioned table without an explicit override.

`dbt-duckdb` was uninstalled with the target. With no DuckDB target left the
adapter was dead weight that made `dbt --version` report `duckdb: 1.9.6 - Not
compatible!` at anyone debugging -- a misleading signal for a package nothing
used.

DuckDB remains as a **reader for people**, not an engine for the pipeline:
`scripts/duckdb_console.py` opens a read-only session against published `main`.
It is a script rather than a dbt target on purpose -- the engine macros are
Spark-only, so a DuckDB target could not compile the models anyway, and a target
that works for some models and not others is a trap.

## dbt-spark-session-mode

Both dbt targets use `method: session`, which builds an in-process SparkSession
via dbt-spark's `SessionConnectionWrapper`, and turns `server_side_parameters`
into `.config(k, v)` calls on that builder.

For `spark_local` that is what puts the build on the cluster: dbt is the
*driver* inside the Airflow container, and every task runs on `spark-worker`.
Without `spark.master` the builder defaults to `local[*]` and the cluster sits
idle while the build quietly succeeds in-process.

The rest of the catalog and jar wiring has to be repeated in that target rather
than relying on `conf/spark-defaults.conf`, which is not mounted into the
Airflow container -- and the driver needs the jars regardless of what the
executors have baked in.

For `spark_ocp`, dbt runs inside the driver pod that `spark-submit` created for
the build, so there is one SparkSession per build and the Nessie ref is
unambiguous. That target carries far less config on purpose: a driver pod built
from the Spark image does have `spark-defaults.conf`, so only the per-run
override belongs there. Duplicating the rest would be a forked copy that drifts.

A connection method other than `session` was tried there and removed: it served
no purpose the design had chosen and carried a silent-failure risk on the branch
guarantee.

`host` is inert in session mode but dbt-spark's credential validation demands it
for every method -- `dbt parse` fails with "Must specify `host` in profile"
without it.

## no-unused-config-paths

`dbt_project.yml` has no `seeds:` block, and `docs/ADDING-A-FEED.md` says not to
add a `raw:` one under `models:`, for the same reason: there is no `seeds/`
directory and no `.csv` in this project -- reference data arrives as a feed like
everything else -- so a `seeds: {reporting_platform: ...}` block configured
nothing and made dbt print

```
[WARNING]: Configuration paths exist in your dbt_project.yml file which do not
apply to any resources. There are 1 unused configuration paths: - seeds...
```

on EVERY invocation: `parse`, `ls`, `run`, `test`, and once per Cosmos-rendered
task. A warning that is always there is a warning nobody reads, including the
next real one.

Add the block back in the same commit that adds the first seed file.

## raw-is-a-source

`dbt/models/raw/` deliberately contains no models. dbt does not build the raw
layer and cannot: `ingest_feed.py` creates the table (`ensure_raw_table`) and
writes it (`df.writeTo(...).append()`), per file, on its own Nessie branch,
driven by arrival rather than by a build. The load is imperative -- schema
reconciliation into `_extra_columns`, a `MAX+1` `_file_version` lookup,
`_row_number` over file order, an abort below `expected_min_rows` -- not a
SELECT, so there is nothing there for dbt to materialise. In dbt's terms raw is
a **source**: data that arrived by other means.

It lives in its own folder anyway so the file tree mirrors the layer model in
`docs/ARCHITECTURE.md` (raw -> prepared -> reporting) rather than filing raw
under the layer that happens to consume it. dbt scans every path under
`model-paths` for YAML, and source config in `dbt_project.yml` is keyed by
project name rather than by directory, so the location is free.

**Do not add a `raw:` key under `models:`** in `dbt_project.yml` to match the
other two layers. That key configures *models* in a directory; with none there
it applies to nothing and dbt warns about it on every invocation -- see
[no-unused-config-paths](#no-unused-config-paths).

There is also no `database:` on the source on purpose. dbt-spark's
`SparkRelation` raises `Cannot set database in spark!` whenever `database` is
set and differs from `schema` -- it only supports a two-level `schema.table`
namespace. `spark.sql.defaultCatalog=lakehouse`, set in `profiles.yml`, makes
unqualified `raw.trade` resolve against the lakehouse/Nessie catalog instead.

## scd2-is-current

`is_current` on the prepared SCD2 tables means "the live **version** of this
record", not the older business meaning of "in force on the delivered date".

The business version was a function of `business_date`, which an SCD2 row does
not have. That definition lives on as the `limit_in_force()` macro.

## cosmos-rendered-builds

The build tasks are not two hand-written `dbt run` / `dbt test` subprocess
calls. `DbtTaskGroup` reads the dbt project and emits **one Airflow task per
model**, wired in the models' own `ref()` order, plus a test task -- so a broken
model is a red task carrying that model's name rather than a 4000-character log
tail, and a clear-and-retry restarts from the model that failed instead of from
the top of the layer.

Nothing about the *shape* of the build changed: branch -> build -> test ->
merge-only-if-clean, with the branch retained on failure. Cosmos supplies the
middle; `open_branch` and `publish` are the same tasks they always were.

**Adding a model requires no DAG edit.** The graph is derived from the dbt
project on every DAG parse, so a new `.sql` under `models/prepared/` appears as
a new task in `prepared_build` by itself, the same way a new entry in
`feeds.yml` appears as a new ingest DAG. That symmetry is the point.

## cosmos-load-bearing-settings

Four settings in `airflow/dags/dbt_builds.py` are load-bearing.

**`InvocationMode.SUBPROCESS`.** Cosmos defaults to `DBT_RUNNER`, which invokes
dbt *in the calling process*. The dbt target is `method: session` -- dbt builds
a SparkSession -- so `DBT_RUNNER` would leave a JVM with non-daemon threads
inside the Airflow task process, heartbeats would stop, and the scheduler would
zombie-reap the task ~300s after the work had already succeeded. Same constraint
that puts every other Spark call behind `scripts/_spark_task.py`.

**`pool="lakehouse_write"` on every rendered task.** One dbt invocation is one
Spark application, and each caps itself at 2 cores against a 6-core worker.
Per-model tasks mean Airflow would otherwise start several at once and the
cluster would hand out cores until nothing could get a full share -- standalone
mode grants free cores on request and holds them until the session stops, so the
losers wait forever rather than failing. The single pool slot serialises them
exactly as the old monolithic `dbt run` did by holding that slot for its whole
duration.

**`LoadMode.DBT_LS`.** `LoadMode.CUSTOM` (Cosmos's own parser, no dbt
invocation) looks attractive because it is fast and touches no adapter -- but on
this project it emits **every test twice**, once under a bare id and once under
a `test.dbt.` one, which would collide as Airflow task ids, and it misses
model-level tests entirely: the `dbt_utils.unique_combination_of_columns` blocks
that prove `dedupe_rank` works never appear. Verified by loading the graph both
ways. `DBT_LS` shells out to real dbt, finds all 51 tests, and does not connect
to Spark -- `dbt ls` resolves the profile without opening a session. It costs
~5s per DAG parse, which Cosmos caches against a hash of the project files.

**`TestBehavior.AFTER_ALL`**, not the `AFTER_EACH` default and not `BUILD`.
Every rendered task is a separate dbt invocation and therefore a separate Spark
application with its own ~30s session startup. `AFTER_EACH` would render one
task per *test* -- 51 of them -- and the layer would spend most of an hour
starting and stopping JVMs. `BUILD` (model and its tests in one `dbt build` per
node) is wrong for a second reason: under eager indirect selection a
`relationships` test is pulled in with the model it is declared on, but its
OTHER parent may not have been built yet -- `primary_limits`' relationship to
`counterparty` is not a dependency of the *model*, so Cosmos has no reason to
order them. Under cautious selection that test is silently dropped instead,
which is worse. Testing the whole layer once, after it is whole, has neither
problem. Overridable via `COSMOS_TEST_BEHAVIOR` so a developer can flip to
`AFTER_EACH` while chasing one failing test.

## cosmos-packages

dbt packages are installed **once** by `airflow-init`, not per task:
`install_dbt_deps` would make every rendered task run `dbt deps` against the
network before doing any work.

`copy_dbt_packages` is `False`. It was `True` while packages lived in the
project directory, to carry them into the temporary project Cosmos builds for
each task -- without them that directory has no `dbt_utils` and every
`dbt_utils` test fails to compile. It is `False` now because
`packages-install-path` is **absolute** (see
[dbt-packages-volume](#dbt-packages-volume)). Cosmos resolves that key against
the project folder to find what to copy, and joining a folder with an absolute
path yields the absolute path itself, so the copy would have the same source and
destination. Nothing needs copying: the path is identical inside every process
in the container, so the dbt subprocess in the temporary project resolves it
directly.

Verified with a Cosmos-shaped temporary project whose `dbt_packages` symlink
pointed at an **empty** directory: every `dbt_utils_*` test still resolved.

## cosmos-profile-config

One `ProfileConfig` for everything: the committed `dbt/profiles.yml`, used
as-is. Cosmos can also *synthesise* a profile from an Airflow connection
(`profile_mapping`), and that is deliberately not used -- `profiles.yml` carries
about thirty `server_side_parameters` lines of Iceberg/Nessie/S3A wiring, and a
second generated copy of that in the Airflow connections table is a forked
definition that drifts. There is one profile, it is in git, and dbt on the
command line and dbt under Cosmos read the same file.

## cosmos-emit-datasets

`emit_datasets=False`, or Cosmos attaches a Dataset outlet to every model task.

The cascade in this platform is deliberately **layer-grained**: the `prepared`
asset means "the whole prepared layer is published and merged to main", which is
emitted by `publish` and is the only thing `reporting_build` should react to.
Per-model datasets would fire on a branch, before any audit, and before the
merge.

## cosmos-exclude-exposures

dbt `exposures` are documentation -- they declare who *consumes* a mart and
build nothing. Cosmos has no converter for them and logs `Unavailable conversion
function for <DbtResourceType.EXPOSURE>` on every DAG parse, for each one.
Dropping them at selection time is honest about what they are and keeps the
parse log readable; they are still rendered in `dbt docs`, which is where they
belong.

## dbt-target-guard

`dbt_builds.py` refuses a non-Spark `DBT_TARGET` at **import time**.

The failure it prevents is silent. The branch each build opens is passed to dbt
as the `nessie_ref` var, and only the Spark profiles honour it; an engine that
cannot address a Nessie branch ignores it and writes to the catalog's default
branch instead. The build would then **succeed**, having written to `main` with
no branch, no audit and nothing red anywhere.

The fallback value matters for the same reason. It used to be `duckdb_local`,
which was harmless only because `duckdb_local` was broken -- an unset
`DBT_TARGET` crashed loudly. Fixing that target would have turned the loud
failure into a silent one.

The check lives at import time rather than inside a task because Cosmos builds
the dbt command itself, so there is no single call site to guard. A bad
`DBT_TARGET` becomes a DAG import error visible in the UI rather than a green
run that published to main.

## assets-are-or-not-and

A bare list is **AND** in Airflow: `schedule=[a, b, c]` waits until every one of
them has a new event since the last run. That is the opposite of what this
platform needs -- `docs/ARCHITECTURE.md` says "triggered by ANY upstream asset",
"No feed waits for any other feed to arrive", and "a feed that is late does not
block the ones that arrived". With a list, one late feed silently holds up every
build, which is exactly the batch window the per-feed design exists to remove.

Verified against the live scheduler: with `schedule=[trade, cpty, rating]`, an
ingest of trade alone emitted its dataset event and `prepared_build` never
fired.

`any_of()` reduces with `|`, which yields `DatasetAny`/`AssetAny` on Airflow
2.9+ and 3.x. If that is unavailable the list is returned unchanged **and a
warning is logged**, because degrading to AND silently is how this was missed in
the first place.

## retry-delay

Retry delay is seconds, not the five minutes it used to be.

Five minutes is a sensible production number -- it waits out a transient cluster
or catalog blip without hammering it. On a laptop it is dead time: the whole
prepared build is about three minutes, so one retried task doubled the wall
clock of the thing you were watching, and a mid-graph failure left the rest of
the graph parked behind the pool for longer than the build itself takes.

Env-var'd via `AIRFLOW_RETRY_DELAY_SECONDS` rather than hard-coded, so the
OpenShift deployment can put its own number back without a code change. The
default is the local-stack one, because this repo *is* the local stack.

## spark-in-a-subprocess

Anything running Spark inside an Airflow task must go through
`scripts/_spark_task.py`, as a subprocess.

An in-process SparkSession makes the task hang after it returns: the JVM's
non-daemon threads keep the process alive, heartbeats stop, and the scheduler
reaps the task as a zombie ~300s later even though the work succeeded.

This is still true on the cluster -- the *driver* is what lives in that process.
In OpenShift the subprocess becomes a `KubernetesPodOperator` issuing
`spark-submit`: same module, same arguments.

It is the same constraint that forces `InvocationMode.SUBPROCESS` in Cosmos --
see [cosmos-load-bearing-settings](#cosmos-load-bearing-settings).

## log-tail-plus-head

Failed subprocesses report the **head of the last traceback as well as the
tail**.

A tail alone is not enough to diagnose. A `Py4JJavaError` carries a Java stack
far longer than the tail budget, so the exception *message* -- the only line
that says what went wrong -- falls off the front and the log shows nothing but
Java frames. That cost a full re-run by hand to read the `ValidationException`
behind it.

## one-shared-write-pool

Every task that touches table files -- ingest, the dbt model tasks, and
maintenance -- holds the **same** one-slot `lakehouse_write` pool. That is what
prevents `remove_orphan_files` running underneath an in-flight write, which
corrupts the table.

It must stay **one** pool. An Airflow task belongs to exactly one pool, so
splitting maintenance into its own one-slot pool does *not* exclude it from
writers -- two one-slot pools happily run in parallel with each other. That was
the original arrangement (a separate `iceberg_maintenance` pool) and it left the
corruption window open while looking deliberate. `max_active_runs=1` on the
housekeeping DAG already prevents it colliding with itself, so the second pool
bought nothing even on its own terms.

The accepted cost: a feed arriving mid-compaction queues behind it rather than
running concurrently. That is the right trade -- maintenance is scheduled after
the last publication of the day, and a feed is late, not lost
(`arrival_timeout_hours: 26`).

Without the pool, every task sits `queued` forever with nothing to say why,
which is why `airflow-init` creates it -- see
[airflow-init-four-things](#airflow-init-four-things).

## gc-lag-and-assertions

Identification and removal are two different steps, a deferral window apart: the
Nessie GC sweep records what is collectable, and a later run deletes it.
Reclamation is therefore **lagged by design**, and `storage_report` cannot
assert that bytes fell tonight.

Two consequences in `platform_housekeeping.py`:

- A night whose eligible live-sets held nothing removes nothing. That is correct
  and expected, so it logs at INFO, not WARNING. A standing warning that means
  "working" trains people to ignore it -- including the next real one.
- The live-set assertion is a **machinery** assertion, not a deletion one, which
  is why it runs *before* the dry-run return. A dry run must not be held to
  assertions about deletion -- but this is not one. Retention deliberately
  swallows a deferred-delete failure so the rest of the chain still completes,
  so nothing else would notice the mechanism rotting. A dry run still lists the
  live-sets, so an error here means the GC database or the tool is unreachable,
  which is exactly as broken on a dry run as on a real one, and is the cheapest
  possible place to find out.

Expiring snapshots before expiring tags reclaims nothing while appearing to
succeed, which is why the order in that DAG's docstring is the point.

## table-naming-no-layer-prefix

Table names carry no layer prefix. They were `prep_*` and `rpt_*`; the namespace
already says which layer a table is in, so the prefix repeated it inside the
name -- `prepared.prep_trade`, `reporting.rpt_exposure_change`.

The layer is now the only thing distinguishing a table from its upstream:
`raw.trade` is the landed 1:1 copy and `prepared.trade` the conformed one, same
name, different namespace. That is legal because dbt keeps models and sources in
separate namespaces -- a model named `trade` and a source `raw.trade` coexist
without collision. Verified, not assumed.

The dbt model name and the `PREPARED_TABLES` entry must be renamed
**together**. A mismatch in either direction points every maintenance and
retention task at a table that does not exist, and does so silently, because
`managed_tables()` never checks that its entries resolve.

## managed-tables-single-definition

`managed_tables()` is one definition, imported by both the DAG and the CLIs.

It lived in `platform_housekeeping.py`, which meant the `--table` examples in
the Makefile and README were a hand-maintained subset -- and they had already
drifted to five tables against the DAG's nine, so `make retention` quietly left
four tables growing. A forked copy that stops matching the original, where the
copy looks authoritative.

The raw half is derived from `feeds()` rather than listed, so adding a feed
extends maintenance and retention automatically. `PREPARED_TABLES` is the half
that is not derived, which is why it is the one file in
`docs/ADDING-A-FEED.md` that fails silently when skipped.

## spark-master-no-local-fallback

`spark_session()` refuses a `local` master rather than falling back to it.

A missing or blank `SPARK_MASTER` meaning "run the whole job inside this
container" is a configuration error that **looks like success**: the job
completes, the cluster sits idle, and nothing anywhere is red. Failing loudly is
the only way that surfaces.

The default in code is the same address `docker-compose.yml` sets, so a bare
`python -m ...` inside the container still works. See
[spark-master-single-source](#spark-master-single-source) for the other reader.

## branch-in-the-table-name

The Nessie branch is named **in the table reference** --
``lakehouse.raw.`trade@ingest/trade/...` `` -- rather than in session config.

This is what lets one Spark session serve a whole chunk of files. When the
branch was session-level config (`spark.sql.catalog.lakehouse.ref`), every file
needed its own SparkSession: 127 Spark applications for 183 files, each paying
executor acquisition and catalog init before doing a few seconds of actual work.

Per-file branch isolation is unchanged; only how the branch is named changed.
Backticks are required, because branch names contain `/` and `-`.

Verified against the live catalog that all three operations the ingest module
performs work through it -- `CREATE TABLE IF NOT EXISTS`, the
`MAX(_file_version)` read, and `DataFrameWriterV2.append()` -- and that a write
lands on the branch with `main` untouched.

## watchdog-wall-clock-window

The warehouse-flatness window is **wall-clock, not samples**, and the current
sample is part of it.

The check originally required five flat *evaluations*, which at `--loop 300` is
twenty-five minutes. Reclamation is nightly. So on a perfectly healthy platform
the check went WARN twenty-five minutes after every reclamation and stayed there
until the next one -- firing continuously in the live logs. A monitor whose
quiet state is unreachable teaches people to ignore it.

Reading only `history`, which is written *after* the checks run, meant a
warehouse that had just changed still failed the flatness test -- and the
message quoted the new size as the value that had been flat.

The general form: **a check whose window does not contain the thing it describes
will either never fire or never stop.** Match the window to the cadence of
whatever clears it.

## watchdog-eligible-vs-overdue

Eligible is not the same as overdue, and conflating them made the deferred-
backlog check fire almost continuously on a healthy platform.

It originally alerted as soon as a live-set was older than the deferral window.
But the window is `deferred_delete_after_hours` while the thing that *acts* on
it is the nightly DAG, so a set recorded at 22:00 with a 1h window is "overdue"
from 23:00 until the next night's run twenty-three hours later -- on a platform
doing exactly what it should. Its message even said the pass "is not running",
which was false: an explanation that reads as a diagnosis.

The condition is not "time has passed". It is **a housekeeping run completed
after these files became eligible, and they are still here** -- which is the
actual statement "the deferred-delete pass ran and did not do its job".

Same shape as [watchdog-wall-clock-window](#watchdog-wall-clock-window).

## retention-partial-failure-report

A half-applied run is the failure shape this chain actually produces, so it
reports which tables were applied instead of throwing that away with the
exception -- and the CLI prints the report **before** failing.

Re-running is safe: every step recomputes what is left to do rather than
replaying what it did. But "safe to re-run" is only useful to someone who knows
what state they are re-running from, so the report has to say it. A traceback on
its own is not enough to decide anything.

## minio-per-object-delete

Orphan sweeps delete one object at a time. Batched `delete_objects` is faster,
but MinIO rejects it without a `Content-MD5` header, which current botocore does
not send:

```
MissingContentMD5: Missing required header for this request: Content-Md5
```

Per-object `DELETE` has no such requirement and behaves the same on MinIO and
real S3. For table-sized prefixes the difference does not matter, and being
portable matters more than being quick in a destructive path.

## generated-data-must-hold-still

Generated feed data is a function of **(entity, epoch)**, not (entity, date). An
epoch is a block of days an attribute holds still for; `epoch()` numbers the
blocks and `stable_rng()` draws the value from the block number, so a value is
identical on every date inside a block and changes when the block rolls.

Without that, every value in every row changes on every delivery, and a
generated feed looks like the most volatile market data imaginable rather than
like the reference data most feeds are. That mattered beyond realism: it made
two questions the platform exists to answer unanswerable, because the answer
measured the generator rather than the design. *How much of the warehouse is
unchanged restatement? Would slowly-changing-dimension storage pay for itself?*
On the old seed the honest answer to both was "cannot tell from here".

Three specific traps this closes:

- **`trade_id` must not embed the business date.** `TRD{bd}{n}` means every
  delivery invents an entirely new portfolio and no trade ever appears twice --
  16,400 rows with 16,400 distinct `trade_id`s across 41 dates, a book with no
  continuity, in which `exposure_change` never sees an UNCHANGED row.
- **Which agencies rate a name is decided once per (counterparty, agency)**, not
  redrawn per file, or coverage flickers on and off.
- **The console's generator keys its RNG on the epoch too**, with `version` in
  the key so a `_v2` redelivery is a genuine restatement.

See `reporting_platform/common/volatility.py` for `HOLD_BY_TYPE`.

## resolve-types-is-authoritative

`scaffold.resolve_types()` is the single answer to "what is this column?",
called by the API summary, the scaffold, and the sample-data generator.

Calling `infer_types` separately from each gives the same answer only while
nobody disagrees with the guess. The moment someone does, the scaffold uses
their choice and the generator uses the guess, and the two artefacts no longer
describe the same column -- a `decimal` column gets a non-numeric sample value,
`safe_cast` nulls it, and nothing fails, because nulling is what `safe_cast` is
for.

It is sparse by design: `feed.column_types` holds only genuine overrides.

**Pass `types=` when calling `sampledata.generate()` directly.** Leaving it off
is exactly the bug above.

## one-session-per-chunk

`ingest()` opens its own SparkSession and stops it in a `finally` block at the
end of every call, so calling it in a tight loop in-process does **not** reuse
one JVM the way it looks like it should. Each call tears down and rebuilds the
SparkContext, re-resolving and reloading the Iceberg, Nessie and
aws-sdk-bundle jars through a fresh `URLClassLoader` every time.

Across ~48 sequential ingests in one long-lived process that leaked enough
classloader and heap state to kill the JVM with `java.lang.OutOfMemoryError:
Java heap space`, alongside recurring "Unclosed S3FileIO instance" warnings
pointing at the same per-call teardown.

`scripts/bulk_ingest.py` therefore drives ingests as separate processes, one
session per chunk of files. The branch is named per statement rather than per
session so a single session can serve a whole chunk -- see
[branch-in-the-table-name](#branch-in-the-table-name).

## one-destructive-dialog

Feed deletion asks **once**, and the secondary choice (also delete the model
`.sql`) is a checkbox on the page rather than a second `confirm()`.

A second `confirm()` *after* the type-the-name gate has already passed is a
trap: Cancel or Escape there returns `false`, which did not cancel the delete --
it deleted the feed and kept the file. Escape means "get me out of this"
everywhere else, so the one key a hesitant person reaches for was the one that
committed.

The checkbox is visible before you commit, and the prompt states which files
will go.
