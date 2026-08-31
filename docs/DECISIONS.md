# Decisions

Why the code is shaped the way it is.

Almost everything here was learned by running the stack and reading what it
actually said, not by design. That is worth keeping: a constraint you cannot see
is one someone removes, and most of the entries below exist because someone
already tried the obvious thing and it failed in a way that did not name itself.

**The code points here rather than repeating it.** A line like

```python
# See docs/DECISIONS.md#cosmos-packages
copy_dbt_packages=False,
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
iceberg extension has no catalog `ATTACH` at all: `ATTACH ... (TYPE ICEBERG)`
fails with ``Binder Error: Unrecognized storage type "ICEBERG"``. 1.5.5 attaches
to an Iceberg REST catalog and reads it, which is what
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
