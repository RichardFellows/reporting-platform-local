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

## Keeping an entry honest as the code moves

An entry records why a decision was made, and that reasoning outlives the
implementation — which is the point of the file, and also its one failure
mode: **a reader cannot tell a rule still in force from a rule that was
replaced, unless the entry says.** Three conventions keep that legible, and
two of them are enforced by `tests/test_doc_claims.py`.

**Superseded reasoning is kept and labelled, never deleted.** What was tried
and what forced the change is the expensive part, and it is what stops the
rejected design being proposed again. Mark it in place — a `> **Amended.**`
block, or a sentence in the past tense saying what it used to do — so the
status is visible without cross-referencing the code.

**A claim that something is NOT BUILT names it as the code names it.** The
unbuilt features are listed in `context.NOT_BUILT` and
`context.SUPERSESSION_NOT_BUILT`, and the test requires every such claim here
to name one of them, or to be written as history. This is the class of
statement that goes false *silently*: building the feature is what falsifies
it, and whoever builds it is reading code, not this file. It has happened —
see the `> **Amended.**` block under
[#console-delivery-support](#console-delivery-support).

**No line numbers.** Name the file and the symbol. Twenty-one `file.py:NNN`
references were checked and eighteen pointed at something else, several moved
by the commit that added the entry citing them.

---

## Contents

98 entries. They are grouped here by subject; the file itself is in the order
they were written, which is roughly the order they were learned. **Anchors are
stable** — the code links to them by name — so if you rename one, grep for it
first.

If you arrived from a `# See docs/DECISIONS.md#…` comment, jump straight to the
anchor; this page is for when you do not yet know what you are looking for.

### Versions, images and the container estate

| Anchor | The finding |
|---|---|
| [jar-versions](#jar-versions) | Three version values in `.env` reaching six consumers, and why the split is the finding rather than pedantry |
| [spark-jars-prebaked](#spark-jars-prebaked) | The Spark image bakes its jars instead of resolving `--packages` at submit |
| [nessie-gc-jar](#nessie-gc-jar) | There is no server-side GC endpoint; collecting content needs the CLI jar |
| [nessie-iceberg-rest](#nessie-iceberg-rest) | Nessie serves an Iceberg REST catalog, and what that does and does not replace |
| [nessie-logs-are-ecs-json](#nessie-logs-are-ecs-json) | Nessie logs Elastic Common Schema field names, which is why its logs read oddly |
| [airflow-2-not-3](#airflow-2-not-3) | **Airflow is 2.10.5 deliberately.** Under 3.0.2 no DAG run here could complete |
| [airflow-provider-constraints](#airflow-provider-constraints) | Providers install under Airflow's constraint file so pip cannot drag a version quietly |
| [airflow-api-auth](#airflow-api-auth) | `session` alone authenticates only a browser |
| [image-permissions-layer](#image-permissions-layer) | The permissions layer is last in the Dockerfile because it changes least |
| [containers-run-as-the-host-uid](#containers-run-as-the-host-uid) | `user: "${AIRFLOW_UID:-50000}:0"`, and what a bind mount does to ownership |
| [minio-host-ports](#minio-host-ports) | The published MinIO ports are the host side only |
| [no-folder-markers](#no-folder-markers) | `minio-init` creates the bucket and nothing else |
| [marimo-not-jupyter](#marimo-not-jupyter) | Marimo notebooks *are* Python files, so they diff |
| [notebook-service](#notebook-service) | The developer sandbox for "what is actually in these tables" |
| [feed-ui-same-image](#feed-ui-same-image) | The console runs from the Airflow image because it imports the platform |
| [watchdog-independent](#watchdog-independent) | **A monitor inside the thing it monitors cannot report that thing being down** |
| [seed-clean](#seed-clean) | `seed_clean/` — the same history without the injected data-quality failures |

### Spark: where it runs, and how it is kept from wedging

| Anchor | The finding |
|---|---|
| [spark-master-single-source](#spark-master-single-source) | `SPARK_MASTER` is read in two places that must not diverge |
| [spark-master-no-local-fallback](#spark-master-no-local-fallback) | A `local` master is **refused**, not fallen back to — the failure worth guarding is the one that is not red anywhere |
| [spark-worker-sizing](#spark-worker-sizing) | Cap each application, or standalone mode holds every free core until the session stops |
| [spark-in-a-subprocess](#spark-in-a-subprocess) | The JVM keeps the task process alive; heartbeats stop; the scheduler zombie-reaps it |
| [one-session-per-chunk](#one-session-per-chunk) | `ingest()` opens and stops its own session, in a `finally` |
| [one-shared-write-pool](#one-shared-write-pool) | One `lakehouse_write` slot across everything that touches table files |
| [log-tail-plus-head](#log-tail-plus-head) | Head *and* tail of a failed subprocess: a Java stack pushes the message off the front |
| [branch-in-the-table-name](#branch-in-the-table-name) | The Nessie branch is named in the table reference |

### dbt and the Cosmos-rendered builds

| Anchor | The finding |
|---|---|
| [cosmos-no-deps](#cosmos-no-deps) | **`--no-deps` is not an optimisation.** Under Airflow's constraints, every dbt invocation dies at import |
| [cosmos-rendered-builds](#cosmos-rendered-builds) | One task per model, rendered from the project — which is why adding a model needs no DAG edit |
| [cosmos-load-bearing-settings](#cosmos-load-bearing-settings) | The four settings in `dbt_builds.py` you may not change casually |
| [cosmos-packages](#cosmos-packages) | Packages install once, in `airflow-init`, not per task |
| [cosmos-profile-config](#cosmos-profile-config) | One `ProfileConfig`, the committed `profiles.yml` |
| [cosmos-emit-datasets](#cosmos-emit-datasets) | `emit_datasets=False`, or every model task gets a Dataset outlet |
| [cosmos-exclude-exposures](#cosmos-exclude-exposures) | Exposures are documentation, not something to build |
| [dbt-working-directories](#dbt-working-directories) | Three working directories outside the bind mount, or `dbt deps` hits `Permission denied` |
| [dbt-packages-volume](#dbt-packages-volume) | Mounted one level **above** `dbt_packages`, because `dbt deps` rmtree's that directory |
| [dbt-spark-session-mode](#dbt-spark-session-mode) | `method: session` and what it means for where the driver lives |
| [dbt-target-guard](#dbt-target-guard) | A non-Spark target is refused **at import time** — it would write to the wrong branch and go green |
| [duckdb-is-not-an-engine](#duckdb-is-not-an-engine) | Spark is the only build engine; DuckDB is a read-only query tool |
| [raw-is-a-source](#raw-is-a-source) | `models/raw/` contains no models on purpose |
| [no-unused-config-paths](#no-unused-config-paths) | No `seeds:` block, and why an unused config path is a trap |
| [identifiers-in-macros](#identifiers-in-macros) | Which macros call `ident()`, and why that is a decision |
| [airflow-init-load-bearing-steps](#airflow-init-load-bearing-steps) | Migrate, admin user, the pool, the registry schema, `dbt deps` — miss the last and two DAGs do not import |
| [assets-are-or-not-and](#assets-are-or-not-and) | A bare schedule list is **AND**, which is almost never what you meant |
| [retry-delay](#retry-delay) | Seconds, not the five minutes it used to be |

### Arrival: the inbox, the gate, and the shapes a delivery comes in

| Anchor | The finding |
|---|---|
| [the-inbox-is-the-conformance-gate](#the-inbox-is-the-conformance-gate) | **`landing/` has a contract**; everything that does not meet it goes through the gate |
| [unpacking-happens-at-the-gate](#unpacking-happens-at-the-gate) | One zip in, N ordinary deliveries out; the container never lands |
| [archive-normalizer](#archive-normalizer) | A dated container of undated members |
| [control-file-gate](#control-file-gate) | A normalizer that will not emit a manifest until the control file agrees |
| [control-file-formats](#control-file-formats) | HOW a control file is read (`format`) versus WHAT is read out of it (the fields) |
| [a-control-file-is-attributed-by-its-stem](#a-control-file-is-attributed-by-its-stem) | `{stem}` is not a wildcard, and two feeds that cannot be told apart do not load |
| [a-delivery-shape-is-a-registry-entry](#a-delivery-shape-is-a-registry-entry) | Two tables and four outcome types, so the next awkward delivery is not a branch in the watcher |
| [the-sniffer](#the-sniffer) | Onboarding from a real file |
| [console-delivery-support](#console-delivery-support) | Creating an archive- or control-gated feed through the form |
| [inbox-is-polled](#inbox-is-polled) | Polling, not inotify — *events are an optimisation, the poll is the correctness guarantee* |
| [an-unchanged-resend-is-a-no-op](#an-unchanged-resend-is-a-no-op) | A byte-identical redelivery used to land as `_v2` |
| [no-arrival-timeout](#no-arrival-timeout) | Two config keys deleted for being settings nothing read |
| [delivery-expected-not-completeness](#delivery-expected-not-completeness) | "Is a delivery expected on every COB date" is not "is this delivery complete" |
| [ready-is-a-derived-index](#ready-is-a-derived-index) | **`ready/` is a derived index of `landing/`**, not a queue somebody fills |
| [the-ready-window-bounds-the-parts-not-the-manifests](#the-ready-window-bounds-the-parts-not-the-manifests) | 157 deleted, 157 remade, both logging success |
| [namespace-before-branch](#namespace-before-branch) | The namespace is created against `main`, before the ingest branch exists |

### Feeds, columns and the console

| Anchor | The finding |
|---|---|
| [feed-names-carry-the-source](#feed-names-carry-the-source) | One string is the raw table, DAG id, prefix, source and model at once |
| [feed-conventions](#feed-conventions) | `defaults → convention → feed`, and why there may be only one implementation of that merge |
| [the-registry-is-a-directory](#the-registry-is-a-directory) | One file per feed, and the cache key that makes it work |
| [a-gate-that-cannot-fail](#a-gate-that-cannot-fail) | `lineage --columns` was green on seven tables it could not read |
| [the-jar-versions-are-checked-by-a-test-now](#the-jar-versions-are-checked-by-a-test-now) | Five copies of one version, and what is actually checkable |
| [source-column-names](#source-column-names) | A column may be named differently in the file than in the platform |
| [a-declared-column-migrates-itself](#a-declared-column-migrates-itself) | **The directions are not symmetrical**: declared columns are added, undeclared ones are never dropped |
| [provenance-is-added-not-backfilled](#provenance-is-added-not-backfilled) | The migration is lazy, and one un-migrated feed fails *every* prepared model |
| [supersession-is-declared-not-assumed](#supersession-is-declared-not-assumed) | The value is the refusal: a delta feed deduped as a snapshot loses keys silently |
| [as-of-is-a-var-not-a-second-model](#as-of-is-a-var-not-a-second-model) | Same models, a dbt var, and a refusal to run incrementally |
| [delivery-ref-is-the-fallback-with-the-prefix-stripped](#delivery-ref-is-the-fallback-with-the-prefix-stripped) | `_delivery_id` is a basename; `_source_file` is a key |
| [generated-data-must-hold-still](#generated-data-must-hold-still) | Generated data is a function of (entity, epoch), not (entity, date) |
| [resolve-types-is-authoritative](#resolve-types-is-authoritative) | One answer to "what type is this column?", or the generator and the model disagree |
| [one-destructive-dialog](#one-destructive-dialog) | Feed deletion asks once |
| [the-arrivals-view-is-a-join-not-a-record](#the-arrivals-view-is-a-join-not-a-record) | **Written nowhere** — an arrivals table would hold verdicts that could not be rebuilt |
| [table-naming-no-layer-prefix](#table-naming-no-layer-prefix) | The namespace already says the layer |

### The registry and the publication record

| Anchor | The finding |
|---|---|
| [the-registry-records-observations-not-verdicts](#the-registry-records-observations-not-verdicts) | **No `ingested`, no `superseded`, no `status`** — the whole difference from the `stg` load-control tables |
| [a-run-is-the-first-thing-the-registry-cannot-rebuild](#a-run-is-the-first-thing-the-registry-cannot-rebuild) | Which is why `run_input` has no foreign key and a run has a mutable status |
| [version-is-per-report-and-as-at-date](#version-is-per-report-and-as-at-date) | Not per run, not per family |
| [code-identity-is-a-digest-when-it-cannot-be-a-tag](#code-identity-is-a-digest-when-it-cannot-be-a-tag) | A value **and** a kind, never conflated |
| [a-change-is-a-deployment-event-not-a-run-event](#a-change-is-a-deployment-event-not-a-run-event) | One ticket authorises a version; hundreds of runs inherit it |
| [a-version-diff-is-inputs-and-code-not-data](#a-version-diff-is-inputs-and-code-not-data) | The set difference of two `run_input` sets |
| [the-as-at-date-has-a-lifecycle](#the-as-at-date-has-a-lifecycle) | `open → locked → submitted`, where **`open` is the absence of a row** |
| [quarantine-is-where-a-refused-delivery-goes](#quarantine-is-where-a-refused-delivery-goes) | The rejection date is in the key, because the file usually has no parsable name |
| [catalog-reconciliation](#catalog-reconciliation) | What the catalog holds versus what the project declares |
| [managed-tables-are-derived](#managed-tables-are-derived) | Derived from the dbt project, not listed |
| [managed-tables-single-definition](#managed-tables-single-definition) | One definition, imported by both the DAG and the CLIs |

### Retention, GC and reclamation

| Anchor | The finding |
|---|---|
| [an-incomplete-keep-set-refuses](#an-incomplete-keep-set-refuses) | **A short answer is a deletion order.** Nothing deleted is not nothing to delete |
| [published-tags-are-the-reproducibility-window](#published-tags-are-the-reproducibility-window) | A tag is **data** retention, sized in years, not by the table keep-set |
| [an-ingest-is-not-a-publication](#an-ingest-is-not-a-publication) | They cut different tags, and conflating them kept ingests for ten years |
| [retention-classes-name-the-obligation](#retention-classes-name-the-obligation) | The class is a feed property; the window is per-environment policy |
| [the-evidence-interlock-is-two-halves](#the-evidence-interlock-is-two-halves) | A configuration check and a per-delivery check, neither sufficient alone |
| [reproducibility-is-exercised-not-asserted](#reproducibility-is-exercised-not-asserted) | The pin is read against the real catalog |
| [gc-lag-and-assertions](#gc-lag-and-assertions) | Identification and removal are two steps, a deferral window apart |
| [a-dry-run-may-write-to-the-index-not-to-object-storage](#a-dry-run-may-write-to-the-index-not-to-object-storage) | `{"dry_run": true}` wrote 116 manifests and then mispredicted its own sweep |
| [retention-partial-failure-report](#retention-partial-failure-report) | A half-applied run is the shape this chain actually produces |
| [minio-per-object-delete](#minio-per-object-delete) | One object at a time, and why the faster batch call is not used |
| [lateness-is-a-wall-clock-time-not-a-duration](#lateness-is-a-wall-clock-time-not-a-duration) | A duration needs an origin event that `PutObject` does not have |
| [watchdog-wall-clock-window](#watchdog-wall-clock-window) | **Match the window to the cadence of whatever clears it** |
| [watchdog-eligible-vs-overdue](#watchdog-eligible-vs-overdue) | Eligible is not overdue |

### Lineage

| Anchor | The finding |
|---|---|
| [openlineage-is-an-export-not-a-record](#openlineage-is-an-export-not-a-record) | Not an authority, and a skipped task shows as `RUNNING` forever |
| [lineage-is-derived-from-the-dbt-project](#lineage-is-derived-from-the-dbt-project) | A job per task and **no datasets at all**, until the extractor was registered under the variable Airflow reads |
| [a-column-with-no-source-says-so](#a-column-with-no-source-says-so) | 116 of 136 traced, and reporting only those makes a literal, a `count(*)` and a parser failure identical |
| [marquez-on-ubi](#marquez-on-ubi) | Both images built here, from Marquez's own source |

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
*both* drivers (`spark_session()` in `common/spark.py`, `spark.jars.packages`
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
security-mandated server bump possible without moving the Spark jars. **It is
also what decides the server's log format** — see
[nessie-logs-are-ecs-json](#nessie-logs-are-ecs-json) — which is why the server
here is pinned ahead of the extensions rather than level with them.

`hadoop-aws` and `aws-java-sdk-bundle` are deliberately not parameterised. They
track Spark 3.5's Hadoop, not Iceberg.

Defaults everywhere repeat the pinned combination, so a clone with no `.env`
builds what this one builds — including the server tag, because a default
behind 0.104 would leave the `quarkus.log.console.json.*` keys in
`docker-compose.yml` inert. `docker compose exec
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

## nessie-logs-are-ecs-json

Nessie logs JSON in Elastic Common Schema field names —
`quarkus.log.console.json.enabled` and `quarkus.log.console.json.log-format:
ECS` on the `nessie:` block — so a collector needs no per-service grok and a
stack trace arrives as `error.stack_trace` rather than as unparsed lines glued
onto `message`.

**This is a version decision wearing a config decision's clothes.** JSON
logging is Quarkus's `quarkus-logging-json`, a **build-time** extension: it is
either augmented into the image or it is not, and no property, mounted jar or
environment variable can add it afterwards. It is absent from 0.99.0 and
present from around 0.104. A server below that logs plain text with
`QUARKUS_LOG_CONSOLE_JSON=true` set, unchanged, and says nothing about the
property it ignored — the jar is either in `/deployments/lib/main` or it is
not.

So a server pinned back below 0.104 must lose those two lines with it. Left
behind they are well-formed configuration that reads as working and does
nothing, which is the failure mode this file exists to prevent.

Two things the format does not give you:

- **`service.environment` is the Quarkus profile** (`prod`), not
  `REPORTING_ENV`. The `additional-field` override does not survive env-var
  mangling of a dotted field name, so it cannot be set from the compose
  environment block.
- **Access logs stay unstructured.** `io.quarkus.http.access-log` becomes one
  ECS record whose `message` holds the whole combined-log line; the HTTP fields
  are not broken out. Currently that is one record every 10s from the
  healthcheck alone.

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

## airflow-init-load-bearing-steps

`airflow-init` runs once and everything else waits on it *completing*, so no
component races the database into existence. It does five things, each of which
was once a manual step that silently broke the platform when skipped.

THE ANCHOR NO LONGER COUNTS THEM, and that is the point of the rename: it was
`airflow-init-four-things`, the registry schema became the fifth, and a name
that holds a number goes stale the next time this list grows. What is load
bearing is that every step is.

1. `db migrate` — the metadata schema. (Unlike Airflow 3, `airflow users` exists
   here.)
2. `users create` — admin/admin for the web UI and the REST API the feed console
   calls.
3. `pools set` — **one** pool, at one slot. Ingest, dbt model tasks and
   maintenance all take this same slot; a second one-slot pool would *not*
   exclude them from each other, which is the bug that once let
   `remove_orphan_files` run alongside a write. Without it every task sits
   `queued` forever with nothing to say why.
4. `registry schema` — the delivery registry's tables, so a cold stack has
   them before any DAG parses. `registry/db.py` also ensures the schema on its
   first connect, and that is not redundant: `inbox`, `feed-ui` and `watchdog`
   share this image but not this container's `depends_on`, so they can be up
   and recording deliveries while this has never run.
5. `dbt deps` — no longer merely "the build fails until you run it". Cosmos
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
diverge: `spark_session()` in `common/spark.py` and `spark.master` in
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

Each application caps itself at 2 cores / 2g (`spark.cores.max` in `common/spark.py`
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
point is that it edits `reporting_platform/config/feeds/`, the dbt project
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
unqualified `raw.fo_trade` resolve against the lakehouse/Nessie catalog instead.

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
OTHER parent may not have been built yet -- `collateral`'s relationship to
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

The fallback value matters for the same reason. `duckdb_local` was broken -- an unset
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

`any_of()` reduces with `|`, which yields `DatasetAny`/`AssetAny` on Airflow
2.9+ and 3.x. If that is unavailable the list is returned unchanged **and a
warning is logged**, because degrading to AND silently is how this was missed in
the first place.

## retry-delay

Retry delay is **seconds**, set by `AIRFLOW_RETRY_DELAY_SECONDS`.

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
(and nothing declares it lost — see
[#no-arrival-timeout](#no-arrival-timeout)).

Without the pool, every task sits `queued` forever with nothing to say why,
which is why `airflow-init` creates it -- see
[airflow-init-load-bearing-steps](#airflow-init-load-bearing-steps).

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
`raw.fo_trade` is the landed 1:1 copy and `prepared.fo_trade` the conformed one, same
name, different namespace. That is legal because dbt keeps models and sources in
separate namespaces -- a model named `trade` and a source `raw.fo_trade` coexist
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

- **`trade_id` must not embed the COB date.** `TRD{bd}{n}` means every
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

## managed-tables-are-derived

`managed_tables()` derives the prepared and reporting table sets from the dbt
project directory -- one model file, one table -- rather than from a list in
`common/context.py`.

The dbt project is the right source because it is *declarative*. The catalog is
not: `SHOW TABLES` describes what happens to be there, so a table left behind by
a removed model would keep being maintained, and before the first build the set
would be empty. It would also cost a Spark session, or a network call, inside a
module the Airflow DAG processor imports on every parse.

Three things make the derivation safe rather than merely shorter:

- **Keyed on the DIRECTORY's mtime**, which changes when a model is added or
  removed -- the only events that change the set. Same reasoning as
  [`_load`](#managed-tables-single-definition): a long-lived process must not
  hold a stale answer. Verified live: a new `.sql` appears in
  `managed_tables()` without a restart.
- **A missing directory RAISES.** Returning `()` would be the same silent
  failure in a new place -- a container without the dbt project mounted (the
  watchdog is one) would report that the platform manages nothing, and every
  maintenance and retention pass would succeed having done nothing.
- **A dbt `alias` RAISES.** The derivation rests on model filename == table
  name. An alias would break that in the direction that matters, pointing
  maintenance and retention at a table that does not exist. No model sets one
  today; the guard is there so the first one that does says so.

The consequence: adding a feed is five files, not six, and the feed console no
longer splices Python source with `ast` to register the table.

## catalog-reconciliation

`check_orphan_tables` in the watchdog compares what the Nessie catalog holds on
`main` against what `managed_tables()` declares. It is the inverse of the
failure that list used to have.

Deriving the set from feeds.yml and the dbt project
([managed-tables-are-derived](#managed-tables-are-derived)) means a table stops
being maintained the moment its feed or model is deleted -- correctly, but
silently. The data does not go anywhere: it sits in the warehouse, never
compacted, its snapshots never expiring, retention never trimming it, and
nothing says so. This check is what says so.

**Declared-but-absent is deliberately not a finding.** A model that has never
been built has no table yet, which is the normal state of a fresh clone and of
any model added since the last build. Alerting on it would fire on every
healthy new checkout -- the exact shape this file has twice been corrected for
(see [watchdog-wall-clock-window](#watchdog-wall-clock-window) and
[watchdog-eligible-vs-overdue](#watchdog-eligible-vs-overdue)). It is recorded
as a fact, `unbuilt_tables`, so it is visible without being an alarm.

**The orphan warning does not self-clear, and that is right.** An orphan is a
real condition that persists until someone drops the table or restores what
declared it. The test that matters is whether the quiet state is *reachable*,
not whether the warning is short-lived -- and it is: drop the table and the
watchdog returns to OK. Verified by creating one, seeing the WARN name it,
dropping it and watching the severity go back.

It reads the catalog over the Nessie REST API rather than with `SHOW TABLES`,
because the watchdog imports no Spark and runs every five minutes; a
SparkSession would cost about 22 seconds of that. The listing is paginated and
the client follows the pages -- on a warehouse this size that is one page,
which is precisely why ignoring `hasMore` would go unnoticed until it did not.

This is also why the watchdog mounts `./dbt` read-only: it needs the declared
set, and nothing else from the project.

## source-column-names

A column may declare the name it has **in the delivered file** separately from
the name the platform uses:

```yaml
columns:
  - trade_id: "Trade Id"
  - counterparty_id: "Cpty Ref"
  - notional
```

Sparse, like `column_types`: a bare string means the header is already a usable
identifier, which is most of them. The rename happens once, in
`reconcile_schema` during ingest, so **raw onwards sees only identifiers**.

Real deliveries do not arrive with snake_case headers. `Trade Id`,
`Cpty Ref`, `Notional (USD)` are ordinary, and a space in a column name is not
a cosmetic problem downstream: dbt macros interpolate column names into SQL,
so `PARTITION BY Trade Id` is a syntax error. Renaming in every prepared model
instead would mean quoting at every call site, in every model, forever --
and getting it wrong produces a build failure a long way from the cause.

Raw stays 1:1 with the delivery in the way that matters: same rows, same
values, same order, everything a string. Only the identifiers are normalised.

Two things follow, and both are the same rule -- **drift is a statement about
the file**:

- `missing_columns` and `extra_columns` are reported in SOURCE names, from
  ingest and from the console's header check alike. "The delivery did not have
  `Cpty Ref`" is something an upstream can act on; the platform name it would
  have become is not.
- `_extra_columns` keys are source names, necessarily -- an undeclared column
  has no platform name.
- The sample-data generator writes the SOURCE header, or the deliveries it
  generates would be ones the ingest cannot read.

## identifiers-in-macros

`ident()` in `macros/engine.sql` quotes a column name. Which macros call it is
the distinction that matters, and it is not stylistic:

- **Identifier-typed** -- `dedupe_rank`, `scd2_hash`, `scd2_effective_to`,
  `scd2_incremental_scope`, `scd2_columns` -- are handed a *name* and
  interpolate it into SQL. These quote.
- **Expression-typed** -- `safe_cast`, `clean_string`, `parse_date` -- are
  handed an *expression* and nest inside one another
  (`safe_cast(clean_string('x'), 'DECIMAL(18,2)')`). Quoting their argument
  would produce `` TRIM(`NULLIF(...)`) `` and break every existing model. These
  do not, and must not.

Already-qualified or already-quoted names pass through untouched, so
`as_of('r', 'cob_date')` and `r.counterparty_id` still work.

With [source-column-names](#source-column-names) doing the normalising at
ingest, the prepared layer rarely sees an awkward identifier at all. This is
the second line of defence, and it is what makes a model that reads raw
directly safe to write.

## inbox-is-polled

The inbox watcher polls. It does not use inotify, `watchdog`, or filesystem
events of any kind, and that is deliberate: **filesystem events do not cross a
Docker Desktop bind mount** on Windows or macOS. The host writes the file, the
container is never notified, and an event-driven watcher sits there reporting
itself healthy while files pile up — a monitor whose failure mode is silence.

Polling costs a directory listing every ten seconds. For a folder receiving a
handful of files a day that is nothing, and it behaves identically on every
host, which an event-based watcher demonstrably does not.

Four behaviours make it safe to leave running, and each replaces a way a naive
loop loses data:

- **It waits for the file to stop changing.** A file exists from the moment it
  is created, not when it is finished, so uploading on sight means uploading
  half a CSV — which then ingests *cleanly*, with `expected_min_rows` the only
  thing between that and a silently truncated delivery. A file is ready when
  its size and mtime are unchanged across two consecutive polls.
- **It routes by the feeds' own `filename_pattern`s**, so nothing here repeats
  `feeds.yml`. A file matching none is moved to `.rejected/` rather than left,
  because a file left in place is retried and logged forever.
- **A file matching more than one feed is rejected, not guessed.** Overlapping
  patterns are a configuration error, and picking one would put a delivery in
  the wrong raw table — which looks like data rather than like an error.
- **It moves the file before triggering the DAG.** If the trigger fails the
  file is already out of the way and recorded as landed, so the next pass does
  not re-upload it as a new `_file_version`. A missed trigger is a button
  press; a duplicate ingest is not.

This is the "object-created event" the ingest DAGs' `schedule=None` comment
always referred to. Until this existed, nothing triggered an ingest
automatically at all — the comment described an intended mechanism rather than
a working one.

## feed-names-carry-the-source

A feed is named `<source_system>_<feed>`: `fo_trade`, `ref_counterparty`,
`treasury_margin_call`. It is **typed into `feeds.yml`**, not derived.

The point is disambiguation: two systems delivering something called
`positions` are two different feeds, and the name is the only thing that
separates them at a glance in the object store, the catalog, the DAG list and
the model tree.

**Typed rather than derived, deliberately.** The feed name is already the raw
table, the Airflow DAG id, the landing prefix, the dbt source table and the
prepared model name — one string doing five jobs, which is what keeps them
impossible to desynchronise. Deriving a *different* string for some of those
reintroduces exactly the mismatch `managed_tables()` cannot detect and
`ADDING-A-FEED.md` warns about: rename one without the other and every
maintenance and retention task addresses a table that does not exist, silently.
Prefixing the name prefixes all five at once, and costs no code at all.

This replaced a per-source **namespace** scheme (`raw_<source>.<feed>`, with
`landing/<source>/<feed>/` and a sources file per system) that was built and
then backed out. Worth knowing why, if it is ever proposed again:

- It cannot nest. dbt-spark supports only a two-level `schema.table`, so
  `raw.<source>.<table>` is impossible ([raw-is-a-source](#raw-is-a-source)) and
  the namespace has to be a single flattened segment anyway.
- It multiplies namespaces, and every one of them is a thing to create, grant,
  retain and reason about, for a distinction the table name can carry for free.
- It moves the source system OUT of the name, so the DAG id, the model file and
  the prepared table stop mentioning it. The disambiguation only exists where
  the namespace is visible.

The one thing worth keeping from that work is written down separately, because
it is a real bug and not a design preference: see
[namespace-before-branch](#namespace-before-branch).

## namespace-before-branch

`ensure_raw_namespace()` runs against `main` **before** the ingest branch is
cut, separately from `ensure_raw_table()`.

Creating it *on the branch* is not the alternative it looks like. Nessie's `` `table@branch` `` suffix applies to a **table** identifier. Used on
a namespace it does not fail -- it creates a namespace literally named
`` `raw@ingest/...` `` on main. Verified against the live catalog, and then
cleaned up by hand. A mechanism that fails by making junk rather than by
erroring is one to write down.

So the namespace is created on `main` first and the branch inherits it. A
namespace holds no data, and this is the same precedent the cold-start
bootstrap already sets.

## a-gate-that-cannot-fail

`python -m reporting_platform.lineage --columns` is CLAUDE.md's CI seam: the
lineage package may never raise, so `unresolved` has to be caught somewhere
allowed to say no, and that CLI exits 1 on any. But column lineage needs the
compiled SQL and the catalog, so without a stack it reads only the tables that
exist — four of eleven on a bare runner — and exits **0**: green on almost
nothing.

**`unresolved` and `not derivable` are different facts and only one was a
failure.** `unresolved` is a column the parser READ and could not trace -- a
defect in the export. `not derivable` is a table it could not read at all.
Reporting the second as a clean bill of health is precisely
[an-incomplete-keep-set-refuses](#an-incomplete-keep-set-refuses) -- *a
subject it could not READ is not a subject that is EMPTY, and the two are the
same value* -- applied to the tool that enforces the rest of them.

`--require-derivable` adds the second condition, and the derivable count now
prints either way, because `unresolved: none` over four of eleven tables reads
as a pass unless the other seven are counted next to it.

**NO TIER THAT EXISTS RUNS EITHER FORM**, and this paragraph claimed otherwise
for a while -- that `.github/workflows/config.yml` ran `--columns` without the
flag. It never did, and it should not. Without the flag, the four tables a
config-level run CAN read are read from `feeds.yml`'s declared columns, not
from a catalog, so every column is `sourced` by construction -- 28 of 28,
measured -- and `unresolved` cannot arise at all: a tick that cannot fail,
which is what this section is named after. With the flag it fails every time
and says only "you have not built yet". The gate belongs to a post-build tier,
there is not one, and `config.yml`'s header now says that instead of claiming
the check. The tier that DOES exist above it, `parse.yml`, changes nothing
here: `dbt parse` writes no compiled SQL.

The flag alone, without `--columns`, is an argparse error rather than a silent
no-op -- for the same reason `member_pattern` on a non-archive feed is
refused: a setting that cannot apply must not look like one that did.

## the-jar-versions-are-checked-by-a-test-now

[jar-versions](#jar-versions) has been documented in CLAUDE.md, `.env.example`
and here for as long as it has existed, and was checked by nothing.
`common/spark.py` reads two of the three and interpolates them straight into
`spark.jars.packages`; the failure is `NoSuchMethodError` on the first write,
after the image builds and the stack comes up, naming no version.

`tests/test_versions.py` pins what is actually checkable, which is not the
same as what the rule says:

- **`ICEBERG_VERSION` agrees across all five files that declare it** --
  `.env.example`, `docker-compose.yml`, `Dockerfile.spark`,
  `common/spark.py`'s fallback and `dbt/profiles.yml`'s. Five, because the
  image bakes the jars in and both drivers resolve the coordinates again;
  a bump touching four of five is the realistic mistake.
- the extensions likewise, across five.
- **`NESSIE_SERVER_VERSION` against the `nessie-gc` jar**, which
  `docker-compose.yml`'s own comment says must be equal.
- **the server may lead the extensions, not lag them.**
- **the configured (Iceberg, extensions) pair is one somebody has run.**

That last one is a hand-kept list, and deliberately: whether a given
extensions build works with a given Iceberg is a property of what upstream
compiled against, and nothing in this repo can compute it. Both entries come
from `.env.example`'s own header -- the shipped pair and the "known-good
fully-upgraded alternative" it documents. Adding one is a claim that somebody
ran it, made once in one place rather than implied by whatever is in `.env`.

A seventh test pairs the Quarkus JSON logging keys with the server version
that honours them, because `.env.example` warns that pinning the server below
0.104 leaves those keys behind as inert config that reads as working -- which
is this repo's most-repeated failure shape and was, again, enforced by nothing.

## the-registry-is-a-directory

The feed registry is a directory, one file per feed:

```
reporting_platform/config/feeds/
    _defaults.yml            the defaults tier -- the mapping itself
    conventions/ref_src.yml  one convention -- the settings mapping itself
    fo_trade.yml             one feed -- the block itself
```

No wrapper keys: a feed file IS what used to sit under `feeds:`. Re-stating
the key inside the file that names it is the kind of redundancy that drifts.

**The reason is the diff, and the diff is the deliverable.** Adding a feed was
an append into a shared sequence, so two feed PRs conflicted with each other
by construction; a column change was a few lines inside a file that grows with
the estate. Now a new feed is one new file with no context lines, a column
change is a small diff in a small file, `git log config/feeds/fo_trade.yml` is
that feed's history rather than everybody's, and CODEOWNERS can name a feed --
which cannot be expressed inside one file at all. The per-feed *comments* move
with their feed, which matters more than it sounds: `ref_collateral`'s note on
why its retention class is `operational` is the most valuable content in that
block, and it used to be nine lines of context in everyone else's diff.

**The filename is the identity, cross-checked at load.** `fo_trade.yml`
declares `name: fo_trade` or it is an error, and the filename must be a legal
name -- lowercase, digits, underscores. That is not tidiness. The name is the
raw table, the DAG id, the landing prefix, the dbt source table and the
prepared model at once ([feed-names-carry-the-source](#feed-names-carry-the-source)),
so a filename that disagrees with the `name:` inside it adds a sixth spelling
that nothing reconciles -- and the realistic way in is copying a file to start
a new feed and changing one of the two. A convention file carries no `name:`
at all: a convention setting one would supply it to every feed inheriting it.

**The cache key is the part that is easy to get subtly wrong.** Every config
cache here is keyed on an mtime so that a long-lived process -- Airflow's DAG
file processor, the console -- picks up an edit without a restart. The obvious
translation is the directory's mtime, and it is wrong: **a directory's mtime
moves when a file is added or removed and NOT when one is edited.** That is
fine for `models_in`, whose set only changes on add/remove, and wrong here.
Keyed that way, editing a convention would never reach the DAG processor --
a convention changed, no feed changing, nothing reporting an error, which is
precisely the bug `_load`'s mtime key was written to kill, in a new shape.
`layout.registry_files()` therefore stats every file in the tree. Measured at
40 feeds: 115us to build the key against 3.5us for a single stat, against the
0.17s `lineage/columns.py` already spends tracing one model's columns.
`tests/test_layout.py` pins all three edit paths, and they fail as expected
when the key is swapped for the directory's mtime -- verified by swapping it.

**`python -m reporting_platform.config show <feed> --origin` is the other half.**
A middle tier only earns its keep if values are not written where they are
used, which makes "why is this feed reading a pipe delimiter" a question about
three files. Layering is survivable when there is a render command; that is
the render command, and it reads `effective_defaults()` rather than re-walking
the tiers, so it cannot disagree with the loader it describes.

## feed-conventions

`conventions:` is the middle tier. Resolution is `defaults -> convention ->
feed`, each layer overriding the last, and it is the same
`{**a, **b}` the `defaults` merge already was.

**A convention may name a `parent:`**, so the middle tier is a chain rather
than a single link -- global, vendor, system, feed. An undefined parent and a
cycle are both errors at load, for the reasons every other resolution failure
here is: the first would resolve silently to `_defaults.yml` alone and produce
a feed configured subtly wrong, the second would be a `RecursionError` naming
nothing. Depth is what makes
[the-registry-is-a-directory](#the-registry-is-a-directory)'s `--origin` view
load-bearing rather than a convenience.

**Shallow at every layer.** A dict-valued key such as `column_types` is
replaced by the more specific layer, not merged into it. With a deep merge
there is no way to *remove* an inherited entry, and "why is this column still a
decimal" becomes a question answered by reading three places.

**One implementation of the merge, in `context.effective_defaults()`.** The
feed console needs the same answer for a different reason: `ui/registry._block`
omits a key whose value matches what the feed would inherit, so that the
convention stays the one place that value is written. Comparing instead against
a hardcoded default map — which is what it did — meant a feed inheriting
`delimiter: "|"` had `delimiter: "|"` written into its own block on the next
save, pinning the value where the convention could no longer change it, in a
diff that looked deliberate. That rule now covers **every** managed key, not
just the format ones: a convention supplies `source_system` and
`expected_min_rows` as readily as `delimiter`, and the narrower rule pinned
those two on the first save. Verified by round-tripping every feed in the real
`feeds.yml` through the console's save path and diffing.

Clearing a feed's convention writes back everything it was supplying, rather
than letting the feed silently revert to `defaults:` — a feed that quietly
starts reading a pipe file with a comma delimiter lands one column holding the
whole row, and does not fail.

| Wrong | Why it cannot be a warning |
|---|---|
| a feed naming an undefined convention | falls back to `defaults:` and produces a feed configured subtly wrong, rather than one that does not exist |
| an unknown key inside a convention | dropped by the `allowed` filter with no comment — `delimeter:` would simply never apply |
| a convention setting `name` or `convention` | `name` collapses two feeds into one registry entry, last one wins; `convention` is how a FEED names one, so it would record a name that had no effect -- a convention chains with `parent:` |

Unknown keys are rejected in `conventions:` but **not** in feed blocks. That is
inconsistent on purpose: conventions are new surface with nothing depending on
them, so they can be strict from the start, whereas adding the same check to
feed blocks could refuse to load an existing `feeds.yml` and take the platform
down at import over a key that has always been harmlessly ignored. Worth doing
later, deliberately, as its own change.

The console offers the defined conventions as a **closed list**, never free
text, for the same reason: a typo there is not an error anyone would see.

`find_gaps` is `if how == "weekly": ... else: <daily>`, so a weekly
feed typed `fortnightly` reports every non-delivery day as a gap -- the
exact failure `cadence:` was added to remove, reinstated by a typo. And the
feed's own `delimiter` was unchecked while a **control file's** was not,
which is the wrong way round: the feed's is the one every delivery is read
with.

They are `check_*` functions in `common/context.py` now, applied at load to
**declared values only** — a key absent from all three tiers is left absent so
the `Feed` default applies, because restating each default beside its check
would be a second copy of every default. `tests/test_value_checks.py` asserts
each from both sides, since one shared function is the only thing stopping
them drifting apart again.

`schema_drift` is the exception worth naming: `ingest_feed` already checked it
and still does. That check is what finally made `fail` mean something after
the setting spent months being read by nothing, and a `Feed` built by hand
rather than through `feeds()` still reaches it. Checking at load as well just
moves the cost from a delivery to a parse.

## ready-is-a-derived-index

So the two are separate prefixes with separate lifetimes:

| Prefix | Job | Lifetime | Deletion rule |
|---|---|---|---|
| `landing/<feed>/` | evidence, byte for byte | `keep_years`, per retention class ([RETENTION.md](RETENTION.md#landing-everything-for-its-retention-class)) | never on a guess |
| `ready/<feed>/` | work queue: a manifest per delivery, plus derived parts | `keep_days: 7` | freely, once ingested |

**`ready/` is a DERIVED INDEX of `landing/`, not a queue somebody fills.**
`find_pending` reconciles it first — a cheap, idempotent, Spark-free pass. That
is not tidiness: the production arrival path is an agent doing a PutObject
straight into the bucket, which runs no code of ours, so a queue that had to be
*filled* would have an ordering bug with no error attached to it. Everything in
`ready/` can be rebuilt by re-normalizing, and that property is the one to
protect — the moment something there cannot be, it has become a third copy of
the data.

### What the manifest is for

One delivery, one JSON object, recording the three things every downstream
reader was previously re-deriving from the filename with its own copy of the
same regex: the COB date, which objects hold the rows, and how to read
them. `Feed.parse_filename` had **fourteen call sites across seven modules**,
each free to disagree.

**A plain CSV is not copied.** The manifest's single part points back at the
landing object, so the common case costs one small JSON object — measured on
the live stack at 500 bytes against 15KB of data — and `ingest` still has
exactly one code path, because it reads `parts` and neither knows nor cares
whether they point into `landing/` or `ready/`. A normalizer copies bytes only
when it transforms them.

**`format` is recorded and read back at ingest**, rather than re-read live from
`feeds.yml`, so an ingest is reproducible: what delimiter a delivery was
actually read with is stored next to it. Correcting a wrong one is "fix
feeds.yml, re-normalize", which is cheap because `ready/` is a cache.

**A manifest is a pure function of (feed config, landing object).**
`received_at` is the landing object's `LastModified`, not the time normalize
ran, so re-normalizing an unchanged delivery rewrites byte-identical content.
Anything time-based there — a `now()`, a uuid — would quietly turn an
idempotent operation into one that produces a new delivery every time.

### What is deliberately NOT in it

**Ingestion status.** `already_ingested` derives that from `_source_file` in the
raw table precisely so it cannot drift from reality; the legacy `stg`
load-control tables are what that avoids. A manifest carrying
`"ingested": true` is that table under a new name. The manifest records
**observations about an event** — what arrived, how big, how to read it, what a
control file declared — never derived state.

The same line is why `ready/` retention reads `already_ingested` rather than a
flag: a manifest whose parts are not yet in the raw table is never swept,
regardless of age. Sweeping one is not data loss, since landing still holds the
object — but nothing would re-normalize it on its own, so it is a *silent* drop.

### Two couplings that must not be undone

**`_source_file` holds the PART's key, not the manifest's.** `already_ingested`
matches on it, so writing the manifest key there would make every delivery look
un-ingested forever and re-ingest on the next pass. With `kind: file` the part
*is* the landing key, which is what that column has always held — verified on
the live stack: ingesting through a manifest wrote
`landing/fo_trade/TRADE_20260903.csv`, and the next `pending` came back empty.

**`find_pending` straddles both prefixes.** Candidates come from `ready/`, but
the retention keep-set is still computed from the dates observed in
**`landing/`** — the only prefix that still holds every date after raw has
expired them, which is what `retention.yml`'s `landing:` comment is about.
Computing it from a days-long cache would silently narrow the window and start
reporting live COB dates as expired. Giving manifests an eight-year
lifetime so they could serve instead is the load-control-table trap in a
different hat.

### Compatibility kept on purpose

`ingest --object landing/...` still works and normalizes on the fly **without**
writing to `ready/`. That form is what every runbook, the README walkthrough and
`docs/ADDING-A-FEED.md` tell you to type, and a one-off manual ingest should not
leave a queue entry behind.

## archive-normalizer

Step 3 of `docs/DELIVERY-SHAPES.md`: `custodyPositions_20260903.zip` holding
CSVs whose own names say nothing about which day they are for. The date is on
the container, not the members, and that is the one case this builds --
`delivery: {kind: archive, cob_date_from: container, parts: concat}`.
`cob_date_from: member`/`path` and `parts: separate` are recognised keys
with no implementation behind them.

> **This is one of two archive mechanisms, and the date picks which.** A zip
> whose MEMBERS carry the date is unpacked at the door instead and never
> lands, which is [#unpacking-happens-at-the-gate](#unpacking-happens-at-the-gate)
> -- a different key (`arrival.archive`), a different stage, N deliveries out
> rather than one. Read that before concluding from `cob_date_from: member`
> being unbuilt that a member-dated zip cannot be handled: it can, just not
> by this normalizer. Either may be gated on a control file
> ([#control-file-gate](#control-file-gate)).

**Unbuilt values raise "NOT BUILT", not "unknown".** A typo and a missing
feature are different problems with different fixes, so
`context.NOT_BUILT` (`common/context.py`) lists them by name and
`resolve_delivery_config` checks that table before the `allowed` one. Folding
them into one error would make a real gap look like a fixable spelling
mistake, and it would only be caught by someone reading the source rather than
the message.

**The container is still `landing/`'s ordinary filename.** `matching()` and
landing retention run `filename_pattern` over the container exactly as they do
for a plain CSV, so archives need no special case in either -- routing and the
evidence sweep stay ignorant that this feed unpacks at all. That is only true
because `cob_date_from: container` is the one case built: the date comes
from the same name `parse_filename` already parses.

**This is the first normalizer that copies bytes, and the copies live under
`ready/<feed>/<stem>/`, never `landing/<feed>/.unpacked/`.** That was the
first instinct, and it is wrong for a reason that only surfaces months later:
`sweep_landing` walks `landing/<feed>/` and refuses to delete anything it
cannot parse, because unparseable means *evidence, keep it*. An extracted
member matches no `filename_pattern` and would report as `unrecognised` on
every nightly sweep, forever, with the count logged once a night and acted on
by nobody. Putting the members under `ready/` instead removes three problems
at once rather than needing three guards: `list_landing` is prefix-scoped so
it never sees them, landing retention never sees them, and `landing/`'s
evidence semantics stay honest -- the container is what the upstream sent, a
member is a derived artefact.

**A member's `object_key` is derived from the container's filename and the
member's own name, never a timestamp or a run id.** `already_ingested`
matches on `_source_file` (`arrival.py`), so an unstable key would
re-ingest a re-normalized delivery as a new `_file_version` -- the same
silent loop `find_pending`'s retention filter exists to prevent, one level
down.

**A member name is validated, not sanitised, before it is joined onto the
destination prefix.** `_safe_member_name` (`ingest/normalize.py`) refuses
anything containing a path separator or naming `.`/`..` outright, rather than
stripping or normalising it into something that looks safe -- a zip member
naming `../../ref_counterparty/injected.csv` is the standard archive-traversal
bug, and it would write into another feed's `ready/` prefix or over a
manifest if joined blindly.

**`parts` order is sorted by member name, not archive order.** A zip's
internal member order is whatever the sender's library happened to write, and
letting it drive `parts` -- and therefore the union order `ingest` reads --
would make ingestion depend on how the sender built the file. Sorting makes
re-normalizing an unchanged archive byte-identical, the same property
`file/v1` gets for free from having only one part.

**Write is not optional here**, unlike the pass-through normalizer, which
`normalize(..., write=False)` uses so a manual `--object landing/...` ingest
leaves no queue entry. An archive's members have to be materialised under
`ready/` before anything can read them, and the `ready/` sweep collects them
by iterating manifests -- skip the write and the extracted members exist with
no manifest pointing at them, uncollectable by the retention pass that is
supposed to own them. Cheaper to always enqueue and let the delivery be
marked ingested normally.

**An archive that unpacks to zero matching members is a load error, not an
empty day.** `expected_min_rows` exists to catch a truncated file; landing a
delivery with no parts would pass that floor by accident rather than by
having actually delivered rows.

## control-file-gate

A second object beside the delivery — the control file — saying the sender
considers it complete and what it contains. `normalize` emits no manifest
until it has landed, and reads the integrity pair out of it.

```yaml
delivery:
  control:
    pattern: '{stem}\.ctl'
    row_count: 'ROWS=(?P<rows>\d+)'
    md5: 'MD5=(?P<md5>[0-9a-fA-F]{32})'
```

**It gates the DELIVERY, not the file**, so it sits on either kind:

* `row_count` is the total across an archive's members — what ingest counts
  once the parts are unioned.
* `md5` is the CONTAINER's for an archive, the object's for a plain delivery:
  the sender hashed what it sent, and an archive's members are this
  platform's own extraction. The manifest's `checksum_objects` records which
  objects a declared md5 covers, so `ingest_feed` never branches on the kind;
  absent, it falls back to `parts`, whose one part IS the source object for
  any delivery that could carry a declared md5 before the key existed.
* **That invariant spans two modules.** `ui/arrivals.checks()` compares the
  declared md5 against the md5 the registry measured of the source object.
  `tests/test_arrivals.py` pins it.

**One gate, every normalizer.** The wait is `normalize._gate`, called by both
rather than implemented in each, so a normalizer added later inherits gating.
For an archive it runs BEFORE the members are extracted: waiting is the
ordinary state and `reconcile` runs on every poll, so extracting first would
rewrite every member on each pass of a wait that has not finished.

**`pattern` is a regex template, not a filename template.**
`'{stem}\.ctl'.format(stem="X")` is `'X\.ctl'` — the backslash escapes the dot
for the regex and is not a character in any real filename. `_find_control_key`
substitutes the data file's own stem, `re.escape`d, and full-matches that
regex against the other objects in the same landing folder; it does not build
one candidate key and HEAD it. `resolve_delivery_config` validates
`pattern.format(stem="X")` as a compilable regex for the same reason: the
validation and the runtime have to agree what kind of string this is.

**`NotReady` is not `ValueError`.** Absent is a wait that clears on its own;
present-but-disagreeing is a format change upstream that never will, and
conflating them makes a real break look like an ordinary wait.
`feed_ingest.normalize_task` turns `NotReady` into `AirflowSkipException` —
without it, `DEFAULT_ARGS`' two retries at `RETRY_DELAY` (seconds) harden "not
here yet" into a failed run inside a minute. The delivery is picked up by the
poll path (`find_pending`, `scripts.bulk_ingest`) once the control file lands.
No timeout: [#no-arrival-timeout](#no-arrival-timeout).

**The exact count is an equality check beside `expected_min_rows`, not a
replacement for it.** The floor catches a truncated file and exists for every
feed; the equality exists only where a control file states a count. Both
abandon the branch and leave `main` untouched.

**The manifest records `control_object`, `declared_row_count` and
`declared_md5` as OBSERVATIONS**, never as an ingested flag —
[#ready-is-a-derived-index](#ready-is-a-derived-index).

**A control file matches no data pattern**, so `route()` checks control
patterns only after both data patterns decline the name, and triggers with
`key=None` — it names no delivery, and handing its key to `resolve_arrival`
would try to normalize the control file as data. `None` routes the run through
`find_pending`, which picks up whichever delivery it just unblocked. Which
FEED a control file belongs to is
[#a-control-file-is-attributed-by-its-stem](#a-control-file-is-attributed-by-its-stem).

## control-file-formats

`control.format` names HOW a control file is read, separately from WHAT is
read out of it. The original reading -- a regex per field over the file's
whole text -- stays the default and the name of a format nobody has to write
down; the second is `kind: delimited`, for a control file that is a small
table.

```yaml
delivery:
  control:
    pattern: '{stem}\.ctl'
    format:
      kind: delimited
      delimiter: '|'          # required, never inherited
    row_count: RECORD_COUNT   # a COLUMN NAME, not a regex
    md5: CHECKSUM
```

against

```
FEED|BUSINESS_DATE|RECORD_COUNT|CHECKSUM
POSITIONS|20260801|2|517263d1618098b81bb21c1cb7cfed25
```

**The field values change meaning with the format, and that is the point.**
A regex over a delimited line has to count the fields in front of the one it
wants -- `^(?:[^|]*\|){3}(?P<rows>\d+)` -- so a column inserted upstream
reads the wrong value rather than failing. Naming the column moves the
question to the header row, where the sender answers it on every delivery.

**One reader, two callers.** `arrival.control` (identity: COB date, version,
read at the door) and `delivery.control` (integrity: row count, md5, read on
the landing side) are the same file: the gate PROMOTES the control file into
`landing/` byte for byte rather than consuming it, see
[#the-inbox-is-the-conformance-gate](#the-inbox-is-the-conformance-gate).
They had a regex loop each, identical but for the wording of the error.
A second format would have made that two implementations of one dispatch, so
`ingest/control.py` is now the only place a control file is parsed and the
loops are gone. What each caller does with the strings it gets back stays
where it was -- `conform` turns `cob_date` into a date it must name a file
after, `ingest_feed` compares `row_count` against rows it counted -- because
that boundary is the identity/integrity split and it does not belong inside a
parser.

**The format is declared on each block, and `check_gates_are_coherent`
refuses two that disagree.** The alternative was one declaration inherited by
the other block, which is fewer lines and leaves `arrival.control` unable to
say how to read the file it is handed; `resolve_arrival_config` would also
have needed the delivery block threaded into it, including on the console's
validation path where the two are validated apart. Declaring it twice is safe
only because disagreement is a LOAD error: the two blocks parse the same
bytes, so a divergence would not fail at load, it would fail later, on one of
the two paths, looking exactly like an upstream format change.

**Refusals, and why each is a refusal rather than a guess:**

* **No `delimiter`, and it is never defaulted from the feed's own.** A
  comma-separated data file routinely arrives beside a pipe-separated control
  file. Reading pipes as commas raises nothing anywhere in `csv` -- it yields
  ONE column whose name is the entire header line -- so the error names that
  as the likely cause, because it is the mistake that produces it.
* **Exactly one row of values.** A control file with several describes
  several deliveries, and which row belongs to this one is not decidable from
  the delivery's name. Taking the first would pick silently, and wrongly on
  the day it mattered.
* **`columns:` or a header row, never both.** With both, a sender who
  reorders their columns and updates their header is read against the stale
  list: every field found, all of them the wrong value.
* **A field naming an undeclared column.** Only decidable at load for a
  headerless file, where the list is the only thing that can say where a value
  is -- so it is checked there, and left to the delivery where a real header
  row will answer it.

**The console writes the format too, and that is not decoration.** The feed
console rewrites a feed's whole block from the form payload, so a key the form
does not carry is a key the next save DELETES. A delimited control file
silently reverting to the regex reading is not a validation failure anywhere:
the fields simply stop matching, at ingest, on a feed nobody touched. One
widget writes one format into both control blocks, which makes the coherence
error above unreachable from the form rather than something to be understood
in it.

## a-control-file-is-attributed-by-its-stem

**The stem was never a wildcard.** A control file's name is its DATA file's
stem plus a suffix -- `find_control` builds it that way and the predicates read
it back -- so the stem is the shape of a name the feed already declares.
Substituting the feed's own data-name shape for `{stem}` answers the question
that was actually being asked, statically, with no I/O and no new state:

| control filename | before | after |
|---|---|---|
| `MARGIN_20260901.ctl` | ambiguous | `tr_margin_call` |
| `positions.ctl` | ambiguous | `trs_position` |
| `something_else.ctl` | claimed by whichever feed declared `.ctl` | unroutable |

### Reading a name is not building one

`render_filename` refuses `\d`, `{8}` and `[A-Z]`: it must produce a name the
feed will accept, so it only handles constructs with one literal form. That is
much too strict for RECOGNISING a name -- `POS_\d{8}\.TXT` is an ordinary
legacy pattern whose stems are perfectly recognisable. Hence `stem_pattern`,
which splits the pattern at its last literal extension rather than rendering
it, and `sample_name`, which produces one representative string for a pattern
`render_filename` would refuse. The two are deliberately separate from the
strict path: nothing parses a sample back, so it may be permissive.

### And what cannot be told apart does not load

Two feeds whose data names differ only by their extension -- `A_….csv` and
`A_….txt`, both sending `A_20260903.ctl` -- are genuinely undecidable at the
door. There is no rule to write: the name is all there is, and it belongs
equally to both.

So `check_control_patterns_are_distinguishable` refuses the pair at LOAD,
which makes it a `config check` failure and therefore a CI failure. The
argument is `check_gates_are_coherent`'s: a combination the platform cannot
execute must not be loadable. Leaving it to the door would mean the platform
"handles" it by rejecting every control file both feeds ever send, silently,
for ever.

It works by probe: each feed's own data-name shape yields a representative
control filename, and every feed is asked whether it claims it. Two claimants
is a collision. A feed whose pattern has no single literal extension claims
control files by suffix alone -- it is a wildcard -- and is caught the moment
any other feed declares a control block, which is right; alone it still loads,
because nothing that loads today may stop loading.

**The console checks it too, and must.** A collision refused at load is
refused for the WHOLE registry: a feed saved with one would not break itself,
it would stop `feeds()` resolving at all, taking every other feed and every
DAG with it until somebody edited YAML by hand. `registry.add()` does not
verify what it wrote, so `validate()` runs the same function over the registry
the feed is about to join.

**What the probe does not prove.** `sample_name` produces one representative
name per pattern, so two feeds overlapping only somewhere that sample does not
land are not detected. Proving the general case means intersecting two regular
languages -- a great deal of machinery for a case no real onboarding produces,
while the realistic collision always lands on the sample.

## the-sniffer

`reporting_platform/ingest/sniff.py`: propose delimiter, quote, header,
encoding, per-column types and business-key candidates from a real delivered
file. A normalizer that writes nothing — a human decides.

**DuckDB's `sniff_csv()`, not hand-rolled frequency analysis.**
`scripts/duckdb_console.py` already depends on DuckDB, so this adds no
dependency. `sniff_delivery(con, path)` takes any path a duckdb connection
can read; `sniff_bytes`/`sniff_archive`/`propose_feed` fetch the delivery
with the boto3 client `ingest/arrival.py` already uses (or read a local file,
for `.rejected/`) and sniff a LOCAL temp copy with a bare `duckdb.connect()`
— no Iceberg attach and no S3 secret, for something that reads one object.

**`sniff_csv`'s type names are DuckDB's own SQL types, not Arrow's** —
`BIGINT`/`DOUBLE`/`VARCHAR`/`DATE`/`TIMESTAMP`/`BOOLEAN`. `DUCKDB_TYPE_MAP`
translates them into this platform's `column_types` vocabulary
(`ui/scaffold.py`'s `COLUMN_TYPES`) with a module-level `assert` that keeps
the two from drifting apart silently. A decimal-looking column comes back as
plain `DOUBLE`, never `DECIMAL(p,s)`, but the map strips a parameter list
before lookup so nothing depends on that.

**A type with no platform cast falls back to "string", not to the closest
thing.** `TIME`, `TIMESTAMP`/`TIMESTAMPTZ`, `INTERVAL`, `BLOB` and `UUID` are
deliberately absent from `DUCKDB_TYPE_MAP`: `dbt/macros/engine.sql`'s
`parse_date` parses a DATE-shaped string and there is no timestamp
equivalent, so mapping `TIMESTAMP` to `date` would drop the time of day with
no error anywhere. "string" commits to nothing, which is why it is safe.

**Encoding is not detected by `sniff_csv` at all.** It assumes UTF-8
(silently stripping a BOM) and raises a catchable error on anything else, so
`_sniff_with_encoding` tries a BOM-implied encoding first, then
`ENCODING_FALLBACKS` in order. Two rules in that list are load-bearing and
`tests/test_sniff.py` asserts both:

* **`utf-16` is only ever tried when a BOM implies it**, never guessed.
  `sniff_csv(..., encoding='utf-16')` on ASCII or latin-1 bytes does not
  raise — it reinterprets byte pairs as UTF-16 code units and "succeeds" with
  one garbled VARCHAR column at HIGH confidence, before any real candidate
  gets a turn.
* **`cp1252` is tried AFTER `latin-1`.** DuckDB's `latin-1` rejects the C1
  range (0x80–0x9F) that `cp1252` accepts, so cp1252's accepted bytes are a
  strict superset: in the other order latin-1 can never be the one that
  succeeds, which is dead code wearing a comment that lies about it. In this
  order plain Western-European text gets the more accurate label and `cp1252`
  — the widest net — is what `encoding_confidence: "low"` flags. A byte
  undefined in both raises one clear `ValueError` rather than a duckdb
  traceback from whichever fallback ran last.

**`candidate_keys` reuses `sniff_csv`'s own `Prompt` field** — a complete,
already-escaped `read_csv(...)` call — rather than re-deriving the escaping to
build a uniqueness scan. Single-column candidates only; a composite key is a
human's call, and several independently-unique columns is not the claim that
together they are the key. A header-only file proposes none.

**It returns the FULL per-column type map, not yet reduced to overrides**,
matching `ui.scaffold.resolve_types`. A caller persisting it into a feed file
calls `ui.scaffold.overrides_only` first, the same reduction every other write
path uses, so a column the sniffer agrees with `infer_type` about produces no
diff.

### Archives, and the console side

`sniff_archive` extracts matching members to a temp file and sniffs the FIRST
one by name — the same ordering the archive normalizer uses. That is an
assumption worth stating: `parts: concat` means every member is the same
logical shape cut into files, so one member's shape IS the delivery's.
`member_pattern` is optional — absent (onboarding, no feed yet), every member
is a candidate and `member_pattern_candidate` groups them by extension;
passed (re-sniffing an existing feed), only matching members are considered.

**Only `cob_date_from: container` is ever proposed**, because
`delivery.cob_date_from: member`/`path` are NOT BUILT and `context.NOT_BUILT`
rejects them at load. `_container_has_a_date` reuses
`ui.registry.derive_pattern`'s check — does the container's name have an
8-digit run to anchor a group to? — and `propose_feed` reports the answer as
`container_has_date` rather than proposing a value guaranteed to fail.

**The console surfaces `inbox/.rejected/`, not a bucket-wide scan.** A
general "any unclaimed object anywhere" discovery is an unsolved design
question; what exists is the inbox's own backlog of files `route()` could not
match. `list_rejected` lists it, **re-running `route()` rather than trusting a
stored reason** — feeds.yml may have moved on, and a file that would now land
is flagged (`now_claimed_by`, `now_routes_as_control`) rather than offered up
to sniff, which would create a second feed for something an existing one
claims. `read_rejected` validates its `filename` as a bare name before joining
it onto `INBOX`, the traversal concern `_safe_member_name` guards for an
archive member, here for a name arriving over HTTP.

`feed-ui` has a read-only `./inbox` mount (`inbox` keeps the read-write one,
being the process that moves files into `.rejected/`) and three routes: `GET
/api/unclaimed`, `POST /api/unclaimed/{filename}/sniff`, and `POST /api/sniff`
for a plain upload. A proposal pre-fills `feedForm` directly, since its fields
already read `f?.xxx` for an existing feed and a proposal is shaped the same
way.

**Business key candidates are a note, never auto-selected**, and
`draft.delivery_expected` is defaulted before `feedForm` sees a proposal:
`feedForm` reads `f ? f.delivery_expected : true`, a truthy check on the
object rather than on the field, so a proposal that does not set it would
silently opt the new feed OUT of the gap check with nothing on screen saying
so.

## console-delivery-support

`ui.registry.FeedSpec` carries `delivery` and `arrival`, `BLOCK_ORDER` carries
both keys (after `filename_pattern`), and `feedForm` has the UI for them: the
console creates and edits an archive or control-gated feed, rather than only
sniffing one and describing what a human would then add by hand.

**Validation reuses `context.resolve_delivery_config` and
`resolve_arrival_config` directly, called from `ui.registry.validate`.** The
same functions feeds.yml load calls — so a typo or an unbuilt combination (a
`member_pattern`-less archive, `delivery.parts: separate`) fails in the form
with the SAME message it would raise at the next Airflow parse. What the
loader refuses is not this entry's to enumerate.

`validate` also runs `check_control_patterns_are_distinguishable` over the
registry the feed is about to join, because that refusal is about the whole
registry rather than one feed:
[#a-control-file-is-attributed-by-its-stem](#a-control-file-is-attributed-by-its-stem).

**The control block is offered for BOTH kinds.** A container gated on a
control file beside it in landing is a shape the loader accepts, so
`syncDeliveryVisibility` shows the control fields whichever kind is selected
and `readDelivery` reads them for both. Only `member_pattern` is archive-only.

**`kind: file` is never written explicitly.** It is the implicit default, and
writing it for the ordinary case would put `delivery: {kind: file}` in every
feed the form creates. `_delivery_from_payload` drops it; a blank `control:
{pattern: "", row_count: ""}` — the fields present on the form but unused —
collapses to `{}` rather than a block that validates as broken.

**Editing preserves a block it was not asked to change, and removes one that
was cleared.** Both directions are load-bearing and neither is free:
`spec_from_feed` round-trips `fd.delivery` and `fd.arrival`, or any edit
through the console — renaming a description, say — silently deletes them,
since `update()`'s generic loop drops a `BLOCK_ORDER` key the new spec does
not set. `_arrival_block` must therefore write every sub-block it can be
handed, `archive:` included. Clearing the control fields on an edit deletes
the key, the same mechanism in the other direction. Both asserted in
`tests/test_delivery_form.py`.

**A sniffed proposal fills the fields rather than describing them.** An
archive sniff's `member_pattern_candidate` sets `deliveryKind` to `archive`
and pre-fills `memberPattern`, in the upload handler and in `newFeed(draft)`,
which builds the `f?.delivery` shape `feedForm` expects from the proposal's
flatter fields.

## the-inbox-is-the-conformance-gate

`landing/` has a CONTRACT: every object in it is correctly named and
classified. `Feed.parse_filename` answers for every delivery, landing
retention can date every object, and `find_pending` computes its keep-set from
the dates it sees there. Every simplification downstream rests on that.

Two ways in, and the contract holds either way:

| Path | For | What happens |
|---|---|---|
| direct `PutObject` into `landing/` | an **approved** sender that adheres to the contract | nothing; it already meets the standard |
| the **inbox gate** | a legacy sender that does not | classify, wait for the control file, name it correctly, promote |

`arrival:` marks the second; a feed without one is the first. Opt-in, because
making it mandatory would demand a control file from senders with no reason to
ship one.

### The inbox establishes IDENTITY. Ingestion verifies INTEGRITY.

The line the whole design turns on. The gate ensures a delivery is correctly
named and has its prerequisites — source system, feed, COB date, version. It
does **not** check the row count or checksum, and neither block may carry the
other's keys:

```yaml
arrival:                       # IDENTITY -- read by the inbox
  source_pattern: 'positions\.csv'
  control:
    pattern: '{stem}\.ctl'
    cob_date: 'ReportingDate\|(?P<cob_date>\d{8})'
delivery:                      # INTEGRITY -- read in landing, checked at ingest
  control:
    pattern: '{stem}\.ctl'
    row_count: 'ROWS=(?P<rows>\d+)'
    md5: 'MD5=(?P<md5>[0-9a-fA-F]{32})'
```

`arrival.control` rejects `row_count`/`md5` at load as unknown keys. Checking
them at the door instead would put a second implementation of the same check
on one of the two paths — and leave the TRUSTED path less verified than the
untrusted one, since an approved sender writing straight to landing would get
no checksum check at all. Both paths verify identically, once, at ingest.

### The control file is PROMOTED, not consumed

The delivery is the data file and its control file **together**, so the gate
renames both into `landing/` and the prefix always holds the whole delivery
whichever way it arrived. That is what lets `delivery.control` read it there
for a legacy feed exactly as for an approved sender. Consuming it at the door
stalls the delivery forever and silently: landing never sees one, `normalize`
waits on a sibling that cannot arrive, and `reconcile` reports
`awaiting_control` at INFO because from its side the sender is merely late.

So the two blocks are **complementary, not alternatives**:
`check_gates_are_coherent` rejects an `arrival.control` with no
`delivery.control`, which would promote a control file nothing reads and
quietly lose the row count and checksum for the feeds least likely to deserve
that trust.

**The promoted control file is named from `delivery.control.pattern`**, not
from the name it had in the inbox, because that is the pattern which has to
find it — and it is checked against that same regex before being used.

### Two names, and the rename cannot be wrong

`positions.csv` in the inbox; `trs_position_20260801.csv` in landing.
`Feed.claims_source` answers the first, `Feed.parse_filename` the second, and
`ingest/conform.py` is the only thing that crosses between them.

`common/filenames.render_filename` builds the landing name FROM
`filename_pattern` and feeds it back through `parse_filename` before returning
it, so "the inbox renamed a file to something landing will not match" is
structurally impossible rather than something a test must remember. That
failure is the silent kind: the file lands, `find_pending` never matches it,
and the feed reports nothing pending for ever.

### Versions, so a re-delivery cannot overwrite evidence

A corrected file for a landed COB date must not overwrite the first — the
original is the evidence of what was originally ingested. The gate renders the
unversioned name and, if the listing shows it taken, tries `_v2`, `_v3`. A
version the control file itself declares wins outright: the sender said which
restatement this is.

### Two failure modes, and they land differently

* **IDENTITY failure** — no feed claims the name, or no COB date can be found.
  The file cannot be NAMED, so there is no landing key to write it to.
  `.rejected/`.
* **INTEGRITY failure** — wrong row count or checksum. Nothing to do with
  naming: the delivery **lands**, because "the upstream sent a truncated file
  on the 3rd" is precisely what the evidence copy exists to prove, and the
  ingest then refuses, abandons its branch and leaves `main` untouched.

A missing control file is neither: nothing is wrong and the sender is not
done, so the delivery is HELD for the next pass. `NotReady` and
`ConformanceError` stay separate types —
[#control-file-gate](#control-file-gate).

### The metadata sibling

*Every key, with worked examples, is in
[DELIVERY-SHAPES.md#the-metadata-sibling](DELIVERY-SHAPES.md#the-metadata-sibling);
this is why it holds what it holds.*

`landing/<feed>/<delivery>.meta.json`. The landed objects are byte-identical
to what the upstream sent but carry the PLATFORM's names, so the originals —
and everything else about the arrival — survive only here: source filename,
source control filename, source system, received and promoted times, and the
bytes/rows/md5 as measured at the door. Those measurements are **recorded, not
compared**; if the ingest later disputes the row count, they say whether the
file changed after arrival or arrived wrong. It embeds no copy of the control
file's text, which is promoted intact and would otherwise have two versions
with nothing keeping them in step.

**The `.meta.json` suffix is load-bearing.** `retention/landing.py` dates an
object from its own name and refuses to delete anything it cannot date; a
metadata object carries its delivery's name inside its own, so it dates by
stripping the suffix, with no lookup, and an orphan still expires.

**A control file is dated from the SIBLING it gates** — the delivery in the
same prefix whose stem its `delivery.control.pattern` matches, found with no
extra S3 call because the sweep has already listed the prefix. It matches no
`filename_pattern` of its own, so without this the sweep counts it
`unrecognised` and never deletes it. Searching the control filename for an
8-digit run would be simpler and is deliberately not done: this authorises
deletion from the evidence copy, and a name with two 8-digit runs picks the
wrong one. No sibling means keep.

## containers-run-as-the-host-uid

`docker-compose.yml` sets `user: "${AIRFLOW_UID:-50000}:0"` on the
`x-airflow-common` anchor, and `.env` carries `AIRFLOW_UID=$(id -u)` on Linux.

**Half the bind mounts here are READ-WRITE by design**, which is what makes
this necessary rather than cosmetic. The feed console writes `feeds.yml`, the
dbt source file and a scaffolded model; the inbox watcher creates
`.processed/` and moves delivered files into it. A bind mount takes its
ownership from the HOST, which no `chown` in the image can reach -- the same
constraint [#dbt-working-directories](#dbt-working-directories) solved for
dbt's own working directories by moving them OUT of the bind mount. That
escape is not available here: writing to the checkout is the point.

**The symptom never says "permissions."** The console returns a bare HTTP 500
with the traceback only in `docker compose logs feed-ui`, and the inbox
watcher uploads a delivery to landing correctly and THEN dies moving the local
file -- leaving a delivery that is landed, correctly renamed, and still
sitting in the inbox. Both were hit during the work on
[#the-inbox-is-the-conformance-gate](#the-inbox-is-the-conformance-gate) and
worked around locally before being fixed properly here.

**The gid stays 0 and that is the load-bearing half.** Everything the image
needs to write -- `/home/airflow` (`drwxrwx--- 50000:0`), `/opt/airflow/logs`,
`/opt/platform/run` and the named volumes under it -- is owned `airflow:0`
with `g+rwX`. `Dockerfile.airflow`'s last layer says so in as many words:
"airflow:0 with g+rwX, so an arbitrary assigned uid in group 0 can still
write." The image was already built for this; it was simply never switched on
at the compose level. So the uid may change freely and the gid may not.

Default `50000` preserves the old behaviour for anyone with no `.env` entry,
and macOS/Windows should leave it unset -- Docker Desktop's VM maps ownership
and 50000 is correct there.

## no-folder-markers

`minio-init` creates the **bucket** and nothing else. S3 has no directories: a
prefix exists exactly when an object whose key starts with it exists, so
nothing can create one in advance. An `mc mb local/lakehouse/landing` reads as
"make the prefix" and does not do that — it PUTs a
**zero-byte object whose key is literally `landing/`**, a folder marker,
which makes the MinIO console draw a folder icon and is otherwise a lie.

### Why bother removing two empty objects

They are objects that no feed owns, no `filename_pattern` matches and nothing
reads. Any sweep over the bare `landing/` prefix therefore has to classify
them, and the honest classification is "unrecognised, never delete" -- the
category reserved for a delivery whose name is wrong, which is a
configuration error worth chasing. Two permanent false entries in that count
is exactly the kind of noise that trains people to ignore it.

Both existing sweeps happen to miss them, and that is luck rather than design:
`retention/landing.py` is scoped to `landing/<feed>/` and never sees the bare
prefix, and `retention/orphan_storage.py` skips keys with fewer than four
segments (`warehouse/` has two). Neither was written with a marker in mind.
The next bucket-wide sweep would not be so lucky.

### Nothing needs them, and that was checked rather than assumed

Iceberg writes through `S3FileIO` (`spark.sql.catalog.lakehouse.io-impl`,
set identically in `common/spark.py` and `dbt/profiles.yml`), which PUTs
keys directly and has no notion of a parent directory. The only S3A use is
`spark.read.csv("s3a://...")` READING a landing object that already exists.

### Removing the markers from an existing stack

They are not cleaned up automatically, on purpose: an init container that
deletes objects is a footgun, and the obvious `mc rm local/lakehouse/landing/`
is a RECURSIVE delete of every delivery this platform has ever received. Use
an exact-key delete, which cannot recurse:

```bash
docker compose exec -T airflow python -c "
from reporting_platform.ingest.arrival import _client, _bucket
s3, b = _client(), _bucket()
for key in ('landing/', 'warehouse/'):
    assert s3.head_object(Bucket=b, Key=key)['ContentLength'] == 0
    s3.delete_object(Bucket=b, Key=key)
    print('deleted', key)"
```

## unpacking-happens-at-the-gate

`arrival.archive: {member_pattern: '...'}`. A container dropped into the inbox
is unpacked there, and its MEMBERS are landed as ordinary deliveries. The zip
never reaches `landing/`.

**A zip is not a delivery, it is a transport wrapper**, and removing wrappers
is exactly what the gate is for. Once that is said out loud the shape follows:
unpacking turns ONE inbox file into N inbox files, each of which takes the
ordinary single-file path. There is no grouping to invent, no `parts` list, no
manifest holding members together, and nothing downstream needs to know an
archive was ever involved.

That is only true because **each member is a complete delivery for its own
COB date** -- and each must therefore say which day it is for. Two sources
can answer, exactly as for a plain delivery: `member_pattern` captures
`(?P<cob_date>...)`, or `arrival.control` reads the date out of a control
file **packed beside the member in the same container**. Exactly one, checked
at load: neither and the gate cannot name what it unpacks; both and one fact
has two sources that can disagree.

Members that are PARTS of one delivery (a single date split for size) are a
genuinely different shape: they would have to stay grouped, which needs
something to record the grouping, and that is the `parts: concat` design the
archive normalizer implements. The gate has no key of its own for it and
needs none -- `delivery.kind: archive` is the mechanism for that shape, and
the error a member-dated pattern raises now says so.

### A member's control file is the same idea in a different namespace

A plain delivery's control file sits beside it in the inbox directory; a
member's sits beside it inside the zip. Same question -- which of these names
is this delivery's control file, and what does it declare -- asked of two
namespaces, so `find_control` and `read_control` serve both and the pairing
rule exists once. That is what `conform.Siblings` is: the names a shape can
see and how to read one, supplied by the shape.

Two consequences worth stating, because they are where the shapes genuinely
differ:

**A member's missing control file is a REFUSAL, not a wait.** A container
arrives complete, so a file absent from it will never turn up beside it later.
Holding the member would wait for ever and report nothing at INFO -- which is
precisely the failure this path was built to remove (see below).

**A member shaped like the control file is never landed as a delivery**, even
when `member_pattern` claims it. A permissive `.*\.csv` and a sender who
writes its control file as CSV is an ordinary combination, and landing it
would ingest the control file's own text as rows.

### The container is recorded, not landed

The members are the evidence: they hold the rows the upstream sent, byte for
byte, and landing them loses nothing that the rows themselves carried. What
would be lost is the fact that a container existed at all, so the metadata
sibling records `source_container`, `source_container_bytes` and
`source_container_md5`. What arrived stays provable -- and provable against
the original bytes, since the md5 is of the zip as received -- without keeping
an object in the evidence prefix that nothing ever reads and that landing
retention would have to learn to date.

The container itself survives in `inbox/.processed/<feed>/` for as long as
that directory is kept, which is an operational choice rather than a platform
guarantee.

### Two details that are easy to get wrong

**`taken` advances as members land, it is not read once.** Two members
resolving to the same COB date inside one zip would otherwise both render
the unversioned name and the second would silently overwrite the first, inside
the evidence copy. The set is seeded from `list_landing` and added to after
each member, so the second becomes `_v2`.

**One bad member does not discard the rest.** A member that cannot be named is
reported and skipped; the others land. Rejecting a whole week of files because
one of them is misnamed would turn a small upstream error into an outage, and
the good members are real deliveries that arrived.

The traversal guard moved across with the unpacking: a member named
`../../etc/passwd` would be written outside the feed's landing prefix, into
another feed's evidence. Only a flat filename is accepted, rejected outright
rather than normalised into something that looks safe. Note a strict
`member_pattern` filters such a name out before the guard is reached -- the
guard exists for a permissive one, and the test uses a permissive pattern for
exactly that reason.

## a-delivery-shape-is-a-registry-entry

Every awkward delivery is a variation on two questions: what does this file at
the door BECOME, and what does that landed object become a MANIFEST of. Each
has a table, and adding a shape is an entry plus a function:

| Seam | Table | Contract |
|---|---|---|
| the door | `conform.ARRIVAL_SHAPES` | `planner(feed, filename, content, siblings, ...) -> list[Outcome]`, PURE |
| the landing side | `normalize.NORMALIZERS` | `normalizer(feed, object_key) -> manifest`, may copy bytes into `ready/` only |

**The planners write nothing.** They return what SHOULD be written, which is
what lets every shape be tested without S3 or a watcher, and what let the
member-control path be verified before an inbox had ever run it.

**Four outcome types are the whole vocabulary**: `Planned`, `Refused`,
`Duplicate`, `Waiting`. `inbox.py` dispatches on those and on nothing else --
not on the kind, not on whether a control file exists -- so a shape added
later needs no change there. The rule that keeps it honest: **a new shape must
not need a new outcome type.** If it does, it is asking the watcher to do
something the watcher does not know how to do, and that is a conversation to
have before writing the planner rather than a fifth branch.

**What is deliberately NOT abstracted.** There is no plugin loader, no entry
points, no configuration naming a Python path. A shape is code in this repo
that a reviewer can read, and the tables are `dict`s two modules long. The
extension point is "add a function and a line", not "make it configurable" --
which for a platform whose whole argument is that config must be refused at
load rather than interpreted at run time would be the wrong direction.

## an-unchanged-resend-is-a-no-op

A byte-identical file delivered twice through the inbox gate lands **nothing**
— not a `_v2`, which would be a restatement the upstream never made.

**Sameness is decided on the bytes, not on the name**, and only against
deliveries already landed for the same COB date. `conform()` and
`conform_member()` take a `landed_md5` callable beside `taken`; `_free_name`
compares every candidate that is already taken — including one a control file
declared a `version` for — and raises `DuplicateDelivery` on a match instead
of stepping past it. That exception is deliberately **not** a
`ConformanceError`: `inbox._promote` routes one of those to `.rejected/`, and
nothing is wrong with a retried transfer. The inbox copy moves to
`.processed/`, where `_move` timestamps a colliding name, so the resend stays
on disk as evidence that it happened — just not as evidence of a new delivery.

**None means unknown, and unknown versions.** `landed_md5` returns None when
there is no record, and the caller must not read that as "different bytes"
either way round. Versioning is the fail-open direction: a needless `_v2`
costs one object, a suppressed restatement costs the evidence permanently.

`arrival.landed_md5_lookup` reads the `.meta.json` sibling's recorded `md5`
first and falls back to hashing the landed object. The fallback is not
redundant — `_promote` writes the data file *before* the metadata and treats a
failed metadata write as the survivable failure, an approved sender writing
straight into `landing/` has no sibling at all, and neither does anything
landed before this existed. The feed's landing listing is already in hand, so
a name with nothing under it costs no request.

**A container resent whole is the expensive case**, and it forced one further
change. `_promote` left the container in the inbox when nothing
landed, so a zip whose members were all duplicates would be unpacked again on
every pass forever. Duplicates now count as handled.

This does not *record* the resend anywhere but the log and the sweep outcome.
The delivery registry is what makes a rejected or duplicate delivery a row
rather than a moved file; until then, a no-op is the whole requirement.

## delivery-expected-not-completeness

`Feed.completeness` is now `Feed.delivery_expected`.

It is a boolean meaning "a delivery is expected on every COB date",
read only by `monitoring/completeness.py` to opt a feed out of gap detection.
"Completeness" separately means "is *this delivery* whole, and by what
evidence" — a declared row count, a checksum, a control file. Different
subject, different lifetime, different reader.

Two meanings under one key, in the one file every team edits, is how one of
them ends up set to answer the other — and the failure is silent in both
directions: a monthly feed left in the gap check reports a false gap every
day, and a daily feed opted out stops being checked with nothing on screen
saying so. The names were separated before the second meaning acquired any
config of its own, which is the only cheap moment to do it.

The check keeps its name: `monitoring/completeness.py`, the `completeness` op
in `scripts/_spark_task.py` and the `completeness_check` housekeeping task all
still do COB-date completeness, and that is what they are called.

A hard rename with no compatibility shim. No feed block in `feeds.yml` set the
key, and the YAML allowlist is derived from `Feed.__dataclass_fields__`, so a
hand-added `completeness:` is now a load-time error naming itself rather than
a silently ignored line. Nine files: `common/context.py`, `ui/registry.py`
(field, `from_payload`, `spec_from_feed`, `BLOCK_ORDER`,
`OPTIONAL_WITH_DEFAULT`), `ui/app.py`, `ui/static/index.html`,
`monitoring/completeness.py`, and the four docs that describe the key.

## no-arrival-timeout

`arrival_timeout_hours` and `arrival_poke_seconds` are deleted. Nothing read
either of them.

Deleted rather than implemented, on purpose. The requirements introduce
`expected_by` as the lateness concept, with a named owner and an action.
Building a second, weaker one now and superseding it in a phase's time repeats
the `completeness` collision knowingly — see
[#delivery-expected-not-completeness](#delivery-expected-not-completeness).

What actually prevents a late control file from failing a run is unchanged and
was never this field: `feed_ingest.normalize_task` catches `NotReady` and
raises `AirflowSkipException` rather than letting `DEFAULT_ARGS` turn "not here
yet" into a hard failure in under a minute, and the safety-net poll path
(`find_pending`, or `scripts.bulk_ingest`) picks the delivery up whenever the
control file lands, however long that takes.

## published-tags-are-the-reproducibility-window

`references.published_tags` is DATA retention, sized in years, and it is not
the keep-set the table layers use. A tag pins every data file its commit
referenced, so **its lifetime is the only thing that decides whether a
published run can still be read** — size it by a table keep-set and an
ordinary daily publication loses its pin after about a fortnight.

**The `nessie_gc` cutoff is not a second threat.** It bounds how much history
*behind* a reference stays live and never decides whether the reference's own
state does — even at `P0D` the walk takes the HEAD before stopping. So a
published tag protects what it pins at any cutoff, and the tag's own window is
the whole of the question.

### The policy

Flat age in years, resolved per report — the reasoning `landing:` already
carries, that sampling evidence by keep-set destroys exactly what it exists to
preserve. `default_keep_years` is a bare number or a map keyed by
`REPORTING_ENV`, the shape `nessie_gc.deferred_delete_after_hours` already
uses, because `landing.keep_years` is per environment and `dev` deliberately
shortens it.

**Ten years is provisional.** The regulatory period is unconfirmed;
over-retaining costs storage and under-retaining costs the evidence
permanently, so it is the longest plausible value rather than the assumed
seven.

**`per_report` matches nothing today**, and it is written down as a forward
hook rather than presented as a working mechanism: no publication yet knows
which report it is for, so every tag resolves to the default. `TAG_RE` accepts
`published/<report>/<cob_date>/<run_id>` beside today's shape, so when a
publication does name its report, retention already honours it instead of
silently applying the default to a report that declared otherwise. Naming that
plainly is the point — a guard written against a mechanism that does not exist
is the failure mode this repo keeps rediscovering.

> **Amended.** `per_report` matches now. The reporting build's `publish` task
> cuts one `published/<report>/<cob_date>/<run_id>` tag per report when it
> merges (`context.published_tag`), and `expire_tags` resolves each tag's
> window from the `report` group `TAG_RE` reads out of it — the tag moved
> there from the ingest DAG, and why is
> [#an-ingest-is-not-a-publication](#an-ingest-is-not-a-publication). Every tag
> still resolves to the default in practice, but because `per_report` is
> empty — no report has declared a period of its own — not because nothing
> could match. The two-segment shape called "today's" above is now the legacy
> one: still matched, and judged by the default because it names no report.
> Accepting the report segment before anything wrote it is what let retention
> honour the per-report tags on the day they started appearing, rather than
> silently giving a report that declared a window the default.

**Age is the commit time**, not the COB date. A retention period runs from
when the record was made, and a restatement published today for an old COB
date is a new record that needs its own full window; measuring from the
COB date would expire it on arrival. The COB date is the fallback for
a tag with no readable commit time, and it is conservative by construction —
a publication cannot precede the date it reports on, so it can only ever keep a
tag the commit time would also have kept.

### The interlock refuses

`check_reproducibility_window()` aborts the whole chain when
`landing.keep_years` is shorter than the longest published-tag window. A tag
pins the *tables*; reproducing a published run also means showing its inputs,
and `landing/` is the only copy of what the upstream sent.

Refuses rather than warns, unlike `landing.keep_years()`'s own interlock, on
the same test the GC cutoff already applies: landing running short of the raw
window degrades gradually and is fixed by raising it, whereas deleting landing
evidence a live pin depends on is unrecoverable and happens nightly and
unattended. It runs first in `run()` and before the dry-run branch — it refuses
on *configuration*, and a dry run that passed where the real run would refuse
would teach the wrong thing.

**This is not REQ-602 in full.** The complete rule is "retention must not
delete anything a published RUN depends on", which needs a run record
enumerating its delivery set — Phase 2. The window comparison catches the
configuration that guarantees the loss; it cannot catch a single delivery
expiring early inside an otherwise coherent window. Stated here so the
approximation is not later mistaken for the requirement.

## reproducibility-is-exercised-not-asserted

REQ-702. `reporting_platform/monitoring/reproducibility.py`, run as the last
task of `platform_housekeeping`.

Everything that makes a published run reproducible is a *pin*, and nothing
about a pin announces its own failure. A tag deleted too early, a GC cutoff
that collected a file the tag still referenced, a compaction that rewrote data
before a snapshot expiry removed what the tag pointed at — each leaves a
catalog that looks healthy and a pin that no longer resolves, and the first
person to find out is whoever was asked to reproduce a figure from years ago.
So the pin is EXERCISED, on a schedule, against the real catalog: the
reference is resolved, the commit's metadata is opened, and every data file it
names is confirmed to still be an object.

**It runs after the maintenance chain, and that ordering is the test.**
`maintain` compacts, `enforce_retention` expires tags, runs GC and expires
snapshots. Running the check first would exercise yesterday's state and pass on
the night the damage was done.

**It holds a data file `main` no longer references, or it reports
`not_yet_meaningful` and does not pass.** A tag whose every file `main` still
uses proves almost nothing: those files are kept alive by `main`, so the read
succeeds whether pinning works or not. What makes it mean something is the pin
holding a file nothing else keeps alive — the file a too-eager collector takes.
This is CLAUDE.md's own rule about a check whose window does not contain the
thing it describes, and the honest answer is a third outcome rather than a
green one. Among the pins that diverge the oldest is chosen, because its
exclusive files have been unreferenced by anything else for longest: the most
GC identify passes have had a chance at them.

### `SELECT COUNT(*)` does not read the data

**It fails the run, where `completeness_check` beside it only warns.** A
completeness gap is an upstream missing a Tuesday: real, not the platform's
doing, and a red run there would be indistinguishable to the watchdog from
housekeeping being down. A pin that no longer resolves is the platform
destroying its own evidence in the step that just ran.

A table ABSENT at a pin is not a failure. The platform gains tables over time,
and a run published before `reporting.exposure_by_country` existed cannot be
expected to contain it. A table that exists and cannot be READ is what this
looks for.

### What it does not claim

That the numbers match what was published. It asserts the tables are READABLE
at the pin and reports their row counts; comparing against the figures actually
published needs the run record — which deliveries, which code, which report
version — and that is Phase 2/5. Until then this is a liveness check on the
pin, which is the failure mode that actually occurs and the one that is silent.

## the-registry-records-observations-not-verdicts

The registry is the platform's record of what arrived: one row per delivery,
carrying the COB date, arrival time, size, checksum, the name the upstream
used, the control file's declarations and the column contract it was read
against. It is the first thing that can answer "did they send it, and what was
in it?" without listing object storage by hand.

**Postgres, because `sequence_no` needs a serialising authority.** The order in
which the platform saw deliveries cannot be allocated by `MAX(...)+1` over a
table several writers append to — that is `next_file_version`'s read-then-write,
correct today only because the `lakehouse_write` pool has one slot, which is
precisely what the concurrency work intends to change. A database sequence is
serialised at any pool size. An Iceberg replica for analytical joins is the
other half of the recommendation and is deliberately not built yet: it needs a
namespace outside the dbt project, which `managed_tables()` derives from, so
maintenance and retention would not cover it without explicit registration.

**It records no verdicts, and that is the whole difference from the `stg`
load-control tables this platform refuses by name.** There is no `ingested`,
no `superseded`, no `status`. Whether a delivery reached the raw table stays
derived from that table's own `_source_file`, where it cannot drift; whether
one delivery supersedes another stays `dedupe_rank`'s answer, computed at read
time. Every column is a fact about an object that exists in `landing/` right
now. `tests/test_registry.py` asserts the absence of the forbidden names,
because it is a rule about what must not be added and a comment does not fail
when somebody adds it.

**Rebuildable from object storage, by construction rather than by a second
implementation.** `deliveries.reconcile()` walks `landing/` and `ready/` and
registers everything missing, and it is the same `register()` the inline path
calls. That is `arrival.py`'s own rule applied one layer up: *events are an
optimisation, the poll is the correctness guarantee.* `normalize()` registers a
delivery the moment it writes its manifest, best-effort — a failed registry
write is logged and counted, never fatal, because `landing/` is the evidence
and the raw table is the ledger, and taking ingestion down to protect an index
would be the wrong way round. `coverage()` is what makes the resulting lag
visible; verified by deleting a row, seeing it reported missing, and seeing
reconcile put it back.

**What a rebuild does not preserve is the integers in `sequence_no`.**
Reconcile inserts in `received_at` order, so the ORDER is reproduced and the
values are not. Nothing may key on the value: `_delivery_id` on the raw table
references `(feed, delivery_id)`, which is the landing filename and is stable.

**The md5 is measured once.** A landing object is immutable, so the hash comes
from the `.meta.json` sidecar where the gate wrote one, otherwise from the
object's ETag — MinIO and S3 both return the content md5 for a single-part
upload, verified against this stack — and only from reading the object when the
ETag is a multipart hash of hashes. A row already registered is never
re-hashed: `md5`, `bytes`, `received_at` and `sequence_no` are left alone on
conflict, and only what config can legitimately change is refreshed.

## quarantine-is-where-a-refused-delivery-goes

REQ-106. `registry/rejections.py`, `retention/quarantine.py`, and a
`quarantine:` block in `retention.yml`.

So the bytes go to `quarantine/` in object storage and `registry.rejection`
says what they were and why. `.rejected/` stays and is written second — it is
what the console's unclaimed queue reads and what the sniffer offers to onboard,
and both want a local file to open. It is now a working copy of something
durable rather than the only copy.

**The rejection date goes into the key**, `quarantine/<feed>/<yyyy>/<mm>/
<timestamp>_<name>`. `landing.py` dates an object by parsing its filename and
refuses to delete anything it cannot parse — correct there, where everything is
conformant by contract. Nothing in `quarantine/` is: *not being nameable* is one
of the commonest reasons a file is here, so filename parsing would decline to
delete essentially the whole prefix and the sweep would do nothing forever. The
property that mattered is kept — the date is in the object's own name, needs no
lookup — and what changes is that the platform chose the name. A key whose
folders and timestamp disagree is still left alone, because this platform did
not write it.

**The timestamp makes each ATTEMPT its own object.** An upstream resending the
same broken file every morning is producing a new event each time, and
collapsing those onto one key would keep only the last and hide the pattern.
The filename is sanitised, not trusted — the inbox is a directory anyone can
write to, and this is the same traversal `normalize._safe_member_name` refuses.

**A separate `keep_years` from `landing`, with the same value today.** The two
answer to different things: landing's has a hard floor from the published-tag
interlock and the raw keep-set, and this one has neither, because nothing is
reproduced from a delivery that never landed. Shortening this is a policy call
somebody can make on its own; shortening landing is not. **The row outlives the
bytes** — `registry.rejection` is small and is not swept, so "has this upstream
sent us something broken before?" stays answerable.

**An INTEGRITY failure is never quarantined.** A delivery whose declared row
count or checksum does not match LANDS and fails at ingest, because landing is
the evidence copy and a bad delivery is exactly what it exists to prove. Only
an IDENTITY failure — one that cannot be named — comes here. See
`#the-inbox-is-the-conformance-gate`.

## provenance-is-added-not-backfilled

REQ-303, REQ-304. Four columns on every raw table — `_delivery_id`,
`_received_at`, `_schema_version`, `_source_system` — carried into `prepared`
by `source_provenance()` in `dbt/macros/engine.sql`, so a typed value traces
to the delivery it came from, the contract it was read against and when it
arrived, not merely to a file.

**`_delivery_id` is not `_source_file`.** The latter is the PART, and
`already_ingested` depends on it staying the part, so for an archive the two
differ.

**`_schema_version` is derived, not declared** — a twelve-character digest of
the ordered `(platform name, name in the file)` pairs. A version somebody has
to remember to bump is wrong the first time somebody forgets, and the thing it
describes is there to be hashed. `column_types` is deliberately outside the
digest: it says what the prepared model does with a column, not what the file
contains, so retyping one must not look like an upstream schema change.

**Added, never backfilled.** Iceberg adds a column as metadata, so rows
ingested before the change read NULL. Backfilling would rewrite every
partition of every raw table — new data files under live published tags,
interacting with snapshot expiry and the pins retention keeps. The consequence
is stated rather than left to be discovered: an as-of query cannot use
`_delivery_id` to reach past the change and must fall back to `_source_file`.

**The migration is LAZY, which is why `ingest/migrate_raw.py` exists.**
`ensure_raw_schema` runs inside `ingest()`, on the branch, for the feed being
ingested — the right place, because that is where the table is guaranteed to
exist and where a schema change can be abandoned with a failed load. But a
feed that has not delivered since the column was added keeps the old schema
while every prepared model selects the new one, so the next build fails for
every feed that has not happened to deliver, with
`[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... _delivery_id cannot be resolved` —
naming the column and not the reason. `migrate_raw` ensures the columns on
every feed's raw table in one pass, on a branch, merged; it is idempotent,
commits nothing when they are current, and runs first in
`platform_housekeeping`. Run it by hand when deploying a new provenance
column, BEFORE the next ingest.

It covers the FEEDS' own columns too, by the same code —
[a-declared-column-migrates-itself](#a-declared-column-migrates-itself).

## a-declared-column-migrates-itself

An upstream extends its extract, and a column is added to a feed that has been
delivering for months. **This is the most frequent change a live feed ever
undergoes** — far more common than onboarding a new feed, which the whole of
[ADDING-A-FEED.md](ADDING-A-FEED.md) exists for — and until this it was the
one change with no path at all.

**So `ensure_raw_schema` reconciles the whole contract, not just the
platform's provenance four.** Same place, same branch, same commit discipline:
the `ALTER TABLE ... ADD COLUMNS` happens after the branch is cut, so an
ingest that then fails leaves the column no more merged than the rows.

**Added, never backfilled**, exactly as for a provenance column — history
reads NULL, which is the honest answer for a column the upstream was not
sending. Where a delivery *did* carry it before it was declared, the value is
not lost either: it is in `_extra_columns`, which is what would make a
backfill possible later without making it one now.

### The two directions are not symmetrical

Adding is automatic. **Removing is not, and never will be.** A column the raw
table has and `feeds.yml` no longer declares is filled with NULL on the way
in, reported, and kept.

The reason is that the platform cannot tell the two edits apart. Renaming
`trade_id` to `trade_ref` in `feeds.yml` is character-for-character a drop plus
an add, and a `DROP COLUMN` issued on that reading would delete the history of
a column that was only renamed. So the destructive reading of an ambiguous
edit is the one nothing acts on: the new column appears, the old one stays
holding what it holds, and both are reported for a human to settle. Dropping
it is a deliberate `ALTER TABLE ... DROP COLUMN`, made by someone who knows
which of the two edits it was.

This costs nothing at the write, because **the append resolves by name once
the arity matches** — verified rather than assumed: a same-arity frame written
in reversed column order reads back correctly, so the NULL fill can be
appended at the end of the frame and still land in the right column.

### Which columns are the platform's is derived, not listed

A raw table is the feed's declared columns plus ingest's own, and ingest's own
all begin with `_`. So a column that is neither declared nor `_`-prefixed is
one `feeds.yml` used to declare — and no second list of ingest's DDL is needed
to know it.

That is the same rule `lineage/columns.py:ingest_columns` already uses to
classify a raw column, which is not a coincidence and is the point: an orphan
here is the same column `python -m reporting_platform.lineage --columns`
reports as `unresolved`, so the CI seam that fails on one is the CI seam that
sees the other. Two lists would have been free to disagree.

### The model layer does the same thing, for the same reason

`dbt_project.yml` sets `on_schema_change: append_new_columns` on every model
in the project. dbt's default is `ignore`, which on an incremental model means
the new column exists in the SELECT and never in the target: **the build stays
green and the column is silently absent**, and the only remedy is a
`--full-refresh` somebody has to know to run. That default is wrong for the
change this project makes most often.

`append_new_columns` and not `sync_all_columns`, which is the same asymmetry
as above one layer up: new columns are added, and a column removed from a
model is left in place rather than dropped, because a rename is
indistinguishable from a removal plus an addition.

**It adds the column; it does not populate rows the run does not touch.**
On an SCD2 dimension a column added by `append_new_columns` reads NULL on
every *current* row until its entity next changes; on a COB-date model it
carries only across the lookback window. `--full-refresh` is therefore the
answer when history has to hold the value -- the difference is that it is a
choice about DATA rather than the only way to obtain the COLUMN.

### The order the change is deployed in

`feeds.yml` and the prepared model change together; the raw table in between
has to be told, and the table is not in the git diff. Config, migrate, build —
`docs/ADDING-A-COLUMN.md` has it in order. The lazy path means an ingest will
migrate its own feed regardless, so the ordering matters for the FEEDS THAT DO
NOT DELIVER THAT DAY, which is the same failure `migrate_raw.py` was written
for one requirement earlier.

## the-evidence-interlock-is-two-halves

REQ-602. `retention.check_reproducibility_window()` and
`monitoring/evidence.py`, and neither is the whole requirement.

The first compares two numbers — `landing.keep_years` against the longest
`references.published_tags` window — and refuses the whole sweep if landing is
shorter. That catches the CONFIGURATION that guarantees evidence loss, which
was the live defect, and it is cheap enough to run before every delete. What it
structurally cannot catch is one delivery going missing inside an otherwise
coherent window: a landing object deleted by hand, a sweep that ran against a
shorter window last month, an upload that was never actually made. The numbers
still agree; the evidence is still gone.

So the second half is per delivery. For each live published tag it takes the
COB date the tag names, asks the registry which deliveries were received
for that date, and checks each one's landing object is still there. It runs
AFTER the retention chain, because it has to observe what that chain left
behind, and after `registry_reconcile`, because a delivery with no row looks
exactly like a pinned date whose evidence is gone.

**It fails the run on a missing object and only warns on a date with no
registered deliveries at all.** The second is what an unreconciled registry
looks like, indistinguishable from the real thing on the evidence available
there, and failing the chain the first time a tag is cut before reconcile has
run would make the first red the one everybody learns to ignore. On its first
run against the live catalog it reported two pinned dates as unbacked — the
tags of a throwaway probe feed from an earlier session whose landing objects
were deliberately removed, which is exactly the condition, correctly placed in
the weaker category because nothing left can prove which it was.

**Together they are still an approximation, and this is the honest limit.** A
published tag names ONE COB date, because `record_publication` cuts it
from the date of the ingest that triggered the build, and a published run reads
more than that date. So the check can miss a delivery from another date the run
depended on; it cannot raise a false alarm, because everything it does check
genuinely was received for that date. The exact input set needs the run record
enumerating its deliveries — REQ-400 — and until that exists REQ-602 is closer,
not done.

## supersession-is-declared-not-assumed

REQ-202. `supersession:` in feeds.yml, resolved by
`context.resolve_supersession_config` and validated at load like `delivery:`
and `arrival:` before it. One key, `mode`, and one built value,
`full_snapshot`.

**The behaviour is old; the declaration is new, and that is the point.**
`dedupe_rank` has always implemented `full_snapshot` — newest `_file_version`
wins, last row in file order wins within it — so a delta feed run through it is
silently reduced to its newest file, losing every key that file omits, with
nothing raising anywhere. Declaring the mode does not make the other shapes
work; it makes the platform REFUSE a feed whose supersession it cannot
implement. The value IS the refusal.

    feeds.yml: feed 'x' `supersession.mode: delta_append` is described in the
    requirements (REQ-202) but NOT BUILT -- each delivery carries only what
    changed, so a COB date's population is the UNION of its deliveries
    rather than the newest one [...]

Two levels, deliberately. `SUPERSESSION_NOT_BUILT` refuses at config load,
where a feed is onboarded; `dedupe_rank(partition_keys, mode=...)` raises a dbt
compiler error, for a model written by hand with a mode nothing checked. The
scaffold emits the mode only when it is not the default, so no existing model
changed on the day this landed.

It is inheritable through `conventions:`, because supersession is a property of
the SOURCE SYSTEM far more often than of one feed — the same reasoning that put
`delimiter` and `expected_min_rows` there.

## as-of-is-a-var-not-a-second-model

REQ-300, REQ-301. "What did we believe on date X" is answered by the models
that already exist, filtered by `known_as_of()`, driven by a dbt var:

    dbt build --full-refresh --select path:models/prepared \
      --vars '{nessie_ref: <branch>, knowledge_time: "2026-08-10"}'

**A var rather than a per-call-site argument** because the filter has to reach
every model and a model that quietly omits it returns everything, whatever the
caller asked for. With no `knowledge_time` set the macro compiles to `1 = 1`,
so the nightly build is byte-identical to what it was before this existed.

**A WHERE clause rather than a macro every model already calls.** There is no
such macro: two of the four prepared models do not call `incremental_window` on
their incremental path at all, they use `scd2_incremental_scope` and their own
predicate. So `known_as_of()` is at each call site, and
`tests/test_supersession.py` greps every file in `models/prepared/` for it —
CLAUDE.md's rule that fixing a macro proves nothing about models that do not
call it, written as a test rather than trusted.

**The clock is `coalesce(_received_at, _ingest_ts)`, and the fallback is not
decoration.** `_received_at` is the delivery's arrival time and the right
answer; it is also NULL for every row ingested before provenance existed
(#provenance-is-added-not-backfilled), so a filter on it alone would exclude
the whole of history rather than include it. `_ingest_ts` has been on every raw
table since the beginning and is never null. The two differ by however long a
delivery waited to be ingested — a Friday arrival loaded on Monday — so the
fallback is the later and more conservative of the two: it can include a row in
an as-of query slightly earlier than the truth, never exclude one it should
have shown.

**It refuses to run incrementally.** On the incremental path dbt MERGEs into
the target, so an as-of build would restate the published table backwards —
silently, with a green run. `known_as_of()` raises a compiler error when
`knowledge_time` is set and `is_incremental()` is true; full-refresh on a
throwaway branch is the only way to materialise one.

Reporting models carry no arrival clock of their own — they read `ref()`s — so
an as-of reporting build is a build of the whole chain on one branch with the
var set. That is also why the var reaches every model rather than being passed
model by model.

## delivery-ref-is-the-fallback-with-the-prefix-stripped

The half of phase 3's stated limit that had to be built. `_delivery_id` was
added and never backfilled, so a run record, an as-of query and the evidence
check could see nothing about the deliveries behind older rows.
`delivery_ref()` in `engine.sql` is the fallback:

    coalesce(_delivery_id, element_at(split(_source_file, '/'), -1))

**The basename, not the key, and that is the whole subtlety.**
`_delivery_id` holds a bare filename; `_source_file` holds a full object key.
Coalescing them without stripping the prefix would put two namespaces in one
column — every join and group-by over it silently wrong for exactly the rows
that predate provenance, and looking perfectly ordinary in both. It is exact
for `kind: file`, which is every delivery in this catalog, because the part IS
the landing object. The one shape it is not exact for is an archive ingested
before the provenance columns existed, where the part is an extracted member
and its basename is a member name; no feed here is `kind: archive`, so no such
row exists, and the limit is written down rather than left to be found.

`source_provenance()` projects it as `delivery_id`, so a rebuilt prepared table
can name the delivery behind every row it holds — which is what makes a run's
input set enumerable across the whole history rather than from phase 3 onwards.

**A macro change reaches only the models rebuilt after it**, and this one is
the prepared layer's version of the migration `migrate_raw.py` performs for
raw. The guard is a `not_null` test on `delivery_id` in `_prepared.yml`: after
the coalesce there is no legitimate NULL, so a NULL means the table has not
been rebuilt and its rows cannot say where they came from. It fails the build,
which is correct — an unmigrated prepared table must not publish a run whose
inputs cannot be enumerated. `ui/scaffold.py` writes the same test, so a new
feed cannot be the one model without it.

## an-ingest-is-not-a-publication

`feed_ingest` cut `published/<cob_date>/<run_id>` at the end of every
per-feed ingest DAG. It now cuts `snapshot/<feed>/<cob_date>/<run_id>`,
and a REPORT publication — `published/<report>/<cob_date>/<run_id>` — is
cut by the reporting build, which is the only thing that knows what it
published.

The old name was not a cosmetic problem. Three consequences, all live:

  * **Every check that read `published/` was reading ingests.**
    `monitoring/reproducibility.py` exercised an ingest pin,
    `monitoring/evidence.py` asked what a COB date received rather than
    what a run read, and both correctly reported on a thing nobody publishes.
  * **`references.published_tags.per_report` could never match anything**, so
    the per-report retention window was a resolver with no input.
  * **An ingest was retained for the reproducibility window** — ten years of
    pinned raw data files per feed per COB date, because nothing is
    reclaimable while a tag references it.

An ingest pin is still worth cutting: raw is where retention deletes COB
dates, so the state an ingest left is exactly what somebody may need to read
back. It is simply a different object with a different lifetime, and
`references.snapshot_tags` says so — seven years locally against the published
ten, and explicitly NOT bound by the landing interlock, because nothing is
reproduced from a snapshot. Nothing claims a snapshot's evidence is still in
`landing/`; a publication does.

`TAG_RE` still matches the old two-segment shape. Tags cut under it are real
pins, and a sweep that fails to recognise something skips it forever rather
than judging it — the same rule `clean_working_branches` follows for `hold/`.

## a-run-is-the-first-thing-the-registry-cannot-rebuild

REQ-400, REQ-401, REQ-404. `registry.run`, `registry.run_input`,
`registry.report_version`, `registry.submission` and
`registry.submission_item`, in the same Postgres schema as the deliveries and
under a different rule.

Everything phase 2 put there is an OBSERVATION about an object that exists in
storage, so `reconcile()` can reconstruct it — that property is what makes the
registry an index rather than a second source of truth
(#the-registry-records-observations-not-verdicts). A run is not: it is an event
that happened once, at a time, from a particular commit of the code, and no
object anywhere records that it happened. Two things follow that are easy to
get wrong:

  * **`run_input` carries no foreign key to `delivery`.** It names
    `(feed, delivery_id)` and looks like it should. A foreign key would let a
    registry rebuild — drop, reconcile from object storage — CASCADE run
    history away: destroying the only copy of something to protect the
    integrity of a table that has a second copy in storage. Exactly the wrong
    way round. `evidence.py` treats a delivery it cannot find as a finding.
  * **A run has a mutable `status` and a delivery still may not.** That is not
    the phase 2 boundary being relaxed. A delivery's status would be a verdict
    about something already true and derivable elsewhere — was it ingested,
    was it superseded — which is how it drifts. A run's status is the record of
    how the run ended: nothing else knows it, and refusing to store it would
    simply lose it.

**The input set is derived, not declared.** `publish` reads the distinct
`delivery_id` out of the prepared models ON THE BRANCH, before the merge, while
the state is exactly what was audited — reading main afterwards would answer a
different question and depend on what else merged in between. A declared input
set would be a second statement of something the rows already carry, and the
two would disagree the first time a model changed which sources it reads.

**It is "the deliveries whose rows are in what was published", not "the
deliveries the run scanned",** and for an SCD2 model those differ a lot. A
model that keeps one row per VERSION drops every delivery that restated an
unchanged entity: measured here, `ref_counterparty` contributes 10 of its 40
ingested deliveries, against 40 of 40 for `fo_trade` and 36 of 36 for
`ref_rating`, whose ratings change on nearly every delivery. That is the right
set for the question REQ-602 asks — re-deriving the published tables needs
exactly the deliveries whose data is in them, and an SCD2 table re-derives
identically from the versions that survived — and the wrong set for "what did
this run read". The number looks like a bug the first time you see it, which
is why it is written down here.

It asks the PREPARED layer, not the reporting one, because only prepared knows
which FEED a delivery belongs to: a prepared model is one feed by the naming
rule (#table-naming-no-layer-prefix), where a reporting model joins several and
keeps no column saying which delivery came from where. A reporting run's input
set is its prepared tables' input set.

**The run is opened before the build, not after it.** A run that fails is the
one most worth having a record of, and a row written only on success describes
a platform that has never had a bad night. `keep_failed_branch` closes it as
`failed`.

## code-identity-is-a-digest-when-it-cannot-be-a-tag

REQ-404. `context.code_ref()` returns a value AND a kind:
`PLATFORM_CODE_REF` if the deployment supplies one — an image tag, a release
SHA — and otherwise a sixteen-character content digest of the mounted
`reporting_platform/`, `scripts/` and `airflow/` trees, labelled
`tree-digest`.

**A git SHA would be a lie here.** In this stack those directories are
BIND-MOUNTED from a working tree, so the code that ran is whatever was on disk
at the time — which a commit hash does not describe, and describes most wrongly
exactly when the tree is dirty and somebody most needs to know. `.git` is not
mounted into any container either, so there is nothing to read even if it were
the right answer. The kind travels with the value so a laptop digest can never
be read as a release.

`dbt_manifest_ref()` is the same shape over `dbt/models`, `dbt/macros` and
`dbt/tests`, and deliberately NOT dbt's own `target/manifest.json`: Cosmos runs
one dbt subprocess per model, each overwriting that file with its own
invocation id, so there is no single manifest for a run and the field would
record whichever task finished last. The project's source is what determines
what was built.

Both follow `Feed.schema_version`: derived, not declared, because a version
somebody has to remember to bump is wrong the first time somebody forgets.

## version-is-per-report-and-as-at-date

Open decision 5, settled. `registry.report_version` is keyed
`(report, as_at_date, version_no)`, and reports submitted together are grouped
on the SUBMISSION record instead.

A version number answers "this is the Nth answer we have given for this report
and this date", which is a question about one report. Numbering per RUN would
move a report's version when an unrelated report was rebuilt in the same run;
numbering per FAMILY would move it when a sibling was restated. Both make a
report's own version history depend on things that are not about it.

REQ-402 already separates the version from the submission, so the family has a
place to live that costs nothing: `registry.submission` carries `family`,
`destination` and who sent it, and `submission_item` names the exact versions
that went. The platform submits nothing and this does not pretend otherwise —
it is the record that a submission was made, written at the time rather than
reconstructed from email afterwards.

The number is allocated by Postgres inside the transaction that inserts it,
with the primary key as the backstop. A plain sequence will not do, because it
restarts per (report, as-at date) rather than running globally — and
`MAX(...)+1` read by two publishers is the read-then-write `sequence_no` was
made a BIGSERIAL to avoid. It is idempotent on the TAG: a retried publish task
finds the version it already minted rather than minting a second.

**A report is a dbt EXPOSURE** (`context.reports()`), derived from the project
exactly as the managed tables are. The alternative was a `reports:` block in a
new config file, which would be a second declaration of something the dbt
project already makes — and the two disagree the first time a model is renamed.
An exposure is also already the thing that answers "what breaks if I change
this model", and already names an owner.

## the-as-at-date-has-a-lifecycle

REQ-500..503. A `(report, as-at date)` pair moves `open -> locked ->
submitted`, and back to `reopened` only deliberately. `registry.as_at_transition`
is append-only and **`open` is the ABSENCE of a row**.

Open is not stored because storing it would require every pair to be seeded,
and that set is DERIVED — from the exposures and from whichever COB dates
have deliveries. A seeded table is a second list of reports, and it goes stale
the moment somebody adds an exposure. Absence costs nothing and cannot drift.
It also makes the table honestly append-only: there is no
`(report, as_at_date)` primary key to update in place, so the history of who
closed a date and why is the record rather than a column that was overwritten.

**Reopening a SUBMITTED date needs the report's exposure owner to approve it;
reopening a merely LOCKED date does not.** This is open decision 3, settled
alongside REQ-501's named owner. A lock is an internal control and undoing one
should cost a name and a reason. A submission has left the building: restating
it changes a figure somebody else is holding, so the approver is checked — and
it is checkable precisely because `owner.name` comes from the dbt exposure, not
from the caller. There is no identity provider here and this does not pretend
otherwise; `actor` and `reason` are `NOT NULL` because an unattributed lock is
one nobody can ask about later, and that record is the whole of the
accountability.

**The gate runs BEFORE the merge, not with the versioning.** `publish()` merges
the build branch into `main` first and cuts tags and allocates versions second.
A refusal placed with the versioning would fire after `main` had already
moved — honest and useless. It sits between reading the input set (which is
what makes the as-at date knowable at all) and the merge, so a refusal fails
the task with `main` untouched and `keep_failed_branch` retaining the branch
for inspection. `tests/test_lifecycle.py` asserts the ordering, because it is
invisible afterwards.

**The policy only bites on a CLOSED date whose inputs have MOVED.** Every word
is load-bearing. An open or reopened date publishes whatever changed — that is
the ordinary daily path and it must not acquire a lifecycle cost. A closed date
whose input set for that date is unchanged publishes too, because rebuilding a
locked date after a code change is not a restatement of the data, and refusing
it would make `lock` mean "this report may never be rebuilt". Verified on the
live stack: with 2026-08-20 locked for both reports, a reporting build
published v2 of each, `carried_forward: []`, and the diff reported 127
unchanged deliveries with both code refs moved.

**The comparison is restricted to deliveries for that as-at date**, and
`unregistered` means unknown to the registry — not "for another date". A run's
inputs are what it PUBLISHED, so an SCD2 reference feed contributes deliveries
for many COB dates: on this stack, 123 of a 127-delivery input set are
legitimately off-date. An earlier version of `inputs_changed` called all of
them `unregistered`, which would have sent somebody looking for a registry gap
that was not there. They are counted as `off_date` instead. See
`registry/inputs.py` for why the input set is shaped that way.

**There is no platform-wide restatement default.** Open decision 2, settled as
*fail*: `context.restatement_policy()` refuses a report whose exposure declares
no `meta.restatement`, and `tests/test_lifecycle.py` asserts every shipped
exposure declares one. The refusal is in `restatement_policy()` rather than in
`reports()` because `reports()` is a pure derivation that the nightly retention
chain now calls — putting the refusal there would let one undeclared exposure
take the retention sweep down.

REQ-503 falls out of there being no branch anywhere on what KIND of report this
is. A daily internal dashboard and a quarterly regulatory return traverse the
same functions; `country_exposure_dashboard` is `carry_forward` and
`counterparty_exposure_report` is `restate`, and that is a per-report policy
rather than a per-class one. The claim is checked structurally — a grep for
`type` and `maturity` in the lifecycle and publish paths — the same shape as
`test_supersession`'s grep for `known_as_of()`, because a comment claiming it
would not survive the first convenient special case.

## retention-classes-name-the-obligation

REQ-600/601. A feed names a `retention_class` in `feeds.yml`; the windows live
in `retention.yml`, per environment. Naming a class the windows file does not
declare is refused at LOAD, like an undefined `convention` and an unreadable
`tag_retention_years`.

Two files because they are two decisions with two owners. The class is a
property of the obligation a feed carries and is set by whoever onboards it;
the window is retention policy and is set per environment by whoever owns that.
Putting the years in `feeds.yml` would make a policy change a sweep through
every feed block, and would have no way to be shorter in `dev`.

**Classes govern the evidence prefixes only — `landing/` and `quarantine/`.**
Table keep-sets stay per LAYER. A table window says how much history is
queryable, which is a decision about a layer; and a per-feed raw window would
fight `find_pending`, which already derives one keep-set per feed from that
feed's own landing prefix. `PREFIX_CLASSES` is closed and `class_keep_years`
refuses anything outside it, so this cannot spread by accident.

What it costs is that a feed's window is now only as protected as the lineage
walk is correct, which is why `feeds_behind_report` raises on a ref it cannot
resolve rather than returning a short list. On this stack both reports resolve
to `fo_trade`, `ref_counterparty` and `ref_rating`; `ref_collateral` is behind
neither, so it is legitimately `operational` at 7 years — above the raw layer's
80 month-ends (≈6.7y) and below the 10-year pin window. If a report ever
`ref()`s it, the nightly sweep refuses until the class is moved back. **That
refusal is the mechanism**: the class cannot silently outlive its correctness.

**A `per_report` entry naming no live exposure binds every feed.** A report
removed from the project keeps the tags it already cut, and `expire_tags` still
resolves their window by the name in the tag — so that entry is still in force
for pins that still exist, while the lineage that would say which feeds were
behind it is gone. The conservative answer is the only available one. Iterating
live exposures alone would quietly stop honouring a window still being applied.

**`snapshot_tags` stays outside the interlock**, and must not creep back in.
Nothing is reproduced from a snapshot tag; it buys the ability to read a raw
COB date back after retention removed it, which is a storage decision
rather than an evidence one. Binding it here would impose the published window
on every feed again and undo the whole thing.

**The second interlock moved too.** `landing.keep_years()`'s warning that
landing is shorter than the raw layer's own window was comparing the DEFAULT
class against the raw window — so it would have gone silent about exactly the
feed a short class was applied to. It is per feed now, for the reason CLAUDE.md
gives: a check whose window does not contain the thing it describes will either
never fire or never stop.

**`quarantine:` ships with no `classes:` block, and the absence is the point.**
A class that shortens landing and says nothing about quarantine gets
quarantine's own window — the over-retaining direction. The two answer to
different things: a rejected delivery explains a missing COB date whether
or not anything published depends on the feed.

## lateness-is-a-wall-clock-time-not-a-duration

REQ-201. `Feed.expected_by` is `"HH:MM"` — a wall clock rather than a duration,
because what an upstream actually commits
to is "by 07:00", and a duration needs an origin event that a delivery arriving
by `PutObject` does not have. **The deadline is `expected_by` on the day AFTER
the COB date**, fixed rather than configurable: a delivery describes a
COB date, so that date has to have ended before the extract can be taken.
Fixing it at +1 is a choice in the FORGIVING direction — a reference snapshot
that legitimately arrives the same day is judged against a later deadline than
it needed — so the check can under-report lateness and cannot invent it.

**It must be quoted, and the refusal of a non-string is not pedantry.** YAML 1.1
reads an unquoted `7:00` as sexagesimal: `yaml.safe_load("a: 7:00")` is
`{"a": 420}`. A leading zero happens to block pyyaml's resolver, so `07:00`
survives unquoted and `9:00` does not — which makes it the worst kind of trap,
working for every padded hour until the first unpadded one. `24:00` is refused
rather than folded to midnight: a delivery due at the end of the day is due at
`23:59`, and accepting an hour that does not exist invites the reader to
believe some rollover rule is implemented. There is none.

**This is not the completeness check and must not become it.**
`completeness.py` asks which COB dates a feed is MISSING; this asks, of
the deliveries that did arrive, which arrived late. A date with no delivery at
all is a gap, not an infinitely late delivery, and reporting it in both places
would double-report every outage. A feed with no `expected_by` has made no
promise and is skipped — inventing a default of `00:00` to have something to
measure is how a monitor starts reporting policy it made up.

**A backfill is reported as ONE event.** If every late date for a feed arrived
on the same calendar day, that is one bulk load — a seed, a migration, a
re-delivery of history after an outage — and the log says so instead of listing
ten missed deadlines. It is a derivation with nothing to tune: more than one
COB date, exactly one arrival day. The finding is described differently,
never suppressed: `total_late` and `--fail-on-late` are unaffected, because a
backfill of dates that were due weeks ago genuinely is late. The seeded stack
is exactly this case — `generate_feeds.py` writes ~17 days of history in one
write, so all four feeds report every date late with one timestamp — and it is
what the behaviour was written against.

No Spark: the arrival time is `registry.delivery.received_at`, the landing
object's `LastModified`, so the check is psycopg2 and runs in the Airflow task
process directly.

## a-version-diff-is-inputs-and-code-not-data

§11, and nearly free once phase 5 made a run enumerate the deliveries behind
what it published. A v1→v2 comparison is the set difference of two `run_input`
sets, plus the `code_ref` and `change_ref` on each run.

**Nothing here diffs the DATA.** The published tables are pinned by their tags
and can be compared directly with `nessie_ref`; what was missing was the
ability to say which INPUTS differ, which no query over the tables can answer.

**The delivery join is a LEFT JOIN**, because `run_input` deliberately carries
no foreign key to `delivery`. A delivery the registry cannot currently describe
is reported with nulls rather than dropped — an inner join would remove it from
both sides equally and make a real difference look like agreement, reporting
"nothing changed" for exactly the case somebody is investigating.

**Both halves are reported because either alone lies.** The real case on this
stack today is two versions of one date with identical 126-delivery input sets
and different code refs (`1da75dbe51fed0eb` → `cf50684d0cf3d257`, change_ref
`RPT-1421` → `RPT-1490`). A diff that only differenced deliveries would say "no
change" about a rebuild that moved every figure.


## the-ready-window-bounds-the-parts-not-the-manifests

`ready.keep_days` bounds a delivery's DERIVED PARTS and its orphaned
manifests. It does not bound manifests generally, and a sweep that deletes
them is undone by the next `normalize.reconcile` — 157 deleted, 157 re-made,
both logging success. A standing no-op is the shape of thing that survives for
years, because nothing is ever red.

**The manifest is the delivery's description of record**, not a queue entry.
`deliveries.reconcile` walks MANIFESTS — the registry follows the platform
having ACCEPTED a delivery as readable, not a landing object whose COB date
nothing has established yet — so sweeping them by age would make the registry
rebuildable for a week rather than from object storage. It is about 1KB
against a landing object kept for years, and it lives as long as that object.

That also rules out the tempting fix in the other direction: teaching
`normalize.reconcile` to skip landing objects that are already ingested. It
would stop the churn and quietly break the same property.

What the window actually reclaims:

* **Derived parts** — a zip member `_normalize_archive` extracted — of a
  manifest past `keep_days` whose parts are all ingested. That is the real
  duplication: a second copy of data `landing/` already holds. The manifest
  stays, so `reconcile` skips the landing object and nothing re-extracts;
  `normalize --force` rebuilds them if anything needs them. A queue of plain
  CSVs reclaims nothing, because their parts point back into `landing/` and
  nothing was ever copied.
* **Orphaned manifests**, whose `source_object` is gone from `landing/`, at
  any age. Nothing recreates one, because `reconcile` walks landing. These are
  what the landing sweep leaves behind, which is why it runs immediately
  before this one in `retention.run()`.

`already_ingested` opens a Spark session per feed and is taken LAZILY — only
when some manifest has derived parts old enough to consider, or an orphan to
classify. On a queue of plain CSVs that is never.

The report counts what was REMOVED — `parts_deleted`, and `manifests_deleted`
for orphans only. A counter for "past the window" would describe work the next
`reconcile` undoes.

## a-dry-run-may-write-to-the-index-not-to-object-storage

Same run, same reading. `registry_reconcile` took no notice of the `dry_run`
param, and `deliveries.reconcile()` calls `normalize.reconcile()` first so that
a delivery pushed straight into the bucket has a manifest to follow. A
housekeeping run explicitly triggered with `{"dry_run": true}` therefore wrote
**116 manifests into `ready/`** and registered zero deliveries — and
invalidated its own forecast, because the dry run predicted a sweep of 42
manifests and the real run then removed 157. The dry run had created the
difference.

**Gating the whole task on `dry_run` would have been the wrong fix.** The
task's docstring argues at length that it should always run: it is the registry
rebuild path, and `arrival.py`'s rule — *events are an optimisation, the poll
is the correctness guarantee* — is precisely what a skipped poll throws away. A
dry run of the nightly chain that leaves the registry stale for the night is a
worse outcome than the defect.

The distinction that resolves it is not "does this write" but **what the write
changes**. A registry row is an index entry: the write is an upsert, it is
idempotent, and nothing that deletes reads it, so making one on a dry run
changes nothing about what the real run would do. A manifest object is an input
to the `ready/` sweep two tasks back, so making one *does*. So `dry_run`
narrows the task to `normalize_first=False`: every delivery that has a manifest
is still reconciled and still gets its row, and the count of landing objects
that have none is reported as `would_normalize` instead of being manufactured.
The prediction is computed with the SAME predicate `normalize.reconcile` skips
on, so it is what the real run would do rather than a second opinion about it.

The rule, stated once so the next dry-run flag has somewhere to look: **a dry
run may write to the index; it may not write anything a later step reads to
decide what to delete.**


## openlineage-is-an-export-not-a-record

Airflow emits an OpenLineage event per task run; Marquez consumes them and draws
the graph. Both are behind one switch and are OFF by default — `OPENLINEAGE_DISABLED`
in `.env` and the `lineage` compose profile.

**Marquez is a CONSUMER and must never become an authority.** This platform
already derives lineage from the dbt project, and that derivation is
load-bearing: `feeds_behind_report()` walks an exposure's `ref()` closure and
**retention refuses to sweep** on its answer. A second graph that drifts from
the project is the exact failure this repo keeps rejecting elsewhere — "a second
list of reports", "a second registry of feeds". Marquez observes; the dbt
project decides.

**It is also not the record of what a run published.** `registry.run_input` is,
and the two answer different questions on purpose. A run's inputs are the
deliveries whose rows are PRESENT in what was published, so an SCD2 dimension
contributes 10 of its 40 ingested deliveries; OpenLineage reports what the job
READ, which is all 40. Both numbers are right for their own question and neither
should be reconciled to the other. Anyone who "fixes" one to match the other has
broken REQ-602.

### The provider was already installed, and installing it would have broken dbt

`apache-airflow-providers-openlineage` 2.0.0 (with `openlineage-python` 1.27.0)
is already in the image as a transitive dependency. Nothing needed installing —
which is fortunate, because doing it the obvious way is the cosmos trap
exactly. Under Airflow's own constraint file, installing the provider
explicitly pins `typing_extensions` back.

The image currently has 4.16.0. The constraint pins 4.12.2, dbt's `mashumaro`
needs `evaluate_forward_ref` from 4.13+, and every dbt invocation would then die
at import — in dbt, not in openlineage, and not until something ran dbt. Caught
by the dry run this repo already mandates before moving `COSMOS_VERSION`; the
rule generalises to any provider added later.

### A SKIPPED task never closes, and Airflow 2.10 cannot fix it

It is not a provider defect and no version bump fixes it. Airflow 2.10's
listener spec offers exactly three task hooks — `on_task_instance_running`,
`_success` and `_failed`.

There is no `on_task_instance_skipped`. A task that emits START and then skips
has no hook through which a terminal event could ever be sent. **This matters
here more than it would elsewhere**, because skipping is the ingest DAGs' normal
idle behaviour — `resolve_arrival` raises `AirflowSkipException` whenever there
is nothing pending, which is most runs — so each feed accumulates one
permanently-`RUNNING` run in Marquez per idle poll.

Read the graph accordingly: **`RUNNING` in Marquez means "started and did not
succeed or fail", which on this platform usually means skipped.** It is not
evidence that anything is stuck. The authority on whether a run is still going
is Airflow, and the authority on whether it published anything is
`registry.run`.

### Why OpenSearch is not here

`marquez.dev.yml` defaults `search.enabled` to true against an OpenSearch on
:9200. That is the component that makes a full catalogue deployment too heavy
for a developer box — OpenMetadata's own quickstart asks for 6 GiB and 4 vCPUs,
against roughly 6 GB free on the machine this was built on. `SEARCH_ENABLED=false`
drops it entirely. Measured cost of what remains: **220 MB for the API and 25 MB
for the web front end.** Search is a nice-to-have; lineage is the point.

## marquez-on-ubi

Both Marquez images are BUILT HERE, from Marquez's own source, on Red Hat UBI
bases — `Dockerfile.marquez-api` and `Dockerfile.marquez-web`. Upstream ships
`marquezproject/marquez` on eclipse-temurin/Ubuntu 24.04 and
`marquezproject/marquez-web` on node:18-alpine, and neither base is permitted
in this estate. Everything else about the deployment is unchanged: same
version, same `marquez.dev.yml`, same entrypoints, same ports, same
`SEARCH_ENABLED=false`.

**From source, not copied out of the upstream image.** Lifting the artifacts
with a `COPY --from=marquezproject/marquez:0.51.1` stage would have been a
two-line change and produces byte-identical output, but it still pulls the
image the policy exists to keep out — it moves the base out of the running
container without removing it from the build. Maven Central is not an
alternative either: `io.github.marquezproject:marquez-api` stops at 0.50.0 and
publishes a THIN jar, which `server marquez.dev.yml` cannot start.

So the API image runs Marquez's own `:api:shadowJar` in a
`ubi8/openjdk-17` builder and ships the result on `ubi8/openjdk-17-runtime`.
The whole upstream image was three files — the shaded jar, `marquez.dev.yml`
and a twelve-line `entrypoint.sh` — and all three come out of the same source
tree, so the runtime image is those three files and nothing else. The
entrypoint is upstream's, unmodified: it globs `marquez-*.jar`, which is why
the jar keeps its versioned name.

### The web image is a bundle and two packages, not a build tree

`marquezproject/marquez-web` is 1.36 GB, of which 702 MB is `node_modules` and
40 MB is the bundle it built. At runtime the application is `node
setupProxy.js`: an Express server for `dist/`, proxying `/api/v1` and
`/api/v2beta` to the API. So `npm run build` happens in a `ubi8/nodejs-18`
builder and the runtime stage on `ubi8/nodejs-18-minimal` gets the bundle,
`setupProxy.js`, the entrypoint and a separately-resolved two-package
`node_modules`.

**Both of those packages are pinned, and one of them is load-bearing.**
`http-proxy-middleware` must stay 2.x: 3.x removed the
`createProxyMiddleware(path, options)` form `setupProxy.js` calls, and the
failure is a 404 at request time rather than anything at startup. `express` is
pinned at the 4.19.2 upstream resolved because `setupProxy.js` requires it and
`web/package.json` never declared it — it arrives transitively through
`webpack-dev-server`, which is a build-time dependency that does not ship here.

### Two things the UBI bases do not have

- **No gzip in `ubi8/openjdk-17`.** `tar` is there, so `curl | tar xz` fails
  with `gzip: Cannot exec: No such file or directory` from tar's grandchild and
  a `curl: (23) Failed writing body` — neither of which names a missing
  package. One `microdnf install -y gzip`. The node builder has it already.
- **No `npm ci`.** `web/package.json` carries a `file:./libs/graph`
  dependency; upstream's own Dockerfile uses `npm install` and so does this
  one.

## lineage-is-derived-from-the-dbt-project

Airflow's OpenLineage export reports JOBS. The datasets, and the edges between
them, are derived here — from the dbt project — by a custom extractor, because
neither built-in path can supply them and both reasons are permanent.

### Neither half arrives on its own, and both reasons are structural

**The ingest DAGs' outlet is not convertible.** Airflow turns a task's outlets
into OpenLineage datasets only for a URI scheme with a registered converter,
and this image registers three — `file`, `gs`, `s3`.

`Feed.asset_uri` is `iceberg://lakehouse/raw/fo_trade`, so
`translate_airflow_asset` returns `None` and the ingest task reports nothing it
wrote. Registering an `iceberg` converter means shipping a provider package,
which is a lot of machinery to describe four tables.

**Cosmos's own dbt extractor cannot work against this profile.** It parses the
artifacts with `openlineage-integration-common`, which raises for anything but
thrift/http/odbc:

```
NotImplementedError: Connection method `session` is not supported for spark adapter.
```

`method: session` is deliberate here — it is how dbt reaches the Nessie catalog
on the Spark cluster — so this is a permanent disagreement, not a version to
bump. Two details made it invisible for a while: Cosmos catches that exception
and logs it at **debug**, and there is a *second*, earlier failure hiding
behind it. `DBT_TARGET_PATH` is absolute (`/opt/platform/run/dbt/target`,
because the `./dbt` bind mount is not writable by uid 50000), while the
processor reads `<the temp project Cosmos cloned>/target/manifest.json`. Fixing
only the path gets you a `FileNotFoundError` replaced by the
`NotImplementedError` and still no datasets, which is a good way to lose an
afternoon.

### So the datasets are derived, and from the source that already decides

A custom extractor, registered through `AIRFLOW__OPENLINEAGE__EXTRACTORS`
(**not** `__CUSTOM_EXTRACTORS`; the option is `[openlineage] extractors` and
the more obvious spelling is read by nothing and reported by nothing).
`ExtractorManager.get_extractor_class` checks `task_type` against the custom
extractors *before* it looks for `get_openlineage_facets_on_*` on the operator,
so it wins over Cosmos's — which is the only reason this approach works at all.

**It attaches datasets to the runs Airflow is already reporting.** The
alternative — a task that posts its own events — would put a second job node
next to every model and draw each transformation twice. There is exactly one
node per task, and the operational record and the data flow are the same
object.

**The edges come from `context.model_refs()`, which is what
`feeds_behind_report()` walks to size a retention window.** That sharing is the
entire point and not an optimisation. This repo already refuses second lists of
reports and second registries of feeds; a second lineage walker is worse than
either, because the two answers are read by different audiences. People reason
from the picture, while the rule that quietly decides whether a feed's evidence
still exists is the other one. `tests/test_lineage.py` asserts that walking the
exported edges backwards from a report reaches exactly the feeds
`feeds_behind_report()` names, and that assertion was confirmed to fail when
the walk is broken.

Cosmos is asked only **which model a task builds** (`dbt_node_config`'s
`unique_id`, which it resolved by running `dbt ls`). Identity, not
dependencies: slicing `dbt.<model>_run` apart would be a second guess at a
Cosmos naming convention, and taking the dependencies from there instead would
reintroduce the second walker.

### What is deliberately not in the graph

- **`dbt_test` produces no dataset.** A test reads models and writes nothing;
  drawing it as a transformation would put a node in the graph that never
  produced a table. The audit half of write-audit-publish is visible in Airflow
  and in `registry.run`, which is where a verdict belongs.
- **A landing node is the PREFIX, not the object.** `s3://lakehouse` +
  `landing/fo_trade`. The graph describes the shape of the flow; a node per
  delivery would redraw it 157 times on a cold load.
- **Datasets carry their layer** — `raw.fo_trade` and `prepared.fo_trade` are
  different nodes. Every layer holds a table per feed with the same name, so
  naming a dataset by the table alone would collapse the three layers into one
  node and draw a table that feeds itself.

### The extractor claims every `@task` in the estate

There is no narrower hook: the ingest task is a plain decorated function, so
the only class name to register is `_PythonDecoratedOperator`, which every
`@task` shares. Two obligations follow, and `extractor.py` honours both. It
must be **cheap** — it does no I/O until it has recognised the task — and it
must be **total**: an extractor that raises is caught and logged by the
manager rather than failing the task, but it takes the datasets of whatever it
was extracting with it, so every path returns an empty lineage instead. An
unrecognised task returns `OperatorLineage()` and the manager then falls back
to that task's own inlets and outlets exactly as before, so nothing that used
to emit stopped emitting.

### The columns come from the TABLE, not from the dbt project

The edges are derived; the schema deliberately is not. The dbt schema YAML
documents the columns somebody wrote a test or a description for --
`_sources.yml` names two columns of `raw.fo_trade`, and the table has twenty.
Emitting that would not be an incomplete answer but a WRONG one: nothing in
Marquez marks a field list as partial, so a reader would conclude the other
eighteen do not exist. Empty is honest, partial is not.

So `schemas.py` runs `DESCRIBE` through DuckDB -- the platform's established
no-Spark read path (`scripts/duckdb_console.py`), measured at 0.59s for all
eleven tables plus 0.31s to attach, and cached per process. Spark here would
put a JVM in the task process, which
docs/DECISIONS.md#spark-in-a-subprocess forbids. Note
`information_schema.columns` is NOT usable: the Iceberg attach answers it with
one placeholder column per table, so it must be `DESCRIBE`.

**It reports the PUBLISHED schema**, because DuckDB can only address the
catalog's default branch. That is the right one: Marquez should show the shape
a reader can actually query on `main`, not what the branch this run is
building might merge in a minute. A table that has never been published gets
no facet and appears after the first run that merges it -- which is also when
it becomes true. The facet is **omitted** rather than sent empty in that case,
since an empty `fields` list is a claim that the table has no columns.

**A landing prefix is not a table**, so its columns are `Feed.file_header` --
the names the FILE carries, before ingest renames them. The graph therefore
shows the rename this platform performs, and the ingest's own contribution
becomes visible as columns: `landing/fo_trade` has the upstream's 9,
`raw.fo_trade` has 20, and the 11 added are `_extra_columns` (drift),
`_cob_date`, `_ingest_ts`, `_source_file`, `_file_version`, `_row_number`,
`_batch_id` and the four provenance columns.

### Column lineage is parsed, from the COMPILED SQL

The models are Jinja -- `clean_string()`, `safe_cast()`, `dedupe_rank()` -- so
the template says nothing about which columns a macro reads. The parseable
artefact is `target/compiled/**/<model>.sql`, which dbt writes on every build
and which persists because `DBT_TARGET_PATH` is absolute.

**`openlineage-sql` is already in the image and cannot do this job.** Measured
on this project: of 59 fields it produced, 26 resolved to a real table and the
rest named the CTE the column arrived through (`trades`, `deduped`). The
prepared layer produced **zero** -- and prepared is the layer that does the
renaming and the casting, so it is the entire point. The cause is not a bug:
those models open with `select *`, and no parser can expand a star without
knowing the table's columns.

**sqlglot can, because it takes a schema, and the platform already reads one.**
`schemas.py` describes every managed table off the catalog; handing that to
sqlglot resolves the CTEs and expands the stars. 116 of 136 columns then trace
to a source column. The 20 that do not are `dbt_invocation_id`, `nessie_ref`,
`dbt_updated_at` and a `count(*)` -- columns genuinely computed from no input
column. So this is complete rather than partial, which is the distinction
`schemas.py` refuses to blur: every column that HAS a source gets one.

**The transformation is reported, not just the dependency.** A rename carries
no description -- the input field's own name is the whole story -- while a
computation carries the SQL that performs it. That is the DEEPEST non-trivial
expression on the path from output column to source: the outermost is always a
passthrough from the final CTE (`typed.notional AS notional`), and the useful
one is `TRY_CAST(NULLIF(NULLIF(NULLIF(TRIM(deduped.notional), ''), 'NULL'),
'N/A') AS DECIMAL(28,4))`.

**Landing -> raw is not parsed**, because ingest performs a declared rename
rather than a query: that mapping IS `Feed.source_column()`. The platform's own
added columns come from no file column -- they are `ingest_added`, and they
are PRESENT rather than absent; see the next section for why that changed.

## a-column-with-no-source-says-so

Column lineage that reports only the columns it can TRACE makes a literal, a
`count(*)` and a parser failure look identical — all three are simply absent
from the facet, and absence reads as "nothing to say" rather than "this one
could not be resolved". So every column of every managed table carries a
**classification**:

| class | what it means |
|---|---|
| `sourced` | computed from >=1 upstream table column, which is named |
| `row_aggregate` | an aggregate over ROWS, not columns: `count(*)` |
| `build_metadata` | a property of the build: `current_timestamp()` |
| `literal` | a constant the build injected: `dbt_invocation_id`, `nessie_ref` |
| `ingest_added` | ingest's own column, sourceless by construction |
| `unresolved` | **the defect class** — the export could not read it |

**The class is read off the same node the transformation description is.**
`_deepest()` walks to the deepest non-passthrough expression — the cast, the
trim, the `count(*)` — because that is where the work happens and the
outermost layer is always a passthrough from the final CTE. Its node TYPE is
what tells `COUNT(*)` over a `Star` from `CurrentTimestamp` from a cast
`Literal`. Sharing that walk is deliberate: the classification and the SQL
shown for it can never describe two different nodes.

**Which raw columns are the platform's is DERIVED, not listed.** A raw table is
the feed's declared columns plus ingest's own, so a column `feeds.yml` does not
declare is ingest's — and re-listing the `CREATE TABLE` from `ingest_feed.py`
here would be the second list this document keeps refusing. The `_` prefix is
only a TIEBREAK, consulted for a column the feed did not declare: one that is
neither declared nor underscore-prefixed is drift between the table and
`feeds.yml`, and it is reported as `unresolved` rather than filed under a
reassuring name.

### `unresolved` is a defect, and it does not fail a build

It is reachable and detectable: sqlglot raises `Cannot find column 'x' in
query` whenever the TABLE has a column the current SQL does not produce — a
column dropped from a model, or added by a later version. It is empty against
the shipped project, which is the point; a class that is only ever asserted to
be empty is one nobody has shown can be entered, so `tests/test_lineage.py`
drives it deliberately.

It is **reported, not enforced**, for three reasons that all point the same
way. Nothing in `reporting_platform/lineage/` may raise — it runs inside an
OpenLineage extractor, where an exception costs the DATASETS of whatever was
being extracted. An export is not an authority (see
*openlineage-is-an-export-not-a-record*), so a DESCRIPTION of the pipeline must
never acquire the power to stop the pipeline. And the condition is legitimately
transient: the compiled SQL on disk is from the LAST build while the table
schema is read from `main`, so mid-change the two disagree by construction and
a build-time refusal would fire on a correct deployment.

The seam is therefore CI, not runtime. `python -m reporting_platform.lineage
--columns` classifies every column and **exits 1** on any unresolved one, which
is gate-able without inverting the dependency.

### Marquez carries an empty `inputFields`, and the graph endpoint drops it

The shape is therefore: **every** column goes in `columnLineage`, sourceless ones with
an empty `inputFields` — never with a fabricated input to make the entry look
well-formed, because a fabricated edge is worse than an absent one. And because
that facet's per-field vocabulary cannot SAY which kind of sourceless a column
is, the classification rides in a second, producer-defined dataset facet,
`columnClassification`, which Marquez also stores and returns verbatim
(verified the same way). It hoists `unresolved` to the top level, because a
defect nobody has to go looking for is a defect somebody will find.

### A facet VALUE passes through Airflow's SecretsMasker

The class is `ingest_added` now. The general rule, pinned by
`tests/test_lineage.py`, is that **no value this package emits may contain a
word that is also a credential in this estate** -- `platform` being both the
user and the password in `REPORTING_DSN` and `REGISTRY_DSN`, it is the one that
bites.

## a-change-is-a-deployment-event-not-a-run-event

One ticket authorises a version, and every run until the next deployment
inherits it. So a change reference has two scopes, and they are separate
columns because they are separate facts:

| | Scope | Source |
|---|---|---|
| `deployment_change_ref` | every run of a deployed version | the environment, set by the chart |
| `change_ref` | one run | the trigger, for a restatement or an out-of-cycle rerun |

"Published under the standing deployed version" and "published under a
specific authorisation" cannot both be expressed by one nullable column.

### The declared version and the computed digest are both required

`dbt_manifest_ref` is a content digest of the project on disk. It answers *was
this run's SQL the same SQL as that run's*, and it cannot answer *which commit
is that SQL supposed to be* — so `dbt_project_ref` carries the commit the
pipeline built from, and it is what resolves onward to the change record.

Keeping both is not redundancy, because they disagree in a way that matters.
The declared version says what SHOULD be running; the digest says what IS.
They diverge whenever the project is writable at run time — and this platform
writes to it: the feed console scaffolds a prepared model and edits
`_sources.yml` straight into `DBT_PROJECT_DIR`.

`check_project_drift()` reconciles them, and **the comparison is digest to
digest** because a commit id and a content digest are different value spaces.
The pipeline computes `DBT_PROJECT_DIGEST` by running
`python -m reporting_platform.registry provenance` over the project it is
deploying, so both sides are one implementation rather than two that agree
until somebody changes one.

**It refuses only in `uat` and `prod`.** The console is a dev tool and is not
deployed above dev, so divergence there is the normal working state and
refusing on it would make the tool that edits the project unusable with the
platform that reads it. Getting this the wrong way round breaks dev or makes
prod's attribution a guess, which is why `CONTROLLED_ENVIRONMENTS` is pinned by
a test rather than left as a literal somebody can extend in passing.

The check runs BEFORE the run row is opened and is deliberately **outside** the
try/except that makes the registry write best-effort: a drifted project is a
refusal, not a lost audit row.

### Adding a column to a table that already exists

`SCHEMA` is `CREATE TABLE IF NOT EXISTS`, which is a no-op against an existing
table and does **not** reconcile its columns. Every column added after a
database was first created therefore has to appear in `MIGRATIONS` as well, or
it silently never appears — and the failure lands far from the cause:
`ensure_schema()` succeeds, and the INSERT naming the column fails later, in a
task, at publish time. `ADD COLUMN IF NOT EXISTS` keeps it idempotent on every
connection, additive only; a drop or a retype is a real migration and does not
belong in a startup path. `tests/test_provenance.py` asserts that every
migrated column is also declared in `SCHEMA`, so a fresh database and a
migrated one converge.

## the-arrivals-view-is-a-join-not-a-record

So the view is a **join over what already records each leg**, computed per
request and written nowhere:

| leg | already recorded by |
|---|---|
| at the door, and what claims it | `inbox/` itself, through the gate's own `route()` |
| refused, and which class of refusal | `registry.rejection` |
| accepted, under what name, out of what | `registry.delivery` |
| what its control file declared | `declared_row_count`, `declared_md5` on that row |
| what ran next | Airflow, by the object key the run was told to ingest |

Nothing new is derived; the console reads five things and puts them on one
line. That is also why it degrades in pieces rather than as a whole: Airflow
being down costs the last column and nothing else.

### The two checks are different kinds of answer

`delivery.control` declares two facts and the console can honestly answer only
one of them.

**The checksum is answerable.** `declared_md5` is what the control file said;
`md5` on the same row is what the registry measured of the delivery's SOURCE
object. `ingest_feed._delivery_md5` hashes the manifest's `checksum_objects`,
which every normalizer sets to that same source object — the CSV for a plain
delivery, the container for an archive, because that is what the sender
hashed. So comparing them is the same comparison ingest makes rather than an
approximation of it, and it holds for a multi-part delivery too: an archive
gated on a control file is exactly that case, and calling it `not_comparable`
on a part count would hide a mismatch ingest is about to fail on.

THAT IS AN INVARIANT ACROSS TWO MODULES and it is pinned in
`tests/test_arrivals.py`. A normalizer that ever points `checksum_objects`
somewhere other than the source object makes this comparison wrong rather than
merely unavailable, and has to add whatever distinguishes it to the registry
row before the console can answer again.

**The row count is not.** Nothing counts rows without reading the file, and
reading the file is a Spark job. So the declared number is shown with the
verdict `at_ingest`, and the ingest run sits in the next column: a mismatch
fails that task, so the run's state *is* the verdict.

And neither is a claim that the check has been *made*. A delivery can sit in
landing for days with a checksum that agrees perfectly and never be ingested.
`ok` says the two recorded values match; the run beside it says whether
anything acted on that.

### `no run recorded` is not `not ingested`

The one label on these pages that would be believed and wrong. Airflow trims
its own run history and the registry does not, so the older half of any
arrivals list will outlive the runs that ingested it — and whether the rows
reached raw is derived from `_source_file`, which costs a Spark job to ask on a
page that is redrawn on every refresh (the same reasoning that keeps
`/api/feeds/<name>/state` Spark-free). The column reports the state of a RUN
and says so. `no ingest DAG` is kept separate for the same reason: a feed
removed from `feeds.yml` keeps every delivery it ever made, which is the point
of an index over object storage, and reading that as "nothing ever ingested
this" sends somebody looking for a lost run.

### A run is matched by the key it was told to ingest

One rule, read wherever it ended up. A run triggered by the inbox watcher or by
the console carries the key in `dag_run.conf`, because both have it in hand; a
run triggered with no conf resolves its own delivery and the key it chose is in
`resolve_arrival`'s XCom. The cheap one is tried first and the second call is
made only for the runs the first cannot answer. Airflow 2.10's XCom endpoint
returns the value as a Python **repr**, not as JSON — `{'object_key': '…'}`,
quotes and all — unless the deploy opts into
`AIRFLOW__API__ENABLE_XCOM_DESERIALIZE_SUPPORT`, which loads arbitrary pickled
objects into the webserver and is off here for good reason. So it is parsed
with `literal_eval`, and anything that will not parse yields no key at all: a
run that cannot be placed is shown as an ingest of the feed naming no delivery,
which is exactly what is known about it. Attaching it to a delivery on a guess
is how a page reports another delivery's failure as this one's.

### `origin` records whether the GATE promoted it, not which door it came through

The label that reads wrong until you know what it means. A conformant file
dropped into `inbox/` is uploaded under its own name and needs no gate, so it
is registered `direct` — identical to one an approved sender PUT straight into
the bucket, because that is what `origin` is a statement about. The console
therefore labels the two "renamed by the gate" and "own name" rather than
"inbox" and "direct", and shows the pill only for the gated case: `direct` is
every feed with no `arrival:` block, and a pill saying so on all fifty rows is
fifty repetitions of the default dressed up as information.

## an-incomplete-keep-set-refuses

`retention/orphan_storage.py` deletes a warehouse prefix that no Nessie
reference points at. Its input is not a list of things to delete — it is a set
of things **not** to delete, and everything else goes. That inverts the usual
relationship between an error and its blast radius: a query that fails and
returns nothing normally does nothing, and here it deletes the warehouse.

**So the live set is complete or the sweep does not run.** Three rules, and
the second is the one that would not have occurred to anybody writing this
the first time:

- **A reference that could not be read refuses the whole answer.** Only a 404
  is tolerated — that is the ref-vanished case, identified by status code
  rather than by the shape of the message. Anything else raises
  `IncompleteLiveSet`, which `sweep_orphan_prefixes` turns into a reported
  refusal: no deletions, an `ERROR` line, and `refused` in the report.
- **An empty live set against a non-empty warehouse refuses too.** Not every
  short answer arrives as an exception. A catalog that answers cleanly and
  says it holds no tables at all, while object storage holds some, is either a
  catastrophe or a misconfiguration; "delete everything" is not the reading to
  act on. An empty warehouse is still an ordinary no-op.
- **A refusal exits non-zero.** Nothing was deleted, but nothing was checked
  either, and a zero exit from an unattended sweep reads as "no orphans".

The prefix depth had the same shape of bug from the other end.
`warehouse_table_prefixes` took `"/".join(parts[:3])` while `_warehouse()`
reads a configurable `REPORTING_WAREHOUSE`; `_METADATA_RE` captures the whole
root into a live prefix, so with a nested root (`s3a://bucket/a/warehouse`)
the two halves computed different strings, nothing ever matched, and every
namespace read as an orphan. The depth is now derived from the root. The
shipped `s3a://lakehouse/warehouse` happened to align, which is why it had
never been seen.

### The same shape in the monitor next door

`monitoring/completeness.py` had the mirror image: it caught the Spark read of
a feed's raw table, logged a warning, and recorded `set()` — which `find_gaps`
renders as `{"status": "no data", "missing": []}`, contributing 0 to
`total_missing` so that `--fail-on-gap` passes. A table nobody could open and
a table with nothing in it are the same value, and the module goes to some
length to enumerate its own blind spots without this one among them. A monitor
that goes green on a table it never read is worse than no monitor.

There are **three** answers, not two, and the third is why this does not
simply fail on every error:

| answer | what it means | counted |
|---|---|---|
| `no data` | the table exists and is empty | no |
| `no table` | Spark said `TABLE_OR_VIEW_NOT_FOUND` — a feed declared in `feeds.yml` that has never delivered | no |
| `unreadable` | anything else: catalog down, branch gone, permissions | **yes**, and it fails `--fail-on-gap` |

Matched on the error CLASS rather than on the prose, and an error class that
changes falls through to `unreadable` — the conservative direction. An unread
feed also contributes nothing to the inferred calendar, in either state: it is
not evidence that a date was a business day, and letting a broken read move
the window would change the verdict for every other feed.
