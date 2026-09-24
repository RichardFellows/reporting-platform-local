# Airflow orchestration (Phase 6)

Phase 6 makes Airflow the orchestration owner of the new-platform path:

```text
received/<transport-id>/_COMPLETE.json
        |
        v
   validate_transport
        |
        v
   create_delivery          (Phase 2 domain code)
        |
        v
   normalize_delivery       (Phase 3 domain code)
        |
        v
   ingest_raw                (Phase 4 domain code, Spark)
        |
        v
   Raw asset emitted
        |
        v
   prepared_build  ->  Prepared asset  ->  reporting_build  (Phase 0/unchanged)
```

Read this alongside `docs/TRANSPORT-CONTRACT.md`, `docs/DELIVERY-CONTRACT.md`,
`docs/NORMALIZATION-CONTRACT.md` and `docs/RAW-INGESTION-CONTRACT.md`, which
define the domain evidence and operations every task here is a thin wrapper
over. This document is about orchestration only: triggering, retries,
concurrency, and recovery. It adds no new domain logic.

## The acquisition boundary

DCM owns DFS/SFTP acquisition. The platform's first fact is
`received/<transport-id>/_COMPLETE.json` -- the uploader's assertion that
every source object for one Transport is durably stored
(`docs/TRANSPORT-CONTRACT.md`). Airflow never infers completeness from
modification time, file-size stability, or sleeps; `validate_transport`
re-verifies the marker's claims (existence, byte count, SHA-256) before
anything downstream acts on them.

Nothing here implements DFS polling, SMB, Kerberos, SFTP polling, or
partial-file detection. That boundary is unchanged from Phase 1.

## Responsibility split

| Owns | What |
|---|---|
| DCM | Acquisition and the `_COMPLETE.json` assertion |
| Airflow | Triggering, task dependencies, retries, timeouts, concurrency, execution state, operational waiting |
| RPL Python | Transport validation, Feed resolution, Delivery/business identity, normalization, domain validation (unchanged Phase 1-4 code) |
| Spark | Scalable Raw ingestion (`scripts/_spark_task.py ingest-v2`) |
| dbt | Prepared/reporting transformation and tests (unchanged) |

No domain logic was moved into a DAG file. Every task in `transport_ingest`
calls exactly one existing Phase 1-4 function and returns a short reference;
see "Task graph" below for the mapping.

## Trigger design

Three mechanisms exist, in the priority order the Phase 6 brief sets:

1. **Approved object-store event integration** -- not available. This repo's
   MinIO has no bucket notification/webhook wired to anything, and inventing
   one would be infrastructure this repository and its OpenShift target do
   not support today. `docs/OPENSHIFT-MAPPING.md` leaves "S3 event / SFTP
   landing prefix poll" as an open decision for the cluster target, not a
   settled mechanism to imitate.
2. **Airflow-native asset/event mechanism** -- used, but only for the
   INTERNAL Raw -> Prepared coupling (see "Raw asset emission" below).
   Airflow Assets/Datasets model events Airflow itself emits; they do not
   reach an external object arriving in `received/`, which Airflow never
   wrote.
3. **A lightweight deferrable object sensor** -- `transport_watch`, below.
   Chosen over (4) alone because it is available at zero new infrastructure
   cost: `apache-airflow-providers-amazon` is already an image dependency
   (`Dockerfile.airflow`) and was otherwise unused, and its `S3KeySensor`
   supports `deferrable=True` out of the box.
4. **Periodic discovery/reconciliation** -- `transport_reconcile`, below.
   Never optional: per the Phase 6 brief, "events are an optimisation;
   reconciliation is correctness." This DAG is what makes that literally
   true here, independent of whether the sensor in (3) is even running.

### `transport_watch` (fast path)

One DAG, one `S3KeySensor(deferrable=True)` watching
`received/*/_COMPLETE.json` -- **one sensor for the whole platform, not one
per Feed.** The unit of acquisition is Transport; a per-Feed poller would
mean hundreds of permanently-running sensors once DCM's Feed count grows,
which the Phase 6 brief explicitly rules out.

Deferrable means the sensor holds no worker slot while it waits: the
triggerer polls asynchronously, not a task process. On finding at least one
match it hands off to `trigger_discovered`, which lists every currently
completed Transport (`transport.list_completed_transports()`) and attempts to
trigger `transport_ingest` for each -- see "Idempotency" for why re-listing
history on every cycle is safe and cheap.

Timing out with nothing new is the ordinary steady state, not a failure:
`soft_fail=True` turns that into a skip, exactly the same idiom
`feed_ingest.py`'s `resolve_arrival` already uses for "nothing pending"
(`AirflowSkipException`).

**Verified against a live scheduler.** See "Verifying the fast path locally"
below for what was run and observed -- the sensor reaches MinIO, triggers
`transport_ingest`, and re-triggering an already-processed TransportID dedups
via `DagRunAlreadyExists` exactly as designed.

### `transport_reconcile` (correctness path)

Runs on a coarser schedule (20 minutes vs. `transport_watch`'s 1) and derives
progress from object-storage evidence alone -- never from Airflow's own run
history, and never from a mutable status column. See "Reconciliation" below
for the staged walk and its scaling argument.

## Task graph (`transport_ingest`)

```text
validate_transport -> create_delivery -> normalize_delivery -> ingest_raw
    -> report_drift -> record_snapshot
```

| Task | Step (`ingest/transport_steps.py`) | Domain call inside it | XCom out | Fails for |
|---|---|---|---|---|
| `validate_transport` | `validate` | `transport.read_validated_transport(marker_key)` | marker key (str) | Missing/mismatched object, bad hash, unsupported contract version |
| `create_delivery` | `deliver` | `delivery.create_delivery(marker_key)` | DeliveryManifest key (str) | Unknown external Feed id, business-identity conflict, a declared control file that did not arrive |
| `normalize_delivery` | `normalize` | `normalization.normalize_delivery(delivery_manifest_key)` | NormalizationManifest key (str) | Unsafe archive member, invalid zip, contract conflict |
| `ingest_raw` | `ingest_raw` | `spark_task.run("ingest-v2", normalization_manifest_key, attempt_id)` (-> `ingest_feed.ingest_normalized_delivery`) | `{feed, delivery_id, cob_date, rows, already_ingested, asset_uri, run_id, commit}` + drift column names | Schema drift with `schema_drift: fail`, row floor/ceiling, row-count/checksum mismatch |
| `report_drift` | `steps.drift_warnings` | -- (logs) | the same summary | Never: drift is reported, not fatal |
| `record_snapshot` | `steps.record_snapshot` | `Nessie.create_tag(snapshot/<feed>/<bd>/<run_id>, hash=commit)` | the summary + `tag` | Never: a tag that cannot be cut is returned as `tag_error`. No tag when `already_ingested` |

The last two are `ingest_<feed>`'s own last two tasks, calling the same
functions, so a Transport ingest is reported and pinned exactly as an inbox
one. The tag names the commit `ingest_raw`'s merge made, not `main`'s head
(`docs/DECISIONS.md#a-snapshot-tag-names-its-merge-commit`), which is what
lets it run outside the write pool.

Each task is one call to its step, and the step -- with the receipt and
validation evidence it records -- is what `python -m reporting_platform.ingest
transport` runs with no Airflow (`docs/STANDALONE-PIPELINE.md`). The task
adds only what Airflow alone knows: the dag and run id, the try number in the
branch name, `AirflowFailException` for a refusal, and the raw asset event.
`tests/test_transport_steps.py` fails if a task grows its own copy of a step.
No step reimplements Transport parsing, Feed resolution, control parsing, or
manifest creation.
Failure attribution therefore matches the Phase 6 brief's list exactly: an
invalid Transport fails `validate_transport`, an unknown Feed id or identity
conflict fails `create_delivery`, an unsafe archive fails
`normalize_delivery`, and a schema/row/checksum failure fails `ingest_raw`.
Standard `retries`/`retry_delay` apply to a failure that might not recur.
**A refusal is not retried**: a `DeliveryError` in `create_delivery` (a
missing declared control file included) and a blocking Raw check in
`ingest_raw` -- schema drift under `fail`, the row floor/ceiling, a declared
row count or md5 that does not match -- fail the task once, with
`AirflowFailException`, because the same evidence fails the same check every
time. The legacy `ingest` task does the same. See
`docs/DECISIONS.md#a-refusal-is-not-retried`.

Which task writes which `registry.transport_receipt` stage, and how
`failed` is later overwritten (by a retry, or by `transport_reconcile`), is
drawn in
[OPERATIONAL-CONTROL-PLANE.md §6](OPERATIONAL-CONTROL-PLANE.md#6-transportreceipt-semantics).

`transport_ingest` itself is triggered only -- `schedule=None` -- by
`transport_watch`, `transport_reconcile`, or a manual replay. It never
decides for itself when a Transport is due.

**Phase 7.** The first three tasks each catch their domain call's exception,
record a `registry.validation_result` row (`layer=delivery`, keyed by
`transport_id`), and re-raise unchanged -- the failure attribution above is
unaffected, only now investigable after the fact by `registry validation
transport <id>` rather than only through Airflow's own log retention. A
success writes nothing here; the DeliveryManifest/NormalizationManifest that
task returns is already the evidence. `ingest_raw`'s own checks record their
own PASS/FAIL evidence one layer down, inside `ingest_feed.py` -- see
[`VALIDATION.md`](VALIDATION.md).

## XCom: references, not evidence

Every inter-task value above is a short S3 key (a few hundred bytes at most)
or a small summary dict of identifiers and counts. No task ever pushes a
`.as_manifest()` dict, a `Delivery`/`Transport` object, or row data. The
authoritative evidence stays in object storage at the key each task returns;
a cleared task re-derives it by reading that key again, which is exactly what
a retry does regardless of whether Airflow cleared it or the task failed on
its own.

## Idempotency across retries and duplicate events

Every task boundary is safe to repeat, because the Phase 1-4 domain
operations it calls already are:

- `read_validated_transport` only reads and re-verifies; it has no side
  effect to repeat.
- `create_delivery` is create-once: the same marker key always returns the
  same DeliveryManifest, written on the first call and read-and-verified
  (never re-derived from today's Feed config) on every one after.
- `normalize_delivery` is create-once per deterministic Ready key; a retry
  after a partial archive extraction resumes from whatever parts already
  exist and accepts only byte-identical ones.
- `ingest_normalized_delivery` is guarded by `_delivery_id` on committed Raw
  `main` -- `already_ingested_delivery` -- so a retry after a merge is a
  fast, safe no-op, and a retry after a failed branch (never merged) runs
  the write again on a NEW branch -- `ingest_attempt_id` names one per
  attempt. Until it did, the retry collided with the branch its failed
  predecessor kept for inspection and died on Nessie's `409 Conflict`, so
  this sentence was false for every failure it describes.

So **duplicate events are harmless by construction**: two `_COMPLETE.json`
notifications for the same TransportID resolve to the same DeliveryID
(`docs/DELIVERY-CONTRACT.md`), the same NormalizationManifest, and the same
committed Raw Delivery. Nothing in Phase 6 adds a
`transport.processed`/`delivery.status`/`registry.delivery.ingested` column
to make retries safe -- the durable evidence chain already is the guard.

`transport_watch` and `transport_reconcile` add one more idempotency layer on
top, at the orchestration level: both trigger `transport_ingest` with a
DETERMINISTIC run id, `transport__<transport-id>`
(`airflow/dags/_transport_trigger.py`). Airflow's own uniqueness on
`(dag_id, run_id)` means triggering the same Transport twice -- from a
duplicate event, the fast path and reconciliation both noticing the same
Transport, or a stale reconciliation candidate the fast path already
started -- raises `DagRunAlreadyExists`, caught and treated as "already
queued or done." This is Airflow's own run history, not a new table Phase 6
introduces to make retries safe.

## Reconciliation

`reporting_platform/ingest/transport_reconcile.py` derives a Transport's
stage purely from durable evidence, in three cheap reads before anything
expensive:

```text
list_completed_transports()
    |
    for each: read the small completion marker (cheap; no source-byte hashing)
    |
    DeliveryManifest key exists?  --no-->  needs_full_chain
    |  yes
    NormalizationManifest key exists?  --no-->  needs_full_chain
    |  yes
    candidate: (feed, delivery_id, transport_id)
```

`needs_full_chain` transports are handed straight to `transport_ingest`,
which re-enters the whole domain chain from `validate_transport` --
deliberately: `create_delivery` always re-validates on every call, by design
(`docs/DELIVERY-CONTRACT.md`, "Historical interpretation wins over today's
Feed configuration"), so there is no cheaper safe way to "resume from the
Delivery stage" without re-entering it.

Deliveries that ARE normalized still need one more check that object storage
cannot answer cheaply: whether Raw has committed them. Raw's `_delivery_id`
is the only v2 ingestion ledger by design
(`docs/RAW-INGESTION-CONTRACT.md`) -- there is no status column to read
instead. `check_raw` therefore issues **one Spark query per Feed that has a
candidate** (`SELECT DISTINCT _delivery_id`, via the new
`raw-delivery-ids` `_spark_task` op and
`ingest_feed.raw_delivered_ids`), never one per Delivery or per Transport,
and diffs the candidates against that set
(`transport_reconcile.raw_pending`). This read is deliberately outside the
`lakehouse_write` pool -- it only reads Raw, matching every other read-only
Spark job in this platform (completeness, reproducibility, maintenance
metrics; see `docs/ARCHITECTURE.md`, "Where Spark actually runs").

Nothing here is a mutable "current stage": every classification is
recomputed from object storage (and, for the last stage, Raw) on every run.
If reconciliation itself were lost or reset, the very next run reconstructs
the same answer from the same evidence -- there is no cursor whose loss
would silently skip anything.

### Reconciliation scale

`transport_watch` lists the whole `received/` prefix on every run rather than
tracking a cursor. That is a deliberate choice, not an oversight:

- The prefix listing itself (`list_objects_v2`) is the same cost class this
  platform already accepts for `registry.deliveries.reconcile()` and
  `reconcile_v2()` -- a bounded, paginated S3 LIST, not a bucket-wide scan of
  data files.
- Re-listing history is cheap because most of it is filtered for free by
  Airflow's own DagRun-id uniqueness before any object storage evidence needs
  reading again.

If a smarter cursor were wanted later (for example, an S3 inventory report,
or a real event-driven discovery feed), it would only ever be an
optimisation on top of this walk -- per the Phase 6 brief's own words,
"optimisation must not become the sole correctness state." A lost or absent
cursor must still be recoverable by the broader evidence walk above, and it
is: there is no cursor to lose.

### Reconciliation scale (v2)

`transport_reconcile` **no longer** lists the whole `received/` prefix by
default. This is a genuine narrowing of the paragraph above, made possible by
Contract v2's `received/cob_date=<date>/source_system=<system>/<transport-id>/`
partitioning (`docs/TRANSPORT-CONTRACT.md`) -- not an addition on top of the
old behaviour, and worth being explicit that it changes an earlier documented
decision rather than merely extending it.

The scheduled run bounds `discover_transport_progress` to the last
`TRANSPORT_RECONCILE_WINDOW_DAYS` calendar days (default 7, `os.environ`-
configurable) via `list_completed_transports(cob_dates=[...])`, which lists
only those COB partitions. Cost is now **O(transports within the window)**,
not O(all transports ever published) -- the missing piece the staged walk
above (still unchanged) could not itself bound, because it only controls cost
*per discovered Transport*, not how many Transports are discovered in the
first place.

The unbounded walk is still available -- `discover_transport_progress(
cob_dates=None)`, `list_completed_transports(cob_dates=None)` -- but is no
longer what the periodic schedule calls. Trigger it explicitly for an
occasional full catch-all:

```bash
docker compose exec -T airflow airflow dags trigger transport_reconcile \
  -r full_sweep_1 -c '{"full_sweep": true}'
```

Two things are **only** found by the unbounded form, never the bounded one:
a Transport whose `cob_date` falls outside the window, and any v1 marker (no
`cob_date=` partition exists for it to be found by at all -- see
`docs/TRANSPORT-CONTRACT.md`, "v1 compatibility"). This is the direct,
accepted cost of bounding the scan: a stuck Transport older than the window
is not self-healing on the default schedule and needs an operator (or a
longer window, or a periodic full-sweep run on a much coarser cadence) to
surface it. Nothing here implements that coarser periodic full sweep --
deferred, not built, because the brief for this change was "make the default
path bounded," not "also design its own safety net's safety net."

`transport_watch` is intentionally NOT bounded the same way: its job is
noticing a Transport newly completing, which could be for any COB date
(today's, ordinarily, but nothing prevents a backdated correction), so
narrowing its scan by COB date would risk silently ignoring exactly the
change that matters. Its cost argument above (cheap re-listing via DagRun
dedup) already holds regardless.

## Raw asset emission

`ingest_raw` declares `outlets=[AssetAlias("raw-table-updated")]`
(`DatasetAlias` pre-Airflow-3) rather than a static list of every Feed's
asset. A single generic DAG processing any Feed cannot know which concrete
asset to declare at parse time, and a static list of every asset would mark
EVERY Feed updated on every Transport regardless of which one it actually
touched -- wrong, and a regression the Phase 6 brief calls out by name
("Raw asset emitted before commit" is listed among the regressions to
check for, and a spuriously-updated unrelated asset is the same defect from
the other direction). At run time, once the Feed is known,
`ingest_raw` resolves the alias to the SAME concrete asset the legacy path
already uses -- `Asset(feed.asset_uri)`, `iceberg://<catalog>/<namespace>/<feed>`
-- via `context["outlet_events"][RAW_ASSET_ALIAS].add(...)`.

Because `prepared_build`'s existing schedule
(`airflow/dags/dbt_builds.py`, `any_of(RAW_ASSETS)`) matches Datasets by URI,
not by which DAG or task emitted them, **`dbt_builds.py` needed no change at
all.** The event this DAG emits is indistinguishable, to `prepared_build`,
from one the legacy per-feed `ingest_<feed>` DAG emitted.

The event is only ever added after `scripts._spark_task.run("ingest-v2", ...)`
returns. That call does not return until `ingest_normalized_delivery` has
either merged the Delivery onto `main` or raised, leaving `main` untouched
-- so the sequence is always:

```text
Spark branch write -> validation -> Nessie merge -> Raw committed
                                                          |
                                                          v
                                              Raw asset emitted
                                                          |
                                                          v
                                              prepared_build eligible
```

Emitting on the `already_ingested: True` (idempotent no-op) path is correct,
not merely harmless: that path returns `True` precisely because the Delivery
IS already committed on `main`, so the asset update is just as true on a
retried or duplicate-triggered run as on the run that did the write.

## Concurrency and pools

- `ingest_raw` is the only Phase 6 task on `pool="lakehouse_write"` --
  the same single-slot pool every existing writer (legacy ingest, dbt
  builds, maintenance, retention) already shares. Phase 6 adds no second
  pool and no lock table; the single-slot writer constraint documented in
  `docs/ARCHITECTURE.md` and `docs/DECISIONS.md#one-shared-write-pool` is
  unchanged and is called out here as a scalability concern to revisit
  later, not solved by this phase.
- `check_raw` (`transport_reconcile`) is read-only and deliberately outside
  that pool, matching the existing precedent for read-only Spark jobs.
- `validate_transport`, `create_delivery`, `normalize_delivery` touch no
  Iceberg write path and run outside the pool, so independent Transports --
  different Feeds, or the same Feed on different days -- can validate,
  create, and normalize concurrently. `transport_ingest` caps
  `max_active_runs=10` as a bounded concurrency guard against a burst of
  arrivals flooding the scheduler; only `ingest_raw`'s pool actually
  serialises writes.
- `transport_watch` and `transport_reconcile` each cap `max_active_runs=1`:
  there is exactly one instance of each, and overlapping runs of the SAME
  watcher/reconciler would only duplicate discovery work, never parallelise
  useful writing.

## Replaying a Transport

An operator who knows a Transport's marker key can safely resume or replay
it directly:

```bash
docker compose exec -T airflow airflow dags trigger transport_ingest \
  -r replay-<distinct-id> -c '{"marker_key": "received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/dcm-1234-849217/_COMPLETE.json"}'
```

Since Contract v2 (`docs/TRANSPORT-CONTRACT.md`) a marker key also encodes
`cob_date`/`source_system`, which `transport_id` alone no longer determines
-- `marker_key` is therefore what `transport_watch`/`transport_reconcile`
actually pass through `_transport_trigger.py`, and is the most direct way to
replay one by hand too. Two other forms remain accepted by `validate_transport`
for convenience:

```bash
# v2, from its parts
docker compose exec -T airflow airflow dags trigger transport_ingest \
  -r replay-<distinct-id> -c \
  '{"transport_id": "dcm-1234-849217", "cob_date": "2026-09-21", "source_system": "RISK_ENGINE_X"}'

# v1 (legacy), transport_id alone is still enough -- there is no partitioning to supply
docker compose exec -T airflow airflow dags trigger transport_ingest \
  -r replay-<distinct-id> -c '{"transport_id": "dcm-1234-98765"}'
```

Because every domain operation in the chain is idempotent, this is always
safe regardless of how far the Transport previously got: `create_delivery`
returns the existing DeliveryManifest if there is one, `normalize_delivery`
returns the existing NormalizationManifest, and `ingest_raw` no-ops if Raw
already has the Delivery. No registry row needs deleting, no file needs
renaming, no manifest needs editing, and no "ingested" flag needs resetting
-- because none of those exist for this path.

Use a run id distinct from `transport__<transport-id>` (as above) if the
automatic DagRun already exists and you want a fresh run recorded for
observability; reusing `transport__<transport-id>` itself is also safe and
simply resumes/no-ops against the same evidence.

## Backward compatibility with the legacy path

The legacy `landing/ -> ready/ (v1) -> raw` path (`feed_ingest.py`,
`ingest_feed.ingest`) is unchanged and remains fully operational. Phase 6
adds no code path from `received/` into `landing/`, and the legacy
`feed_ingest.py` DAGs do not discover `received/` -- `resolve_arrival` and
`arrival.find_pending` only ever look at `landing/`/`ready/`. The two paths
share the object-storage destination, Raw, the `lakehouse_write` pool that
already serialises every writer, and one feed's `ready/<feed>/` prefix
itself -- `normalization.py` (v2) deliberately writes its rebuildable plan
one level deeper in that same prefix, `ready/<feed>/<delivery-id>/`
(`docs/NORMALIZATION-CONTRACT.md`, "beside, not in place of"). Found live: a
naive prefix-plus-`.json` match in the legacy `list_manifests` (v1) picked up
v2's nested manifest too and `read_manifest` raised on its (different)
`manifest_version` key, breaking `bulk_ingest`/`find_pending` for any feed a
Transport had ever touched. Fixed in `is_manifest_key`
(`reporting_platform/ingest/normalize.py`) to require no further `/` after
the prefix -- the shape every v1 manifest key always has and a v2 one never
does; `test_normalize.test_a_v2_delivery_manifest_sharing_this_prefix_is_not_picked_up`
pins it.

## Verifying the fast path locally

**Re-verified live against Contract v2**, against a long-running dev stack
(not a fresh seed). Confirmed working end to end through a committed Raw row:
`scripts.simulate_dcm_transport` (v2 CLI: `--legacy-feed-id qa-happy-position
--producer-run-id quickstart-1 --cob-date 2026-09-14 --source-system QA ...`)
published a Contract v2 marker at
`received/cob_date=2026-09-14/source_system=QA/dcm-qa-happy-position-quickstart-1/`;
`transport_watch`'s sensor found it within its next cycle and triggered
`transport_ingest`, whose four tasks (`validate_transport`, `create_delivery`,
`normalize_delivery`, `ingest_raw`) all succeeded; the row landed in
`raw.qa_happy_position` with `_source_file` pointing at the v2 key and
`_cob_date = 2026-09-14`, confirmed by `duckdb_console`.

**One real bug this run caught and fixed**: `docker-compose.yml` never
mounted the new `reporting_transport/` package into any container --
`scripts/simulate_dcm_transport.py` (`feed-ui`) and
`reporting_platform/ingest/transport.py` (every Airflow service, which now
imports `reporting_transport.contract`/`.storage`) both raised
`ModuleNotFoundError` before the mount was added to `x-airflow-common` and to
`feed-ui`'s own restated `volumes:` list. Neither the pure-Python test tier
nor CI's `parse` tier catches this class of bug -- both run against a plain
checkout, never through the compose bind-mount topology -- which is exactly
why this needed a live container, not just `python -m tests.run`.

**Not re-verified this run**: the `prepared_build` → `reporting_build` →
`reporting.qa_happy_position_summary` hop. This particular long-running dev
stack has raw tables only for `fo_trade` and `qa_happy_position` --
`qa_headerless_position`, `ref_counterparty`, `ref_collateral` and
`ref_rating` were never ingested here, so `prepared_build`'s shared
write-audit-publish run (one Nessie branch for every feed, one merge
decision) failed on those unrelated models'
`[TABLE_OR_VIEW_NOT_FOUND] raw.qa_headerless_position` before it ever reached
the publish gate -- blocking `qa_happy_position`'s otherwise-successful branch
build from merging too, alongside everyone else's. This is a pre-existing gap
in this one stack's seed state (not a fresh `generate_feeds.py --clean`, and
not something Contract v2 touched), not a defect in the Transport path
itself; the original v1 run below was against a stack that did not have this
problem, which is why it could observe the merge and the final `reporting`
row. Re-run on a freshly seeded stack (or after landing/bulk-ingesting the
other feeds) to re-confirm that last hop for v2.

**Original Contract v1 run, for reference** (superseded above for the
Transport→Raw portion, not repeated for prepared/reporting since a fresh
stack was not available this session):

`docker compose exec -T airflow airflow connections get aws_default`
resolves `aws_default` to `http://minio:9000` with the credentials from
`docker-compose.yml`'s `AIRFLOW_CONN_AWS_DEFAULT`. A Transport simulated with
`scripts.simulate_dcm_transport --legacy-feed-id qa-happy-position ...` was
picked up by `transport_watch`'s `S3KeySensor` within its next 1-minute
cycle (`wait_for_completed_transport` succeeded, `trigger_discovered`
triggered `transport_ingest`), which ran
`validate_transport -> create_delivery -> normalize_delivery -> ingest_raw`
to a committed Raw Delivery and a `dataset_triggered__...` `prepared_build`
run, which on success triggered `reporting_build` the same way, ending in
real rows in `reporting.qa_happy_position_summary`. Re-triggering the same
TransportID (the marker persists, so `transport_watch` keeps finding it every
cycle) deduped every time via `DagRunAlreadyExists`
(`trigger_discovered`'s XCom: `{"triggered": [], "already_queued_or_done":
1}`), with no failure and no duplicate Raw rows. `transport_reconcile`,
triggered manually with `transport_watch` paused, correctly discovered a
second simulated Transport that had never gone through the fast path and
triggered `transport_ingest` for it, including `check_raw`'s
`raw-delivery-ids` Spark query against real Iceberg/Nessie. `airflow dags
list-import-errors` and `python -m scripts.check_dag_imports` are both
clean. Reproduce with:

```bash
docker compose up -d --force-recreate airflow airflow-webserver airflow-triggerer
docker compose exec -T airflow airflow connections get aws_default
docker compose exec -T feed-ui python -m scripts.simulate_dcm_transport \
  --legacy-feed-id qa-happy-position --producer-run-id verify-1 \
  --cob-date 2026-09-14 --source-system QA \
  --source-observed-at 2026-09-17T05:42:17Z \
  --data /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.csv \
  --control /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.ctl
docker compose exec -T airflow airflow dags list-runs -d transport_watch -o plain
docker compose exec -T airflow airflow dags list-runs -d transport_ingest -o plain
docker compose exec -T airflow airflow dags list-runs -d prepared_build -o plain
```

`transport_reconcile` needed no such caveat: it uses the same plain-boto3
client every other module in this platform already uses successfully against
MinIO, with no new provider or connection -- confirmed live along with
everything else above.

One unrelated, pre-existing gap surfaced while seeding a clean baseline for
this run and is **not** a Phase 6 defect: `scripts/generate_feeds.py` /
`reporting_platform/ui/sampledata.py` never emit `.ctl` control files for
`qa_happy_position` or `qa_headerless_position`, and `scripts/land_feeds.py`
only lands files matching a feed's DATA `filename_pattern`, never a control
file. Both feeds' seeded deliveries sit permanently "awaiting control" under
the standard QUICKSTART seed -> land -> bulk_ingest walkthrough, predating
Phase 6 (introduced with these two feeds, commit `ca59b8d`). Verifying this
session's own baseline required hand-landing matching `.ctl` objects; that
gap is left for whoever next touches the sample-data generator, not fixed
here per this task's scope.

## Dual-run migration orchestration (Phase 8)

`migration_reconcile` is a second, wholly INDEPENDENT DAG -- it calls nothing
here and nothing here calls it. It reads what `transport_ingest`/
`dbt_builds.py` have already published (registered Deliveries, Raw/prepared/
reporting tables) plus a legacy adapter, and writes only to
`registry.migration_comparison`. This DAG's schedule, retries or a legacy
outage cannot affect ingestion latency or correctness on this page's own
task graph. See [`MIGRATION.md`](MIGRATION.md).

## What is deliberately not here

Per the Phase 6 brief's explicit non-goals: OpenMetadata, DCM .NET changes,
DFS/SMB/SFTP polling, migrating all DCM schedules/gates, supersession/SCD2
redesign, retention redesign, historical backfill, legacy ingestion removal,
Ready v1 removal, a custom DQ catalogue, generic value-level lineage, or a
Spark writer concurrency redesign. `expected_by`/cadence/`delivery_expected`
remain monitoring-only (`reporting_platform/monitoring/lateness.py`,
unchanged) and never gate Delivery creation; a late or missing expected Feed
is an operational observation, not a manufactured Delivery or a `late` status
on any manifest.

## Deferred to later phases

- Migrating DCM's remaining follow-on-job/gate semantics feed by feed
  (Phase 6 brief, #16) -- until then, DCM remains authoritative for those,
  and Airflow assets/dbt `ref()` dependencies are preferred over recreating
  them for any NEW dependency introduced in this path.
- OpenMetadata integration (Phase 7+).
- A custom lightweight discovery `Trigger` class that would avoid
  `transport_watch` listing the prefix twice per cycle (once inside the
  sensor's own poke, once in `trigger_discovered`) -- a minor, acknowledged
  inefficiency of using the ready-made provider sensor rather than writing
  new async code this session could not validate live.
