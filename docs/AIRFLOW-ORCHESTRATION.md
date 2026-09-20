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

**Unverified against a live scheduler.** No stack was running in the session
that wrote this DAG; see "Verifying the fast path locally" below before
relying on it in place of reconciliation alone.

### `transport_reconcile` (correctness path)

Runs on a coarser schedule (20 minutes vs. `transport_watch`'s 1) and derives
progress from object-storage evidence alone -- never from Airflow's own run
history, and never from a mutable status column. See "Reconciliation" below
for the staged walk and its scaling argument.

## Task graph (`transport_ingest`)

```text
validate_transport -> create_delivery -> normalize_delivery -> ingest_raw
```

| Task | Domain call | XCom out | Fails for |
|---|---|---|---|
| `validate_transport` | `transport.read_validated_transport(marker_key)` | marker key (str) | Missing/mismatched object, bad hash, unsupported contract version |
| `create_delivery` | `delivery.create_delivery(marker_key)` | DeliveryManifest key (str) | Unknown external Feed id, business-identity conflict |
| `normalize_delivery` | `normalization.normalize_delivery(delivery_manifest_key)` | NormalizationManifest key (str) | Unsafe archive member, invalid zip, contract conflict |
| `ingest_raw` | `scripts._spark_task.run("ingest-v2", normalization_manifest_key, run_id)` (-> `ingest_feed.ingest_normalized_delivery`) | `{feed, delivery_id, cob_date, rows, already_ingested, asset_uri}` | Schema drift with `schema_drift: fail`, row-count/checksum mismatch |

Each task is a few lines calling one existing function; none reimplements
Transport parsing, Feed resolution, control parsing, or manifest creation.
Failure attribution therefore matches the Phase 6 brief's list exactly: an
invalid Transport fails `validate_transport`, an unknown Feed id or identity
conflict fails `create_delivery`, an unsafe archive fails
`normalize_delivery`, and a schema/row/checksum failure fails `ingest_raw`.
Standard `retries`/`retry_delay` apply the same as every other DAG in this
platform; a permanently invalid Transport still retries and still fails, the
same as the legacy path's `ingest` task does today.

`transport_ingest` itself is triggered only -- `schedule=None` -- by
`transport_watch`, `transport_reconcile`, or a manual replay. It never
decides for itself when a Transport is due.

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
  fast, safe no-op, and a retry after a failed branch (never merged) simply
  runs the write again.

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

`transport_watch` and `transport_reconcile` both list the whole `received/`
prefix on every run rather than tracking a cursor. That is a deliberate
choice, not an oversight:

- The prefix listing itself (`list_objects_v2`) is the same cost class this
  platform already accepts for `registry.deliveries.reconcile()` and
  `reconcile_v2()` -- a bounded, paginated S3 LIST, not a bucket-wide scan of
  data files.
- For `transport_watch`, re-listing history is cheap because most of it is
  filtered for free by Airflow's own DagRun-id uniqueness before any object
  storage evidence needs reading again.
- For `transport_reconcile`, a completed-but-processed Transport still costs
  one marker GET and up to two HEADs -- small, fixed, and independent of
  source file size. What it deliberately does NOT do is re-hash the original
  source bytes or spin up a Spark application per already-ingested Delivery:
  the staged walk above exists specifically to bound that cost to
  **O(feeds with an open candidate)**, not O(transports) or O(bucket size).

If a smarter cursor were wanted later (for example, an S3 inventory report,
or a real event-driven discovery feed), it would only ever be an
optimisation on top of this walk -- per the Phase 6 brief's own words,
"optimisation must not become the sole correctness state." A lost or absent
cursor must still be recoverable by the broader evidence walk above, and it
is: there is no cursor to lose.

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

An operator who knows a Transport's TransportID (its source is recorded in
the Transport but not needed to locate the marker: `received/<transport-id>/`
does not vary by source) can safely resume or replay it:

```bash
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
share nothing except their final destination, Raw, and the same
`lakehouse_write` pool that already serialises every writer.

## Verifying the fast path locally

No live stack was available in the session that wrote `transport_watch`, so
its `S3KeySensor`/MinIO connection is unverified. To confirm it once a stack
is available:

```bash
docker compose up -d --force-recreate airflow airflow-webserver airflow-triggerer
docker compose exec -T airflow airflow connections get aws_default
docker compose exec -T feed-ui python -m scripts.simulate_dcm_transport \
  --transport-id dcm-verify-1 --legacy-feed-id <a configured source_identifiers value> \
  --source-observed-at 2026-09-17T05:42:17Z \
  --data /opt/platform/inbox/<a sample file>
docker compose exec -T airflow airflow dags list-runs -d transport_watch -o plain
docker compose exec -T airflow airflow dags list-runs -d transport_ingest -o plain
docker compose exec -T airflow airflow dags list-runs -d prepared_build -o plain
```

`transport_reconcile` needs no such caveat: it uses the same plain-boto3
client every other module in this platform already uses successfully against
MinIO, with no new provider or connection.

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
- Confirming `transport_watch`'s S3KeySensor/MinIO connection against a live
  stack (see above) -- until then, `transport_reconcile` alone is the
  proven, if slower, correctness path.
- A custom lightweight discovery `Trigger` class that would avoid
  `transport_watch` listing the prefix twice per cycle (once inside the
  sensor's own poke, once in `trigger_discovered`) -- a minor, acknowledged
  inefficiency of using the ready-made provider sensor rather than writing
  new async code this session could not validate live.
