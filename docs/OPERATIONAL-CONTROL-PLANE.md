# The operational control plane and COB Feed Status

Historically DCM gave operators one view: every feed, grouped by source
system, with its load status for a COB date. This platform's move to
**generic ingestion** (one `transport_ingest` DAG for every Feed, not one DAG
per Feed) removes the thing that view used to read — a DAG per Feed — without
removing the question an operator asks: *for this COB date, which feeds were
expected, which arrived, which are processing, which completed, which
failed, and which have not shown up?*

This document is the design record for the small addition that answers it:
**COB Status**, `reporting_platform/monitoring/feed_status.py`, its two new
Postgres tables, and the wiring that keeps them durable and idempotent.
Read [`ARCHITECTURE.md`](ARCHITECTURE.md#the-registry-what-is-recorded-and-what-stays-derived)
first — this is an addition to the registry described there, not a second
persistence layer beside it.

## 1. Why this needed a Postgres addition at all

Before this, "how far has Transport X got" was answered **fresh, on every
call**, by walking S3 evidence three reads deep
(`ingest/transport_reconcile.py: discover_transport_progress`) — correct, and
a deliberate choice at the time (see that module's own header), but it has
two shapes of question it cannot answer at all:

- **"Is anything currently running?"** A task mid-flight in Airflow is not
  evidence anywhere in object storage. The S3 walk can say a Transport has
  *not yet* reached NormalizationManifest; it cannot say whether that is
  because nothing has started or because `create_delivery` is running right
  now.
- **"Show me a COB date, cheaply, on every page render."** Three S3 reads per
  Transport is fine for a periodic reconciliation DAG; it is not something an
  operator's status page should pay on every open, and it grows with however
  many Transports arrived that day across however many feeds.

So this is a genuine, additive control-plane requirement, not a preference —
see §2 for why it is still small.

## 2. Why S3 remains the immutable evidence store

Nothing here moves source bytes, hashes, or manifests into Postgres.
`registry.transport_receipt` and `registry.delivery_committed` (§6, §7) each
carry only identifiers and timestamps that point back at `received/` and
`deliveries/` — the same discipline every existing registry table already
keeps (`registry.delivery` does not hold row data; `registry.rejection`
points at `quarantine/`). Both new tables are, in the existing vocabulary,
**observations**: reconstructible, in principle, by re-walking the evidence
they were written from (`transports.sync_from_progress`,
`scripts._spark_task reconcile-committed`, §9) — never a second copy of the
evidence itself.

## 3. Why Iceberg is not used for operational state

Unchanged from the registry's own reasoning
([`ARCHITECTURE.md`](ARCHITECTURE.md#the-registry-what-is-recorded-and-what-stays-derived)):
`sequence_no`-style ordering needs a serialising authority a table on a
Nessie branch does not give cheaply, and an operational status row is
mutated far more often, and far more cheaply, than a Nessie commit is meant
to be. Nothing about generic ingestion changes that calculus.

## 4. Why Airflow's metadata database is not the system of record

`registry.transport_receipt` records `airflow_dag_id`/`airflow_run_id` as
**references** — enough to build a link to the run, never enough to need
Airflow's own database queried to answer "what state is this Transport in".
The COB Status query (`feed_status.build_report`) makes **zero** calls into
Airflow. This mirrors `ui/arrivals.py`'s existing rule exactly: that page
already joins Airflow in for run *display*, live, per request, and is
explicit that "the console does not write an arrivals record and must not
start" — COB Status keeps the same boundary, one layer further from Airflow
rather than closer to it.

## 5. Persisted facts, derived status

The one rule this whole feature is built to honour. Nothing computes or
stores a feed's *status* — `WAITING`/`RECEIVED`/`PROCESSING`/`COMPLETE`/
`FAILED`/`MISSING`/`NOT_EXPECTED` never appears as a column anywhere.
`monitoring/feed_status.status_of` is a pure function from four already-
durable facts to one of those seven strings, called fresh on every request:

```
Feed.delivery_expected / .cadence          -- is a delivery expected at all?
registry.transport_receipt.status          -- has a Transport been discovered,
                                               validated, or has it failed,
                                               for this (feed, cob_date)?
registry.delivery (existing table)         -- has a Delivery been normalized?
                                               TRUE for legacy AND Transport
                                               deliveries alike.
registry.delivery_committed                -- has it reached Raw on `main`?
                                               Also true for both paths.
Feed.expected_by + monitoring.lateness     -- has the deadline passed?
```

`status_of`'s own docstring documents the order these are checked in and why
— in short, a later successful Delivery always outranks an earlier failed
Transport for the same (feed, cob_date), so a correction is never reported
FAILED because of what it corrected.

**`LATE` is not a status.** A feed that arrived after its deadline is exactly
as `COMPLETE`/`PROCESSING` as one that arrived on time; lateness and
processing state are different questions, the same separation
`monitoring/lateness.py`'s own header already draws against
`completeness.py`. `late` rides as a boolean on an arrived feed's entry
instead of being an eighth status that would collide with the other seven.

## 6. TransportReceipt semantics

`registry.transport_receipt` (schema in `registry/db.py`, code in
`registry/transports.py`) is modelled on `registry.run`, **not** on
`registry.delivery`: it is an EXECUTION record — how far *this* Transport
occurrence has been carried through `transport_ingest` — and it legitimately
has a mutable `status`, for the same reason a `run` does. Nothing else
durably records "how far did this attempt get"; object storage records what
each stage *produced*, not that a particular attempt is in flight.

- **Identity**: `transport_id` (Transport's own deterministic identity,
  `docs/TRANSPORT-CONTRACT.md`) is the primary key, so re-discovery upserts
  the same row rather than creating a second one.
- **Vocabulary**: `discovered` → `validated` → `delivered` → `normalized`, or
  `failed` at any point. No `ingested` — whether Raw committed is
  `delivery_committed`'s question (§7), answered identically for both
  ingestion paths, not duplicated here.
- **Monotonic, except failure**: `stage_rank` blocks a stale/racing task
  retry from moving `status` backwards, but `failed` may always be written,
  and a row already `failed` accepts the next real stage unconditionally —
  a successful retry must not stay stuck reading FAILED forever
  (`registry/transports.py::record_stage`).
- **Only for Transport-origin (v2) deliveries.** A legacy `landing/`-direct
  feed has no Transport and therefore no row here — `has_delivery`
  (`registry.delivery`) is what gives that path a PROCESSING signal instead
  (§5).
- **Feed and delivery_id are filled in as soon as they are known** — even on
  a FAILURE, best-effort, because `create_delivery`'s own failure mode
  (identity/date conflict) can occur *after* the Feed itself was resolved,
  and a `FAILED` row with no feed attached would never surface on that
  feed's COB Status row, which is exactly the case an operator most needs to
  see (`airflow/dags/transport_ingest.py::create_delivery_task`).

## 7. Delivery persistence: `delivery_committed`

`registry.delivery` already indexes every accepted Delivery — but at
**normalize** time, before Raw is ever touched (see that table's own header
in `registry/db.py`). So a row existing there is evidence of neither a
commit nor a failure to commit. `registry.delivery_committed` is the missing
fact: one append-only row, written once, by `ingest_feed._ingest_manifest` —
the single function BOTH the legacy and the Transport path funnel through —
right where it already knows it has either appended-and-merged or found the
Delivery already there (the `already_ingested_delivery` short-circuit).

It is a satellite fact table in the same shape as `delivery_part`/
`normalization_part`, not a status column: `PRIMARY KEY (feed, delivery_id)`,
insert-once (`ON CONFLICT DO NOTHING`), and Raw's own `_delivery_id` remains
the authoritative ledger underneath it — this is a cache of that fact for a
status page that must not need a Spark query on every render, exactly the
same relationship `registry.delivery` already has to `landing/`.

**Rows are nullable where a fresh count would cost a Spark action for no
benefit** — the `already_ingested` short-circuit records `rows: NULL` rather
than re-counting an idempotent no-op.

## 8. Feed expectation semantics

No new config, no `FeedExpectation` table. `Feed.delivery_expected`,
`Feed.cadence` and `Feed.expected_by` already existed and already say
everything §5 needs — this was verified during design, not assumed. Two
things are worth being explicit about, because they are real limitations
rather than oversights:

- **No business-day calendar.** A `daily` feed is judged against *every*
  calendar date named, weekends and holidays included — the same choice
  `calendar_rules.py`/`completeness.py` already made, applied here rather
  than invented twice. A feed whose weekends genuinely are not business days
  has no per-date opt-out today; it would report `MISSING` on a non-business
  day it was never going to deliver on. Documented rather than built,
  per the brief this feature was scoped against.
- **`weekly` cadence is judged per ISO week, not per day**, reusing
  `completeness.find_gaps`'s own rule: a weekly feed is `expected` on a given
  COB date only if that date's ISO week has not already seen a delivery
  (`monitoring/feed_status.is_expected`). There is no `day_of_week` field, so
  a weekly feed asked about a single day cannot say *which* day it owes — the
  smallest useful model that does not invent one.

Expectations are **calculated dynamically on every request**, not
materialised. Nothing today needs the alternative: a `FeedExpectation` row
per (feed, date) would be a second thing to keep in sync with `feeds.yml`
edits, for a computation cheap enough to redo on every page load.

## 9. Reconciliation and idempotency

Both discovery paths converge on the same idempotent write, exactly as
required:

```
S3 event (transport_watch, every 1 min)  ---\
                                              +--> registry.transports
transport_reconcile (evidence walk,          |    .record_discovered()
  every 20 min, bounded to the last N days) -/     (INSERT ... ON CONFLICT
                                                     DO NOTHING, keyed on
                                                     transport_id)
```

- `transport_watch`'s `trigger_discovered` calls `record_discovered_quietly`
  for every marker it lists, before (and independently of) triggering
  `transport_ingest` — a duplicate discovery event is a no-op upsert, not a
  second row.
- `transport_reconcile`'s new `sync_receipts` task calls
  `registry.transports.sync_from_progress` against the *same*
  `discover_transport_progress` classification the DAG already computed for
  triggering — so the correctness path and the fast path write through the
  same function, not two implementations that could drift.
- `transport_ingest`'s own tasks (`validate_transport`, `create_delivery`,
  `normalize_delivery`, `ingest_raw`) call `record_discovered_quietly` +
  `record_stage_quietly` right next to their existing
  `_record_delivery_failure` calls — this is **self-healing**: if neither
  discovery path saw a Transport before this DAG ran (a manual replay,
  `docs/AIRFLOW-ORCHESTRATION.md#replaying-a-transport`), the first task to
  run creates the missing receipt row itself.
- Every one of these writes is `_quietly` — best-effort, logged on failure,
  never raised. The registry is an index; a write failing here must never
  change what the pipeline actually does, the same rule
  `deliveries.register_quietly` already follows.

**What this does NOT change**: `transport_watch` still lists the whole
`received/` prefix every minute and relies on Airflow's own `DagRunAlreadyExists`
for trigger idempotency (`_transport_trigger.py`) — that mechanism already
works and this feature does not touch it. Bounding that unbounded listing as
Transport history grows over years is a real, separate concern the existing
code already names (`docs/AIRFLOW-ORCHESTRATION.md#reconciliation-scale`) and
is deliberately left alone here — a checkpoint/watermark for it is follow-up
work, not part of this control plane.

## 10. Failure and recovery semantics

No distributed transaction anywhere. Each step is independently retryable:

- A Transport marker existing in S3 with no receipt row yet is recovered by
  either discovery path on its next pass, or by `transport_ingest` itself
  self-healing (§9) — there is no window where evidence exists and can never
  be found.
- A receipt stuck at `failed` is not stuck: the next successful attempt for
  the same `transport_id` (a manual replay after a fix, or a retried task)
  overwrites it unconditionally (§6) — `failed` is a fact about the last
  attempt, not a terminal state.
- A `delivery_committed` write failing after a successful Raw commit does
  not corrupt anything: Raw is still the ledger, `registry.delivery` still
  has its row, and the next `scripts._spark_task reconcile-committed <feed>`
  run (§11) recovers the missing fact from Raw directly.
- **Database restart loses nothing that was not already about to be
  recomputed.** Every row in both new tables is either overwritten by the
  pipeline's own next natural step (an in-flight Transport simply reports
  its last durably-recorded stage until the next task updates it) or
  rebuildable by reconciliation (§9, §11) — neither table is the only place
  a fact lives.

## 11. Migration implications for existing local data

**One real one, and it is a Raw provenance limitation, not a new bug**:
`_delivery_id` is a raw provenance column added lazily, per
[`ARCHITECTURE.md`](ARCHITECTURE.md#a-declared-column-migrates-itself) —
"provenance is added, not backfilled". A Delivery ingested before that
column existed on a given feed's raw table has no `_delivery_id` to read at
all, so `scripts._spark_task reconcile-committed <feed>` cannot recover a
`delivery_committed` fact for it, and that Delivery will show `PROCESSING`
on the COB Status page **until that feed's raw table is rebuilt**
(`--full-refresh`) or that specific COB date ages out of retention. This was
observed directly against this stack's own data during verification, not
assumed — see §12.

Run the backfill once per feed after deploying this feature, for every feed
whose raw table already exists:

```bash
docker compose exec -T airflow python -m scripts._spark_task reconcile-committed <feed>
```

Idempotent (`ON CONFLICT DO NOTHING`) — safe to re-run, and safe to run
before `registry.transport_receipt` exists at all, since it only touches
`delivery_committed`.

## 12. Verification against the live stack

Demonstrated end to end against this repository's own local stack, not
assumed:

- **COMPLETE / PROCESSING / MISSING / NOT_EXPECTED** appeared organically
  from this stack's existing historical data once `reconcile-committed` was
  run — no fixtures needed.
- **RECEIVED → PROCESSING → COMPLETE** was watched live: a real
  `reporting_transport` publish, picked up by `transport_watch` within a
  minute, carried through `transport_ingest` end to end, with
  `registry.transport_receipt` and `registry.delivery_committed` updating at
  each stage exactly as designed.
- **FAILED** was demonstrated with a genuine identity conflict (a Transport
  whose filename and control file disagreed on business date) — `FAILED`
  appeared on the correct feed's row, with the real exception text, and
  `late: false`/`late: true` were both observed depending on the COB date
  and `expected_by` involved.
- `transport_reconcile`'s new `sync_receipts` task was triggered manually
  and completed successfully alongside `discover_progress`/`check_raw`/
  `trigger_pending`.
- All existing DAGs still parse (`scripts.check_dag_imports`, 13 DAGs from 7
  files, unchanged) and the full pure test suite passes
  (798 passed, 0 failed, 14 skipped — matching the documented baseline in
  `tests/README.md`) after these changes.

## 13. What was deliberately not built

- **A materialised `FeedExpectation` table.** See §8 — nothing needs it yet.
- **A multi-COB-date history matrix.** `feed_status.build_report` takes one
  COB date; the persistence underneath it (two indexed-by-`cob_date` tables)
  is already shaped to answer a range query the same way, so a history view
  is an additional endpoint over the same facts, not a schema change, when
  it is actually wanted.
- **A watermark/checkpoint for `transport_watch`'s unbounded listing.** A
  real, separate, pre-existing concern (§9) — left alone, on purpose, rather
  than folded into this feature's scope.
- **Any change to how `transport_watch` decides to trigger `transport_ingest`.**
  Airflow's own `DagRunAlreadyExists` already makes that idempotent; this
  feature only adds observability alongside it.
