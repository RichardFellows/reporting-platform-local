# Dual-run migration and reconciliation (Phase 8)

How a Feed proves it is safe to cut over from the true legacy estate to this
platform: same source delivery through both paths, explicit comparison,
durable evidence, a derived readiness answer, and a human who still has to
flip the switch. `reporting_platform/migration/`.

## Terminology: which "legacy" this document means

This repo already uses "legacy" loosely in two different senses, and Phase 8
needs them kept apart:

1. **The true legacy estate** -- the enterprise SQL Server / stored-procedure
   / legacy-ETL-tool / legacy-report-server stack this WHOLE platform
   replaces (`docs/ARCHITECTURE.md`'s opening diagram: "Upstream CSV -> the
   legacy ETL tool package -> the legacy RDBMS stg tables -> ..."). This is
   what "legacy" means EVERYWHERE in this document. It has no code in this
   repo and no local approximation of its own.
2. **The Landing/Ready-v1 ingestion path** -- an older ENTRY POINT of this
   SAME new platform (`landing/` -> `ready/` -> Raw, predating the Transport
   contract). `docs/ARCHITECTURE.md` calls this "the legacy `landing/` path"
   in one place, which reads as sense (1) out of context. It is not: both
   Landing/Ready-v1 and the Transport-driven path feed the same Raw layer and
   are equally "new" for Phase 8's purposes. Nothing in this phase treats
   Landing/Ready-v1 as the system being migrated away from.

Every occurrence of "legacy" below is sense (1).

## Architecture

```
                         DCM / producer
                    ┌───────────┴───────────┐
                    ▼                       ▼
              legacy path              Transport (new)
                    │                       │
                    ▼                       ▼
          legacy SQL/ETL result    Delivery -> Raw -> prepared -> reporting
                    │                       │
                    └──────────┬────────────┘
                                ▼
                          correlate()
                                ▼
                          comparators
                                ▼
                    registry.migration_comparison
                                ▼
                     acceptance.evaluate_acceptance()
                                ▼
                       READY / NOT_READY
                                ▼
                   human edits migration.mode  (never automatic)
```

Nothing in the new platform's own pipeline (Transport, Delivery,
normalization, Raw ingestion, dbt builds) calls anything in
`reporting_platform/migration/`, and nothing in `migration_reconcile` writes
to the new platform's tables. A legacy outage makes today's comparisons come
back `NOT_COMPARABLE`; it cannot fail, delay, or invalidate an otherwise
valid new-platform ingest (section 24).

## Migration is Feed configuration

A `migration:` block on a Feed (`reporting_platform/common/context.py`,
`resolve_migration_config`), the same shape `delivery:`/`arrival:`/
`supersession:` already use: absent means the safe default, present is
validated at LOAD, and the resolved value is what every piece of generic
machinery reads.

```yaml
migration:
  mode: dual_run
  compare:
    checkpoint: raw       # raw | prepared | reporting
    table: null           # optional override; required for `reporting`
    key: [counterparty_id]
    columns: [rating, exposure, country]
    aggregates:
      - column: exposure
        function: sum
        tolerance: {absolute: 0.01}
  acceptance:
    consecutive_successes: 10
    allow_warnings: false
```

There is exactly ONE comparison engine and ONE orchestration DAG for every
Feed that sets `mode: dual_run`. Onboarding feed 501 is a YAML edit, never a
new DAG, sensor, or Python module -- see `test_migration_config.py` and
`airflow/dags/migration_reconcile.py`.

## Migration modes

| Mode | Meaning |
|---|---|
| `legacy` | Default. Legacy is authoritative; the new platform may have no Delivery at all for this Feed. No comparison runs. |
| `dual_run` | Both paths process the same logical deliveries. Legacy stays authoritative. `migration_reconcile` compares new output against legacy on the configured checkpoint. |
| `new_primary` | The new platform is accepted as primary. Legacy may still run and still gets compared (rollback/observation), but nothing here treats its output as ground truth any more. |

Mode is what changes PRODUCTION OWNERSHIP, and it never changes itself.
`acceptance.evaluate_acceptance()` can return `READY`; nothing in this
package writes `mode: new_primary` in response. That edit goes through the
same feeds.yml review process as every other Feed change (section 21-22).

### Rollback

`new_primary -> dual_run` is a config edit, not a data-model transition:
`migration.mode` is a plain string on the Feed, comparison evidence for the
Feed is untouched either way, and nothing about entering `new_primary`
deletes, archives, or disables the legacy adapter or its fixtures. A Feed can
move back after a bad cutover exactly as easily as it moved forward. Actual
legacy DECOMMISSIONING -- turning the legacy pipeline off, deleting its
code -- is explicitly Phase 9's, not this one's.

## Correlation: pairing the same logical delivery

`reporting_platform/migration/correlate.py`. Ranked strongest to weakest,
matching section 6 exactly:

1. **Shared producer/DCM execution identity** (`producer_run_id`) -- the
   strongest signal, when both sides can name the originating run.
2. **Original source content hash** (`source_sha256`) -- exact bytes, when
   both sides hashed the same algorithm.
3. **Original filename + source system** -- weaker, but concrete.
4. **Business date alone** -- the weakest, and refused by default
   (`allow_business_date_only=False`); a caller must opt in explicitly per
   Feed, which is itself worth recording as "this Feed's correlation is
   weak" rather than silently accepted.

**`_file_version`/`file_version` is never used as producer identity.** That
integer is platform ordering ("the 2nd delivery THIS SIDE saw for this COB
date"), assigned independently by each side; a legacy "version 2" and a new
"version 2" for the same date are not guaranteed to be the same delivery, and
treating them as such is exactly the bug section 6 calls out. The DeliveryID
(`dlv_...`, `ingest/delivery.py:delivery_id_for`) is likewise never assumed
equal to any legacy identifier -- the two identity schemes are deliberately
independent (`docs/DELIVERY-CONTRACT.md`).

Two sides that both name a producer run id or a content hash and DISAGREE are
refused outright rather than falling through to a weaker tier -- disagreement
is evidence of NON-correlation, not absence of evidence.

## Legacy adapter (section 25)

`reporting_platform/migration/legacy.py`. `LegacyResultSource` is the
interface; `LocalFixtureLegacySource` -- small JSON fixtures under a
configured directory -- is the only implementation this repo ships, because
this local/public environment cannot and should not approximate a real
enterprise SQL Server estate.

A production `SqlServerLegacySource` (not built here) implements the same
one method, `fetch(feed, business_date, checkpoint) -> LegacyResult | None`,
against a read-only legacy query instead of a file. It needs to supply, per
docs in `legacy.py`:

- `evidence` (`CorrelationEvidence`) built from whatever legacy load-control
  identity actually exists;
- `reference`, a human-investigable legacy identifier (a load/batch id, a
  query snapshot id) -- never a new identity scheme invented on the legacy
  estate's behalf;
- either `rows` (bounded, comparison-scale) or `aggregates` computed
  server-side (preferred at real volume).

Connection details belong to that future adapter's own environment
configuration, never hard-coded in this package -- see section 40's
constraint, reiterated here.

## Comparison checkpoints (section 8)

`compare.checkpoint` is one of `raw` (proves ingestion equivalence),
`prepared` (proves transformation equivalence), or `reporting` (proves
consumer-facing equivalence; needs an explicit `compare.table` because a
reporting mart is not named after any one Feed --
`docs/ARCHITECTURE.md`'s "Why `prepared` and `reporting` are both dbt"). A
Feed declares exactly the checkpoint(s) it needs; nothing requires all three.

## Comparison strategies (sections 9-10)

`reporting_platform/migration/comparators.py`, pure functions over
already-summarised data:

| Strategy | What it proves | What it misses alone |
|---|---|---|
| `compare_row_count` | Gross volume matches | Different rows, same count |
| `compare_key_set` | Same business keys on both sides | Values inside a matched key |
| `compare_row_hash` | Business VALUES match for matched keys | Nothing not covered by the columns named |
| `compare_aggregate` | A control total matches within an EXPLICIT tolerance | Row-level drift that cancels out in the sum |

Canonicalisation (`canonical_value`/`canonical_row_hash`) makes a value
hash identically regardless of source engine -- NULL is a distinct sentinel
from `""`, booleans are `true`/`false`, dates/timestamps are ISO-8601,
numbers go through a fixed-precision float format. This trades exact decimal
precision for cross-engine determinism; a comparison that needs exact
decimal equality uses `compare_aggregate`'s explicit tolerance instead.
Spark, not this module, does the actual aggregation at platform scale
(`new_side.py`) -- what lands here is the SUMMARY, never the underlying
rows, so Postgres and Airflow XCom never hold a dataset (sections 30/39).

`TECHNICAL_COLUMNS`/`drop_technical_columns` exclude `_cob_date`,
`_ingest_ts`, `_source_file`, `_file_version`, `_row_number`, `_batch_id`,
`_delivery_id`, `_received_at`, `_schema_version`, `_source_system` by
default -- a `columns:` block naming business values never needs to repeat
this, and a comparison that genuinely wants to check provenance names one of
these columns explicitly instead of relying on the default.

Tolerance is per-aggregate and explicit (`{absolute: ..., relative: ...}`);
there is no global fuzzy-tolerance switch anywhere in this package (section
9's explicit prohibition).

Business-specific reconciliation that does not reduce to
count/key-set/hash/aggregate belongs in a dbt test against the SAME
`prepared`/`reporting` tables this package reads, reusing Phase 7's
validation evidence (`registry.validation_result`) rather than a second
comparison engine (section 12).

## Comparison identity and contract versioning (sections 35-37)

`reporting_platform/migration/contract.py`:

- `comparison_id(feed, checkpoint, legacy_ref, new_ref, contract_hash)` is
  DETERMINISTIC -- a retried Airflow task or a repeated
  `migration_reconcile` pass reproduces the same id and
  `evidence.record` is `ON CONFLICT DO NOTHING`, exactly
  `registry/validation.py`'s idempotency shape. A corrected/restated
  Delivery is a genuinely different `new_ref`, hence a different id and its
  own row (section 27).
- `comparison_contract_hash(checkpoint, key, columns, aggregates)`
  snapshots what the comparison MEANT. Editing a Feed's `migration.compare`
  block changes this hash, so a later comparison against the same
  (feed, legacy_ref, new_ref) pair becomes a NEW logical comparison rather
  than retroactively reinterpreting an old PASS.

## Evidence (sections 13-14, 31)

`registry.migration_comparison` (`reporting_platform/registry/db.py`),
written by `reporting_platform/migration/evidence.py`. It sits BESIDE
`registry.validation_result` rather than inside it: a migration comparison
identifies a legacy reference that has no Delivery/Transport/dbt-node
identity, and forcing it through those columns would make
`validation_result`'s existing rows misleading. It reuses
`validation_result`'s exact outcome vocabulary (PASS/WARN/FAIL/ERROR) and its
append-only, deterministic-id idempotency shape rather than inventing either.

No FOREIGN KEY to `registry.delivery`, `registry.run`, or itself -- same
rebuildability reasoning as `run_input`/`validation_result`
(`registry/db.py`'s module header): this evidence must outlive a rebuild of
either table, and the legacy side has no row anywhere in this registry to
reference.

`registry.run` is reused, not duplicated, for the ONE case where a
comparison genuinely IS part of an existing run's story: a `prepared`/
`reporting` checkpoint comparison records `new_run_id` pointing at the
`registry.run` that built the checkpoint being compared. Raw has no
`registry.run` of its own (`docs/ARCHITECTURE.md`: only prepared/reporting
runs are recorded), so `new_run_id` stays NULL there and the DeliveryID
(`new_ref`) is the only new-side reference. No second "ValidationRun" /
"ComparisonRun" concept was introduced.

**There is no `WAITING_FOR_LEGACY`/`WAITING_FOR_NEW` row.** Section 18 asks
for the distinction, and it is made by NEVER PERSISTING that state at all:
`run.compare_business_date` returns `NOT_COMPARABLE` (a Python value, never
written to `registry.migration_comparison`) when either side has nothing
yet. A row in the evidence table means a comparison ACTUALLY EXECUTED. This
keeps `migration_comparison` an append-only fact table with no mutable
"current stage", matching `docs/DECISIONS.md`'s rule against workflow-state
tables.

## Difference artifacts (section 30)

`reporting_platform/migration/diffs.py`, written to
`migration-diffs/<feed>/<comparison-id>/` in the same bucket
`registry/artifacts.py` already uses for dbt artifacts, ONLY when a
comparison is not a clean PASS. Contents are diagnostic SAMPLES
(`legacy-only.json`, `new-only.json`, `changed.json`, each capped), never a
full difference dump -- `registry.migration_comparison.diff_ref` points at
the prefix, `summary` on the same row carries the counts. A clean PASS writes
no artifact and leaves `diff_ref` NULL.

## Validation integration (section 31)

Outcome vocabulary (PASS/WARN/FAIL/ERROR) and the FAIL-vs-ERROR distinction
are `docs/VALIDATION.md`'s, reused verbatim rather than redefined:
`ERROR` means the comparator itself could not execute (a legacy query
failed, a Spark read raised); `FAIL` means both sides were read successfully
and materially differed. `NOT_COMPARABLE` is Phase 8's own addition to this
vocabulary at the ORCHESTRATION layer (never persisted, see above) -- it is
not a fifth outcome on the evidence row.

## Acceptance (sections 19-22, 37)

`reporting_platform/migration/acceptance.py`. `evaluate_acceptance(feed)`
reads the Feed's own `migration.acceptance` policy plus its comparison
history and returns `READY`/`NOT_READY`/`NOT_STARTED` with reasons -- it
writes nothing, ever.

**Restatement semantics (section 27), resolved explicitly:** when a business
date has more than one comparison row (an original delivery, then a
correction), acceptance uses the LATEST-EXECUTED row for that date. The
earlier row is not deleted or hidden -- both remain queryable forever in
`registry.migration_comparison` -- but for the purpose of "is this Feed
ready", the corrected delivery's outcome is what counts. This is a choice,
not the only reasonable one: an operator wanting "every attempt must have
passed" is asking a different, valid question this function does not answer.

**Streak semantics:** walking from the most recent business date backwards,
an unbroken run of successes (PASS, or WARN when `allow_warnings` is set)
counts toward `consecutive_successes`; a FAIL or an ERROR both break the
streak -- an ERROR is treated as inconclusive rather than a free pass,
because readiness requires actual passing evidence, not merely the absence
of a recorded failure.

**Policy is read live, not baked into evidence rows** (section 37): raising
`consecutive_successes` from 10 to 20 changes today's verdict immediately
without touching a single historical `registry.migration_comparison` row.

## SCD2 handling (section 28)

Comparing an SCD2 dimension (`prepared.ref_counterparty`/`ref_rating`, see
`docs/ARCHITECTURE.md`'s "Slowly-changing dimensions in `prepared`") by raw
physical row count is wrong by construction: a restatement legitimately adds
history rows, closes a version, or reopens one without the CURRENT business
state having changed at all. This package does not redesign SCD2 semantics
(explicitly out of scope, section 44) -- it compares the BUSINESS state SCD2
already exposes:

- `key` names the SCD2 business key (e.g. `counterparty_id, agency` for
  `ref_rating` -- see `docs/ARCHITECTURE.md`'s "grain is per business key,
  not per table");
- `columns` names business-value columns, never `effective_from`/
  `effective_to`/`dbt_invocation_id` unless a comparison genuinely wants to
  assert provenance itself;
- comparing at a `prepared` checkpoint on a specific `business_date` reads
  the table AS OF that date implicitly through the row filter Spark applies
  (`new_side.py`'s `cob_date`/`business_date` predicate resolves to whichever
  version was current then, since a raw row-count/key-set read against an
  SCD2 table is inherently reading its CURRENT rows on that build) -- so a
  restatement that changes history but not today's current state produces
  identical row hashes for today, which is correct: the business answer for
  today did not change.

A comparison that specifically needs to assert history shape (e.g. "this
restatement correctly retracted the prior version") is a dbt test against
`prepared`, per section 12 -- not something this generic engine invents a
special case for.

## Cutover remains explicit (sections 21-22)

`evaluate_acceptance() == READY` is a fact about evidence. Changing
`migration.mode` is a fact about ownership, and this package never performs
that edit. The intended human flow:

```
comparison evidence -> READY -> human review -> feeds.yml PR -> new_primary
```

## Airflow orchestration (section 38)

`airflow/dags/migration_reconcile.py` -- ONE generic DAG, independent of
`transport_ingest`:

```
discover_candidates()          -- (feed, business_date) pairs, bounded lookback
        │
        ▼ dynamic task mapping (.expand), one mapped task definition
compare_one(pair)               -- scripts._spark_task migration-compare
        │
        ▼
registry.migration_comparison row
```

`discover_candidates` filters to Feeds in `dual_run`/`new_primary` and a
bounded lookback window (`MIGRATION_RECONCILE_LOOKBACK_DAYS`, default 14) --
never a full historical rescan on every tick (section 39). Each mapped task
invocation is one Spark subprocess
(`scripts/_spark_task.py migration-compare`), the same
subprocess-per-Spark-call rule every other Spark caller in this platform
follows (`docs/DECISIONS.md#spark-in-a-subprocess`) and runs on the
`lakehouse_write` pool for the same single-slot-serialises-Spark reason
`dbt_builds.py`'s tasks do. XCom carries only the small JSON
`evidence.record` already wrote to Postgres -- IDs, counts, an outcome
string -- never row-level data.

`transport_ingest` calls nothing here and is called by nothing here.
Comparison latency is entirely this DAG's own schedule (hourly, coarser than
the ingestion path on purpose -- assurance, not the pipeline).

## Scalability (section 39)

No per-Feed DAG, no per-Feed sensor: `migration_reconcile` is the ONLY DAG
this phase adds, and it fans out over `discover_candidates()`'s bounded
result with dynamic task mapping -- the same pattern `transport_reconcile`
already uses for its per-Feed Raw check. Hundreds of Feeds means hundreds of
mapped task instances on one DAG definition, not hundreds of DAG files.

## Operational CLI (sections 32-33)

```
docker compose exec -T airflow python -m reporting_platform.migration overview
docker compose exec -T airflow python -m reporting_platform.migration status <feed>
docker compose exec -T airflow python -m reporting_platform.migration compare show <comparison-id>
docker compose exec -T airflow python -m reporting_platform.migration compare recent <feed>
```

Same shape as `reporting_platform.registry`'s CLI: no Spark, JSON output.

## DCM ownership during migration (section 23)

DCM remains authoritative for source-side acquisition/completion on BOTH
paths during dual-run -- Phase 8 does not touch DCM scheduling, gates, or
follow-on-job semantics, exactly as `docs/AIRFLOW-ORCHESTRATION.md` already
states for the Transport path alone. A Feed entering `dual_run` needs no DCM
change: DCM continues writing whatever it already writes for both the legacy
delivery and the S3 Transport handoff.

## Non-goals (section 44)

Not built in Phase 8, and not attempted: migrating every enterprise Feed;
automatic legacy shutdown; deleting legacy ingestion code; DCM replacement;
migrating DCM's remaining gates/schedules; OpenMetadata; historical Delivery
backfill; value-level lineage; SCD2/supersession/retention redesign; a
migration web application; per-feed DAGs/sensors; automatic cutover; a large
comparison DSL; enterprise credentials of any kind. Legacy ingestion cleanup
after migration is proven is Phase 9's.
