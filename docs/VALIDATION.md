# Validation controls and execution evidence

Phase 7. What this platform checks, where each check runs, and where the
durable record of what it found lives. Read `docs/ARCHITECTURE.md` first for
the pipeline shape and `docs/REGISTRY.md` for the tables this document adds
to.

**This is not a validation engine.** Every control described below already
ran somewhere before Phase 7 -- in RPL (`reporting_platform/ingest/`), in
Spark (`ingest_feed.py`), or as a dbt test. Phase 7 adds one thing: a durable,
queryable record of what each control observed, next to the controls
themselves, in `registry.validation_result` (`reporting_platform/registry/
validation.py`). It does not introduce a second place any of them are
*defined*.

## The three layers

```
Feed configuration (feeds.yml)
       |
       +-----------------------------+
       v                             v
   Delivery                      dbt tests
  (Transport/Delivery             (schema.yml
   contract checks)                data_tests)
       |                             |
       v                             v
   Raw ingestion                 dbt build/test
  (schema/row-count/                 |
   checksum checks)                  |
       |                             |
       +--------------+--------------+
                       v
             registry.validation_result
                       |
                       v
                    registry
```

| Layer      | What it concerns                                   | Where it runs                                          | Owner  |
|------------|-----------------------------------------------------|----------------------------------------------------------|--------|
| `delivery` | What arrived -- Transport contract, business identity, feed resolution | `reporting_platform/ingest/transport.py`, `delivery.py`, `normalization.py` | RPL |
| `raw`      | Whether a Delivery can validly become Raw data -- schema drift, row-count floor/ceiling, producer checksum | `reporting_platform/ingest/ingest_feed.py` (Spark) | RPL/Spark |
| `dbt`      | The data model -- `not_null`, `unique`, `accepted_values`, `relationships`, cross-feed reconciliations | `dbt/models/**/*.yml` `data_tests`, run by Cosmos | dbt |

Each layer keeps its own execution mechanism. Phase 7 does not move a
Delivery/Raw check into dbt, and does not reimplement a dbt test in Python --
see `docs/DECISIONS.md#validation-evidence-is-append-only` for why one
evidence table serves all three without becoming a fourth engine.

## Rules versus results

A **rule** (or expectation) is configuration: `expected_min_rows` in
`feeds.yml`, a `data_tests:` block in `schema.yml`, the `TransportEvidenceError`
check in `transport.py`. A **result** is what happened the one time it ran
against one Delivery or one build. This document, and `validation_result`,
are about results. Rules are read from their own files, same as before Phase
7 -- there is no shadow copy of a dbt test's definition anywhere in the
registry.

### Producer assertions versus platform expectations versus platform observations

Three different numbers can all be "the row count", and Phase 7 keeps them
apart rather than collapsing them into one row-count check:

* **Producer assertion** -- `declared_row_count`, read from the sender's
  control file (`delivery.control.row_count`). What the SENDER counted.
* **Platform expectation** -- `expected_min_rows` / `expected_max_rows`, a
  Feed-level floor/ceiling declared in `feeds.yml`. What THIS PLATFORM expects
  of the feed as a whole, independent of any one delivery.
* **Platform observation** -- the row count Spark actually read off the
  landed parts. What HAPPENED.

`declared_row_count` is checked for *equality* against the observation (a
sender who says 4,521,847 and gets 4,521,846 back has a real discrepancy, not
a threshold breach); `expected_min_rows`/`expected_max_rows` are checked as a
*range* (a floor/ceiling the platform sets independent of what any one sender
declares). The same distinction applies to checksums: `declared_md5` is the
producer's assertion, compared against the observed hash of the landed bytes.
Never conflate a producer's claim with the platform's own policy -- a producer
that stops sending a control file at all should not silently widen the
platform's own floor.

**Historical validation uses the SNAPSHOTTED expectation, not today's
config.** `expected_min_rows`, `expected_max_rows` and `schema_drift` are
captured into the immutable `normalization_contract`
(`reporting_platform/ingest/delivery.py:normalization_contract`,
`ingest/normalization.py`) at the same point the rest of a Delivery's parsing
contract is frozen, exactly the pattern Phases 2-3 already established for
`columns`/`source_columns`/`format`. A later edit to `feeds.yml` changing the
floor does not retroactively reinterpret whether an already-committed
Delivery passed.

## Outcomes

| Outcome | Meaning |
|---------|---------|
| `PASS`  | The control executed and the expectation was satisfied. |
| `WARN`  | The control executed and breached a warning threshold, but publication policy permits continuation. |
| `FAIL`  | The control executed and breached a **blocking** expectation. |
| `ERROR` | The control itself could not execute (a bad delivery is data failing a check; an ERROR is the check failing to run at all). |

`severity` (`blocking` / `warn` / `info`) is a property of the CONFIGURATION
-- what a breach is *allowed* to do to the pipeline -- independent of
`outcome`, which is what happened this one time. A `schema_drift: warn` feed
that drifts records `severity=warn, outcome=WARN`; the same drift on a
`schema_drift: fail` feed records `severity=blocking, outcome=FAIL` and
aborts the branch. Integrity invariants (Transport SHA-256, the declared
checksum) are always `severity=blocking` -- there is no configuration knob
that turns a checksum mismatch into a warning, on purpose (Phase 7 brief
§12: "Do not make hard Transport-integrity failures configurable warnings").

dbt's own `error`/`warn` `config.severity` is mapped explicitly (`registry/
validation.py:_dbt_node_context`) onto this platform's `blocking`/`warn`
vocabulary rather than read through verbatim, and dbt's run-results
`status` (`pass`/`warn`/`fail`/`error`/`skipped`) is mapped explicitly onto
the four outcomes above -- `skipped` is omitted entirely (the test did not
execute, and none of the four outcomes claims otherwise).

## Delivery-layer evidence

Delivery/Transport checks record durable evidence **only on failure**
(`airflow/dags/transport_ingest.py:_record_delivery_failure`, called from
`validate_transport`, `create_delivery`, `normalize_delivery`). A success is
already proven by the DeliveryManifest/NormalizationManifest that exists as a
result -- adding a PASS row beside it would say nothing that evidence does
not already say (Phase 7 brief §8: "the immutable Transport and
DeliveryManifest already prove many successful assertions... a registry row
may add little").

A failure is recorded before the exception propagates, keyed by
`transport_id` (never a fake `registry.delivery` row -- see
`docs/DECISIONS.md#failed-delivery-validation-has-no-fake-manifest`):

```
docker compose exec -T airflow python -m reporting_platform.registry validation transport <transport-id>
```

`outcome` is `FAIL` for a known validation exception (`TransportContractError`
and subclasses, `DeliveryError` and subclasses) and `ERROR` for anything
else -- the control ran and found a real problem versus the control itself
breaking.

## Raw ingestion evidence

Every Raw control in `ingest_feed.py:_ingest_manifest` records **both PASS
and FAIL** next to the check itself, tied to `delivery_id`, before it raises
(schema drift never blocks the durable record even when it blocks the
branch):

* `schema_drift` -- missing/extra columns against the declared contract.
* `expected_min_rows` -- the floor.
* `expected_max_rows` -- the ceiling (Phase 7; `None` means no ceiling
  configured, and no row is written for a feed that declares none).
* `declared_row_count` -- the producer's assertion, checked only when the
  delivery's control file declared one.
* `declared_md5` -- the producer's checksum, checked only when declared.

```
docker compose exec -T airflow python -m reporting_platform.registry validation delivery <delivery-id> [--feed F]
```

Writes are best-effort (`validation.record_quietly`): a registry outage never
turns an already-decided PASS into a failed ingest, or blocks a FAIL from
aborting the branch. The check itself is unaffected either way -- this is
evidence about a decision already made, not a gate of its own.

## dbt integration

dbt test results are captured from the **machine-readable artifacts dbt
itself wrote** (`manifest.json`, `run_results.json`), never from logs. Phase
5 already archives both, immutably, to `s3://.../dbt-artifacts/<run_id>/
<task_id>/attempt-<n>/` from `airflow/dags/dbt_builds.py`'s
`_archive_dbt_artifacts` callback, which runs on **both**
`on_success_callback` and `on_failure_callback` -- so a failing test task's
artifacts are retained exactly like a passing one, before publication is
even decided.

Phase 7 adds one call in that same callback,
`registry.validation.capture_dbt_task`, which parses the just-archived local
`run_results.json`/`manifest.json` (before the next Cosmos task overwrites
them) into one `validation_result` row per **test** node
(`unique_id` starting `test.`; model-run rows are not validation evidence).
Each row keeps:

* the dbt `unique_id` verbatim, as `control_id` -- so a later OpenMetadata
  ingestion of the same artifacts can join on it without this registry
  inventing its own identifier space;
* `model_name`/`column_name`, resolved from `manifest.json`'s `depends_on`
  and `column_name`;
* `run_id` -- the SAME `registry.run.run_id` `dbt_builds.py` already opens in
  `open_branch`, never a second "ValidationRun" concept;
* `evidence_ref` -- the `s3://.../dbt-artifacts/<run_id>/` prefix, the
  authoritative artifact this row is a projection of.

```
docker compose exec -T airflow python -m reporting_platform.registry validation run <run-id>
```

**Failed dbt runs keep their evidence.** Capture happens in the SAME
callback that archives the artifacts, regardless of whether the task
succeeded or failed, and regardless of whether the run goes on to publish.
A `dbt test` failure still blocks `publish` from merging (unchanged
write-audit-publish), but the FAIL rows for exactly which tests broke, on
which model/column, with how many failures, are queryable by `run_id`
whether or not `main` ever moved.

## Multi-feed dbt tests and `run_input`

A dbt test does not carry a `delivery_id` unless it genuinely runs against
one Delivery's data -- a cross-feed reconciliation or an aggregate test has
no single Delivery to name, and Phase 7 does not invent one (never a
comma-joined list, never the first contributing Delivery standing in for
all of them). The contributing Deliveries stay reachable exactly the way
Phase 5 already made them reachable for the run as a whole:

```
validation_result --(run_id)--> registry.run --(run_input)--> (feed, delivery_id)*
```

`registry validation run <run-id>` plus `registry inputs --run-id <run-id>`
together answer "which tests ran, and which Deliveries fed the run they ran
in" without a validation row ever naming more than one Delivery.

## Idempotency and retry

`validation_id` is a deterministic hash of `(layer, control_id,
attempt_key)`, and every write is `INSERT ... ON CONFLICT (validation_id) DO
NOTHING`:

* An Airflow task retry within the SAME dag run reuses the same `run_id`
  (Delivery layer) or the same `(run_id, task_id, try_number)` (dbt layer, where
  `try_number` changes only on a genuine retry) -- so retrying the identical
  execution writes the identical row once, never twice.
* A genuinely later, independent execution (a new Airflow run id, a new
  `registry.run`) gets a different `attempt_key` and therefore its own row --
  history accumulates rather than being overwritten. Live-verified: two
  independent triggers of `transport_ingest` for the same TransportID
  (`transport_watch`'s fast path and a manual replay, twelve seconds apart)
  produced two distinct `delivery_identity` FAIL rows, correctly, because
  they were two distinct Airflow runs.

## What this is not

* Not a replacement for `registry.delivery`, which stays observation-only --
  no `outcome`/`status` column was added to it. See `docs/REGISTRY.md`.
* Not the ingestion state machine: raw commit/no-commit is still decided by
  the `raise` inside `ingest_feed.py`, exactly as before Phase 7.
  `validation_result` records the decision; it does not make it.
* Not an OpenMetadata integration. `control_id` preserving dbt's `unique_id`
  and `evidence_ref` pointing at the untouched dbt artifacts are both
  deliberate preparation for one, not a dependency on one -- see
  `docs/DECISIONS.md`'s Phase 7 entries.
* Not a generic Python validation DSL. There is no YAML rule list and no
  engine that executes it; every control this document describes is code
  that already existed, in the language its layer already used.

## Retention

`validation_result` rows must outlive the run/Delivery they describe, so a
retention sweep must never remove a row whose `run_id`/`delivery_id` is
still referenced elsewhere. This phase does not add a retention policy of its
own -- see `docs/DECISIONS.md#published-tags-are-the-reproducibility-window`
for the existing dbt-artifact and Delivery-evidence retention obligations
this table now sits beside. `dbt-artifacts/` objects a `validation_result`
row's `evidence_ref` points at are retained by the same rule Phase 5 already
applies (kept at least as long as the `registry.run` that references them);
`validation_result` itself is not yet swept by anything, which is the safe
default until a retention policy is written for it deliberately.
