"""The delivery registry's transactional store: Postgres, in the `platform` DB.

WHY POSTGRES AND NOT ICEBERG. The registry needs one thing Iceberg cannot give
cheaply: a serialising authority. `sequence_no` is the order this platform saw
deliveries in, and an order allocated by `MAX(...)+1` over a table several
writers append to is the same read-then-write `next_file_version` has --
correct today only because the `lakehouse_write` pool has one slot, which is
exactly what the concurrency work intends to change.

WHY THIS IS NOT THE `stg` LOAD-CONTROL TABLE. That is the trap this platform
refuses by name -- see `arrival.already_ingested` -- and the difference is not
the technology but what the table may say:

  * The registry records OBSERVATIONS about deliveries: what arrived, when, how
    big, what it hashed to, what it was called upstream. Each is a fact about
    an object that already exists in `landing/`.
  * It records NO VERDICTS. No `ingested`, no `superseded`, no `status`.
    Whether a delivery reached raw stays derived from `_source_file`; whether
    it supersedes another stays `dedupe_rank`'s answer, computed at read time.

So every row here is reconstructible from object storage, and
`deliveries.reconcile()` is the function that does it. A registry that could
not be rebuilt would be a second source of truth; one that can is an index.

WHAT A REBUILD DOES NOT PRESERVE: the exact integers in `sequence_no`. Rebuilt
rows are inserted in `received_at` order, so the ORDER is reproduced and the
values are not. Nothing may key on the value.

RUNS, VERSIONS, SUBMISSIONS AND AS-AT TRANSITIONS ARE THE EXCEPTION, and the
only one. A RUN is not an observation about an object: it is an event that
happened once, from a particular commit, and no object records that it
happened. Neither is the version a report was published under, nor that
somebody submitted it, nor that somebody locked or reopened an as-at date --
REQ-500's lifecycle is a sequence of human acts.

Two things follow that are easy to get wrong:

  * `run_input` carries NO FOREIGN KEY to `delivery`. It looks like it should,
    but a foreign key would let a registry rebuild CASCADE run history away --
    destroying the only copy of something to protect a table that has a second
    copy in object storage. `monitoring/evidence.py` treats a delivery it
    cannot find as a finding rather than an impossibility.
  * A run legitimately HAS A STATUS, where a delivery does not. A delivery's
    status would be a verdict about something already true, derivable and
    therefore driftable. A run's status is how the run ended, which nothing
    else knows and nothing can derive.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager

from reporting_platform.common import settings

log = logging.getLogger("registry.db")

# One process ensures the schema once. Not a correctness mechanism -- the DDL
# is idempotent -- just a way of not issuing six CREATEs per reconcile.
_ensured = False


def dsn() -> str:
    """The registry connection string, or a refusal. See `settings.registry_dsn`."""
    return settings.registry_dsn()


@contextmanager
def connect(ensure: bool = True):
    """A registry connection, committed on success and rolled back on error.

    `ensure=False` skips the schema check, which is what `ensure_schema`
    itself uses and what a caller in a tight loop passes once it has already
    connected successfully.
    """
    import psycopg2

    conn = psycopg2.connect(dsn())
    try:
        if ensure:
            ensure_schema(conn)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


SCHEMA = """
CREATE SCHEMA IF NOT EXISTS registry;

-- One row per DELIVERY: a set of producer bytes accepted for one feed and one
-- COB date. Legacy rows identify the conformant landing filename (`_v2` is a
-- distinct delivery); Phase 3 rows use the transport-derived `dlv_...` ID.
-- Both are stable within their own evidence contract, so the additive natural
-- key remains (feed, delivery_id).
CREATE TABLE IF NOT EXISTS registry.delivery (
    feed               TEXT        NOT NULL,
    delivery_id        TEXT        NOT NULL,
    -- Allocated by the database. See the module header on what a rebuild
    -- does and does not preserve.
    sequence_no        BIGSERIAL   NOT NULL UNIQUE,
    source_system      TEXT        NOT NULL,
    cob_date      DATE        NOT NULL,
    -- The DELIVERY's durable arrival time -- legacy Landing LastModified or
    -- v2 Transport uploaded_at -- not the moment this row was written.
    received_at        TIMESTAMPTZ NOT NULL,
    -- The moment the registry first saw it. The gap between the two is how
    -- far behind the registry was running, and it is the only clock in this
    -- table that belongs to the registry rather than to the delivery.
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_object      TEXT        NOT NULL,
    manifest_key       TEXT,
    normalizer         TEXT        NOT NULL,
    bytes              BIGINT      NOT NULL,
    md5                TEXT        NOT NULL,
    -- The DECLARED COLUMN CONTRACT this delivery was read against, not the
    -- shape of the file. See `Feed.schema_version`.
    schema_version     TEXT        NOT NULL,
    -- 'inbox' for a delivery the conformance gate renamed and promoted,
    -- 'direct' for one an approved sender wrote into landing/ itself.
    origin             TEXT        NOT NULL,
    origin_uri         TEXT,
    -- What the upstream called it. Differs from delivery_id for exactly the
    -- feeds that go through the gate, and that difference is the reason the
    -- sidecar exists at all.
    source_filename    TEXT,
    source_container   TEXT,
    control_object     TEXT,
    declared_row_count BIGINT,
    declared_md5       TEXT,
    producer_run_id    TEXT,
    PRIMARY KEY (feed, delivery_id)
);

CREATE INDEX IF NOT EXISTS delivery_feed_date
    ON registry.delivery (feed, cob_date);
CREATE INDEX IF NOT EXISTS delivery_cob_date
    ON registry.delivery (cob_date);

-- The objects that actually hold the rows. One for a plain CSV (pointing back
-- into landing/), N for an archive (pointing into ready/). Separate from the
-- delivery because `_source_file` in the raw table is the PART's key, so this
-- is the table that joins raw rows back to their delivery.
CREATE TABLE IF NOT EXISTS registry.delivery_part (
    feed        TEXT   NOT NULL,
    delivery_id TEXT   NOT NULL,
    part_no     INT    NOT NULL,
    object_key  TEXT   NOT NULL,
    bytes       BIGINT,
    PRIMARY KEY (feed, delivery_id, part_no),
    FOREIGN KEY (feed, delivery_id)
        REFERENCES registry.delivery (feed, delivery_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS delivery_part_object
    ON registry.delivery_part (object_key);

-- Phase 3 NormalizationManifest v2 parts. These are deliberately not written
-- to delivery_part: that legacy table is coupled to legacy Ready v1 semantics,
-- while this table describes the v2 rebuildable normalization plan. Phase 4
-- writes these physical keys to Raw without collapsing the two evidence kinds.
-- `materialized=false` is the plain-file pass-through into received/;
-- `materialized=true` is a rebuildable object extracted below ready/.
CREATE TABLE IF NOT EXISTS registry.normalization_part (
    feed          TEXT    NOT NULL,
    delivery_id   TEXT    NOT NULL,
    part_no       INT     NOT NULL,
    object_key    TEXT    NOT NULL,
    bytes         BIGINT,
    source_member TEXT,
    materialized  BOOLEAN NOT NULL,
    PRIMARY KEY (feed, delivery_id, part_no),
    FOREIGN KEY (feed, delivery_id)
        REFERENCES registry.delivery (feed, delivery_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS normalization_part_object
    ON registry.normalization_part (object_key);

-- REQ-106. A delivery that arrived and could not be accepted is evidence too,
-- and until now it was a file moved into a folder on one container's bind
-- mount with nothing recording why. The bytes go to `quarantine/` in object
-- storage; this says what they were and what was wrong with them.
--
-- Keyed on the quarantine object, not on the filename: the same upstream name
-- can be rejected many times, and each attempt is its own event.
CREATE TABLE IF NOT EXISTS registry.rejection (
    quarantine_key  TEXT        PRIMARY KEY,
    -- NULL when the file matched no feed at all, which is a rejection the
    -- registry can still describe.
    feed            TEXT,
    source_filename TEXT        NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 'unroutable', 'ambiguous', 'identity' or 'member' -- the CLASS of
    -- failure, so the common ones can be counted without parsing prose.
    reason_class    TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    bytes           BIGINT      NOT NULL,
    md5             TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS rejection_feed_time
    ON registry.rejection (feed, rejected_at);
-- `rejections.recent` orders by arrival, not by refusal, so that the arrivals
-- view can merge it with `deliveries.recent` on one field. Its own index.
CREATE INDEX IF NOT EXISTS rejection_feed_received
    ON registry.rejection (feed, received_at);
-- ------------------------------------------------------------------- runs
-- REQ-400/REQ-401. One row per BUILD RUN that reached the point of writing
-- something: which branch it built on, what it merged, which code and which
-- dbt project produced it, and how it ended.
--
-- `purpose` is 'prepared' or 'reporting', matching the two build DAGs. Only a
-- reporting run publishes reports; a prepared run is recorded anyway, because
-- "which prepared build produced the tables this report read" is exactly the
-- kind of question that is unanswerable after the fact if nothing wrote it
-- down at the time.
CREATE TABLE IF NOT EXISTS registry.run (
    run_id            TEXT        PRIMARY KEY,
    purpose           TEXT        NOT NULL,
    -- 'running', 'published' or 'failed'. Mutable, unlike anything in the
    -- delivery tables -- see the module header on why that is not the same
    -- boundary being relaxed.
    status            TEXT        NOT NULL,
    environment       TEXT        NOT NULL,
    -- The Airflow dag/run this came from, so a row here leads back to logs.
    dag_id            TEXT,
    airflow_run_id    TEXT,
    branch            TEXT        NOT NULL,
    merged_hash       TEXT,
    -- The COB date the run PUBLISHED: the maximum COB date present
    -- in what it built. Null until it publishes, because until then nothing
    -- has been read to establish it.
    cob_date     DATE,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    -- REQ-404. What code produced this. `code_ref_kind` says whether the
    -- value is a deployed identity or a content digest of the mounted tree --
    -- see context.code_ref(), and never conflate the two.
    code_ref          TEXT        NOT NULL,
    code_ref_kind     TEXT        NOT NULL,
    dbt_manifest_ref  TEXT        NOT NULL,
    -- Object-store prefix containing each Cosmos/dbt task's manifest.json and
    -- run_results.json (plus catalog.json when generated).  Separate from the
    -- project digest above: one identifies model source, the other preserves
    -- the actual per-invocation artifacts.
    dbt_artifacts_ref TEXT,
    -- REQ-405. The change this ONE PUBLICATION was made under, as supplied by
    -- whoever triggered it -- a restatement, a backfill, an out-of-cycle
    -- rerun. Also written onto the Nessie merge commit.
    --
    -- NOT the standing authorisation for the code that ran: that is
    -- `deployment_change_ref` below and it arrives from the environment, not
    -- from the trigger. Merging the two would make a scheduled run either
    -- record no change at all or carry a re-typed one, and a re-typed
    -- identifier is an unverified one.
    change_ref        TEXT,
    -- REQ-406. DEPLOYMENT provenance: what the pipeline knew and a run cannot
    -- work out. Constant across every run of a deployed version, which is the
    -- whole point -- one change authorises a version, and hundreds of runs
    -- inherit it. Null where the deployment supplied nothing, which is every
    -- developer machine.
    dbt_project_ref         TEXT,
    deployment_change_ref   TEXT,
    deployment_pipeline_ref TEXT,
    error             TEXT
);

CREATE INDEX IF NOT EXISTS run_purpose_started
    ON registry.run (purpose, started_at DESC);

-- REQ-400, and the half that closes REQ-602. The deliveries whose rows are
-- PRESENT in what the run published -- DERIVED from what it built rather than
-- declared: the publish step selects the distinct delivery ids out of the
-- models on the branch, which is the same `delivery_ref()` the rows carry. A
-- declared input set would be a second statement of something the data
-- already says, and the two would disagree the first time a model changed
-- which sources it reads.
--
-- NOT "every delivery the run scanned". An SCD2 model keeps one row per
-- version, so a delivery that restated an unchanged entity is not in here.
-- That is the right set for reproducing the published tables and the wrong
-- one for auditing what was read; see registry/inputs.py.
CREATE TABLE IF NOT EXISTS registry.run_input (
    run_id      TEXT NOT NULL REFERENCES registry.run (run_id) ON DELETE CASCADE,
    feed        TEXT NOT NULL,
    -- NO FOREIGN KEY to registry.delivery, deliberately. See the module
    -- header: a registry rebuild must not be able to cascade run history away.
    delivery_id TEXT NOT NULL,
    PRIMARY KEY (run_id, feed, delivery_id)
);

CREATE INDEX IF NOT EXISTS run_input_delivery
    ON registry.run_input (feed, delivery_id);

-- REQ-401/REQ-403. The version a report was published at, and the pin that
-- makes it addressable.
--
-- VERSION IS PER (REPORT, AS-AT DATE) -- settled decision 5. A version number
-- says "this is the Nth answer we have given for this report and this date",
-- which is a question about one report: numbering per run would move a
-- report's version when an unrelated report was rebuilt, and numbering per
-- family would move it when a sibling was restated. Reports submitted
-- together are grouped on the SUBMISSION instead, which is where that
-- relationship actually lives.
--
-- Allocated by Postgres, in a transaction, against the unique constraint --
-- the same serialising authority `sequence_no` needed, and for the same
-- reason: MAX(...)+1 from several writers is a read-then-write.
CREATE TABLE IF NOT EXISTS registry.report_version (
    report        TEXT        NOT NULL,
    as_at_date    DATE        NOT NULL,
    version_no    INT         NOT NULL,
    run_id        TEXT        NOT NULL REFERENCES registry.run (run_id),
    tag           TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (report, as_at_date, version_no)
);

CREATE INDEX IF NOT EXISTS report_version_run ON registry.report_version (run_id);
CREATE UNIQUE INDEX IF NOT EXISTS report_version_tag
    ON registry.report_version (tag);

-- REQ-402. That a version was SENT somewhere, which is a different event from
-- publishing it and frequently happens later, to several reports at once.
--
-- This platform submits nothing: there is no transport here and this table
-- does not pretend otherwise. It is the record that a submission was made,
-- written by whoever made it, and it exists now because the alternative is
-- reconstructing it later from email.
CREATE TABLE IF NOT EXISTS registry.submission (
    submission_id TEXT        PRIMARY KEY,
    -- The FAMILY grouping, decision 5's other half: reports submitted
    -- together as one return. Free text and nullable -- a single-report
    -- submission has no family and inventing one would be noise.
    family        TEXT,
    destination   TEXT        NOT NULL,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_by  TEXT        NOT NULL,
    note          TEXT
);

-- REQ-500..503. The LIFECYCLE of one (report, as-at date): whether it is still
-- open to routine publication, closed, sent, or deliberately reopened.
--
-- APPEND-ONLY, AND THE CURRENT STATE IS THE NEWEST ROW. A mutable
-- current-state row would answer "is this locked" and destroy "who reopened
-- it, when, and why" every time it was updated -- and that history is the
-- entire subject here. This is a third table in the family a rebuild cannot
-- reconstruct (see the module header): a lock is an act somebody performed,
-- and no object in storage records that it happened.
--
-- NO ROW MEANS `open`. An as-at date that nobody has locked is open, so the
-- ordinary case costs no row and a report's first publication needs no
-- lifecycle set-up. `lifecycle.state()` reads the absence as the state.
--
-- `actor` AND `reason` ARE NOT NULL, and that is the authority model, stated
-- honestly: this platform has no identity provider, so it cannot enforce WHO
-- may lock or reopen a date. What it can do is refuse a transition that does
-- not say who made it and why, and check a reopening approver against the
-- report's declared owner -- which is derivable from the dbt exposure and is
-- therefore a real check rather than a log line. See registry/lifecycle.py.
CREATE TABLE IF NOT EXISTS registry.as_at_transition (
    transition_id BIGSERIAL   PRIMARY KEY,
    report        TEXT        NOT NULL,
    as_at_date    DATE        NOT NULL,
    -- 'locked', 'submitted' or 'reopened'. There is no 'open' row: open is
    -- the absence of any row, so nothing has to be written to create a date.
    state         TEXT        NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor         TEXT        NOT NULL,
    reason        TEXT        NOT NULL,
    -- Only on a reopening of a SUBMITTED date, where it must match the
    -- report's exposure owner. Null everywhere else, because requiring an
    -- approver for an ordinary lock would make the one place it matters
    -- indistinguishable from routine.
    approved_by   TEXT,
    -- The submission this state came from, when it came from one. Set by
    -- record_submission so that submitting is not a second thing to remember.
    submission_id TEXT
);

CREATE INDEX IF NOT EXISTS as_at_transition_current
    ON registry.as_at_transition (report, as_at_date, occurred_at DESC);

CREATE TABLE IF NOT EXISTS registry.submission_item (
    submission_id TEXT NOT NULL
        REFERENCES registry.submission (submission_id) ON DELETE CASCADE,
    report        TEXT NOT NULL,
    as_at_date    DATE NOT NULL,
    version_no    INT  NOT NULL,
    PRIMARY KEY (submission_id, report, as_at_date),
    FOREIGN KEY (report, as_at_date, version_no)
        REFERENCES registry.report_version (report, as_at_date, version_no)
);

-- Phase 7. Durable, queryable evidence that a validation CONTROL executed and
-- what it observed -- not a second ingestion ledger and not a verdict on
-- `registry.delivery` (see the module header: that table stays observation,
-- no status column). One row per (logical execution of one control).
--
-- THREE LAYERS share this one table rather than three: `layer` says which of
-- Delivery/Raw/dbt produced the row, and the columns that do not apply to a
-- given layer stay NULL rather than becoming three schemas that drift.
--
-- NO FOREIGN KEY to `registry.run` or `registry.delivery`, deliberately, for
-- the same reason `run_input` has none (see the module header): validation
-- history must outlive a rebuild of either table, and a delivery/transport
-- integrity failure by definition has no successful `registry.delivery` row
-- to reference. `run_id` links dbt-layer rows to the build run that produced
-- them; `execution_ref` is a free-form pointer (an ingest run id, an Airflow
-- run id) for the Delivery/Raw layers, which do not share `registry.run`'s id
-- space at all -- conflating the two would make one column mean two things.
--
-- APPEND-ONLY. `validation_id` is DETERMINISTIC for one logical execution
-- (one attempt of one control against one subject), so a retry of that same
-- attempt is `ON CONFLICT DO NOTHING` rather than a duplicate row, while a
-- later, genuinely new execution gets its own id and its own row. See
-- `registry/validation.py`.
CREATE TABLE IF NOT EXISTS registry.validation_result (
    validation_id  TEXT        PRIMARY KEY,
    -- dbt layer only: the `registry.run` this test execution belongs to.
    run_id         TEXT,
    -- Delivery/Raw layers: whatever identifies the executing process there
    -- (an ingest run id, an Airflow run id). Not `registry.run.run_id`.
    execution_ref  TEXT,
    feed           TEXT,
    delivery_id    TEXT,
    transport_id   TEXT,
    -- 'delivery' | 'raw' | 'dbt'.
    layer          TEXT        NOT NULL,
    -- A stable identifier for the control itself: an exception class name
    -- ('transport_evidence'), a named RPL check ('expected_min_rows'), or a
    -- dbt node's `unique_id` (preserved verbatim so a later OpenMetadata
    -- ingestion of the same dbt artifacts can join on it).
    control_id     TEXT        NOT NULL,
    control_name   TEXT,
    model_name     TEXT,
    column_name    TEXT,
    -- 'blocking' | 'warn' | 'info'. What executing this control and finding a
    -- problem is allowed to do to the pipeline -- not what happened this time
    -- (that is `outcome`).
    severity       TEXT        NOT NULL,
    -- 'PASS' | 'WARN' | 'FAIL' | 'ERROR'. FAIL is the control finding a real
    -- problem; ERROR is the control failing to execute at all. Never conflate
    -- the two -- see docs/VALIDATION.md.
    outcome        TEXT        NOT NULL,
    expected_value TEXT,
    observed_value TEXT,
    failure_count  BIGINT,
    executed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Where the authoritative evidence behind this row lives: a Transport
    -- marker key, a DeliveryManifest key, or an `s3://.../dbt-artifacts/...`
    -- prefix. This row is a queryable PROJECTION over that evidence, not a
    -- replacement for it.
    evidence_ref   TEXT,
    message        TEXT
);

CREATE INDEX IF NOT EXISTS validation_result_run
    ON registry.validation_result (run_id);
CREATE INDEX IF NOT EXISTS validation_result_delivery
    ON registry.validation_result (feed, delivery_id);
CREATE INDEX IF NOT EXISTS validation_result_transport
    ON registry.validation_result (transport_id);
CREATE INDEX IF NOT EXISTS validation_result_layer_outcome
    ON registry.validation_result (layer, outcome, executed_at DESC);

-- Phase 8. Durable, queryable evidence that a dual-run MIGRATION COMPARISON
-- executed and what it found -- reusing `validation_result`'s outcome
-- vocabulary (PASS/WARN/FAIL/ERROR) rather than inventing a second one, but
-- kept as its OWN table rather than folded into `validation_result`: a
-- migration comparison identifies a LEGACY reference that has no Delivery,
-- Transport or dbt-node identity, and forcing it through those columns would
-- make validation_result's rows misleading (see docs/MIGRATION.md).
--
-- NOT A WORKFLOW-STATE TABLE. There is no row for "waiting on legacy" or
-- "waiting on new" -- absence of a comparable pair is derived at query time
-- from `registry.delivery`/`registry.run_input` and the legacy adapter, never
-- stored. A row here means a comparison actually EXECUTED.
--
-- APPEND-ONLY AND IDEMPOTENT, same shape as `validation_result`:
-- `comparison_id` is deterministic over (feed, checkpoint, legacy_ref,
-- new_ref, comparison_contract_hash), so a retried Airflow task or a repeated
-- reconciliation pass writes the SAME row rather than a duplicate, while a
-- corrected/restated Delivery -- a genuinely different new_ref -- gets its
-- own row. History accumulates; nothing here is ever updated in place.
--
-- NO FOREIGN KEY to `registry.delivery`, `registry.run` or
-- `registry.migration_comparison` itself, for the same rebuildability
-- reasoning `run_input`/`validation_result` already carry: this table must
-- outlive a rebuild of the tables it references, and a legacy-side reference
-- has no row anywhere in this registry to reference at all.
CREATE TABLE IF NOT EXISTS registry.migration_comparison (
    comparison_id            TEXT        PRIMARY KEY,
    feed                     TEXT        NOT NULL,
    business_date            DATE        NOT NULL,
    -- 'raw' | 'prepared' | 'reporting'.
    checkpoint               TEXT        NOT NULL,
    -- The CORRELATION key this comparison paired the two sides on -- see
    -- `reporting_platform/migration/correlate.py`. Not a foreign key to
    -- anything: it is evidence identity, not a relationship.
    correlation_key          TEXT        NOT NULL,
    -- New-platform side: the strongest reference available. `new_run_id`
    -- links to `registry.run` for a prepared/reporting checkpoint (no FK,
    -- same reasoning as `run_input`); Raw has no run of its own, so it stays
    -- NULL there and `new_ref` alone (a DeliveryID) is the reference.
    new_ref                  TEXT        NOT NULL,
    new_run_id               TEXT,
    -- Legacy side: the strongest reference the adapter could return. Free
    -- text because the true legacy estate defines its own identity scheme
    -- (a query snapshot id, a load id, a filename+hash) that this platform
    -- does not own -- see docs/MIGRATION.md#legacy-provenance.
    legacy_ref               TEXT        NOT NULL,
    -- A hash of the comparison CONTRACT actually used -- checkpoint, key,
    -- columns, aggregates, strategy versions -- snapshotted so a later
    -- config edit can never retroactively reinterpret what a historical
    -- PASS meant. See docs/MIGRATION.md#comparison-contract-versioning.
    comparison_contract_hash TEXT        NOT NULL,
    -- 'PASS' | 'WARN' | 'FAIL' | 'ERROR' -- identical vocabulary to
    -- `validation_result.outcome`. There is no fifth "WAITING" outcome:
    -- a row is only ever written once both sides were actually compared.
    outcome                  TEXT        NOT NULL,
    -- 'blocking' | 'warn' -- whether a WARN/FAIL here counts against
    -- acceptance. Independent of `outcome`, same relationship
    -- `validation_result.severity` has to its own `outcome`.
    severity                 TEXT        NOT NULL,
    -- Summary evidence only -- never the differing rows themselves. See
    -- docs/MIGRATION.md#difference-artifacts for why detail lives in object
    -- storage, not here.
    summary                  JSONB       NOT NULL,
    -- Object-store prefix holding the full difference artifact
    -- (legacy-only/new-only/changed + summary.json), written only when there
    -- is something to investigate. NULL for an ordinary PASS.
    diff_ref                 TEXT,
    executed_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    message                  TEXT
);

CREATE INDEX IF NOT EXISTS migration_comparison_feed_date
    ON registry.migration_comparison (feed, business_date);
CREATE INDEX IF NOT EXISTS migration_comparison_outcome
    ON registry.migration_comparison (feed, outcome, executed_at DESC);

-- Operational control plane (COB Feed Status). Two tables, deliberately not
-- one, because they answer different questions and neither is `delivery`
-- with a status column bolted on:
--
--   * `transport_receipt` is an EXECUTION record, like `run` above -- how far
--     THIS occurrence of a Transport has been carried through
--     transport_ingest, discovered by the fast path (`transport_watch`) or
--     the correctness path (`transport_reconcile`) and advanced by the DAG's
--     own tasks. It legitimately has a mutable `status`, for the same reason
--     `run` does: nothing else durably records "how far did this attempt
--     get" -- object storage records what each stage PRODUCED, not that a
--     particular attempt is in flight. It exists only for Transport-origin
--     (v2) deliveries; a legacy `landing/`-direct feed has no Transport and
--     therefore no row here.
--   * `delivery_committed` is an OBSERVATION, like `delivery_part` above --
--     one immutable fact ("this Delivery reached Raw and is on `main`")
--     recorded once, by the one function that actually performs that commit
--     (`ingest_feed._ingest_manifest`), so it is written identically for the
--     legacy and the Transport path. `registry.delivery` itself gets a row
--     at NORMALIZE time, before Raw is ever touched -- see its own header --
--     so delivery existing is evidence of neither commit nor failure, and a
--     dedicated append-only fact is what answers COMPLETE without a Spark
--     query on every status page render.
--
-- Neither table replaces Raw as the ledger: `_delivery_id` in Raw remains
-- authoritative for whether a Delivery committed, exactly as
-- `registry/deliveries.py`'s header says. `delivery_committed` is a cache of
-- that fact for cheap operational queries, and unreadable/absent rows here
-- are a reason to reconcile, never a reason to trust Raw less.
CREATE TABLE IF NOT EXISTS registry.transport_receipt (
    transport_id       TEXT        PRIMARY KEY,
    source             TEXT        NOT NULL,
    legacy_feed_id     TEXT        NOT NULL,
    producer_run_id    TEXT,
    -- NULL for a v1 marker, which carries neither field (see
    -- docs/TRANSPORT-CONTRACT.md, "v1 compatibility").
    cob_date           DATE,
    source_system      TEXT,
    marker_key         TEXT        NOT NULL,
    source_observed_at TIMESTAMPTZ,
    -- Fixed at the Transport's first publication and never rewritten by a
    -- retry (see TRANSPORT-CONTRACT.md) -- so neither is this column, once set.
    uploaded_at        TIMESTAMPTZ NOT NULL,
    -- When THIS PLATFORM first saw the marker, which is a fact about the
    -- registry's own clock, not the Transport's -- see the `first_seen_at`
    -- reasoning on `delivery` above.
    discovered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Filled in once create_delivery resolves a Feed; NULL until then, and
    -- forever NULL for a Transport that failed before that point.
    feed               TEXT,
    delivery_id        TEXT,
    -- 'discovered' | 'validated' | 'delivered' | 'normalized' | 'failed'.
    -- No 'ingested': whether Raw committed is `delivery_committed`'s
    -- question, answered universally for both paths, not duplicated here.
    status             TEXT        NOT NULL,
    -- Denormalized rank of `status` in the pipeline's forward order, so an
    -- UPDATE can refuse to move status BACKWARDS on a racing/stale task
    -- retry without a read-then-write. Not part of this table's public
    -- meaning -- callers read `status`, not this.
    stage_rank         INT         NOT NULL DEFAULT 0,
    failure_reason     TEXT,
    airflow_dag_id     TEXT,
    airflow_run_id     TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transport_receipt_cob_source
    ON registry.transport_receipt (cob_date, source_system);
CREATE INDEX IF NOT EXISTS transport_receipt_feed_cob
    ON registry.transport_receipt (feed, cob_date);
CREATE INDEX IF NOT EXISTS transport_receipt_status
    ON registry.transport_receipt (status);
CREATE INDEX IF NOT EXISTS transport_receipt_delivery
    ON registry.transport_receipt (feed, delivery_id);

CREATE TABLE IF NOT EXISTS registry.delivery_committed (
    feed          TEXT        NOT NULL,
    delivery_id   TEXT        NOT NULL,
    cob_date      DATE        NOT NULL,
    source_system TEXT        NOT NULL,
    file_version  INT,
    -- NULL when the commit was discovered via the idempotent
    -- already-committed short-circuit rather than counted fresh: re-counting
    -- Raw only to fill in a number nothing here needs would cost a Spark
    -- action for no operational benefit.
    rows          BIGINT,
    run_id        TEXT        NOT NULL,
    committed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (feed, delivery_id)
);

CREATE INDEX IF NOT EXISTS delivery_committed_feed_cob
    ON registry.delivery_committed (feed, cob_date);
CREATE INDEX IF NOT EXISTS delivery_committed_cob
    ON registry.delivery_committed (cob_date);
"""



# COLUMNS ADDED TO A TABLE THAT ALREADY EXISTS, which `SCHEMA` above cannot do.
# `CREATE TABLE IF NOT EXISTS` is a no-op against an existing table -- it does
# not reconcile columns -- so every column added after a database was first
# created has to arrive here or it silently never appears. The failure mode is
# the bad one: `ensure_schema` succeeds and the INSERT naming the new column
# fails later, in a task, at publish time.
#
# `ADD COLUMN IF NOT EXISTS` makes each statement idempotent, so this runs on
# every connection like the schema does. Additive only -- a column needing a
# drop or retype is a real migration. Same lazy, idempotent shape
# `ensure_raw_schema()` uses one layer down.
MIGRATIONS = """
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS dbt_project_ref         TEXT;
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS deployment_change_ref   TEXT;
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS deployment_pipeline_ref TEXT;
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS dbt_artifacts_ref       TEXT;
"""


def ensure_schema(conn=None) -> None:
    """Create the registry schema if it is not there. Idempotent.

    RUN FROM TWO PLACES, on purpose. `airflow-init` runs it explicitly so a
    cold stack has the tables before any DAG parses; every connection also
    ensures once per process, because `inbox`, `feed-ui` and `watchdog` share
    the image but not `airflow-init`'s `depends_on` -- they can be up, and
    writing deliveries, while airflow-init has never run.
    """
    global _ensured
    if _ensured and conn is None:
        return
    if conn is None:
        with connect(ensure=False) as own:
            ensure_schema(own)
        return

    import psycopg2

    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
    except psycopg2.errors.UniqueViolation:
        # `CREATE TABLE IF NOT EXISTS` is not atomic against a concurrent
        # identical CREATE: both see the table absent, both insert into
        # pg_type, and the loser raises a unique violation on
        # pg_type_typname_nsp_index rather than the DuplicateTable that
        # IF NOT EXISTS swallows. Two containers starting together is that
        # race. The retry finds the table present and does nothing.
        conn.rollback()
        log.info("registry schema was created concurrently; re-checking")
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
    with conn.cursor() as cur:
        cur.execute(MIGRATIONS)
    conn.commit()
    _ensured = True
