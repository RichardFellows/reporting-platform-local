"""The delivery registry's transactional store: Postgres, in the `platform` DB.

WHY POSTGRES AND NOT ICEBERG. The registry needs one thing Iceberg cannot give
it cheaply: a serialising authority. `sequence_no` is the order in which this
platform saw deliveries, and an order allocated by `MAX(...)+1` over a table
several writers append to is the same read-then-write `next_file_version` has
-- correct today only because the `lakehouse_write` pool has one slot, which is
exactly what the concurrency work intends to change. A database sequence is
serialised by the database, at any pool size.

The `platform` database has existed since the first compose file and nothing
has ever connected to it. This is its first user, which is why a connection
helper is new surface rather than an import.

WHY THIS IS NOT THE `stg` LOAD-CONTROL TABLE. That is the trap this platform
refuses by name -- see `arrival.already_ingested` -- and the difference is not
the technology, it is what the table is allowed to say:

  * The registry records OBSERVATIONS about deliveries: what arrived, when,
    how big, what it hashed to, what it was called upstream. Every one of
    them is a fact about an object that already exists in `landing/`.
  * It records NO VERDICTS. There is no `ingested`, no `superseded`, no
    `status`. Whether a delivery reached the raw table stays derived from the
    raw table's own `_source_file`, where it cannot drift; whether a delivery
    supersedes another stays `dedupe_rank`'s answer, computed at read time.

So every row here is reconstructible from object storage, and
`deliveries.reconcile()` is the function that does it. A registry that could
not be rebuilt would be a second source of truth; one that can is an index.

WHAT A REBUILD DOES NOT PRESERVE: the exact integers in `sequence_no`. Rebuilt
rows are inserted in `received_at` order, so the ORDER is reproduced and the
values are not. Nothing may key on the value -- `_delivery_id` on the raw
table references `(feed, delivery_id)`, which is the landing filename and is
stable.

RUNS, VERSIONS, SUBMISSIONS AND AS-AT TRANSITIONS ARE THE EXCEPTION, and it is
the only one.
Everything above is an observation about an object that exists in storage, so
it can be recomputed from storage. A RUN is not: it is an event that happened
once, at a time, from a particular commit of the code, and no object anywhere
records that it happened. Neither is the version number a report was published
under, nor the fact that somebody submitted it, nor that somebody locked or
reopened an as-at date -- REQ-500's lifecycle is a sequence of human acts, and
`registry.as_at_transition` is the only place any of them is written down.

So these tables are the first rows in this database that a rebuild cannot
reconstruct, and two things follow that are easy to get wrong:

  * `run_input` carries NO FOREIGN KEY to `delivery`. It looks like it should
    -- it names (feed, delivery_id) -- but a foreign key would let a registry
    rebuild (drop, reconcile) CASCADE run history away, destroying the only
    copy of something to protect the integrity of a table that has a second
    copy in object storage. Exactly the wrong way round. The join still works;
    it is simply not enforced, and `monitoring/evidence.py` treats a delivery
    it cannot find as a finding rather than as an impossibility.
  * A run legitimately HAS A STATUS, where a delivery does not. That is not
    the boundary being relaxed: a delivery's status would be a verdict about
    something already true (was it ingested, was it superseded), derivable and
    therefore driftable. A run's status is the record of how the run ended,
    which nothing else knows and nothing can derive.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager

log = logging.getLogger("registry.db")

# One process ensures the schema once. Not a correctness mechanism -- the DDL
# is idempotent -- just a way of not issuing six CREATEs per reconcile.
_ensured = False


def dsn() -> str:
    """The registry connection string, or a refusal.

    NO DEFAULT, deliberately. Every other connection string in this stack is
    written down in `docker-compose.yml` where it can be seen and changed;
    a default buried here would let a container come up pointing at a
    database nobody configured and report a healthy, empty registry. The
    failure mode this avoids is the one `tag_retention_years` avoids by
    refusing bad config rather than falling back.
    """
    value = os.environ.get("REGISTRY_DSN", "").strip()
    if not value:
        raise RuntimeError(
            "REGISTRY_DSN is not set, so the delivery registry has no store. "
            "It is set on the shared `x-airflow-common` environment block in "
            "docker-compose.yml; a container started before that was added "
            "needs recreating, not restarting.")
    return value


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

-- One row per DELIVERY: a set of bytes this platform accepted into landing/
-- for one feed and one business date. The natural key is the landing
-- filename, because that is what identity means here -- `_v2` is a different
-- delivery from the file it corrects, and that is the whole point of the
-- versioning `conform._free_name` does.
CREATE TABLE IF NOT EXISTS registry.delivery (
    feed               TEXT        NOT NULL,
    delivery_id        TEXT        NOT NULL,
    -- Allocated by the database. See the module header on what a rebuild
    -- does and does not preserve.
    sequence_no        BIGSERIAL   NOT NULL UNIQUE,
    source_system      TEXT        NOT NULL,
    business_date      DATE        NOT NULL,
    -- The DELIVERY's arrival time -- the landing object's LastModified, the
    -- same value the manifest carries -- not the moment this row was written.
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
    ON registry.delivery (feed, business_date);
CREATE INDEX IF NOT EXISTS delivery_business_date
    ON registry.delivery (business_date);

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
    -- The business date the run PUBLISHED: the maximum business date present
    -- in what it built. Null until it publishes, because until then nothing
    -- has been read to establish it.
    business_date     DATE,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    -- REQ-404. What code produced this. `code_ref_kind` says whether the
    -- value is a deployed identity or a content digest of the mounted tree --
    -- see context.code_ref(), and never conflate the two.
    code_ref          TEXT        NOT NULL,
    code_ref_kind     TEXT        NOT NULL,
    dbt_manifest_ref  TEXT        NOT NULL,
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
"""



# COLUMNS ADDED TO A TABLE THAT ALREADY EXISTS, which `SCHEMA` above cannot do.
# `CREATE TABLE IF NOT EXISTS` is a no-op against an existing table -- it does
# not reconcile its columns -- so every column added after a database was first
# created has to arrive here or it silently never appears. The failure mode is
# the bad one: `ensure_schema` succeeds, and the INSERT naming the new column
# fails later, in a task, at publish time.
#
# `ADD COLUMN IF NOT EXISTS` makes each statement idempotent, so this runs on
# every connection like the schema does. Additive only -- a column that needs
# dropping or retyping is a real migration and does not belong in a startup
# path. This is the same lazy, idempotent shape `ensure_raw_columns()` uses one
# layer down, for the same reason.
MIGRATIONS = """
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS dbt_project_ref         TEXT;
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS deployment_change_ref   TEXT;
ALTER TABLE registry.run ADD COLUMN IF NOT EXISTS deployment_pipeline_ref TEXT;
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
        # IF NOT EXISTS swallows. Two containers starting together is exactly
        # that race. The retry finds the table present and does nothing.
        conn.rollback()
        log.info("registry schema was created concurrently; re-checking")
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
    with conn.cursor() as cur:
        cur.execute(MIGRATIONS)
    conn.commit()
    _ensured = True
