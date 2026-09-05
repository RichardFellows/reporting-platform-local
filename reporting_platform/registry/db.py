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
    conn.commit()
    _ensured = True
