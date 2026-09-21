"""TransportReceipt: durable, idempotent progress for one Transport occurrence.

WHY THIS EXISTS. Before this module, "how far has Transport X got" was
answered fresh, every time, from three S3 reads
(`reporting_platform.ingest.transport_reconcile.discover_transport_progress`)
-- correct, and deliberately not a mutable status table, but too slow to call
on every render of an operator-facing status page and unable to say anything
about a Transport that is currently BEING processed (a running Airflow task
is not evidence anywhere in object storage). This table is the durable,
queryable answer to that: an EXECUTION record, modelled on `registry.run`
rather than on `registry.delivery` -- see `registry/db.py`'s header on this
table for why a mutable `status` is the right shape here and not there.

WHAT IT IS NOT. Not a second ledger for whether a Delivery reached Raw --
that stays `delivery_committed` (this module's sibling) for BOTH ingestion
paths, and Raw's own `_delivery_id` remains authoritative underneath it. Not
a replacement for `discover_transport_progress`, which remains the
correctness backstop: `sync_from_progress` below is what makes reconciliation
converge on the same rows the fast path writes, exactly as
`deliveries.reconcile()` is the rebuild path for `registry.delivery`.

IDENTITY AND MONOTONICITY. `transport_id` is the primary key -- Transport's
own deterministic identity (docs/TRANSPORT-CONTRACT.md), so a duplicate
discovery event or a replayed Airflow task upserts the SAME row rather than
creating a second one. `status` only ever advances forward
(`stage_rank`), except that 'failed' may always be recorded -- a later
attempt genuinely can fail after an earlier one reached further, and a stale
'failed' from an abandoned attempt is overwritten by whichever real stage a
successful retry next reaches. See `record_stage`.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from reporting_platform.registry import db

log = logging.getLogger("registry.transports")

# No 'ingested': see registry/db.py's header on this table.
STAGES = ("discovered", "validated", "delivered", "normalized")
FAILED = "failed"


def _rank(status: str) -> int:
    return STAGES.index(status) if status in STAGES else -1


_DISCOVERED_COLUMNS = (
    "transport_id", "source", "legacy_feed_id", "producer_run_id",
    "cob_date", "source_system", "marker_key", "source_observed_at",
    "uploaded_at",
)


def record_discovered(transport: Any, marker_key: str, *, conn=None) -> dict:
    """Ensure a receipt row exists for a Transport. Insert-once, like `delivery`.

    `transport` is a `reporting_transport.contract.Transport` (or RPL's
    re-export of it). Every field taken from it is fixed at the Transport's
    own first publication (TRANSPORT-CONTRACT.md), so ON CONFLICT DO NOTHING
    is correct: a rediscovery must not requery `source_observed_at`.
    """
    values = {
        "transport_id": transport.transport_id,
        "source": transport.source,
        "legacy_feed_id": transport.legacy_feed_id,
        "producer_run_id": transport.producer_run_id,
        "cob_date": transport.cob_date,
        "source_system": transport.source_system,
        "marker_key": marker_key,
        "source_observed_at": transport.source_observed_at,
        "uploaded_at": transport.uploaded_at,
        "status": "discovered",
        "stage_rank": _rank("discovered"),
    }
    cols = (*_DISCOVERED_COLUMNS, "status", "stage_rank")
    sql = (f"INSERT INTO registry.transport_receipt ({', '.join(cols)}) "
          f"VALUES ({', '.join('%(' + c + ')s' for c in cols)}) "
          f"ON CONFLICT (transport_id) DO NOTHING")
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, values)
    else:
        with db.connect() as own, own.cursor() as cur:
            cur.execute(sql, values)
    return values


def record_discovered_quietly(transport: Any, marker_key: str) -> None:
    try:
        record_discovered(transport, marker_key)
    except Exception as exc:                                     # noqa: BLE001
        log.warning("could not record discovery of transport %s: %s",
                    getattr(transport, "transport_id", "?"),
                    f"{type(exc).__name__}: {exc}")


def record_stage(transport_id: str, status: str, *, feed: str | None = None,
                 delivery_id: str | None = None,
                 failure_reason: str | None = None,
                 airflow_dag_id: str | None = None,
                 airflow_run_id: str | None = None,
                 occurred_at: datetime | None = None, conn=None) -> dict | None:
    """Advance one Transport's receipt. Returns the updated row, or None if
    there is nothing to update (no receipt exists yet for this id).

    A FORWARD-ONLY UPDATE for real stages, and an ANY-TIME one for 'failed':
    `stage_rank` guards against a retried/racing task moving status
    backwards, but a genuine failure is news regardless of how far a previous
    attempt got. A row that is already 'failed' accepts the next real stage
    unconditionally too (WHERE ... OR status = 'failed'): a successful retry
    after a transient failure must not stay stuck reporting FAILED forever.
    """
    if status != FAILED and status not in STAGES:
        raise ValueError(f"unknown transport receipt status {status!r}")
    new_rank = _rank(status)
    values = {
        "transport_id": transport_id, "status": status, "new_rank": new_rank,
        "feed": feed, "delivery_id": delivery_id,
        "failure_reason": failure_reason if status == FAILED else None,
        "airflow_dag_id": airflow_dag_id, "airflow_run_id": airflow_run_id,
        "updated_at": occurred_at or datetime.now().astimezone(),
    }
    sql = (
        "UPDATE registry.transport_receipt SET "
        "  status = %(status)s, "
        "  stage_rank = CASE WHEN %(status)s = 'failed' THEN stage_rank "
        "                    ELSE %(new_rank)s END, "
        "  feed = COALESCE(%(feed)s, feed), "
        "  delivery_id = COALESCE(%(delivery_id)s, delivery_id), "
        "  failure_reason = CASE WHEN %(status)s = 'failed' "
        "                        THEN %(failure_reason)s ELSE NULL END, "
        "  airflow_dag_id = COALESCE(%(airflow_dag_id)s, airflow_dag_id), "
        "  airflow_run_id = COALESCE(%(airflow_run_id)s, airflow_run_id), "
        "  updated_at = %(updated_at)s "
        "WHERE transport_id = %(transport_id)s "
        "  AND (%(status)s = 'failed' OR status = 'failed' "
        "       OR %(new_rank)s >= stage_rank) "
        "RETURNING transport_id")
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            updated = cur.fetchone() is not None
    else:
        with db.connect() as own, own.cursor() as cur:
            cur.execute(sql, values)
            updated = cur.fetchone() is not None
    return values if updated else None


def record_stage_quietly(transport_id: str, status: str, **kwargs) -> None:
    try:
        record_stage(transport_id, status, **kwargs)
    except Exception as exc:                                     # noqa: BLE001
        log.warning("could not record transport %s -> %s: %s", transport_id,
                    status, f"{type(exc).__name__}: {exc}")


def record_stage_by_delivery_id(feed: str, delivery_id: str, status: str, *,
                                failure_reason: str | None = None,
                                airflow_dag_id: str | None = None,
                                airflow_run_id: str | None = None) -> bool:
    """Like `record_stage`, addressed by (feed, delivery_id) instead of
    transport_id -- for the one call site (`ingest_raw` failing) that knows
    the Delivery but would otherwise have to re-derive its Transport just to
    write one failure row. `delivery_id` is set on the receipt from the
    'delivered' stage onward, so this finds the same row `record_stage` would.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT transport_id FROM registry.transport_receipt "
                    "WHERE feed = %s AND delivery_id = %s",
                    (feed, delivery_id))
        row = cur.fetchone()
        if row is None:
            return False
        return record_stage(row[0], status, failure_reason=failure_reason,
                            airflow_dag_id=airflow_dag_id,
                            airflow_run_id=airflow_run_id, conn=conn) is not None


def record_stage_by_delivery_id_quietly(feed: str, delivery_id: str,
                                        status: str, **kwargs) -> None:
    try:
        record_stage_by_delivery_id(feed, delivery_id, status, **kwargs)
    except Exception as exc:                                     # noqa: BLE001
        log.warning("could not record %s/%s -> %s: %s", feed, delivery_id,
                    status, f"{type(exc).__name__}: {exc}")


_COLUMNS = (
    "transport_id, source, legacy_feed_id, producer_run_id, cob_date, "
    "source_system, marker_key, source_observed_at, uploaded_at, "
    "discovered_at, feed, delivery_id, status, failure_reason, "
    "airflow_dag_id, airflow_run_id, updated_at"
)


def get(transport_id: str) -> dict | None:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_COLUMNS} FROM registry.transport_receipt "
                    "WHERE transport_id = %s", (transport_id,))
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip([c[0] for c in cur.description], row))


def for_cob_date(cob_date, source_system: str | None = None) -> list[dict]:
    """Every receipt for one COB date -- the read the status view uses."""
    sql = f"SELECT {_COLUMNS} FROM registry.transport_receipt WHERE cob_date = %s"
    args: list[Any] = [cob_date]
    if source_system:
        sql += " AND source_system = %s"
        args.append(source_system)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(sql + " ORDER BY updated_at DESC", args)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def for_feed_cob(feed: str, cob_date) -> list[dict]:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_COLUMNS} FROM registry.transport_receipt "
                    "WHERE feed = %s AND cob_date = %s ORDER BY updated_at DESC",
                    (feed, cob_date))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ------------------------------------------------------- reconciliation sync
def sync_from_progress(progress: dict, *, client=None, bucket: str | None = None
                       ) -> dict:
    """Converge the receipt table onto what `discover_transport_progress`
    (the S3-evidence correctness walk) just found.

    Called from `transport_reconcile`, this is what makes the two discovery
    paths -- the fast S3-event one (`transport_watch`) and this correctness
    one -- write the SAME idempotent rows, exactly as the task set out to
    require. Coarser than the per-task updates `transport_ingest` itself
    writes: a Transport found here past NormalizationManifest is recorded as
    'normalized' even though this walk cannot itself tell 'delivered' from
    'normalized' as distinctly as the DAG's own tasks do -- reconciliation is
    the backstop for a MISSED event, not a second copy of the DAG's own
    bookkeeping.
    """
    from reporting_platform.ingest import transport as transport_contract

    client = client or transport_contract._client()               # noqa: SLF001
    bucket = bucket or transport_contract._bucket()                # noqa: SLF001
    marker_keys = progress.get("marker_keys", {})
    out = {"discovered": 0, "advanced": 0, "failed_to_sync": []}

    def _ensure_discovered(transport_id: str) -> None:
        marker_key = marker_keys.get(transport_id)
        if not marker_key:
            return
        transport = transport_contract.read_transport(
            marker_key, client=client, bucket=bucket)
        record_discovered(transport, marker_key)
        out["discovered"] += 1

    for transport_id in progress.get("needs_full_chain", []):
        try:
            _ensure_discovered(transport_id)
        except Exception as exc:                                  # noqa: BLE001
            out["failed_to_sync"].append(
                {"transport_id": transport_id, "error": f"{type(exc).__name__}: {exc}"})

    for feed_name, pairs in progress.get("candidates_by_feed", {}).items():
        for delivery_id, transport_id in pairs:
            try:
                _ensure_discovered(transport_id)
                record_stage(transport_id, "normalized", feed=feed_name,
                            delivery_id=delivery_id)
                out["advanced"] += 1
            except Exception as exc:                              # noqa: BLE001
                out["failed_to_sync"].append(
                    {"transport_id": transport_id, "error": f"{type(exc).__name__}: {exc}"})
    return out
