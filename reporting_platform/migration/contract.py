"""Comparison identity and contract versioning (Phase 8).

Two different hashes protect two different things, and conflating them would
silently change the meaning of history:

  * `comparison_id` is IDENTITY -- "this is the same logical comparison as
    last time", so a retry (an Airflow task retry, a repeated
    `migration_reconcile` pass) writes the SAME row instead of a duplicate.
    It is deterministic over (feed, checkpoint, legacy_ref, new_ref,
    comparison_contract_hash) -- the same shape as
    `registry/validation.py`'s `_id`.

  * `comparison_contract_hash` is VERSIONING -- "this is what the comparison
    MEANT" at the moment it ran: which checkpoint, which key, which columns,
    which aggregates and tolerances. A later edit to a feed's `migration:`
    block changes this hash, so a NEW comparison against the same
    (feed, legacy_ref, new_ref) pair is a DIFFERENT logical comparison and
    gets its own row -- exactly the outcome required by
    docs/MIGRATION.md#comparison-contract-versioning: historical PASS never
    silently acquires today's meaning.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

CONTRACT_VERSION = 1  # bump if the comparator semantics themselves change


def _digest(*parts: str) -> str:
    joined = "\x1f".join(p if p is not None else "" for p in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()  # noqa: S324


def comparison_contract_hash(checkpoint: str, key: list[str], columns: list[str],
                             aggregates: list[dict[str, Any]]) -> str:
    """Snapshot the config that gives a comparison its meaning. 12 hex chars.

    Order-independent on `columns`/`aggregates` so re-listing the same
    columns in a different order does not manufacture a new contract; `key`
    stays ORDER-SENSITIVE, because a composite key's column order is part of
    how the canonical row hash in `comparators.py` is built.
    """
    material = json.dumps(
        {"contract_version": CONTRACT_VERSION, "checkpoint": checkpoint,
         "key": list(key), "columns": sorted(columns),
         "aggregates": sorted(
             ({"column": a["column"], "function": a["function"],
               "tolerance": a.get("tolerance") or {}} for a in aggregates),
             key=lambda a: (a["column"], a["function"]))},
        sort_keys=True, default=str)
    return _digest(material)[:12]


def comparison_id(feed: str, checkpoint: str, legacy_ref: str, new_ref: str,
                  contract_hash: str) -> str:
    """A short, deterministic id for one logical comparison execution.

    Retrying the identical (feed, checkpoint, legacy_ref, new_ref, contract)
    tuple reproduces this id, so `evidence.record` is `ON CONFLICT DO
    NOTHING` exactly like `validation.record`. A corrected/restated Delivery
    is a different `new_ref` and therefore a different id -- see
    docs/MIGRATION.md#restatements.
    """
    return "mcmp_" + _digest(feed, checkpoint, legacy_ref, new_ref,
                            contract_hash)[:24]
