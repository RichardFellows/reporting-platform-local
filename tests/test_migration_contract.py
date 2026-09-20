"""Comparison identity and contract versioning (Phase 8, sections 35-37)."""
from __future__ import annotations

from reporting_platform.migration.contract import (
    comparison_contract_hash, comparison_id,
)


def test_comparison_id_is_stable_for_the_same_logical_comparison():
    contract_hash = comparison_contract_hash("raw", [], [], [])
    a = comparison_id("fo_trade", "raw", "legacy-1", "dlv_abc", contract_hash)
    b = comparison_id("fo_trade", "raw", "legacy-1", "dlv_abc", contract_hash)
    assert a == b


def test_comparison_id_changes_for_a_corrected_delivery():
    """Section 27: a restated Delivery is a DIFFERENT new_ref and must get
    its own comparison id, never overwrite the earlier one."""
    contract_hash = comparison_contract_hash("raw", [], [], [])
    original = comparison_id("fo_trade", "raw", "legacy-1", "dlv_v1", contract_hash)
    corrected = comparison_id("fo_trade", "raw", "legacy-1", "dlv_v2", contract_hash)
    assert original != corrected


def test_contract_hash_changes_when_columns_change():
    """Section 36: historical PASS must not silently acquire today's meaning
    -- a config edit must produce a NEW contract hash."""
    before = comparison_contract_hash("prepared", ["id"], ["rating"], [])
    after = comparison_contract_hash("prepared", ["id"], ["rating", "exposure"], [])
    assert before != after


def test_contract_hash_is_order_independent_on_columns():
    a = comparison_contract_hash("prepared", ["id"], ["rating", "exposure"], [])
    b = comparison_contract_hash("prepared", ["id"], ["exposure", "rating"], [])
    assert a == b


def test_contract_hash_is_order_sensitive_on_key():
    a = comparison_contract_hash("prepared", ["a", "b"], [], [])
    b = comparison_contract_hash("prepared", ["b", "a"], [], [])
    assert a != b


def test_contract_hash_changes_with_tolerance():
    agg = {"column": "exposure", "function": "sum"}
    before = comparison_contract_hash("prepared", [], [], [{**agg, "tolerance": {}}])
    after = comparison_contract_hash(
        "prepared", [], [], [{**agg, "tolerance": {"absolute": 0.01}}])
    assert before != after
