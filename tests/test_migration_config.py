"""Feed-level `migration:` config validation (Phase 8)."""
from __future__ import annotations

from reporting_platform.common import context as ctx


def test_absent_migration_block_defaults_to_legacy():
    resolved = ctx.resolve_migration_config("f", None)
    assert resolved == {"mode": "legacy"}


def test_unknown_mode_is_refused_at_load():
    try:
        ctx.resolve_migration_config("f", {"mode": "bogus"})
    except ValueError as exc:
        assert "bogus" in str(exc)
        assert "dual_run" in str(exc)
    else:
        raise AssertionError("expected a refusal")


def test_unknown_top_level_key_is_refused():
    try:
        ctx.resolve_migration_config("f", {"mode": "dual_run", "bogus": 1})
    except ValueError as exc:
        assert "bogus" in str(exc)
    else:
        raise AssertionError("expected a refusal")


def test_dual_run_with_full_compare_and_acceptance_resolves():
    resolved = ctx.resolve_migration_config("f", {
        "mode": "dual_run",
        "compare": {
            "checkpoint": "prepared",
            "key": ["counterparty_id"],
            "columns": ["rating", "exposure"],
            "aggregates": [{"column": "exposure", "function": "sum",
                            "tolerance": {"absolute": 0.01}}],
        },
        "acceptance": {"consecutive_successes": 5, "allow_warnings": True},
    })
    assert resolved["mode"] == "dual_run"
    assert resolved["compare"]["checkpoint"] == "prepared"
    assert resolved["compare"]["key"] == ["counterparty_id"]
    assert resolved["acceptance"] == {"consecutive_successes": 5,
                                      "allow_warnings": True}


def test_unknown_checkpoint_is_refused():
    try:
        ctx.resolve_migration_config(
            "f", {"mode": "dual_run", "compare": {"checkpoint": "bogus"}})
    except ValueError as exc:
        assert "bogus" in str(exc)
    else:
        raise AssertionError("expected a refusal")


def test_aggregate_needs_a_column():
    try:
        ctx.resolve_migration_config(
            "f", {"mode": "dual_run",
                 "compare": {"aggregates": [{"function": "sum"}]}})
    except ValueError as exc:
        assert "column" in str(exc)
    else:
        raise AssertionError("expected a refusal")


def test_negative_tolerance_is_refused():
    try:
        ctx.resolve_migration_config(
            "f", {"mode": "dual_run", "compare": {"aggregates": [
                {"column": "exposure", "tolerance": {"absolute": -1}}]}})
    except ValueError as exc:
        assert "non-negative" in str(exc)
    else:
        raise AssertionError("expected a refusal")


def test_reporting_checkpoint_may_name_an_explicit_table():
    resolved = ctx.resolve_migration_config(
        "f", {"mode": "dual_run",
             "compare": {"checkpoint": "reporting", "table": "counterparty_exposure"}})
    assert resolved["compare"]["table"] == "counterparty_exposure"
