"""Validation evidence: the pure parts (Phase 7).

WHAT THIS COVERS AND WHAT IT DELIBERATELY DOES NOT, the same line
`tests/test_registry.py` draws: `parse_run_results` and `_id` are pure
functions of files/strings, so they are pinned here exactly. Everything that
talks to Postgres -- `record`'s insert, the `ON CONFLICT` idempotency, the
`for_delivery`/`for_transport`/`for_run` reads -- was verified by RUNNING it
against the live stack (a real Transport through `transport_ingest`, a real
`prepared_build`), not mocked here. A fake database would agree with whatever
this code asked it, which is the one thing a registry test must not do. See
tests/README.md and docs/VALIDATION.md.
"""
from __future__ import annotations

import json
import pathlib
import tempfile

from reporting_platform.registry import validation


def _target(manifest: dict, run_results: dict) -> pathlib.Path:
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-validation-"))
    (d / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (d / "run_results.json").write_text(json.dumps(run_results), encoding="utf-8")
    return d


MANIFEST = {
    "nodes": {
        "test.reporting_platform.not_null_fo_trade_trade_id.abc123": {
            "name": "not_null_fo_trade_trade_id",
            "column_name": "trade_id",
            "resource_type": "test",
            "config": {"severity": "error"},
            "depends_on": {"nodes": ["model.reporting_platform.fo_trade"]},
        },
        "model.reporting_platform.fo_trade": {
            "name": "fo_trade", "resource_type": "model",
        },
        "test.reporting_platform.accepted_values_x__A__B.def456": {
            "name": "accepted_values_x__A__B",
            "column_name": "status",
            "resource_type": "test",
            "config": {"severity": "warn"},
            "depends_on": {"nodes": ["model.reporting_platform.ref_counterparty"]},
        },
        "model.reporting_platform.ref_counterparty": {
            "name": "ref_counterparty", "resource_type": "model",
        },
    }
}


# --------------------------------------------------------- outcome mapping
def test_a_passing_test_is_pass_at_blocking_severity():
    target = _target(MANIFEST, {"results": [
        {"unique_id": "test.reporting_platform.not_null_fo_trade_trade_id.abc123",
         "status": "pass", "failures": 0},
    ]})
    out = validation.parse_run_results(target)
    assert len(out) == 1
    row = out[0]
    assert row["outcome"] == "PASS"
    assert row["severity"] == "blocking"
    assert row["model_name"] == "fo_trade"
    assert row["column_name"] == "trade_id"


def test_a_warn_severity_test_that_breaches_is_warn_not_fail():
    """dbt itself decides `warn` vs `fail` from `config.severity` -- this
    platform's severity vocabulary is read off the same config, never
    re-derived from the outcome."""
    target = _target(MANIFEST, {"results": [
        {"unique_id": "test.reporting_platform.accepted_values_x__A__B.def456",
         "status": "warn", "failures": 3},
    ]})
    row = validation.parse_run_results(target)[0]
    assert row["outcome"] == "WARN"
    assert row["severity"] == "warn"
    assert row["failure_count"] == 3


def test_a_blocking_severity_test_that_breaches_is_fail():
    target = _target(MANIFEST, {"results": [
        {"unique_id": "test.reporting_platform.not_null_fo_trade_trade_id.abc123",
         "status": "fail", "failures": 12},
    ]})
    row = validation.parse_run_results(target)[0]
    assert row["outcome"] == "FAIL"
    assert row["severity"] == "blocking"
    assert row["failure_count"] == 12


def test_a_test_that_could_not_execute_is_error_not_fail():
    """FAIL is data breaching a control; ERROR is the control itself not
    running. dbt's own `error` status maps to ERROR, never FAIL."""
    target = _target(MANIFEST, {"results": [
        {"unique_id": "test.reporting_platform.not_null_fo_trade_trade_id.abc123",
         "status": "error", "message": "relation does not exist"},
    ]})
    row = validation.parse_run_results(target)[0]
    assert row["outcome"] == "ERROR"
    assert row["message"] == "relation does not exist"


def test_a_skipped_test_is_omitted_not_recorded_as_any_outcome():
    """A skipped test did not execute -- usually an upstream model failed
    first. Recording it as PASS/WARN/FAIL/ERROR would all claim it ran."""
    target = _target(MANIFEST, {"results": [
        {"unique_id": "test.reporting_platform.not_null_fo_trade_trade_id.abc123",
         "status": "skipped"},
    ]})
    assert validation.parse_run_results(target) == []


def test_a_model_run_result_is_not_a_test_result():
    """`run_results.json` also carries `model.*` rows for `dbt run`; only
    `test.*` nodes are validation evidence."""
    target = _target(MANIFEST, {"results": [
        {"unique_id": "model.reporting_platform.fo_trade", "status": "success"},
    ]})
    assert validation.parse_run_results(target) == []


def test_no_run_results_file_is_an_empty_list_not_an_error():
    """The task's `dbt run` invocation (not `dbt test`) produces no test
    results at all -- that is normal, not a parse failure."""
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-validation-"))
    assert validation.parse_run_results(d) == []


def test_a_unique_id_absent_from_the_manifest_still_records_with_defaults():
    """A manifest/run_results pair that do not quite match (an edge case, not
    the ordinary path) still yields a row -- with what run_results alone
    gives, rather than being dropped."""
    target = _target(MANIFEST, {"results": [
        {"unique_id": "test.reporting_platform.unknown_test.zzz999",
         "status": "pass"},
    ]})
    row = validation.parse_run_results(target)[0]
    assert row["model_name"] is None
    assert row["severity"] == "blocking"
    assert row["control_name"] == "unknown_test"


# ------------------------------------------------------------- identity/ids
def test_the_same_attempt_is_the_same_id():
    a = validation._id("raw", "expected_min_rows", "run1:dlv_x")
    b = validation._id("raw", "expected_min_rows", "run1:dlv_x")
    assert a == b


def test_a_different_attempt_is_a_different_id():
    a = validation._id("raw", "expected_min_rows", "run1:dlv_x")
    b = validation._id("raw", "expected_min_rows", "run2:dlv_x")
    assert a != b


def test_a_different_control_on_the_same_attempt_is_a_different_id():
    a = validation._id("raw", "expected_min_rows", "run1:dlv_x")
    b = validation._id("raw", "declared_md5", "run1:dlv_x")
    assert a != b


# ------------------------------------------------------------ guardrails
def test_record_refuses_an_unknown_layer():
    try:
        validation.record(layer="bogus", control_id="x", outcome="PASS",
                          severity="blocking", attempt_key="k")
    except ValueError as exc:
        assert "layer" in str(exc)
    else:
        raise AssertionError("an unknown layer was accepted")


def test_record_refuses_an_unknown_outcome():
    try:
        validation.record(layer="raw", control_id="x", outcome="MAYBE",
                          severity="blocking", attempt_key="k")
    except ValueError as exc:
        assert "outcome" in str(exc)
    else:
        raise AssertionError("an unknown outcome was accepted")


def test_record_refuses_an_unknown_severity():
    try:
        validation.record(layer="raw", control_id="x", outcome="PASS",
                          severity="critical", attempt_key="k")
    except ValueError as exc:
        assert "severity" in str(exc)
    else:
        raise AssertionError("an unknown severity was accepted")
