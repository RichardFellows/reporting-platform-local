"""A Transport into raw as steps: one implementation, the DAG and the CLI.

`transport_ingest`'s four task bodies moved to `ingest/transport_steps.py` so
the standalone runner can take Transports instead of the inbox. These pin
that the DAG only CALLS them (read as text: this tier has no Airflow), and
the behaviour the runner adds around them: what counts as pending, and that
a refused Transport is reported apart from a failed one -- the pipeline
builds past the first and stops at the second.
"""
from __future__ import annotations

import contextlib
from datetime import date

from tests.support import DAGS


@contextlib.contextmanager
def _patched(module, **replacements):
    saved = {name: getattr(module, name) for name in replacements}
    for name, value in replacements.items():
        setattr(module, name, value)
    try:
        yield
    finally:
        for name, value in saved.items():
            setattr(module, name, value)


def _task(source: str, name: str) -> str:
    start = source.index(f"def {name}(")
    end = source.find("\n    @task(", start)
    return source[start:end if end > 0 else len(source)]


# ------------------------------------------------- the DAG only calls steps
def test_each_transport_ingest_task_calls_its_step_and_nothing_else():
    source = (DAGS / "transport_ingest.py").read_text(encoding="utf-8")
    calls = {"validate_transport": "transport_steps.validate(",
             "create_delivery_task": "_run_step(transport_steps.deliver,",
             "normalize_delivery_task": "_run_step(transport_steps.normalize,",
             "ingest_raw_task": "_run_step(transport_steps.ingest_raw,"}
    # What a step does. Any of these back in a task body is a second copy.
    owned = ("read_validated_transport(", "create_delivery(",
             "normalize_delivery(", 'run("ingest-v2"', "record_stage",
             "record_discovered", "validation.record")
    for task, call in calls.items():
        body = _task(source, task)
        assert call in body, (task, call)
        for construct in owned:
            assert construct not in body, (task, construct)
    for construct in owned:
        assert construct not in source, construct


def test_the_refusal_rule_is_the_steps_own():
    source = (DAGS / "transport_ingest.py").read_text(encoding="utf-8")
    assert "from reporting_platform.ingest.transport_steps import is_refusal" in source
    assert "def _is_refusal(" not in source


# --------------------------------------------------------------- the steps
def test_a_refusal_is_fail_and_anything_else_is_error():
    from reporting_platform.common.spark_task import SparkTaskRefused
    from reporting_platform.ingest import delivery, transport_steps
    from reporting_platform.registry import validation

    rows = []
    with _patched(validation, record_quietly=lambda **kw: rows.append(kw)):
        caller = transport_steps.Caller(execution_ref="r1")
        for exc in (delivery.IdentityResolutionError("x"),
                    SparkTaskRefused("md5"), RuntimeError("store down")):
            transport_steps.record_failure(
                exc, control_id="c", evidence_ref="t", caller=caller)
    assert [r["outcome"] for r in rows] == ["FAIL", "FAIL", "ERROR"]
    assert all(r["execution_ref"] == "r1" for r in rows)


def _stub_steps(fail_at: str | None = None, exc: Exception | None = None):
    from reporting_platform.ingest import transport_steps

    calls = []

    def step(name, value):
        def fn(*args, **kwargs):
            calls.append(name)
            if name == fail_at:
                raise exc
            return value
        return fn

    raw = {"feed": "f", "delivery_id": "dlv_1", "already_ingested": False}
    return calls, dict(
        validate=step("validate", "m"), deliver=step("deliver", "d"),
        normalize=step("normalize", "n"), ingest_raw=step("ingest_raw", raw))


def test_ingest_transport_runs_the_four_steps_then_after_ingest():
    from reporting_platform.ingest import steps, transport_steps

    calls, stubs = _stub_steps()
    after = []

    def fake_after(feed, result):
        after.append(feed)
        return {**result, "tag": "snapshot/f/2026-09-14/r"}

    with _patched(transport_steps, **stubs), _patched(steps, after_ingest=fake_after):
        got = transport_steps.ingest_transport("received/x/_COMPLETE.json")
    assert calls == ["validate", "deliver", "normalize", "ingest_raw"]
    assert after == ["f"], "the drift report and tag did not follow the ingest"
    assert got["delivery_id"] == "dlv_1" and "error" not in got
    assert got["tag"] == "snapshot/f/2026-09-14/r"
    assert got["marker_key"] == "received/x/_COMPLETE.json"


def test_ingest_transport_reports_a_refusal_apart_from_a_failure():
    from reporting_platform.common.spark_task import SparkTaskRefused
    from reporting_platform.ingest import transport_steps

    for fail_at, exc, refused in (
            ("ingest_raw", SparkTaskRefused("md5 mismatch"), True),
            ("validate", RuntimeError("store down"), False)):
        calls, stubs = _stub_steps(fail_at, exc)
        from reporting_platform.ingest import steps

        def no_after(*_):
            raise AssertionError("after_ingest ran after a failed step")

        with _patched(transport_steps, **stubs), _patched(steps, after_ingest=no_after):
            got = transport_steps.ingest_transport("m")
        assert got["stage"] == fail_at, got
        assert got["refused"] is refused, got
        assert calls[-1] == fail_at      # nothing after the failed step


def test_pending_is_the_reconcile_set_and_keeps_what_it_could_not_read():
    from reporting_platform.common import spark_task
    from reporting_platform.ingest import transport_reconcile, transport_steps

    report = {
        "needs_full_chain": ["t-new"],
        "candidates_by_feed": {"f": [("dlv_in", "t-done"),
                                     ("dlv_out", "t-gap")]},
        "failed": [{"marker": "received/bad/_COMPLETE.json", "error": "x"}],
        "marker_keys": {"t-new": "m-new", "t-done": "m-done", "t-gap": "m-gap"},
    }
    seen = {}

    def discover(cob_dates=None):
        seen["cob_dates"] = cob_dates
        return report

    with _patched(transport_reconcile, discover_transport_progress=discover), \
            _patched(spark_task, run=lambda op, feed: {"delivery_ids": ["dlv_in"]}):
        got = transport_steps.pending(["2026-09-24"])
    assert got["marker_keys"] == ["m-gap", "m-new"]
    assert got["unreadable"] == report["failed"]      # not "nothing pending"
    assert seen["cob_dates"] == ["2026-09-24"]


def test_the_window_is_the_last_n_days_inclusive():
    from reporting_platform.ingest.transport_steps import window_cob_dates

    assert window_cob_dates(3, today=date(2026, 9, 24)) == [
        "2026-09-24", "2026-09-23", "2026-09-22"]


# ----------------------------------------------------------- the pipeline
def _pipeline_with(results):
    from reporting_platform.ingest import transport_steps
    from reporting_platform.pipeline import __main__ as pipeline
    from reporting_platform.transform import dbt

    built = []
    by_marker = dict(results)
    return built, [
        _patched(transport_steps,
                 ingest_transport=lambda m, **_: {"marker_key": m, **by_marker[m]}),
        _patched(dbt, build_layer=lambda purpose, label, **_: (
            built.append(purpose) or {"ok": True, "branch": f"b/{purpose}"})),
    ], pipeline


def _run(results):
    built, patches, pipeline = _pipeline_with(results)
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        code, report = pipeline.run_transports(
            [m for m, _ in results], cob_dates=None, through="reporting",
            change_ref=None, label="t")
    return code, built, report


def test_a_refused_transport_is_built_past_and_still_exits_1():
    """Like a quarantined file: it never reached raw, so the rest can build."""
    code, built, _ = _run([("m1", {"rows": 3}),
                           ("m2", {"error": "md5", "refused": True})])
    assert built == ["prepared", "reporting"]
    assert code == 1


def test_a_failed_transport_stops_before_the_builds():
    code, built, _ = _run([("m1", {"rows": 3}),
                           ("m2", {"error": "store down", "refused": False})])
    assert built == []
    assert code == 1


def test_all_clean_builds_and_exits_0():
    code, built, _ = _run([("m1", {"rows": 3})])
    assert built == ["prepared", "reporting"] and code == 0
