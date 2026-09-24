"""A refusal is not retried, and a retry gets its own branch.

A declared md5 that did not match failed its first attempt correctly; the two
retries then died on Nessie's 409 for the branch that attempt kept, so the
task's last error named neither the checksum nor the control file. These pin
the three pieces that fix it, none of which needs Spark, Airflow or a stack:
the child's exit status, the parent's exception type, and the branch name.
See docs/DECISIONS.md#a-refusal-is-not-retried
"""
from __future__ import annotations

import sys
import types


def test_a_refused_exit_raises_its_own_type_and_is_still_a_runtime_error():
    from reporting_platform.common.spark_task import (
        REFUSED_EXIT, SparkTaskRefused, parse_result,
    )

    try:
        parse_result(("ingest-v2", "k"), REFUSED_EXIT, "",
                     "Traceback (most recent call last):\nX: md5 mismatch\n")
    except SparkTaskRefused as exc:
        assert isinstance(exc, RuntimeError)
        assert "md5 mismatch" in str(exc), exc
    else:
        raise AssertionError("exit REFUSED_EXIT did not raise SparkTaskRefused")


def test_any_other_failure_is_an_ordinary_runtime_error():
    from reporting_platform.common.spark_task import SparkTaskRefused, parse_result

    for code in (1, 2, 137):
        try:
            parse_result(("ingest-v2", "k"), code, "", "boom")
        except SparkTaskRefused:
            raise AssertionError(f"exit {code} was reported as a refusal")
        except RuntimeError:
            pass
        else:
            raise AssertionError(f"exit {code} did not raise")


def _op_module(raising: Exception) -> str:
    """A throwaway op module whose op raises `raising`."""
    name = "tests._refusal_fake_op"
    module = types.ModuleType(name)

    def op(args):
        raise raising

    module.op = op
    sys.modules[name] = module
    return f"{name}:op"


def test_the_child_exits_refused_only_for_a_refusal():
    from reporting_platform.common import spark_task
    from reporting_platform.ingest.ingest_feed import IngestValidationError

    saved = dict(spark_task.OPS)
    try:
        spark_task.OPS["fake"] = _op_module(IngestValidationError("md5"))
        assert spark_task.main(["fake"]) == spark_task.REFUSED_EXIT

        spark_task.OPS["fake"] = _op_module(ValueError("not a refusal"))
        try:
            spark_task.main(["fake"])
        except ValueError:
            pass
        else:
            raise AssertionError("an ordinary ValueError was swallowed")
    finally:
        spark_task.OPS.clear()
        spark_task.OPS.update(saved)
        sys.modules.pop("tests._refusal_fake_op", None)


def test_the_real_child_process_exits_refused():
    """AS `python -m`, which is how `run()` launches it. There the module is
    `__main__`, whose own `Refused` is a different class: the in-process test
    above passed while every live refusal exited 1."""
    import pathlib
    import subprocess
    import textwrap

    from reporting_platform.common.spark_task import MODULE, REFUSED_EXIT

    root = pathlib.Path(__file__).resolve().parent.parent
    script = textwrap.dedent(f"""
        import runpy, sys, types
        from reporting_platform.common import spark_task
        from reporting_platform.ingest.ingest_feed import IngestValidationError
        fake = types.ModuleType("fake_refusing_op")
        def op(args):
            raise IngestValidationError("md5 mismatch")
        fake.op = op
        sys.modules["fake_refusing_op"] = fake
        spark_task.OPS["fake"] = "fake_refusing_op:op"
        sys.argv = ["x", "fake"]
        # Exactly what `python -m` does: a SECOND module object named
        # __main__. The patch above is on the imported one, so this only
        # reaches it if __main__ hands off -- which is the point.
        runpy.run_module({MODULE!r}, run_name="__main__", alter_sys=True)
    """)
    proc = subprocess.run([sys.executable, "-c", script], cwd=root,
                          capture_output=True, text=True)
    assert proc.returncode == REFUSED_EXIT, (proc.returncode, proc.stderr[-2000:])


def test_a_raw_validation_failure_is_a_refusal_and_still_a_value_error():
    from reporting_platform.common.spark_task import Refused
    from reporting_platform.ingest.ingest_feed import IngestValidationError

    exc = IngestValidationError("x")
    assert isinstance(exc, Refused)
    assert isinstance(exc, ValueError)


def test_every_blocking_raw_check_raises_the_refusal():
    """Grep, not a Spark run: the five checks' raises, and nothing else in
    the validate block left as a bare ValueError to be retried into a 409."""
    import inspect

    from reporting_platform.ingest import ingest_feed

    source = inspect.getsource(ingest_feed._ingest_manifest)
    block = source[source.index('if contract_fd.schema_drift == "fail" and drifted'):
                   source.index("if dry_run:")]
    assert block.count("raise IngestValidationError(") == 5, block
    assert "raise ValueError(" not in block


def test_each_attempt_names_its_own_branch():
    from reporting_platform.common.context import ingest_attempt_id

    run = "manual__2026-09-24T11:05:02.123456+00:00"
    one, two = ingest_attempt_id(run, 1), ingest_attempt_id(run, 2)
    assert one != two
    assert one.endswith("-a1") and two.endswith("-a2")
    assert one == ingest_attempt_id(run, 1)
    assert ":" not in one and "+" not in one
    assert len(one) <= 24 + 3


def test_the_control_object_is_named_from_the_delivery_manifest():
    from reporting_platform.ingest import delivery, ingest_feed

    source_files = [types.SimpleNamespace(role="data", object_key="received/t/a.csv"),
                    types.SimpleNamespace(role="control", object_key="received/t/a.ctl")]
    saved = delivery.read_delivery_manifest
    delivery.read_delivery_manifest = (
        lambda key, **_: types.SimpleNamespace(source_files=source_files))
    try:
        base = {"delivery_manifest": "deliveries/DCM/t/delivery-manifest.json",
                "delivery_id": "dlv_x"}
        got = ingest_feed._with_control_object(
            {**base, "declared_md5": "a" * 32}, client=None, bucket="b")
        assert got["control_object"] == "received/t/a.ctl", got

        # Nothing declared: nothing to name, and no read.
        untouched = {**base, "declared_md5": None, "declared_row_count": None}
        assert ingest_feed._with_control_object(
            untouched, client=None, bucket="b") is untouched
    finally:
        delivery.read_delivery_manifest = saved


def test_an_unreadable_delivery_manifest_never_becomes_the_error():
    from reporting_platform.ingest import delivery, ingest_feed

    def boom(key, **_):
        raise delivery.DeliveryManifestError("gone")

    saved = delivery.read_delivery_manifest
    delivery.read_delivery_manifest = boom
    try:
        manifest = {"delivery_manifest": "k", "delivery_id": "dlv_x",
                    "declared_row_count": 3}
        assert ingest_feed._with_control_object(
            manifest, client=None, bucket="b") is manifest
    finally:
        delivery.read_delivery_manifest = saved
