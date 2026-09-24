"""transform/: the write-audit-publish sequence around a dbt build.

It used to be the body of three Airflow tasks in `dbt_builds.py`, which the
config tier cannot import (cosmos, airflow), so its ORDER was checked by
finding strings in the file. As a plain module it can be RUN: here against
recorded fakes for Nessie, the Spark launcher and the registry, so what is
asserted is what it does -- the evidence verified, the input set read, the
lifecycle gate consulted, all BEFORE main moves, and a refusal stopping it.
Whether the real Nessie and Postgres accept the calls is verified by running
it on the stack.
"""
from __future__ import annotations

import json

from tests.fakes3 import FakeS3
from tests.support import DAGS, config_dir

BRANCH = "build/reporting/2026-09-24/r1"


def _wap():
    config_dir()
    from reporting_platform.transform import wap
    return wap


# ------------------------------------------------------------ names and keys
def test_the_run_key_is_derived_from_the_branch_and_carries_the_purpose():
    """Recomputing the slug in two places is how one run ends up as two
    rows; and the prepared and reporting builds of one dataset-triggered run
    slugify the SAME Airflow run id, so the purpose has to be in the key."""
    wap = _wap()
    assert wap.run_key("build/prepared/2026-09-06/abc-123") == "prepared-abc-123"
    assert wap.run_key("build/reporting/2026-09-06/abc-123") == "reporting-abc-123"


def test_a_branch_is_slugged_whole_not_sliced():
    from datetime import datetime

    wap = _wap()
    b = wap.branch_name("prepared",
                        "dataset_triggered__2026-08-21T10:15:55.674897+00:00",
                        now=datetime(2026, 8, 21))
    assert b == ("build/prepared/2026-08-21/"
                 "dataset-triggered-2026-08-21T10-15-55-67"), b
    assert wap.purpose_of(b) == "prepared"
    for bad in (lambda: wap.branch_name("raw", "x"),
                lambda: wap.branch_name("prepared", "::"),
                lambda: wap.purpose_of("ingest/fo_trade/x")):
        try:
            bad()
        except ValueError:
            continue
        raise AssertionError("accepted a name that is not a build")


# ----------------------------------------------------- publish, against fakes
class _Recorder:
    """Every outside call publish() makes, in order, as (name, detail)."""

    def __init__(self, *, refuse_gate=False, artifacts_ok=True):
        self.calls: list[tuple] = []
        self.refuse_gate = refuse_gate
        self.artifacts_ok = artifacts_ok

    def install(self):
        from reporting_platform.common import context, spark_task
        from reporting_platform.registry import artifacts, lifecycle, runs

        rec = self
        saved = []

        def patch(mod, name, value):
            saved.append((mod, name, getattr(mod, name)))
            setattr(mod, name, value)

        class Nessie:
            def merge(self, branch, into, message, properties):
                rec.calls.append(("merge", branch, message))

            def get_reference(self, name):
                return {"reference": {"hash": "abc123"}}

            def delete_reference(self, name):
                rec.calls.append(("delete", name))

            def create_tag(self, tag, from_ref):
                rec.calls.append(("tag", tag))

        def require_complete(run_id, attempts):
            rec.calls.append(("artifacts", run_id, list(attempts)))
            if not rec.artifacts_ok:
                raise RuntimeError("dbt artifact retention is incomplete")

        def spark_run(*args):
            rec.calls.append(("spark", args))
            return {"max_cob_date": "2026-09-23",
                    "inputs": [["fo_trade", "d1"], ["ref_rating", "d2"]]}

        def check_publishable(report, as_at, candidate):
            rec.calls.append(("gate", report, as_at))
            if rec.refuse_gate:
                raise lifecycle.LifecycleRefused(f"{report} is submitted")
            return {"carried_forward": False}

        patch(context, "Nessie", Nessie)
        patch(context, "reports", lambda: {"exposure_by_country": {}})
        patch(spark_task, "run", spark_run)
        patch(artifacts, "require_complete", require_complete)
        patch(lifecycle, "check_publishable", check_publishable)
        patch(runs, "record_inputs",
              lambda run_id, inputs: rec.calls.append(("inputs", sorted(inputs))))
        patch(runs, "allocate_version",
              lambda report, as_at, run_id, tag: rec.calls.append(("version", report)) or 1)
        patch(runs, "finish_run",
              lambda run_id, status, **kw: rec.calls.append(("finish", status)))
        return saved

    @staticmethod
    def uninstall(saved):
        for mod, name, value in reversed(saved):
            setattr(mod, name, value)

    def names(self):
        return [c[0] for c in self.calls]


def _publish(rec, **kw):
    wap = _wap()
    saved = rec.install()
    try:
        return wap.publish(BRANCH, dbt_attempts=[("dbt.build", 1)], **kw)
    finally:
        rec.uninstall(saved)


def test_publish_verifies_reads_and_gates_before_main_moves():
    """The whole reason the sequence is written down once. Evidence, then
    the input set (which makes the as-at date knowable), then the lifecycle
    gate -- and only then the merge, the per-report tag and its version."""
    rec = _Recorder()
    out = _publish(rec, change_ref="CHG-9")
    assert rec.names() == ["artifacts", "spark", "inputs", "gate", "merge",
                           "delete", "tag", "version", "finish"], rec.calls
    assert rec.calls[1] == ("spark", ("run-inputs", BRANCH))
    merge = rec.calls[rec.names().index("merge")]
    assert "CHG-9" in merge[2] and "2026-09-23" in merge[2], merge
    assert rec.calls[rec.names().index("finish")] == ("finish", "published")
    assert out["published"] == [{"report": "exposure_by_country",
                                 "tag": rec.calls[6][1], "version": 1}]
    assert rec.calls[6][1].startswith("published/exposure_by_country/2026-09-23/")


def test_a_lifecycle_refusal_stops_the_publication_with_main_untouched():
    """LifecycleRefused is the one error publish must NOT swallow. Caught by
    name: `config_dir()` reloads the package, so a class imported here first
    would be a different object from the one raised."""
    rec = _Recorder(refuse_gate=True)
    try:
        _publish(rec)
    except Exception as exc:                                    # noqa: BLE001
        assert type(exc).__name__ == "LifecycleRefused", repr(exc)
    else:
        raise AssertionError("a refused as-at date was published")
    assert "merge" not in rec.names() and "delete" not in rec.names(), rec.calls


def test_missing_dbt_evidence_refuses_before_anything_is_read_or_merged():
    rec = _Recorder(artifacts_ok=False)
    try:
        _publish(rec)
    except RuntimeError as exc:
        assert "incomplete" in str(exc)
    else:
        raise AssertionError("published without its dbt artifacts")
    assert rec.names() == ["artifacts"], rec.calls


def test_publish_refuses_with_no_dbt_invocations_at_all():
    wap = _wap()
    try:
        wap.publish(BRANCH, dbt_attempts=[])
    except RuntimeError as exc:
        assert "no successful dbt" in str(exc)
    else:
        raise AssertionError("published a build nothing ran")


def test_a_prepared_build_merges_but_publishes_no_report():
    """A prepared build is a run, not a publication: no gate, no tag."""
    wap = _wap()
    rec = _Recorder()
    saved = rec.install()
    try:
        wap.publish("build/prepared/2026-09-24/r1",
                    dbt_attempts=[("dbt.build", 1)])
    finally:
        rec.uninstall(saved)
    assert rec.names() == ["artifacts", "spark", "inputs", "merge", "delete",
                           "finish"], rec.calls


# ------------------------------------------- publishing on dbt's own results
def _archived(results: list[dict], tries=(1,)):
    """An S3 holding one archived hand-run attempt per try number."""
    s3 = FakeS3()
    for n in tries:
        base = f"dbt-artifacts/reporting-r1/dbt.build/attempt-{n}/"
        s3.put(base + "manifest.json", "{}")
        s3.put(base + "run_results.json", json.dumps({"results": results}))
    return s3


def _verified(s3):
    config_dir()
    from reporting_platform.registry import artifacts
    from reporting_platform.transform import dbt

    saved = artifacts._client, artifacts._bucket
    artifacts._client, artifacts._bucket = (lambda: s3), (lambda: "lakehouse")
    try:
        return dbt.verified_attempt(BRANCH)
    finally:
        artifacts._client, artifacts._bucket = saved


def test_publish_by_hand_needs_every_node_to_have_passed():
    """`transform publish` is a separate command from `transform dbt`, so it
    publishes on the archived evidence, not on the invoker's word. A SKIP is
    a model dbt never built because something upstream failed."""
    ok = [{"unique_id": "model.a", "status": "success"},
          {"unique_id": "test.b", "status": "pass"},
          {"unique_id": "test.c", "status": "warn"}]
    assert _verified(_archived(ok, tries=(1, 2))) == ("dbt.build", 2)
    for bad in ("fail", "error", "skipped"):
        s3 = _archived(ok + [{"unique_id": "model.x", "status": bad}])
        try:
            _verified(s3)
        except RuntimeError as exc:
            assert f"model.x={bad}" in str(exc), str(exc)
        else:
            raise AssertionError(f"published over a {bad!r} node")
    for empty in (_archived([]), FakeS3()):
        try:
            _verified(empty)
        except RuntimeError:
            continue
        raise AssertionError("published with no dbt results at all")


def test_a_rebuild_of_one_branch_is_a_new_attempt():
    config_dir()
    from reporting_platform.registry import artifacts

    s3 = _archived([], tries=(1, 3))
    assert artifacts.attempts("reporting-r1", "dbt.build",
                              client=s3, bucket="lakehouse") == [1, 3]
    assert artifacts.attempts("reporting-r2", "dbt.build",
                              client=s3, bucket="lakehouse") == []


# ----------------------------------------------------- the DAG is the caller
def test_the_build_dag_delegates_rather_than_carrying_a_second_copy():
    """The DAG supplies what only Airflow knows and calls `wap`. A merge or a
    lifecycle check written back into it is the drift this module exists to
    end: the hand-run and scheduled builds would publish differently."""
    text = (DAGS / "dbt_builds.py").read_text(encoding="utf-8")
    for call in ("wap.open_build(", "wap.publish(", "wap.fail_build(",
                 "archive_invocation("):
        assert call in text, f"dbt_builds.py no longer calls {call}"
    for copy in ("n.merge(", ".merge(", "check_publishable", "create_tag(",
                 "allocate_version", "require_complete"):
        assert copy not in text, f"dbt_builds.py does {copy} itself again"


def test_a_non_spark_target_is_refused():
    import os

    config_dir()
    from reporting_platform.transform import dbt

    saved = os.environ.get("DBT_TARGET")
    try:
        os.environ["DBT_TARGET"] = "duckdb"
        try:
            dbt.target()
        except RuntimeError as exc:
            assert "not a Spark target" in str(exc)
        else:
            raise AssertionError("a DuckDB target was accepted")
        os.environ["DBT_TARGET"] = "spark_ocp"
        assert dbt.target() == "spark_ocp"
    finally:
        if saved is None:
            os.environ.pop("DBT_TARGET", None)
        else:
            os.environ["DBT_TARGET"] = saved
