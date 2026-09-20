"""Per-task dbt artifacts survive the shared target directory."""
from __future__ import annotations

import pathlib
import tempfile

from tests.fakes3 import FakeS3
from tests.support import DAGS


def test_artifacts_are_immutable_and_associated_with_the_run():
    from reporting_platform.registry import artifacts

    target = pathlib.Path(tempfile.mkdtemp(prefix="rp-dbt-artifacts-"))
    (target / "manifest.json").write_text('{"metadata":{"invocation_id":"i1"}}', encoding="utf-8")
    (target / "run_results.json").write_text('{"metadata":{"invocation_id":"i1"}}', encoding="utf-8")
    (target / "catalog.json").write_text('{"nodes":{}}', encoding="utf-8")
    s3 = FakeS3()
    first = artifacts.archive("reporting-r1", "dbt.model", 1,
                              target_path=target, client=s3, bucket="lakehouse")
    assert first["reference"] == "s3://lakehouse/dbt-artifacts/reporting-r1/"
    assert len(first["written"]) == 3 and not first["missing"]
    # Callback retry is safe only for identical evidence.
    artifacts.archive("reporting-r1", "dbt.model", 1,
                      target_path=target, client=s3, bucket="lakehouse")
    checked = artifacts.require_complete(
        "reporting-r1", [("dbt.model", 1)], client=s3, bucket="lakehouse")
    assert checked["artifacts_checked"] == 2


def test_publication_guard_reports_the_missing_run_results():
    from reporting_platform.registry import artifacts

    target = pathlib.Path(tempfile.mkdtemp(prefix="rp-dbt-artifacts-"))
    (target / "manifest.json").write_text("{}", encoding="utf-8")
    s3 = FakeS3()
    result = artifacts.archive("prepared-r1", "dbt.model", 2,
                               target_path=target, client=s3, bucket="lakehouse")
    assert result["missing"] == ["run_results.json"]
    try:
        artifacts.require_complete(
            "prepared-r1", [("dbt.model", 2)], client=s3, bucket="lakehouse")
    except RuntimeError as exc:
        assert "run_results.json" in str(exc) and "refusing to publish" in str(exc)
    else:
        raise AssertionError("publication accepted an incomplete dbt artifact set")


def test_artifact_and_input_guards_run_before_the_nessie_merge():
    """Publication cannot move main before retaining artifacts or inputs."""
    source = (DAGS / "dbt_builds.py").read_text(encoding="utf-8")
    publish = source[source.index("def publish("):]
    merge = publish.index("n.merge(")
    assert publish.index("artifacts.require_complete") < merge
    assert publish.index('inputs = _spark_run("run-inputs", branch)') < merge
