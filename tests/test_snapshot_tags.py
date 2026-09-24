"""A snapshot tag names the commit its ingest's merge made -- on both paths.

The tag used to be cut from `main`'s head, which is the ingest's state only
until the next merge: Airflow tags after the ingest has let go of the write
pool, and the batch command tagged every delivery after merging all of them.
And the Transport path cut no tag at all. These pin the rule and both
paths' use of it. docs/DECISIONS.md#a-snapshot-tag-names-its-merge-commit
"""
from __future__ import annotations

import contextlib
import inspect

from tests.support import DAGS


class _FakeNessie:
    def __init__(self, fail=None):
        self.tags = []
        self.fail = fail

    def create_tag(self, name, from_ref="main", hash=None):
        if self.fail:
            raise self.fail
        self.tags.append((name, from_ref, hash))
        return {}


@contextlib.contextmanager
def _nessie(fake):
    from reporting_platform.common import context

    saved = context.Nessie
    context.Nessie = lambda: fake
    try:
        yield fake
    finally:
        context.Nessie = saved


def _result(**over):
    return {"cob_date": "2026-09-14", "run_id": "run-1", "commit": "c0ffee" * 10,
            "already_ingested": False, **over}


def test_the_tag_is_cut_at_the_merge_commit_not_mains_head():
    from reporting_platform.ingest import steps

    with _nessie(_FakeNessie()) as fake:
        got = steps.record_snapshot("qa_happy_position", _result())
    assert fake.tags == [("snapshot/qa_happy_position/2026-09-14/run-1",
                          "main", "c0ffee" * 10)]
    assert got["tag"] == "snapshot/qa_happy_position/2026-09-14/run-1"
    assert "tag_error" not in got


def test_a_delivery_raw_already_held_gets_no_tag():
    from reporting_platform.ingest import steps

    with _nessie(_FakeNessie()) as fake:
        got = steps.record_snapshot("f", _result(already_ingested=True, commit=None))
    assert fake.tags == [] and got["tag"] is None


def test_no_commit_is_not_tagged_at_the_head_instead():
    """The fallback IS the bug this replaced: it would pin later merges."""
    from reporting_platform.ingest import steps

    with _nessie(_FakeNessie()) as fake:
        got = steps.record_snapshot("f", _result(commit=None))
    assert fake.tags == []
    assert "no merge commit" in got["tag_error"]


def test_a_tag_that_cannot_be_cut_is_reported_not_raised():
    from reporting_platform.ingest import steps

    with _nessie(_FakeNessie(fail=RuntimeError("409 already exists"))):
        got = steps.record_snapshot("f", _result())
    assert "409" in got["tag_error"]


def test_after_ingest_reports_drift_then_tags():
    from reporting_platform.ingest import steps

    with _nessie(_FakeNessie()) as fake:
        got = steps.after_ingest("f", _result(columns_added=["new_col"]))
    assert len(fake.tags) == 1 and got["tag"]


def test_the_batch_command_tags_through_after_ingest():
    from reporting_platform.ingest import steps

    body = inspect.getsource(steps.ingest)
    assert "after_ingest(feed_name, result)" in body
    assert "record_snapshot(" not in body       # no second path to a tag


def test_the_ingest_returns_the_commit_its_merge_made():
    from reporting_platform.ingest import ingest_feed

    source = inspect.getsource(ingest_feed._ingest_manifest)
    # Through `_merge_ingest_branch`, which returns the merge response whole.
    assert ("commit = _merge_ingest_branch(\n"
            "                nessie, branch, fd, bdate, version)"
            '.get("resultantTargetHash")' in source)
    merge = inspect.getsource(ingest_feed._merge_ingest_branch)
    assert 'return nessie.merge(branch, into="main")' in merge
    assert '"commit": commit,' in source
    assert '"commit": None,' in source          # the already-ingested result


def test_transport_ingest_ends_with_the_inbox_dags_two_tasks():
    source = (DAGS / "transport_ingest.py").read_text(encoding="utf-8")
    assert "from reporting_platform.ingest.steps import drift_warnings" in source
    assert "from reporting_platform.ingest.steps import record_snapshot as pin" in source
    assert "report_drift(ingested) >> record_snapshot(ingested)" in source
    summary = source[source.index("def ingest_raw_task("):
                     source.index('@task(task_id="report_drift")')]
    for key in ('"run_id"', '"commit"', '"columns_added"'):
        assert key in summary, key
