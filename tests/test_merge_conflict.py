"""`ingest()`'s merge conflict, at the smallest level that does not mock all
of `ingest()`.

Nessie refuses a merge that would silently overwrite a key another commit
already changed on `main` since the branch was cut -- see
tests/test_nessie_merge.py and docs/DECISIONS.md#a-merge-conflict-not-the-
pool-keeps-file-version-unique for the live 409 this reacts to. `ingest()`
delegates its `nessie.merge(...)` call to `_merge_ingest_branch`, which is
the seam pinned here.

REFERENCE_CONFLICT is not exclusively "another write raced this one" --
Nessie returns the same error code for a moved/recreated reference or "No
common ancestor", so `_merge_ingest_branch` may only claim the concurrent-
write story when Nessie's OWN message names this table's content key
(`raw.fo_trade` for `lakehouse.raw.fo_trade`). Both branches are pinned
below: the key present (claim made, live 409 shape) and the key absent (no
claim, message quoted verbatim instead).

Also pinned: the message must not contradict what Airflow actually does.
`feed_ingest.py`'s `DEFAULT_ARGS` sets `retries: 2`, so after this exception
the scheduler retries the SAME task automatically -- it does not wait to be
told. Both retries reuse the same branch name (deterministic from
feed/cob_date/run_id) and `ingest()`'s `nessie.create_branch` has no
`exist_ok`, so both 409 with a bare "already exists" that never mentions
`_file_version` or this conflict. So the message must say: the cause is in
THIS attempt's log because Airflow's own retries will fail elsewhere: and
recovery is a NEW run (new DAG run or CLI invocation, each with its own run
id and so its own branch) -- not deleting the old branch, which is optional
inspection, not a precondition.
"""
from __future__ import annotations

from datetime import date

from tests.support import feeds_from


def _fd():
    feeds, _ = feeds_from()
    return feeds["fo_trade"]


def _response(message: str, status_code: int = 409,
             error_code: str = "REFERENCE_CONFLICT"):
    """Stands in for `requests.Response`: the two attributes
    `_merge_ingest_branch` reads off a failed merge."""
    body = {"status": status_code, "reason": "Conflict",
            "message": message, "errorCode": error_code}

    class _R:
        pass

    r = _R()
    r.status_code = status_code
    r.json = lambda: body
    return r


class _ConflictingNessie:
    """`.merge` always raises, shaped like `Nessie._req` raises it: a
    `requests.exceptions.HTTPError` carrying `.response`."""

    def __init__(self, response):
        self._response = response

    def merge(self, from_branch, into="main", **kw):
        import requests
        raise requests.exceptions.HTTPError(
            "409 Conflict for url http://fake/trees/main@x/history/merge: "
            "conflict", response=self._response)


def test_a_conflict_naming_the_table_blames_a_concurrent_write():
    """The live shape: Nessie's message names `raw.fo_trade`, the content key
    for `fd.raw_table` (`lakehouse.raw.fo_trade`) with the catalog stripped."""
    from reporting_platform.ingest.ingest_feed import _merge_ingest_branch

    fd = _fd()
    branch = "ingest/fo_trade/20260819/run1"
    nessie = _ConflictingNessie(_response(
        "The following keys have been changed in conflict: 'raw.fo_trade'"))
    try:
        _merge_ingest_branch(nessie, branch, fd, date(2026, 8, 19), 2)
    except Exception as exc:
        msg = str(exc)
        assert fd.raw_table in msg, msg
        assert "2026-08-19" in msg, msg
        assert "_file_version=2" in msg, msg
        assert branch in msg, msg
        assert "another write" in msg and "merged" in msg, msg
        # Nessie's own words are quoted, not paraphrased away.
        assert "raw.fo_trade" in msg, msg
        # Must not contradict Airflow's own retries: they WILL fire, and
        # they WILL fail elsewhere (create_branch), not recover anything.
        assert "retry" in msg.lower() or "retries" in msg.lower(), msg
        assert "create_branch" in msg, msg
        assert "already exists" in msg.lower(), msg
        # Recovery is a new run, not "delete the branch first".
        assert "new" in msg.lower() and "run" in msg.lower(), msg
        assert "after deleting" not in msg.lower(), msg
        assert branch in msg, msg
        import requests
        assert isinstance(exc.__cause__, requests.exceptions.HTTPError), exc.__cause__
    else:
        raise AssertionError(
            "a 409 REFERENCE_CONFLICT merged silently instead of raising")


def test_a_conflict_not_naming_the_table_makes_no_claim():
    """A REFERENCE_CONFLICT Nessie also uses for reasons that are not a
    version race (a moved/recreated ref, "No common ancestor", ...). The
    message must be quoted, not reinterpreted as "another write merged"."""
    from reporting_platform.ingest.ingest_feed import _merge_ingest_branch

    fd = _fd()
    branch = "ingest/fo_trade/20260819/run1"
    other_message = "No common ancestor in parents of abc123 and def456"
    nessie = _ConflictingNessie(_response(other_message))
    try:
        _merge_ingest_branch(nessie, branch, fd, date(2026, 8, 19), 2)
    except Exception as exc:
        msg = str(exc)
        assert other_message in msg, msg
        assert "another write" not in msg, msg
        assert "merged into main since" not in msg, msg
        # The retry/recovery guidance is unconditional -- present either way.
        assert "new" in msg.lower() and "run" in msg.lower(), msg
        assert "create_branch" in msg, msg
        assert "after deleting" not in msg.lower(), msg
    else:
        raise AssertionError(
            "a 409 REFERENCE_CONFLICT merged silently instead of raising")


def test_a_non_conflict_error_is_not_reinterpreted():
    from reporting_platform.ingest.ingest_feed import _merge_ingest_branch
    import requests

    class _OtherErrorNessie:
        def merge(self, from_branch, into="main", **kw):
            raise requests.exceptions.HTTPError(
                "boom", response=_response(
                    "Internal Server Error", status_code=500,
                    error_code="UNKNOWN"))

    fd = _fd()
    try:
        _merge_ingest_branch(_OtherErrorNessie(), "ingest/x", fd,
                             date(2026, 8, 19), 1)
    except requests.exceptions.HTTPError:
        pass
    else:
        raise AssertionError(
            "a non-conflict HTTPError should propagate unchanged")
