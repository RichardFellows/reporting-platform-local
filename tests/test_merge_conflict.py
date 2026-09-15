"""`ingest()`'s merge conflict, at the smallest level that does not mock all
of `ingest()`.

Nessie refuses a merge that would silently overwrite a key another commit
already changed on `main` since the branch was cut -- see
tests/test_nessie_merge.py and docs/DECISIONS.md#a-merge-conflict-not-the-
pool-keeps-file-version-unique for the live 409 this reacts to. `ingest()`
delegates its `nessie.merge(...)` call to `_merge_ingest_branch`, which is
the seam pinned here: given a Nessie stand-in whose `merge` raises the
measured 409, does `ingest()`'s caller get a message naming the table, the
COB date, the version this attempt computed and the abandoned branch --
instead of either the bare HTTPError or (worse) a silent success.
"""
from __future__ import annotations

from datetime import date

from tests.support import feeds_from


def _fd():
    feeds, _ = feeds_from()
    return feeds["fo_trade"]


class _ConflictResponse:
    """Stands in for `requests.Response`: the two attributes
    `_merge_ingest_branch` reads off a 409."""

    status_code = 409

    def json(self):
        return {
            "status": 409, "reason": "Conflict",
            "message": "The following keys have been changed in conflict: "
                       "'raw.fo_trade'",
            "errorCode": "REFERENCE_CONFLICT",
        }


class _ConflictingNessie:
    """`.merge` always raises the measured 409, shaped like `Nessie._req`
    raises it: a `requests.exceptions.HTTPError` carrying `.response`."""

    def merge(self, from_branch, into="main", **kw):
        import requests
        raise requests.exceptions.HTTPError(
            "409 Conflict for url http://fake/trees/main@x/history/merge: "
            "conflict", response=_ConflictResponse())


class _OtherErrorNessie:
    """`.merge` raises an HTTPError that is NOT a 409 REFERENCE_CONFLICT --
    must pass through unchanged, not be reinterpreted as a version race."""

    class _Response:
        status_code = 500

        def json(self):
            return {"status": 500, "reason": "Internal Server Error"}

    def merge(self, from_branch, into="main", **kw):
        import requests
        raise requests.exceptions.HTTPError("boom", response=self._Response())


def test_a_409_reference_conflict_names_table_date_version_and_branch():
    from reporting_platform.ingest.ingest_feed import _merge_ingest_branch

    fd = _fd()
    branch = "ingest/fo_trade/20260819/run1"
    try:
        _merge_ingest_branch(_ConflictingNessie(), branch, fd,
                             date(2026, 8, 19), 2)
    except Exception as exc:
        msg = str(exc)
        assert fd.raw_table in msg, msg
        assert "2026-08-19" in msg, msg
        assert "2" in msg, msg
        assert branch in msg, msg
        # Chained, not swallowed.
        import requests
        assert isinstance(exc.__cause__, requests.exceptions.HTTPError), exc.__cause__
    else:
        raise AssertionError(
            "a 409 REFERENCE_CONFLICT merged silently instead of raising")


def test_a_non_conflict_error_is_not_reinterpreted():
    from reporting_platform.ingest.ingest_feed import _merge_ingest_branch
    import requests

    fd = _fd()
    try:
        _merge_ingest_branch(_OtherErrorNessie(), "ingest/x", fd,
                             date(2026, 8, 19), 1)
    except requests.exceptions.HTTPError:
        pass
    else:
        raise AssertionError(
            "a non-conflict HTTPError should propagate unchanged")
