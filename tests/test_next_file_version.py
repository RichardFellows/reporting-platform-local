"""`next_file_version` must fail the ingest on a read failure, not guess.

The one call site (`ingest`, in `ingest_feed.py`) runs this straight after
`ensure_raw_table` and `ensure_raw_schema` have created and reconciled the
table on THIS branch (see `next_file_version`'s own docstring). So by the
time this runs, a failure reading it --
`TABLE_OR_VIEW_NOT_FOUND` included -- means a wrong ref or a wrong name, not
a first delivery. It used to catch every exception and return `1`, which is
exactly CLAUDE.md's "a subject it could not READ is not a subject that is
EMPTY": an unreadable table and a genuinely empty one both produced the same
answer, and the genuinely-empty case is already handled correctly by the
`COALESCE(MAX(_file_version), 0)` inside the query.

These are pure-Python stand-ins for `spark.sql(...).collect()` -- not a claim
that Spark behaves this way, only that `next_file_version` reacts correctly
to what its one call site would see. See tests/README.md.
"""
from __future__ import annotations

from datetime import date

from tests.support import feeds_from


def _fd():
    feeds, _ = feeds_from()
    return feeds["fo_trade"]


class _RaisingSpark:
    """`.sql` always raises, the way a bad ref or a catalog error would."""

    def sql(self, query):
        raise RuntimeError("boom: TABLE_OR_VIEW_NOT_FOUND")


class _RowSpark:
    """Returns one fixed `_file_version` row, shaped like Spark's `collect()`
    (a mapping indexable by column name)."""

    def __init__(self, v):
        self._v = v

    def sql(self, query):
        return self

    def collect(self):
        return [{"v": self._v}]


def test_a_read_failure_raises_naming_table_and_date():
    from reporting_platform.ingest.ingest_feed import next_file_version

    fd = _fd()
    table = "raw_fo.fo_trade@ingest/fo_trade/20260811/run1"
    try:
        next_file_version(_RaisingSpark(), fd, date(2026, 8, 11), table=table)
    except Exception as exc:
        msg = str(exc)
        assert table in msg, msg
        assert "2026-08-11" in msg, msg
    else:
        raise AssertionError(
            "an unreadable raw table returned a version instead of raising")


def test_an_empty_table_is_version_1():
    from reporting_platform.ingest.ingest_feed import next_file_version

    fd = _fd()
    assert next_file_version(_RowSpark(0), fd, date(2026, 8, 11)) == 1


def test_an_existing_version_increments():
    from reporting_platform.ingest.ingest_feed import next_file_version

    fd = _fd()
    assert next_file_version(_RowSpark(3), fd, date(2026, 8, 11)) == 4
