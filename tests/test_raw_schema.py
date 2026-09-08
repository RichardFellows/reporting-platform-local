"""What `plan_raw_schema` decides when a feed's declared columns change.

ADDING A COLUMN TO AN EXISTING FEED IS THE ORDINARY EVENT in the life of a
feed -- an upstream extends its extract -- and until this was pinned the
migration for it did not exist: `ensure_raw_table` is CREATE TABLE IF NOT
EXISTS, so the raw table kept the old schema and the next ingest failed inside
`writeTo().append()` on every delivery from then on.

The decision is PURE -- (declared columns, table columns) in, (add, orphaned)
out -- so it is pinned here, in a second, with no catalog. Applying it is
`ensure_raw_schema`, which is Spark and is verified by running it: whether
Iceberg's ALTER TABLE ADD COLUMNS commits on a Nessie branch is not something
a test in this directory can honestly claim. See tests/README.md.
"""
from __future__ import annotations

from tests.support import feeds_from

# Ingest's own columns, as `ensure_raw_table` writes them. Spelled out HERE
# rather than imported, because the point of these cases is that
# `plan_raw_schema` does not consult a list of them -- it derives "the
# platform's" from the `_` prefix. A copy that drifts from the DDL is exactly
# what makes these cases worth having.
PLATFORM = [
    ("_extra_columns", "map<string,string>"),
    ("_business_date", "date"),
    ("_ingest_ts", "timestamp"),
    ("_source_file", "string"),
    ("_file_version", "int"),
    ("_row_number", "bigint"),
    ("_batch_id", "string"),
    ("_delivery_id", "string"),
    ("_received_at", "timestamp"),
    ("_schema_version", "string"),
    ("_source_system", "string"),
]


def _plan(fd, table_columns):
    from reporting_platform.ingest.ingest_feed import plan_raw_schema
    return plan_raw_schema(fd, table_columns)


def _current(fd):
    """The table as the current contract would have created it."""
    return [(c, "string") for c in fd.columns] + PLATFORM


def test_a_table_matching_the_contract_needs_nothing():
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    plan = _plan(fd, _current(fd))
    assert plan == {"add": [], "orphaned": []}, plan


def test_a_newly_declared_column_is_added():
    """THE CASE THIS EXISTS FOR. The upstream extends its extract, the column
    goes into feeds.yml, and the raw table has to gain it."""
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    table = [(c, "string") for c in fd.columns if c != "book"] + PLATFORM

    plan = _plan(fd, table)
    assert plan["add"] == [("book", "STRING")], plan["add"]
    # And nothing is taken away to make room for it.
    assert plan["orphaned"] == [], plan["orphaned"]


def test_a_missing_provenance_column_is_still_added():
    """The original job of this migration, unchanged by the wider one."""
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    table = [(c, "string") for c in fd.columns] + [
        (n, t) for n, t in PLATFORM if n != "_delivery_id"]

    plan = _plan(fd, table)
    assert plan["add"] == [("_delivery_id", "STRING")], plan["add"]


def test_an_undeclared_column_is_orphaned_and_never_added():
    """A column the table has and feeds.yml no longer declares.

    Reported, written as NULL, never dropped: dropping it would delete history
    to satisfy a config edit.
    """
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    plan = _plan(fd, _current(fd) + [("legacy_ref", "string")])

    assert plan["orphaned"] == [("legacy_ref", "string")], plan["orphaned"]
    assert plan["add"] == [], plan["add"]


def test_the_platforms_own_columns_are_never_orphaned():
    """THE RULE IS THE `_` PREFIX, not a second list of ingest's DDL.

    Same rule as `lineage/columns.py:ingest_columns`, which is why an orphan
    here is the same column `lineage --columns` calls `unresolved`. A list
    here would be free to drift from both.
    """
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    plan = _plan(fd, _current(fd) + [("_a_column_added_later", "string")])
    assert plan == {"add": [], "orphaned": []}, plan


def test_a_renamed_column_reads_as_an_add_and_an_orphan():
    """The ambiguous edit, and why the two directions are not symmetrical.

    Renaming `trade_id` to `trade_ref` in feeds.yml is indistinguishable from
    dropping one column and adding another -- so the destructive reading is
    the one nothing acts on. The new column is added; the old one is kept,
    holding its history, and reported for a human to settle.
    """
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    table = [(("trade_ref" if c == "trade_id" else c), "string")
             for c in fd.columns] + PLATFORM

    plan = _plan(fd, table)
    assert plan["add"] == [("trade_id", "STRING")], plan["add"]
    assert plan["orphaned"] == [("trade_ref", "string")], plan["orphaned"]


def test_the_comparison_is_case_insensitive():
    """Iceberg resolves column names case-insensitively, so a contract that
    differs from the table only in case must not add a duplicate."""
    feeds, _ = feeds_from()
    fd = feeds["fo_trade"]
    table = [(c.upper(), "string") for c in fd.columns] + PLATFORM
    plan = _plan(fd, table)
    assert plan["add"] == [], plan["add"]
    assert plan["orphaned"] == [], plan["orphaned"]
