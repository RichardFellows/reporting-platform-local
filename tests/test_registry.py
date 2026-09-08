"""The delivery registry's pure parts: what a row says, and what a key means.

WHAT THESE COVER AND WHAT THEY DELIBERATELY DO NOT. `observations()` is a pure
projection -- (feed, manifest, sidecar, md5, bucket) in, a row out -- so it can
be pinned here exactly. The same is true of `Feed.schema_version`, the
quarantine key shape and the date this platform reads back out of it.

Everything that talks to Postgres is verified by RUNNING it, not mocked here.
A fake database would agree with whatever this code asked it, which is the one
thing a registry test must not do; see tests/README.md. The insert, the
conflict clause, the sequence and the reconcile gap-fill were exercised against
the live stack.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from tests.support import config_dir, feeds_from, synthetic

MANIFEST = {
    "manifest_version": 1,
    "feed": "fo_trade",
    "cob_date": "2026-08-11",
    "delivery_id": "TRADE_20260811.csv",
    "received_at": "2026-08-11T06:00:00+00:00",
    "source_object": "landing/fo_trade/TRADE_20260811.csv",
    "parts": [{"object_key": "landing/fo_trade/TRADE_20260811.csv",
               "bytes": 25}],
    "format": {"delimiter": ",", "quote_char": '"', "header": True,
               "encoding": "utf-8"},
    "control_object": None,
    "declared_row_count": None,
    "declared_md5": None,
    "normalizer": "file/v1",
}


def _observations(sidecar=None, manifest=None):
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.registry.deliveries import observations
    fd = feeds()["fo_trade"]
    return fd, observations(fd, manifest or MANIFEST, sidecar, "d41d8c" * 5 + "ff",
                            "lakehouse")


# ------------------------------------------------------------ the projection
def test_a_direct_delivery_projects_to_a_row():
    fd, row = _observations()
    assert row["feed"] == "fo_trade"
    assert row["delivery_id"] == "TRADE_20260811.csv"
    assert row["source_system"] == fd.source_system
    assert row["cob_date"] == date(2026, 8, 11)
    assert row["bytes"] == 25
    assert row["normalizer"] == "file/v1"
    assert row["parts"] == [{"part_no": 0, "bytes": 25,
                             "object_key": MANIFEST["source_object"]}]


def test_no_sidecar_means_the_upstream_wrote_to_landing_itself():
    """The two ways into `landing/`, told apart by the sidecar's own field.

    A feed with no `arrival:` block -- which is every feed shipped today --
    has no metadata sibling at all, and calling that `direct` rather than
    leaving it null is the difference between "an approved sender put it
    there" and "we do not know how it got there".
    """
    _, row = _observations()
    assert row["origin"] == "direct"
    assert row["source_filename"] is None
    # The bucket is NOT in the manifest -- `source_object` is a key -- so a
    # URI built without it names a bucket called `landing`.
    assert row["origin_uri"] == \
        "s3://lakehouse/landing/fo_trade/TRADE_20260811.csv"


def test_a_gated_delivery_keeps_the_name_the_upstream_used():
    _, row = _observations(sidecar={
        "promoted_by": "inbox", "source_filename": "POS.TXT",
        "md5": "abc", "declared": {"producer_run_id": "UP-42"}})
    assert row["origin"] == "inbox"
    assert row["source_filename"] == "POS.TXT"
    assert row["origin_uri"] == "inbox:POS.TXT"
    assert row["producer_run_id"] == "UP-42"


def test_the_row_carries_no_verdict():
    """REQ-101 as amended: observations only.

    The whole difference between this and the `stg` load-control tables the
    platform refuses. Written as a test rather than a comment because it is a
    rule about what MUST NOT be added, and a comment does not fail when
    somebody adds it.
    """
    _, row = _observations()
    forbidden = {"ingested", "superseded", "status", "state", "ok", "valid",
                 "processed", "loaded"}
    assert not forbidden & set(row), forbidden & set(row)


def test_an_archive_delivery_sizes_itself_from_its_members():
    """Not from the container, which is never landed."""
    manifest = {**MANIFEST, "normalizer": "archive/v1", "parts": [
        {"object_key": "ready/fo_trade/box/a.csv", "bytes": 10},
        {"object_key": "ready/fo_trade/box/b.csv", "bytes": 15}]}
    _, row = _observations(manifest=manifest)
    assert row["bytes"] == 25
    assert [p["part_no"] for p in row["parts"]] == [0, 1]


# --------------------------------------------------------- the schema stamp
def test_schema_version_changes_with_the_column_contract():
    base, _ = feeds_from(synthetic())
    renamed, _ = feeds_from(synthetic().replace("columns: [k, v]",
                                                "columns: [k, w]"))
    reordered, _ = feeds_from(synthetic().replace("columns: [k, v]",
                                                  "columns: [v, k]"))
    assert base["t_one"].schema_version != renamed["t_one"].schema_version
    assert base["t_one"].schema_version != reordered["t_one"].schema_version


def test_schema_version_follows_the_source_name_not_only_the_platform_one():
    """A column renamed IN THE FILE is a schema change, even though the
    platform name it lands under is unchanged."""
    base, _ = feeds_from(synthetic())
    aliased, _ = feeds_from(
        synthetic().replace("columns: [k, v]",
                            'columns:\n      - k\n      - v: "V Col"'))
    assert base["t_one"].schema_version != aliased["t_one"].schema_version


def test_schema_version_ignores_prepared_layer_typing():
    """`column_types` says what the PREPARED model does with a column, not
    what the file contains. Retyping one in the console must not look like the
    upstream having changed its schema."""
    base, _ = feeds_from(synthetic())
    typed, _ = feeds_from(synthetic() +
                          "\n    column_types: {v: decimal}\n")
    assert base["t_one"].schema_version == typed["t_one"].schema_version


def test_schema_version_is_stable_across_loads():
    a, _ = feeds_from(synthetic())
    b, _ = feeds_from(synthetic())
    assert a["t_one"].schema_version == b["t_one"].schema_version
    assert len(a["t_one"].schema_version) == 12


# ------------------------------------------------------------- quarantine
def test_quarantine_key_carries_the_rejection_date():
    config_dir()
    from reporting_platform.common.context import feeds
    from reporting_platform.registry.rejections import quarantine_key
    from reporting_platform.retention.quarantine import rejected_on

    when = datetime(2026, 3, 11, 9, 0, tzinfo=timezone.utc)
    key = quarantine_key(feeds()["fo_trade"], "POS.TXT", when)
    assert key == "quarantine/fo_trade/2026/03/20260311T090000Z_POS.TXT", key
    # The whole point of putting the date in the key: retention can read it
    # back with no lookup, on a file that frequently has no parsable name.
    assert rejected_on(key) == date(2026, 3, 11)


def test_a_file_no_feed_claimed_still_gets_a_key():
    from reporting_platform.registry.rejections import quarantine_key
    from reporting_platform.retention.quarantine import rejected_on

    key = quarantine_key(None, "whatever.csv",
                         datetime(2026, 3, 11, 9, 0, tzinfo=timezone.utc))
    assert key.startswith("quarantine/_unclaimed/2026/03/")
    assert rejected_on(key) == date(2026, 3, 11)


def test_a_hostile_filename_cannot_escape_the_prefix():
    """The same traversal `normalize._safe_member_name` refuses, in the other
    place a caller-supplied name becomes part of a key. The inbox is a
    directory anyone can write to."""
    from reporting_platform.registry.rejections import quarantine_key

    for name in ("../../etc/passwd", "a/b.csv", "..", ""):
        key = quarantine_key(None, name,
                             datetime(2026, 3, 11, 9, 0, tzinfo=timezone.utc))
        rest = key[len("quarantine/_unclaimed/2026/03/"):]
        assert "/" not in rest, key
        assert key.startswith("quarantine/_unclaimed/2026/03/"), key


def test_retention_refuses_a_key_it_did_not_write():
    """Nothing in `quarantine/` is deleted on a guess -- the same rule
    `landing.py` follows, on a prefix where nothing is nameable by contract."""
    from reporting_platform.retention.quarantine import rejected_on

    assert rejected_on("quarantine/fo_trade/hand_written.csv") is None
    # Folders and stamp disagreeing means this platform did not write it.
    assert rejected_on(
        "quarantine/fo_trade/2014/03/20140911T090000Z_x.csv") is None
    assert rejected_on("landing/fo_trade/TRADE_20260811.csv") is None


def test_an_unknown_rejection_class_is_refused():
    """`reason_class` is counted, not read, so a typo would silently create a
    fifth class that nothing aggregates."""
    config_dir()
    from reporting_platform.registry import rejections
    try:
        rejections.quarantine(None, "x.csv", b"x", reason_class="oops",
                              reason="r")
    except ValueError as exc:
        assert "oops" in str(exc)
    else:
        raise AssertionError("an unknown reason_class was accepted")


# ------------------------------------------- the boundary, at schema level
def _table_ddl(name: str) -> str:
    """The CREATE TABLE body for one registry table, out of db.SCHEMA."""
    import sys

    from tests.support import REPO
    sys.path.insert(0, str(REPO))
    from reporting_platform.registry import db

    start = db.SCHEMA.index(f"CREATE TABLE IF NOT EXISTS registry.{name} (")
    return db.SCHEMA[start:db.SCHEMA.index(");", start)]


def test_the_delivery_table_still_carries_no_verdict():
    """The projection test above covers what `observations()` builds; this
    covers the TABLE, which is where somebody would add a column without
    going anywhere near that function."""
    ddl = _table_ddl("delivery").lower()
    for forbidden in ("ingested", "superseded", " status", "processed"):
        assert forbidden not in ddl, forbidden


def test_a_run_does_carry_a_status_and_that_is_not_the_same_relaxation():
    """A delivery's status would be a VERDICT about something already true and
    derivable elsewhere -- which is how it drifts. A run's status is the
    record of how the run ended: nothing else knows it and nothing can derive
    it, so refusing to store it would simply lose it."""
    assert "status" in _table_ddl("run").lower()


def test_run_inputs_are_not_foreign_keyed_to_deliveries():
    """A foreign key would let a registry rebuild -- drop and reconcile from
    object storage -- CASCADE run history away: destroying the only copy of
    something to protect a table that has a second copy in storage."""
    ddl = _table_ddl("run_input")
    assert "REFERENCES registry.run (run_id)" in ddl
    # The CONSTRAINT, not the word: the table's comment names
    # `registry.delivery` precisely to say why it does not reference it.
    assert "REFERENCES registry.delivery" not in ddl


def test_a_report_version_is_keyed_per_report_and_as_at_date():
    """Decision 5. Numbering per run would move a report's version when an
    unrelated report was rebuilt; numbering per family would move it when a
    sibling was restated."""
    ddl = _table_ddl("report_version")
    assert "PRIMARY KEY (report, as_at_date, version_no)" in ddl


# ------------------------------------------ the as-at lifecycle (phase 6)
def test_the_transition_table_is_append_only_and_open_is_not_a_row():
    """`open` is the ABSENCE of a transition. Storing it would need every
    (report, date) pair seeded -- and that set is DERIVED from the exposures
    and the calendar, so a seeded table is a second list that goes stale the
    moment a report is added. The surrogate key is what makes it append-only:
    there is no (report, as_at_date) primary key to update in place."""
    ddl = _table_ddl("as_at_transition")
    assert "transition_id BIGSERIAL   PRIMARY KEY" in ddl
    assert "PRIMARY KEY (report, as_at_date)" not in ddl
    assert "'open'" not in ddl.lower().replace("no 'open' row", "")


def test_a_transition_cannot_be_written_without_an_actor_and_a_reason():
    """Enforced in the schema as well as in `transition()`, because the CLI is
    not the only thing that can reach this table -- and an unattributed lock
    is one nobody can ask about later. There is no identity provider here, so
    this record is the whole of the accountability."""
    ddl = _table_ddl("as_at_transition")
    assert "actor         TEXT        NOT NULL" in ddl
    assert "reason        TEXT        NOT NULL" in ddl
    # approved_by is deliberately nullable: requiring one for an ordinary lock
    # would make the one place it matters indistinguishable from routine.
    assert "approved_by   TEXT," in ddl


def test_a_transition_is_not_foreign_keyed_to_anything_rebuildable():
    """Same rule as `run_input`, and the reason is the same: transitions are
    the unrebuildable event family. A reference to `delivery` or to
    `report_version` would let a registry rebuild cascade away the only copy
    of who locked a date and why."""
    ddl = _table_ddl("as_at_transition")
    assert "REFERENCES registry.delivery" not in ddl
    assert "REFERENCES registry.report_version" not in ddl


def test_reconcile_does_not_touch_the_unrebuildable_tables():
    """`deliveries.reconcile()` is the rebuild path -- it is allowed to delete
    and rewrite observations. Runs, versions, submissions and now transitions
    are events that happened once, so a DELETE reaching one of them would
    destroy the only copy. Checked as text because the failure would be one
    added line in a module whose whole job is deleting and re-inserting."""
    from tests.support import REPO
    src = (REPO / "reporting_platform" / "registry" / "deliveries.py").read_text(
        encoding="utf-8")
    for table in ("registry.run", "registry.run_input", "registry.report_version",
                  "registry.submission", "registry.as_at_transition"):
        assert f"DELETE FROM {table}" not in src, table
        assert f"UPDATE {table}" not in src, table


def test_the_family_lives_on_the_submission_not_on_the_version():
    """Decision 5's other half: reports submitted together are a SUBMISSION
    concern, so grouping them must not touch how either one is numbered."""
    assert "family" in _table_ddl("submission")
    assert "family" not in _table_ddl("report_version")


# ------------------------------------------------- the dry-run write it made
class _NoDatabase(Exception):
    """Sentinel: the reconcile reached Postgres, which these tests do not have."""


def _reconcile_to_the_db_boundary(normalize_first: bool):
    """Run `deliveries.reconcile` over a fake bucket, stopping at `db.connect`.

    Everything before that boundary is object storage, which is exactly the
    half under test. The Postgres half is verified by running it -- see this
    module's header.
    """
    from tests.fakes3 import FakeS3, install, uninstall

    config_dir()
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.registry import db, deliveries

    fd = feeds()["fo_trade"]
    s3.put("landing/fo_trade/TRADE_20260811.csv", "trade_id\nT1\n")
    monkey.append((db, "connect", db.connect))

    def _refuse():
        raise _NoDatabase()

    db.connect = _refuse
    try:
        try:
            deliveries.reconcile(fd, normalize_first=normalize_first)
        except _NoDatabase:
            pass
        return {k for k in s3.objects if k.startswith("ready/")}
    finally:
        uninstall(monkey)


def test_a_reconcile_that_may_not_write_creates_no_manifest():
    """The defect: `platform_housekeeping` triggered with `{"dry_run": true}`
    wrote 116 manifests into `ready/` and registered 0 -- and invalidated its
    own forecast, since the sweep it predicted at 42 then removed 157. A dry
    run whose side effects change what the real run does is not a dry run.

    The registry ROWS are deliberately still written; only the object-storage
    write is withheld. See `deliveries.reconcile`.
    """
    assert _reconcile_to_the_db_boundary(normalize_first=False) == set()


def test_and_the_real_one_still_does():
    """The control. Without this the test above passes on a reconcile that
    stopped working."""
    assert _reconcile_to_the_db_boundary(normalize_first=True) == {
        "ready/fo_trade/TRADE_20260811.csv.json"}


def test_the_nightly_task_is_narrowed_by_dry_run_not_skipped():
    """`registry_reconcile` is the REBUILD path -- *events are an
    optimisation, the poll is the correctness guarantee* -- so gating the
    whole task on `dry_run` throws away the thing it is for. It must pass the
    flag down to the one write that leaks instead. Checked as text because the
    failure is a one-line `if p.get("dry_run"): return` somebody adds later.
    """
    from tests.support import REPO
    src = (REPO / "airflow" / "dags" / "platform_housekeeping.py").read_text(
        encoding="utf-8")
    body = src[src.index("def registry_reconcile"):src.index("def evidence_check")]
    assert "reconcile_all(normalize_first=not dry)" in body
    assert "return report" in body
