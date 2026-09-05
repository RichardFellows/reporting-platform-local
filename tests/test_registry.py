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
    "business_date": "2026-08-11",
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
    assert row["business_date"] == date(2026, 8, 11)
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
