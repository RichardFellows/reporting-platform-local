"""The archive normalizer: a dated container of undated members.

Step 3 of docs/DELIVERY-SHAPES.md, and the shape this whole design started
from -- `custodyPositions_20260903.zip` holding CSVs whose own names say
nothing about which day they are for.

Real zip bytes through tests/fakes3.py, so the zip handling is genuinely
exercised; only S3 is stubbed.
"""
from __future__ import annotations

import io
import zipfile

from tests.fakes3 import FakeS3, install, uninstall
from tests.support import config_dir, synthetic

FEED = """
defaults:
  landing_prefix: landing
  ready_prefix: ready
  delimiter: ","

feeds:
  - name: cus_position
    description: Custody positions, delivered zipped.
    source_system: CUS
    filename_pattern: 'custodyPositions_(?P<business_date>\\d{8})\\.zip'
    business_key: [position_id]
    expected_min_rows: 1
    delivery:
      kind: archive
      member_pattern: 'positions_.*\\.csv'
    columns: [position_id, counterparty_id, quantity]
"""

ZIP_KEY = "landing/cus_position/custodyPositions_20260903.zip"


def _zip(members: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


def _setup(members=None, feeds_yml=FEED):
    config_dir(feeds_yml)
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import normalize as norm

    fd = feeds()["cus_position"]
    if members is not None:
        s3.put(ZIP_KEY, _zip(members))
    return s3, monkey, fd, norm


TWO_PARTS = {
    "positions_1.csv": "position_id,counterparty_id,quantity\nP1,CP1,10\n",
    "positions_2.csv": "position_id,counterparty_id,quantity\nP2,CP2,20\n",
}


# ---------------------------------------------------------------- the config
def test_delivery_block_resolves():
    s3, monkey, fd, norm = _setup()
    try:
        assert fd.delivery["kind"] == "archive", fd.delivery
        assert fd.delivery["business_date_from"] == "container", fd.delivery
        assert fd.delivery["parts"] == "concat", fd.delivery
    finally:
        uninstall(monkey)


def _bad(feeds_yml):
    try:
        _setup(feeds_yml=feeds_yml)
    except ValueError as exc:
        return str(exc)
    raise AssertionError("expected a ValueError")


def test_archive_without_member_pattern_is_rejected():
    msg = _bad(FEED.replace("      member_pattern: 'positions_.*\\.csv'\n", ""))
    assert "member_pattern" in msg and "not guessable" in msg, msg


def test_unknown_delivery_key_is_rejected():
    msg = _bad(FEED.replace("      kind: archive", "      knid: archive"))
    assert "unknown key" in msg and "knid" in msg, msg


def test_not_built_values_say_so_rather_than_unknown():
    """A missing feature and a typo are different problems with different
    fixes, so the message must not call one the other."""
    msg = _bad(FEED.replace("      kind: archive",
                            "      kind: archive\n      parts: separate"))
    assert "NOT BUILT" in msg, msg
    msg = _bad(FEED.replace("      kind: archive",
                            "      kind: archive\n      business_date_from: member"))
    assert "NOT BUILT" in msg, msg


def test_member_pattern_on_a_file_feed_is_rejected():
    msg = _bad(FEED.replace("      kind: archive", "      kind: file"))
    assert "only read for archives" in msg, msg


# ------------------------------------------------------------ the normalizer
def test_members_become_parts_with_the_containers_date():
    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        m = norm.normalize(fd, ZIP_KEY)
        assert m["business_date"] == "2026-09-03", m["business_date"]
        assert m["normalizer"] == "archive/v1"
        assert [p["member"] for p in m["parts"]] == ["positions_1.csv",
                                                     "positions_2.csv"]
        assert [p["object_key"] for p in m["parts"]] == [
            "ready/cus_position/custodyPositions_20260903/positions_1.csv",
            "ready/cus_position/custodyPositions_20260903/positions_2.csv"], m
    finally:
        uninstall(monkey)


def test_members_are_extracted_under_ready_not_landing():
    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        norm.normalize(fd, ZIP_KEY)
        extracted = [k for k in s3.objects if k.endswith(".csv")]
        assert len(extracted) == 2, extracted
        assert all(k.startswith("ready/") for k in extracted), extracted
        # The container is the evidence copy and stays exactly as delivered.
        assert ZIP_KEY in s3.objects
        assert s3.objects[
            "ready/cus_position/custodyPositions_20260903/positions_1.csv"
        ][0] == TWO_PARTS["positions_1.csv"].encode()
    finally:
        uninstall(monkey)


def test_non_matching_members_are_left_out():
    """A zip routinely carries a checksum or another feed's file."""
    s3, monkey, fd, norm = _setup(
        {**TWO_PARTS, "MANIFEST.txt": "whatever", "other_feed.csv": "a,b\n"})
    try:
        m = norm.normalize(fd, ZIP_KEY)
        assert [p["member"] for p in m["parts"]] == ["positions_1.csv",
                                                     "positions_2.csv"], m
        assert "ready/cus_position/custodyPositions_20260903/MANIFEST.txt" \
            not in s3.objects
    finally:
        uninstall(monkey)


def test_part_order_does_not_depend_on_archive_order():
    """`parts` order is the union order at ingest, so it must not depend on
    how the sender happened to build the zip."""
    reversed_zip = {"positions_2.csv": TWO_PARTS["positions_2.csv"],
                    "positions_1.csv": TWO_PARTS["positions_1.csv"]}
    s3, monkey, fd, norm = _setup(reversed_zip)
    try:
        m = norm.normalize(fd, ZIP_KEY)
        assert [p["member"] for p in m["parts"]] == ["positions_1.csv",
                                                     "positions_2.csv"], m
    finally:
        uninstall(monkey)


def test_an_archive_with_no_matching_member_is_an_error():
    """Zero parts would land zero rows, which passes expected_min_rows only
    by accident and reads as an empty day rather than a broken delivery."""
    s3, monkey, fd, norm = _setup({"README.txt": "nothing here"})
    try:
        try:
            norm.normalize(fd, ZIP_KEY)
        except ValueError as exc:
            assert "no member matching" in str(exc), exc
        else:
            raise AssertionError("expected a ValueError")
    finally:
        uninstall(monkey)


def test_a_member_naming_a_path_is_refused():
    """Archive traversal: joining a member name onto a prefix could write
    outside this feed's ready/ prefix, over another feed's manifest."""
    s3, monkey, fd, norm = _setup({"positions_../../evil.csv": "a\n"})
    try:
        try:
            norm.normalize(fd, ZIP_KEY)
        except ValueError as exc:
            assert "contains a path" in str(exc), exc
        else:
            raise AssertionError("expected a ValueError")
        assert not any("evil" in k for k in s3.objects), list(s3.objects)
    finally:
        uninstall(monkey)


def test_renormalizing_is_byte_identical():
    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        first = norm.normalize(fd, ZIP_KEY)
        second = norm.normalize(fd, ZIP_KEY)
        assert first == second, (first, second)
    finally:
        uninstall(monkey)


def test_member_keys_are_stable_so_reingest_does_not_happen():
    """`already_ingested` matches on `_source_file`, which holds a part's key.
    A timestamp or uuid in that key re-ingests every delivery forever."""
    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        before = [p["object_key"] for p in norm.normalize(fd, ZIP_KEY)["parts"]]
        after = [p["object_key"] for p in norm.normalize(fd, ZIP_KEY)["parts"]]
        assert before == after, (before, after)
    finally:
        uninstall(monkey)


def test_ready_sweep_removes_extracted_members_too():
    from datetime import date, timedelta

    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        from reporting_platform.retention import ready
        m = norm.normalize(fd, ZIP_KEY)
        monkey.append((ready, "already_ingested", ready.already_ingested))
        ready.already_ingested = lambda feed: {p["object_key"] for p in m["parts"]}
        ready.sweep_feed(fd, date.today() + timedelta(days=1), dry_run=False)
        assert not any(k.startswith("ready/") for k in s3.objects), list(s3.objects)
        # ...and never the container.
        assert ZIP_KEY in s3.objects
    finally:
        uninstall(monkey)
