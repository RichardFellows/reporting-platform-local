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
    filename_pattern: 'custodyPositions_(?P<cob_date>\\d{8})\\.zip'
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
        assert fd.delivery["cob_date_from"] == "container", fd.delivery
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
                            "      kind: archive\n      cob_date_from: member"))
    assert "NOT BUILT" in msg, msg


def test_member_pattern_on_a_file_feed_is_rejected():
    msg = _bad(FEED.replace("      kind: archive", "      kind: file"))
    assert "only read for archives" in msg, msg


# ------------------------------------------------------------ the normalizer
def test_members_become_parts_with_the_containers_date():
    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        m = norm.normalize(fd, ZIP_KEY)
        assert m["cob_date"] == "2026-09-03", m["cob_date"]
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


def test_ready_cache_can_be_deleted_and_rebuilt_from_the_archive():
    """The retained Landing container can recreate every archive artifact."""
    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        manifest = norm.normalize(fd, ZIP_KEY)
        ready_before = {
            key: value[0] for key, value in s3.objects.items()
            if key.startswith("ready/")
        }
        container = s3.objects[ZIP_KEY][0]

        for key in list(ready_before):
            s3.delete_object(Bucket="lakehouse", Key=key)
        assert not any(key.startswith("ready/") for key in s3.objects)
        assert s3.objects[ZIP_KEY][0] == container

        rebuilt = norm.reconcile(fd)
        assert rebuilt["created"] == [norm.manifest_key(fd, ZIP_KEY)], rebuilt
        assert {
            key: value[0] for key, value in s3.objects.items()
            if key.startswith("ready/")
        } == ready_before
        assert norm.read_manifest(rebuilt["created"][0]) == manifest
        assert s3.objects[ZIP_KEY][0] == container
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


def test_ready_sweep_removes_extracted_members_but_keeps_the_manifest():
    """The extracted members ARE the duplication `ready:` exists to reclaim.

    The manifest is not, and keeping it is what stops the sweep and
    `normalize.reconcile` undoing each other every night -- with the manifest
    gone, reconcile would re-extract the whole archive tomorrow. See
    reporting_platform/retention/ready.py.
    """
    from datetime import date, timedelta

    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        from reporting_platform.retention import ready
        m = norm.normalize(fd, ZIP_KEY)
        monkey.append((ready, "already_ingested", ready.already_ingested))
        ready.already_ingested = lambda feed: {p["object_key"] for p in m["parts"]}
        report = ready.sweep_feed(fd, date.today() + timedelta(days=1),
                                  dry_run=False)
        assert report["parts_deleted"] == len(m["parts"]), report
        assert report["manifests_deleted"] == 0, report
        members = [p["object_key"] for p in m["parts"]]
        assert not any(k in s3.objects for k in members), list(s3.objects)
        assert any(k.startswith("ready/") and k.endswith(".json")
                   for k in s3.objects), list(s3.objects)
        # ...and never the container.
        assert ZIP_KEY in s3.objects
    finally:
        uninstall(monkey)


def test_a_swept_archive_is_not_re_extracted():
    """The property, not the count: reconcile must find nothing to do."""
    from datetime import date, timedelta

    s3, monkey, fd, norm = _setup(TWO_PARTS)
    try:
        from reporting_platform.retention import ready
        m = norm.normalize(fd, ZIP_KEY)
        monkey.append((ready, "already_ingested", ready.already_ingested))
        ready.already_ingested = lambda feed: {p["object_key"] for p in m["parts"]}
        ready.sweep_feed(fd, date.today() + timedelta(days=1), dry_run=False)
        after = set(s3.objects)
        assert norm.reconcile(fd)["created"] == []
        assert set(s3.objects) == after
    finally:
        uninstall(monkey)


# ================================= a container gated on its own control file
# `delivery.control` used to be refused for `kind: archive` at load. A
# container is a landed delivery like any other, and what it declares is about
# the DELIVERY -- the row count across its members -- except the checksum,
# which is the container's own because that is the object the sender hashed.

def _gated(*fields: str) -> str:
    """FEED with a `delivery.control` block declaring exactly `fields`.

    Two fixtures rather than one because a field the block declares and the
    file omits is refused -- correctly -- so a test about the row count must
    not silently also require a checksum.
    """
    lines = "".join(f"        {f}\n" for f in fields)
    return FEED.replace(
        "      member_pattern: 'positions_.*\\.csv'",
        "      member_pattern: 'positions_.*\\.csv'\n"
        "      control:\n"
        "        pattern: '{stem}\\.ctl'\n" + lines)


GATED = _gated("row_count: 'ROWS=(?P<rows>\\d+)'")
GATED_MD5 = _gated("row_count: 'ROWS=(?P<rows>\\d+)'",
                   "md5: 'MD5=(?P<md5>[0-9a-fA-F]{32})'")

CONTROL_KEY = "landing/cus_position/custodyPositions_20260903.ctl"
TWO_MEMBERS = {
    "positions_1.csv": "position_id,counterparty_id,quantity\nP1,C1,10\n",
    "positions_2.csv": "position_id,counterparty_id,quantity\nP2,C2,20\n",
}


def test_an_archive_waits_for_its_control_file():
    s3, monkey, fd, norm = _setup(TWO_MEMBERS, feeds_yml=GATED)
    try:
        try:
            norm.normalize(fd, ZIP_KEY)
        except norm.NotReady as exc:
            assert "waiting on a control file" in str(exc), exc
        else:
            raise AssertionError("expected NotReady")
    finally:
        uninstall(monkey)


def test_nothing_is_extracted_while_the_container_waits():
    """The gate runs BEFORE the members are written. Extracting first would
    rewrite every member into `ready/` on each poll of a wait that has not
    finished -- and leave parts behind that no manifest yet points at."""
    s3, monkey, fd, norm = _setup(TWO_MEMBERS, feeds_yml=GATED)
    try:
        before = set(s3.objects)
        try:
            norm.normalize(fd, ZIP_KEY)
        except norm.NotReady:
            pass
        assert set(s3.objects) == before, set(s3.objects) - before
    finally:
        uninstall(monkey)


def test_the_declared_row_count_is_the_total_across_the_members():
    s3, monkey, fd, norm = _setup(TWO_MEMBERS, feeds_yml=GATED)
    try:
        s3.put(CONTROL_KEY, "ROWS=2\n")
        m = norm.normalize(fd, ZIP_KEY)
        assert m["control_object"] == CONTROL_KEY, m
        # Two members, one row each: what ingest counts once they are unioned.
        assert m["declared_row_count"] == 2, m
        assert len(m["parts"]) == 2, m
    finally:
        uninstall(monkey)


def test_the_declared_checksum_covers_the_container_not_the_parts():
    """The sender hashed the zip it sent. The members under `ready/` are this
    platform's own extraction, and no checksum the sender could write would
    describe them -- so `checksum_objects` names the container, and ingest
    hashes that without knowing what kind of delivery it is holding."""
    import hashlib

    s3, monkey, fd, norm = _setup(TWO_MEMBERS, feeds_yml=GATED_MD5)
    try:
        container_md5 = hashlib.md5(s3.objects[ZIP_KEY][0]).hexdigest()
        s3.put(CONTROL_KEY, f"ROWS=2\nMD5={container_md5}\n")
        m = norm.normalize(fd, ZIP_KEY)
        assert m["checksum_objects"] == [ZIP_KEY], m
        assert m["declared_md5"] == container_md5, m

        from reporting_platform.ingest import ingest_feed
        assert ingest_feed._checksum_objects(m) == [ZIP_KEY], m
        assert ingest_feed._delivery_md5(m) == container_md5
    finally:
        uninstall(monkey)


def test_a_manifest_written_before_checksum_objects_existed_still_verifies():
    """The key is new. Every delivery that could carry a declared md5 before
    it was single-part, and its one part IS its source object, so falling back
    to `parts` hashes the same bytes -- an identity, not a guess."""
    import hashlib

    s3, monkey, fd, norm = _setup(TWO_MEMBERS, feeds_yml=GATED_MD5)
    try:
        from reporting_platform.ingest import ingest_feed
        old = {"parts": [{"object_key": ZIP_KEY}]}
        assert ingest_feed._checksum_objects(old) == [ZIP_KEY], old
        assert ingest_feed._delivery_md5(old) == hashlib.md5(
            s3.objects[ZIP_KEY][0]).hexdigest()
    finally:
        uninstall(monkey)


def test_the_control_file_is_not_mistaken_for_a_delivery():
    """`matching()` routes landed objects by `filename_pattern`, which a
    control file never satisfies -- so the .ctl beside the zip is not itself
    normalized into a delivery."""
    s3, monkey, fd, norm = _setup(TWO_MEMBERS, feeds_yml=GATED)
    try:
        s3.put(CONTROL_KEY, "ROWS=2\n")
        report = norm.reconcile(fd)
        assert len(report["created"]) == 1, report
        assert report["created"][0].endswith(
            "custodyPositions_20260903.zip.json"), report
    finally:
        uninstall(monkey)
