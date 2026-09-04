"""The control-file gate: step 4 of docs/DELIVERY-SHAPES.md.

A control file is a second object, alongside the data file, that says
something ABOUT the delivery -- that the sender considers it complete, how
many rows it should contain. Until it lands, the delivery is not normalized
at all: not an error, a wait.

Runs against tests/fakes3.py, not MinIO. What that cannot tell you is whether
a real inbox-triggered Airflow run actually skips cleanly on NotReady rather
than retrying into a failure -- that is verified by running the stack.
"""
from __future__ import annotations

from tests.fakes3 import FakeS3, install, uninstall
from tests.support import config_dir

FEED = """
defaults:
  landing_prefix: landing
  ready_prefix: ready
  delimiter: ","

feeds:
  - name: trs_margin
    description: Treasury margin calls, gated on a control file.
    source_system: TRS
    filename_pattern: 'MarginCall_(?P<business_date>\\d{8})\\.csv'
    business_key: [margin_call_id]
    expected_min_rows: 1
    delivery:
      kind: file
      control:
        pattern: '{stem}\\.ctl'
        row_count: 'ROWS=(?P<rows>\\d+)'
    columns: [margin_call_id, amount]
"""

DATA_KEY = "landing/trs_margin/MarginCall_20260903.csv"
CONTROL_KEY = "landing/trs_margin/MarginCall_20260903.ctl"
DATA_BODY = "margin_call_id,amount\nM1,100\nM2,200\n"


def _setup(feeds_yml=FEED, *, land_control=False, control_body="ROWS=2\n"):
    config_dir(feeds_yml)
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import normalize as norm

    fd = feeds()["trs_margin"]
    s3.put(DATA_KEY, DATA_BODY)
    if land_control:
        s3.put(CONTROL_KEY, control_body)
    return s3, monkey, fd, norm


def _bad(feeds_yml):
    try:
        _setup(feeds_yml=feeds_yml)
    except ValueError as exc:
        return str(exc)
    raise AssertionError("expected a ValueError")


# ---------------------------------------------------------------- the config
def test_delivery_block_resolves():
    s3, monkey, fd, norm = _setup()
    try:
        assert fd.delivery["control"] == {
            "pattern": "{stem}\\.ctl", "row_count": "ROWS=(?P<rows>\\d+)"}, fd.delivery
    finally:
        uninstall(monkey)


def test_control_with_no_row_count_is_a_pure_gate():
    yml = FEED.replace("        row_count: 'ROWS=(?P<rows>\\d+)'\n", "")
    s3, monkey, fd, norm = _setup(yml)
    try:
        assert fd.delivery["control"] == {"pattern": "{stem}\\.ctl"}, fd.delivery
    finally:
        uninstall(monkey)


def test_control_needs_a_pattern():
    msg = _bad(FEED.replace("        pattern: '{stem}\\.ctl'\n", ""))
    assert "sets no `pattern`" in msg, msg


def test_pattern_without_stem_is_rejected():
    msg = _bad(FEED.replace("{stem}\\.ctl", "fixed.ctl"))
    assert "{stem}" in msg, msg


def test_unparseable_pattern_is_rejected():
    msg = _bad(FEED.replace("{stem}\\.ctl", "{stem}(unclosed"))
    assert "not a valid regex" in msg, msg


def test_row_count_needs_a_rows_group():
    msg = _bad(FEED.replace("ROWS=(?P<rows>\\d+)", "ROWS=\\d+"))
    assert "(?P<rows>...)" in msg, msg


def test_unparseable_row_count_is_rejected():
    msg = _bad(FEED.replace("ROWS=(?P<rows>\\d+)", "ROWS=(?P<rows>"))
    assert "not a valid regex" in msg, msg


def test_unknown_control_key_is_rejected():
    msg = _bad(FEED.replace("      control:", "      control:\n        rowz: 1"))
    assert "unknown key" in msg and "rowz" in msg, msg


def test_control_with_kind_archive_is_not_built():
    msg = _bad(FEED.replace("      kind: file", "      kind: archive")
               .replace("    columns:", "      member_pattern: '.*'\n    columns:"))
    assert "NOT BUILT" in msg, msg


# ---------------------------------------------------------- the normalizer
def test_missing_control_file_is_not_ready_not_failed():
    s3, monkey, fd, norm = _setup(land_control=False)
    try:
        try:
            norm.normalize(fd, DATA_KEY)
        except norm.NotReady as exc:
            assert "MarginCall_20260903" in str(exc), exc
            assert "not a failed one" in str(exc), exc
        else:
            raise AssertionError("expected NotReady")
    finally:
        uninstall(monkey)


def test_control_file_present_unblocks_normalize():
    s3, monkey, fd, norm = _setup(land_control=True)
    try:
        m = norm.normalize(fd, DATA_KEY)
        assert m["control_object"] == CONTROL_KEY, m
        assert m["declared_row_count"] == 2, m
        assert m["parts"] == [{"object_key": DATA_KEY, "bytes": len(DATA_BODY)}], m
    finally:
        uninstall(monkey)


def test_declared_row_count_is_none_without_a_row_count_pattern():
    yml = FEED.replace("        row_count: 'ROWS=(?P<rows>\\d+)'\n", "")
    s3, monkey, fd, norm = _setup(yml, land_control=True, control_body="anything\n")
    try:
        m = norm.normalize(fd, DATA_KEY)
        assert m["control_object"] == CONTROL_KEY, m
        assert m["declared_row_count"] is None, m
    finally:
        uninstall(monkey)


def test_control_file_not_matching_row_count_is_a_real_error():
    """The control file arrived but does not say what it was validated to
    say -- a format change upstream, not a timing problem, so this is a
    ValueError, not NotReady."""
    s3, monkey, fd, norm = _setup(land_control=True, control_body="nonsense\n")
    try:
        try:
            norm.normalize(fd, DATA_KEY)
        except norm.NotReady:
            raise AssertionError("a malformed control file is not NotReady")
        except ValueError as exc:
            assert "does not match" in str(exc), exc
        else:
            raise AssertionError("expected a ValueError")
    finally:
        uninstall(monkey)


def test_control_key_is_found_by_regex_not_literal_format():
    """'{stem}\\.ctl'.format(stem=X) is 'X\\.ctl' -- a literal backslash if
    treated as a plain filename template. The control file actually landed
    is 'MarginCall_20260903.ctl', with no backslash in it."""
    s3, monkey, fd, norm = _setup(land_control=True)
    try:
        assert CONTROL_KEY in s3.objects
        assert not any("\\" in k for k in s3.objects), list(s3.objects)
        m = norm.normalize(fd, DATA_KEY)
        assert m["control_object"] == CONTROL_KEY, m
    finally:
        uninstall(monkey)


def test_is_control_file_recognises_the_shape_not_a_specific_delivery():
    """Used by inbox routing, which sees one dropped file at a time and has
    no landed data file to match a stem against yet -- so this checks SHAPE
    (does it look like a control file for this feed?), not that a
    corresponding delivery exists. That correspondence is `_find_control_key`'s
    job, at normalize time, once both files have actually landed."""
    s3, monkey, fd, norm = _setup()
    try:
        assert norm.is_control_file(fd, "MarginCall_20260903.ctl")
        assert norm.is_control_file(fd, "anything.ctl")
        assert not norm.is_control_file(fd, "MarginCall_20260903.csv")
    finally:
        uninstall(monkey)


def test_reconcile_counts_awaiting_control_separately_from_failed():
    s3, monkey, fd, norm = _setup(land_control=False)
    try:
        result = norm.reconcile(fd)
        assert result["created"] == [], result
        assert result["failed"] == [], result
        assert [a["object"] for a in result["awaiting_control"]] == [DATA_KEY], result
    finally:
        uninstall(monkey)


def test_reconcile_creates_the_manifest_once_control_lands():
    s3, monkey, fd, norm = _setup(land_control=True)
    try:
        result = norm.reconcile(fd)
        assert result["awaiting_control"] == [], result
        assert len(result["created"]) == 1, result
        manifests = norm.manifests_for(fd)
        assert len(manifests) == 1, manifests
    finally:
        uninstall(monkey)


def test_pending_is_empty_while_awaiting_control():
    from reporting_platform.ingest import arrival
    from reporting_platform.ingest.arrival import find_pending

    s3, monkey, fd, norm = _setup(land_control=False)
    try:
        # already_ingested() reads Spark; stubbed here as test_find_pending.py
        # does, since this test is about the reconcile/NotReady interaction,
        # not about that lookup.
        monkey.append((arrival, "already_ingested", arrival.already_ingested))
        arrival.already_ingested = lambda feed: set()
        assert find_pending(fd) == []
    finally:
        uninstall(monkey)
