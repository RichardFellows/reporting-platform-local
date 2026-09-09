"""A control file the platform did not choose the shape of.

`control.format` is HOW the file is read, separately from WHAT is read out of
it. The original reading -- a regex per field over the file's whole text --
stays the default and is covered by tests/test_control.py and
tests/test_conform.py. This module covers the second one: a small delimited
table, a header row and one row of values, where each field names a COLUMN.

The feed here has BOTH control blocks, on purpose, because they read THE SAME
BYTES: the gate takes the identity pair at the door, the normalizer takes the
integrity pair from the promoted copy in landing/. One reader, two callers, so
the interesting failures are the ones where those two could drift.

No stack: pure functions and tests/fakes3.py.
"""
from __future__ import annotations

import hashlib

from tests.fakes3 import FakeS3, install, uninstall
from tests.support import config_dir

FEED = """
defaults:
  landing_prefix: landing
  ready_prefix: ready
  delimiter: ","

feeds:
  - name: trs_position
    description: Positions from a sender whose control file is a pipe table.
    source_system: TRS
    filename_pattern: 'trs_position_(?P<cob_date>\\d{8})(?:_v(?P<version>\\d+))?\\.csv'
    business_key: [position_id]
    expected_min_rows: 1
    arrival:
      source_pattern: 'positions\\.csv'
      control:
        pattern: '{stem}\\.ctl'
        format:
          kind: delimited
          delimiter: '|'
        cob_date: BUSINESS_DATE
        version: FILE_VERSION
    delivery:
      kind: file
      control:
        pattern: '{stem}\\.ctl'
        format:
          kind: delimited
          delimiter: '|'
        row_count: RECORD_COUNT
        md5: CHECKSUM
    columns: [position_id, quantity]
"""

DATA = b"position_id,quantity\nP1,10\nP2,20\n"
MD5 = hashlib.md5(DATA).hexdigest()

HEADER = "FEED|BUSINESS_DATE|FILE_VERSION|RECORD_COUNT|CHECKSUM"


def _ctl(bd="20260801", version=1, rows=2, md5=None):
    return (f"{HEADER}\n"
            f"POSITIONS|{bd}|{version}|{rows}|{md5 or MD5}\n")


def _feed(feeds_yml=FEED, name="trs_position"):
    config_dir(feeds_yml)
    from reporting_platform.common.context import feeds
    return feeds()[name]


def _conform():
    from reporting_platform.ingest import conform
    return conform


def _bad(feeds_yml):
    try:
        _feed(feeds_yml)
    except ValueError as exc:
        return str(exc)
    raise AssertionError("expected a ValueError")


# ---------------------------------------------------------------- the config
def test_the_format_resolves_on_both_blocks():
    fd = _feed()
    fmt = {"kind": "delimited", "delimiter": "|", "quote_char": '"',
           "header": True}
    assert fd.arrival["control"] == {
        "pattern": "{stem}\\.ctl", "format": fmt,
        "cob_date": "BUSINESS_DATE", "version": "FILE_VERSION"}, fd.arrival
    assert fd.delivery["control"] == {
        "pattern": "{stem}\\.ctl", "format": fmt,
        "row_count": "RECORD_COUNT", "md5": "CHECKSUM"}, fd.delivery


def test_no_format_is_the_regex_reading_and_says_nothing():
    """The default is not written into the resolved block. A feed that
    declares no format keeps exactly the shape it had before formats existed,
    which is what stops this from being a migration."""
    fd = _feed(FEED
               .replace("        format:\n          kind: delimited\n"
                        "          delimiter: '|'\n", "")
               .replace("cob_date: BUSINESS_DATE",
                        "cob_date: 'Date\\|(?P<cob_date>\\d{8})'")
               .replace("version: FILE_VERSION",
                        "version: 'Ver\\|(?P<version>\\d+)'")
               .replace("row_count: RECORD_COUNT",
                        "row_count: 'ROWS=(?P<rows>\\d+)'")
               .replace("md5: CHECKSUM",
                        "md5: 'MD5=(?P<md5>[0-9a-f]{32})'"))
    assert "format" not in fd.arrival["control"], fd.arrival
    assert "format" not in fd.delivery["control"], fd.delivery


def test_an_unknown_format_key_is_rejected():
    msg = _bad(FEED.replace("          delimiter: '|'",
                            "          delimiter: '|'\n          seperator: '|'"))
    assert "unknown key" in msg and "seperator" in msg, msg


def test_an_unknown_kind_is_rejected():
    msg = _bad(FEED.replace("kind: delimited", "kind: fixed_width"))
    assert "not recognised" in msg and "fixed_width" in msg, msg


def test_a_delimiter_is_required_and_never_inherited():
    """The feed's own delimiter is a comma here and the control file is
    pipes. Defaulting one from the other would not fail -- it would read one
    column named by the whole header line."""
    msg = _bad(FEED.replace("          delimiter: '|'\n", ""))
    assert "delimiter" in msg and "not inherited" in msg, msg


def test_a_multi_character_delimiter_is_rejected():
    msg = _bad(FEED.replace("delimiter: '|'", "delimiter: '||'"))
    assert "not a single character" in msg, msg


def test_a_regex_format_may_not_carry_delimited_settings():
    msg = _bad(FEED.replace("kind: delimited", "kind: regex"))
    assert "delimiter" in msg and "never runs" in msg, msg


def test_columns_without_header_false_is_rejected():
    msg = _bad(FEED.replace("          delimiter: '|'",
                            "          delimiter: '|'\n          columns: [A, B]"))
    assert "One source for the names" in msg, msg


def test_header_false_without_columns_is_rejected():
    msg = _bad(FEED.replace("          delimiter: '|'",
                            "          delimiter: '|'\n          header: false"))
    assert "names nothing" in msg, msg


def test_a_field_naming_an_undeclared_column_is_rejected_at_load():
    """With no header row the declared list is the only thing that can say
    where a value is, so a typo is decidable at LOAD rather than at the next
    delivery."""
    msg = _bad(FEED.replace(
        "          delimiter: '|'",
        "          delimiter: '|'\n          header: false\n"
        "          columns: [FEED, BUSINESS_DATE, FILE_VERSION, RECORD_COUNT, CHECKSUM]")
        .replace("row_count: RECORD_COUNT", "row_count: RECORDCOUNT"))
    assert "RECORDCOUNT" in msg and "columns" in msg, msg


def test_a_column_name_is_not_read_as_a_regex():
    """Under `kind: regex` `BUSINESS_DATE` is a valid regex with no named
    group, and the load error would be about a missing (?P<cob_date>...) --
    true, unhelpful, and about the wrong line."""
    fd = _feed()
    assert fd.arrival["control"]["cob_date"] == "BUSINESS_DATE"


def test_the_two_blocks_may_not_read_one_file_two_ways():
    msg = _bad(FEED.replace("          delimiter: '|'\n"
                            "        cob_date: BUSINESS_DATE",
                            "          delimiter: ','\n"
                            "        cob_date: BUSINESS_DATE"))
    assert "reads one control file two ways" in msg, msg
    assert "same bytes" in msg, msg


# ------------------------------------------------------------------ the gate
def test_the_gate_reads_identity_out_of_the_columns():
    fd, conform = _feed(), _conform()
    declared = conform.read_control(fd, _ctl(), "positions.ctl")
    assert declared["cob_date"].isoformat() == "2026-08-01", declared
    assert declared["version"] == 1, declared
    # IDENTITY ONLY. The row count and checksum are in this very file and the
    # gate does not look at them -- they are checked once, at ingest, for
    # every delivery however it arrived.
    assert set(declared) == {"cob_date", "version"}, declared


def test_the_landing_name_comes_out_of_the_control_columns():
    fd, conform = _feed(), _conform()
    plan = conform.conform(fd, "positions.csv", DATA,
                           control_filename="positions.ctl",
                           control_text=_ctl())
    # `_v1` because the control file DECLARED a version, which wins outright
    # over the unversioned name -- the same rule a regex-read control file
    # has always followed. The column is read; nothing downstream of it
    # changes.
    assert plan["landing_filename"] == "trs_position_20260801_v1.csv", plan
    assert plan["control_landing_filename"] == "trs_position_20260801_v1.ctl", plan
    assert plan["metadata"]["declared"] == {"cob_date": "2026-08-01",
                                            "version": 1}, plan["metadata"]


def test_quoting_is_honoured_not_split_on():
    """csv, not str.split: a quoted field holding the delimiter is one value,
    the same reason count_rows parses rather than counting newlines."""
    fd, conform = _feed(), _conform()
    text = ('FEED|BUSINESS_DATE|FILE_VERSION|RECORD_COUNT|CHECKSUM\n'
            '"POSITIONS|EOD"|20260801|1|2|' + MD5 + '\n')
    declared = conform.read_control(fd, text, "positions.ctl")
    assert declared["cob_date"].isoformat() == "2026-08-01", declared


def test_a_missing_column_names_the_columns_that_are_there():
    fd, conform = _feed(), _conform()
    text = "FEED|COB_DATE|FILE_VERSION\nPOSITIONS|20260801|1\n"
    try:
        conform.read_control(fd, text, "positions.ctl")
    except conform.ConformanceError as exc:
        assert "no column 'BUSINESS_DATE'" in str(exc), exc
        assert "FEED, COB_DATE, FILE_VERSION" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


def test_the_wrong_delimiter_is_reported_as_the_one_column_it_produces():
    """A pipe file read as commas is not an error anywhere in csv -- it is one
    column whose name is the whole header line. The message says so because
    that is the actual mistake."""
    fd = _feed(FEED.replace("          delimiter: '|'", "          delimiter: ','"))
    conform = _conform()
    try:
        conform.read_control(fd, _ctl(), "positions.ctl")
    except conform.ConformanceError as exc:
        assert HEADER in str(exc), exc
        assert "read as commas" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


def test_two_rows_of_values_are_refused_rather_than_guessed():
    fd, conform = _feed(), _conform()
    text = _ctl() + f"POSITIONS|20260802|1|2|{MD5}\n"
    try:
        conform.read_control(fd, text, "positions.ctl")
    except conform.ConformanceError as exc:
        assert "holds 2 row(s)" in str(exc), exc
        assert "not built" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


def test_blank_lines_and_trailing_whitespace_do_not_count_as_rows():
    fd, conform = _feed(), _conform()
    declared = conform.read_control(fd, "\n" + _ctl() + "\n  \n", "positions.ctl")
    assert declared["cob_date"].isoformat() == "2026-08-01", declared


def test_an_empty_control_file_says_which_row_is_missing():
    fd, conform = _feed(), _conform()
    try:
        conform.read_control(fd, "", "positions.ctl")
    except conform.ConformanceError as exc:
        assert "is empty" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


def test_a_headerless_file_is_read_off_the_declared_columns():
    fd = _feed(FEED.replace(
        "          delimiter: '|'",
        "          delimiter: '|'\n          header: false\n"
        "          columns: [FEED, BUSINESS_DATE, FILE_VERSION, RECORD_COUNT, CHECKSUM]"))
    conform = _conform()
    declared = conform.read_control(
        fd, f"POSITIONS|20260801|3|2|{MD5}\n", "positions.ctl")
    assert declared["cob_date"].isoformat() == "2026-08-01", declared
    assert declared["version"] == 3, declared


def test_a_date_in_the_wrong_shape_is_still_an_identity_failure():
    """The column was found, so the remaining question is what is in it."""
    fd, conform = _feed(), _conform()
    try:
        conform.read_control(fd, _ctl(bd="01/08/2026"), "positions.ctl")
    except conform.ConformanceError as exc:
        assert "not yyyyMMdd" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


# ------------------------------------------------------------ the normalizer
DATA_KEY = "landing/trs_position/trs_position_20260801.csv"
CONTROL_KEY = "landing/trs_position/trs_position_20260801.ctl"


def _landed(feeds_yml=FEED, control_body=None):
    config_dir(feeds_yml)
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import normalize as norm

    fd = feeds()["trs_position"]
    s3.put(DATA_KEY, DATA.decode())
    s3.put(CONTROL_KEY, _ctl() if control_body is None else control_body)
    return s3, monkey, fd, norm


def test_the_normalizer_reads_integrity_out_of_the_same_columns():
    """The promoted control file, read on the landing side by the OTHER
    block -- same bytes, same reader, the two fields the gate ignored."""
    s3, monkey, fd, norm = _landed()
    try:
        m = norm.normalize(fd, DATA_KEY)
        assert m["control_object"] == CONTROL_KEY, m
        assert m["declared_row_count"] == 2, m
        assert m["declared_md5"] == MD5, m
    finally:
        uninstall(monkey)


def test_a_renamed_column_fails_at_normalize_not_silently():
    s3, monkey, fd, norm = _landed(
        control_body="FEED|BUSINESS_DATE|FILE_VERSION|ROW_COUNT|CHECKSUM\n"
                     f"POSITIONS|20260801|1|2|{MD5}\n")
    try:
        try:
            norm.normalize(fd, DATA_KEY)
        except norm.NotReady:
            raise AssertionError("a renamed column is not a timing problem")
        except ValueError as exc:
            assert "no column 'RECORD_COUNT'" in str(exc), exc
        else:
            raise AssertionError("expected a ValueError")
    finally:
        uninstall(monkey)


def test_an_empty_column_is_not_the_sender_saying_zero():
    fd, conform = _feed(), _conform()
    try:
        conform.read_control(fd, f"{HEADER}\nPOSITIONS||1|2|{MD5}\n",
                             "positions.ctl")
    except conform.ConformanceError as exc:
        assert "is empty" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


def test_a_version_that_is_not_a_number_names_itself():
    """Only reachable for a delimited control file: a `(?P<version>\\d+)`
    group either matches digits or does not match at all, but a COLUMN holds
    whatever the sender put in it."""
    fd, conform = _feed(), _conform()
    try:
        conform.read_control(fd, _ctl(version="FINAL"), "positions.ctl")
    except conform.ConformanceError as exc:
        assert "not a number" in str(exc), exc
    else:
        raise AssertionError("expected a ConformanceError")


def test_a_row_count_that_is_not_a_number_names_the_column():
    s3, monkey, fd, norm = _landed(control_body=_ctl(rows="N/A"))
    try:
        try:
            norm.normalize(fd, DATA_KEY)
        except ValueError as exc:
            assert "not a number" in str(exc), exc
            assert "RECORD_COUNT" in str(exc), exc
        else:
            raise AssertionError("expected a ValueError")
    finally:
        uninstall(monkey)
