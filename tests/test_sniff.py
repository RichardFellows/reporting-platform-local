"""The sniffer: step 5 of docs/DELIVERY-SHAPES.md, propose mode.

Against a REAL duckdb.connect() and REAL local temp files, not
tests/fakes3.py -- DuckDB is an embedded library, not a service, and this is
the one module here worth testing against the genuine engine. See
tests/README.md.

Every encoding-fallback case here was found by actually running duckdb, not
assumed: a first draft tried `utf-16` speculatively (it never raises, so it
"succeeded" with a garbled column before latin-1 ever got a turn), and
ordered `cp1252` before `latin-1` (which made latin-1 dead code, since
cp1252's accepted byte range turned out to be its strict superset in
duckdb's own implementation). Both are asserted against below so a
regression back to either is caught.
"""
from __future__ import annotations

import hashlib
import tempfile

import duckdb

from reporting_platform.ingest import sniff

CON = duckdb.connect()


def _path(data: bytes) -> str:
    f = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    f.write(data)
    f.close()
    return f.name


# ------------------------------------------------------------- the type map
def test_duckdb_types_map_onto_column_types_vocabulary():
    from reporting_platform.ui.scaffold import COLUMN_TYPES

    assert set(sniff.DUCKDB_TYPE_MAP.values()) <= set(COLUMN_TYPES)


def test_integer_and_decimal_families():
    for t in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT"):
        assert sniff.platform_type(t) == "integer", t
    for t in ("FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC"):
        assert sniff.platform_type(t) == "decimal", t


def test_parametrised_type_is_stripped_before_lookup():
    assert sniff.platform_type("DECIMAL(18,2)") == "decimal"
    assert sniff.platform_type("decimal(18,2)") == "decimal"


def test_unmapped_types_default_to_string_not_crash():
    """TIMESTAMP has no platform cast (parse_date only parses a date-shaped
    string) -- string is the safe fallback, not an error."""
    for t in ("TIMESTAMP", "TIME", "BLOB", "UUID", "INTERVAL", "SOMETHING_NEW"):
        assert sniff.platform_type(t) == "string", t


# ------------------------------------------------------------ the sniffer
def test_delimiter_quote_and_platform_names():
    p = _path(b'Trade Id|Notional (USD)|Ccy\n"T1"|1000.50|USD\nT2|2000|GBP\n')
    r = sniff.sniff_delivery(CON, p)
    assert r["delimiter"] == "|", r
    assert r["quote_char"] == '"', r
    assert r["header"] is True, r
    assert r["columns"] == ["trade_id", "notional_usd", "ccy"], r
    assert r["source_columns"] == {
        "trade_id": "Trade Id", "notional_usd": "Notional (USD)", "ccy": "Ccy"}, r


def test_types_come_from_values_not_names():
    """The whole reason this exists over infer_type: a column named 'flag'
    tells infer_type nothing, but real boolean-shaped values do."""
    p = _path(b"id,flag,amount,seen\n1,true,100.50,2026-09-01\n"
              b"2,false,200.25,2026-09-02\n")
    r = sniff.sniff_delivery(CON, p)
    assert r["column_types"] == {
        "id": "integer", "flag": "boolean", "amount": "decimal", "seen": "date"}, r


def test_business_key_candidates_are_columns_unique_across_the_file():
    p = _path(b"trade_id,ccy,notional\nT1,USD,100\nT2,GBP,200\nT1,EUR,300\n")
    r = sniff.sniff_delivery(CON, p)
    # trade_id repeats (T1 twice); ccy and notional happen to be unique here.
    assert "trade_id" not in r["business_key_candidates"], r
    assert set(r["business_key_candidates"]) == {"ccy", "notional"}, r


def test_header_only_file_proposes_no_candidates():
    """No rows means no evidence of uniqueness -- not a false positive."""
    p = _path(b"a,b,c\n")
    r = sniff.sniff_delivery(CON, p)
    assert r["business_key_candidates"] == [], r


# --------------------------------------------------------------- encoding
def test_plain_utf8_is_high_confidence():
    r = sniff.sniff_delivery(CON, _path(b"a,b\n1,2\n"))
    assert r["file_encoding"] == "utf-8", r
    assert r["encoding_confidence"] == "high", r


def test_utf8_bom_is_stripped_and_stays_utf8():
    p = _path("﻿a,b\n1,2\n".encode("utf-8"))
    r = sniff.sniff_delivery(CON, p)
    assert r["file_encoding"] == "utf-8", r
    assert r["columns"] == ["a", "b"], r  # not "﻿a"


def test_utf16_is_only_ever_tried_via_its_bom():
    p = _path("name,city\nJose,SP\n".encode("utf-16"))
    r = sniff.sniff_delivery(CON, p)
    assert r["file_encoding"] == "utf-16", r
    assert r["columns"] == ["name", "city"], r


def test_utf16_is_not_guessed_from_plain_ascii_bytes():
    """The bug this guards: sniff_csv(..., encoding='utf-16') does not raise
    on ASCII/latin-1 bytes -- it reinterprets byte-pairs as UTF-16 code
    units and 'succeeds' with one garbled column. utf-16 must never be
    reachable except through a real BOM."""
    assert "utf-16" not in sniff.ENCODING_FALLBACKS


def test_plain_latin1_text_resolves_as_latin1_not_cp1252():
    """Ordering matters: cp1252's accepted byte range is latin-1's plus 27
    more (checked byte-by-byte), so with cp1252 tried first latin-1 could
    never win. latin-1 goes first so ordinary accented text gets the more
    accurate label."""
    p = _path("name,city\nJose,Sao Paulo\nAndre,Bras\xedlia\n".encode("latin-1"))
    r = sniff.sniff_delivery(CON, p)
    assert r["file_encoding"] == "latin-1", r


def test_cp1252_specific_bytes_fall_through_as_low_confidence():
    """0x93/0x94 are smart quotes in cp1252 and undefined in duckdb's
    latin-1, so this can only succeed on the cp1252 attempt."""
    p = _path(b"name,note\nX," + bytes([0x93]) + b"hi" + bytes([0x94]) + b"\n")
    r = sniff.sniff_delivery(CON, p)
    assert r["file_encoding"] == "cp1252", r
    assert r["encoding_confidence"] == "low", r


def test_bytes_invalid_in_every_fallback_raise_cleanly():
    """0x81 is a C1 control byte undefined in BOTH cp1252 and duckdb's
    latin-1 -- there is no encoding here it can succeed under, and that must
    surface as one clear error, not whichever fallback happened to run last
    and a raw duckdb traceback."""
    p = _path(b"a,b\n" + bytes([0x81]) + b",2\n")
    try:
        sniff.sniff_delivery(CON, p)
    except ValueError as exc:
        assert "utf-8" in str(exc) and "latin-1" in str(exc) and "cp1252" in str(exc), exc
    else:
        raise AssertionError("expected a ValueError")


# ------------------------------------------------------------- sniff_bytes
def test_sniff_bytes_matches_sniff_delivery():
    """The upload/S3-bytes entry point must agree with the path-based one --
    it is a thin wrapper, not a second implementation."""
    data = b"a,b\n1,2\n3,4\n"
    from_path = sniff.sniff_delivery(CON, _path(data))
    from_bytes = sniff.sniff_bytes(CON, data, "whatever.csv")
    assert from_path == from_bytes, (from_path, from_bytes)


def test_sniff_bytes_dispatches_zip_to_sniff_archive():
    r = sniff.sniff_bytes(CON, _zip({"a.csv": "x,y\n1,2\n"}), "delivery.zip")
    assert "archive_members" in r, r


# ------------------------------------------------------------ sniff_archive
def _zip(members: dict) -> bytes:
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


TWO_MEMBERS = {
    "positions_1.csv": "position_id,counterparty_id,quantity\nP1,CP1,10\nP2,CP1,20\n",
    "positions_2.csv": "position_id,counterparty_id,quantity\nP3,CP2,30\n",
}


def test_archive_sniffs_the_first_member_sorted_by_name():
    """parts: concat assumes every member shares a shape, so looking at one
    -- the first, matching ingest/normalize.py's own ordering -- is looking
    at all of them."""
    reversed_order = {"positions_2.csv": TWO_MEMBERS["positions_2.csv"],
                      "positions_1.csv": TWO_MEMBERS["positions_1.csv"]}
    r = sniff.sniff_archive(CON, _zip(reversed_order))
    assert r["sniffed_member"] == "positions_1.csv", r
    assert r["columns"] == ["position_id", "counterparty_id", "quantity"], r


def test_archive_lists_every_member_regardless_of_which_is_sniffed():
    r = sniff.sniff_archive(CON, _zip(TWO_MEMBERS))
    assert r["archive_members"] == ["positions_1.csv", "positions_2.csv"], r


def test_member_pattern_candidate_groups_by_extension():
    r = sniff.sniff_archive(
        CON, _zip({**TWO_MEMBERS, "MANIFEST.txt": "whatever"}))
    assert r["member_pattern_candidate"] == r".*\.csv", r


def test_member_pattern_narrows_the_candidates():
    """Passed an existing feed's member_pattern (the re-sniff case), only
    matching members are considered -- a checksum or manifest file
    alongside the data must not become the sniffed member."""
    r = sniff.sniff_archive(
        CON, _zip({"README.txt": "not data", **TWO_MEMBERS}),
        member_pattern=r"positions_.*\.csv")
    assert r["sniffed_member"] == "positions_1.csv", r


def test_empty_archive_raises_cleanly():
    try:
        sniff.sniff_archive(CON, _zip({}))
    except ValueError as exc:
        assert "empty" in str(exc), exc
    else:
        raise AssertionError("expected a ValueError")


def test_no_member_matches_pattern_raises_cleanly():
    try:
        sniff.sniff_archive(CON, _zip(TWO_MEMBERS), member_pattern=r"nope_.*")
    except ValueError as exc:
        assert "nope_" in str(exc), exc
    else:
        raise AssertionError("expected a ValueError")


# ------------------------------------------------------------- propose_feed
def test_propose_feed_adds_filename_pattern():
    r = sniff.propose_feed("MarginCall_20260904.csv",
                           b"margin_call_id,amount\nM1,100\n")
    assert r["filename_pattern"] == \
        r"MarginCall_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv", r


def test_propose_feed_flags_an_archive_with_no_date_on_the_container():
    """cob_date_from: member/path is real, described in
    DELIVERY-SHAPES.md, and NOT BUILT (context.NOT_BUILT) -- proposing it
    would suggest a value guaranteed to fail at load."""
    r = sniff.propose_feed("positions.zip", _zip(TWO_MEMBERS))
    assert r["container_has_date"] is False, r


def test_propose_feed_confirms_the_date_on_a_dated_container():
    r = sniff.propose_feed("custodyPositions_20260904.zip", _zip(TWO_MEMBERS))
    assert r["container_has_date"] is True, r


# ------------------------------------------- members with their own control files
# A container whose members each carry a control file is the `arrival.archive`
# shape. The sniffer used to get the member pattern right only ALPHABETICALLY:
# `POS_A.csv` sorts before `POS_A.ctl`, `POS_A.dat` does not, and then the
# control file was the member sniffed and `.*\.ctl` the pattern proposed.

DAT_A = "position_id,quantity\nP1,10\nP2,20\n"
DAT_B = "position_id,quantity\nP3,30\n"


def _md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


KEY_VALUE_ZIP = {
    "POS_A.dat": DAT_A, "POS_A.ctl": f"DATE=20260831\nROWS=2\nMD5={_md5(DAT_A)}\n",
    "POS_B.dat": DAT_B, "POS_B.ctl": f"DATE=20260901\nROWS=1\nMD5={_md5(DAT_B)}\n",
}
DELIMITED_ZIP = {
    "POS_A.csv": DAT_A,
    "POS_A.ctl.csv": "FEED|BUSINESS_DATE|RECORD_COUNT\nPOS|20260831|2\n",
    "POS_B.csv": DAT_B,
    "POS_B.ctl.csv": "FEED|BUSINESS_DATE|RECORD_COUNT\nPOS|20260901|1\n",
}


def test_a_control_member_that_sorts_first_is_not_the_member_sniffed():
    """The bug, reproduced: `.ctl` sorts before `.dat`."""
    r = sniff.sniff_archive(CON, _zip(KEY_VALUE_ZIP))
    assert r["sniffed_member"] == "POS_A.dat", r
    assert r["columns"] == ["position_id", "quantity"], r
    assert r["member_pattern_candidate"] == r".*\.dat", r
    assert r["member_control"]["pattern"] == r"{stem}\.ctl", r
    assert r["member_control"]["pairs"] == {
        "POS_A.ctl": "POS_A.dat", "POS_B.ctl": "POS_B.dat"}, r


def test_a_control_file_with_a_claimed_extension_is_still_a_control_member():
    """`POS_A.ctl.csv` shares the data's extension, so grouping by extension
    alone would count it as data. The name pairs it with `POS_A.csv`."""
    r = sniff.sniff_archive(CON, _zip(DELIMITED_ZIP))
    assert r["sniffed_member"] == "POS_A.csv", r
    assert r["member_control"]["pattern"] == r"{stem}\.ctl\.csv", r
    assert r["member_control"]["format"] == {"kind": "delimited", "delimiter": "|"}, r


def test_field_candidates_are_measured_not_guessed():
    """row_count and md5 are offered only where the value EQUALS what the
    paired member holds; cob_date wherever every file has a real date."""
    r = sniff.sniff_archive(CON, _zip(KEY_VALUE_ZIP))
    assert r["member_control"]["field_candidates"] == {
        "cob_date": [r"(?m)^\s*DATE\s*=\s*(?P<cob_date>\d{8})"],
        "row_count": [r"(?m)^\s*ROWS\s*=\s*(?P<rows>\d+)"],
        "md5": [r"(?m)^\s*MD5\s*=\s*(?P<md5>[0-9a-fA-F]{32})"],
    }, r
    wrong_count = {**KEY_VALUE_ZIP, "POS_B.ctl": "DATE=20260901\nROWS=7\n"}
    got = sniff.sniff_archive(CON, _zip(wrong_count))["member_control"]
    assert "row_count" not in got["field_candidates"], got
    delimited = sniff.sniff_archive(CON, _zip(DELIMITED_ZIP))["member_control"]
    assert delimited["field_candidates"] == {
        "cob_date": ["BUSINESS_DATE"], "row_count": ["RECORD_COUNT"]}, delimited


def test_two_dates_in_a_control_file_are_both_candidates_and_neither_chosen():
    """Which one is the COB date is a claim about meaning -- the business-key
    problem -- so both are offered and the console fills in neither."""
    two = {"POS_A.csv": DAT_A, "POS_A.ctl": "RUN=20260902\nCOB=20260901\n"}
    got = sniff.sniff_archive(CON, _zip(two))["member_control"]
    assert len(got["field_candidates"]["cob_date"]) == 2, got
    note = sniff.propose_feed("positions.zip", _zip(two))["member_control"]["note"]
    assert "Not filled in" in note, note


def test_a_key_pipe_value_file_is_not_mistaken_for_a_table():
    """`ReportingDate|20260901` over `Rows|2` splits into two rows of two
    cells, exactly like a header and a value row. The all-digit cell says it
    is not a header."""
    kv = {"POS_A.csv": DAT_A, "POS_A.ctl": "ReportingDate|20260901\nRows|2\n"}
    got = sniff.sniff_archive(CON, _zip(kv))["member_control"]
    assert got["format"] is None, got
    assert got["field_candidates"]["cob_date"] == [
        r"(?m)^\s*ReportingDate\s*\|\s*(?P<cob_date>\d{8})"], got


def test_an_empty_done_file_proposes_the_pattern_and_no_fields():
    empty = {"POS_A.txt": DAT_A, "POS_A.done": "", "MANIFEST.txt": "x"}
    got = sniff.sniff_archive(CON, _zip(empty))["member_control"]
    assert got["pattern"] == r"{stem}\.done", got
    assert got["field_candidates"] == {}, got
    assert got["members_without_control"] == ["MANIFEST.txt"], got


def test_pairs_that_disagree_propose_no_pattern():
    """`.ctl` for one member and `.CTL` for another is not one pattern, and
    picking the commoner would leave the rest refused at the gate."""
    mixed = {"A.csv": DAT_A, "A.ctl": "DATE=20260901", "B.csv": DAT_B, "B.CTL": "DATE=20260902"}
    got = sniff.sniff_archive(CON, _zip(mixed))["member_control"]
    assert got["pattern"] is None, got
    assert "none is proposed" in sniff.propose_feed(
        "c.zip", _zip(mixed))["member_control"]["note"]


def test_an_existing_member_pattern_never_selects_a_control_member():
    r = sniff.sniff_archive(CON, _zip(DELIMITED_ZIP), member_pattern=r".*\.csv")
    assert r["sniffed_member"] == "POS_A.csv", r
    try:
        sniff.sniff_archive(CON, _zip(DELIMITED_ZIP), member_pattern=r".*\.ctl\.csv")
    except ValueError:
        pass
    else:
        raise AssertionError("a pattern claiming only control members sniffs nothing")


def test_extensionless_data_members_pair_by_the_gates_own_stem():
    """Review finding: `POSA` beside `POSA.ctl` -- the mainframe shape -- was
    skipped for having no extension, and the result was `.*\\.ctl`, the very
    misproposal pairing exists to prevent. `conform._stem` treats a name with
    no dot as its own stem, and so does the sniffer now."""
    from reporting_platform.ingest import conform

    mainframe = {"POSA": DAT_A, "POSA.ctl": "DATE=20260831\nROWS=2\n",
                 "POSB": DAT_B, "POSB.ctl": "DATE=20260901\nROWS=1\n"}
    r = sniff.sniff_archive(CON, _zip(mainframe))
    assert r["sniffed_member"] == "POSA", r
    assert r["member_pattern_candidate"] == r"[^.]+", r
    mc = r["member_control"]
    assert mc["pairs"] == {"POSA.ctl": "POSA", "POSB.ctl": "POSB"}, mc
    assert mc["pattern"] == r"{stem}\.ctl", mc
    assert "row_count" in mc["field_candidates"], mc
    assert sniff._stem("POSA") == conform._stem("POSA") == "POSA"


def test_extensionless_and_extensioned_data_in_equal_number_propose_no_member_pattern():
    """No most-common shape to name, so None -- and the note says so rather
    than leaving an empty box unexplained."""
    mixed = {"POSA": DAT_A, "POSA.ctl": "DATE=20260831\n",
             "POSB.csv": DAT_B, "POSB.ctl": "DATE=20260901\n"}
    r = sniff.propose_feed("w.zip", _zip(mixed))
    assert r["member_pattern_candidate"] is None, r
    assert "No member pattern is proposed" in r["member_control"]["note"], r


def test_a_key_pipe_value_file_that_also_reads_as_a_table_proposes_no_format():
    """Review finding: `FEED|POSITIONS` over `ROWS|2` is two KEY|VALUE lines
    AND a table with columns FEED and POSITIONS -- and read as the table,
    POSITIONS 'is' the row count. Nothing in the bytes says which the sender
    means, so neither is proposed; the candidates under each are evidence."""
    kv = {"A.csv": DAT_A, "A.ctl": "FEED|POSITIONS\nROWS|2\n",
          "B.csv": DAT_A, "B.ctl": "FEED|POSITIONS\nROWS|2\n"}
    p = sniff.propose_feed("w.zip", _zip(kv))
    mc = p["member_control"]
    assert mc["format"] is None and mc["format_ambiguous"] is True, mc
    assert mc["field_candidates"] == {}, mc
    by = mc["field_candidates_by_reading"]
    assert by["text"]["field_candidates"]["row_count"] == [
        r"(?m)^\s*ROWS\s*\|\s*(?P<rows>\d+)"], by
    assert by["delimited"]["format"] == {"kind": "delimited", "delimiter": "|"}, by
    assert "AMBIGUOUS FORMAT" in mc["note"], mc["note"]


def test_a_real_one_row_table_is_still_proposed_as_delimited():
    """`20260901` cannot be a key, so the text reading fails and the table is
    the only reading -- the discriminator does not refuse the ordinary case."""
    table = {"A.csv": DAT_A, "A.ctl": "cob_date|row_count\n20260831|2\n",
             "B.csv": DAT_B, "B.ctl": "cob_date|row_count\n20260901|1\n"}
    mc = sniff.sniff_archive(CON, _zip(table))["member_control"]
    assert mc["format"] == {"kind": "delimited", "delimiter": "|"}, mc
    assert mc["format_ambiguous"] is False, mc
    assert mc["field_candidates"] == {"cob_date": ["cob_date"],
                                      "row_count": ["row_count"]}, mc


def test_indented_key_value_lines_still_yield_candidates():
    """Review finding: `_KEY_VALUE` tolerated leading whitespace and the
    expression anchored `^KEY`, so an indented file was recognised and then
    every candidate failed its read-back."""
    indented = {"A.csv": DAT_A, "A.ctl": "  DATE=20260831\n\tROWS=2\n"}
    mc = sniff.sniff_archive(CON, _zip(indented))["member_control"]
    assert mc["field_candidates"] == {
        "cob_date": [r"(?m)^\s*DATE\s*=\s*(?P<cob_date>\d{8})"],
        "row_count": [r"(?m)^\s*ROWS\s*=\s*(?P<rows>\d+)"]}, mc


def test_control_files_are_decoded_with_the_proposed_encoding():
    """Review finding: the gate decodes a member's control file with the
    feed's `file_encoding`, the sniffer decoded UTF-8. A latin-1 key read as
    UTF-8 is a replacement character, not a word, and the file stopped
    reading as KEY=VALUE at all."""
    data = "name,city\nAndre,Bras\xedlia\nJose,Sao Paulo\n".encode("latin-1")
    ctl = "Soci\xe9t\xe9=ACME\nDATE=20260831\nROWS=2\n".encode("latin-1")
    r = sniff.sniff_archive(CON, _zip({"A.csv": data, "A.ctl": ctl}))
    assert r["file_encoding"] == "latin-1", r
    assert "row_count" in r["member_control"]["field_candidates"], r


def test_each_data_member_is_measured_once():
    """Review finding: the md5 and row count were recomputed per candidate
    key, with every member's bytes held at once."""
    calls = []
    real = sniff._member_facts
    sniff._member_facts = lambda data, proposal: calls.append(1) or real(data, proposal)
    try:
        sniff.sniff_archive(CON, _zip(KEY_VALUE_ZIP))
    finally:
        sniff._member_facts = real
    assert len(calls) == 2, calls


def test_paired_member_pattern_ties_do_not_depend_on_name_order():
    """Second review: only the first two ranked shapes were compared, so
    `A.csv,B.dat,C` proposed `.*\\.csv` and `A,B.dat,C.csv` proposed None.
    Extensionless tied with anything is None, in every order."""
    for names in (["A.csv", "B.dat", "C"], ["A", "B.dat", "C.csv"],
                  ["C", "A.csv", "B.dat"], ["POSA", "POSB.csv"]):
        assert sniff._paired_member_pattern_candidate(names) is None, names
    assert sniff._paired_member_pattern_candidate(["POSA", "POSB", "POSC.csv"]) == r"[^.]+"
    assert sniff._paired_member_pattern_candidate(["A.csv", "B.csv", "C"]) == r".*\.csv"


def test_a_tie_between_extensions_follows_the_unpaired_rule():
    """Two extensions tied get what `_member_pattern_candidate` -- unchanged
    from main -- gives the same members, whatever order they arrive in."""
    for names in (["A.csv", "B.dat"], ["B.dat", "A.csv"]):
        assert sniff._paired_member_pattern_candidate(names) == \
            sniff._member_pattern_candidate(sorted(names)) == r".*\.csv", names


def test_a_separator_that_is_not_the_delimiter_is_not_an_ambiguity():
    """Second review: `A=1,B=2` over `C=3,D=4` is a comma table and `=` lines,
    and the note called them "KEY,VALUE lines". Not the same cells read two
    ways; the table's header would be `A=1`, under which nothing is readable,
    so the text reading is the one that survives."""
    eq = {"A.csv": DAT_A, "A.ctl": "A=1,B=2\nC=3,D=4\n",
          "B.csv": DAT_A, "B.ctl": "A=1,B=2\nC=3,D=4\n"}
    mc = sniff.propose_feed("w.zip", _zip(eq))["member_control"]
    assert mc["format_ambiguous"] is False, mc
    assert mc["format"] is None, mc
    assert "AMBIGUOUS" not in mc["note"] and "KEY," not in mc["note"], mc["note"]
    # ...while the genuine case records the separator it was worded from
    kv = {"A.csv": DAT_A, "A.ctl": "FEED|POSITIONS\nROWS|2\n"}
    genuine = sniff.propose_feed("w.zip", _zip(kv))["member_control"]
    assert genuine["key_value_separators"] == ["|"], genuine
    assert "KEY|VALUE lines" in genuine["note"], genuine["note"]


def test_a_different_separator_stays_ambiguous_when_the_table_reads_a_field():
    """Third review: the rule that let the text reading win for `A=1,B=2`
    must MEASURE that nothing is readable under the table, not assume it.
    `Time:UTC|Rows` over `T08:00|3` is a `:` line and a `|` table whose
    `Rows` is the member's row count -- a real reading, silently discarded
    by a shape-only rule."""
    members = {"A.csv": DAT_A, "A.ctl": "Time:UTC|Rows\nT08:00|2\n"}
    mc = sniff.propose_feed("w.zip", _zip(members))["member_control"]
    assert mc["format_ambiguous"] is True, mc
    assert mc["field_candidates_by_reading"]["delimited"]["field_candidates"] == {
        "row_count": ["Rows"]}, mc
    assert mc["key_value_separators"] == [":"], mc
    assert "KEY:VALUE lines" in mc["note"], mc["note"]


def test_mixed_separators_are_named_one_by_one_in_the_note():
    """Fourth review: separators joined into one string named `KEY:=VALUE`,
    which no line in the file uses."""
    members = {"A.csv": DAT_A, "A.ctl": "A=x|B\nC:1|2\n"}
    mc = sniff.propose_feed("w.zip", _zip(members))["member_control"]
    assert mc["format_ambiguous"] is True, mc
    assert mc["key_value_separators"] == [":", "="], mc
    assert "KEY:VALUE / KEY=VALUE lines" in mc["note"], mc["note"]
    assert ":=" not in mc["note"], mc["note"]


# ...and with NO pairs, nothing changes.
def test_no_pairs_means_no_member_control_and_the_old_proposal():
    """Checked against `main`'s sniffer when this was written, output for
    output; pinned here as the shape. An unpaired control-suffixed name
    (a container-level `BATCH.done`) is not a member's control file."""
    for members in (TWO_MEMBERS, {**TWO_MEMBERS, "BATCH.done": ""},
                    {"a.csv": "x,y\n1,2\n", "b.ctl": "DATE=20260901"}):
        r = sniff.propose_feed("custodyPositions_20260904.zip", _zip(members))
        assert "member_control" not in r, r
        assert "arrival_source_pattern" not in r, r
        assert r["filename_pattern"] == (
            r"custodyPositions_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.zip"), r
        assert set(r) == {
            "delimiter", "quote_char", "header", "file_encoding",
            "encoding_confidence", "columns", "source_columns", "column_types",
            "business_key_candidates", "archive_members", "sniffed_member",
            "member_pattern_candidate", "filename_pattern", "filename_has_date",
            "container_has_date"}, sorted(r)


def test_propose_feed_proposes_the_arrival_shape_for_control_gated_members():
    """The container becomes the SOURCE pattern and the derived landing
    pattern is withdrawn: under `arrival.archive` it names each member after
    renaming, and a `\\.zip` there matches nothing for ever."""
    dated = sniff.propose_feed("weekly_20260901.zip", _zip(KEY_VALUE_ZIP))
    assert dated["arrival_source_pattern"] == r"weekly_\d{8}(?:_v\d+)?\.zip", dated
    assert dated["filename_pattern"] is None, dated
    undated = sniff.propose_feed("weekly.zip", _zip(KEY_VALUE_ZIP))
    assert undated["arrival_source_pattern"] == r"weekly\.zip", undated
    note = undated["member_control"]["note"]
    assert "`delivery.control` is REQUIRED" in note, note
    assert "arrival.archive" in note, note


# --------------------------------------------- ...and what it proposes LOADS
def _feed_from_proposal(p: dict, name: str) -> dict:
    """The feed a human would save from this proposal: every proposed value
    taken, the first candidate of each field typed in, the landing name
    chosen. Deliberately built from the proposal's own keys, not restated."""
    mc = p["member_control"]
    fmt = {"format": mc["format"]} if mc["format"] else {}
    cands = mc["field_candidates"]
    delivery_fields = {k: cands[k][0] for k in ("row_count", "md5") if k in cands}
    return {
        "name": name, "description": "d", "source_system": "CUS",
        "filename_pattern": name + r"_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv",
        "business_key": [p["columns"][0]], "columns": p["columns"],
        "arrival": {"source_pattern": p["arrival_source_pattern"],
                    "control": {"pattern": mc["pattern"], **fmt,
                                "cob_date": cands["cob_date"][0]},
                    "archive": {"member_pattern": p["member_pattern_candidate"]}},
        "delivery": {"control": {"pattern": mc["pattern"], **fmt,
                                 **delivery_fields}},
    }


def _load_and_plan(members: dict, container: str):
    import yaml

    from tests.support import config_dir

    p = sniff.propose_feed(container, _zip(members))
    feed = _feed_from_proposal(p, "cus_pos")
    config_dir(yaml.safe_dump({"defaults": {"landing_prefix": "landing",
                                            "delimiter": ","},
                               "feeds": [feed]}))
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import conform

    fd = feeds()["cus_pos"]                   # the REAL loader, all checks
    return fd, conform.plan_arrival(fd, container, _zip(members))


def test_a_key_value_proposal_loads_and_the_gate_unpacks_with_it():
    fd, outcomes = _load_and_plan(KEY_VALUE_ZIP, "weekly_20260901.zip")
    from reporting_platform.ingest import conform
    assert all(isinstance(o, conform.Planned) for o in outcomes), outcomes
    assert [o.landing_filename for o in outcomes] == [
        "cus_pos_20260831.csv", "cus_pos_20260901.csv"], outcomes
    assert [o.control_landing_filename for o in outcomes] == [
        "cus_pos_20260831.ctl", "cus_pos_20260901.ctl"], outcomes


def test_a_delimited_proposal_loads_and_never_lands_its_control_members():
    """`.*\\.csv` claims `POS_A.ctl.csv` too; the proposed control pattern is
    what keeps it from landing as rows."""
    fd, outcomes = _load_and_plan(DELIMITED_ZIP, "weekly.zip")
    from reporting_platform.ingest import conform
    assert all(isinstance(o, conform.Planned) for o in outcomes), outcomes
    assert [o.source_name for o in outcomes] == [
        "weekly.zip!POS_A.csv", "weekly.zip!POS_B.csv"], outcomes
    assert [o.control_landing_filename for o in outcomes] == [
        "cus_pos_20260831.ctl.csv", "cus_pos_20260901.ctl.csv"], outcomes


def test_an_extensionless_proposal_loads_and_the_gate_unpacks_with_it():
    fd, outcomes = _load_and_plan(
        {"POSA": DAT_A, "POSA.ctl": "DATE=20260831\nROWS=2\n",
         "POSB": DAT_B, "POSB.ctl": "DATE=20260901\nROWS=1\n"}, "weekly.zip")
    from reporting_platform.ingest import conform
    assert all(isinstance(o, conform.Planned) for o in outcomes), outcomes
    assert [o.source_name for o in outcomes] == [
        "weekly.zip!POSA", "weekly.zip!POSB"], outcomes
    assert [o.control_landing_filename for o in outcomes] == [
        "cus_pos_20260831.ctl", "cus_pos_20260901.ctl"], outcomes
