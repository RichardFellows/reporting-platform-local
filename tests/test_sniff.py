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
        r"MarginCall_(?P<business_date>\d{8})(?:_v(?P<version>\d+))?\.csv", r


def test_propose_feed_flags_an_archive_with_no_date_on_the_container():
    """business_date_from: member/path is real, described in
    DELIVERY-SHAPES.md, and NOT BUILT (context.NOT_BUILT) -- proposing it
    would suggest a value guaranteed to fail at load."""
    r = sniff.propose_feed("positions.zip", _zip(TWO_MEMBERS))
    assert r["container_has_date"] is False, r


def test_propose_feed_confirms_the_date_on_a_dated_container():
    r = sniff.propose_feed("custodyPositions_20260904.zip", _zip(TWO_MEMBERS))
    assert r["container_has_date"] is True, r
