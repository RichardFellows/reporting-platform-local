"""Sanitized byte fixtures for the inspection/production parsing boundary."""
from types import SimpleNamespace


def feed(**overrides):
    values = dict(file_encoding="utf-8", control_encoding=None, delimiter=",",
                  quote_char='"', header=True, csv_options={}, name="test_sample")
    return SimpleNamespace(**(values | overrides))


def test_preview_strips_utf8_bom():
    from reporting_platform.ui.feeddata import header_of
    assert header_of(b"\xef\xbb\xbfid,value\r\n1,ok\r\n", feed()) == ["id", "value"]


def test_count_refuses_invalid_utf8_instead_of_replacing_it():
    from reporting_platform.ingest.conform import count_rows
    try:
        count_rows(feed(), b"id,value\n1,\xff\n")
    except ValueError:
        return
    raise AssertionError("invalid source bytes were silently accepted")


def test_count_refuses_unclosed_quotes():
    from reporting_platform.ingest.conform import count_rows
    try:
        count_rows(feed(), b'id,value\n1,"unfinished\n')
    except ValueError:
        return
    raise AssertionError("malformed CSV was silently counted")


def test_counts_quoted_newlines_and_headerless_records():
    from reporting_platform.ingest.conform import count_rows
    data = b'id,value\r\n1,"hello,\r\nworld"\r\n2,"say ""yes"""\r\n'
    assert count_rows(feed(), data) == 2
    assert count_rows(feed(header=False), data.split(b"\r\n", 1)[1]) == 2


def test_supported_byte_fixtures_share_the_python_contract():
    from tests.parsing_fixtures import supported_cases
    from reporting_platform.common.parsing import csv_rows, feed_format
    from reporting_platform.ingest.conform import count_rows
    for label, data, overrides in supported_cases():
        fd = feed(**overrides)
        rows = list(csv_rows(data, feed_format(fd)))
        assert count_rows(fd, data) == len(rows) - int(fd.header), label


def test_bom_mismatch_and_corrupt_unicode_do_not_fall_back():
    from reporting_platform.common.parsing import decode_bytes, detect_text
    for action in (lambda: decode_bytes(b"\xfe\xff\x00a", "utf-16-le"),
                   lambda: detect_text(b"\xef\xbb\xbf\xff"),
                   lambda: detect_text(b"\xff\xfea")):
        try:
            action()
        except ValueError:
            pass
        else:
            raise AssertionError("corrupt or conflicting BOM accepted")


def diagnostic(data=b"ROWS=0\n", **kwargs):
    from reporting_platform.ingest.control import diagnose
    defaults = dict(encoding="utf-8", fields=("row_count",), feed_name="test",
                    filename="sample.ctl", block="delivery.control", observed={"row_count": 0})
    cfg = kwargs.pop("config", {"row_count": r"(?m)^ROWS=(?P<rows>\d+)$"})
    return diagnose(cfg, data, **(defaults | kwargs))


def test_control_diagnostics_distinguish_zero_missing_decode_extract_and_mismatch():
    assert diagnostic()["fields"]["row_count"]["status"] == "match"
    assert diagnostic(None)["stage"] == "missing_control"
    assert diagnostic(b"\xff")["stage"] == "decoding"
    assert diagnostic(b"different\n")["fields"]["row_count"]["status"] == "extraction"
    assert diagnostic(b"ROWS=2\n")["fields"]["row_count"]["status"] == "mismatch"
    assert diagnostic(b"ROWS=0\nROWS=1\n")["fields"]["row_count"]["status"] == "ambiguous"


def test_multiline_regex_flags_and_captures_are_visible():
    text = b"DATE=20260917\nROWS=0\n"
    report = diagnostic(text)
    assert report["ok"] and report["fields"]["row_count"]["captures"] == [{"rows": "0"}]
    report = diagnostic(text, config={"row_count": r"^ROWS=(?P<rows>\d+)$"})
    assert not report["ok"] and report["fields"]["row_count"]["match_count"] == 0


def test_delimited_control_bom_and_invalid_value():
    cfg = {"format": {"kind": "delimited", "delimiter": "|", "quote_char": '"', "header": True},
           "row_count": "ROWS"}
    assert diagnostic(b"\xef\xbb\xbfROWS|NOTE\r\n0|ok\r\n", config=cfg)["ok"]
    assert diagnostic(b"ROWS|NOTE\nwrong|ok\n", config=cfg)["fields"]["row_count"]["status"] == "invalid_value"


def test_key_value_parser_rejects_duplicates_and_diagnostics_reuse_it():
    from reporting_platform.common.context import resolve_control_format
    cfg = {"format": resolve_control_format("test", "delivery.control", {"kind": "key_value"}),
           "row_count": "ROWS"}
    assert diagnostic(config=cfg)["ok"]
    assert not diagnostic(b"ROWS=0\nROWS=1\n", config=cfg)["ok"]


def test_mixed_data_and_control_encodings_compare_original_md5():
    import hashlib
    from reporting_platform.ingest.sample_diagnostics import diagnose_samples
    data = 'id,value\n1,€\n'.encode("cp1252")
    cfg = {"format": {"kind": "key_value", "separator": "="}, "row_count": "ROWS", "md5": "MD5"}
    fd = feed(file_encoding="cp1252", control_encoding="utf-16", arrival={}, delivery={"control": cfg})
    ctl = f"ROWS=1\nMD5={hashlib.md5(data).hexdigest()}\n".encode("utf-16")
    assert diagnose_samples(fd, data, ctl)["ok"]
    bad = ("ROWS=1\nMD5=" + "0" * 32).encode("utf-16")
    result = diagnose_samples(fd, data, bad)
    assert result["controls"]["delivery"]["fields"]["md5"]["status"] == "mismatch"


def test_manual_encoding_override_is_revalidated_without_duckdb_extensions():
    import duckdb
    from reporting_platform.ingest.sniff import sniff_bytes
    with duckdb.connect() as con:
        proposal = sniff_bytes(con, b"id,value\n1,\x81\n", "x.csv", "latin-1")
        assert proposal["file_encoding"] == "latin-1" and proposal["encoding_confidence"] == "low"
        try:
            sniff_bytes(con, b"id,value\n1,\xff\n", "x.csv", "utf-8")
        except ValueError:
            pass
        else:
            raise AssertionError("manual override was not revalidated")


def test_new_csv_options_are_validated():
    from reporting_platform.common.parsing import check_csv_options, csv_rows, feed_format
    for options in ({"multiline": "false"}, {"escape_char": "xx"}, {"skip_rows": 2}):
        try:
            check_csv_options("test", options)
        except ValueError:
            pass
        else:
            raise AssertionError(options)
    try:
        list(csv_rows(b'a,b\n1,"two\nlines"\n', feed_format(feed(csv_options={"multiline": False}))))
    except ValueError:
        pass
    else:
        raise AssertionError("multiline:false was ignored")


def test_draft_definition_uses_real_samples_without_saving():
    from reporting_platform.ingest.sample_diagnostics import from_definition
    from tests.support import config_dir
    config_dir()
    definition = dict(name="test_draft", description="Synthetic paired sample", source_system="TEST",
                      filename_pattern=r"sample_(?P<cob_date>\d{8})\.csv", business_key=["id"],
                      columns=["id", "value"], file_encoding="cp1252", control_encoding="utf-8",
                      csv_options={"escape_char": '"', "multiline": True},
                      delivery={"control": {"pattern": r"{stem}\.ctl", "row_count": "ROWS",
                                            "format": {"kind": "key_value", "separator": "="}}})
    result = from_definition(definition, b"id,value\n1,\x80\n", b"ROWS=1\n",
                             data_filename="sample_20260917.csv", control_filename="sample_20260917.ctl")
    assert result["ok"] and len(result["definition_sha256"]) == 64, result
    from reporting_platform.common.context import feeds
    assert "test_draft" not in feeds()


def test_encoding_and_dialect_roundtrip_through_registry():
    from tests.support import config_dir
    config_dir()
    from reporting_platform.ui import registry
    from reporting_platform.common import context
    original = context.feeds()["fo_trade"]
    spec = registry.spec_from_feed(original)
    spec.control_encoding = "utf-16-be"
    spec.csv_options = {"escape_char": "\\", "multiline": False}
    registry.update(spec)
    updated = context.feeds()[spec.name]
    assert updated.control_encoding == "utf-16-be"
    assert updated.csv_options == spec.csv_options


def test_invalid_identity_control_is_refused_but_bad_data_still_lands():
    from tests.test_conform import _feed, DATA
    fd = _feed()
    from reporting_platform.ingest import conform
    bad_control = conform.Siblings(names=["positions.ctl"], read=lambda _: b"\xff")
    assert isinstance(conform.plan_arrival(fd, "positions.csv", DATA, siblings=bad_control)[0], conform.Refused)
    good_control = conform.Siblings(names=["positions.ctl"], read=lambda _: b"ReportingDate|20260801\n")
    outcome = conform.plan_arrival(fd, "positions.csv", b"id,value\n1,\xff\n", siblings=good_control)[0]
    assert isinstance(outcome, conform.Planned), outcome
    assert outcome.metadata["row_count"] is None
