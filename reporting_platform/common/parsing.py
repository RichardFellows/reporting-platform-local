"""Byte and CSV contracts shared by inspection, controls and ingestion.

Landing bytes are never rewritten. Python validates strictly before Spark is
allowed to read them (Java decoders otherwise replace malformed input).
"""
from __future__ import annotations

import codecs
import csv
import io


class DecodeError(ValueError):
    pass


def decode_bytes(data: bytes, encoding: str, *, source: str = "sample") -> str:
    """Strict decoding; consume one compatible leading BOM, never an interior one."""
    try:
        codec = codecs.lookup(encoding).name
        if data.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
            raise DecodeError(f"{source}: UTF-32 BOM is outside the supported CSV contract")
        if data.startswith(codecs.BOM_UTF8) and codec in ("utf-8", "utf-8-sig"):
            codec = "utf-8-sig"
        elif data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
            actual = "utf-16-le" if data.startswith(codecs.BOM_UTF16_LE) else "utf-16-be"
            if codec in ("utf-16", actual):
                codec = "utf-16"
            elif codec in ("utf-16-le", "utf-16-be", "utf-8", "utf-8-sig"):
                raise DecodeError(f"{source}: BOM conflicts with encoding {encoding!r}")
        return data.decode(codec, errors="strict")
    except (UnicodeError, LookupError) as exc:
        raise DecodeError(f"{source}: cannot decode as {encoding!r}: {exc}") from exc


def detect_text(data: bytes, encoding: str | None = None) -> tuple[str, str]:
    """Conservative proposal only; single-byte detection is always ambiguous.

    C1 controls are not evidence for Latin-1. An explicit override can still
    select Latin-1 for such bytes. A BOM is authoritative: never retry corrupt
    Unicode as a permissive single-byte codec.
    """
    if encoding:
        return decode_bytes(data, encoding), encoding
    if data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return decode_bytes(data, "utf-16"), "utf-16"
    if data.startswith(codecs.BOM_UTF8):
        return decode_bytes(data, "utf-8"), "utf-8"
    for candidate in ("utf-8", "latin-1", "cp1252"):
        if candidate == "latin-1" and any(0x80 <= b <= 0x9f for b in data):
            continue
        try:
            return decode_bytes(data, candidate), candidate
        except DecodeError:
            pass
    raise DecodeError("could not read as any of utf-8, latin-1, cp1252; specify the source encoding")


def check_csv_options(name: str, value: object) -> dict:
    if not isinstance(value, dict) or set(value) - {"escape_char", "multiline"}:
        raise ValueError(f"{name}: csv_options accepts only escape_char and multiline")
    if "escape_char" in value and (not isinstance(value["escape_char"], str)
                                  or len(value["escape_char"]) != 1):
        raise ValueError(f"{name}: csv_options.escape_char must be one character")
    if "multiline" in value and not isinstance(value["multiline"], bool):
        raise ValueError(f"{name}: csv_options.multiline must be true or false")
    return dict(value)


def feed_format(feed) -> dict:
    options = check_csv_options(feed.name, getattr(feed, "csv_options", {}))
    fmt = dict(delimiter=feed.delimiter, quote_char=feed.quote_char,
               header=feed.header, encoding=feed.file_encoding,
               escape_char=options.get("escape_char", feed.quote_char),
               multiline=options.get("multiline", True), parser_contract=2)
    if not feed.header and hasattr(feed, "file_header"):
        fmt["columns"] = list(feed.file_header)
    return fmt


def csv_rows(data: bytes, fmt: dict, *, source: str = "sample"):
    text = decode_bytes(data, fmt["encoding"], source=source)
    quote = fmt.get("quote_char", '"')
    escape = fmt.get("escape_char", quote)
    # Spark/Univocity normalizes CRLF inside quoted values to LF. Apply the
    # same universal-newline rule to counts and previews.
    reader = csv.reader(io.StringIO(text, newline=None), delimiter=fmt["delimiter"],
                        quotechar=quote, doublequote=escape == quote,
                        escapechar=None if escape == quote else escape, strict=True)
    previous = 0
    width = len(fmt["columns"]) if fmt.get("columns") else None
    try:
        for row in reader:
            lines = reader.line_num - previous
            previous = reader.line_num
            if not fmt.get("multiline", True) and lines > 1:
                raise ValueError(f"{source}: multiline CSV record requires csv_options.multiline")
            if row:
                if width is not None and len(row) != width:
                    raise ValueError(f"{source}: CSV row at line {reader.line_num} has {len(row)} fields; expected {width}")
                width = len(row)
                yield row
    except csv.Error as exc:
        raise ValueError(f"{source}: malformed CSV at line {reader.line_num}: {exc}") from exc


def spark_encoding(encoding: str) -> str:
    """Explicit Python/Java aliases for the encodings verified by the harness."""
    aliases = {"utf-8": "UTF-8", "utf-8-sig": "UTF-8", "utf-16": "UTF-16",
               "utf-16-le": "UTF-16LE", "utf-16-be": "UTF-16BE",
               "iso8859-1": "ISO-8859-1", "cp1252": "windows-1252", "ascii": "US-ASCII"}
    codec = codecs.lookup(encoding).name
    if codec not in aliases:
        raise ValueError(f"encoding {encoding!r} has no verified Spark CSV mapping")
    return aliases[codec]


def raw_values(row: list[str]) -> list[str | None]:
    """Spark's default empty-string null token, made explicit for previews."""
    return [None if value == "" else value for value in row]
