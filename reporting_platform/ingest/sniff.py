"""Propose a feed's format, columns and types from a real delivered file.

Step 5 of docs/DELIVERY-SHAPES.md: a normalizer running in PROPOSE MODE.
Give it a real delivery and it suggests delimiter, quote, header, encoding,
per-column types and business-key candidates -- a starting point for the
console form, never a value written without a human looking at it.

USES DUCKDB'S sniff_csv() rather than hand-rolled frequency analysis: it is
a real, tested CSV sniffer, and `scripts/duckdb_console.py` /
`notebooks/explore.py` already depend on DuckDB being installed and able to
read `s3://lakehouse/...` directly (`REPORTING_DUCKDB_S3_SECRET`), so this
adds no new dependency and no new connection recipe -- callers pass in a
connection made with `scripts.duckdb_console.connect()`.

What sniff_csv does NOT do, verified against a real duckdb 1.5.5 rather than
assumed:

* It does not know this platform's `column_types` vocabulary. Its `Columns`
  field carries duckdb's OWN SQL type names -- BIGINT, DOUBLE, VARCHAR --
  which are NOT Arrow's (`int64`/`float64`/`utf8`). DUCKDB_TYPE_MAP below
  translates.
* It does not detect encoding. It assumes UTF-8 (silently stripping a UTF-8
  BOM) and raises a clear, catchable error on anything else -- see
  `_sniff_with_encoding`.
* It has no notion of a business key. `candidate_keys` is a uniqueness scan
  layered on top, reusing sniff_csv's own `Prompt` -- a ready-to-run
  `read_csv(...)` call -- rather than re-deriving delimiter/quote escaping
  by hand.
* "Not applicable" comes back as the LITERAL STRING `"(empty)"`, not `""` --
  checked, not assumed, after a first draft's `row["Quote"] or DEFAULT`
  silently failed to catch it (a non-empty string is truthy). See
  `_or_default`.
"""
from __future__ import annotations

import ast
import re

from reporting_platform.ui.registry import platform_names
from reporting_platform.ui.scaffold import COLUMN_TYPES

# duckdb's SQL type name (the part before any `(...)` parameters -- so a
# future duckdb version returning `DECIMAL(18,2)` still matches `DECIMAL`) ->
# this platform's column_types vocabulary (ui/scaffold.py:COLUMN_TYPES).
#
# Checked against a real duckdb 1.5.5, not assumed: a decimal-looking column
# ("100.50") comes back as plain DOUBLE, never a parametrised DECIMAL, and an
# integer overflowing BIGINT's range falls back to DOUBLE too rather than
# HUGEINT. The base-name strip handles a parametrised type anyway, since
# nothing here depends on duckdb continuing to prefer DOUBLE.
#
# Deliberately NOT listed, falling back to "string" via `platform_type`:
# TIME, TIMESTAMP (and TIMESTAMPTZ), INTERVAL, BLOB, UUID. Every one of them
# has no platform cast to land in -- `dbt/macros/engine.sql`'s `parse_date`
# only parses a DATE-shaped string, there is no `parse_timestamp` -- and
# "string" is the safe direction: a TRY_CAST-free passthrough, never a value
# the platform would silently mis-cast or truncate (forcing a TIMESTAMP into
# `date` would drop the time of day with no error raised anywhere).
DUCKDB_TYPE_MAP = {
    "TINYINT": "integer", "SMALLINT": "integer", "INTEGER": "integer",
    "BIGINT": "integer", "HUGEINT": "integer",
    "UTINYINT": "integer", "USMALLINT": "integer", "UINTEGER": "integer",
    "UBIGINT": "integer", "UHUGEINT": "integer",
    "FLOAT": "decimal", "DOUBLE": "decimal", "REAL": "decimal",
    "DECIMAL": "decimal", "NUMERIC": "decimal",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "VARCHAR": "string",
}
assert set(DUCKDB_TYPE_MAP.values()) <= set(COLUMN_TYPES), (
    "DUCKDB_TYPE_MAP names a platform column kind ui.scaffold.COLUMN_TYPES "
    "does not -- keep the two in sync")

# What duckdb prints in a field that does not apply, e.g. `Quote` when
# nothing in the sample needed quoting. A LITERAL STRING, not "" -- `or` will
# not catch it, an explicit membership check is required.
_NOT_APPLICABLE = "(empty)"


def _or_default(value: str, default: str) -> str:
    return default if value in ("", _NOT_APPLICABLE) else value


def platform_type(duckdb_type: str) -> str:
    """DuckDB's own SQL type name -> this platform's column_types vocabulary.

    Falls back to "string" for anything DUCKDB_TYPE_MAP does not name -- see
    the module header for why that is the safe direction, not a cop-out.
    """
    base = duckdb_type.split("(", 1)[0].strip().upper()
    return DUCKDB_TYPE_MAP.get(base, "string")


def _lit(value: str) -> str:
    """A single-quoted SQL string literal. Doubling `'` is the whole rule."""
    return "'" + value.replace("'", "''") + "'"


def _row(con, sql: str) -> dict:
    """One row as {column_name: value}. `.description` is None on a duckdb
    Relation after `.fetchone()` -- checked -- so column names come from
    `.columns` on the relation instead, read before fetching consumes it."""
    rel = con.sql(sql)
    values = rel.fetchone()
    return dict(zip(rel.columns, values)) if values else {}


# --------------------------------------------------------------- encoding
# Tried in order once a BOM does not settle it. sniff_csv rejects a byte
# sequence invalid for the encoding it is told to assume, and EVERY encoding
# here can genuinely fail -- checked against a real duckdb, not assumed.
#
# ORDER IS NOT ARBITRARY, and getting it backwards makes one of the two
# entries dead code. duckdb's `latin-1` rejects the C1 control range
# (0x80-0x9F) that ordinary ISO-8859-1 would accept; `cp1252` accepts that
# ENTIRE range except five undefined slots. Checked byte-by-byte against a
# real duckdb: every byte latin-1 accepts, cp1252 also accepts, plus 27 more
# -- so with cp1252 tried first, latin-1 can never be the one that succeeds.
# latin-1 goes first instead: plain Western-European text with no smart
# quotes or em-dashes is genuinely ISO-8859-1, and calling it that is a more
# accurate label than the Windows-specific one. `cp1252` is what is actually
# tried LAST here, because it is the one that accepts the widest range of
# anything in this list -- which is why `sniff_delivery` reports it as
# low-confidence rather than presenting it with the same weight as a clean
# UTF-8 read.
#
# `utf-16` IS NOT HERE, and that absence was earned, not an oversight: a
# first draft included it and `sniff_csv(..., encoding='utf-16')` on plain
# ASCII/latin-1 bytes does NOT raise -- checked -- it reinterprets byte-pairs
# as UTF-16 code units and "succeeds" with a single garbled column, which is
# worse than useless as a fallback: it "succeeds" before anything else in
# this list ever gets a turn, reporting HIGH confidence for mojibake. UTF-16
# has no reliable signature in the bytes themselves without a BOM, so it is
# only ever tried when `_bom_encoding` finds one -- see `_sniff_with_encoding`.
ENCODING_FALLBACKS = ["utf-8", "latin-1", "cp1252"]


def _bom_encoding(head: bytes) -> str | None:
    """An encoding implied by a byte-order mark, or None.

    The ONLY path that ever proposes "utf-16" -- see ENCODING_FALLBACKS for
    why guessing it from content alone is actively wrong. UTF-8's BOM is not
    handled here: sniff_csv strips it itself (checked), so plain "utf-8" --
    already first in ENCODING_FALLBACKS -- handles that case with no special
    casing needed above this function.
    """
    if head.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    return None


def _peek(con, path: str, n: int = 8) -> bytes:
    """The delivery's first `n` bytes, local or `s3://` alike."""
    row = _row(con, f"SELECT content FROM read_blob({_lit(path)}) LIMIT 1")
    return row.get("content", b"")[:n]


def _is_encoding_failure(exc: Exception) -> bool:
    """Whether `exc` is sniff_csv rejecting the ENCODING it was told to
    assume, rather than some other invalid-input problem worth surfacing as
    -is.

    Matched on the message, and deliberately loosely: a first draft matched
    the exact wording duckdb uses for a UTF-8 failure ("...is not utf-8
    encoded") and missed latin-1's differently-worded one ("File is not
    latin-1 encoded", no "Invalid unicode" preamble at all) entirely, so
    that failure propagated raw instead of moving to the next candidate.
    "encoded" is the one word every observed shape shares.
    """
    return "encoded" in str(exc).lower()


def _sniff_with_encoding(con, path: str) -> tuple[dict, str]:
    """(sniff_csv()'s row as a dict, the encoding that actually worked).

    A BOM-implied encoding is tried first; ENCODING_FALLBACKS after that, in
    order, skipping anything already tried. Every encoding here CAN raise --
    see ENCODING_FALLBACKS -- so reaching the end of the list without one
    working is real and raises a clear error; it is the caller's job to
    treat even a successful `latin-1` read as low-confidence, not this
    function's job to refuse it.
    """
    import duckdb as ddb

    bom = _bom_encoding(_peek(con, path))
    ordered = [bom] + ENCODING_FALLBACKS if bom else ENCODING_FALLBACKS
    tried: list[str] = []
    last_exc: Exception | None = None
    for encoding in ordered:
        if encoding in tried:
            continue
        tried.append(encoding)
        try:
            row = _row(con, f"SELECT * FROM sniff_csv({_lit(path)}, "
                            f"encoding={_lit(encoding)})")
            return row, encoding
        except ddb.InvalidInputException as exc:
            if not _is_encoding_failure(exc):
                raise  # a real format problem, not an encoding guess to retry
            last_exc = exc
    raise ValueError(
        f"{path}: could not read as any of {tried} -- last error: {last_exc}")


# ------------------------------------------------------------- the proposal
def _file_headers(prompt: str) -> list[str]:
    """The FILE's own header names, in order, out of sniff_csv's `Prompt`.

    `Prompt`'s `columns={...}` dict is already in file order and is what
    `candidate_keys` quotes into its uniqueness query -- it must match the
    file's actual headers, not the platform names `platform_names` derives
    from them.
    """
    m = re.search(r"columns=(\{.*?\})", prompt)
    if not m:
        raise ValueError(f"sniff_csv Prompt has no columns=... to parse: {prompt!r}")
    return list(ast.literal_eval(m.group(1)).keys())


def candidate_keys(con, path: str, prompt: str, names: list[str]) -> list[str]:
    """Platform column names whose values are unique across the WHOLE file.

    Single-column candidates only -- a composite key ("business_date plus
    counterparty_id") is still a human's call, per
    docs/DELIVERY-SHAPES.md#5-onboard-from-a-real-file.

    Reuses sniff_csv's own `Prompt` -- a complete, already-escaped
    `read_csv(...)` call -- rather than re-deriving delimiter/quote/encoding
    escaping here a second time. `names` are the PLATFORM names in column
    order; the file's own headers (from `prompt`) are what get quoted into
    the query, and results are zipped back onto `names` positionally.
    """
    file_headers = _file_headers(prompt)
    exprs = ", ".join(
        f'count(DISTINCT "{h}") AS c{i}' for i, h in enumerate(file_headers))
    query = f"SELECT count(*) AS n, {exprs} {prompt.rstrip(';').strip()}"
    row = con.sql(query).fetchone()
    total, counts = row[0], row[1:]
    return [name for name, unique in zip(names, counts)
           if total > 0 and unique == total]


def sniff_delivery(con, path: str) -> dict:
    """Propose a feeds.yml shape for the file at `path`.

    `path` is anything `con` can already read -- a local path for a test, an
    `s3://lakehouse/...` URI in production, using the same DuckDB connection
    `scripts/duckdb_console.connect()` builds (httpfs, the S3 secret gated on
    REPORTING_DUCKDB_S3_SECRET).

    Returns the FULL per-column type map, like `ui.scaffold.resolve_types`
    does -- not yet reduced to overrides. A caller persisting this into
    feeds.yml calls `ui.scaffold.overrides_only(columns, column_types)` on
    it first, the same reduction every other write path uses, so a column
    this sniffer agrees with `infer_type` about still produces no diff.
    """
    row, encoding = _sniff_with_encoding(con, path)
    file_headers = [c["name"] for c in row["Columns"]]
    names, sources = platform_names(file_headers)
    types = {name: platform_type(col["type"])
             for name, col in zip(names, row["Columns"])}
    keys = candidate_keys(con, path, row["Prompt"], names)

    return {
        "delimiter": row["Delimiter"],
        # An empty/"(empty)" Quote means sniff_csv saw no field in the
        # sample that needed quoting, not that this delivery format has
        # none -- the platform default is what every reader here assumes.
        "quote_char": _or_default(row["Quote"], '"'),
        "header": bool(row["HasHeader"]),
        "file_encoding": encoding,
        "encoding_confidence": ("low" if encoding == ENCODING_FALLBACKS[-1]
                                else "high"),
        "columns": names,
        "source_columns": sources,
        "column_types": types,
        "business_key_candidates": keys,
    }
