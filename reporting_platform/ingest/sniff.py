"""Propose a feed's format, columns and types from a real delivered file.

Step 5 of docs/DELIVERY-SHAPES.md: a normalizer running in PROPOSE MODE. Give
it a real delivery and it suggests delimiter, quote, header, encoding,
per-column types and business-key candidates -- a starting point for the
console form, never a value written without a human looking at it.

USES DUCKDB'S sniff_csv() rather than hand-rolled frequency analysis: it is a
real, tested CSV sniffer, and `scripts/duckdb_console.py` already depends on
DuckDB, so this adds no new dependency. `sniff_delivery` takes any path a
duckdb connection can read. The console-facing entry points always sniff a
LOCAL temp file, fetching the bytes with the same boto3 client
`ingest/arrival.py` uses rather than through DuckDB's S3 support -- simpler,
and it needs no Iceberg attach for something that is just reading one object.

What sniff_csv does NOT do, verified against a real duckdb 1.5.5:

* It does not know this platform's `column_types` vocabulary. Its `Columns`
  field carries duckdb's OWN SQL type names -- BIGINT, DOUBLE, VARCHAR --
  which are NOT Arrow's. DUCKDB_TYPE_MAP below translates.
* It does not detect encoding. It assumes UTF-8 (silently stripping a BOM) and
  raises a clear, catchable error on anything else.
* It has no notion of a business key. `candidate_keys` is a uniqueness scan
  layered on top, reusing sniff_csv's own `Prompt` rather than re-deriving
  delimiter/quote escaping by hand.
* "Not applicable" comes back as the LITERAL STRING `"(empty)"`, not `""` --
  checked, not assumed, after a first draft's `row["Quote"] or DEFAULT`
  silently failed to catch it. See `_or_default`.
"""
from __future__ import annotations

import ast
import re

from reporting_platform.ui.registry import platform_names
from reporting_platform.ui.scaffold import COLUMN_TYPES

# duckdb's SQL type name (the part before any `(...)` parameters, so a future
# duckdb returning `DECIMAL(18,2)` still matches `DECIMAL`) -> this platform's
# column_types vocabulary (ui/scaffold.py:COLUMN_TYPES).
#
# Checked against a real duckdb 1.5.5, not assumed: a decimal-looking column
# ("100.50") comes back as plain DOUBLE, never a parametrised DECIMAL, and an
# integer overflowing BIGINT falls back to DOUBLE rather than HUGEINT.
#
# Deliberately NOT listed, falling back to "string" via `platform_type`: TIME,
# TIMESTAMP(TZ), INTERVAL, BLOB, UUID. None has a platform cast to land in --
# `engine.sql`'s `parse_date` only parses a DATE-shaped string -- and "string"
# is the safe direction: forcing a TIMESTAMP into `date` would drop the time of
# day with no error raised anywhere.
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
# here can genuinely fail -- checked against a real duckdb.
#
# ORDER IS NOT ARBITRARY, and reversing it makes one entry dead code. duckdb's
# `latin-1` rejects the C1 control range (0x80-0x9F) that `cp1252` accepts
# except for five undefined slots. Checked byte-by-byte: every byte latin-1
# accepts, cp1252 also accepts, plus 27 more -- so with cp1252 first, latin-1
# could never succeed. latin-1 goes first because plain Western-European text
# is genuinely ISO-8859-1 and that is the more accurate label; cp1252 is tried
# LAST as the widest-accepting, which is why `sniff_delivery` reports it as
# low-confidence.
#
# `utf-16` IS NOT HERE, and that absence was earned: `sniff_csv(...,
# encoding='utf-16')` on plain ASCII/latin-1 bytes does NOT raise -- it
# reinterprets byte-pairs as UTF-16 code units and "succeeds" with a single
# garbled column, before anything else gets a turn, reporting HIGH confidence
# for mojibake. Without a BOM UTF-16 has no reliable signature, so it is only
# tried when `_bom_encoding` finds one.
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

    Single-column candidates only -- a composite key ("cob_date plus
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


# ------------------------------------------------------ from raw bytes
def sniff_bytes(con, data: bytes, filename: str) -> dict:
    """`sniff_delivery`, given the delivery's bytes directly rather than a
    path `con` can already read -- an upload, or a file already fetched from
    object storage. `.zip` dispatches to `sniff_archive`; everything else is
    written to a local temp file (`sniff_csv` needs a real path) and sniffed
    from there.
    """
    import tempfile

    if filename.lower().endswith(".zip"):
        return sniff_archive(con, data)
    suffix = "." + filename.rsplit(".", 1)[-1] if "." in filename else ""
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(data)
    return sniff_delivery(con, f.name)


# ------------------------------------------------ member control files
# Extensions a sender uses for a file that SAYS SOMETHING ABOUT another file
# rather than holding rows. Deliberately short: every entry is a name that is
# a control file in practice and almost never a data file, because a wrong
# entry would take a real data member out of the proposal.
CONTROL_SUFFIXES = ("ctl", "trl", "done", "ok")


def _stem(name: str) -> str:
    """THE GATE'S OWN STEM RULE, not a second copy of it: `conform._stem`,
    which is what `{stem}` is substituted with when a member's control file
    is looked for. A name with no extension is its own stem -- `POSA` beside
    `POSA.ctl`, the mainframe shape -- and a sniffer that skipped those
    proposed `.*\\.ctl` for exactly the containers this exists to read."""
    from reporting_platform.ingest.conform import _stem as gate_stem

    return gate_stem(name)


def _has_control_suffix(name: str) -> bool:
    """Whether any EXTENSION of `name` -- every dot-component after the first
    -- is a control suffix. `POS_A.ctl`, `POS_A.ctl.csv` and `POS_A.csv.done`
    all are; `POS_A.csv` and `POSA` are not."""
    return any(part.lower() in CONTROL_SUFFIXES for part in name.split(".")[1:])


def _pair_control_members(names: list[str]) -> dict[str, str]:
    """{control member: the data member it belongs to}, recognised BY NAME.

    A control member is a name with a control suffix that is some other
    member's STEM plus a dot and more -- `POS_A.ctl`, `POS_A.ctl.csv` or
    `POS_A.csv.ctl` beside `POS_A.csv`, and `POSA.ctl` beside an
    extensionless `POSA`. That is exactly the relation `conform.find_control`
    reads with a `{stem}` pattern, so recognising it here and proposing
    `{stem}...` there are the same rule.

    NAMES ONLY, NEVER CONTENT. "A short key/value or single-row delimited
    file" is also exactly what a one-row data delivery for a quiet day looks
    like, so content cannot tell a control file from a small data file -- and
    classifying a real member as control would drop it from the proposal
    without a word. Content is read only AFTER a pair is recognised, to say
    how the control file might be read (`_control_readings`).

    An unpaired name with a control suffix is not claimed: a container-level
    `BATCH.done` is not a member's control file, and nothing here would know
    what `{stem}` should be for it. With several candidate stems the LONGEST
    wins, so `POS.A.ctl` belongs to `POS.A.csv`, not `POS.csv`.
    """
    stems = {n: _stem(n) for n in names if not _has_control_suffix(n)}
    out: dict[str, str] = {}
    for name in names:
        if name in stems:
            continue
        owners = [d for d, stem in stems.items() if name.startswith(stem + ".")]
        if owners:
            out[name] = max(owners, key=lambda d: len(stems[d]))
    return out


def _control_pattern(pairs: dict[str, str]) -> str | None:
    """The one `{stem}...` pattern every pair fits, in the syntax both
    control blocks load (`'{stem}\\.ctl'`), or None when the pairs disagree
    -- `.ctl` for some members and `.CTL` for others is not one pattern, and
    picking the commoner would leave the rest refused at the gate.

    Checked the way the gate will use it, not assumed: each control member
    must fullmatch the pattern with its data member's stem substituted
    (`conform.control_filename_for`), and no data member may match its
    `{stem}`-as-wildcard form (`conform.is_member_control_file`), which is the
    test that keeps a data member from being skipped as a control file.
    """
    remainders = {c[len(_stem(d)):] for c, d in pairs.items()}
    if len(remainders) != 1:
        return None
    remainder = remainders.pop()
    if "{" in remainder or "}" in remainder:
        return None                    # `str.format` would read it as a field
    pattern = "{stem}" + re.escape(remainder)
    for control, data in pairs.items():
        if not re.fullmatch(pattern.format(stem=re.escape(_stem(data))), control):
            return None
    wildcard = re.compile(pattern.replace("{stem}", "(?P<stem>.+)"))
    if any(wildcard.fullmatch(d) for d in set(pairs.values())):
        return None
    return pattern


# What a control file may declare that the sniffer can offer, and the group
# each needs under `kind: regex` -- `context.CONTROL_FIELD_GROUPS`, restated as
# the value shape that group has to capture.
_FIELD_VALUE = {"cob_date": r"\d{8}", "row_count": r"\d+",
                "md5": r"[0-9a-fA-F]{32}"}
_KEY_VALUE = re.compile(r"^\s*([A-Za-z][\w .-]*?)\s*([=:|])\s*(\S.*?)\s*$")
_CONTROL_DELIMITERS = ("|", ",", "\t", ";")


def _table_reading(lines: list[list[str]]) -> tuple[dict, list[dict[str, str]]] | None:
    """The files read as `kind: delimited`: one header row over exactly one
    row of values, in one delimiter and under one header every file agrees
    on -- what `control._read_delimited` reads. A header cell that is all
    digits is not a column name, which is what rules out
    `ReportingDate|20260801` over `Rows|2`."""
    import csv
    import io

    for delim in _CONTROL_DELIMITERS:
        parsed = []
        for ls in lines:
            rows = list(csv.reader(io.StringIO("\n".join(ls)), delimiter=delim))
            if len(rows) != 2 or len(rows[0]) < 2 or len(rows[0]) != len(rows[1]):
                break
            header = [c.strip() for c in rows[0]]
            if (not all(header) or len(set(header)) != len(header)
                    or any(c.isdigit() for c in header)):
                break
            parsed.append(dict(zip(header, (v.strip() for v in rows[1]))))
        else:
            if len({tuple(p) for p in parsed}) == 1:
                return {"kind": "delimited", "delimiter": delim}, parsed
    return None


def _separators(lines: list[list[str]]) -> set[str]:
    """Every key/value separator the text reading found, line by line --
    counted before `_text_reading` keeps one value per key."""
    return {m.group(2) for ls in lines for ln in ls
            if (m := _KEY_VALUE.match(ln))}


def _text_reading(lines: list[list[str]]) -> tuple[None, list[dict[str, str]]] | None:
    """The files read as the default text format: every non-blank line a
    `KEY=VALUE` (or `:`/`|`) pair, keyed by `KEY=` as written."""
    parsed = []
    for ls in lines:
        fields: dict[str, str] = {}
        for ln in ls:
            m = _KEY_VALUE.match(ln)
            if not m:
                return None
            fields.setdefault(m.group(1) + m.group(2), m.group(3))  # "DATE="
        parsed.append(fields)
    return None, parsed


def _control_readings(texts: list[str]) -> list[tuple[dict | None, list[dict[str, str]]]]:
    """Every CONSISTENT reading of the control files, as (format, each file's
    {field name: value}); the format is None for the default text reading.

    Empty when there is none -- including an EMPTY control file, the ordinary
    `.done`/`.ok`, which says "complete" and declares nothing.

    TWO READINGS IS AN ANSWER, not a tie to break. `FEED|POSITIONS` over
    `ROWS|2` is two KEY|VALUE lines and also a table with columns FEED and
    POSITIONS; `cob_date|row_count` over `20260901|3` is only a table,
    because `20260901` cannot be a key. Where the bytes genuinely read both
    ways, which one the sender means decides what every field names, and
    nothing in the files says -- so the caller proposes no format.

    Two narrowings, each because the second "reading" was a regex being
    permissive rather than the same bytes read another way:

    * Only a TWO-column table. A KEY|VALUE line has exactly two cells, so
      `FEED|BUSINESS_DATE|RECORD_COUNT` "matching" as key FEED with value
      `BUSINESS_DATE|RECORD_COUNT` is not a key/value line.
    * Only where every line's key/value separator IS the table's delimiter.
      `A=1,B=2` over `C=3,D=4` is a comma table AND `=` lines, but those are
      not the same cells read two ways -- and the TEXT reading survives: the
      table's "header" is `A=1` and its values `C=3`, so no field could ever
      be a number or a date under it, while a text-format regex can read any
      of the four. A format under which nothing is readable is not a reading.
    """
    lines = [[ln for ln in t.splitlines() if ln.strip()] for t in texts]
    if not all(lines):
        return []
    table = _table_reading(lines)
    text = _text_reading(lines)
    if table is not None and len(table[1][0]) > 2:
        return [table]
    if table is not None and text is not None \
            and _separators(lines) != {table[0]["delimiter"]}:
        return [text]
    return [r for r in (table, text) if r]


def _field_expression(fmt: dict | None, key: str, field: str) -> str:
    """A field as the block declares it: a COLUMN NAME under delimited, a
    regex with the one named group the loader requires under text. Anchored
    to the start of a line, or `ROWS=` would also read `TOTAL_ROWS=` -- and
    allowing the same leading whitespace `_KEY_VALUE` allows, or an indented
    file is recognised and then every candidate fails its read-back."""
    if fmt is not None:
        return key
    from reporting_platform.common.context import CONTROL_FIELD_GROUPS

    name, sep = key[:-1], key[-1]
    return (f"(?m)^\\s*{re.escape(name)}\\s*{re.escape(sep)}\\s*"
            f"(?P<{CONTROL_FIELD_GROUPS[field]}>{_FIELD_VALUE[field]})")


def _member_facts(data: bytes, proposal: dict) -> dict:
    """What a data member's control file could be checked against, measured
    ONCE per member: its md5, and its rows read the way `conform.count_rows`
    reads them, with the dialect the sniffer just proposed -- so a quoted
    newline is one row."""
    import csv
    import hashlib
    import io

    text = data.decode(proposal["file_encoding"], errors="replace")
    rows = sum(1 for r in csv.reader(io.StringIO(text),
                                     delimiter=proposal["delimiter"],
                                     quotechar=proposal["quote_char"]) if r)
    return {"md5": hashlib.md5(data).hexdigest(),
            "rows": max(rows - 1, 0) if proposal["header"] else rows}


def _control_field_candidates(pattern: str, fmt: dict | None,
                              parsed: list[dict[str, str]],
                              controls: list[tuple[str, str, dict]]
                              ) -> dict[str, list[str]]:
    """{field: [expression, ...]} -- every key that COULD be each field, as
    evidence rather than as a choice. `controls` is (control member, its
    text, its data member's `_member_facts`), in `parsed`'s order.

    * `cob_date`: an 8-digit value that is a real yyyyMMdd date in EVERY
      control file. Which date in a control file is the COB date -- rather
      than a run date or a creation date -- is a claim about meaning that no
      measurement can make, the business-key problem exactly.
    * `row_count`: a value EQUAL to the paired member's row count, in every
      pair.
    * `md5`: a value EQUAL to the paired member's md5, in every pair. The
      member's own, because under `arrival.archive` each member lands as a
      plain delivery and that is the object `delivery.control.md5` covers.

    `version` is never offered: nothing observable distinguishes a version
    number from any other small integer.

    EVERY CANDIDATE IS READ BACK THROUGH `ingest/control.py` -- the only
    control-file parser -- over every control member, and dropped unless it
    returns the value it was derived from. The expression offered is one the
    gate will read, not one that looks right.
    """
    from datetime import datetime

    from reporting_platform.common.context import resolve_control_format
    from reporting_platform.ingest import control as control_mod

    def is_date(v: str) -> bool:
        try:
            return bool(re.fullmatch(r"\d{8}", v)) and bool(
                datetime.strptime(v, "%Y%m%d"))
        except ValueError:
            return False

    tests = {
        "cob_date": lambda v, facts: is_date(v),
        "row_count": lambda v, facts: v.isdigit() and int(v) == facts["rows"],
        "md5": lambda v, facts: (bool(re.fullmatch(_FIELD_VALUE["md5"], v))
                                 and v.lower() == facts["md5"]),
    }
    keys = set(parsed[0]).intersection(*parsed[1:])
    resolved = resolve_control_format("sniff", "control", fmt)
    out: dict[str, list[str]] = {}
    for field, test in tests.items():
        found = []
        for key in sorted(keys):
            if not all(test(p[key], facts)
                       for p, (_c, _t, facts) in zip(parsed, controls)):
                continue
            expression = _field_expression(fmt, key, field)
            block = {"pattern": pattern, "format": resolved, field: expression}
            try:
                read_back = [control_mod.read(block, text, fields=(field,),
                                              feed_name="sniff", filename=c,
                                              block="control").get(field)
                             for c, text, _facts in controls]
            except control_mod.ControlParseError:
                continue
            if read_back == [p[key] for p in parsed]:
                found.append(expression)
        if found:
            out[field] = found
    return out


def _member_control(zf, names: list[str], pairs: dict[str, str],
                    proposal: dict) -> dict:
    """What a container of control-gated members proposes -- see
    `sniff_archive`. Never a decision: `pattern` and `format` are what the
    NAMES and the files' SHAPE say, and every field is a list of candidates.

    Control files are decoded with the proposal's `file_encoding` -- the value
    the form saves, and what the gate decodes a member's control file with --
    so the read-back checks the text the gate will read.
    """
    pattern = _control_pattern(pairs)
    paired_data = set(pairs.values())
    out: dict = {
        "pattern": pattern,
        "format": None,
        "format_ambiguous": False,
        # {control member: its data member}. Keyed by the CONTROL member,
        # because a data member may have two (`X.ctl` and `X.done`).
        "pairs": dict(sorted(pairs.items())),
        "members_without_control": [n for n in names
                                    if n not in pairs and n not in paired_data],
        "field_candidates": {},
    }
    if pattern is None:
        return out
    encoding = proposal["file_encoding"]
    # One member's bytes at a time: measured, then released.
    facts = {d: _member_facts(zf.read(d), proposal) for d in sorted(paired_data)}
    controls = [(c, zf.read(c).decode(encoding, errors="replace"), facts[d])
                for c, d in sorted(pairs.items())]
    readings = _control_readings([text for _c, text, _f in controls])
    if len(readings) == 1:
        fmt, parsed = readings[0]
        out["format"] = fmt
        out["field_candidates"] = _control_field_candidates(
            pattern, fmt, parsed, controls)
    elif len(readings) == 2:
        out["format_ambiguous"] = True
        # Only ever the same cells read two ways -- see `_control_readings`
        # -- so the text reading's separator IS the table's delimiter; it is
        # recorded from the text reading itself, and the note words it so.
        out["key_value_separator"] = sorted(_separators(
            [[ln for ln in text.splitlines() if ln.strip()]
             for _c, text, _f in controls]))[0]
        out["field_candidates_by_reading"] = {
            ("text" if fmt is None else "delimited"):
                {"format": fmt,
                 "field_candidates": _control_field_candidates(
                     pattern, fmt, parsed, controls)}
            for fmt, parsed in readings}
    return out


def _member_pattern_candidate(names: list[str]) -> str | None:
    """A regex matching the archive's most common member extension, as a
    STARTING GUESS for `delivery.member_pattern` -- a human still decides
    which members actually belong to this feed. None if nothing in the
    archive has an extension to group by.
    """
    from collections import Counter

    exts = Counter(n.rsplit(".", 1)[-1] for n in names if "." in n)
    if not exts:
        return None
    ext, _count = exts.most_common(1)[0]
    return rf".*\.{re.escape(ext)}"


def _paired_member_pattern_candidate(data_names: list[str]) -> str | None:
    """`_member_pattern_candidate` for the data members that HAVE a control
    file, where having no extension is a shape of its own.

    `[^.]+` -- every member with no dot in its name -- is the honest analogue
    of `.*\\.csv` for `POSA`/`POSB`: the same claim, "the members shaped like
    the ones that came with a control file", and just as much a guess to
    check. It cannot claim `POSA.ctl`.

    TIES, decided by the counts and never by where a name sorts:

    * the extensionless shape shares the top count with any other shape --
      None. `[^.]+` and `.*\\.csv` claim disjoint sets of members, so either
      would silently drop the other half, and nothing says which half the
      sender means.
    * two EXTENSIONS share it -- whatever `_member_pattern_candidate` picks
      for those members, which is the unpaired rule and must stay what it
      is. The same tie then gets the same answer whether or not the members
      came with control files; a second tie rule here would make pairing
      change a proposal that has nothing to do with control files.
    """
    from collections import Counter

    names = sorted(data_names)
    shapes = Counter(n.rsplit(".", 1)[-1] if "." in n else None for n in names)
    if not shapes:
        return None
    top = max(shapes.values())
    leaders = [shape for shape, count in shapes.items() if count == top]
    if None in leaders:
        return r"[^.]+" if len(leaders) == 1 else None
    return _member_pattern_candidate([n for n in names
                                      if "." in n and n.rsplit(".", 1)[-1] in leaders])


def sniff_archive(con, zip_bytes: bytes, member_pattern: str | None = None) -> dict:
    """Propose a feeds.yml `delivery: {kind: archive, ...}` shape.

    Extracts matching members to a local temp file and sniffs the FIRST one
    (sorted by name, matching `ingest/normalize.py`'s own member ordering)
    -- `parts: concat` (the only mode this platform builds, see
    DECISIONS.md#archive-normalizer) means every member in a delivery is
    assumed to share the same shape, so looking at one is looking at all of
    them.

    `member_pattern` is optional: absent, every member is a candidate, which
    is the ONBOARDING case -- there is no feed yet to have declared one, and
    `member_pattern_candidate` in the result is a starting guess grouped by
    extension. Passed, only matching members are considered, which is the
    re-sniff-an-existing-feed case.

    Only proposes `cob_date_from: "container"`, the one value this
    platform actually reads (`context.NOT_BUILT` rejects `member`/`path` at
    load) -- see `_container_has_a_date`.

    MEMBERS WITH THEIR OWN CONTROL FILES are a different shape, and one this
    used to get right only alphabetically: `POS_A.csv` sorts before
    `POS_A.ctl`, but `POS_A.dat` does not, and the control file was then the
    member sniffed and `.*\\.ctl` the member pattern proposed. When
    `_pair_control_members` recognises pairs, the control members are taken
    out of both, the data members that HAVE a control file are the evidence
    for the member pattern, and `member_control` carries the proposal for
    the control half -- see `propose_feed` for what it means. With no pairs
    the result is exactly what it was.
    """
    import io
    import zipfile as _zipfile

    with _zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = sorted(i.filename for i in zf.infolist() if not i.is_dir())
        pairs = _pair_control_members(names)
        # The members that have a control file are the deliveries; without
        # pairs every member is a candidate, as it always was.
        data_names = sorted(set(pairs.values())) if pairs else names
        candidates = data_names
        if member_pattern:
            rx = re.compile(member_pattern)
            candidates = [n for n in names
                          if rx.fullmatch(n) and n not in pairs]
        if not candidates:
            raise ValueError(
                "archive holds no member matching "
                f"{member_pattern!r}" if member_pattern else "archive is empty")
        member = candidates[0]
        data = zf.read(member)

        proposal = sniff_bytes(con, data, member)
        proposal["archive_members"] = names
        proposal["sniffed_member"] = member
        proposal["member_pattern_candidate"] = (
            _paired_member_pattern_candidate(data_names) if pairs
            else _member_pattern_candidate(names))
        if pairs:
            proposal["member_control"] = _member_control(zf, names, pairs,
                                                         proposal)
    return proposal


def _container_has_a_date(filename: str) -> bool:
    """Whether the CONTAINER's own name has an 8-digit run to anchor a
    `cob_date` group to -- reusing `ui.registry.derive_pattern`'s own
    check, since that is exactly what decides whether
    `cob_date_from: container` (the only value this platform reads) is
    even proposable. If not, it cannot be a `delivery.kind: archive` feed --
    `member`/`path` sourcing is real, described in docs/DELIVERY-SHAPES.md,
    and NOT BUILT (`context.NOT_BUILT`) -- and the proposal says so rather
    than suggesting a value that will fail at load. It can still be an
    `arrival.archive` feed, unpacked at the gate, where the container's name
    is not a date source at all.
    """
    from reporting_platform.ui.registry import derive_pattern

    return derive_pattern(filename) is not None


def propose_feed(filename: str, data: bytes) -> dict:
    """The whole onboarding proposal for one delivered file: everything
    `sniff_bytes`/`sniff_archive` return, plus the `filename_pattern`
    `ui.registry.derive_pattern` would suggest from `filename` and, for an
    archive, whether the container's own name has a date to source
    `cob_date_from: container` from at all.

    What the console calls. Opens its own `duckdb.connect()` -- callers pass
    a filename and bytes, not a connection to manage.
    """
    import duckdb

    from reporting_platform.ui.registry import derive_pattern

    con = duckdb.connect()
    proposal = sniff_bytes(con, data, filename)
    dated = derive_pattern(filename)
    proposal["filename_pattern"] = dated
    proposal["filename_has_date"] = dated is not None

    # A PLAIN FILE WITH NO DATE IN ITS NAME IS ONBOARDABLE, and the proposal
    # has to say how or the console dead-ends: `derive_pattern` returns None
    # and the form requires a cob_date group. So propose the arrival shape --
    # the source pattern is the name as sent, escaped, and the landing pattern
    # is left for the operator, who is the only one who knows what this feed
    # should be called.
    #
    # Only for a plain file here. A zip is proposed as `delivery.kind:
    # archive` unless its members carry their own control files, below.
    if dated is None and not filename.lower().endswith(".zip"):
        proposal["arrival_source_pattern"] = re.escape(filename)
    if filename.lower().endswith(".zip"):
        proposal["container_has_date"] = _container_has_a_date(filename)

    # MEMBERS WITH THEIR OWN CONTROL FILES ARE THE `arrival.archive` SHAPE,
    # whatever the container is called. `delivery.kind: archive` reads a
    # control file beside the CONTAINER in landing and never looks inside it,
    # so a control file per member only means something if each member is its
    # own delivery -- unpacked at the gate, the container never landing.
    #
    # Which changes what two fields mean. The container's name becomes the
    # arrival `source_pattern` (a date in it is matched, never read), and the
    # derived `filename_pattern` is withdrawn: under this shape it names each
    # MEMBER after renaming, and a `\.zip` there matches nothing for ever.
    # Like the undated plain file above, the landing name is the operator's.
    control = proposal.get("member_control")
    if control is not None:
        m = re.search(r"(?<!\d)\d{8}(?!\d)", filename)
        proposal["arrival_source_pattern"] = (
            re.escape(filename[:m.start()]) + r"\d{8}(?:_v\d+)?"
            + re.escape(re.sub(r"^_v\d+", "", filename[m.end():]))
            if m else re.escape(filename))
        proposal["filename_pattern"] = None
        control["note"] = _member_control_note(
            control, proposal["member_pattern_candidate"])
    return proposal


def _member_control_note(control: dict, member_pattern_candidate: str | None) -> str:
    """The proposal in words, for the console -- including what it is NOT."""
    pairs = control["pairs"]
    no_member_pattern = (
        " No member pattern is proposed: the members with a control file do "
        "not share one shape to name (some have an extension, as many do not) "
        "-- write `arrival.archive.member_pattern` by hand."
        if member_pattern_candidate is None else "")
    if control["pattern"] is None:
        return (f"{len(pairs)} member(s) have what looks like their own control "
                f"file, but the names do not share one `{{stem}}` pattern "
                f"(e.g. .ctl for some, .CTL for others), so none is proposed "
                f"and neither control block is filled in. Write the SAME "
                f"pattern into `arrival.control.pattern` and "
                f"`delivery.control.pattern` by hand -- one without the other "
                f"is refused at load -- and give the COB date a source, or the "
                f"feed will not save." + no_member_pattern)
    bits = [
        f"{len(pairs)} member(s) carry their own control file inside the "
        f"container, which is the `arrival.archive` shape: unpacked at the "
        f"gate, each member landing as its own delivery. Proposed "
        f"`arrival.control.pattern: '{control['pattern']}'`. "
        f"`delivery.control` is REQUIRED alongside it, with the same pattern "
        f"and format -- `arrival.control` alone is refused at load -- and a "
        f"member whose control file is missing from a container is refused "
        f"at the gate. A proposal: confirm it against what the sender "
        f"actually promises before saving." + no_member_pattern]
    if control["members_without_control"]:
        bits.append("Members with no control file, which this shape would "
                    "refuse if the member pattern claims them: "
                    + ", ".join(control["members_without_control"]) + ".")
    if control["format_ambiguous"]:
        readings = control["field_candidates_by_reading"]
        delimiter = readings["delimited"]["format"]["delimiter"]
        separator = control["key_value_separator"]
        bits.append(
            f"AMBIGUOUS FORMAT: every control file reads both as KEY{separator}"
            f"VALUE lines and as a one-row table delimited by {delimiter!r}, "
            f"and which the sender means decides what every field names -- so "
            f"no format is proposed. Choose one; the candidates under each "
            f"reading are:")
        for name, label in (("text", "as text"), ("delimited", "as a table")):
            found = readings[name]["field_candidates"]
            bits.append(f"{label}: " + ("; ".join(
                f"{field} {', '.join(v)}" for field, v in found.items())
                or "none") + ".")
        return " ".join(bits)
    candidates = control["field_candidates"]
    if not candidates:
        bits.append("No field in the control files could be matched to a "
                    "date, the member's row count or its md5 in every file -- "
                    "an empty `.done`/`.ok` declares nothing -- so none is "
                    "offered, and the COB date has to come from the member "
                    "pattern or an expression written by hand.")
    for field, label in (("cob_date", "COB date"), ("row_count", "row count"),
                         ("md5", "md5")):
        if field in candidates:
            block = "arrival.control" if field == "cob_date" else "delivery.control"
            bits.append(f"{label} candidate(s) for `{block}.{field}`: "
                        + ", ".join(candidates[field]) + ".")
    if "cob_date" in candidates:
        bits.append("Not filled in: which date is the COB date is a claim "
                    "about meaning, and the member pattern must not capture "
                    "one as well -- one fact, one source.")
    return " ".join(bits)
