"""Read and write reporting_platform/config/feeds.yml.

ROUND-TRIP, NOT RE-EMIT. feeds.yml is more comment than data -- the reasoning
for `cadence: weekly` on rating, for the per-feed filename pattern, for
`schema_drift: warn` -- and a plain `yaml.safe_load` / `yaml.safe_dump` cycle
silently deletes all of it. ruamel's round-trip loader preserves comments,
key order and quoting style, so adding a feed through the UI produces a diff
that touches only the feed being added.
"""
from __future__ import annotations

import codecs
import io
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from reporting_platform.common.context import CONFIG_DIR, Feed

FEEDS_YML = Path(CONFIG_DIR) / "feeds.yml"

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
COLUMN_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
# A source header may be anything the upstream felt like. It only has to be
# something that can appear in a CSV header and be matched against it.
BAD_SOURCE = re.compile(r"[\r\n]")


def platform_name(header: str) -> str:
    """`Notional (USD)` -> `notional_usd`. The name the platform will use.

    Real headers are title-cased, spaced and parenthesised; the platform needs
    an identifier, because a column name reaches SQL through dbt macros. The
    original is kept as the column's `source` -- see
    docs/DECISIONS.md#source-column-names -- so this is a suggestion the person
    can overrule, not a rename that loses anything.
    """
    out = re.sub(r"[^a-z0-9]+", "_", header.strip().lower()).strip("_")
    if not out:
        return "column"
    return out if COLUMN_RE.match(out) else f"c_{out}"


def platform_names(headers: list[str]) -> tuple[list[str], dict[str, str]]:
    """Suggested identifiers for a real header row, plus the source mapping.

    Collisions are resolved rather than allowed: `Trade Id` and `Trade-Id`
    both normalise to `trade_id`, and two columns of the same name would fail
    validation with a message about duplicates rather than about the headers
    that caused them.
    """
    names: list[str] = []
    sources: dict[str, str] = {}
    for header in headers:
        base = platform_name(header)
        name, n = base, 2
        while name in names:
            name, n = f"{base}_{n}", n + 1
        names.append(name)
        if name != header:
            sources[name] = header
    return names, sources

# Keys the UI writes into a feed block, in the order docs/ADDING-A-FEED.md
# presents them. Anything not listed here is left alone -- a per-feed
# `arrival_timeout_hours` set by hand survives an edit through the UI.
BLOCK_ORDER = ["name", "description", "source_system", "convention",
               "filename_pattern",
               "delimiter", "quote_char", "header", "file_encoding",
               "business_key", "expected_min_rows", "cadence", "completeness",
               "schema_drift", "columns", "column_types"]

# Keys only written when they differ from what the feed would INHERIT, because
# a block repeating an inherited value is noise in the diff. The four format
# keys are here rather than absent because a pipe-delimited or latin-1 feed is
# ordinary, and the alternative was hand-editing feeds.yml after every
# console-created feed.
#
# THESE VALUES ARE THE FALLBACK, NOT THE ANSWER. What a feed actually inherits
# is `defaults:` overlaid with its convention, which only feeds.yml knows --
# see `_inherited()` below. This map supplies the two keys that have no entry
# in `defaults:` at all (`cadence`, `completeness`, which default in the Feed
# dataclass) and covers the case where feeds.yml cannot be read.
OPTIONAL_WITH_DEFAULT = {"cadence": "daily", "completeness": True,
                         "schema_drift": "warn", "delimiter": ",",
                         "quote_char": '"', "header": True,
                         "file_encoding": "utf-8"}

# Two-character sequences a person types into a one-character field, because
# there is no other way to type a tab into a text input.
ESCAPES = {"\\t": "\t", "\\\\": "\\"}


def unescape_char(value: str) -> str:
    """`\\t` -> an actual tab. Anything else is returned unchanged."""
    return ESCAPES.get(value, value)


def _yaml() -> YAML:
    y = YAML()
    y.preserve_quotes = True
    # Matches the existing file: `- ` indented two inside `feeds:`, mapping
    # keys two further in.
    y.indent(mapping=2, sequence=4, offset=2)
    y.width = 100
    return y


class FeedValidationError(ValueError):
    """One or more feed fields are unusable. Carries every problem at once."""

    def __init__(self, errors: dict[str, str]):
        self.errors = errors
        super().__init__("; ".join(f"{k}: {v}" for k, v in errors.items()))


@dataclass
class FeedSpec:
    """A feed as the UI form describes it, before it becomes YAML."""
    name: str
    description: str
    source_system: str
    filename_pattern: str
    business_key: list[str]
    columns: list[str]
    expected_min_rows: int = 10
    cadence: str = "daily"
    completeness: bool = True
    schema_drift: str = "warn"
    # The `conventions:` entry this feed inherits from, or "" to stand alone.
    # Every key the convention supplies is then omitted from the feed's own
    # block, so the convention stays the single place that value is written.
    convention: str = ""
    # How to READ the file. All four default to the `defaults:` block and are
    # written only when they differ -- see OPTIONAL_WITH_DEFAULT. They reach
    # Spark's reader unchanged (ingest_feed.py), so a wrong delimiter lands one
    # column holding the whole row rather than failing.
    delimiter: str = ","
    quote_char: str = '"'
    header: bool = True
    file_encoding: str = "utf-8"
    # Sparse: ONLY the columns whose type disagrees with what infer_type()
    # guesses. The caller reduces it (scaffold.overrides_only) before handing
    # it over, so this module stays ignorant of how a type is guessed -- it
    # writes what it is told and nothing else.
    column_types: dict[str, str] = field(default_factory=dict)
    # Platform name -> the name in the FILE, for the columns that differ.
    # Sparse, like column_types. See docs/DECISIONS.md#source-column-names
    source_columns: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "FeedSpec":
        def _list(v):
            if isinstance(v, str):
                return [x.strip() for x in re.split(r"[,\s]+", v) if x.strip()]
            return [str(x).strip() for x in (v or []) if str(x).strip()]

        return cls(
            name=str(payload.get("name", "")).strip(),
            description=str(payload.get("description", "")).strip(),
            source_system=str(payload.get("source_system", "")).strip(),
            filename_pattern=str(payload.get("filename_pattern", "")).strip(),
            business_key=_list(payload.get("business_key")),
            columns=_list(payload.get("columns")),
            expected_min_rows=int(payload.get("expected_min_rows") or 0),
            cadence=str(payload.get("cadence") or "daily").strip(),
            completeness=bool(payload.get("completeness", True)),
            schema_drift=str(payload.get("schema_drift") or "warn").strip(),
            convention=str(payload.get("convention") or "").strip(),
            delimiter=unescape_char(str(payload.get("delimiter") or ",")),
            quote_char=unescape_char(str(payload.get("quote_char") or '"')),
            header=bool(payload.get("header", True)),
            file_encoding=str(payload.get("file_encoding") or "utf-8").strip(),
            column_types={str(k): str(v) for k, v in
                          (payload.get("column_types") or {}).items() if v},
            source_columns={str(k): str(v) for k, v in
                            (payload.get("source_columns") or {}).items()
                            if v and str(v) != str(k)},
        )


def validate(spec: FeedSpec, *, existing: set[str], updating: bool = False) -> None:
    """Every check that would otherwise be a silent failure downstream.

    Most of the ways a feed block can be wrong do not raise anywhere --
    docs/ADDING-A-FEED.md calls out three of them explicitly. A pattern that
    does not cover the whole filename matches nothing, and the feed reports
    "0 pending" forever; a business key that is not in `columns` gives
    `dedupe_rank` a column the raw table does not have, which fails much later
    and much less legibly.
    """
    errors: dict[str, str] = {}

    if not NAME_RE.match(spec.name):
        errors["name"] = ("must be lowercase letters, digits and underscores, "
                          "starting with a letter -- it becomes a table name, "
                          "a DAG id and an S3 prefix at once")
    elif not updating and spec.name in existing:
        errors["name"] = f"feed {spec.name!r} already exists"
    elif updating and spec.name not in existing:
        errors["name"] = f"no such feed: {spec.name!r}"

    if not spec.description:
        errors["description"] = "required -- it is the DAG description too"
    if not spec.source_system:
        errors["source_system"] = "required -- it becomes the DAG's tag"

    if spec.convention:
        # Checked here as well as at load, because the two failures are not
        # the same one. context.effective_defaults() raises when feeds.yml is
        # already wrong, which takes the whole platform down at import; this
        # catches a typo on its way IN, while it is still a message next to a
        # form field.
        from reporting_platform.common import context

        try:
            known = context.conventions()
        except Exception:                                      # noqa: BLE001
            known = {}
        if spec.convention not in known:
            errors["convention"] = (
                f"no convention named {spec.convention!r} -- defined: "
                f"{', '.join(sorted(known)) or '(none)'}")

    if not spec.filename_pattern:
        errors["filename_pattern"] = "required"
    else:
        try:
            compiled = re.compile(spec.filename_pattern)
        except re.error as exc:
            errors["filename_pattern"] = f"not a valid regex: {exc}"
        else:
            if "business_date" not in compiled.groupindex:
                errors["filename_pattern"] = (
                    "must contain a named group (?P<business_date>...) -- "
                    "arrival routing reads the business date out of the "
                    "filename and has nowhere else to get it")

    if not spec.columns:
        errors["columns"] = "at least one column is required"
    else:
        bad = [c for c in spec.columns if not COLUMN_RE.match(c)]
        dupes = sorted({c for c in spec.columns if spec.columns.count(c) > 1})
        if bad:
            errors["columns"] = f"not usable as column names: {', '.join(bad)}"
        elif dupes:
            errors["columns"] = f"duplicated: {', '.join(dupes)}"

    if not spec.business_key:
        errors["business_key"] = "at least one column is required"
    else:
        missing = [c for c in spec.business_key if c not in spec.columns]
        if missing:
            errors["business_key"] = (
                f"not in columns: {', '.join(missing)} -- the key is what "
                f"dedupe_rank partitions by, so it must be a declared column")

    unknown = sorted(set(spec.source_columns) - set(spec.columns))
    if unknown:
        errors["source_columns"] = (
            f"not declared columns: {', '.join(unknown)} -- a source name maps "
            f"ONTO a platform column, so the column has to exist")
    bad_source = sorted(k for k, v in spec.source_columns.items()
                        if not str(v).strip() or BAD_SOURCE.search(str(v)))
    if bad_source:
        errors["source_columns"] = (
            f"unusable source name for: {', '.join(bad_source)} -- it has to be "
            f"something that can appear in a header row")
    clashes = sorted({v for v in spec.source_columns.values()
                      if list(spec.source_columns.values()).count(v) > 1})
    if clashes:
        errors["source_columns"] = (
            f"two columns claim the same source name: {', '.join(clashes)}")

    if spec.expected_min_rows < 0:
        errors["expected_min_rows"] = "cannot be negative"
    if spec.cadence not in ("daily", "weekly"):
        errors["cadence"] = "must be 'daily' or 'weekly'"
    if spec.schema_drift not in ("warn", "fail"):
        errors["schema_drift"] = "must be 'warn' or 'fail'"

    # Spark's CSV reader takes a single character for `sep` and `quote`. A
    # two-character value is accepted by the form and then either throws inside
    # the ingest or, worse, splits on neither character and lands one column.
    if len(spec.delimiter) != 1:
        errors["delimiter"] = (
            "must be exactly one character (type \\t for a tab) -- it becomes "
            "Spark's `sep`, which takes a single character")
    if len(spec.quote_char) != 1:
        errors["quote_char"] = "must be exactly one character"
    try:
        codecs.lookup(spec.file_encoding)
    except LookupError:
        errors["file_encoding"] = (
            f"unknown encoding {spec.file_encoding!r} -- try utf-8, latin-1 "
            f"or cp1252")

    if errors:
        raise FeedValidationError(errors)


def derive_pattern(example_filename: str) -> str | None:
    """Turn `marginCalls_20260801.csv` into the regex feeds.yml wants.

    The filename pattern is the field most likely to be got wrong, and it
    fails silently when it is -- `find_pending` simply never matches. Deriving
    it from a real delivered filename removes that whole class of mistake; the
    caller can still edit the result.

    Returns None when the example holds no 8-digit date, because then there is
    nothing to anchor a business_date group to.
    """
    m = re.search(r"(?<!\d)(\d{8})(?!\d)", example_filename)
    if not m:
        return None
    head = re.escape(example_filename[:m.start()])
    tail = example_filename[m.end():]
    # A trailing _v<N> in the example is a version marker, not part of the name.
    tail = re.sub(r"^_v\d+", "", tail)
    return (head + "(?P<business_date>\\d{8})"
            + "(?:_v(?P<version>\\d+))?" + re.escape(tail))


def _inherited(spec: FeedSpec) -> dict[str, Any]:
    """What this feed's block would inherit if it declared nothing.

    `defaults:` overlaid with the feed's convention, from feeds.yml, over the
    dataclass-level fallbacks in OPTIONAL_WITH_DEFAULT.

    WITHOUT THIS, A CONVENTION IS DEFEATED BY THE FIRST CONSOLE EDIT. `_block`
    omits a key whose value matches the default; comparing against the
    hardcoded map alone, a feed inheriting `delimiter: "|"` from its convention
    would have `delimiter: "|"` written into its own block on the next save --
    pinning the value where the convention can no longer change it, in a diff
    that looks like someone meant it.

    Falls back to the hardcoded map if feeds.yml cannot be read or names no
    such convention. That direction is safe: it writes a key that could have
    been inherited, which is noise. The opposite -- assuming inheritance that
    is not there -- would DROP a key the feed needs.
    """
    from reporting_platform.common import context

    try:
        return {**OPTIONAL_WITH_DEFAULT,
                **context.effective_defaults(spec.convention or "")}
    except Exception:                                          # noqa: BLE001
        return dict(OPTIONAL_WITH_DEFAULT)


def _block(spec: FeedSpec) -> CommentedMap:
    """The YAML mapping for one feed, inherited values omitted."""
    block = CommentedMap()
    inherited = _inherited(spec)
    for key in BLOCK_ORDER:
        value = getattr(spec, key)
        # Omit ANY key whose value is exactly what the feed would inherit --
        # not just the OPTIONAL_WITH_DEFAULT subset. A convention may supply
        # `source_system` or `expected_min_rows` as readily as `delimiter`,
        # and the narrower rule wrote those back into every feed block on the
        # first save, pinning them where the convention could no longer change
        # them.
        #
        # Safe for the identity keys because `defaults:` cannot supply them:
        # `name`, `description`, `filename_pattern`, `business_key` and
        # `columns` are never in `inherited`, so they are always written.
        if key in inherited and value == inherited[key]:
            continue
        if key == "convention":
            # Omitted entirely when the feed stands alone, which is the normal
            # case and how every feed block looked before conventions existed.
            if not value:
                continue
            block[key] = value
        elif key == "filename_pattern":
            # Single-quoted so the regex backslashes stay literal and the block
            # keeps looking like the ones around it.
            block[key] = SQ(value)
        elif key == "column_types":
            # Omitted entirely when there is nothing to override, which is the
            # normal case -- an empty mapping in the diff would be noise.
            if not value:
                continue
            block[key] = CommentedMap(value)
        elif key == "columns":
            # Mixed list: a bare name where the file header is already usable,
            # `{name: source}` where it is not. Most columns need no mapping,
            # and a uniform mapping form would double the length of every feed
            # block to say nothing.
            # See docs/DECISIONS.md#source-column-names
            seq = CommentedSeq()
            for col in value:
                source = spec.source_columns.get(col)
                if source:
                    entry = CommentedMap({col: source})
                    entry.fa.set_flow_style()
                    seq.append(entry)
                else:
                    seq.append(col)
            block[key] = seq
        elif key == "business_key":
            seq = CommentedSeq(value)
            seq.fa.set_flow_style()          # [trade_id], as the others have
            block[key] = seq
        else:
            block[key] = value
    return block


def _position_for(block: CommentedMap, key: str) -> int:
    """Index at which `key` belongs, per BLOCK_ORDER.

    Keys the UI does not manage keep their relative position: this only looks
    for the first MANAGED key that should come after `key` and inserts before
    it, so a hand-added `arrival_timeout_hours` is not stepped over.
    """
    after = BLOCK_ORDER[BLOCK_ORDER.index(key) + 1:]
    for i, existing in enumerate(block.keys()):
        if existing in after:
            return i
    return len(block)


def read_raw() -> tuple[Any, YAML]:
    y = _yaml()
    with FEEDS_YML.open(encoding="utf-8") as fh:
        return y.load(fh), y


def _write(doc, y: YAML) -> None:
    """Serialise to a string first, then replace the file in one write.

    Not a straight `y.dump(doc, fh)`: that truncates feeds.yml before the
    emitter produces anything, so an emitter error would leave the single
    source of truth for the whole platform as a zero-byte file.
    """
    buf = io.StringIO()
    y.dump(doc, buf)
    text = buf.getvalue()
    if "feeds:" not in text:
        raise RuntimeError("refusing to write a feeds.yml with no `feeds:` key")
    # Exactly one trailing newline. `add` puts a blank line before the block it
    # appends, and ruamel attaches that to the PREVIOUS item's trailing
    # comment -- so removing the last feed leaves the blank line behind, and
    # deleting a feed you had just added did not restore the file. Harmless in
    # YAML terms and pure noise in a diff, which for a file whose diff is the
    # deliverable is the part that matters.
    text = text.rstrip() + "\n"
    FEEDS_YML.write_text(text, encoding="utf-8")


def add(spec: FeedSpec) -> None:
    doc, y = read_raw()
    doc["feeds"].append(_block(spec))
    # Blank line before the new block, matching how the hand-written ones are
    # separated. Cosmetic, but this file is read far more often than it is
    # written, and a console-added feed should not be identifiable by its
    # spacing.
    try:
        doc["feeds"].yaml_set_comment_before_after_key(
            len(doc["feeds"]) - 1, before="\n")
    except Exception:                                          # noqa: BLE001
        pass
    _write(doc, y)


def update(spec: FeedSpec) -> None:
    """Rewrite one feed's block in place, keeping its position and comments.

    Keys the UI does not manage are left untouched, so hand-tuning a feed in
    the file and then editing it in the UI does not quietly revert the tuning.
    """
    doc, y = read_raw()
    for block in doc["feeds"]:
        if block.get("name") != spec.name:
            continue
        new = _block(spec)
        for key in BLOCK_ORDER:
            if key in new:
                if key in block:
                    block[key] = new[key]
                else:
                    # A key set for the first time. Assigning it would append
                    # it after `columns`, ordering the block by when it was
                    # edited rather than the order docs/ADDING-A-FEED.md reads
                    # in. Insert it at its place instead.
                    block.insert(_position_for(block, key), key, new[key])
            elif key in block:
                # Fell back to the default: drop the override rather than
                # leaving a stale value behind.
                del block[key]
        _write(doc, y)
        return
    raise FeedValidationError({"name": f"no such feed: {spec.name!r}"})


def remove(name: str) -> None:
    doc, y = read_raw()
    kept = [b for b in doc["feeds"] if b.get("name") != name]
    if len(kept) == len(doc["feeds"]):
        raise FeedValidationError({"name": f"no such feed: {name!r}"})
    while len(doc["feeds"]):
        doc["feeds"].pop()
    for b in kept:
        doc["feeds"].append(b)
    _write(doc, y)


def spec_from_feed(fd: Feed) -> FeedSpec:
    return FeedSpec(
        name=fd.name, description=fd.description, source_system=fd.source_system,
        filename_pattern=fd.filename_pattern,
        delimiter=fd.delimiter, quote_char=fd.quote_char,
        header=fd.header, file_encoding=fd.file_encoding, business_key=list(fd.business_key),
        columns=list(fd.columns), expected_min_rows=fd.expected_min_rows,
        cadence=fd.cadence, completeness=fd.completeness,
        schema_drift=fd.schema_drift, convention=fd.convention,
        column_types=dict(fd.column_types or {}),
        source_columns=dict(fd.source_columns or {}),
    )
