"""Read and write the feed registry under reporting_platform/config/feeds/.

ROUND-TRIP, NOT RE-EMIT. A feed file is more comment than data -- the reasoning
for `cadence: weekly` on rating, for the per-feed filename pattern, for
`retention_class: operational` on collateral -- and a plain `yaml.safe_load` /
`yaml.safe_dump` cycle silently deletes all of it. ruamel's round-trip loader
preserves comments, key order and quoting style.

ONE FILE PER FEED, so adding one is a new file and removing one is an unlink.
That is most of what this module used to do by hand: appending into a shared
`feeds:` sequence needed a blank line inserted before the new block, and
ruamel attaches such a line to the PREVIOUS item's trailing comment -- so
deleting a feed you had just added left the blank line behind and did not
restore the file. There is no sequence to append to now, and that whole class
of problem went with it. See docs/DECISIONS.md#the-registry-is-a-directory
"""
from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from reporting_platform.common import layout
from reporting_platform.common.context import CONFIG_DIR, Feed

# Redirected by `tests/support.registry_on` at a throwaway tree, so every
# path below is derived from it rather than bound once at import.
CONFIG_ROOT = Path(CONFIG_DIR)


def feed_path(name: str) -> Path:
    return layout.feeds_root(CONFIG_ROOT) / f"{name}.yml"

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
# `raw_namespace` set by hand survives an edit through the UI.
BLOCK_ORDER = ["name", "description", "source_system", "convention",
               "filename_pattern", "arrival", "delivery",
               "delimiter", "quote_char", "header", "file_encoding",
               "business_key", "expected_min_rows", "cadence",
               "delivery_expected", "expected_by", "retention_class",
               "schema_drift", "columns", "column_types"]

# Keys only written when they differ from what the feed would INHERIT, because
# a block repeating an inherited value is noise in the diff. The four format
# keys are here rather than absent because a pipe-delimited or latin-1 feed is
# ordinary.
#
# THESE VALUES ARE THE FALLBACK, NOT THE ANSWER. What a feed actually inherits
# is `defaults:` overlaid with its convention, which only feeds.yml knows --
# see `_inherited()` below. This map supplies the keys with no entry in
# `defaults:` at all (`cadence`, `delivery_expected`) and covers the case where
# feeds.yml cannot be read.
OPTIONAL_WITH_DEFAULT = {"cadence": "daily", "delivery_expected": True,
                         "schema_drift": "warn", "delimiter": ",",
                         "quote_char": '"', "header": True,
                         "file_encoding": "utf-8", "expected_by": "",
                         "retention_class": "standard"}

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
    # "expected to deliver on every COB date" -- see Feed.delivery_expected
    # for why it is not called `completeness`.
    delivery_expected: bool = True
    schema_drift: str = "warn"
    # REQ-201. "HH:MM", or "" for a feed that has promised nothing. Usually
    # inherited from the convention -- one delivery arrangement, one deadline
    # -- so this is empty on most feeds and omitted from their blocks.
    expected_by: str = ""
    # REQ-600/601. The evidence obligation this feed's landing and quarantine
    # objects are under. A `<select>` rather than free text, populated from
    # what retention.yml declares, because an unrecognised class is refused at
    # LOAD and the form must not be able to write one.
    retention_class: str = "standard"
    # The `conventions:` entry this feed inherits from, or "" to stand alone.
    # Every key the convention supplies is then omitted from the feed's own
    # block, so the convention stays the single place that value is written.
    convention: str = ""
    # How a landed object becomes units of work -- absent/empty means
    # `kind: file`, one object, one delivery. Validated with the SAME function
    # feeds.yml load does (context.resolve_delivery_config), so a feed created
    # here fails in the form on exactly what would otherwise fail silently at
    # the next Airflow parse.
    # See docs/DECISIONS.md#archive-normalizer and #control-file-gate.
    delivery: dict[str, Any] = field(default_factory=dict)
    # How the delivery arrives in the INBOX when it is not already conformant
    # -- absent/empty means the upstream writes a correctly named file to
    # landing/ directly. Validated with the SAME function feeds.yml load does
    # (context.resolve_arrival_config).
    # See docs/DECISIONS.md#the-inbox-is-the-conformance-gate.
    arrival: dict[str, Any] = field(default_factory=dict)
    # How to READ the file. All four default to the `defaults:` block and are
    # written only when they differ -- see OPTIONAL_WITH_DEFAULT. They reach
    # Spark's reader unchanged, so a wrong delimiter lands one column holding
    # the whole row rather than failing.
    delimiter: str = ","
    quote_char: str = '"'
    header: bool = True
    file_encoding: str = "utf-8"
    # Sparse: ONLY the columns whose type disagrees with infer_type()'s guess.
    # The caller reduces it (scaffold.overrides_only) before handing it over,
    # so this module stays ignorant of how a type is guessed.
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
            delivery_expected=bool(payload.get("delivery_expected", True)),
            schema_drift=str(payload.get("schema_drift") or "warn").strip(),
            expected_by=str(payload.get("expected_by") or "").strip(),
            retention_class=str(
                payload.get("retention_class") or "standard").strip(),
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
            delivery=_delivery_from_payload(payload.get("delivery")),
            arrival=_arrival_from_payload(payload.get("arrival")),
        )


def _arrival_from_payload(raw: Any) -> dict[str, Any]:
    """The form's `arrival` object -> the sparse dict `resolve_arrival_config`
    expects, blank fields dropped.

    Same division of labour as `_delivery_from_payload`: this strips blanks
    and nothing else. Whether the block makes sense -- a date source that is
    named twice or not at all, a control pattern with no `{stem}` -- is
    `resolve_arrival_config`'s job in `validate()`, which is the same function
    feeds.yml load calls, so the form and the loader cannot disagree about
    which feeds are legal.
    """
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    source_pattern = str(raw.get("source_pattern") or "").strip()
    if source_pattern:
        out["source_pattern"] = source_pattern
    control = _control_from_payload(raw.get("control"),
                                    ("pattern", "cob_date", "version"))
    if control:
        out["control"] = control
    # An `arrival:` block with only a control block and no source_pattern is
    # meaningless -- nothing would match it -- and returning {} means a form
    # whose arrival fields are present but unused produces a feed with no
    # arrival block, rather than one that validates as broken.
    return out if source_pattern else {}


def _delivery_from_payload(raw: Any) -> dict[str, Any]:
    """The form's `delivery` object -> the sparse dict `resolve_delivery_config`
    expects, blank fields dropped.

    Deliberately permissive about SHAPE here -- an unknown `kind`, a bad
    `member_pattern` regex, `control` combined with `kind: archive` -- all of
    that is `resolve_delivery_config`'s job in `validate()` below, not this
    function's. This only strips blanks, so an empty `control: {pattern: "",
    row_count: ""}` sent by a form that has the control fields present but
    unused becomes `{}` and is genuinely absent, not a delivery block that
    validates as broken.
    """
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    kind = str(raw.get("kind") or "").strip()
    # "file" is the implicit default (resolve_delivery_config treats an absent
    # `kind` the same way) -- writing it explicitly would put
    # `delivery: {kind: file}` in every feed the form creates.
    if kind and kind != "file":
        out["kind"] = kind
    member_pattern = str(raw.get("member_pattern") or "").strip()
    if member_pattern:
        out["member_pattern"] = member_pattern
    cob_date_from = str(raw.get("cob_date_from") or "").strip()
    if cob_date_from:
        out["cob_date_from"] = cob_date_from
    parts = str(raw.get("parts") or "").strip()
    if parts:
        out["parts"] = parts
    control = _control_from_payload(raw.get("control"),
                                    ("pattern", "row_count", "md5"))
    if control:
        out["control"] = control
    return out


def _control_from_payload(raw: Any, keys: tuple[str, ...]) -> dict[str, Any]:
    """One `control` object -> the sparse dict the resolvers expect.

    Shared by both blocks: they take different fields but the same treatment,
    and `format` in particular has to survive BOTH round trips. A key the form
    does not send is a key the next save deletes -- the console rewrites the
    whole feed block from the payload -- so a delimited control file edited
    through the form would come back parsed as a regex, which is not a
    validation failure anywhere: the fields simply stop matching, at ingest,
    on a feed nobody changed.
    """
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for key in keys:
        value = str(raw.get(key) or "").strip()
        if value:
            out[key] = value
    # Added only to a block that exists, and only when it is not the default
    # -- otherwise a form whose format inputs always have a value would turn
    # every feed into one with a control block. What a format may SAY is
    # `context.resolve_control_format`'s to decide, in `validate()`, which is
    # the same function feeds.yml load calls.
    fmt = raw.get("format")
    if out and isinstance(fmt, dict) and str(fmt.get("kind") or "") == "delimited":
        out["format"] = _format_from_payload(fmt)
    return out


def _format_from_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """One `control.format` object -> the sparse dict `resolve_control_format`
    expects. Blanks dropped, nothing defaulted and nothing judged: an empty
    `delimiter` has to reach the resolver to be reported as the missing thing
    it is, rather than being quietly filled in with a comma here."""
    out: dict[str, Any] = {"kind": "delimited"}
    for key in ("delimiter", "quote_char"):
        value = str(raw.get(key) or "")
        if value:
            out[key] = value
    columns = [str(c).strip() for c in (raw.get("columns") or [])
               if str(c).strip()]
    # A headerless file is the only reason to list them, so the two travel
    # together -- the resolver rejects either one alone.
    if columns:
        out["header"] = False
        out["columns"] = columns
    return out


# ANY path ending `.yml: `, not the literal `feeds.yml: ` this matched when
# the registry was one file. Since the split a message opens with the feed's
# own path -- `feeds/fo_trade.yml: `, and the ABSOLUTE path for a feed that
# already exists, because `context.where()` reports the file it actually
# read. Left as it was, every form error carried a container path in front of
# it, and no test noticed: the assertions are on what the message SAYS, not
# on what was stripped from the front of it.
_FILE_PREFIX_RE = re.compile(r"^\S*\.yml: ")


def _form_message(exc: Exception) -> str:
    """A load-time error, addressed to somebody looking at a form field.

    The platform's messages name the FILE and the FEED because they are read
    out of an Airflow log, where neither is otherwise knowable. On the form the
    file is a given, so its name is stripped -- and only its name. The FEED
    name stays, because several of these messages continue with a verb whose
    subject it is ("feed 'x' names retention class 'gold', which..."), and
    removing it leaves a dangling sentence. This used to be
    `str(exc).split(": ", 2)[-1]`, which split on
    every colon in the message: an unknown retention class came back as
    "operational, standard. Add it under `retention_classes:` there before
    naming it here", with the half that said what was wrong removed. Verified
    against the running console before and after.
    """
    return _FILE_PREFIX_RE.sub("", str(exc)).strip()


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
        # already wrong, taking the platform down at import; this catches a
        # typo on its way IN, while it is still a message next to a form field.
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
            if "cob_date" not in compiled.groupindex:
                errors["filename_pattern"] = (
                    "must contain a named group (?P<cob_date>...) -- "
                    "this is the name the file has IN LANDING, and everything "
                    "downstream reads the COB date out of it. If the "
                    "UPSTREAM sends no date in the name, leave this as the "
                    "name you want and describe the real one under Arrival "
                    "below; the inbox renames it on the way in")

    if spec.arrival:
        # THE SAME FUNCTION feeds.yml load calls. `filename_pattern` is passed
        # because the gate's job is producing a name that pattern accepts, so a
        # landing pattern with no date to write into is an arrival error too,
        # and saying so here names which of the two patterns is wrong.
        from reporting_platform.common import context

        try:
            context.resolve_arrival_config(spec.name or "(unnamed)",
                                           spec.arrival, spec.filename_pattern)
        except ValueError as exc:
            errors["arrival"] = str(exc)

    if spec.delivery:
        # THE SAME FUNCTION feeds.yml load calls, not a second copy of the
        # rules -- an archive/control feed created here fails in the form on
        # exactly what would otherwise fail silently at the next Airflow parse
        # (an unknown kind falls through to the pass-through normalizer and
        # ingests a zip as one column of binary rubbish). `feed_name` in the
        # message is spec.name, which may still be invalid at this point.
        from reporting_platform.common import context

        try:
            context.resolve_delivery_config(spec.name or "(unnamed)", spec.delivery)
        except ValueError as exc:
            errors["delivery"] = str(exc)

    # The two control-file gates are COMPLEMENTARY -- one file, read at the
    # door for identity and on the landing side for integrity -- so what fails
    # here is a pair that does not add up: an `arrival.control` with no
    # `delivery.control` to read the promoted file, or two `format` blocks
    # disagreeing about the shape of it. See context.check_gates_are_coherent.
    # Checked after both blocks so the message names the pair rather than one
    # half of it.
    if "arrival" not in errors and "delivery" not in errors:
        from reporting_platform.common import context

        try:
            context.check_gates_are_coherent(
                spec.name or "(unnamed)",
                context.resolve_arrival_config(spec.name or "(unnamed)",
                                               spec.arrival),
                context.resolve_delivery_config(spec.name or "(unnamed)",
                                                spec.delivery))
        except ValueError as exc:
            errors["delivery"] = str(exc)

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

    # EVERY VALUE CHECK BELOW IS THE PLATFORM'S OWN FUNCTION, not a second
    # copy of the rules. A form that accepted `7am`, a class retention.yml
    # does not declare, or `cadence: fortnightly` would write a feed file the
    # next Airflow parse refuses to load -- and the console's whole point is
    # that its diff is one you can merge.
    #
    # `cadence`, `schema_drift`, `expected_min_rows`, `delimiter`,
    # `quote_char` and `file_encoding` used to be restated here, and the
    # loader checked NONE of them: the form refused what a hand edit or a
    # merge could still write, and `cadence: fortnightly` then behaved as
    # `daily` with nothing saying so. They moved to `common/context.py`
    # alongside the two that were already shared.
    from reporting_platform.common.context import (
        check_cadence, check_expected_min_rows, check_file_encoding,
        check_retention_class, check_schema_drift, check_single_char,
        parse_expected_by,
    )
    named = lambda key: lambda name, value: check_single_char(name, key, value)
    for field, check, value in (
            ("expected_by", parse_expected_by, spec.expected_by or ""),
            ("retention_class", check_retention_class, spec.retention_class),
            ("cadence", check_cadence, spec.cadence),
            ("schema_drift", check_schema_drift, spec.schema_drift),
            ("expected_min_rows", check_expected_min_rows,
             spec.expected_min_rows),
            ("delimiter", named("delimiter"), spec.delimiter),
            ("quote_char", named("quote_char"), spec.quote_char),
            ("file_encoding", check_file_encoding, spec.file_encoding)):
        try:
            check(spec.name or "this feed", value)
        except ValueError as exc:
            errors[field] = _form_message(exc)

    if errors:
        raise FeedValidationError(errors)


def derive_pattern(example_filename: str) -> str | None:
    """Turn `marginCalls_20260801.csv` into the regex feeds.yml wants.

    The filename pattern is the field most likely to be got wrong, and it
    fails silently when it is -- `find_pending` simply never matches. Deriving
    it from a real delivered filename removes that whole class of mistake; the
    caller can still edit the result.

    Returns None when the example holds no 8-digit date, because then there is
    nothing to anchor a cob_date group to.
    """
    m = re.search(r"(?<!\d)(\d{8})(?!\d)", example_filename)
    if not m:
        return None
    head = re.escape(example_filename[:m.start()])
    tail = example_filename[m.end():]
    # A trailing _v<N> in the example is a version marker, not part of the name.
    tail = re.sub(r"^_v\d+", "", tail)
    return (head + "(?P<cob_date>\\d{8})"
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


def _arrival_block(value: dict[str, Any]) -> CommentedMap:
    """`spec.arrival` -> the nested YAML mapping.

    `source_pattern` is a regex, so it is single-quoted like
    `filename_pattern` and for the same reason: a double-quoted
    `'{stem}\\.ctl'` would have YAML eat the backslash, and the pattern would
    then match a literal dot only by accident. The control block is
    `_control_block`'s, shared with `delivery:` so one control file cannot be
    written down two ways.
    """
    av = CommentedMap()
    if "source_pattern" in value:
        av["source_pattern"] = SQ(value["source_pattern"])
    control = value.get("control")
    if isinstance(control, dict) and control:
        av["control"] = _control_block(control, ("cob_date", "version"))
    return av


def _control_block(control: dict[str, Any], fields: tuple[str, ...]) -> CommentedMap:
    """One `control` block -> the nested YAML mapping, in a fixed order:
    pattern first because it is what finds the file, then how the file is
    read, then what is read out of it.

    Single-quoted for the same reason `filename_pattern` is: under
    `kind: regex` these are regexes, and a double-quoted `'{stem}\\.ctl'`
    would have YAML eat the backslash so the pattern matched a literal dot
    only by accident. Under `kind: delimited` they are column names, where the
    quoting is merely harmless.
    """
    cv = CommentedMap()
    if "pattern" in control:
        cv["pattern"] = SQ(control["pattern"])
    fmt = control.get("format")
    if isinstance(fmt, dict) and fmt:
        fv = CommentedMap()
        # A RESOLVED format arrives here on any edit -- `spec_from_feed`
        # hands back what the loader filled in -- so the two defaults are
        # dropped again on the way out, the same rule `_block` follows for
        # inherited values. `header: false` is never a default and always
        # travels, because `columns` alone is rejected without it.
        fv["kind"] = fmt.get("kind", "delimited")
        if fmt.get("delimiter"):
            fv["delimiter"] = SQ(fmt["delimiter"])
        if fmt.get("quote_char", '"') != '"':
            fv["quote_char"] = SQ(fmt["quote_char"])
        if fmt.get("header", True) is False:
            fv["header"] = False
        if fmt.get("columns"):
            fv["columns"] = CommentedSeq(fmt["columns"])
        cv["format"] = fv
    for key in fields:
        if key in control:
            cv[key] = SQ(control[key])
    return cv


def _delivery_block(value: dict[str, Any]) -> CommentedMap:
    """`spec.delivery` -> the nested YAML mapping, in the order
    docs/DELIVERY-SHAPES.md's own examples use: kind, member_pattern,
    cob_date_from, parts, control.

    `member_pattern` is a regex, single-quoted like `filename_pattern` so its
    backslashes stay literal; the control block is `_control_block`'s.
    """
    dv = CommentedMap()
    if "kind" in value:
        dv["kind"] = value["kind"]
    if "member_pattern" in value:
        dv["member_pattern"] = SQ(value["member_pattern"])
    if "cob_date_from" in value:
        dv["cob_date_from"] = value["cob_date_from"]
    if "parts" in value:
        dv["parts"] = value["parts"]
    control = value.get("control")
    if isinstance(control, dict) and control:
        dv["control"] = _control_block(control, ("row_count", "md5"))
    return dv


def _block(spec: FeedSpec) -> CommentedMap:
    """The YAML mapping for one feed, inherited values omitted."""
    block = CommentedMap()
    inherited = _inherited(spec)
    for key in BLOCK_ORDER:
        value = getattr(spec, key)
        # Omit ANY key whose value is exactly what the feed would inherit --
        # not just the OPTIONAL_WITH_DEFAULT subset. A convention may supply
        # `source_system` or `expected_min_rows` as readily as `delimiter`, and
        # the narrower rule wrote those into every feed block on the first
        # save, pinning them where the convention could no longer change them.
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
        elif key == "arrival":
            # Omitted entirely for a conformant upstream, which is the
            # ordinary case and how every feed block looks today.
            if not value:
                continue
            block[key] = _arrival_block(value)
        elif key == "delivery":
            # Omitted entirely for `kind: file` with no control block -- the
            # ordinary case, and how the four original feeds' blocks have
            # always looked. See docs/DELIVERY-SHAPES.md for the shape.
            if not value:
                continue
            block[key] = _delivery_block(value)
        elif key == "column_types":
            # Omitted entirely when there is nothing to override, which is the
            # normal case -- an empty mapping in the diff would be noise.
            if not value:
                continue
            block[key] = CommentedMap(value)
        elif key == "columns":
            # Mixed list: a bare name where the file header is already usable,
            # `{name: source}` where it is not. Most columns need no mapping,
            # and a uniform mapping form would double every feed block's length
            # to say nothing. See docs/DECISIONS.md#source-column-names
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
    it, so a hand-added `raw_namespace` is not stepped over.
    """
    after = BLOCK_ORDER[BLOCK_ORDER.index(key) + 1:]
    for i, existing in enumerate(block.keys()):
        if existing in after:
            return i
    return len(block)


def read_raw(name: str) -> tuple[Any, YAML]:
    """One feed's file, round-tripped. Raises if there is no such feed."""
    y = _yaml()
    path = feed_path(name)
    if not path.is_file():
        raise FeedValidationError({"name": f"no such feed: {name!r}"})
    with path.open(encoding="utf-8") as fh:
        return y.load(fh), y


def _write(block, y: YAML, path: Path) -> None:
    """Serialise to a string first, then replace the file in one write.

    Not a straight `y.dump(block, fh)`: that truncates the file before the
    emitter produces anything, so an emitter error would leave a feed's
    definition as a zero-byte file -- which, unlike a missing one, still
    loads, as a feed with no keys at all.

    The `name:` check is the successor to the old "refusing to write a
    feeds.yml with no `feeds:` key": the one thing the file may never lose is
    the identity that has to match its filename.
    """
    buf = io.StringIO()
    y.dump(block, buf)
    text = buf.getvalue()
    if "name:" not in text:
        raise RuntimeError(
            f"refusing to write {path.name} with no `name:` key")
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def add(spec: FeedSpec) -> None:
    path = feed_path(spec.name)
    if path.exists():
        raise FeedValidationError({"name": f"feed exists: {spec.name!r}"})
    path.parent.mkdir(parents=True, exist_ok=True)
    _write(_block(spec), _yaml(), path)


def update(spec: FeedSpec) -> None:
    """Rewrite one feed's block in place, keeping its position and comments.

    Keys the UI does not manage are left untouched, so hand-tuning a feed in
    the file and then editing it in the UI does not quietly revert the tuning.
    """
    block, y = read_raw(spec.name)
    new = _block(spec)
    for key in BLOCK_ORDER:
        if key in new:
            if key in block:
                block[key] = new[key]
            else:
                # A key set for the first time. Assigning it would append it
                # after `columns`, ordering the block by when it was edited
                # rather than the order docs/ADDING-A-FEED.md reads in.
                # Insert it at its place instead.
                block.insert(_position_for(block, key), key, new[key])
        elif key in block:
            # Fell back to the default: drop the override rather than
            # leaving a stale value behind.
            del block[key]
    _write(block, y, feed_path(spec.name))


def remove(name: str) -> None:
    path = feed_path(name)
    if not path.is_file():
        raise FeedValidationError({"name": f"no such feed: {name!r}"})
    path.unlink()


def spec_from_feed(fd: Feed) -> FeedSpec:
    return FeedSpec(
        name=fd.name, description=fd.description, source_system=fd.source_system,
        filename_pattern=fd.filename_pattern,
        delimiter=fd.delimiter, quote_char=fd.quote_char,
        header=fd.header, file_encoding=fd.file_encoding, business_key=list(fd.business_key),
        columns=list(fd.columns), expected_min_rows=fd.expected_min_rows,
        cadence=fd.cadence, delivery_expected=fd.delivery_expected,
        schema_drift=fd.schema_drift, convention=fd.convention,
        expected_by=fd.expected_by, retention_class=fd.retention_class,
        column_types=dict(fd.column_types or {}),
        source_columns=dict(fd.source_columns or {}),
        delivery=dict(fd.delivery or {}),
        arrival=dict(fd.arrival or {}),
    )
