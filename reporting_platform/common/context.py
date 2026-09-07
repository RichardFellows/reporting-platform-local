"""Shared platform context: config loading, Spark session, Nessie helpers.

Everything environment-varying is read from config + env here, so no DAG or
job module contains an endpoint, a credential or a policy value.
"""
from __future__ import annotations

import hashlib
import os
import re
import uuid
from urllib.parse import quote as _urlquote
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(os.environ.get("REPORTING_CONFIG_DIR", "/opt/platform/reporting_platform/config"))
CATALOG = os.environ.get("REPORTING_CATALOG", "lakehouse")
ENV = os.environ.get("REPORTING_ENV", "local")


# --------------------------------------------------------------------- config
@lru_cache(maxsize=None)
def _load_at(name: str, mtime_ns: int) -> dict[str, Any]:
    """Parse a config file. Cached on (name, mtime) rather than name alone."""
    with open(CONFIG_DIR / name) as fh:
        return yaml.safe_load(fh)


def _load(name: str) -> dict[str, Any]:
    """Load a config file, re-reading it if it has changed on disk.

    THE MTIME IS PART OF THE CACHE KEY ON PURPOSE. This was a plain
    `@lru_cache` on the name, which is correct for a process whose config
    cannot change under it -- but `reporting_platform/ui` edits feeds.yml
    while the platform is running, and every long-lived process that had
    already called `feeds()` would then hold the pre-edit registry until it
    was restarted. That includes Airflow's DAG file processor, which reuses
    its worker processes across parses: a new feed would be written, the DAG
    file re-parsed, and no `ingest_<feed>` DAG would appear, with nothing
    anywhere reporting an error.

    Re-keying on mtime keeps the caching (a hot path still parses no YAML)
    and makes a config edit visible everywhere within one file-process
    interval. `st_mtime_ns` rather than `st_mtime`: two edits inside the same
    filesystem timestamp tick are entirely possible from a web form, and a
    coarser key would miss the second one.
    """
    return _load_at(name, (CONFIG_DIR / name).stat().st_mtime_ns)


@dataclass(frozen=True)
class Feed:
    name: str
    description: str
    source_system: str
    filename_pattern: str
    business_key: list[str]
    columns: list[str]
    expected_min_rows: int = 0
    landing_prefix: str = "landing"
    # Where normalization puts a delivery's manifest and any derived parts.
    # SEPARATE FROM `landing_prefix` ON PURPOSE, and not a subfolder of it:
    # `retention/landing.py` walks the landing prefix and never deletes an
    # object whose name it cannot parse, because that prefix is the evidence
    # copy. Manifests match no filename_pattern, so under landing/ they would
    # count as `unrecognised` on every nightly sweep and accumulate forever.
    # Kept apart, `list_landing` and the landing sweep are both prefix-scoped
    # and simply never see them -- no exclusion filter to forget.
    # See docs/DELIVERY-SHAPES.md.
    ready_prefix: str = "ready"
    raw_namespace: str = "raw"
    delimiter: str = ","
    quote_char: str = '"'
    header: bool = True
    file_encoding: str = "utf-8"
    schema_drift: str = "warn"
    # Per-column prepared-layer treatment, e.g. {"haircut_pct": "decimal"}.
    #
    # OPTIONAL AND SPARSE: only columns whose treatment differs from what
    # ui/scaffold.infer_type() guesses from the name are recorded, so an
    # existing feed that never needed an override has no entry here and no
    # diff. Everything else falls back to that inference.
    #
    # It exists because the guess and the human disagreed and the human's
    # answer was being thrown away. The feed console let you set a type, used
    # it once to scaffold the prepared model, and then discarded it -- the API
    # re-inferred from the column name on every read. So a column typed
    # `decimal` in the form got `safe_cast(..., DECIMAL(18,2))` in the model
    # while the sample-data generator, re-inferring `string`, produced values
    # that could not cast. The column published as 100% NULL and the build
    # went green, because safe_cast is *meant* to land NULL and no test
    # covered it. Verified end to end: 75 rows, 0 non-null.
    #
    # Raw is still all strings -- this does not type the raw table. It records
    # what the PREPARED model should do with the column, which is the one
    # thing the scaffold and the generator both need to agree on.
    column_types: dict[str, str] = field(default_factory=dict)
    # Platform column name -> the name that column has IN THE FILE, for the
    # ones that differ. Sparse, like column_types: a feed whose headers are
    # already usable identifiers has none of these and no diff.
    #
    # Real deliveries do not arrive with snake_case headers. `Trade Id`,
    # `Cpty Ref`, `Notional (USD)` are ordinary, and a name with a space in it
    # poisons everything downstream of raw: dbt macros interpolate column
    # names into SQL, and `PARTITION BY Trade Id` is a syntax error rather than
    # a quoting inconvenience. Renaming at INGEST rather than in every model
    # means the awkward name exists in exactly one place -- the file, and this
    # mapping -- and raw onwards is ordinary identifiers.
    #
    # Raw stays 1:1 with the delivery in the way that matters: same rows, same
    # values, same order, everything a string. Only the identifiers are
    # normalised. See docs/DECISIONS.md#source-column-names
    source_columns: dict[str, str] = field(default_factory=dict)
    # Whether this feed is expected to deliver on every business date. False
    # opts it out of the gap check in monitoring/completeness.py, which infers
    # the business calendar from what other feeds delivered -- a feed that does
    # not deliver daily would otherwise show every non-delivery day as a gap.
    #
    # NAMED FOR THE QUESTION IT ANSWERS, not for the check that reads it. It
    # was `completeness` until "completeness" acquired a second, per-DELIVERY
    # meaning -- is this delivery whole, and by what evidence (a declared row
    # count, a checksum) -- which is a different question about a different
    # subject. Two meanings under one key, in the file every team edits, is
    # how one of them gets set to answer the other.
    delivery_expected: bool = True
    # How often the feed is expected to deliver: "daily" (a business date is
    # expected whenever another feed delivered on it) or "weekly" (only that
    # each week containing business dates saw at least one delivery).
    cadence: str = "daily"
    # REQ-201. The time of day, in the platform's timezone, by which a
    # delivery for a business date is expected to have ARRIVED. Empty means no
    # expectation is declared and lateness is not asserted for this feed.
    #
    # THE ONE LATENESS CONCEPT, and it is deliberately a wall-clock time
    # rather than a duration. `arrival_timeout_hours` and
    # `arrival_poke_seconds` were both deleted in phase 0 for being config
    # nothing read; the second of those had already been mistaken for a
    # mechanism once. What an upstream actually commits to is "by 07:00", not
    # "within six hours of something", and a duration needs an origin event
    # that a feed arriving by PutObject does not have.
    #
    # It says nothing about whether the delivery is WHOLE -- that is
    # `delivery.control`'s row count and md5 -- and nothing about whether one
    # is expected at all, which is `delivery_expected`. Three questions, three
    # keys, because the last time two of them shared one the answer to one was
    # being used for the other.
    #
    # Validated at load by `parse_expected_by`: "HH:MM", 24-hour.
    expected_by: str = ""
    # The `conventions:` entry this feed drew its defaults from, or "" for a
    # feed that stands alone. Recorded rather than discarded so the console can
    # round-trip it and so a resolved Feed can say where a surprising value
    # came from -- a feed whose delimiter is "|" with no `delimiter` key in its
    # own block is otherwise unexplainable from feeds.yml alone.
    convention: str = ""
    # How a landed object becomes units of work. Absent means `kind: file` --
    # one object, one delivery, date from the filename -- which is every feed
    # that existed before archives did and stays the default forever.
    # Validated at load by `resolve_delivery_config`.
    # See docs/DECISIONS.md#ready-is-a-derived-index and docs/DELIVERY-SHAPES.md
    delivery: dict[str, Any] = field(default_factory=dict)
    # How this feed's delivery arrives in the INBOX, when it does not already
    # arrive conformant. Absent means the upstream sends a correctly named
    # file straight into `landing/` -- which is every feed here today, so an
    # absent block is the default forever.
    #
    # `landing/` HAS A CONTRACT: everything in it is correctly named and
    # classified, so `parse_filename` answers for every object and landing
    # retention can date every object. A legacy upstream that sends
    # `positions.csv` with the date inside a control file does not satisfy
    # that contract, and the answer is to make it conformant AT THE DOOR
    # rather than to teach the whole platform a second shape. This block is
    # what the door needs: how to recognise the delivery, where its control
    # file is, and what the control file declares.
    # Validated at load by `resolve_arrival_config`.
    # See docs/DECISIONS.md#the-inbox-is-the-conformance-gate
    arrival: dict[str, Any] = field(default_factory=dict)
    # How a LATER delivery relates to an earlier one for the same business
    # date. Absent means `mode: full_snapshot` -- each delivery restates the
    # whole population, newest version wins -- which is what every feed here
    # does and what `dedupe_rank` has always implemented.
    #
    # Written down rather than assumed because the assumption is invisible
    # when it is wrong: a delta feed deduped as a snapshot loses every key the
    # newest file does not mention, with nothing raising anywhere.
    # Validated at load by `resolve_supersession_config`.
    supersession: dict[str, Any] = field(default_factory=dict)
    # REQ-600/601. Which retention class this feed's EVIDENCE belongs to --
    # the `landing/` and `quarantine/` prefixes. Named here and defined in
    # retention.yml; validated at load against the classes declared there, so
    # a typo is an error naming itself rather than a silent fall back to the
    # default window.
    #
    # THE NAME LIVES HERE AND THE WINDOWS LIVE THERE, and the split is the
    # point. Listing feed names inside retention.yml would create a second
    # registry of feeds that drifts from this one -- the failure
    # `context.reports()` avoids by deriving reports from the exposures
    # instead of restating them. Conversely, putting the YEARS here would put
    # a per-environment policy value in the file every feed team edits.
    #
    # INHERITABLE THROUGH `conventions:`, and that is where it usually
    # belongs: a retention obligation is a property of the source system and
    # the agreement behind it far more often than of one feed.
    #
    # It governs the evidence prefixes ONLY. The raw/prepared/reporting
    # keep-sets stay per LAYER: a table window says how much history is
    # queryable, which is a decision about a layer, and making raw's per feed
    # would put it in tension with `find_pending`, which derives one keep-set
    # per feed from that feed's landing prefix.
    # See docs/DECISIONS.md#retention-classes-name-the-obligation
    retention_class: str = "standard"

    @property
    def needs_conforming(self) -> bool:
        """Whether this feed's deliveries reach landing via the inbox gate."""
        return bool(self.arrival)

    @property
    def supersession_mode(self) -> str:
        """How a later delivery relates to an earlier one. See `supersession`.

        Resolved at load, so this never has to re-apply the default; an empty
        block would mean the feed was constructed by hand rather than through
        `feeds()`, and `full_snapshot` is the right answer there too.
        """
        return (self.supersession or {}).get("mode", "full_snapshot")

    def claims_source(self, filename: str) -> bool:
        """Whether `filename` is this feed's delivery AS THE UPSTREAM SENDS IT.

        Distinct from `parse_filename`, which asks the same question of a name
        that has already been made conformant. A legacy feed's two names are
        genuinely different strings -- `positions.csv` in the inbox,
        `trs_position_20260801.csv` in landing -- and conflating them is how
        the inbox would start rejecting the files it exists to accept.

        A feed with no `arrival:` block claims nothing at the door: its
        upstream writes to `landing/` directly.
        """
        pattern = (self.arrival or {}).get("source_pattern")
        return bool(pattern) and re.fullmatch(pattern, filename) is not None

    def source_control_pattern(self) -> str | None:
        return ((self.arrival or {}).get("control") or {}).get("pattern")

    def source_business_date(self, filename: str) -> date | None:
        """The business date the SOURCE filename carries, if it carries one.

        Some legacy names are wrong without being dateless -- `POS_20260801.TXT`
        for a feed whose landing convention is `trs_position_20260801.csv`.
        Those need renaming but no control file to date them, so the gate reads
        the date here and never opens one.
        """
        pattern = (self.arrival or {}).get("source_pattern")
        if not pattern:
            return None
        m = re.fullmatch(pattern, filename)
        if not m or "business_date" not in m.groupdict():
            return None
        return datetime.strptime(m.group("business_date"), "%Y%m%d").date()

    def source_column(self, name: str) -> str:
        """The name this platform column has in the delivered file."""
        return self.source_columns.get(name, name)

    @property
    def file_header(self) -> list[str]:
        """Column names as the FILE carries them, in declared order.

        What drift is measured against, what the sample-data generator writes,
        and what an uploaded header is compared to.
        """
        return [self.source_column(c) for c in self.columns]

    @property
    def schema_version(self) -> str:
        """A short digest of this feed's DECLARED COLUMN CONTRACT.

        What REQ-303 needs to tell "the upstream did not supply this column"
        apart from "the upstream supplied it empty". `_extra_columns` and
        `schema_drift` already record the drift itself; what was missing was a
        name for the contract the drift was measured AGAINST. Stamped on every
        raw row and every registry row, so a value read years later can be
        traced to the column list that was in force when it landed.

        DERIVED, NOT DECLARED. There is no `schema_version:` key in feeds.yml
        and there should not be: a version somebody has to remember to bump is
        a version that is wrong the first time somebody forgets, and the thing
        it describes -- the ordered list of columns and the names they have in
        the file -- is right there to be hashed. Adding, removing, renaming or
        reordering a column changes it; nothing else does.

        `column_types` is deliberately NOT in the digest. It says what the
        PREPARED model should do with a column, not what the file contains, so
        retyping a column in the console would otherwise look like the
        upstream having changed its schema. Nor is `delimiter`/`quote_char`/
        `header`: the manifest already records the format each delivery was
        actually read with, per delivery, which is the stronger statement.

        Twelve hex characters. Long enough that a collision is not a practical
        concern across the number of column lists an estate has, short enough
        to read in a table.
        """
        material = "\n".join(f"{c}\t{self.source_column(c)}"
                             for c in self.columns)
        return hashlib.sha1(material.encode("utf-8")).hexdigest()[:12]

    @property
    def raw_table(self) -> str:
        return f"{CATALOG}.{self.raw_namespace}.{self.name}"

    @property
    def asset_uri(self) -> str:
        """Airflow asset URI emitted when this feed's raw table is updated."""
        return f"iceberg://{CATALOG}/{self.raw_namespace}/{self.name}"

    def parse_filename(self, filename: str) -> tuple[date, int] | None:
        """Return (business_date, version) or None if the name does not match."""
        m = re.fullmatch(self.filename_pattern, filename)
        if not m:
            return None
        bd = datetime.strptime(m.group("business_date"), "%Y%m%d").date()
        raw_version = m.groupdict().get("version")
        return bd, int(raw_version) if raw_version else 1


def split_columns(declared: list) -> tuple[list[str], dict[str, str]]:
    """`columns:` entries -> (platform names, {platform: source} for the odd ones).

    Each entry is either a bare string, when the file's header is already a
    usable identifier, or a single-key mapping `{trade_id: "Trade Id"}` when it
    is not. Both forms in one list, because most columns need no mapping and a
    uniform mapping form would make every feed block twice as long to say
    nothing.
    """
    names: list[str] = []
    sources: dict[str, str] = {}
    for item in declared:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and len(item) == 1:
            name, source = next(iter(item.items()))
            names.append(str(name))
            if source is not None and str(source) != str(name):
                sources[str(name)] = str(source)
        else:
            raise ValueError(
                f"unusable `columns` entry {item!r}: expected a name, or a "
                f"single-key mapping of platform name to source name")
    return names, sources


# ------------------------------------------------------------------ delivery
# What `delivery:` may say. Every value here is DISPATCHED ON by
# ingest/normalize.py -- there is no key in this table that nothing reads,
# which is the failure this repo keeps having (`schema_drift` was documented
# and read by nothing for months, so `fail` silently meant `warn`).
DELIVERY_KINDS = ("file", "archive")
BUSINESS_DATE_FROM = ("container",)
PARTS_MODES = ("concat",)
DELIVERY_KEYS = {"kind", "member_pattern", "business_date_from", "parts", "control"}
CONTROL_KEYS = {"pattern", "row_count", "md5"}

# Values named in docs/DELIVERY-SHAPES.md that are NOT built yet. Listed so the
# error can say "not built" rather than "unknown", which are different
# problems with different fixes -- one is a typo, the other is a missing
# feature and a decision about whether to write it.
NOT_BUILT = {
    "business_date_from": {
        "member": "the date is on each member rather than the container, so "
                  "the container name need not match filename_pattern at all "
                  "-- which `matching()` and landing retention both rely on",
        "path": "the date is a folder in the key, which needs a pattern over "
                "the whole key rather than the filename",
    },
    "parts": {
        "separate": "one manifest per member instead of one with N parts; "
                    "normalize() would have to return a list",
    },
}


def resolve_delivery_config(feed_name: str, delivery: Any) -> dict[str, Any]:
    """Validate a `delivery:` block and fill its defaults.

    Checked at LOAD, like `conventions:`, and for the same reason: every way
    this can be wrong is otherwise silent. A misspelled `kind` would fall
    through to the pass-through normalizer and ingest a zip as if it were a
    CSV -- which does not fail, it lands one column of binary rubbish.
    """
    if not delivery:
        return {"kind": "file"}
    if not isinstance(delivery, dict):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery:` must be a mapping, got "
            f"{type(delivery).__name__}")

    unknown = set(delivery) - DELIVERY_KEYS
    if unknown:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery:` has unknown key(s) "
            f"{', '.join(sorted(unknown))}. Valid: "
            f"{', '.join(sorted(DELIVERY_KEYS))}")

    out = {"kind": delivery.get("kind", "file")}
    for key, allowed in (("kind", DELIVERY_KINDS),
                         ("business_date_from", BUSINESS_DATE_FROM),
                         ("parts", PARTS_MODES)):
        value = delivery.get(key)
        if value is None:
            continue
        if value in NOT_BUILT.get(key, {}):
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.{key}: {value}` is "
                f"described in docs/DELIVERY-SHAPES.md but NOT BUILT -- "
                f"{NOT_BUILT[key][value]}")
        if value not in allowed:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.{key}: {value!r}` is "
                f"not recognised. Valid: {', '.join(allowed)}")
        out[key] = value

    if out["kind"] == "archive":
        out.setdefault("business_date_from", "container")
        out.setdefault("parts", "concat")
        # No default: which members belong to this feed is not guessable, and
        # a wrong guess silently ingests the wrong files.
        if not delivery.get("member_pattern"):
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} is `kind: archive` and sets no "
                f"`member_pattern`. Which members belong to this feed is not "
                f"guessable -- a zip routinely carries a manifest, a checksum "
                f"or another feed's file alongside the data.")
        out["member_pattern"] = delivery["member_pattern"]
        try:
            re.compile(out["member_pattern"])
        except re.error as exc:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.member_pattern` is "
                f"not a valid regex: {exc}") from exc
    elif delivery.get("member_pattern"):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} sets `member_pattern` with "
            f"`kind: {out['kind']}`. It is only read for archives, so leaving "
            f"it here would suggest a filter that never runs.")

    control = delivery.get("control")
    if control is not None:
        if out["kind"] != "file":
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} sets `delivery.control` with "
                f"`kind: {out['kind']}`. Gating an archive on a control file "
                f"is described in docs/DELIVERY-SHAPES.md but NOT BUILT -- "
                f"only `kind: file` reads `control:`.")
        out["control"] = _resolve_control(feed_name, control)
    return out


def _resolve_control(feed_name: str, control: Any) -> dict[str, str]:
    """Validate a `delivery.control` block and fill its defaults.

    Same reasoning as the rest of `delivery:`: every key here is read by
    `ingest/normalize.py`, so a typo must fail at load rather than silently
    never gating anything.
    """
    if not isinstance(control, dict):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery.control` must be a "
            f"mapping, got {type(control).__name__}")

    unknown = set(control) - CONTROL_KEYS
    if unknown:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery.control` has unknown "
            f"key(s) {', '.join(sorted(unknown))}. Valid: "
            f"{', '.join(sorted(CONTROL_KEYS))}")

    pattern = control.get("pattern")
    if not pattern or not isinstance(pattern, str):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery.control` sets no "
            f"`pattern`. Which control file belongs to a delivery is not "
            f"guessable.")
    if "{stem}" not in pattern:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery.control.pattern` "
            f"{pattern!r} does not reference `{{stem}}` -- without it every "
            f"delivery for this feed would look for the same control filename.")
    try:
        re.compile(pattern.format(stem="X"))
    except re.error as exc:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `delivery.control.pattern` is not "
            f"a valid regex once `{{stem}}` is filled in: {exc}") from exc

    out = {"pattern": pattern}
    # The INTEGRITY checks, both optional -- a control file may be a pure
    # readiness gate. Read on the landing side and checked at ingest, so they
    # run once for every delivery however it arrived: one an approved sender
    # wrote straight into the bucket, and one the inbox renamed and promoted.
    # See docs/DECISIONS.md#the-inbox-is-the-conformance-gate
    for key, group in (("row_count", "rows"), ("md5", "md5")):
        value = control.get(key)
        if value is None:
            continue
        try:
            compiled = re.compile(value)
        except re.error as exc:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.control.{key}` "
                f"is not a valid regex: {exc}") from exc
        if group not in compiled.groupindex:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.control.{key}` "
                f"{value!r} has no `(?P<{group}>...)` group -- that is the "
                f"only thing normalize reads out of a match.")
        out[key] = value
    return out


# -------------------------------------------------------------- supersession
# What `supersession:` may say: HOW A LATER DELIVERY RELATES TO AN EARLIER ONE
# for the same business date. Every feed here today restates its whole
# population on every delivery, and `dedupe_rank` has always assumed exactly
# that -- newest `_file_version` wins, last row in file order wins within it.
#
# THE ASSUMPTION WAS TRUE AND UNDECLARED, which is the problem. A feed whose
# second delivery is a DELTA rather than a restatement would be ingested
# without complaint and then silently reduced to that delta alone: every key
# not present in the newest file simply stops existing in `prepared`, for the
# dates it covers, with no error anywhere. Declaring the mode does not make
# the other shapes work -- it makes the platform REFUSE a feed whose
# supersession it cannot implement, which is the whole of the value here.
#
# Same rule as `delivery:` and `arrival:`: one key, and it is dispatched on.
SUPERSESSION_KEYS = {"mode"}
SUPERSESSION_MODES = ("full_snapshot",)

# Modes named in the requirements (REQ-202) that are NOT built. Listed so the
# error says "not built" rather than "unknown" -- a typo and a missing feature
# are different problems with different fixes.
SUPERSESSION_NOT_BUILT = {
    "delta_append": (
        "each delivery carries only what changed, so a business date's "
        "population is the UNION of its deliveries rather than the newest one "
        "-- `dedupe_rank` would have to rank across versions instead of "
        "selecting the newest, and a deletion would need a tombstone "
        "convention the feed does not have"),
    "correction": (
        "a delivery restates individual keys of an EARLIER business date, so "
        "supersession crosses the partition `dedupe_rank` ranks within and "
        "the corrected date has to be rebuilt rather than the delivered one"),
}


def resolve_supersession_config(feed_name: str, supersession: Any) -> dict[str, Any]:
    """Validate a `supersession:` block and fill its default.

    Checked at LOAD for the same reason `delivery:` is: the failure is
    otherwise silent, and here it is silent in the direction that loses rows
    rather than the one that loses a file.

    An absent block means `full_snapshot`, which is what every feed has always
    been and what `dedupe_rank` implements. That default is not a guess: it is
    the behaviour already in force, written down.
    """
    if not supersession:
        return {"mode": "full_snapshot"}
    if not isinstance(supersession, dict):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `supersession:` must be a mapping, "
            f"got {type(supersession).__name__}")

    unknown = set(supersession) - SUPERSESSION_KEYS
    if unknown:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `supersession:` has unknown key(s) "
            f"{', '.join(sorted(unknown))}. Valid: "
            f"{', '.join(sorted(SUPERSESSION_KEYS))}")

    mode = supersession.get("mode", "full_snapshot")
    if mode in SUPERSESSION_NOT_BUILT:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `supersession.mode: {mode}` is "
            f"described in the requirements (REQ-202) but NOT BUILT -- "
            f"{SUPERSESSION_NOT_BUILT[mode]}. Every prepared model resolves "
            f"supersession with `dedupe_rank`, which implements "
            f"`full_snapshot` only.")
    if mode not in SUPERSESSION_MODES:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `supersession.mode: {mode!r}` is "
            f"not recognised. Valid: {', '.join(SUPERSESSION_MODES)} "
            f"(not built: {', '.join(sorted(SUPERSESSION_NOT_BUILT))})")
    return {"mode": mode}


# ------------------------------------------------------------- expected_by
_EXPECTED_BY_RE = re.compile(r"^(?P<h>[01]\d|2[0-3]):(?P<m>[0-5]\d)$")


def parse_expected_by(feed_name: str, value: Any) -> str:
    """Validate `expected_by:` and return it normalised, or "" if absent.

    REQ-201. A WALL-CLOCK TIME, "HH:MM", 24-hour, in the platform's timezone.

    Refused at LOAD rather than at the check, because a lateness expectation
    nothing can parse is worse than none at all: `completeness.py` would skip
    the feed, the check would go green, and the green would be read as "it
    arrived on time". A feed that declares nothing is the honest absence and
    is not an error; a feed that declares `7am` is a mistake and says so.

    "24:00" is refused rather than folded to midnight. A delivery expected by
    the end of the day is expected by 23:59, and accepting an hour that does
    not exist invites the reader to believe some rollover rule is implemented
    here. There is none.
    """
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        # YAML 1.1 reads an unquoted `7:00` as SEXAGESIMAL -- the integer 420.
        # Verified with the pyyaml this platform loads config with:
        # `yaml.safe_load("a: 7:00")` is `{"a": 420}`, and `23:59` is 1439.
        # A LEADING ZERO happens to block it (pyyaml's int resolver requires
        # [1-9] first, so `07:00` stays a string), which makes this the worst
        # kind of trap: it depends on whether somebody padded the hour, so it
        # would work for every feed until the first one written `9:00`.
        # Quoting is the rule; this branch is what makes forgetting it loud.
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `expected_by: {value!r}` is "
            f"{type(value).__name__}, not a string. Quote it -- unquoted, "
            f"YAML reads 7:00 as the integer 420 (sexagesimal).")
    text = value.strip()
    if not _EXPECTED_BY_RE.match(text):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `expected_by: {value!r}` is not a "
            f"24-hour HH:MM time. Examples: '07:00', '18:30'. A delivery due "
            f"at the end of the day is '23:59'; '24:00' is not a time.")
    return text


# --------------------------------------------------------------- the gate
# What `arrival:` may say. Same rule as `delivery:`: every key here is read by
# ingest/conform.py, and a key nothing dispatches on does not go in this table.
#
# IDENTITY ONLY. The inbox exists to establish that a delivery is correctly
# named and has its prerequisites -- which feed, which source system, which
# business date, which version. It does NOT verify content: `row_count` and
# `md5` live on `delivery.control` and are checked at ingest, once, the same
# way for a legacy delivery and for one an approved sender wrote straight into
# landing. Putting them here too would be a second implementation of the same
# check on one of the two paths.
# See docs/DECISIONS.md#the-inbox-is-the-conformance-gate
ARRIVAL_KEYS = {"source_pattern", "control", "archive"}
ARRIVAL_CONTROL_KEYS = {"pattern", "business_date", "version"}


def resolve_arrival_config(feed_name: str, arrival: Any,
                           filename_pattern: str | None = None) -> dict[str, Any]:
    """Validate an `arrival:` block and fill its defaults.

    Checked at LOAD, like `delivery:` and `conventions:`, because every way
    this can be wrong is otherwise silent: a `source_pattern` that matches
    nothing means the inbox rejects the feed's real deliveries to
    `.rejected/` forever, and nothing says why.
    """
    if not arrival:
        return {}
    if not isinstance(arrival, dict):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival:` must be a mapping, got "
            f"{type(arrival).__name__}")

    unknown = set(arrival) - ARRIVAL_KEYS
    if unknown:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival:` has unknown key(s) "
            f"{', '.join(sorted(unknown))}. Valid: "
            f"{', '.join(sorted(ARRIVAL_KEYS))}")

    source_pattern = arrival.get("source_pattern")
    if not source_pattern or not isinstance(source_pattern, str):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} sets `arrival:` with no "
            f"`source_pattern`. That pattern is how the inbox recognises this "
            f"feed's delivery under the name the UPSTREAM sends, which is by "
            f"definition not the name landing will hold.")
    try:
        compiled = re.compile(source_pattern)
    except re.error as exc:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.source_pattern` is not a "
            f"valid regex: {exc}") from exc

    out: dict[str, Any] = {"source_pattern": source_pattern}
    # `in`, not `.get() is not None`: `control:` with nothing under it parses
    # as None, and skipping it silently would let the block fall through to
    # the business-date rule below and fail with a message about dates rather
    # than about the empty block that actually caused it.
    for key, resolver in (("control", _resolve_arrival_control),
                          ("archive", _resolve_arrival_archive)):
        if key not in arrival:
            continue
        if not arrival[key]:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} has an empty "
                f"`arrival.{key}:` block. Fill it in, or remove the key -- an "
                f"empty one reads as configured and does nothing.")
        out[key] = resolver(feed_name, arrival[key])

    # EXACTLY ONE SOURCE FOR THE BUSINESS DATE, and both failures are real.
    # Neither, and the gate cannot name the file it is meant to produce.
    # Both, and one fact has two sources that can disagree, with the winner
    # decided by whichever the gate happens to read first.
    # AN ARCHIVE IS THE THIRD CASE. Each member is its own delivery carrying
    # its own date, so `member_pattern` is where the date comes from and the
    # container needs none -- it is a transport wrapper, not a delivery. The
    # container may still carry one (senders often name the zip by run date),
    # and it is simply not read, so this rule stops short of forbidding it.
    if "archive" in out:
        return _finish_arrival(feed_name, out, filename_pattern)

    in_name = "business_date" in compiled.groupindex
    in_control = "business_date" in out.get("control", {})
    if in_name and in_control:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} takes its business date from both "
            f"`arrival.source_pattern` and `arrival.control.business_date`. "
            f"One fact, one source -- drop whichever is not the real one.")
    if not in_name and not in_control:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival:` gives the gate no way "
            f"to find the business date. Either `source_pattern` captures "
            f"(?P<business_date>\\d{{8}}), or `arrival.control.business_date` "
            f"reads it out of the control file -- without one the delivery "
            f"cannot be given the name landing requires.")

    # The gate's whole job is producing a name `parse_filename` accepts, so a
    # feed whose landing pattern has no date to write into is unservable. It
    # is already an error for any feed (checked where filename_pattern is),
    # but failing here says which of the two patterns is the problem.
    return _finish_arrival(feed_name, out, filename_pattern)


def _finish_arrival(feed_name: str, out: dict[str, Any],
                    filename_pattern: str | None) -> dict[str, Any]:
    """The gate's whole job is producing a name `parse_filename` accepts, so a
    feed whose landing pattern has no date to write into is unservable. That is
    already an error for any feed; failing here says which of the two patterns
    is the problem."""
    if filename_pattern is not None:
        try:
            if "business_date" not in re.compile(filename_pattern).groupindex:
                raise ValueError(
                    f"feeds.yml: feed {feed_name!r} has an `arrival:` block, so "
                    f"the inbox renames its deliveries to match "
                    f"`filename_pattern` -- but that pattern captures no "
                    f"(?P<business_date>...), so there is nowhere to write the "
                    f"date the gate just went and found.")
        except re.error:
            pass          # reported as a filename_pattern error elsewhere
    return out


def _resolve_arrival_archive(feed_name: str, archive: Any) -> dict[str, str]:
    """Validate `arrival.archive` -- a container the gate unpacks.

    A ZIP IS NOT A DELIVERY, it is a transport wrapper, and removing wrappers
    is what the gate is for. So the container is unpacked at the door and its
    MEMBERS are landed as ordinary deliveries; `landing/` never holds an
    archive, and nothing downstream needs a reader for one.

    Each member is a complete delivery for its own business date, which is why
    `member_pattern` must capture one: unpacking turns one inbox file into N
    inbox files, and each then follows the ordinary single-file path with no
    grouping, no `parts` list and no manifest to hold them together.
    """
    if not isinstance(archive, dict):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.archive` must be a "
            f"mapping, got {type(archive).__name__}")
    unknown = set(archive) - {"member_pattern"}
    if unknown:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.archive` has unknown "
            f"key(s) {', '.join(sorted(unknown))}. Valid: member_pattern")

    pattern = archive.get("member_pattern")
    if not pattern or not isinstance(pattern, str):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.archive` sets no "
            f"`member_pattern`. Which members belong to this feed is not "
            f"guessable -- a zip routinely carries a checksum, a manifest or "
            f"another feed's file alongside the data, and a wrong guess would "
            f"silently land the wrong files.")
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.archive.member_pattern` "
            f"is not a valid regex: {exc}") from exc
    if "business_date" not in compiled.groupindex:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.archive.member_pattern` "
            f"{pattern!r} captures no (?P<business_date>...). Each member is "
            f"landed as its own delivery, so each must say which day it is "
            f"for -- a date on the CONTAINER instead would mean the members "
            f"are parts of one delivery, which is a different shape and is "
            f"not built.")
    return {"member_pattern": pattern}


def _resolve_arrival_control(feed_name: str, control: Any) -> dict[str, str]:
    """Validate `arrival.control` -- the control file AS IT ARRIVES.

    A separate block from `delivery.control`, because they answer different
    questions and a legacy feed needs BOTH.

    This one is about IDENTITY: which control file belongs to this delivery,
    and what does it say the delivery's business date and version are -- the
    facts the inbox needs to give the file its correct name. It is read at the
    door and then the control file is promoted to `landing/` alongside its
    data file, renamed to match.

    `delivery.control` is about INTEGRITY: it gates the landed delivery until
    its control file is beside it and checks the row count and checksum at
    ingest. That runs for EVERY delivery -- legacy or from an approved sender
    that wrote straight to landing -- which is what makes the two paths
    identical from landing onwards.
    """
    if not isinstance(control, dict):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.control` must be a "
            f"mapping, got {type(control).__name__}")

    unknown = set(control) - ARRIVAL_CONTROL_KEYS
    if unknown:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.control` has unknown "
            f"key(s) {', '.join(sorted(unknown))}. Valid: "
            f"{', '.join(sorted(ARRIVAL_CONTROL_KEYS))}")

    pattern = control.get("pattern")
    if not pattern or not isinstance(pattern, str):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.control` sets no "
            f"`pattern`. Which control file belongs to a delivery is not "
            f"guessable.")
    if "{stem}" not in pattern:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.control.pattern` "
            f"{pattern!r} does not reference `{{stem}}` -- without it every "
            f"delivery for this feed would look for the same control filename.")
    try:
        re.compile(pattern.format(stem="X"))
    except re.error as exc:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} `arrival.control.pattern` is not a "
            f"valid regex once `{{stem}}` is filled in: {exc}") from exc

    out = {"pattern": pattern}
    # Each of these is a regex over the control file's TEXT with one named
    # group, and the group name is the only thing read out of a match.
    # IDENTITY ONLY -- `row_count` and `md5` belong to `delivery.control`.
    for key, group in (("business_date", "business_date"),
                       ("version", "version")):
        value = control.get(key)
        if value is None:
            continue
        try:
            compiled = re.compile(value)
        except re.error as exc:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `arrival.control.{key}` is not "
                f"a valid regex: {exc}") from exc
        if group not in compiled.groupindex:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `arrival.control.{key}` "
                f"{value!r} has no `(?P<{group}>...)` group -- that is the "
                f"only thing the gate reads out of a match.")
        out[key] = value
    return out


def check_gates_are_coherent(feed_name: str, arrival: dict[str, Any],
                             delivery: dict[str, Any]) -> None:
    """A feed that arrives through the inbox needs BOTH control blocks.

    They are complementary, not alternatives, and an earlier draft of this
    function had that exactly backwards -- it REJECTED the combination,
    because at the time the inbox consumed the control file and never
    promoted it, so `delivery.control` really would have waited forever. The
    inbox now passes the control file through to `landing/` renamed, which is
    what makes the two arrival paths identical from landing onwards, and the
    combination is not merely legal but required:

      * `arrival.control` says how to find the control file at the door and
        what it declares about IDENTITY (business date, version) -- the facts
        needed to name the file correctly.
      * `delivery.control` gates the landed delivery on that control file
        being beside it and checks INTEGRITY (row count, md5) at ingest, for
        every delivery however it arrived.

    So the error is the other way round: an `arrival.control` with no
    `delivery.control` promotes a control file into landing that nothing ever
    reads, and quietly loses the row-count and checksum checks for exactly
    the feeds least likely to deserve that trust.
    """
    if arrival.get("control") and not (delivery or {}).get("control"):
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} sets `arrival.control` but no "
            f"`delivery.control`. The inbox promotes the control file into "
            f"landing/ alongside the renamed delivery, and `delivery.control` "
            f"is what reads it there -- without it the control file lands and "
            f"nothing checks the row count or checksum, for a legacy feed, "
            f"which is the last place to skip them. Add "
            f"`delivery: {{control: {{pattern: ...}}}}`.")


# A convention may set anything a feed block may, EXCEPT these two.
#
# `name`: sharing one across feeds would silently collapse them into a single
# entry in the registry dict -- the last one wins and the others simply vanish,
# with no error anywhere.
#
# `convention`: conventions DO NOT CHAIN. Resolution reads the name from the
# feed block only, so a convention naming another one would not inherit from
# it -- it would just overwrite the resolved Feed's `convention` field with a
# name that had no effect, which is a lie told by the very field that exists to
# explain where a value came from. Chaining is also a diamond-merge design
# nobody has asked for; one layer between defaults and the feed is the whole
# point.
CONVENTION_FORBIDDEN = {
    "name": "that is per-feed identity, and sharing one would collapse two "
            "feeds into a single registry entry",
    "convention": "conventions do not chain -- setting it here would record a "
                  "name that had no effect",
}


def resolve_conventions(cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate the `conventions:` section and return it, or {} if absent.

    Checked HERE, at load, rather than where a value is used, because the
    failure modes are all silent otherwise: a misspelled convention name would
    fall back to defaults and produce a feed configured subtly wrong rather
    than one that does not exist, and a misspelled KEY inside a convention
    would be dropped by the `allowed` filter below without comment. Both
    produce a working platform doing the wrong thing, which is the failure this
    repo keeps having and keeps regretting.

    Unknown keys are rejected in conventions but NOT in feed blocks. That is
    inconsistent on purpose: `conventions:` is new surface with nothing
    depending on it, so it can be strict from the start, whereas adding the
    same check to feed blocks could refuse to load an existing feeds.yml and
    take the whole platform down at import for a key that has always been
    harmlessly ignored. Worth doing later, deliberately, as its own change.
    """
    section = cfg.get("conventions") or {}
    if not isinstance(section, dict):
        raise ValueError(
            f"feeds.yml: `conventions:` must be a mapping of name to settings, "
            f"got {type(section).__name__}")
    allowed = {f.name for f in Feed.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    for cname, settings in section.items():
        if not isinstance(settings, dict):
            raise ValueError(
                f"feeds.yml: convention {cname!r} must be a mapping, got "
                f"{type(settings).__name__}")
        for key in sorted(CONVENTION_FORBIDDEN.keys() & set(settings)):
            raise ValueError(
                f"feeds.yml: convention {cname!r} may not set {key!r}: "
                f"{CONVENTION_FORBIDDEN[key]}")
        unknown = set(settings) - allowed
        if unknown:
            raise ValueError(
                f"feeds.yml: convention {cname!r} sets unknown key(s) "
                f"{', '.join(sorted(unknown))}. Valid keys are: "
                f"{', '.join(sorted(allowed - CONVENTION_FORBIDDEN.keys()))}")
    return section


def effective_defaults(convention: str = "",
                       cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """What a feed block inherits before its own keys are applied.

    `defaults:` overlaid with the named convention, SHALLOW at each layer --
    a dict-valued key such as `column_types` is replaced by the more specific
    layer, not merged into it. Predictability beats convenience: with a deep
    merge there is no way to *remove* an inherited entry, and "why is this
    column still a decimal" becomes a question answered by reading three
    places. Revisit deliberately if a nested `delivery:` block ever wants
    partial override.

    THIS IS THE ONLY IMPLEMENTATION OF THE MERGE. `_feeds_at` builds every
    Feed on top of it, and the feed console asks it what a block would inherit
    so it can leave inherited values OUT of the block it writes -- see
    `ui/registry._block`. A second copy of this ordering would drift, and it
    would drift silently: the console would start pinning inherited values
    into individual feed blocks, defeating the convention while producing a
    diff that looks deliberate.
    """
    cfg = _load("feeds.yml") if cfg is None else cfg
    known = resolve_conventions(cfg)
    if convention and convention not in known:
        raise ValueError(
            f"feeds.yml: convention {convention!r} is not defined. "
            f"Available: {', '.join(sorted(known)) or '(none)'}")
    return {**(cfg.get("defaults") or {}), **(known.get(convention) or {})}


@lru_cache(maxsize=None)
def _conventions_at(mtime_ns: int) -> dict[str, dict[str, Any]]:
    return resolve_conventions(_load("feeds.yml"))


def conventions() -> dict[str, dict[str, Any]]:
    """The `conventions:` section, keyed by name. Empty if there is none."""
    return _conventions_at((CONFIG_DIR / "feeds.yml").stat().st_mtime_ns)


@lru_cache(maxsize=None)
def _feeds_at(mtime_ns: int) -> dict[str, Feed]:
    cfg = _load("feeds.yml")
    known = resolve_conventions(cfg)
    out: dict[str, Feed] = {}
    for block in cfg["feeds"]:
        cname = block.get("convention") or ""
        if cname and cname not in known:
            raise ValueError(
                f"feeds.yml: feed {block['name']!r} names convention "
                f"{cname!r}, which is not defined. Available: "
                f"{', '.join(sorted(known)) or '(none)'}")
        merged = {**effective_defaults(cname, cfg), **block}
        merged["delivery"] = resolve_delivery_config(
            block["name"], merged.get("delivery"))
        merged["arrival"] = resolve_arrival_config(
            block["name"], merged.get("arrival"), merged.get("filename_pattern"))
        merged["supersession"] = resolve_supersession_config(
            block["name"], merged.get("supersession"))
        merged["expected_by"] = parse_expected_by(
            block["name"], merged.get("expected_by"))
        merged["retention_class"] = check_retention_class(
            block["name"], merged.get("retention_class"))
        check_gates_are_coherent(block["name"], merged["arrival"],
                                   merged["delivery"])
        names, sources = split_columns(merged.get("columns") or [])
        merged["columns"] = names
        # An explicit source_columns: block wins over the inline form, so a
        # feed can use whichever reads better without them fighting.
        merged["source_columns"] = {**sources, **(merged.get("source_columns") or {})}
        allowed = {f.name for f in Feed.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        out[block["name"]] = Feed(**{k: v for k, v in merged.items() if k in allowed})
    return out


def feeds() -> dict[str, Feed]:
    """The feed registry, keyed by name.

    Keyed on feeds.yml's mtime for the same reason `_load` is -- see there.
    A registry edit is picked up by every process on its next call.
    """
    return _feeds_at((CONFIG_DIR / "feeds.yml").stat().st_mtime_ns)


def feed(name: str) -> Feed:
    return feeds()[name]


# The prepared and reporting tables are DERIVED from the dbt project, not
# listed. See docs/DECISIONS.md#managed-tables-are-derived
#
# This rests on model filename == table name, which holds because no model
# carries a layer prefix or a dbt `alias` -- `raw.fo_trade` and `prepared.fo_trade`
# are the same name in different namespaces.
# See docs/DECISIONS.md#table-naming-no-layer-prefix
DBT_MODELS_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")) / "models"

# A model whose `alias` differs from its filename would break the one
# assumption this derivation rests on, silently and in the direction that
# matters: maintenance and retention would address a table that does not exist.
_ALIAS = re.compile(r"\balias\s*=")


@lru_cache(maxsize=None)
def _models_at(layer: str, mtime_ns: int) -> tuple[str, ...]:
    names = []
    for path in sorted((DBT_MODELS_DIR / layer).glob("*.sql")):
        if _ALIAS.search(path.read_text(encoding="utf-8")):
            raise RuntimeError(
                f"{path} sets a dbt `alias`. managed_tables() derives table "
                f"names from model FILENAMES, so an alias would point "
                f"maintenance and retention at a table that does not exist. "
                f"Either drop the alias or teach context.models_in() to read "
                f"the manifest.")
        names.append(path.stem)
    return tuple(names)


def models_in(layer: str) -> tuple[str, ...]:
    """dbt model names in a layer, which are also its table names.

    Keyed on the DIRECTORY's mtime, which changes when a model is added or
    removed -- the only events that change this set. Same reasoning as
    `_load`: a long-lived process must not hold a stale answer.

    RAISES rather than returning empty when the directory is absent. Returning
    () would be the silent failure this derivation exists to remove: a
    container without the dbt project mounted (the watchdog is one) would
    quietly report that the platform manages nothing, and every maintenance and
    retention pass would succeed having done nothing at all.
    """
    directory = DBT_MODELS_DIR / layer
    if not directory.is_dir():
        raise RuntimeError(
            f"no dbt models directory at {directory}. managed_tables() derives "
            f"the prepared and reporting tables from the dbt project, so it "
            f"needs the project mounted -- set DBT_PROJECT_DIR, or mount ./dbt "
            f"into this service.")
    return _models_at(layer, directory.stat().st_mtime_ns)


def managed_tables() -> list[tuple[str, str]]:
    """(fully qualified table, layer) for everything the platform maintains.

    ONE definition, imported by both the DAG and the CLIs, so a hand-maintained
    `--table` list cannot drift from what the DAG actually maintains.
    See docs/DECISIONS.md#managed-tables-single-definition and
    #managed-tables-are-derived

    NOTHING HERE IS HAND-MAINTAINED. The raw half comes from `feeds()`, the
    other two from the dbt project, so adding a feed or a model extends
    maintenance and retention on its own.
    """
    tables = [(f.raw_table, "raw") for f in feeds().values()]
    for layer in ("prepared", "reporting"):
        tables += [(f"{CATALOG}.{layer}.{t}", layer) for t in models_in(layer)]
    return tables


# ---------------------------------------------------------------- reports
# A REPORT IS A dbt EXPOSURE, and it is derived from the project exactly as
# the managed tables are. The alternative was a `reports:` block in a new
# config file, which would be a second place to declare something the dbt
# project already declares -- and the two would disagree the first time a
# model was renamed.
#
# An exposure is the right object for this and not a convenient one: it is
# what already answers "what breaks if I change this model", it already names
# an owner, and `dbt ls --select +exposure:<name>` already resolves it to the
# models behind it. A published run is a run of those models, and the tag it
# cuts is what makes that run addressable per report -- which is what
# `references.published_tags.per_report` has been waiting for.
_EXPOSURE_LAYERS = ("reporting",)


@lru_cache(maxsize=None)
def _reports_at(mtime_ns: int) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for layer in _EXPOSURE_LAYERS:
        directory = DBT_MODELS_DIR / layer
        if not directory.is_dir():
            raise RuntimeError(
                f"no dbt models directory at {directory}. Reports are derived "
                f"from the project's exposures, so it needs the project "
                f"mounted -- set DBT_PROJECT_DIR, or mount ./dbt into this "
                f"service.")
        for path in sorted(directory.glob("*.yml")):
            doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            for exposure in doc.get("exposures") or []:
                name = exposure.get("name")
                if not name:
                    raise ValueError(f"{path}: an exposure has no `name`")
                if "/" in name:
                    # The tag shape is published/<report>/<bd>/<run>, so a
                    # slash in a report name would produce a tag that TAG_RE
                    # parses into the wrong fields -- silently, and only
                    # visibly when retention resolved the wrong window for it.
                    raise ValueError(
                        f"{path}: exposure {name!r} contains '/'. A report "
                        f"name becomes a segment of the published tag, which "
                        f"is split on '/'.")
                out[name] = {
                    "name": name,
                    "label": exposure.get("label") or name,
                    "type": exposure.get("type") or "analysis",
                    "owner": (exposure.get("owner") or {}).get("name") or "",
                    # `ref('x')` -> x. The models this report is built from,
                    # which is what makes a run attributable to it.
                    "models": sorted({
                        m.split("'")[1] if "'" in m else m.strip()
                        for m in (exposure.get("depends_on") or [])
                        if isinstance(m, str)}),
                    # Whatever the exposure declares about itself that the
                    # platform reads: `restatement:` today. Carried through
                    # raw rather than projected key by key, so an exposure can
                    # be given a new policy without this derivation changing.
                    "meta": dict(exposure.get("meta") or {}),
                }
    return out


def reports() -> dict[str, dict[str, Any]]:
    """Every report the platform publishes, derived from the dbt exposures.

    Keyed on the directory mtime like `models_in`, and for the same reason: a
    long-lived process must not hold a stale answer after an exposure is
    added.
    """
    directory = DBT_MODELS_DIR / _EXPOSURE_LAYERS[0]
    if not directory.is_dir():
        raise RuntimeError(
            f"no dbt models directory at {directory}; cannot derive reports.")
    return _reports_at(directory.stat().st_mtime_ns)


def report(name: str) -> dict[str, Any]:
    """One report, or a refusal naming what the project actually declares."""
    known = reports()
    if name not in known:
        raise ValueError(
            f"no report {name!r}. A report is a dbt EXPOSURE, so the ones "
            f"that exist are the ones declared in "
            f"{DBT_MODELS_DIR / _EXPOSURE_LAYERS[0]}: "
            f"{', '.join(sorted(known)) or '(none)'}.")
    return known[name]


def report_owner(name: str) -> str:
    """REQ-501's named owner: the exposure's `owner.name`.

    NOT A NEW FIELD. An exposure already names an owner -- it is one of the
    reasons an exposure is the right object for a report -- and REQ-501 asks
    for somebody to be told when a report's as-at date needs an exception.
    Two requirements, one derivation, and no second place for the name to be
    wrong in.

    Refuses an unnamed owner rather than returning "". `registry/lifecycle.py`
    checks a reopening approver against this value, so an empty owner would
    make that check vacuous -- it would accept every approver, quietly, which
    is the failure mode a guard against an unsettable value always has.
    """
    owner = (report(name).get("owner") or "").strip()
    if not owner:
        raise ValueError(
            f"report {name!r} declares no `owner: name:` in its exposure. "
            f"That name is REQ-501's named owner and is what a reopening "
            f"approver is checked against, so an absent one would make the "
            f"check accept anybody. Add it to the exposure.")
    return owner


# REQ-502. What happens to a published as-at date whose inputs later change.
#
#   restate       -- the figure must be republished. A publish onto a locked
#                    or submitted date whose input set has moved is REFUSED
#                    until somebody reopens it, so the restatement is a
#                    decision somebody made rather than one that happened.
#   carry_forward -- the published figure stands and the change surfaces at
#                    the current date instead. Recorded on the run, so
#                    "why does last month's number not include this" has an
#                    answer that is not somebody's memory.
RESTATEMENT_POLICIES = ("restate", "carry_forward")


def restatement_policy(name: str) -> str:
    """A report's restatement policy, or a refusal. There is NO DEFAULT.

    Open decision 2, settled as `fail`. A silent platform-wide default is how
    a regulatory report gets carried forward when it should have been
    restated: nothing raises, both behaviours look identical until somebody
    asks why two published figures disagree, and by then the decision has been
    made many times by omission.

    DECLARED ON THE EXPOSURE, under `meta:`, because a report IS an exposure
    here -- a separate config block would be a second list of reports to
    disagree with `reports()` the first time one was added.

    REFUSED HERE RATHER THAN IN `reports()`, and that is a deliberate choice
    about blast radius. `reports()` is a pure derivation of the project and
    the retention chain now calls it, through `feeds_behind_report`. Raising
    inside it would take the nightly sweep down over an undeclared restatement
    policy -- a failure with nothing to do with retention. So the refusal is
    at the point of the decision, and `tests/test_lifecycle.py` asserts every
    exposure declares one, which is what makes it un-forgettable rather than
    merely documented.
    """
    meta = report(name).get("meta") or {}
    value = meta.get("restatement")
    if value is None:
        raise ValueError(
            f"report {name!r} declares no `meta: restatement:` on its "
            f"exposure. There is deliberately no default: the two answers -- "
            f"{' or '.join(RESTATEMENT_POLICIES)} -- differ only after a "
            f"published date's inputs change, so a default would be a policy "
            f"nobody chose, in force for years before anybody noticed. "
            f"Declare it in the exposure.")
    if value not in RESTATEMENT_POLICIES:
        raise ValueError(
            f"report {name!r}: `meta: restatement: {value!r}` is not "
            f"recognised. Valid: {', '.join(RESTATEMENT_POLICIES)}.")
    return str(value)


def feeds_behind_report(name: str) -> list[str]:
    """Every feed whose data reaches `name`, by walking the project's `ref()`s.

    WHAT THIS IS FOR. `check_reproducibility_window` has to know whether a
    feed's landing evidence outlives the pins of the reports built from it.
    Before retention classes there was one landing window and one comparison;
    with classes the question is per feed, and answering it needs the lineage
    rather than a list somebody maintains.

    DERIVED, LIKE EVERYTHING ELSE HERE. Exposure `depends_on` gives the
    reporting models; each model's SQL gives its `ref()`s; a ref naming a
    PREPARED model is a feed, by the platform's rule that model filename ==
    table name == feed name, and a ref naming a reporting model is followed.
    That is `managed_tables()`'s derivation applied transitively, and it uses
    the project files rather than dbt's `manifest.json`, which Cosmos
    overwrites once per model and which therefore records whichever task
    finished last (see registry/runs.py on `dbt_manifest_ref`).

    A ref that resolves to neither layer is REPORTED, not guessed at -- the
    caller decides whether an unresolvable dependency is fatal. Returning it
    silently as "no feeds" would make an under-retained feed invisible, which
    is the direction that loses evidence.
    """
    reporting = set(models_in("reporting"))
    prepared = set(models_in("prepared"))
    known_feeds = set(feeds())

    found: set[str] = set()
    seen: set[str] = set()
    queue = list(report(name)["models"])
    while queue:
        model = queue.pop()
        if model in seen:
            # dbt refuses a cyclic ref, but this walk reads files rather than
            # a compiled graph, so it cannot rely on that having been checked.
            continue
        seen.add(model)
        if model in prepared:
            if model in known_feeds:
                found.add(model)
            continue
        if model not in reporting:
            raise ValueError(
                f"report {name!r} depends on {model!r}, which is neither a "
                f"prepared nor a reporting model in {DBT_MODELS_DIR}. The "
                f"lineage behind this report cannot be resolved, so the "
                f"feeds behind it cannot be named.")
        queue.extend(model_refs("reporting", model))
    return sorted(found)


def layer_of(model: str) -> str:
    """Which layer a dbt model lives in, or "" if the project has no such model.

    The model -> layer answer, in ONE place. `managed_tables()` needs it to
    qualify a table, the OpenLineage export needs it to name a dataset, and a
    second copy would be a second opinion about where `fo_trade` is written.
    Returns "" rather than raising because both callers have a more useful
    thing to say about an unknown name than this function does.
    """
    for layer in ("prepared", "reporting"):
        if model in models_in(layer):
            return layer
    return ""


def model_refs(layer: str, model: str) -> list[str]:
    """The `ref()`s a model declares, read off its SQL.

    THE ONE REF WALKER. `feeds_behind_report()` uses it to decide retention
    windows and `reporting_platform/lineage` uses it to draw the graph Marquez
    shows, so the picture and the obligation are derived from the same read of
    the same files. Two walkers would eventually disagree, and the one anybody
    would notice is the picture -- while the one that loses evidence is the
    other. See docs/DECISIONS.md#lineage-is-derived-from-the-dbt-project.
    """
    return _REF_RE.findall(_model_sql(layer, model))


def model_sources(layer: str, model: str) -> list[tuple[str, str]]:
    """The `source()`s a model declares, as (source name, table) pairs.

    The other half of a model's inputs. `feeds_behind_report()` never needed
    this -- it stops at a prepared model, because by the platform's rule that
    model name == feed name it already knows the feed -- but a lineage graph
    has to name the raw table that prepared model actually reads.
    """
    return _SOURCE_RE.findall(_model_sql(layer, model))


def _model_sql(layer: str, model: str) -> str:
    return (DBT_MODELS_DIR / layer / f"{model}.sql").read_text(encoding="utf-8")


# `ref('x')` / `ref("x")`, and `source('raw', 'x')`. Read off the model SQL
# rather than the compiled manifest -- see feeds_behind_report on why the
# manifest is not trustworthy here.
_REF_RE = re.compile(r"""\bref\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")
_SOURCE_RE = re.compile(
    r"""\bsource\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*,"""
    r"""\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")


def retention_policy(layer: str) -> dict[str, Any]:
    return _load("retention.yml")["environments"][ENV][layer]


# ----------------------------------------------------------- retention classes
# REQ-600/601. A retention class is a named EVIDENCE OBLIGATION that a feed is
# in: how long what the upstream actually sent is kept, in `landing/` and in
# `quarantine/`.
#
# THE NAME IS IN feeds.yml AND THE WINDOWS ARE HERE, which is the whole design.
# A feed's class is a property of the feed and belongs beside it, inheritable
# through `conventions:` because the obligation nearly always comes from the
# source system rather than from one feed. The YEARS are a per-environment
# policy value and belong in retention.yml with every other one. Listing feed
# NAMES per class in retention.yml -- the obvious alternative -- would create a
# second registry of feeds that drifts from the first, which is exactly what
# deriving reports from the exposures exists to avoid.
#
# CLASSES ARE DECLARED ONCE, ENVIRONMENT-INDEPENDENTLY, at retention.yml's top
# level, and each environment then gives windows only for the classes it wants
# to move. If the declaration were per environment, a class declared in `local`
# and forgotten in `dev` would make feeds.yml fail to load in dev alone -- a
# config file breaking in one environment for a key that reads fine in the one
# you are looking at.
PREFIX_CLASSES = ("landing", "quarantine")


def retention_classes() -> dict[str, dict[str, Any]]:
    """The declared retention classes, keyed by name. Never empty.

    `standard` is always present: it is `Feed.retention_class`'s default and
    the window every feed had before classes existed, so a retention.yml that
    declares nothing keeps behaving exactly as it did.
    """
    declared = _load("retention.yml").get("retention_classes") or {}
    if not isinstance(declared, dict):
        raise ValueError(
            f"retention.yml: `retention_classes:` must be a mapping of class "
            f"name to settings, got {type(declared).__name__}")
    return {"standard": {"description": "The platform default."}, **declared}


def check_retention_class(feed_name: str, value: Any) -> str:
    """Validate a feed's `retention_class:` against what retention.yml declares.

    REFUSED AT LOAD, like an undefined `convention:`, and for the same reason:
    an unrecognised class would otherwise fall back to the default window
    silently, and the direction of that fallback is over-retention -- which
    looks fine forever and means the class somebody wrote was never in force.
    """
    text = str(value or "standard").strip() or "standard"
    known = retention_classes()
    if text not in known:
        raise ValueError(
            f"feeds.yml: feed {feed_name!r} names retention class {text!r}, "
            f"which retention.yml does not declare. Available: "
            f"{', '.join(sorted(known))}. Add it under `retention_classes:` "
            f"there before naming it here.")
    return text


def class_keep_years(prefix: str, retention_class: str = "standard") -> int:
    """Years the `prefix` evidence for a feed in `retention_class` is kept.

    Resolution is `environments[ENV][prefix].classes[<class>].keep_years`,
    falling back to `environments[ENV][prefix].keep_years` -- the block's own
    value, which is what every feed used before classes existed.

    THE FALLBACK IS PER PREFIX, NOT PER CLASS, deliberately. A class that
    shortens `landing:` and says nothing about `quarantine:` gets quarantine's
    ordinary window, because the two answer to different things and
    retention.yml's own comment already says so: quarantine has neither of
    landing's hard floors, so shortening one is a policy call somebody can
    make without making the other.

    Refuses an unreadable window rather than falling back, matching
    `tag_retention_years`: the number authorises an unrecoverable deletion.
    """
    if prefix not in PREFIX_CLASSES:
        raise ValueError(
            f"retention classes govern {' and '.join(PREFIX_CLASSES)} only, "
            f"not {prefix!r}. A table keep-set is a decision about a LAYER -- "
            f"how much history is queryable -- not about one feed's evidence "
            f"obligation. See docs/DECISIONS.md#retention-classes-name-the-obligation")
    block = retention_policy(prefix)
    classes = block.get("classes") or {}
    if not isinstance(classes, dict):
        raise ValueError(
            f"retention.yml (env {ENV!r}): {prefix}.classes must be a mapping "
            f"of class name to settings, got {type(classes).__name__}")
    entry = classes.get(retention_class) or {}
    value = entry.get("keep_years", block.get("keep_years"))
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"retention.yml (env {ENV!r}): {prefix} keep_years for class "
            f"{retention_class!r} is {value!r}, which is not a whole number of "
            f"years. {prefix}/ is the evidence copy -- an unreadable window "
            f"here would authorise deleting what the upstream actually sent.")
    if value <= 0:
        raise ValueError(
            f"retention.yml (env {ENV!r}): {prefix} keep_years for class "
            f"{retention_class!r} is {value}. A window of zero or less expires "
            f"the whole prefix immediately, which is never what is meant.")
    return value


def reference_policy(kind: str) -> dict[str, Any]:
    return _load("retention.yml")["references"][kind]


# How long a published tag is kept, in years. Its own function rather than a
# `.get()` at the call site because the answer depends on the report and the
# resolution order is a policy decision, not a lookup.
DEFAULT_TAG_KEEP_YEARS = 10


def tag_retention_years(report: str | None = None) -> int:
    """Years a published tag for `report` is kept. `None` = no report named.

    THIS IS DATA RETENTION. A tag pins every data file its commit referenced,
    so this number is the reproducibility window for everything published
    under it -- not a table window, and deliberately not the keep-set the
    table layers use. See retention.yml's `references.published_tags` block
    and docs/DECISIONS.md#published-tags-are-the-reproducibility-window.

    Resolution is `per_report[report]` then `default_keep_years`. A report
    with no entry is the ordinary case and is not an error: the default exists
    precisely so an unnamed or newly added report is over-retained rather than
    silently dropped.

    EITHER WINDOW MAY BE PER-ENVIRONMENT, a bare number or a map keyed by
    `REPORTING_ENV`, exactly as `nessie_gc.deferred_delete_after_hours`
    already is. That is not decoration: `landing.keep_years` is per
    environment and `dev` deliberately shortens it, so a globally fixed tag
    window would leave dev pinning published runs for a decade whose source
    evidence it threw away after one year -- and the interlock in
    retention.py, which refuses exactly that combination, would refuse every
    dev sweep.

    REFUSES BAD CONFIG RATHER THAN FALLING BACK. A missing, non-integer or
    non-positive window would otherwise be read as "expire everything", and
    the deletion it authorises is unrecoverable -- the same reason the GC
    cutoff interlock in retention.py raises instead of warning. A shorter
    per-report window is permitted, because a shorter period can be a real
    answer, but it is logged: it is the direction that loses evidence.
    """
    policy = reference_policy("published_tags")

    def _years(value: Any, what: str) -> int:
        if isinstance(value, dict):
            if ENV not in value:
                raise ValueError(
                    f"retention.yml: references.published_tags {what} has no "
                    f"entry for env {ENV!r} (has {sorted(value)}). Add one, or "
                    f"use a bare number of years.")
            value = value[ENV]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(
                f"retention.yml: references.published_tags {what} is "
                f"{value!r}, which is not a whole number of years. A published "
                f"tag pins every data file its commit referenced, so an "
                f"unreadable window here would authorise deleting the evidence "
                f"a published run is reproduced from.")
        if value <= 0:
            raise ValueError(
                f"retention.yml: references.published_tags {what} is {value}. "
                f"A window of zero or less expires every pin immediately, "
                f"which is never what is meant -- remove the entry to fall "
                f"back to default_keep_years instead.")
        return value

    if "default_keep_years" not in policy:
        raise ValueError(
            "retention.yml: references.published_tags has no "
            "`default_keep_years`. It used to carry the table keep-set "
            "(`keep_business_days`/`keep_month_ends`), which is the wrong "
            "shape for a reproducibility pin -- see the block's comment. "
            "Refusing rather than guessing a window.")
    default = _years(policy["default_keep_years"], "default_keep_years")

    per_report = policy.get("per_report") or {}
    if not isinstance(per_report, dict):
        raise ValueError(
            f"retention.yml: references.published_tags.per_report is "
            f"{type(per_report).__name__}, expected a mapping of report name "
            f"to years.")
    if report is None or report not in per_report:
        return default

    years = _years(per_report[report], f"per_report[{report!r}]")
    if years < default:
        # Local import to match the rest of this module: context.py has no
        # module-level logger, and adding one here would be the only caller.
        import logging as _logging
        _logging.getLogger("retention").warning(
            "published tag retention for report %r is %d years, shorter than "
            "default_keep_years (%d). Pins for that report expire first; this "
            "is the direction that loses evidence permanently.",
            report, years, default)
    return years


def longest_tag_retention_years() -> int:
    """The longest published-tag window any report resolves to.

    What the reproducibility interlock is measured against: retention must not
    destroy the source evidence for a published run while ANY report's pin
    still survives, so the binding number is the maximum, not the default.
    """
    policy = reference_policy("published_tags")
    reports = list((policy.get("per_report") or {}))
    return max([tag_retention_years(None)]
               + [tag_retention_years(r) for r in reports])


def nessie_gc_config() -> dict[str, Any]:
    return _load("retention.yml").get("nessie_gc", {})


def gc_window_hours(key: str, default: int) -> int:
    """An hours-valued nessie_gc policy, scalar or per-environment map.

    `deferred_delete_after_hours` is per-environment because it expresses how
    long a human needs to notice, and a laptop where a day of pipeline runs in
    ten minutes is not prod. A bare number is accepted too, for a deployment
    that does not care. Lives here rather than in retention.py so the watchdog
    can read the same value without importing the thing it monitors.
    """
    value = nessie_gc_config().get(key)
    if value is None:
        return default
    if isinstance(value, dict):
        if ENV not in value:
            raise KeyError(
                f"retention.yml: nessie_gc.{key} has no entry for env {ENV!r} "
                f"(has {sorted(value)}). Add one, or use a bare number."
            )
        return int(value[ENV])
    return int(value)


def all_snapshot_retention_days() -> list[int]:
    """snapshot_retention_days across every layer in the current environment.

    Used by the Nessie GC cutoff interlock: a GC cutoff shorter than the
    longest snapshot retention would collect files those snapshots still
    reference. See retention.nessie_gc().
    """
    env = _load("retention.yml")["environments"][ENV]
    return [v["snapshot_retention_days"] for v in env.values()
            if isinstance(v, dict) and "snapshot_retention_days" in v]


def maintenance_config() -> dict[str, Any]:
    return _load("maintenance.yml")


# ------------------------------------------------------------------- identity
def new_run_id() -> str:
    """Short, sortable run identifier used in branch names and _batch_id."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def branch_name(purpose: str, scope: str, business_date: date, run_id: str) -> str:
    """<purpose>/<scope>/<business_date>/<run_id> — see docs/ARCHITECTURE.md."""
    return f"{purpose}/{scope}/{business_date:%Y-%m-%d}/{run_id}"


def published_tag(report: str, business_date: date, run_id: str) -> str:
    """The pin a PUBLICATION cuts: published/<report>/<bd>/<run_id>.

    THREE SEGMENTS, and the report is not optional. `retention.TAG_RE` accepts
    the two-segment shape as well, because tags cut before this existed are
    still real pins and a retention sweep must never fail to recognise
    something it might delete -- but nothing writes two segments any more.
    A publication that cannot say which report it is for is not a publication;
    it is an ingest, and that cuts `snapshot_tag` instead.
    """
    if not report or "/" in report:
        raise ValueError(
            f"published_tag needs a report name with no '/' in it, got "
            f"{report!r}. The tag is split on '/' by retention and by "
            f"monitoring, so a missing or slashed name silently reparses into "
            f"the wrong fields.")
    return f"published/{report}/{business_date:%Y-%m-%d}/{run_id}"


def snapshot_tag(feed: str, business_date: date, run_id: str) -> str:
    """The pin an INGEST cuts: snapshot/<feed>/<bd>/<run_id>.

    THIS USED TO BE CALLED A PUBLICATION and it never was one. The tag was
    `published/<bd>/<run_id>`, cut by `record_publication` at the end of every
    per-feed ingest DAG -- so "published" meant "some feed landed some rows",
    N feeds publishing one business date cut N tags that carried no feed name
    between them, and the reproducibility and evidence checks that read
    `published/` were reading ingests.

    It is still a pin worth cutting: raw is where retention deletes business
    dates, so pinning the state each ingest left is what makes that state
    addressable afterwards. It is simply a different thing from a report
    publication, kept for a different reason and for a different length of
    time -- `references.snapshot_tags` in retention.yml, not
    `references.published_tags`.
    """
    if not feed or "/" in feed:
        raise ValueError(f"snapshot_tag needs a feed name with no '/', got {feed!r}")
    return f"snapshot/{feed}/{business_date:%Y-%m-%d}/{run_id}"


# ------------------------------------------------------------- code identity
# REQ-404. What a published run has to be able to say about the code that
# produced it, and the honest answer depends on how the platform is deployed.
#
# A GIT SHA WOULD BE A LIE HERE. In this stack `./reporting_platform`, `./dbt`
# and `./scripts` are BIND-MOUNTED from the developer's working tree, so the
# code that ran is whatever was on disk at the time -- which a commit hash
# does not describe, and describes most wrongly exactly when the tree is dirty
# and somebody most needs to know. `.git` is not mounted into any container
# either, so there is nothing to read even if it were the right answer.
#
# So: the deployed identity if the deployment supplies one, and otherwise a
# CONTENT DIGEST of the code that actually ran, labelled as such. The kind is
# recorded beside the value (`code_ref_kind`) so nobody can mistake a laptop
# digest for a release tag. Same shape as `Feed.schema_version`: derived, not
# declared, because a version somebody has to remember to bump is wrong the
# first time somebody forgets.
CODE_ROOTS = ("reporting_platform", "scripts", "airflow")
PLATFORM_ROOT = Path(os.environ.get("PLATFORM_ROOT", "/opt/platform"))


def _tree_digest(roots: list[Path], suffixes: tuple[str, ...]) -> str:
    """A stable digest over the CONTENT of every matching file under `roots`.

    Sorted by path, and the path is hashed alongside the bytes, so moving a
    file changes the digest as much as editing it does. `__pycache__` is
    excluded: a .pyc is a function of the .py that is already hashed, and its
    presence depends on which processes happened to import what.
    """
    h = hashlib.sha256()
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            if "__pycache__" in path.parts:
                continue
            h.update(str(path.relative_to(root)).encode())
            h.update(b"\0")
            h.update(hashlib.sha256(path.read_bytes()).digest())
    return h.hexdigest()[:16]


def code_ref() -> tuple[str, str]:
    """(value, kind) identifying the platform code a run executed. REQ-404.

    `PLATFORM_CODE_REF` first -- an image tag or a release SHA, which is what
    a deployment knows and this process cannot work out. Otherwise a content
    digest of the mounted Python. Never None and never a guess: a run record
    that cannot name its code is a run record that cannot be reproduced from,
    and "unknown" written into the column would be indistinguishable from a
    real one six months later.
    """
    supplied = os.environ.get("PLATFORM_CODE_REF", "").strip()
    if supplied:
        return supplied, "deployed"
    roots = [PLATFORM_ROOT / r for r in CODE_ROOTS]
    return _tree_digest(roots, (".py",)), "tree-digest"


# Environments where the transformation project is deployed rather than edited,
# so a declared version is expected and a divergence from it is a fault. The
# feed console is not deployed here; `local` and `dev` are where it writes
# models and source definitions into the project, which is exactly the
# divergence `check_project_drift()` must NOT refuse on.
CONTROLLED_ENVIRONMENTS = ("uat", "prod")


def dbt_project_ref() -> str:
    """The DECLARED version of the transformation project, or "".

    The commit the deployment pipeline built from -- what an auditor resolves
    in git, and thereafter to the change record and its approval. Empty where
    nothing declared one, which is every developer machine and, deliberately,
    `dev`: the console edits the project there, so a declared version would be
    false within a day.

    NOT a substitute for `dbt_manifest_ref()`. That one is computed from the
    project on disk and answers "was this run's SQL the same SQL as that
    run's"; this one answers "which commit is that SQL supposed to be". Two
    questions, two fields, and `check_project_drift()` is what reconciles
    them.
    """
    return os.environ.get("DBT_PROJECT_REF", "").strip()


def deployment_provenance() -> dict[str, str]:
    """The identifiers the DEPLOYMENT knows and a run cannot work out.

    A CHANGE IS A DEPLOYMENT EVENT, NOT A RUN EVENT. One ticket authorises a
    version and every run executes that version until the next deployment, so
    these come from the environment the chart set, not from whoever triggered
    the build. `registry.run.change_ref` is the other thing -- a per-run
    reference for an exceptional publication, a restatement or an out-of-cycle
    rerun -- and the two must not be merged into one column: "published under
    the standing deployed version" and "published under a specific
    authorisation" are different facts.

    Empty strings rather than None, and never a guess. A value absent here is
    a deployment that did not supply one, which is a true and useful thing for
    the run record to say.
    """
    return {
        "dbt_project_ref": dbt_project_ref(),
        "deployment_change_ref": os.environ.get("DEPLOYMENT_CHANGE_REF", "").strip(),
        "deployment_pipeline_ref": os.environ.get("DEPLOYMENT_PIPELINE_REF", "").strip(),
    }


def check_project_drift() -> str:
    """Does the project on disk match the one the pipeline deployed?

    THE DECLARED VERSION SAYS WHAT SHOULD BE RUNNING; THE DIGEST SAYS WHAT IS.
    A commit id alone cannot detect that the project was modified after
    deployment -- and this platform can modify it, because the feed console
    writes `_sources.yml` and scaffolds a prepared model straight into
    DBT_PROJECT_DIR. The console is a dev tool and is not deployed above dev,
    which is precisely why a divergence in `uat` or `prod` is a fault worth
    refusing on rather than a normal Tuesday.

    The comparison is digest to digest, because a commit id and a content
    digest are different value spaces and cannot be compared. The pipeline
    computes `DBT_PROJECT_DIGEST` with THIS function's counterpart --
    `python -m reporting_platform.registry provenance` prints it -- so the two
    sides are one implementation rather than two that agree until one is
    changed.

    Returns "" when there is nothing to check (no declared digest) or when it
    matches; otherwise a description of the divergence. RAISES only in a
    controlled environment: `dev` diverges by design and must stay usable.
    """
    declared = os.environ.get("DBT_PROJECT_DIGEST", "").strip()
    if not declared:
        return ""
    actual = dbt_manifest_ref()
    if declared == actual:
        return ""
    message = (f"the transformation project on disk (digest {actual}) is not "
               f"the one this deployment declared (digest {declared}, version "
               f"{dbt_project_ref() or 'undeclared'}). Something changed the "
               f"project after it was deployed.")
    if ENV in CONTROLLED_ENVIRONMENTS:
        raise RuntimeError(
            f"{message} Publishing from it would attribute the result to a "
            f"commit that did not produce it. Deploy the change through the "
            f"pipeline rather than editing the project in place.")
    return message


def dbt_manifest_ref() -> str:
    """A digest of the dbt project as built. REQ-404, the model half.

    NOT dbt's own `target/manifest.json`. Cosmos runs ONE dbt subprocess PER
    MODEL, each overwriting that file and each carrying its own
    `invocation_id`, so there is no single manifest for a run and the field
    would record whichever task finished last. The project's source is the
    thing that actually determines what was built, and hashing it answers the
    question the manifest was being asked for: was this run's SQL the same SQL
    as that run's.
    """
    project = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt"))
    return _tree_digest([project / "models", project / "macros",
                         project / "tests"], (".sql", ".yml"))


# ---------------------------------------------------------------------- spark
def spark_session(app_name: str, ref: str = "main"):
    """Build a Spark session bound to the Nessie catalog at a given ref.

    `ref` is the Nessie branch. Ingest and dbt builds run on a working branch;
    maintenance and snapshot expiry run on main.

    THE SESSION IS A CLIENT OF THE STANDALONE CLUSTER, never local[*]. The
    caller's process is the driver; every task runs in an executor on
    `spark-worker`. See the `master` handling below for why there is no
    local fallback.
    """
    from pyspark.sql import SparkSession

    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("REPORTING_WAREHOUSE", "s3a://lakehouse/warehouse")
    nessie_uri = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")

    # No local[*] fallback, deliberately: running in-container is a config
    # error that LOOKS like success. The default below is the same address as
    # docker-compose.yml so a bare `python -m ...` still works.
    # See docs/DECISIONS.md#spark-master-no-local-fallback
    master = os.environ.get("SPARK_MASTER") or "spark://spark-master:7077"
    if master.startswith("local"):
        raise RuntimeError(
            f"SPARK_MASTER is {master!r}. This platform runs every Spark job on "
            f"the spark-master/spark-worker cluster; an in-process local session "
            f"silently bypasses it. Point SPARK_MASTER at the cluster "
            f"(spark://spark-master:7077)."
        )

    # `pyspark` here is the pip-installed runtime baked into
    # Dockerfile.airflow, and it is the DRIVER. It has NONE of the
    # Iceberg/Nessie/S3A jars that Dockerfile.spark curls into
    # spark-master/spark-worker's /opt/spark/jars, so the driver still has to
    # resolve every one of them via Ivy or the first Iceberg SQL statement
    # fails with ClassNotFoundException before it even runs.
    #
    # Keep this list even though the executors already have most of it baked
    # in. spark.jars.packages jars are shipped from the driver's file server
    # to every executor, so what the executors actually load is what is
    # resolved here -- which is why the versions must stay equal to
    # Dockerfile.spark's, and why hadoop-aws (which the Spark image does NOT
    # bake) reaches the executors at all.
    # From the environment, set once in docker-compose.yml from .env, so this
    # and `spark.jars.packages` in dbt/profiles.yml cannot drift from each
    # other or from the jars Dockerfile.spark baked into the executors. The
    # defaults repeat theirs: a process started outside compose still gets the
    # combination this stack was validated against.
    iceberg = os.environ.get("ICEBERG_VERSION", "1.6.1")
    nessie_ext = os.environ.get("NESSIE_SPARK_EXT_VERSION", "0.99.0")
    packages = ",".join([
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg}",
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg}",
        "org.projectnessie.nessie-integrations:"
        f"nessie-spark-extensions-3.5_2.12:{nessie_ext}",
        # Needed separately from iceberg-aws-bundle: reading landing CSVs via
        # spark.read.csv("s3a://...") goes through Hadoop's S3A connector,
        # not Iceberg's own S3FileIO, and Spark's official binaries don't
        # bundle hadoop-aws by default.
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", packages)
        # The driver runs in the calling container and does no task work, so
        # it needs far less heap than a local[*] session would -- but not
        # the 1g default, which is tight once Iceberg/Nessie/aws-sdk-bundle
        # classes are loaded and exercised across repeated catalog operations.
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        # spark.driver.host is left at its default: Spark advertises this
        # container's hostname, and Docker's embedded DNS resolves it from
        # spark-worker, so executors can call back. Verified live -- a task
        # scheduled on the worker returned its result to a driver advertising
        # the raw container id.
        #
        # CAP THE APP so one job cannot take the whole cluster. Standalone
        # mode gives an application every free core by default and holds them
        # until it stops; two overlapping jobs would leave the second waiting
        # forever with "Initial job has not accepted any resources" rather
        # than failing. The `lakehouse_write` pool already serialises the
        # WRITERS -- this is what keeps the read-only jobs outside that pool
        # (arrival checks, completeness, maintenance metrics) from colliding.
        # Sized against SPARK_WORKER_CORES/SPARK_WORKER_MEMORY in
        # docker-compose.yml: three concurrent applications fit.
        .config("spark.cores.max", os.environ.get("SPARK_APP_CORES", "2"))
        .config("spark.executor.cores", os.environ.get("SPARK_APP_CORES", "2"))
        .config("spark.executor.memory", os.environ.get("SPARK_APP_MEMORY", "2g"))
        .config(
            # ORDER MATTERS. Each extension injects a parser that wraps the
            # previous one, so the LAST listed ends up outermost. Iceberg's
            # `rewrite_data_files(strategy => 'sort', sort_order => ...)` checks
            # `parser instanceof ExtendedParser` against the session's active
            # parser -- with Nessie last, Nessie's parser is outermost and the
            # check fails:
            #   java.lang.IllegalStateException: Cannot parse order: parser is
            #   not an Iceberg ExtendedParser
            # which broke maintenance on every `prepared`/`reporting` table,
            # since maintenance.yml gives those layers strategy: sort.
            # Nessie first, Iceberg last.
            "spark.sql.extensions",
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", nessie_uri)
        .config(f"spark.sql.catalog.{CATALOG}.ref", ref)
        .config(f"spark.sql.catalog.{CATALOG}.authentication.type", "NONE")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()


# --------------------------------------------------------------------- nessie
class Nessie:
    """Thin wrapper over Nessie's REST API.

    Deliberately REST rather than the Spark SQL extensions for branch
    management, so branch lifecycle can be driven from Airflow tasks that do
    not need a Spark session (cheaper pods, faster failure).
    """

    def __init__(self, uri: str | None = None):
        self.uri = (uri or os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")).rstrip("/")

    def _req(self, method: str, path: str, **kwargs):
        import requests

        r = requests.request(method, f"{self.uri}{path}", timeout=30, **kwargs)
        if not r.ok:
            # requests' default raise_for_status() drops the response body,
            # which is exactly where Nessie puts the useful part (status/
            # reason/message/errorCode) -- surface it instead of a bare
            # "404 Client Error" with no context.
            raise requests.exceptions.HTTPError(
                f"{r.status_code} {r.reason} for url {r.url}: {r.text}", response=r
            )
        return r.json() if r.content else {}

    def get_reference(self, name: str) -> dict[str, Any]:
        return self._req("GET", f"/trees/{_urlquote(name, safe='')}")

    def create_branch(self, name: str, from_ref: str = "main",
                      exist_ok: bool = False) -> dict[str, Any]:
        """POST /v2/trees?name=<new>&type=BRANCH

        Per Nessie's v2 REST spec (confirmed against the live server's
        /nessie-openapi/openapi.yaml), the new reference's name/type are
        QUERY params, and the JSON body is the SOURCE reference being
        branched from (not the new branch) -- i.e. {type, name, hash} of
        `from_ref`. Getting this backwards produces a 404
        "Named reference '<new-name>' not found", since the server tries to
        resolve the body's name as the existing source ref.
        """
        if exist_ok:
            # Retries must not be poisoned by their own previous attempt. A
            # failed build deliberately leaves its branch behind for
            # inspection (see dbt_builds.keep_failed_branch), so re-running the
            # task hits 409 Conflict "already exists" and can NEVER succeed --
            # which made `retries` actively harmful rather than useless.
            # Reusing the branch is the right behaviour: it is the same run_id,
            # so it is the same logical build.
            try:
                existing = self.get_reference(name)
                log_msg = f"branch {name} already exists; reusing it"
                import logging as _logging
                _logging.getLogger("nessie").info(log_msg)
                return existing
            except Exception:
                pass
        src = self.get_reference(from_ref)["reference"]
        return self._req(
            "POST",
            "/trees",
            params={"name": name, "type": "BRANCH"},
            json={"type": src["type"], "name": src["name"], "hash": src["hash"]},
        )

    def merge(self, from_branch: str, into: str = "main",
              message: str | None = None,
              properties: dict[str, str] | None = None) -> dict[str, Any]:
        """POST /v2/trees/{branch}@{expectedHash}/history/merge

        v2 has no separate `expectedHash` body field -- the target's expected
        HEAD is pinned via `name@hash` in the path (mandatory: the server
        rejects an unpinned merge with "Expected hash must be provided").
        This only works once `into` has at least one real commit -- pinning
        at Nessie's sentinel "no ancestor" hash (a boundary marker, not an
        actual graph node) fails with "No common ancestor in parents of
        <sentinel> and <source-hash>". Callers are expected to have run a
        one-time bootstrap commit against `into` before the first merge (see
        `_bootstrap_main_if_empty` in ingest_feed.py) so this path is only
        ever hit once `into` is off the sentinel.

        `message` and `properties` become the merge commit's CommitMeta. Sent
        only when there is something to say, so the no-message call is
        byte-identical to what this posted before.

        READING THEM BACK, the properties are under **`allProperties`**, not
        `properties` -- v2 returns them multi-valued, `{"change_ref":
        ["RPT-1421"]}`. Looking for `properties` finds nothing and reads
        exactly like the server having dropped them, which is what it looked
        like here the first time. `GET /trees/main/history?fetch=ALL`.
        """
        src = self.get_reference(from_branch)["reference"]
        tgt = self.get_reference(into)["reference"]
        body: dict[str, Any] = {"fromRefName": from_branch,
                                "fromHash": src["hash"]}
        if message or properties:
            # REQ-405. The merge commit is the only place a publication can
            # say WHY it happened in the catalog itself, and it carried
            # nothing at all: Nessie synthesises "Merge <hash> into main",
            # which names two hashes and no change. `properties` is a free-
            # form string map on Nessie's CommitMeta, so the change reference
            # is queryable rather than only greppable out of the message.
            meta: dict[str, Any] = {}
            if message:
                meta["message"] = message
            if properties:
                meta["properties"] = {k: str(v) for k, v in properties.items()
                                      if v is not None}
            body["commitMeta"] = meta
        return self._req(
            "POST",
            f"/trees/{_urlquote(into, safe='')}@{tgt['hash']}/history/merge",
            json=body,
        )

    def list_entries(self, ref: str = "main") -> list[dict[str, Any]]:
        """Every content entry on a ref -- tables and namespaces.

        Paginated: Nessie answers with `hasMore` and a `token`, and a caller
        that ignores them silently sees only the first page. On a warehouse
        this size that is one page, which is exactly why it would go unnoticed
        until it was not.

        This is the cheap way to ask what the catalog holds. The alternative,
        `SHOW TABLES`, costs a SparkSession -- about 22 seconds -- and the
        watchdog runs every five minutes and imports no Spark at all.
        """
        entries: list[dict[str, Any]] = []
        params: dict[str, Any] = {}
        while True:
            page = self._req("GET", f"/trees/{_urlquote(ref, safe='')}/entries",
                             params=params).json()
            entries.extend(page.get("entries", []))
            if not page.get("hasMore"):
                return entries
            params = {"pageToken": page["token"]}

    def create_tag(self, name: str, from_ref: str = "main") -> dict[str, Any]:
        src = self.get_reference(from_ref)["reference"]
        return self._req(
            "POST",
            "/trees",
            params={"name": name, "type": "TAG"},
            json={"type": src["type"], "name": src["name"], "hash": src["hash"]},
        )

    def delete_reference(self, name: str) -> None:
        """DELETE /v2/trees/{name}@{hash}?type=...

        Like merge, the expected hash rides in the path (`name@hash`), not
        as a query param -- an `expectedHash` query param is silently
        ignored by the server.
        """
        ref = self.get_reference(name)["reference"]
        self._req("DELETE", f"/trees/{_urlquote(name, safe='')}@{ref['hash']}",
                  params={"type": ref["type"]})

    def list_entries(self, ref: str) -> list[dict[str, Any]]:
        """Every content entry on `ref`, with the content payload inlined.

        `content=true` is what makes `metadataLocation` available, which is the
        only way to learn where a table's files actually live. Without it you
        get names and ids and no way to map a table to object storage.
        """
        out, token = [], None
        enc = _urlquote(ref, safe="")
        while True:
            params: dict[str, Any] = {"content": "true"}
            if token:
                params["page-token"] = token
            page = self._req("GET", f"/trees/{enc}/entries", params=params)
            out.extend(page.get("entries", []))
            token = page.get("token")
            if not token:
                break
        return out

    def list_references(self, prefix: str = "",
                        fetch_all: bool = False) -> list[dict[str, Any]]:
        """List references, optionally with their commit metadata.

        `fetch_all=True` adds `fetch=ALL`, which is what makes each reference
        carry a `metadata.commitMetaOfHEAD.commitTime`. WITHOUT it the server
        returns only type/name/hash -- no metadata key at all. Any caller that
        wants to reason about a branch's age must pass it, or every age check
        silently sees `None` and treats every branch as arbitrarily old.
        Costs an extra lookup per reference server-side, so it is opt-in.
        """
        out, token = [], None
        while True:
            params: dict[str, Any] = {"fetch": "ALL"} if fetch_all else {}
            if token:
                params["page-token"] = token
            page = self._req("GET", "/trees", params=params)
            out.extend(page.get("references", []))
            token = page.get("token")
            if not token:
                break
        return [r for r in out if r["name"].startswith(prefix)]
