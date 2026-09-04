"""Shared platform context: config loading, Spark session, Nessie helpers.

Everything environment-varying is read from config + env here, so no DAG or
job module contains an endpoint, a credential or a policy value.
"""
from __future__ import annotations

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
    arrival_poke_seconds: int = 60
    arrival_timeout_hours: int = 26
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
    # opts it out of the completeness check, which infers the
    # business calendar from what other feeds delivered -- a feed that does
    # not deliver daily would otherwise show every non-delivery day as a gap.
    completeness: bool = True
    # How often the feed is expected to deliver: "daily" (a business date is
    # expected whenever another feed delivered on it) or "weekly" (only that
    # each week containing business dates saw at least one delivery).
    cadence: str = "daily"
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
CONTROL_KEYS = {"pattern", "row_count"}

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
    row_count = control.get("row_count")
    if row_count is not None:
        try:
            compiled = re.compile(row_count)
        except re.error as exc:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.control.row_count` "
                f"is not a valid regex: {exc}") from exc
        if "rows" not in compiled.groupindex:
            raise ValueError(
                f"feeds.yml: feed {feed_name!r} `delivery.control.row_count` "
                f"{row_count!r} has no `(?P<rows>...)` group -- that is the "
                f"only thing normalize reads out of a match.")
        out["row_count"] = row_count
    return out


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


def retention_policy(layer: str) -> dict[str, Any]:
    return _load("retention.yml")["environments"][ENV][layer]


def reference_policy(kind: str) -> dict[str, Any]:
    return _load("retention.yml")["references"][kind]


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


def published_tag(business_date: date, run_id: str) -> str:
    return f"published/{business_date:%Y-%m-%d}/{run_id}"


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

    def merge(self, from_branch: str, into: str = "main") -> dict[str, Any]:
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
        """
        src = self.get_reference(from_branch)["reference"]
        tgt = self.get_reference(into)["reference"]
        return self._req(
            "POST",
            f"/trees/{_urlquote(into, safe='')}@{tgt['hash']}/history/merge",
            json={"fromRefName": from_branch, "fromHash": src["hash"]},
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
