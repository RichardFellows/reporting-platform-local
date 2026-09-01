"""Generate the four non-registry files a new feed needs.

docs/ADDING-A-FEED.md is six files. `registry.py` writes the first one; this
writes the rest:

  2. dbt/models/raw/_sources.yml       -- declare raw to dbt
  3. dbt/models/prepared/<feed>.sql    -- the prepared model
  4. dbt/models/prepared/_prepared.yml -- the tests
  5. common/context.py PREPARED_TABLES -- register for maintenance

Two things this module is deliberately NOT:

**It is not a template engine anyone should treat as final.** What it emits is
the skeleton the doc describes, with the macro calls that are not optional
already in place. The derivations that make a model worth reading -- the
`is_active` normalisation on counterparty, the boolean CASE on collateral --
are judgement calls about a specific feed, and the generated file says so in
a comment at the top rather than pretending otherwise.

**It is not idempotent by overwriting.** Every writer here checks whether the
feed is already present and returns `skipped` instead of replacing what is
there. Regenerating over a hand-edited model would destroy exactly the work
the previous paragraph asks for.

Step 5 edits Python source. That is done through `ast` rather than a regex:
the module is located by parsing it and the assignment's own source offsets
are used to splice, so a comment containing the string `PREPARED_TABLES`
cannot be mistaken for the assignment.
"""
from __future__ import annotations

import ast
import os
import re
from dataclasses import dataclass
from pathlib import Path

from ruamel.yaml import YAML

from .registry import FeedSpec

DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt"))
SOURCES_YML = DBT_DIR / "models" / "raw" / "_sources.yml"
PREPARED_DIR = DBT_DIR / "models" / "prepared"
PREPARED_YML = PREPARED_DIR / "_prepared.yml"
CONTEXT_PY = Path(__file__).resolve().parent.parent / "common" / "context.py"

# What a column becomes in the prepared layer. Raw is all strings by design,
# so every one of these is a cast or a normalisation applied there.
COLUMN_TYPES = ["string", "upper", "decimal", "integer", "date", "boolean"]

_DECIMAL_HINTS = ("amount", "notional", "value", "price", "balance", "exposure",
                  "mtm", "fee", "cost", "premium", "rate")
_INTEGER_HINTS = ("count", "qty", "quantity", "rank", "days", "num")
_UPPER_HINTS = ("currency", "status", "outlook")


def infer_type(column: str) -> str:
    """A first guess at a column's prepared-layer type, from its name.

    Only ever a default for the form -- the UI shows it and lets it be
    changed. Getting it wrong costs an edit; not offering it costs a
    hand-written expression for every column of every feed.

    ORDER MATTERS HERE, and it is not the order it looks like it could be.
    Substring hints have to come AFTER the suffix rules, or a name that
    contains a money word in a non-money role gets typed as money:
    `limit_type` contains "limit", `rating_date` contains "rate". Suffixes are
    the stronger signal, so they win.
    """
    c = column.lower()
    if c.startswith("is_") or c.startswith("has_"):
        return "boolean"
    if c.endswith("_date") or c == "date":
        return "date"
    if c.endswith("_id"):
        return "string"
    # Codes and enumerations: uppercased so consumers do not each write their
    # own `upper()` and disagree about it.
    if c.endswith("_code") or c.endswith("_type") or c in _UPPER_HINTS:
        return "upper"
    if any(h in c for h in _DECIMAL_HINTS):
        return "decimal"
    if any(h in c for h in _INTEGER_HINTS):
        return "integer"
    return "string"


def infer_types(columns: list[str]) -> dict[str, str]:
    return {c: infer_type(c) for c in columns}


def resolve_types(feed) -> dict[str, str]:
    """The authoritative per-column treatment: stored overrides beat the guess.

    ONE function, called by everything that needs to know what a column is --
    the API summary, the scaffold, and the sample-data generator. Calling
    `infer_types` separately gives the same answer only while nobody disagrees
    with the guess. See docs/DECISIONS.md#resolve-types-is-authoritative

    Sparse by design: `feed.column_types` holds only genuine overrides, so a
    feed nobody has corrected resolves exactly as it always did.
    """
    return {**infer_types(list(feed.columns)), **(feed.column_types or {})}


def overrides_only(columns: list[str], chosen: dict[str, str]) -> dict[str, str]:
    """Reduce a full type map to just what disagrees with the inference.

    What gets persisted. Writing all of them would put eight lines of mostly
    redundant YAML in every feed block and bury the one line that is a
    decision; writing none of them is the bug this exists to fix.
    """
    return {c: t for c in columns
            if (t := chosen.get(c)) and t != infer_type(c)}


@dataclass
class Step:
    """What one scaffolding step did, for the UI to report honestly."""
    file: str
    status: str          # "written" | "skipped" | "failed"
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status != "failed"


def _yaml() -> YAML:
    y = YAML()
    y.preserve_quotes = True
    y.indent(mapping=2, sequence=4, offset=2)
    y.width = 100
    return y


# --------------------------------------------------------- 2. dbt raw source
def write_source(spec: FeedSpec) -> Step:
    """Add the feed to dbt's `raw` source, with arrival-only tests.

    Source tests run against raw BEFORE any modelling, and raw is all strings,
    so these stay `not_null` on the business key -- a range test here would be
    a string comparison. Everything about types and values belongs in step 4.
    """
    rel = str(SOURCES_YML)
    y = _yaml()
    with SOURCES_YML.open(encoding="utf-8") as fh:
        doc = y.load(fh)

    tables = doc["sources"][0]["tables"]
    if any(t.get("name") == spec.name for t in tables):
        return Step(rel, "skipped", f"source raw.{spec.name} already declared")

    from ruamel.yaml.comments import CommentedMap, CommentedSeq

    entry = CommentedMap()
    entry["name"] = spec.name
    # Only name the source system if the description does not already -- most
    # of them end in "from SRC" and "(SRC)" after it reads like a stutter.
    body = spec.description.rstrip(".")
    if spec.source_system.lower() not in body.lower():
        body = f"{body} ({spec.source_system})"
    entry["description"] = f"{body}."
    cols = CommentedSeq()
    for key_col in spec.business_key:
        col = CommentedMap()
        col["name"] = key_col
        tests = CommentedSeq(["not_null"])
        tests.fa.set_flow_style()
        col["tests"] = tests
        cols.append(col)
    entry["columns"] = cols
    tables.append(entry)

    _dump(doc, y, SOURCES_YML)
    return Step(rel, "written", f"source raw.{spec.name} declared")


# ------------------------------------------------------- 3. the prepared model
def _select_expression(column: str, kind: str) -> str:
    """One line of the model's `cleaned` CTE.

    Never a bare CAST. `safe_cast` is TRY_CAST, so an unparseable value lands
    as NULL and fails a TEST rather than failing the LOAD -- which is the
    whole reason raw is typed as strings in the first place.
    """
    clean = f"{{{{ clean_string('{column}') }}}}"
    if kind == "upper":
        return f"upper({clean})"
    if kind == "decimal":
        # DECIMAL rather than DOUBLE: these get summed and compared, and
        # binary floating point makes those comparisons non-reproducible.
        return f"{{{{ safe_cast(clean_string('{column}'), 'DECIMAL(18,2)') }}}}"
    if kind == "integer":
        return f"{{{{ safe_cast(clean_string('{column}'), 'INT') }}}}"
    if kind == "date":
        return f"{{{{ parse_date(clean_string('{column}')) }}}}"
    if kind == "boolean":
        # Upstreams send Y/N/1/0/true/false depending on the release.
        # Normalise once, here, rather than in every consuming report.
        return (f"case\n"
                f"            when upper({clean}) in ('Y', 'YES', 'TRUE', '1') then true\n"
                f"            when upper({clean}) in ('N', 'NO', 'FALSE', '0') then false\n"
                f"            else null\n"
                f"        end")
    return clean


def render_model(spec: FeedSpec, types: dict[str, str]) -> str:
    key_list = ", ".join(f"'{c}'" for c in spec.business_key)
    unique_key = ", ".join(f"'{c}'" for c in ["business_date", *spec.business_key])
    tag = "reference" if len(spec.business_key) == 1 else "transactional"

    # Align every `as <alias>` in one column, the way the hand-written models
    # do -- computed from the widest expression rather than fixed, so a long
    # safe_cast does not push its own alias out of line with the rest.
    rendered = [(col, _select_expression(col, types.get(col, "string")))
                for col in spec.columns]
    widest = max([len("_business_date")]
                 + [len(e) for _, e in rendered if "\n" not in e])
    at = 8 + min(widest + 1, 64)

    def _line(expr: str, alias: str) -> str:
        if "\n" in expr:
            # A multi-line CASE: close it, then align the alias to the same
            # column as everything else.
            head, _, last = expr.rpartition("\n")
            return f"        {head}\n{last.ljust(at)}as {alias},"
        return f"        {expr}".ljust(at) + f"as {alias},"

    body = "\n".join([_line("_business_date", "business_date")]
                     + [_line(expr, col) for col, expr in rendered]
                     + [_line("_source_file", "source_file"),
                        _line("_file_version", "source_file_version")])

    return f"""{{{{
  config(
    materialized='incremental',
    unique_key=[{unique_key}],
    partition_by=['business_date'],
    tags=['prepared', '{tag}']
  )
}}}}

{{#
  {spec.description}

  SCAFFOLDED by the feed console from the registry entry -- conforming and
  typing only. Anything that restates what the feed already says (a validity
  flag derived from its own effective/expiry dates, a status normalisation)
  belongs here and should be added deliberately. Anything that is a report
  opinion -- utilisation, breach flags, anything needing a join -- belongs in
  `reporting`, where it can be joined to exposure.
#}}

with raw_rows as (

    select
        *,
        {{{{ dedupe_rank([{key_list}]) }}}} as _rn
    from {{{{ source('raw', '{spec.name}') }}}}
    where {{{{ incremental_window('_business_date', 'business_date') }}}}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
{body}
        {{{{ audit_columns() }}}}

    from deduped

)

select * from cleaned
"""


def write_model(spec: FeedSpec, types: dict[str, str]) -> Step:
    path = PREPARED_DIR / f"{spec.name}.sql"
    if path.exists():
        return Step(str(path), "skipped", "model already exists; left untouched")
    path.write_text(render_model(spec, types), encoding="utf-8")
    return Step(str(path), "written", "prepared model scaffolded")


# ------------------------------------------------------------- 4. the tests
def write_tests(spec: FeedSpec, existing_models: set[str]) -> Step:
    """The step that makes write-audit-publish mean anything.

    A feed with no tests builds green forever and publishes whatever it is
    given. The minimum the doc asks for is generated unconditionally:
    `not_null` on the business key, a uniqueness test over
    [business_date, <business key>], and a `relationships` test on any foreign
    key -- which is the one that turns a bad reference into a failed test and
    an unmerged branch instead of a bad published figure.
    """
    rel = str(PREPARED_YML)
    y = _yaml()
    with PREPARED_YML.open(encoding="utf-8") as fh:
        doc = y.load(fh)

    if any(m.get("name") == spec.name for m in doc["models"]):
        return Step(rel, "skipped", f"tests for {spec.name} already present")

    from ruamel.yaml.comments import CommentedMap, CommentedSeq

    def _flow(items):
        seq = CommentedSeq(items)
        seq.fa.set_flow_style()
        return seq

    entry = CommentedMap()
    entry["name"] = spec.name
    entry["description"] = spec.description
    entry.yaml_set_comment_before_after_key(
        "columns", indent=4,
        before=("SCAFFOLDED MINIMUM: not_null on the business key, uniqueness\n"
                "over [business_date, <business key>], and relationships on any\n"
                "foreign key. That is enough to prove dedupe_rank works and that\n"
                "references resolve -- it is NOT enough to prove the values are\n"
                "right. Add accepted_values / accepted_range for this feed's\n"
                "domain, or it will publish whatever it is given."))

    cols = CommentedSeq()
    bd = CommentedMap()
    bd["name"] = "business_date"
    bd["tests"] = _flow(["not_null"])
    cols.append(bd)

    for col in spec.columns:
        tests: list = []
        if col in spec.business_key:
            tests.append("not_null")
        # A foreign key to the conformed counterparty reference. Only when
        # that model actually exists -- a relationships test pointing at a
        # missing ref() fails compilation, not data quality.
        if (col == "counterparty_id" and spec.name != "counterparty"
                and "counterparty" in existing_models):
            rel_test = CommentedMap()
            body = CommentedMap()
            body["to"] = "ref('counterparty')"
            body["field"] = "counterparty_id"
            rel_test["relationships"] = body
            if "not_null" not in tests:
                tests.append("not_null")
            tests.append(rel_test)
        # NO GENERATED accepted_range OR accepted_values. Both are statements
        # about the feed's domain that only its owner can make, and guessing
        # produces the worst outcome available: `min_value: 0` on a decimal is
        # right for a notional and wrong for an MTM, and a scaffold whose
        # tests fail on correct data teaches people to ignore failing tests.
        # The generated block carries a comment saying to add them.
        if not tests:
            continue
        c = CommentedMap()
        c["name"] = col
        if all(isinstance(t, str) for t in tests):
            c["tests"] = _flow(tests)
        else:
            c["tests"] = CommentedSeq(tests)
        cols.append(c)

    entry["columns"] = cols

    combo = CommentedMap()
    inner = CommentedMap()
    inner["combination_of_columns"] = _flow(["business_date", *spec.business_key])
    combo["dbt_utils.unique_combination_of_columns"] = inner
    entry["tests"] = CommentedSeq([combo])

    doc["models"].append(entry)
    _dump(doc, y, PREPARED_YML)
    return Step(rel, "written", f"tests for {spec.name} added")


def _flow_map(d: dict):
    from ruamel.yaml.comments import CommentedMap
    m = CommentedMap(d)
    m.fa.set_flow_style()
    return m


def _dump(doc, y: YAML, path: Path) -> None:
    import io
    buf = io.StringIO()
    y.dump(doc, buf)
    path.write_text(buf.getvalue(), encoding="utf-8")


# ------------------------------------------------- 5. PREPARED_TABLES in context
def prepared_tables_source() -> tuple[str, ast.Assign]:
    src = CONTEXT_PY.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "PREPARED_TABLES"):
            return src, node
    raise RuntimeError("PREPARED_TABLES assignment not found in context.py")


def register_prepared_table(name: str) -> Step:
    """Add the model to PREPARED_TABLES.

    THE ONE STEP WITH NO ERROR IF IT IS SKIPPED. Without it the table is never
    compacted, its snapshots never expire and retention never trims it -- it
    just grows, quietly. Which is exactly why the feed console does it rather
    than reminding someone to.
    """
    src, node = prepared_tables_source()
    current = ast.literal_eval(ast.get_source_segment(src, node.value))
    if name in current:
        return Step(str(CONTEXT_PY), "skipped",
                    f"{name} already in PREPARED_TABLES")

    updated = [*current, name]
    literal = _wrap_list(updated, indent=len("PREPARED_TABLES = "))
    lines = src.splitlines(keepends=True)
    start = sum(len(x) for x in lines[:node.lineno - 1]) + node.col_offset
    end = sum(len(x) for x in lines[:node.end_lineno - 1]) + node.end_col_offset
    new_src = src[:start] + f"PREPARED_TABLES = {literal}" + src[end:]

    # Parse before writing: this is the module every other component imports,
    # and a syntax error here takes the whole platform down rather than one
    # feed.
    ast.parse(new_src)
    CONTEXT_PY.write_text(new_src, encoding="utf-8")
    return Step(str(CONTEXT_PY), "written", f"{name} added to PREPARED_TABLES")


def _wrap_list(items: list[str], indent: int, width: int = 79) -> str:
    """Render a list literal wrapped the way the surrounding file wraps them."""
    out, line = [], "["
    pad = " " * (indent + 1)
    for i, item in enumerate(items):
        piece = f'"{item}"' + ("," if i < len(items) - 1 else "")
        if len(line) + len(piece) + (indent if not out else 0) + 1 > width and line != "[":
            out.append(line.rstrip())
            line = pad + piece + " "
        else:
            line += piece + " "
    out.append(line.rstrip() + "]")
    return "\n".join(out)


# ------------------------------------------------------------------ the whole
def existing_prepared_models() -> set[str]:
    return {p.stem for p in PREPARED_DIR.glob("*.sql")}


def scaffold(spec: FeedSpec, types: dict[str, str] | None = None) -> list[Step]:
    """Run steps 2-5. Each step is independent and reports its own outcome.

    Deliberately NOT transactional. A partial scaffold is a set of ordinary
    files in the working tree that `git diff` shows and a human can finish or
    revert; a rollback that half-worked would be strictly harder to reason
    about than that.
    """
    types = types or infer_types(spec.columns)
    models = existing_prepared_models()
    steps: list[Step] = []
    for fn in (lambda: write_source(spec),
               lambda: write_model(spec, types),
               lambda: write_tests(spec, models),
               lambda: register_prepared_table(spec.name)):
        try:
            steps.append(fn())
        except Exception as exc:                       # noqa: BLE001
            steps.append(Step("?", "failed", f"{type(exc).__name__}: {exc}"))
    return steps


def status(name: str) -> dict[str, bool]:
    """Which of the four non-registry pieces exist for this feed.

    Read by the UI so a feed added by hand, or one whose scaffold half-ran,
    shows what it is actually missing rather than being assumed complete.
    """
    src_text = SOURCES_YML.read_text(encoding="utf-8")
    prep_text = PREPARED_YML.read_text(encoding="utf-8")
    ctx_src, node = prepared_tables_source()
    prepared_tables = ast.literal_eval(ast.get_source_segment(ctx_src, node.value))
    return {
        "dbt_source": bool(re.search(rf"^\s*-\s*name:\s*{re.escape(name)}\s*$",
                                     src_text, re.M)),
        "prepared_model": (PREPARED_DIR / f"{name}.sql").exists(),
        "prepared_tests": bool(re.search(rf"^\s*-\s*name:\s*{re.escape(name)}\s*$",
                                         prep_text, re.M)),
        "maintained": name in prepared_tables,
    }
