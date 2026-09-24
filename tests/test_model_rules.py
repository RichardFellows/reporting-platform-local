"""Plan #24: a model review checklist, automated where possible.

THREE OF THE SIX CHECKLIST ITEMS IN `.github/pull_request_template.md` ARE A
TEXT SCAN, not a `dbt parse`/`dbt build`: whether `known_as_of()` and
`source_provenance()` are called, whether `dedupe_rank(...)` runs before or
after cleaning, and whether a numeric literal is hiding a business decision
are all questions about the SHAPE of the SQL, answerable from the file on
disk. The other three -- `insert_overwrite` only when a select returns each
COB date WHOLE, "no business thresholds in prepared" as a semantic (not
textual) property, and an SCD2 model going through `scd2_prepared` once it
exists (plan #15) -- are not: "whole" is a property of what a WHERE clause
*admits*, invisible in its shape, and the template says so rather than
pretending a grep could.

STRIP COMMENTS FIRST, or prose trips or satisfies a rule it is only
describing. Every one of the four prepared/reference models already carries a
comment saying "THE IN-FILE DEDUPE IS ON THE CLEANED KEY" right next to the
`dedupe_rank(` call it is describing -- a rule that read comments as SQL would
pass the moment someone wrote that sentence next to the OLD broken shape, and
CLAUDE.md's own quoted-error and `-- ` conventions live in these same files.
`strip_comments()` removes Jinja `{# ... #}` and SQL `-- ...` before any
pattern below ever sees the text.

WHAT (a) AND (b) ARE SCOPED TO, AND WHY: `dbt/models/prepared/` only, not
`dbt/models/**`. CLAUDE.md states the rank rule as "EVERY PREPARED MODEL
RANKS THE CLEANED KEY" -- prepared, not "everywhere `dedupe_rank` appears" --
and `tests/test_supersession.py` already scopes its own `known_as_of()`/
`dedupe_rank()` greps the same way. The one `dedupe_rank(...)` call outside
`prepared/` is `reporting/counterparty_exposure.sql`'s `delivered` CTE, and it
is a DIFFERENT, reviewed shape: it ranks the raw key inside the same CTE that
reads `source('raw', 'ref_counterparty')`, exactly the textual pattern (b)
flags elsewhere -- but the outer SELECT then `GROUP BY`s on the CLEANED key,
which collapses whatever the raw-key rank left duplicated (' B' and 'B'
surviving as two partitions) before a single row ever leaves the model. See
docs/DECISIONS.md's "counterparty_exposure read raw with the same mistake".
A text-shape rule cannot see the GROUP BY three lines later and tell "safe
because deduplicated downstream" apart from "not yet fixed"; rather than
encode that one model's exception into the general rule, this test leaves
`reporting/` out of (a) and (b) altogether, the same boundary the existing
supersession tests already drew, and (c) -- the only one of the three with no
such exception -- is the one written to scan every layer.

FOUND BY (b), AND FIXED: `qa_happy_position.sql` and
`qa_headerless_position.sql` ranked the RAW key inside `raw_rows` -- the shape
plan #13 fixed out of `fo_trade.sql` and `ref_collateral.sql`. They were
scaffolded before that fix and never regenerated. The rule's first catch; the
allowlist below is empty and stays that way.
"""
from __future__ import annotations

import pathlib
import re

from tests.support import REPO

DBT = REPO / "dbt"
PREPARED = DBT / "models" / "prepared"
ALL_MODELS = DBT / "models"


# ------------------------------------------------------------ text stripping
_JINJA_COMMENT_RE = re.compile(r"\{#.*?#\}", re.S)
_SQL_COMMENT_RE = re.compile(r"--[^\n]*")


def strip_comments(text: str) -> str:
    """Jinja `{# ... #}` and SQL `-- ...`, gone, in that order.

    Order matters only in that a `--` inside a `{# #}` block must not leak a
    dangling comment marker into the SQL either side of it once the Jinja
    comment is removed -- so the Jinja pass runs first and removes the whole
    block, `--` and all, in one piece.
    """
    return _SQL_COMMENT_RE.sub("", _JINJA_COMMENT_RE.sub("", text))


def _rel(path: pathlib.Path) -> str:
    return str(path.relative_to(REPO)).replace("\\", "/")


def _prepared_models() -> list[pathlib.Path]:
    return sorted(PREPARED.glob("*.sql"))


def _all_models() -> list[pathlib.Path]:
    return sorted(ALL_MODELS.glob("**/*.sql"))


def _text(path: pathlib.Path) -> str:
    return strip_comments(path.read_text(encoding="utf-8"))


# ------------------------------------------------- (a) known_as_of / provenance
SOURCE_RAW_RE = re.compile(r"source\(\s*['\"]raw['\"]")
KNOWN_AS_OF_RE = re.compile(r"known_as_of\s*\(")
SOURCE_PROVENANCE_RE = re.compile(r"source_provenance\s*\(")


def reads_raw(text: str) -> bool:
    return bool(SOURCE_RAW_RE.search(text))


def missing_known_as_of(text: str) -> bool:
    """True only for a model that reads raw and never filters on it."""
    return reads_raw(text) and not KNOWN_AS_OF_RE.search(text)


def missing_source_provenance(text: str) -> bool:
    return reads_raw(text) and not SOURCE_PROVENANCE_RE.search(text)


def test_every_prepared_model_that_reads_raw_calls_known_as_of():
    offenders = [_rel(p) for p in _prepared_models() if missing_known_as_of(_text(p))]
    assert not offenders, (
        f"reads source('raw', ...) with no known_as_of(): {offenders} -- an "
        f"as-of build would restate this table with everything, backwards. "
        f"See docs/DECISIONS.md#as-of-is-a-var-not-a-second-model")


def test_every_prepared_model_that_reads_raw_calls_source_provenance():
    offenders = [_rel(p) for p in _prepared_models() if missing_source_provenance(_text(p))]
    assert not offenders, (
        f"reads source('raw', ...) with no source_provenance(): {offenders} "
        f"-- its rows would carry no delivery_id, and a published run cannot "
        f"enumerate what it read. See docs/DECISIONS.md#provenance-is-added-not-backfilled")


# ------------------------------------------------- (b) the rank is on the cleaned key
DEDUPE_RANK_RE = re.compile(r"dedupe_rank\(\s*\[([^\]]*)\]")
_FROM_JOIN_RE = re.compile(r"\b(?:from|join)\s+(\w+)", re.I)


def _clean_string_re(key: str) -> re.Pattern:
    return re.compile(r"clean_string\(\s*['\"]" + re.escape(key) + r"['\"]\s*\)")


def cte_spans(text: str) -> list[tuple[str, int, int]]:
    """Top-level CTEs of a dbt model as (name, body_start, body_end).

    A DEPTH-0 SCANNER, not a strict `,\\s*name as (` chain: after `with`, it
    searches forward for the next `name as (`, consumes that CTE's body by
    paren-balance, then searches again from where it left off. Searching
    (not matching in place) is what lets it SKIP a `{{ macro(...) }}` line
    that renders into a CTE of its own at build time
    (`{{ newest_file_version(...) }}`, `{{ scd2_replay(...) }}`,
    `{{ scd2_changes(...) }}`) -- unrendered, that line holds no `name as (`
    for the header pattern to match, and no `dedupe_rank(` or `source('raw',`
    this rule would ever need to see directly, so skipping it costs nothing.
    Nested `... as (` inside a body is never mistaken for the next header,
    because the body is fully consumed by paren-depth before the next search
    begins -- the same technique tests/test_dedupe_rank.py's `_ctes` uses on
    RENDERED sql; this runs on the unrendered text instead, which is why it
    has to tolerate the macro-shaped gaps that renderer would have removed.
    """
    m = re.search(r"\bwith\b", text)
    if not m:
        return []
    pos = m.end()
    header_re = re.compile(r"(\w+)\s+as\s*\(", re.I)
    spans: list[tuple[str, int, int]] = []
    while True:
        h = header_re.search(text, pos)
        if not h:
            break
        depth, i, quote = 1, h.end(), None
        while depth and i < len(text):
            ch = text[i]
            if quote:
                if ch == quote:
                    quote = None
            elif ch in "'\"":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        spans.append((h.group(1), h.end(), i - 1))
        pos = i
    return spans


def _parse_keys(raw_list: str) -> list[str]:
    return [k.strip().strip("'\"") for k in raw_list.split(",") if k.strip()]


def _reachable_text(start: str, bodies: dict[str, str], deps: dict[str, set[str]]) -> str:
    """Every CTE body reachable from `start` (inclusive), concatenated.

    Enough to answer "was this key cleaned somewhere this CTE reads from",
    not to reconstruct the query -- a DFS over the `from`/`join` references
    between top-level CTE names.
    """
    seen: set[str] = set()
    stack = [start]
    out = []
    while stack:
        name = stack.pop()
        if name in seen or name not in bodies:
            continue
        seen.add(name)
        out.append(bodies[name])
        stack.extend(deps.get(name, ()))
    return "\n".join(out)


def rank_shape_issues(text: str) -> list[str]:
    """One string per `dedupe_rank(...)` call ranked before cleaning.

    A call is a violation if EITHER holds:
      * its own enclosing CTE reads `source('raw', ...)` directly -- the
        pre-#13 shape, ranking the column the feed sent rather than the
        column `clean_string()` produced; or
      * one of its keys has no `clean_string('<key>')` projection anywhere
        in the CTEs that enclosing CTE reads from (including itself),
        i.e. the model never actually cleaned that key before ranking it.

    Empty when every call ranks a cleaned key -- see fo_trade.sql,
    ref_collateral.sql, ref_counterparty.sql, ref_rating.sql and
    reporting_platform/ui/scaffold.py's `render_model`.
    """
    spans = cte_spans(text)
    bodies = {name: text[s:e] for name, s, e in spans}
    names = set(bodies)
    deps = {name: {n for n in _FROM_JOIN_RE.findall(body) if n in names and n != name}
            for name, body in bodies.items()}

    issues = []
    for name, start, end in spans:
        body = bodies[name]
        for m in DEDUPE_RANK_RE.finditer(body):
            keys = _parse_keys(m.group(1))
            if not keys:
                continue
            ranks_raw_directly = bool(SOURCE_RAW_RE.search(body))
            reachable = _reachable_text(name, bodies, deps)
            uncleaned = [k for k in keys if not _clean_string_re(k).search(reachable)]
            if ranks_raw_directly or uncleaned:
                issues.append(f"{name}:{keys}")
    return issues


# Found by this rule, not fixed here -- see the module docstring. Each value
# is exactly what `rank_shape_issues` emits for that file today; a fix that
# changes the shape removes the issue, which the "stale" half of the test
# below turns into a required edit here rather than a silently widening
# allowlist.
KNOWN_RANK_BEFORE_CLEANING: dict[str, list[str]] = {}


def test_the_dedupe_rank_is_on_the_cleaned_key():
    for path in _prepared_models():
        rel = _rel(path)
        issues = rank_shape_issues(_text(path))
        allowed = KNOWN_RANK_BEFORE_CLEANING.get(rel, [])
        unexpected = [i for i in issues if i not in allowed]
        assert not unexpected, (
            f"{rel}: dedupe_rank ranked before cleaning: {unexpected} -- see "
            f"CLAUDE.md's \"EVERY PREPARED MODEL RANKS THE CLEANED KEY\" and "
            f"docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date")
        stale = [a for a in allowed if a not in issues]
        assert not stale, (
            f"{rel}: allowlisted rank-before-cleaning issue no longer "
            f"reproduces ({stale}) -- delete it from KNOWN_RANK_BEFORE_CLEANING")


def test_the_scaffold_ranks_on_the_cleaned_key():
    """A new feed must not be the one model with the old rank. Runs the same
    rule over `render_model()`'s output rather than a hand copy of its shape
    -- see tests/test_dedupe_rank.py::_scaffolded for the same pattern."""
    from reporting_platform.ui.registry import FeedSpec
    from reporting_platform.ui.scaffold import render_model

    spec = FeedSpec(name="t_new", description="Test feed.", source_system="t",
                    filename_pattern=r"X_(?P<cob_date>\d{8})\.csv",
                    business_key=["trade_id"], columns=["trade_id", "mtm"])
    text = render_model(spec, {"trade_id": "string", "mtm": "string"})
    issues = rank_shape_issues(strip_comments(text))
    assert not issues, issues


# ---------------------------------------------------- (c) no business threshold
NUMERIC_LITERAL_RE = re.compile(r"\b\d{5,}\b")

# DECISION: a date literal (`'20260101'`) is not special-cased. Nothing under
# dbt/models/ currently has one -- distinguishing "a date, fine" from "a
# threshold spelled as a string, not fine" would need to know the column it
# sits in, and a rule that cannot tell them apart should refuse both rather
# than wave through whatever LOOKS like a date. Pinned by
# test_numeric_literal_flags_a_quoted_date_the_same_way below: if a real date
# literal is ever needed, this is the line that decision changes.
#
# Known, justified violation, not fixed here -- plan #20 moves this into a
# seed. This allowlist entry is exactly what #20's fix deletes.
KNOWN_NUMERIC_LITERALS: dict[str, set[str]] = {
    "dbt/models/reporting/exposure_change.sql": {"1000000"},
}


def numeric_literals(text: str) -> set[str]:
    return set(NUMERIC_LITERAL_RE.findall(text))


def test_no_business_threshold_literal_outside_the_known_allowlist():
    found: dict[str, set[str]] = {}
    for path in _all_models():
        literals = numeric_literals(_text(path))
        if literals:
            found[_rel(path)] = literals

    for rel, literals in found.items():
        allowed = KNOWN_NUMERIC_LITERALS.get(rel, set())
        unexpected = literals - allowed
        assert not unexpected, (
            f"{rel}: numeric literal(s) of 5+ digits with no allowlist entry: "
            f"{unexpected} -- a business threshold belongs in config, not "
            f"hard-coded in a model. If this genuinely is not one, add a "
            f"narrow, justified entry to KNOWN_NUMERIC_LITERALS")

    for rel, allowed in KNOWN_NUMERIC_LITERALS.items():
        present = found.get(rel, set())
        stale = allowed - present
        assert not stale, (
            f"{rel}: allowlisted literal(s) no longer present ({stale}) -- "
            f"delete from KNOWN_NUMERIC_LITERALS, plan #20 is presumably done")


# ---------------------------------------------------------------- (d) probes
def test_probe_known_as_of_flags_a_raw_read_with_no_as_of_filter():
    bad = ("with raw_rows as (\n"
           "    select * from {{ source('raw', 'fo_trade') }}\n"
           ")\n"
           "select * from raw_rows")
    good = bad.replace("from {{ source('raw', 'fo_trade') }}",
                       "from {{ source('raw', 'fo_trade') }}\n"
                       "    where {{ known_as_of() }}")
    assert missing_known_as_of(strip_comments(bad))
    assert not missing_known_as_of(strip_comments(good))


def test_probe_known_as_of_ignores_a_comment_that_only_talks_about_it():
    """Prose describing the rule must not satisfy it."""
    bad = ("with raw_rows as (\n"
           "    -- calls known_as_of() below, allegedly\n"
           "    select * from {{ source('raw', 'fo_trade') }}\n"
           ")\n"
           "select * from raw_rows")
    assert missing_known_as_of(strip_comments(bad))


def test_probe_source_provenance_flags_a_raw_read_with_no_provenance():
    bad = ("with raw_rows as (\n"
           "    select * from {{ source('raw', 'fo_trade') }}\n"
           "    where {{ known_as_of() }}\n"
           ")\n"
           "select * from raw_rows")
    good = bad + "\n{{ source_provenance() }}"
    assert missing_source_provenance(strip_comments(bad))
    assert not missing_source_provenance(strip_comments(good))


def test_probe_rank_shape_flags_a_rank_on_the_raw_key():
    """The pre-#13 shape: ranked in the same CTE that reads raw directly."""
    bad = """
with raw_rows as (
    select
        *,
        {{ dedupe_rank(['trade_id']) }} as _rn
    from {{ source('raw', 'fo_trade') }}
    where {{ known_as_of() }}
),
deduped as (
    select * from raw_rows where _rn = 1
),
typed as (
    select {{ clean_string('trade_id') }} as trade_id from deduped
)
select * from typed
"""
    assert rank_shape_issues(strip_comments(bad))


def test_probe_rank_shape_flags_a_key_that_was_never_cleaned():
    """Ranked outside the raw CTE, but the key it ranks was never cleaned --
    the second, independent way this rule can fail."""
    bad = """
with raw_rows as (
    select * from {{ source('raw', 'fo_trade') }}
    where {{ known_as_of() }}
),
ranked_rows as (
    select *, {{ dedupe_rank(['trade_id']) }} as _rn from raw_rows
)
select * from ranked_rows where _rn = 1
"""
    assert rank_shape_issues(strip_comments(bad))


def test_probe_rank_shape_accepts_a_rank_on_the_cleaned_key():
    good = """
with raw_rows as (
    select * from {{ source('raw', 'fo_trade') }}
    where {{ known_as_of() }}
),
cleaned as (
    select {{ clean_string('trade_id') }} as trade_id from raw_rows
),
ranked_rows as (
    select *, {{ dedupe_rank(['trade_id']) }} as _rn from cleaned
)
select * from ranked_rows where _rn = 1
"""
    assert not rank_shape_issues(strip_comments(good))


def test_probe_rank_shape_follows_an_indirect_cte_chain():
    """ref_rating.sql's shape: the rank reads from an intermediate CTE
    (`ranked`) that itself reads from `cleaned`, not from `cleaned` directly.
    The rank must still resolve as clean."""
    good = """
with raw_rows as (
    select * from {{ source('raw', 'ref_rating') }}
    where {{ known_as_of() }}
),
cleaned as (
    select {{ clean_string('counterparty_id') }} as counterparty_id,
           upper({{ clean_string('agency') }})   as agency
    from raw_rows
),
ranked as (
    select * from cleaned
),
ranked_rows as (
    select *, {{ dedupe_rank(['counterparty_id', 'agency']) }} as _rn from ranked
)
select * from ranked_rows where _rn = 1
"""
    assert not rank_shape_issues(strip_comments(good))


def test_probe_numeric_literal_flags_a_five_digit_threshold():
    bad = "select case when x > 250000 then 'MATERIAL' else 'OK' end from t"
    assert numeric_literals(strip_comments(bad)) == {"250000"}


def test_probe_numeric_literal_accepts_short_numbers_and_ignores_comments():
    good = ("select case when x > 9999 then 'MATERIAL' else 'OK' end\n"
           "-- was 300000 before the config move\n"
           "from t")
    assert numeric_literals(strip_comments(good)) == set()


def test_numeric_literal_flags_a_quoted_date_the_same_way():
    """Pins the DECISION above: a digit run is a digit run whether or not it
    is quoted, so a hard-coded date is flagged exactly like a threshold."""
    assert numeric_literals(strip_comments("select '20260101' as cob_date")) == {"20260101"}


# ------------------------------------------------------------------ sanity
def test_every_model_file_was_actually_scanned():
    """A rule with nothing to check passes vacuously. `prepared/` alone must
    hold at least the four fixed models plus the three QA fixtures."""
    assert len(_prepared_models()) >= 7, _prepared_models()
    assert len(_all_models()) >= len(_prepared_models()) + 1, _all_models()
