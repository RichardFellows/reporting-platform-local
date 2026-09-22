"""Claims the documentation makes that the code can be asked about.

MOST OF A DOC CANNOT BE TESTED, and pretending otherwise produces a gate that
fires on prose. Two kinds of statement can be, and both are the kind that goes
false SILENTLY -- nothing fails, nobody notices, and the next reader believes
it:

  * **"X is NOT BUILT."** Building X is what makes this false, and whoever
    builds X is reading code, not `docs/DECISIONS.md`. It has happened here:
    `#console-delivery-support` claimed `control:` with `kind: archive` was
    refused at load, and quoted the refusal, for as long as it took somebody
    to implement it and update the two OTHER entries that said the same.
  * **a quoted error message.** `` `"..."` `` in these docs means "this is what
    the code says". A message the code can no longer emit is a doc describing
    behaviour that does not exist, and it reads exactly like one that does.

Two more go false the same silent way and are gated below as well:

  * **the sample prepared model in `ADDING-A-FEED.md`.** It claims to be what
    the console's scaffold emits for the `treasury_margin_call` worked
    example. If `render_model()` changes and the doc is not regenerated, the
    claim is false and the model builds green anyway.
  * **an `ingest_<x>` or `lakehouse.raw.<x>` naming a feed.** Both are a DAG
    id and a raw table name, so `<x>` must be a feed the registry has, or one
    a worked example declares in its own `name:` -- not a stale or
    never-existed feed left behind by a rename.

WHAT IS DELIBERATELY NOT CHECKED HERE. Every backticked identifier in the docs
was tried as a third check and abandoned: 31 candidates, of which one
(`_promote_archive`) was genuinely stale and the rest were example feed names,
external packages, Airflow's own API and history written as history. A gate at
that signal-to-noise needs an allowlist longer than the check, and an allowlist
rots the same way the docs do. The checks below fire only on statements whose
subject the code can be asked about by name.

No stack: the docs are files and `context.NOT_BUILT` is a dict.
"""
from __future__ import annotations

import pathlib
import re
import subprocess

from reporting_platform.ui.registry import FeedSpec
from tests.support import repo_file

REPO = pathlib.Path(__file__).resolve().parent.parent


def _docs() -> list[pathlib.Path]:
    """`CLAUDE.md` and every `docs/*.md`, or `Skipped` if they are not here.

    A FUNCTION, not a module-level list: neither the docs nor `.git` is
    mounted into the container, and `run.py` imports a module before it can
    attribute a skip to it. Resolved when a test asks, so the skip lands on
    the test that needed it.
    """
    claude = repo_file("CLAUDE.md")
    return [claude] + sorted(repo_file("docs").glob("*.md"))

# Written as history rather than as current fact. A paragraph carrying one of
# these is describing what the platform USED to refuse, which is exactly what
# `docs/DECISIONS.md`'s own preamble asks for -- so it is exempt rather than
# rewritten into the passive voice to please a test.
HISTORICAL = re.compile(
    r"\bused to\b|\bno longer\b|supersede[ds]?|was refused|was rejected|"
    r"\bamended\b|\buntil\b|outlived|before this|had this backwards", re.I)

CLAIM = re.compile(r"NOT BUILT|not built", re.I)

# `key: value` as the config spells it -- what a checkable claim names.
CONFIG_TOKEN = re.compile(r"[\w.]+: ?[\w.]+")


def _paragraphs(path: pathlib.Path):
    """(line number, one-line text) for every paragraph of a markdown file."""
    line = 1
    for chunk in re.split(r"(\n\s*\n)", path.read_text(encoding="utf-8")):
        if chunk.strip():
            yield line, " ".join(chunk.split())
        line += chunk.count("\n")


def _backticked(paragraph: str) -> set[str]:
    return {" ".join(t.split()) for t in re.findall(r"`([^`]{2,60})`", paragraph)}


def _unbuilt_tokens() -> set[str]:
    """Every spelling of "not built" the code actually maintains.

    THE CODE IS THE LIST, which is the whole reason this can be a test: both
    tables already exist so the loader can say "not built" rather than
    "unknown", and they move when somebody implements one.
    """
    from reporting_platform.common import context

    out: set[str] = set()
    for key, values in context.NOT_BUILT.items():
        for value in values:
            out |= {value, f"{key}: {value}", f"delivery.{key}: {value}"}
    for value in context.SUPERSESSION_NOT_BUILT:
        out |= {value, f"mode: {value}", f"supersession.mode: {value}"}
    return out


def test_the_unbuilt_tables_are_not_empty():
    """The check below passes vacuously if these are empty, so this asserts the
    thing it reads still exists rather than discovering it much later."""
    tokens = _unbuilt_tokens()
    assert len(tokens) >= 6, tokens
    assert "cob_date_from: member" in tokens, tokens
    assert "supersession.mode: delta_append" in tokens, tokens


def test_every_not_built_claim_names_something_still_unbuilt():
    """A doc saying "X is NOT BUILT" must name an X the code still calls that.

    Two exemptions, both principled rather than convenient: a paragraph
    written as HISTORY (see `HISTORICAL`), and one naming no config value at
    all -- prose about the mechanism ("unbuilt values raise NOT BUILT, not
    unknown") rather than a claim about a feature.
    """
    docs = _docs()
    tokens = _unbuilt_tokens()
    stale = []
    for path in docs:
        for line, para in _paragraphs(path):
            if not CLAIM.search(para) or HISTORICAL.search(para):
                continue
            named = _backticked(para)
            if named & tokens:
                continue
            if not any(CONFIG_TOKEN.fullmatch(t) for t in named):
                continue                    # about the mechanism, not a feature
            stale.append(
                f"{path.relative_to(REPO)}:{line} claims something is NOT "
                f"BUILT and names only {sorted(t for t in named if CONFIG_TOKEN.fullmatch(t))}, "
                f"none of which context.NOT_BUILT or SUPERSESSION_NOT_BUILT "
                f"still lists. Either it was built -- in which case say so, in "
                f"the past tense -- or name the value the code refuses.")
    assert not stale, "\n\n".join(stale)


def test_every_quoted_error_message_is_one_the_code_can_emit():
    """`` `"..."` `` in these docs means "this is what the code says".

    Compared against the source with whitespace flattened, because a message
    is wrapped one way in the docstring that raises it and another way in the
    paragraph quoting it. Leading/trailing ellipses are the docs' own mark for
    "quoted in part" and are stripped.
    """
    # BOTH resolved before anything is read: the docs, and the `.git` that
    # `git ls-files` needs to enumerate the source. Neither is in the
    # container, and a `git ls-files` with no repository returns an empty
    # list quietly -- which would make every quoted message "missing" and
    # this test fail for a reason that has nothing to do with the docs.
    docs = _docs()
    repo_file(".git")
    files = subprocess.run(["git", "ls-files"], cwd=REPO,
                           capture_output=True, text=True).stdout.split()
    source = " ".join(
        " ".join((REPO / f).read_text(encoding="utf-8", errors="ignore").split())
        for f in files if f.endswith(".py"))

    missing = []
    for path in docs:
        for i, text in enumerate(path.read_text(encoding="utf-8").split("\n"), 1):
            for m in re.finditer(r'`"([^"`]{20,240})"`', text):
                quoted = re.sub(r"^\.\.\.|\.\.\.$|[`*]", "", m.group(1))
                quoted = " ".join(quoted.split()).strip(" .")
                if quoted not in source:
                    missing.append(
                        f"{path.relative_to(REPO)}:{i} quotes a message no "
                        f"module raises: {quoted!r}")
    assert not missing, "\n".join(missing)


# ---------------------------------------------------- the sample prepared model
# The spec is READ from section 1 of docs/ADDING-A-FEED.md -- the
# treasury_margin_call feed's own YAML block -- not copied here, so editing
# that YAML without regenerating the sample fails too. The YAML declares no
# types (the console infers them from a sample file), so the one thing kept
# here is the type per column the sample implies.
_SAMPLE_FEED_TYPES = {
    "margin_call_id": "string",
    "counterparty_id": "string",
    "call_type": "upper",
    "call_amount": "decimal",
    "currency": "upper",
    "effective_date": "date",
    "due_date": "date",
    "status": "upper",
}


def _sample_feed_spec(text: str) -> FeedSpec:
    import yaml

    block = next(b for b in re.findall(r"```yaml\n(.*?)\n```", text, re.S)
                 if re.search(r"^name:\s*treasury_margin_call\s*$", b, re.M))
    entry = yaml.safe_load(block)
    return FeedSpec(**{k: entry[k] for k in (
        "name", "description", "source_system", "filename_pattern",
        "business_key", "columns")})


def test_the_sample_prepared_model_is_the_scaffolds_output():
    """ADDING-A-FEED.md's ```sql block must equal `render_model()`'s output.

    Hand-editing the sample is how it drifted before: the doc kept ranking
    the raw key and never called `known_as_of()`/`source_provenance()` while
    the scaffold moved on. Rendering the real template with the doc's own
    spec, rather than copying its output in by hand, is what makes this
    catch the NEXT drift too.
    """
    from reporting_platform.ui.scaffold import render_model

    path = repo_file("docs/ADDING-A-FEED.md")
    text = path.read_text(encoding="utf-8")
    blocks = re.findall(r"```sql\n(.*?)\n```", text, re.S)
    assert blocks, f"no ```sql block in {path}"
    doc_sql = blocks[0]

    spec = _sample_feed_spec(text)
    assert set(_SAMPLE_FEED_TYPES) == set(spec.columns), (
        "the doc's YAML columns changed; update _SAMPLE_FEED_TYPES and "
        "regenerate the sample")
    rendered = render_model(spec, _SAMPLE_FEED_TYPES).strip("\n")

    doc_lines = [line.rstrip() for line in doc_sql.split("\n")]
    rendered_lines = [line.rstrip() for line in rendered.split("\n")]
    assert doc_lines == rendered_lines, (
        f"{path.relative_to(REPO)}'s sample prepared model has drifted from "
        f"reporting_platform.ui.scaffold.render_model(). Regenerate it -- "
        f"see tests/test_dedupe_rank.py::_scaffolded for how.")


# --------------------------------------------- feed names in ingest_<x> / lakehouse.raw.<x>
# `ingest_<x>` is a Feed's DAG id (`airflow/dags/feed_ingest.py`) and
# `lakehouse.raw.<x>` is its raw table -- both name a REAL feed, or a stale
# rename leaves a doc pointing at a DAG that does not exist. A generic
# placeholder such as `ingest_<feed>` or `ingest_*` has nothing matching
# `[a-z0-9_]+` right after the underscore, so it is never a candidate here --
# and `transport_ingest` and friends do not start with `ingest_` at all.
FEED_NAME_TOKEN = re.compile(r"\b(ingest_[a-z0-9_]+|lakehouse\.raw\.[a-z0-9_]+)\b")

# Each of these is a real identifier this pattern also matches that is NOT a
# feed's DAG id or raw table -- named once here rather than guessed at from a
# suffix rule.
NOT_A_FEED_NAME = {
    "ingest_feed",                 # the module reporting_platform.ingest.ingest_feed (ingest_feed.py)
    "ingest_raw",                  # a task/phase name in the ingest pipeline diagrams, not a DAG id
    "ingest_normalized_delivery",  # ingest_feed.py's entry point function, not a DAG
    "ingest_added",                # a column-provenance classification value, not a DAG
    "ingest_columns",              # the function lineage/columns.py:ingest_columns, not a DAG
}

LOCAL_FEED_NAME = re.compile(r"^\s*name:\s*([a-z][a-z0-9_]*)\s*$", re.M)


def _local_feed_names(text: str) -> set[str]:
    """`name:` declared in a fenced ```yaml block -- a worked example's own feed."""
    names: set[str] = set()
    for block in re.findall(r"```yaml\n(.*?)```", text, re.S):
        names |= set(LOCAL_FEED_NAME.findall(block))
    return names


def _feed_name_misses(text: str, local_names: set[str],
                       registry_names: set[str]) -> list[tuple[int, str]]:
    """(1-based line, token) for every `ingest_<x>`/`lakehouse.raw.<x>` in
    `text` whose `<x>` is neither a registered feed nor one `local_names`
    (a worked example's own `name:`) declares.

    Factored out of the test so a probe can run it over a string with no
    file behind it -- see `test_the_feed_name_scanner_catches_a_bad_reference`.
    """
    known = local_names | registry_names
    misses: list[tuple[int, str]] = []
    for m in FEED_NAME_TOKEN.finditer(text):
        token = m.group(1)
        if token in NOT_A_FEED_NAME:
            continue
        start, end = m.span(1)
        before = text[start - 1:start]
        after_py = text[end:end + 3] == ".py"
        if before in (".", "/") or after_py:
            continue                       # a module path, e.g. scripts.ingest_feed / ingest_feed.py
        name = (token[len("lakehouse.raw."):] if token.startswith("lakehouse.raw.")
                else token[len("ingest_"):])
        if name in known:
            continue
        line = text.count("\n", 0, start) + 1
        misses.append((line, token))
    return misses


def test_every_ingest_dag_and_raw_table_named_in_the_docs_is_a_feed():
    """An `ingest_<x>` DAG id or `lakehouse.raw.<x>` table naming a feed that
    does not exist is a stale reference -- a rename that missed a doc, or a
    worked example copied from one feed and half-renamed to another."""
    from reporting_platform.common.context import feeds

    registry_names = set(feeds().keys())
    docs = _docs() + [repo_file("README.md")]

    stale = []
    for path in docs:
        text = path.read_text(encoding="utf-8")
        local_names = _local_feed_names(text)
        for line, token in _feed_name_misses(text, local_names, registry_names):
            stale.append(
                f"{path.relative_to(REPO)}:{line} references `{token}`, "
                f"which names no feed the registry has and none this doc's "
                f"own YAML declares.")
    assert not stale, "\n".join(stale)


def test_the_feed_name_scanner_catches_a_bad_reference():
    """Proves the check above can fail: a stale `ingest_margin_call` (the bug
    this plan item fixed in ADDING-A-FEED.md) against a doc that only
    declares `treasury_margin_call` must be reported."""
    misses = _feed_name_misses("trigger ingest_margin_call",
                              {"treasury_margin_call"}, set())
    assert misses == [(1, "ingest_margin_call")], misses
