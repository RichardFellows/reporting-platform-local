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

WHAT IS DELIBERATELY NOT CHECKED HERE. Every backticked identifier in the docs
was tried as a third check and abandoned: 31 candidates, of which one
(`_promote_archive`) was genuinely stale and the rest were example feed names,
external packages, Airflow's own API and history written as history. A gate at
that signal-to-noise needs an allowlist longer than the check, and an allowlist
rots the same way the docs do. The two checks below fire only on statements
whose subject the code can be asked about by name.

No stack: the docs are files and `context.NOT_BUILT` is a dict.
"""
from __future__ import annotations

import pathlib
import re
import subprocess

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
    for chunk in re.split(r"(\n\s*\n)", path.read_text()):
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
        " ".join((REPO / f).read_text(errors="ignore").split())
        for f in files if f.endswith(".py"))

    missing = []
    for path in docs:
        for i, text in enumerate(path.read_text().split("\n"), 1):
            for m in re.finditer(r'`"([^"`]{20,240})"`', text):
                quoted = re.sub(r"^\.\.\.|\.\.\.$|[`*]", "", m.group(1))
                quoted = " ".join(quoted.split()).strip(" .")
                if quoted not in source:
                    missing.append(
                        f"{path.relative_to(REPO)}:{i} quotes a message no "
                        f"module raises: {quoted!r}")
    assert not missing, "\n".join(missing)
