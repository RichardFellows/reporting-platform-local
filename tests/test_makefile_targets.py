"""`make build`/`prepared`/`reporting` must be the SAFE names, not the unsafe
ones -- see the Makefile's own `*-on-main` note and CLAUDE.md's "Build on a
throwaway branch, never main". This is the guard that keeps that true after
the next edit: a target renamed back, or a new target added the same way the
old three were, goes straight onto Nessie `main` again with no warning
anywhere except a name nobody is required to read.

TWO SEPARATE CLAIMS, because they fail independently:

  * **No target builds on `main` unless its name says so.** "Builds on main"
    means it runs `dbt build`/`dbt run` with no `nessie_ref` var -- directly,
    or one level removed through a prerequisite or a `$(MAKE) x` call. ONE
    level, not the transitive closure: a target hiding behind two hops of
    indirection is a target hiding behind depth instead of behind a name, and
    that is a smell in the Makefile, not a case this test should paper over.
  * **Every `make <target>` a doc tells someone to run is a target that
    exists.** A renamed target leaves a dangling instruction in whichever doc
    was not updated, and it only surfaces the day someone actually types it.

Pure text over the Makefile and the docs. No stack.
"""
from __future__ import annotations

import pathlib
import re

from tests.support import REPO, repo_file

# Deliberately line-based, not a real parser: this Makefile has no pattern
# rules, no multi-target lines and no define/endef blocks, and a hand-rolled
# parser trying to cover those would be a bigger thing to trust than the file
# it is checking.
#
# `(?!=)` excludes `:=`/`?=` variable assignments -- `SELECT ?= ...` would
# otherwise never reach here (no colon touches "SELECT" itself), but `DBT :=
# ...` written with no space before the colon would, and look like a target
# called `DBT` with prerequisite `= docker compose exec ...`.
TARGET_LINE = re.compile(r"^([a-zA-Z0-9_-]+):(?!=)(.*)$")

# `$(DBT)` is this Makefile's own alias for the `dbt` binary (`DBT := docker
# compose exec -T airflow dbt`), so a recipe using the variable never spells
# the word "dbt" at all. Matching literal `dbt` too catches a recipe that
# calls it directly instead. The trailing `(?:\s|$)` is what stops "dbt
# builds" (prose in a recipe comment, plural, not the subcommand) or a
# hypothetical "dbt run-operation" from matching "build"/"run" as a prefix.
DBT_BUILD_OR_RUN = re.compile(r"(?:\$\(DBT\)|\bdbt\b)\s+(?:build|run)(?:\s|$)")

# One level of indirection: a target invoking another by name.
MAKE_CALL = re.compile(r"\$\(MAKE\)\s+([a-zA-Z0-9_-]+)")

ON_MAIN = "on-main"


def parse_makefile(text: str) -> dict[str, dict]:
    """{target: {"prereqs": [...], "recipe": "joined recipe text"}}."""
    targets: dict[str, dict] = {}
    current = None
    for line in text.splitlines():
        if line.startswith("\t"):
            # A recipe line for whichever target we last saw a header for --
            # including a `#`-led recipe COMMENT, which starts with a tab
            # like any other recipe line and is harmless to carry along: it
            # cannot accidentally satisfy DBT_BUILD_OR_RUN by being prose.
            if current is not None:
                targets[current]["recipe_lines"].append(line[1:])
            continue
        match = TARGET_LINE.match(line)
        if match:
            name, rest = match.group(1), match.group(2)
            prereqs = rest.split("##", 1)[0].split()
            targets[name] = {"prereqs": prereqs, "recipe_lines": []}
            current = name
        else:
            # A blank line, a `#`-led comment, or a variable assignment: none
            # of these continue a recipe, so a tab-led line after one of them
            # cannot be misfiled under whatever target came before it.
            current = None
    for info in targets.values():
        info["recipe"] = "\n".join(info["recipe_lines"])
    return targets


def _base_builds_on_main(info: dict) -> bool:
    """`dbt build`/`dbt run` in THIS target's own recipe, no `nessie_ref`."""
    recipe = info["recipe"]
    return bool(DBT_BUILD_OR_RUN.search(recipe)) and "nessie_ref" not in recipe


def builds_on_main(name: str, targets: dict[str, dict]) -> bool:
    """Whether `name` builds on Nessie `main` -- directly, or one level of
    `$(MAKE) x` / prerequisite indirection away from a target that does."""
    info = targets[name]
    if _base_builds_on_main(info):
        return True
    for prereq in info["prereqs"]:
        if prereq in targets and _base_builds_on_main(targets[prereq]):
            return True
    for called in MAKE_CALL.findall(info["recipe"]):
        if called in targets and _base_builds_on_main(targets[called]):
            return True
    return False


def check_no_unsafe_main_builds(text: str) -> list[str]:
    """Target names that build on main without saying so. Empty is clean."""
    targets = parse_makefile(text)
    return sorted(
        name for name in targets
        if builds_on_main(name, targets) and ON_MAIN not in name
    )


def test_probe_a_fake_unsafe_target_is_flagged():
    """The checker itself, before trusting it against the real Makefile: a
    target plainly running `dbt build` with no `nessie_ref` and no
    `on-main` in its name is exactly what (a) means by "builds on main"."""
    fake = "build:\n\t$(DBT) build $(DBT_ARGS)\n"
    assert check_no_unsafe_main_builds(fake) == ["build"]


def test_probe_on_main_naming_and_nessie_ref_both_clear_it():
    fake = (
        "build-on-main:\n\t$(DBT) build $(DBT_ARGS)\n"
        "build-branch:\n\t$(DBT) build $(DBT_ARGS) --vars \"{nessie_ref: x}\"\n"
    )
    assert check_no_unsafe_main_builds(fake) == []


def test_probe_one_level_of_make_indirection_is_resolved():
    fake = (
        "build:\n\t$(DBT) build $(DBT_ARGS)\n"
        "prepared:\n\t@$(MAKE) build\n"
    )
    assert check_no_unsafe_main_builds(fake) == ["build", "prepared"]


def test_no_target_builds_on_main_without_saying_so():
    """Plan #25's done-when: no target builds on main without `on-main` in
    its name. Run against the ORIGINAL Makefile (`git show origin/main:Makefile`)
    this fails, naming `build`/`prepared`/`reporting` -- that is the bug this
    plan item fixes, not a false alarm from this test."""
    path = repo_file("Makefile")
    violations = check_no_unsafe_main_builds(path.read_text())
    assert not violations, (
        "these targets run dbt build/run against Nessie main but do not say "
        f"'on-main' in their name: {violations}")


# ---------------------------------------------------------------------------
# Every documented `make <target>` names a target that exists.
# ---------------------------------------------------------------------------

# Inline code, `` `make X` ``. Everything up to the closing backtick, because
# a doc writes both a bare target (`` `make pools` ``) and one with an
# argument (`` `make build-branch SELECT=<sel>` ``).
BACKTICK_MAKE = re.compile(r"`make\s+([^`]+)`")

# A fenced code block's body, so a shell line inside it that was never put in
# backticks (README's `make up seed land pools deps build`) is still read.
FENCE = re.compile(r"```[a-zA-Z]*\n(.*?)```", re.S)

# A target-name-shaped token. Most "make X" in these docs is the English verb
# ("make retries safe", "make it work"), never inside backticks or a code
# fence -- so this is applied ONLY to text already inside one of those, never
# to prose, and prose is what would otherwise flood this with tokens like
# "retries" or "it" that are not targets and were never meant to be read as
# one.
NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


def _invoked_targets(after_make: str) -> list[str]:
    """The leading run of target-shaped tokens in "make <after_make>".

    Stops at the first token that is not one -- a flag (`-n`), a `KEY=value`
    (`SELECT=<sel>`), or a placeholder (`<target>`) -- which is what makes
    `make -n <target>` resolve to zero targets (nothing to check) and
    `make build-branch SELECT=<sel>` resolve to exactly one (`build-branch`,
    not `SELECT=<sel>`) without special-casing either shape.
    """
    names = []
    for token in after_make.split():
        if NAME.match(token):
            names.append(token)
        else:
            break
    return names


def _documented_make_invocations(text: str) -> list[str]:
    found: list[str] = []
    for m in BACKTICK_MAKE.finditer(text):
        found += _invoked_targets(m.group(1))
    for block in FENCE.findall(text):
        for line in block.splitlines():
            stripped = line.strip()
            if stripped.startswith("make "):
                found += _invoked_targets(stripped[len("make "):])
    return found


def _docs() -> list[pathlib.Path]:
    """README.md, CLAUDE.md, and every doc under docs/ -- recursive, because
    docs/todo/ holds markdown too."""
    return (
        [repo_file("README.md"), repo_file("CLAUDE.md")]
        + sorted(repo_file("docs").glob("**/*.md"))
    )


def test_probe_invocation_extraction_stops_at_the_first_non_target_token():
    assert _invoked_targets("-n <target>") == []
    assert _invoked_targets("build-branch SELECT=<sel>") == ["build-branch"]
    assert _invoked_targets("up seed land pools deps build") == [
        "up", "seed", "land", "pools", "deps", "build"]


def test_every_documented_make_target_exists():
    targets = set(parse_makefile(repo_file("Makefile").read_text()))
    missing: list[str] = []
    for doc in _docs():
        for name in _documented_make_invocations(doc.read_text(encoding="utf-8")):
            if name not in targets:
                missing.append(f"{doc.relative_to(REPO)}: make {name}")
    assert not missing, missing
