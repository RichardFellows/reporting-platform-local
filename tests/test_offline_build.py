"""Every external URL a Dockerfile fetches must be a build ARG.

The corporate build has egress only to an internal mirror, and a mirror
redirects a fetch by setting a build ARG's value -- it cannot rewrite a
literal URL baked into a RUN instruction, because that text is never given
back to it. So an `ARG NAME=<default>` line, whose default is today's public
value, is the only shape of URL this repo may write: `docker build
--build-arg NAME=<mirror-url>` swaps it, everything downstream reads
`${NAME}`, and nothing else changes. A URL typed straight into `curl`, `npm
install <url>` or a `COPY --from=` is a fetch the mirror cannot reach, and it
will not fail until the corporate build actually runs, on someone else's
machine, with a stack trace that says nothing about "url".

Pure string work over the Dockerfiles in the repo root. No stack, no network.
See docs/DECISIONS.md#images-build-from-a-mirror
"""
from __future__ import annotations

import pathlib
import re

from tests.support import IN_CHECKOUT, REPO, Skipped

URL_RE = re.compile(r"https?://\S+")
INSTRUCTION_RE = re.compile(r"^\s*([A-Za-z]+)\b")


def find_url_violations(text: str) -> list[tuple[int, str]]:
    """(1-based line number, line text) for every URL outside an ARG default.

    Walks physical lines, tracking which INSTRUCTION a line belongs to across
    backslash continuations -- a multi-line `RUN` is one instruction, and a
    URL on its third continuation line is exactly the case a naive
    line-by-line regex would miss. A comment line (stripped text starting
    with `#`) is never checked and never starts or extends an instruction,
    matching how the Dockerfiles here actually write comments: on lines of
    their own, never after a continuation backslash.
    """
    violations: list[tuple[int, str]] = []
    current_instruction: str | None = None
    continuing = False

    for lineno, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.strip()

        if not continuing:
            if not stripped or stripped.startswith("#"):
                continue
            match = INSTRUCTION_RE.match(raw)
            current_instruction = match.group(1).upper() if match else None
        elif stripped.startswith("#"):
            # A comment cannot appear mid-continuation in a real Dockerfile,
            # but skip rather than misattribute it to the instruction if one
            # ever does.
            continuing = False
            continue

        if URL_RE.search(stripped) and current_instruction != "ARG":
            violations.append((lineno, stripped))

        continuing = stripped.endswith("\\")

    return violations


def _dockerfiles() -> list[pathlib.Path]:
    if not IN_CHECKOUT:
        raise Skipped("no checkout here to list Dockerfile.* from -- "
                       "see tests/support.py's note on repo-text tests")
    return sorted(REPO.glob("Dockerfile.*"))


def test_no_dockerfile_fetches_a_literal_url():
    dockerfiles = _dockerfiles()
    assert dockerfiles, "no Dockerfile.* found at the repo root -- has it moved?"

    failures = []
    for path in dockerfiles:
        for lineno, line in find_url_violations(
                path.read_text(encoding="utf-8")):
            failures.append(f"{path.name}:{lineno}: {line}")

    assert not failures, (
        "URL(s) fetched outside a build ARG -- the mirror cannot redirect "
        "these:\n  " + "\n  ".join(failures))


# ------------------------------------------------------------- the scanner
# The scanner is the thing actually enforcing this, so it gets its own
# fixtures rather than trusting the repo Dockerfiles alone to exercise both
# branches every time.

def test_probe_a_run_with_a_literal_url_is_caught():
    dockerfile = 'FROM scratch\nRUN curl -o x https://example.com/x\n'
    violations = find_url_violations(dockerfile)
    assert len(violations) == 1, violations
    lineno, line = violations[0]
    assert lineno == 2
    assert "https://example.com/x" in line


def test_probe_an_arg_default_used_via_variable_is_not_caught():
    dockerfile = (
        'FROM scratch\n'
        'ARG X=https://example.com/x\n'
        'RUN curl -o x "${X}"\n'
    )
    assert find_url_violations(dockerfile) == []


def test_probe_a_multiline_run_continuation_is_caught():
    """The case a per-line regex without continuation tracking would miss."""
    dockerfile = (
        'FROM scratch\n'
        'RUN set -eux; \\\n'
        '    curl -fsSL -o x \\\n'
        '      https://example.com/deep/path.jar\n'
    )
    violations = find_url_violations(dockerfile)
    assert len(violations) == 1, violations
    assert violations[0][0] == 4
