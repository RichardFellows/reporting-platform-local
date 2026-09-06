"""Render a filename that a feed's own `filename_pattern` will match.

THE INVERSE OF `Feed.parse_filename`, and it lives next to it for a reason:
two things now need to BUILD a conformant name rather than only read one.

  * the feed console, generating sample deliveries;
  * the inbox gate, which is the point of this module moving here. A legacy
    upstream sends `positions.csv`; `landing/` only accepts correctly named
    deliveries; so something has to turn one into the other, and the name it
    produces must be one `parse_filename` accepts or the file lands and is
    never ingested.

Building a string from a regex is not possible in general, and this does not
pretend otherwise: it walks the pattern handling only the constructs the
platform's patterns actually use -- the two named groups, escaped literals,
and an optional non-capturing group -- and refuses anything else.

**The round-trip check is the whole value.** `render_filename` does not return
a name it has not fed back through the feed's own `parse_filename` and
confirmed parses to the date asked for. That makes "the inbox renamed a file
to something landing will not match" a structural impossibility rather than
something a test has to remember to cover -- which matters because that
failure is silent: the file lands, `find_pending` never matches it, and the
feed reports nothing pending forever with no error anywhere.
"""
from __future__ import annotations

from datetime import date

from reporting_platform.common.context import Feed


class FilenameError(ValueError):
    """A `filename_pattern` no concrete filename can be built from."""


def render_filename(feed: Feed, business_date: date,
                    version: int | None = None) -> str:
    """The name this feed's delivery for `business_date` should have."""
    out: list[str] = []
    i, pattern = 0, feed.filename_pattern
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":                       # escaped literal: \. \_ \-
            if i + 1 >= len(pattern):
                raise FilenameError("filename_pattern ends in a backslash")
            out.append(pattern[i + 1]); i += 2; continue
        if pattern.startswith("(?P<business_date>", i):
            j = _closing_paren(pattern, i)
            out.append(f"{business_date:%Y%m%d}"); i = j + 1; continue
        if pattern.startswith("(?P<version>", i):
            j = _closing_paren(pattern, i)
            out.append(str(version or 1)); i = j + 1; continue
        if pattern.startswith("(?:", i):
            j = _closing_paren(pattern, i)
            optional = j + 1 < len(pattern) and pattern[j + 1] == "?"
            inner = pattern[i + 3:j]
            if "(?P<version>" in inner and version is not None:
                # The re-delivery marker: render it only when a version was
                # asked for, so a v1 file is `FEED_20260819.csv` and not
                # `FEED_20260819_v1.csv`.
                out.append(_render_inner(inner, business_date, version))
            elif not optional:
                out.append(_render_inner(inner, business_date, version))
            i = j + (2 if optional else 1); continue
        if ch in "[]*+?{}()|^$.":
            raise FilenameError(
                f"cannot build a filename from this pattern: it uses {ch!r}, "
                f"which has no single literal form. Upload a CSV instead, or "
                f"simplify the pattern.")
        out.append(ch); i += 1

    candidate = "".join(out)
    parsed = feed.parse_filename(candidate)
    if parsed is None or parsed[0] != business_date:
        raise FilenameError(
            f"generated name {candidate!r} does not match the feed's own "
            f"pattern ({feed.filename_pattern}). Refusing to write a file that "
            f"would land and never be ingested.")
    return candidate


def _render_inner(inner: str, business_date: date, version: int | None) -> str:
    out: list[str] = []
    i = 0
    while i < len(inner):
        if inner[i] == "\\":
            out.append(inner[i + 1]); i += 2; continue
        if inner.startswith("(?P<version>", i):
            j = _closing_paren(inner, i)
            out.append(str(version or 1)); i = j + 1; continue
        if inner.startswith("(?P<business_date>", i):
            j = _closing_paren(inner, i)
            out.append(f"{business_date:%Y%m%d}"); i = j + 1; continue
        if inner[i] in "[]*+?{}()|^$.":
            raise FilenameError("unsupported construct inside an optional group")
        out.append(inner[i]); i += 1
    return "".join(out)


def _closing_paren(s: str, start: int) -> int:
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "\\":
            continue
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    raise FilenameError("unbalanced parentheses in filename_pattern")


def literal_from_regex(pattern: str, groups: dict[str, str], *,
                       what: str) -> str:
    """A concrete string that `pattern` matches, filling its named groups.

    The same trick `render_filename` plays on `filename_pattern`, needed again
    for the control file's NAME. `delivery.control.pattern` is a REGEX
    template -- `'{stem}\\.ctl'` with `{stem}` substituted is
    `'positions\\.ctl'`, where the backslash escapes the dot for the regex
    and is not a character in any real filename -- so the inbox cannot just
    format it and use the result as a key.

    The caller verifies the result against the real regex, which is the point:
    a promoted control file whose name `delivery.control` does not recognise
    would leave the delivery waiting in landing forever on a file sitting
    right beside it.
    """
    out: list[str] = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":
            if i + 1 >= len(pattern):
                raise FilenameError(f"{what} ends in a backslash")
            nxt = pattern[i + 1]
            if nxt in "dwsDWSbAZ":
                raise FilenameError(
                    f"cannot build {what} from {pattern!r}: it uses \\{nxt} "
                    f"outside a named group, which has no single literal form.")
            out.append(nxt); i += 2; continue
        if pattern.startswith("(?P<", i):
            j = _closing_paren(pattern, i)
            name = pattern[i + 4:pattern.index(">", i)]
            if name not in groups:
                raise FilenameError(
                    f"{what} captures (?P<{name}>...), which there is no value "
                    f"for. Known: {', '.join(sorted(groups)) or '(none)'}.")
            out.append(groups[name]); i = j + 1; continue
        if ch in "[]*+?{}()|^$.":
            raise FilenameError(
                f"cannot build {what} from {pattern!r}: it uses {ch!r}, which "
                f"has no single literal form.")
        out.append(ch); i += 1
    return "".join(out)
