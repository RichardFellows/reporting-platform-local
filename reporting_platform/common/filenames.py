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


def render_filename(feed: Feed, cob_date: date,
                    version: int | None = None) -> str:
    """The name this feed's delivery for `cob_date` should have."""
    out: list[str] = []
    i, pattern = 0, feed.filename_pattern
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":                       # escaped literal: \. \_ \-
            if i + 1 >= len(pattern):
                raise FilenameError("filename_pattern ends in a backslash")
            out.append(pattern[i + 1]); i += 2; continue
        if pattern.startswith("(?P<cob_date>", i):
            j = _closing_paren(pattern, i)
            out.append(f"{cob_date:%Y%m%d}"); i = j + 1; continue
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
                out.append(_render_inner(inner, cob_date, version))
            elif not optional:
                out.append(_render_inner(inner, cob_date, version))
            i = j + (2 if optional else 1); continue
        if ch in "[]*+?{}()|^$.":
            raise FilenameError(
                f"cannot build a filename from this pattern: it uses {ch!r}, "
                f"which has no single literal form. Upload a CSV instead, or "
                f"simplify the pattern.")
        out.append(ch); i += 1

    candidate = "".join(out)
    parsed = feed.parse_filename(candidate)
    if parsed is None or parsed[0] != cob_date:
        raise FilenameError(
            f"generated name {candidate!r} does not match the feed's own "
            f"pattern ({feed.filename_pattern}). Refusing to write a file that "
            f"would land and never be ingested.")
    return candidate


def _render_inner(inner: str, cob_date: date, version: int | None) -> str:
    out: list[str] = []
    i = 0
    while i < len(inner):
        if inner[i] == "\\":
            out.append(inner[i + 1]); i += 2; continue
        if inner.startswith("(?P<version>", i):
            j = _closing_paren(inner, i)
            out.append(str(version or 1)); i = j + 1; continue
        if inner.startswith("(?P<cob_date>", i):
            j = _closing_paren(inner, i)
            out.append(f"{cob_date:%Y%m%d}"); i = j + 1; continue
        if inner[i] in "[]*+?{}()|^$.":
            raise FilenameError("unsupported construct inside an optional group")
        out.append(inner[i]); i += 1
    return "".join(out)


def _closing_paren(s: str, start: int) -> int:
    """Index of the `)` closing the group that opens at `start`.

    THE ESCAPE SKIPS TWO CHARACTERS, not one. `\\(` is a literal parenthesis in
    the filename, and skipping only the backslash leaves the paren itself to be
    counted as structural -- so a balanced pattern either has its boundary
    found at the wrong index (a wrong name, caught later by `render_filename`'s
    round-trip check, reported as the pattern not matching its own output) or
    is rejected outright as unbalanced. `_render_inner` already advances by two
    for the same reason.
    """
    depth = 0
    i = start
    while i < len(s):
        ch = s[i]
        if ch == "\\":
            i += 2
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
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


# ===================================================== which feed's control file
# A control file's name is, by construction, its DATA file's stem plus a
# suffix: `find_control` builds it that way and these read it back. What that
# means, and it took a collision to notice, is that `{stem}` is not a wildcard
# -- it is the shape of a name the feed already declares.
#
# Substituting `.+` for it says "any file ending .ctl is mine", so two feeds
# that both send `.ctl` each claimed every control file at the door and
# `route()` refused all of them as ambiguous -- both feeds then waiting for
# ever on control files sitting in `.rejected/`. Substituting the feed's own
# data-name shape instead answers the question that was actually being asked.


def stem_pattern(data_pattern: str) -> str | None:
    """`data_pattern` minus its file extension, or None if it has no literal one.

    The extension is everything after the LAST unescaped `\\.` in the pattern,
    and it must be plain literal characters -- a pattern ending `\\.(csv|txt)`
    has no single extension, so its stems cannot be told from anything.

    Deliberately NOT done by rendering an example name and slicing it:
    `render_filename` refuses `\\d`, `{8}` and `[A-Z]`, which is right for
    building a name a feed must accept and much too strict for reading one.
    `POS_\\d{8}\\.TXT` is an ordinary legacy pattern and its stems are
    perfectly recognisable.
    """
    last = -1
    i = 0
    while i < len(data_pattern):
        if data_pattern[i] == "\\":
            if data_pattern[i + 1:i + 2] == ".":
                last = i
            i += 2
            continue
        i += 1
    if last < 0:
        return None
    extension = data_pattern[last + 2:]
    if not extension or not all(c.isalnum() or c in "_-" for c in extension):
        return None
    return data_pattern[:last]


def sample_name(pattern: str) -> str | None:
    """One concrete string the pattern matches, or None if it cannot be read.

    A REPRESENTATIVE, not a valid delivery name: nothing parses this back, so
    unlike `render_filename` it need not round-trip and can therefore handle
    the constructs that one refuses. It exists for
    `check_control_patterns_are_distinguishable`, which has to ask whether two
    feeds could ever claim the same control filename and can only answer by
    producing one.

    Handles what feed patterns actually use -- literals, escapes, character
    classes, `{n}` repetition, optional and non-capturing groups, alternation
    -- and takes the first branch of every choice. THAT IS THE LIMIT WORTH
    KNOWING: one sample per pattern, so two feeds whose names overlap only
    somewhere the sample does not land are not detected. The realistic
    collision -- two feeds with the same stem shape and the same control
    suffix -- always lands on it.
    """
    out: list[str] = []
    i = 0
    ESCAPES = {"d": "0", "w": "a", "s": "_", "D": "a", "W": "_", "S": "a"}
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":
            nxt = pattern[i + 1:i + 2]
            if not nxt:
                return None
            out.append(ESCAPES.get(nxt, nxt)); i += 2
        elif ch == "[":
            close = pattern.find("]", i)
            if close < 0:
                return None
            inner = pattern[i + 1:close].lstrip("^")
            if not inner:
                return None
            out.append(inner[0] if inner[0] != "\\" else ESCAPES.get(inner[1:2], "a"))
            i = close + 1
        elif ch == "(":
            try:
                close = _closing_paren(pattern, i)
            except FilenameError:
                return None
            inner = pattern[i + 1:close]
            for prefix in ("?P<", "?:", "?"):
                if inner.startswith(prefix):
                    inner = inner[len(prefix):]
                    if prefix == "?P<":
                        inner = inner[inner.index(">") + 1:]
                    break
            optional = pattern[close + 1:close + 2] == "?"
            piece = sample_name(inner.split("|")[0])
            if piece is None:
                return None
            # An optional group is dropped: `(?:_v(?P<version>\\d+))?` is the
            # re-delivery marker, and the FIRST delivery is the representative
            # one -- the same choice `render_filename` makes.
            if not optional:
                out.append(piece)
            i = close + (2 if optional else 1)
        elif ch == "{":
            close = pattern.find("}", i)
            if close < 0 or not out:
                return None
            count = pattern[i + 1:close].split(",")[0]
            if not count.isdigit():
                return None
            out.append(out[-1] * (int(count) - 1))
            i = close + 1
        elif ch in "?*+":
            i += 1                       # zero of the preceding piece is fine
        elif ch in ".^$|)]}":
            return None
        else:
            out.append(ch); i += 1
    return "".join(out)


def control_pattern_for(feed, block: str) -> str | None:
    """This feed's control pattern for one block, as seen AT THE DOOR.

    `arrival` is the control file the gate reads; `delivery` the one that
    gates a landed delivery. An ARCHIVE feed's `arrival.control` answers None
    here on purpose: its control files are packed inside the container and
    never arrive as loose files, so a `.ctl` sitting in the inbox is not its
    -- claiming one would attribute another feed's file, or make an
    unattributable one look attributable.
    """
    if block == "arrival":
        if (feed.arrival or {}).get("archive"):
            return None
        return ((feed.arrival or {}).get("control") or {}).get("pattern")
    return ((feed.delivery or {}).get("control") or {}).get("pattern")


def data_pattern_for(feed, block: str) -> str | None:
    """The pattern naming the DATA file a `block` control file is paired with:
    the name the upstream sends for `arrival`, the name landing holds for
    `delivery`."""
    if block == "arrival":
        return (feed.arrival or {}).get("source_pattern")
    return feed.filename_pattern


def claims_control_file(feed, filename: str, block: str) -> bool:
    """Whether `filename` is a control file THIS feed's delivery would carry.

    Two conditions, and the second is the one that was missing: the name must
    match the block's control pattern, AND the stem it matches with must be a
    stem this feed's own data names can have.

    A data pattern with no literal extension (`\\.(csv|txt)`) yields no stem
    shape, and then this falls back to matching the suffix alone -- exactly
    today's behaviour, so nothing that routes now stops routing. Two such
    feeds are refused at LOAD instead, by
    `context.check_control_patterns_are_distinguishable`, because at the door
    they genuinely cannot be told apart.
    """
    import re

    pattern = control_pattern_for(feed, block)
    if not pattern:
        return False
    m = re.fullmatch(pattern.replace("{stem}", "(?P<stem>.+)"), filename)
    if not m:
        return False
    data_pattern = data_pattern_for(feed, block)
    if not data_pattern:
        return False
    stem = stem_pattern(data_pattern)
    if stem is None:
        return True                      # no stem shape to test: suffix only
    try:
        return re.fullmatch(stem, m.group("stem")) is not None
    except re.error:                     # reported as a pattern error elsewhere
        return True


def example_control_file(feed, block: str) -> str | None:
    """One control filename this feed would claim, for the load-time check."""
    pattern = control_pattern_for(feed, block)
    data_pattern = data_pattern_for(feed, block)
    if not pattern or not data_pattern:
        return None
    stem = stem_pattern(data_pattern)
    if stem is None:
        return None
    sample = sample_name(stem)
    if sample is None:
        return None
    try:
        return literal_from_regex(pattern.format(stem=sample.replace("\\", "\\\\")),
                                  {}, what="an example control filename")
    except FilenameError:
        return None
