"""How a control file is READ. One implementation, both gates.

WHAT a control file may say is fixed and small: `cob_date` and `version`
(IDENTITY, read at the door by `conform`), `row_count` and `md5` (INTEGRITY,
read on the landing side by `normalize`). HOW it says it is not fixed at all,
because every upstream writes a different file.

So `control.format` names a reader and this module is the only place one
lives:

  * `regex` -- the original, and the default when no format is declared. Each
    field is a regex over the file's whole text with one named group, so
    `ROWS=(?P<rows>\\d+)` reads a key=value line and nothing here has to know
    the file has lines at all.
  * `delimited` -- the file is a small table: column names, one data row, in
    whatever delimiter the sender chose. Each field is then a COLUMN NAME.
    A regex can express this and it is a bad way to spend an afternoon:
    `SETTLE_DT` is the sixth of nine pipe-separated fields, so the pattern has
    to count the ones before it, and a column inserted upstream then reads the
    wrong value rather than failing.

ONE IMPLEMENTATION, TWO CALLERS, which is the whole reason this is a module
and not a function in each. The gate and the normalizer read THE SAME BYTES --
the control file is promoted into `landing/`, not consumed -- for different
fields. They had a regex loop each, identical but for the wording of the
error; a second format would have made that two implementations of one
dispatch, drifting the moment either learned something.

Nothing here decides whether a field is REQUIRED, coerces a value, or compares
a declared one against an observed one. It returns the strings the file
declares, and both callers do their own part with them: `conform` turns
`cob_date` into a date it must name a file after, `ingest_feed` compares
`row_count` against rows it counted. Splitting it anywhere else would put the
identity/integrity boundary inside a parser.
"""
from __future__ import annotations

import csv
import io
import re
from typing import Any

from reporting_platform.common.context import CONTROL_FIELD_GROUPS


class ControlParseError(ValueError):
    """The control file arrived and does not say what it was configured to say.

    A format change upstream, not a timing problem: it will not clear on its
    own, so the delivery is refused rather than held. `normalize` has always
    raised `ValueError` here and `conform` wraps this in `ConformanceError`,
    so subclassing keeps both callers' contracts exactly as they were.
    """


def read(control: dict[str, Any], text: str, *, fields: tuple[str, ...],
         feed_name: str, filename: str, block: str) -> dict[str, str]:
    """The raw strings `fields` name, as this control file declares them.

    `control` is a resolved `arrival.control` / `delivery.control` block and
    `block` is that key's name, so the error names the line to go and fix.
    A field the block does not set is absent from the result rather than
    empty: "the sender did not say" and "the sender said zero" are different
    facts, and only the caller knows which of them is allowed.

    Values come back UNCOERCED. What `'20260801'` means is the caller's
    business; this only finds it.
    """
    fmt = control.get("format") or {"kind": "regex"}
    wanted = tuple(k for k in fields if control.get(k) is not None)
    if not wanted:
        return {}
    return _READERS[fmt["kind"]](control, fmt, text, wanted,
                                 feed_name, filename, block)


def _read_regex(control: dict[str, Any], fmt: dict[str, Any], text: str,
                wanted: tuple[str, ...], feed_name: str, filename: str,
                block: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for key in wanted:
        pattern = control[key]
        m = re.search(pattern, text)
        if not m:
            raise ControlParseError(
                f"{feed_name}: control file {filename} does not match "
                f"`{block}.{key}` {pattern!r}. The control file arrived and "
                f"does not say what it was configured to say -- a format "
                f"change upstream, not a timing problem, so it will not clear "
                f"on its own.")
        out[key] = m.group(CONTROL_FIELD_GROUPS[key])
    return out


def _read_delimited(control: dict[str, Any], fmt: dict[str, Any], text: str,
                    wanted: tuple[str, ...], feed_name: str, filename: str,
                    block: str) -> dict[str, str]:
    """A header row and one data row -> the named columns' values.

    Parsed with `csv`, not `str.split`, so a quoted field holding the
    delimiter is one value -- the same reason `conform.count_rows` does.

    EXACTLY ONE DATA ROW. A control file with several describes several
    deliveries, and which row belongs to this one is not guessable from the
    delivery's name; taking the first would pick silently and wrongly on the
    day it mattered.
    """
    rows = [r for r in csv.reader(io.StringIO(text),
                                  delimiter=fmt["delimiter"],
                                  quotechar=fmt["quote_char"])
            if any(cell.strip() for cell in r)]

    names = fmt.get("columns")
    if names is None:
        if not rows:
            raise ControlParseError(
                f"{feed_name}: control file {filename} is empty, so it has "
                f"neither the header row `{block}.format` expects nor a row "
                f"of values under it.")
        names = [cell.strip() for cell in rows.pop(0)]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ControlParseError(
                f"{feed_name}: control file {filename} has more than one "
                f"column called {', '.join(dupes)}. Which one a field names "
                f"is not decidable, so the file is refused rather than read "
                f"from whichever happens to come last.")
    if len(rows) != 1:
        raise ControlParseError(
            f"{feed_name}: control file {filename} holds {len(rows)} row(s) "
            f"of values; `{block}.format` reads exactly one. A control file "
            f"describing several deliveries at once is a different shape and "
            f"is not built -- which row belongs to this delivery is not "
            f"guessable from its name.")

    values = rows[0]
    index = {name: i for i, name in enumerate(names)}
    out: dict[str, str] = {}
    for key in wanted:
        column = control[key]
        if column not in index:
            raise ControlParseError(
                f"{feed_name}: control file {filename} has no column "
                f"{column!r}, which `{block}.{key}` names. Its columns are: "
                f"{', '.join(names) if names else '(none)'}. Either the "
                f"sender renamed one, or the delimiter is wrong -- a "
                f"pipe-separated file read as commas is one column whose "
                f"name is the entire header line.")
        position = index[column]
        if position >= len(values):
            raise ControlParseError(
                f"{feed_name}: control file {filename} names {len(names)} "
                f"column(s) and its row holds {len(values)} value(s), so "
                f"{column!r} -- which `{block}.{key}` names -- has none.")
        value = values[position].strip()
        if not value:
            raise ControlParseError(
                f"{feed_name}: control file {filename} has column "
                f"{column!r} -- which `{block}.{key}` names -- and it is "
                f"empty. The sender writing nothing there is not the sender "
                f"saying zero, and only one of those is something the "
                f"delivery can be checked against.")
        out[key] = value
    return out


# Every `kind` accepted by `context.resolve_control_format` reaches a reader
# here, and nothing else does: an unknown one has already failed at LOAD.
_READERS = {"regex": _read_regex, "delimited": _read_delimited}
