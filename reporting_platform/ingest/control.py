"""How a control file is READ. One implementation, both gates.

WHAT a control file may say is fixed and small: `cob_date` and `version`
(IDENTITY, read at the legacy door by `conform`, or directly from Transport
by Phase 2 Delivery creation), `row_count` and `md5` (INTEGRITY, read on the
landing side by `normalize`). HOW it says it is not fixed at all, because
every upstream writes a different file.

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

ONE IMPLEMENTATION, MULTIPLE CALLERS, which is the whole reason this is a module
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
from reporting_platform.common.parsing import decode_bytes


def decode(feed, data: bytes, filename: str = "control") -> str:
    return decode_bytes(data, getattr(feed, "control_encoding", None) or feed.file_encoding,
                        source=filename)


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
    try:
        return _READERS[fmt["kind"]](control, fmt, text, wanted,
                                     feed_name, filename, block)
    except csv.Error as exc:
        raise ControlParseError(f"{feed_name}: {filename}: malformed control CSV: {exc}") from exc


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
                f"does not say what it was configured to say. Check the sample "
                f"and mapping; use (?m) for line anchors in multiline text. "
                f"This is not a timing problem.")
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
    rows = [r for r in csv.reader(io.StringIO(text, newline=""),
                                  delimiter=fmt["delimiter"],
                                  quotechar=fmt["quote_char"], strict=True)
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
def _read_key_value(control, fmt, text, wanted, feed_name, filename, block):
    values = {}
    separator = fmt["separator"]
    for line in text.splitlines():
        if not line.strip():
            continue
        key, found, value = line.partition(separator)
        key, value = key.strip(), value.strip()
        if not found or not key or key in values:
            raise ControlParseError(f"{feed_name}: {filename}: malformed or duplicate key {key!r}")
        values[key] = value
    out = {}
    for field in wanted:
        key = control[field]
        if key not in values or not values[key]:
            raise ControlParseError(f"{feed_name}: {filename}: {block}.{field} key {key!r} is missing or empty")
        out[field] = values[key]
    return out


def value(field: str, raw: str):
    """Validate extracted values without making an integrity comparison."""
    from datetime import datetime
    if not isinstance(raw, str):
        raise ValueError(f"{field}: the named capture did not participate in the match")
    if field in ("row_count", "version"):
        if not re.fullmatch(r"\d+", raw):
            raise ValueError(f"{field} {raw!r} is not a non-negative integer")
        return int(raw)
    if field == "md5":
        if not re.fullmatch(r"[0-9a-fA-F]{32}", raw):
            raise ValueError(f"md5 {raw!r} must have 32 hexadecimal characters")
        return raw.lower()
    if field == "cob_date":
        if not re.fullmatch(r"\d{8}", raw):
            raise ValueError(f"cob_date {raw!r} must be yyyyMMdd")
        return datetime.strptime(raw, "%Y%m%d").date().isoformat()
    return raw


def diagnose(control: dict, data: bytes | None, *, encoding: str,
             fields: tuple[str, ...], feed_name: str, filename: str,
             block: str, observed: dict | None = None) -> dict:
    """Read real sample bytes through the runtime parser. No config is written."""
    from reporting_platform.common.parsing import DecodeError
    if data is None:
        return {"ok": False, "stage": "missing_control", "filename": filename, "fields": {}}
    try:
        text = decode_bytes(data, encoding, source=filename)
    except DecodeError as exc:
        return {"ok": False, "stage": "decoding", "error": str(exc), "fields": {}}
    results = {}
    for field in fields:
        if field not in control:
            continue
        item = {"mapping": control[field]}
        if (control.get("format") or {}).get("kind", "regex") == "regex":
            pattern = re.compile(control[field])
            matches = list(pattern.finditer(text))
            item.update(flags=pattern.flags, match_count=len(matches),
                        captures=[m.groupdict() for m in matches[:5]])
        try:
            raw = read(control, text, fields=(field,), feed_name=feed_name,
                       filename=filename, block=block)[field]
            item["extracted"] = raw
        except (ControlParseError, csv.Error) as exc:
            item.update(status="extraction", error=str(exc))
        else:
            try:
                extracted = value(field, raw)
            except ValueError as exc:
                item.update(status="invalid_value", error=str(exc))
            else:
                item.update(value=extracted, status="extracted")
                if observed is not None and field in observed:
                    item.update(observed=observed[field],
                                status="match" if extracted == observed[field] else "mismatch")
                if item.get("match_count", 1) > 1:
                    item["status"] = "ambiguous"
        results[field] = item
    return {"ok": all(v["status"] in ("match", "extracted") for v in results.values()),
            "stage": "extraction", "filename": filename, "encoding": encoding, "fields": results,
            "not_configured": [field for field in fields if field not in control]}


_READERS = {"regex": _read_regex, "delimited": _read_delimited, "key_value": _read_key_value}
