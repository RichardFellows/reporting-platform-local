"""Generate a plausible delivery for a feed, from its own definition.

A feed defined five minutes ago has nothing to test against.
`scripts/generate_feeds.py` cannot help -- its generators are hand-written per
feed, one function each -- so today a dev has to produce a CSV by hand before
they can prove anything. This closes that: the console already knows the
columns, their scaffolding types and the filename pattern, which is enough to
emit a file the feed will actually accept.

Three things it does that a naive generator would not, each of which is the
difference between a file that tests something and one that does not:

**It generates for business dates the OTHER feeds delivered on.** A
`relationships` test compares against reference data on the *same*
business_date, so rows dated where `counterparty` has nothing are guaranteed
to fail a test that has found nothing wrong with the feed. The default date
range is therefore taken from what is already in `seed/`, not from today.

**Foreign keys are drawn from the real reference data**, read out of the other
feed's seed CSVs -- no Spark, no catalog. Random `CP#####` values would fail
`relationships` for reasons that say nothing about the feed under test.

**It varies the representations the platform exists to normalise**: dates
alternate between `yyyy-MM-dd` and `yyyyMMdd`, booleans cycle Y/N/true/1/0.
A generator emitting one clean format would leave `parse_date` and the boolean
CASE untested, which is precisely the code most likely to be wrong.

Determinism: the RNG is seeded from a CRC of (feed, date, version) -- NOT
Python's `hash()`, which is randomised per process, so the "same inputs, same
file" property would hold within one run of the console and quietly break
across restarts.
"""
from __future__ import annotations

import csv
import random
import re
import zlib
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from reporting_platform.common.volatility import (
    epoch, epoch_start, hold_for_type, stable_rng)
from reporting_platform.common.context import Feed, feeds

from .feeddata import SEED_DIR, seed_dir
from .scaffold import infer_types

CURRENCIES = ["GBP", "USD", "EUR", "JPY", "CHF"]
COUNTRIES = ["GB", "US", "DE", "FR", "JP", "SG", "CH", "NL", "IE", "AU"]
STATUSES = ["ACTIVE", "SUSPENDED", "EXPIRED"]
BOOLEANS = ["Y", "N", "true", "false", "1", "0"]


class GenerationError(ValueError):
    pass


# ------------------------------------------------------------------ filenames
def filename_for(feed: Feed, business_date: date, version: int | None = None) -> str:
    """Render a filename the feed's own pattern will match.

    Building a string from a regex is not possible in general, and this does
    not pretend otherwise: it walks the pattern handling only the constructs
    the platform's patterns actually use -- the two named groups, escaped
    literals, and an optional non-capturing group -- and gives up on anything
    else.

    THE RESULT IS THEN CHECKED with `feed.parse_filename`. That check is the
    point: a generated name that does not round-trip would produce files that
    land and are never ingested, which is the silent failure this console
    exists to prevent. Better to refuse and say so.
    """
    out: list[str] = []
    i, pattern = 0, feed.filename_pattern
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":                       # escaped literal: \. \_ \-
            if i + 1 >= len(pattern):
                raise GenerationError("filename_pattern ends in a backslash")
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
            raise GenerationError(
                f"cannot build a filename from this pattern: it uses {ch!r}, "
                f"which has no single literal form. Upload a CSV instead, or "
                f"simplify the pattern.")
        out.append(ch); i += 1

    candidate = "".join(out)
    parsed = feed.parse_filename(candidate)
    if parsed is None or parsed[0] != business_date:
        raise GenerationError(
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
            raise GenerationError("unsupported construct inside an optional group")
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
    raise GenerationError("unbalanced parentheses in filename_pattern")


# ----------------------------------------------------------------- the dates
def reference_dates(exclude: str) -> list[date]:
    """Business dates the OTHER feeds have deliveries for, oldest first.

    Read from seed filenames, so this costs a directory listing rather than a
    Spark session. Used as the default date range because data generated for
    dates no reference feed covers cannot pass a relationships test, no matter
    how correct the feed is.
    """
    found: set[date] = set()
    for name, fd in feeds().items():
        if name == exclude:
            continue
        d = seed_dir(fd)
        if not d.exists():
            continue
        for p in d.glob("*.csv"):
            parsed = fd.parse_filename(p.name)
            if parsed is not None:
                found.add(parsed[0])
    return sorted(found)


# ------------------------------------------------------------- foreign keys
def foreign_values(column: str, exclude: str, limit: int = 500) -> list[str]:
    """Real values for `<other_feed>_id`, from that feed's newest seed file.

    Returns [] when there is no such feed, no seed data, or the column is not
    that feed's business key -- the caller then generates synthetic values,
    which is the right answer for a key that references nothing.
    """
    for name, fd in feeds().items():
        if name == exclude or list(fd.business_key) != [column]:
            continue
        d = seed_dir(fd)
        if not d.exists():
            continue
        files = sorted(d.glob("*.csv"))
        if not files:
            continue
        newest = files[-1]
        with newest.open(encoding=fd.file_encoding, newline="") as fh:
            reader = csv.reader(fh, delimiter=fd.delimiter,
                                quotechar=fd.quote_char)
            header = next(reader, [])
            if column not in header:
                continue
            idx = header.index(column)
            values = []
            for row in reader:
                if len(row) > idx and row[idx].strip():
                    values.append(row[idx].strip())
                if len(values) >= limit:
                    break
            if values:
                return sorted(set(values))
    return []


# ------------------------------------------------------------------ values
def _prefix(column: str) -> str:
    """`collateral_id` -> `COL`. A readable stem for a synthetic key."""
    stem = re.sub(r"_id$", "", column)
    letters = re.sub(r"[^a-z]", "", stem.lower())
    return (letters[:3] or "row").upper()


def _enum_for(column: str, rng: random.Random) -> list[str]:
    c = column.lower()
    if "currency" in c or c.endswith("_ccy"):
        return CURRENCIES
    if "country" in c:
        return COUNTRIES
    if "status" in c:
        return STATUSES
    # An unrecognised enumeration: a small stable vocabulary derived from the
    # column name, so the values at least read as belonging to that column and
    # an accepted_values test written against them is possible.
    stem = re.sub(r"[^A-Z]", "", _prefix(column))
    return [f"{stem}_{s}" for s in ("ALPHA", "BETA", "GAMMA")]


def _value(column: str, kind: str, row: int, business_date: date,
           rng: random.Random, is_key: bool, fk: list[str]) -> str:
    if fk:
        return rng.choice(fk)
    if is_key:
        return f"{_prefix(column)}{row:05d}"
    if kind == "decimal":
        return f"{rng.uniform(1_000, 5_000_000):.2f}"
    if kind == "integer":
        return str(rng.randint(1, 500))
    if kind == "date":
        d = business_date + timedelta(days=rng.randint(-400, 400))
        # Alternate the two formats the platform's parse_date macro handles,
        # so a build actually exercises the COALESCE rather than one branch.
        return f"{d:%Y-%m-%d}" if row % 2 else f"{d:%Y%m%d}"
    if kind == "boolean":
        return BOOLEANS[row % len(BOOLEANS)]
    if kind == "upper":
        return rng.choice(_enum_for(column, rng))
    return f"{_prefix(column)}-{rng.randint(1000, 9999)}"


def _row_rng(entity: str, column: str, kind: str, business_date: date,
             version: int | None):
    """The stream one cell is drawn from: stable while its epoch is."""
    return stable_rng(entity, column, version or 1,
                      epoch(f"{entity}|{column}", business_date,
                            hold_for_type(kind)))


def _row_anchor(entity: str, column: str, kind: str, business_date: date) -> date:
    """What a generated `date` column is measured FROM.

    The business date would be the obvious anchor and is wrong: `_value` emits
    `anchor + offset`, so anchoring on `bd` slides the result forward one day
    per delivery and a date column changes every single day however stable its
    epoch is -- which quietly defeats the whole point for any feed that has
    one. Anchoring on the epoch start makes the date hold still with everything
    else and move when the value genuinely changes.

    The cost is that a column that really should track the business date (a
    valuation date, say) now does not. That is the right way round: the
    platform already records the delivery date as `_business_date`, so a feed
    needing "as of today" has it, whereas nothing can recover a stable
    effective_date from one that moves.
    """
    return epoch_start(f"{entity}|{column}", business_date, hold_for_type(kind))


# ---------------------------------------------------------------- generation
def generate(feed: Feed, *, days: int = 3, rows: int = 0,
             end: date | None = None, version: int | None = None,
             types: dict[str, str] | None = None) -> dict[str, Any]:
    """Write one CSV per business date into seed/<feed>/.

    `rows` defaults to comfortably above `expected_min_rows`, because a file
    below it aborts the ingest by design -- generating one would look like the
    generator was broken.
    """
    types = types or infer_types(list(feed.columns))
    rows = rows or max(feed.expected_min_rows * 2, 25)
    if rows < feed.expected_min_rows:
        raise GenerationError(
            f"{rows} rows is below this feed's expected_min_rows "
            f"({feed.expected_min_rows}); the ingest would abort on it.")

    available = reference_dates(exclude=feed.name)
    if end is not None:
        available = [d for d in available if d <= end]
    if not available:
        raise GenerationError(
            "no business dates found in seed/ for the other feeds, so there is "
            "no reference data to generate against. Run generate_feeds.py, or "
            "upload a CSV for this feed instead.")
    chosen = available[-days:] if days > 0 else available

    fk_cache = {c: foreign_values(c, exclude=feed.name) for c in feed.columns}
    key = list(feed.business_key)

    written = []
    target = seed_dir(feed)
    target.mkdir(parents=True, exist_ok=True)
    for bd in chosen:
        name = filename_for(feed, bd, version)
        out_rows = []
        for i in range(1, rows + 1):
            # One RNG per (row, column, EPOCH) rather than one per FILE, so a
            # value holds still for its type's hold period and changes when
            # that period rolls. `version` stays in the key so a _v2
            # redelivery is a genuine restatement.
            # See docs/DECISIONS.md#generated-data-must-hold-still
            entity = f"{feed.name}#{i:05d}"
            out_rows.append([
                _value(col, types.get(col, "string"), i,
                       _row_anchor(entity, col, types.get(col, "string"), bd),
                       _row_rng(entity, col, types.get(col, "string"), bd, version),
                       is_key=col in key, fk=fk_cache.get(col) or [])
                for col in feed.columns
            ])
        path = target / name
        with path.open("w", newline="", encoding=feed.file_encoding) as fh:
            w = csv.writer(fh, delimiter=feed.delimiter,
                           quotechar=feed.quote_char)
            w.writerow(list(feed.columns))
            w.writerows(out_rows)
        written.append({"filename": name, "business_date": bd.isoformat(),
                        "rows": rows})

    return {
        "written": written,
        "rows_each": rows,
        "dates": [w["business_date"] for w in written],
        "foreign_keys": {c: len(v) for c, v in fk_cache.items() if v},
        "seed_dir": str(target).replace(str(SEED_DIR), "seed"),
    }
