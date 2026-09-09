"""Deterministic slowly-changing values for generated sample data.

ONE COPY, imported by both generators: `scripts/generate_feeds.py` and
`reporting_platform/ui/sampledata.py`. They had the same defect independently
and would have been fixed the same way twice, which is how two copies start.

THE DEFECT. Both drew every attribute from a stream keyed on the COB date, so
every value changed on every delivery. A reference dimension held 5,755 rows
expressing 5,755 distinct versions, and `prepared.fo_trade` held 16,400 rows
with 16,400 distinct trade_ids -- a book in which no trade ever appeared twice.
Nothing in the pipeline was wrong; the upstream was simply being simulated as
maximally volatile, the opposite of what reference data does.

That mattered beyond realism: it made two questions the platform exists to
answer unanswerable, because the answer measured the generator rather than the
design. How much of the warehouse is unchanged restatement? Would
slowly-changing-dimension storage pay for itself?

THE FIX. Make a value a function of (entity, EPOCH) rather than (entity, date).
An epoch is a block of days an attribute holds still for; `epoch()` numbers the
blocks and `stable_rng()` draws from the block number.

Two properties are load-bearing:

  * A PURE FUNCTION OF THE DATE. Seeds use month-ends for the early history and
    every business day for the tail, so a random walk over "yesterday's value"
    would give different answers depending on which dates were emitted.
    Verified -- the same COB date generated with `--dense-days 25` and
    `--dense-days 5` produces byte-identical rows.

  * INDEPENDENT OF CALL ORDER. A module-level `random` means the numbers a
    generator gets depend on how many draws came before it, so adding a feed
    would silently rewrite every other feed's history.

Each entity gets its own phase offset, so entities do not all change on the
same day.
"""
from __future__ import annotations

import random
import zlib
from datetime import date


def stable_rng(*parts: object) -> random.Random:
    """A Random seeded from a key, not from a position in a shared stream."""
    return random.Random(zlib.crc32("|".join(str(p) for p in parts).encode()))


def phase(entity: str, period_days: int, salt: str = "") -> int:
    """This entity's offset into the epoch cycle, so changes are staggered."""
    return zlib.crc32(f"{entity}|{salt}".encode()) % max(period_days, 1)


def epoch(entity: str, bd: date, period_days: int, salt: str = "") -> int:
    """Which version of a slowly-changing attribute is in force on `bd`."""
    return (bd.toordinal() + phase(entity, period_days, salt)) // max(period_days, 1)


def epoch_start(entity: str, bd: date, period_days: int, salt: str = "") -> date:
    """The date the in-force version began -- what a `*_date` column should say.

    A `rating_date` equal to the delivery date on every row is not a rating
    date, it is the delivery date wearing the wrong name -- and it guaranteed
    every row differed from yesterday's even when the rating had not moved.
    """
    period_days = max(period_days, 1)
    ph = phase(entity, period_days, salt)
    return date.fromordinal(epoch(entity, bd, period_days, salt) * period_days - ph)


# How long a value of each scaffolded column type holds still, in days. Used by
# the feed console, which knows a column's type but nothing about its meaning.
# Deliberately long: a console-generated feed should come out looking like
# reference data rather than a market feed. `scripts/generate_feeds.py`
# overrides these per attribute for the four built-in feeds.
HOLD_BY_TYPE = {
    "decimal": 180,      # an amount reviewed a couple of times a year
    "integer": 180,
    "upper": 365,        # a code or status
    "date": 365,
    "boolean": 500,      # a flag flipping is a rare event
    "string": 365,
}


def hold_for_type(kind: str) -> int:
    return HOLD_BY_TYPE.get(kind, 365)
