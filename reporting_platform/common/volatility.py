"""Deterministic slowly-changing values for generated sample data.

ONE COPY, imported by both generators: `scripts/generate_feeds.py` (the seed
for the four built-in feeds) and `reporting_platform/ui/sampledata.py` (the
feed console's "generate a delivery"). They had the same defect independently
and would have been fixed the same way twice, which is how two copies start.

THE DEFECT. Both drew every attribute from a stream keyed on the business
date, so every value changed on every delivery. `prepared.primary_limits` held
5,755 rows expressing 5,755 distinct versions -- LIM00001IS carried 35
different amounts across its 35 delivered dates -- and `prepared.trade` held
16,400 rows with 16,400 distinct trade_ids, a book in which no trade ever
appeared twice. Nothing in the pipeline was wrong. The upstream was simply
being simulated as maximally volatile, which is the opposite of what reference
data does.

That mattered beyond realism: it made two questions the platform exists to
answer unanswerable, because the answer measured the generator rather than the
design. How much of the warehouse is unchanged restatement? Would
slowly-changing-dimension storage pay for itself? On the old seed the honest
answer to both was "cannot tell from here".

THE FIX. Make a value a function of (entity, EPOCH) rather than
(entity, date). An epoch is a block of days an attribute holds still for;
`epoch()` numbers the blocks and `stable_rng()` draws the value from the block
number, so the value is identical on every date inside a block and changes when
the block does.

Two properties are load-bearing:

  * A PURE FUNCTION OF THE DATE. Seeds are generated with month-ends for the
    early history and every business day for the tail (`--dense-days`), so a
    random walk over "yesterday's value" would give different answers depending
    on which dates were emitted, and the sparse history would drift from the
    dense one. Blocking the calendar avoids that: `bd` alone decides the epoch.
    Verified -- the same business date generated with `--dense-days 25` and
    `--dense-days 5` produces byte-identical rows.

  * INDEPENDENT OF CALL ORDER. A module-level `random` means the numbers any
    generator gets depend on how many draws came before it, so adding a feed
    silently rewrites every other feed's history. Seeding from the entity key
    removes that hazard rather than working around it.

Each entity gets its own phase offset, so entities do not all change on the
same day -- an upstream where every counterparty is restated simultaneously
would be its own kind of unrealistic.
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
# Deliberately long: a feed the console generates three days of data for should
# come out looking like reference data -- stable -- rather than like a market
# feed. `scripts/generate_feeds.py` overrides these per attribute, because for
# the four built-in feeds we know what each column actually is.
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
