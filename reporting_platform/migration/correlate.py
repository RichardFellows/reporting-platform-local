"""Correlating the same logical upstream delivery across legacy and new.

Section 6 of docs/MIGRATION.md is the design note this module implements.
The short version: the new platform's DeliveryID (`dlv_` + sha256 over
Transport source/id, see `ingest/delivery.py:delivery_id_for`) and whatever
identity the true legacy estate uses are DIFFERENT, DELIBERATE identity
schemes -- one opaque to the acquisition mechanism, one whatever the legacy
load process happens to key on. Nothing here ever assumes they are equal,
and nothing here uses `_file_version`/`file_version` as producer identity:
that integer is PLATFORM ordering ("1st, 2nd, 3rd delivery we saw for this
COB date"), assigned independently by each side, and two sides assigning
"2" to unrelated deliveries would silently pair the wrong ones.

Evidence is ranked strongest first, exactly as docs/MIGRATION.md lists it:

    1. shared producer/DCM execution identity (`producer_run_id`)
    2. original source content hash (`source_sha256`)
    3. original source filename + business date + source system
    4. business date alone (weakest -- flagged explicitly as such)

`CorrelationEvidence` is what EITHER side offers; `correlate` picks the
strongest evidence common to both, and refuses (returns `None`) rather than
guessing when the only evidence sharable is business date alone and the feed
has not been configured to accept that as sufficient.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date

# Ranked strongest-first. Each tier's key material is prefixed with the tier
# name, so two tiers can never coincidentally collide on the same string.
TIER_PRODUCER_RUN = "producer_run"
TIER_SOURCE_HASH = "source_hash"
TIER_FILENAME = "filename"
TIER_BUSINESS_DATE_ONLY = "business_date_only"

TIERS = (TIER_PRODUCER_RUN, TIER_SOURCE_HASH, TIER_FILENAME,
         TIER_BUSINESS_DATE_ONLY)


@dataclass(frozen=True)
class CorrelationEvidence:
    """What one side (legacy or new) can offer to identify its delivery.

    Every field but `feed`/`business_date` is optional: a real legacy
    estate's adapter may be able to supply only a filename and a business
    date, and that is an honest, weaker answer rather than a reason to raise.
    """
    feed: str
    business_date: date
    source_system: str | None = None
    source_filename: str | None = None
    source_sha256: str | None = None
    producer_run_id: str | None = None


@dataclass(frozen=True)
class Correlation:
    """The result of pairing two `CorrelationEvidence` on their strongest
    common tier. `key` is what `registry.migration_comparison.correlation_key`
    stores -- stable, deterministic, and auditable back to `tier`.
    """
    key: str
    tier: str


def _key(tier: str, feed: str, business_date: date, *parts: str) -> str:
    material = "\x1f".join([tier, feed, business_date.isoformat(), *parts])
    return f"{tier}:" + hashlib.sha1(material.encode("utf-8")).hexdigest()[:20]  # noqa: S324


def correlate(legacy: CorrelationEvidence, new: CorrelationEvidence, *,
             allow_business_date_only: bool = False) -> Correlation | None:
    """Pair `legacy` and `new` on the strongest tier both can supply.

    Returns `None` -- an EXPLICIT "cannot correlate", never a guess -- when:
      * `feed`/`business_date` disagree (these are never a source of
        correlation ambiguity: a caller comparing across feeds or dates is a
        caller bug, not a weak-evidence case); or
      * neither side offers anything stronger than business date, and the
        caller has not opted into that weaker tier explicitly.

    `allow_business_date_only` exists because for SOME feeds -- typically
    ones with exactly one delivery per business date and no other
    identifying evidence available from either side -- business date alone
    genuinely is the strongest fact obtainable, and refusing forever would
    make migration for that feed impossible to prove. It defaults to False
    so a feed silently missing better evidence is visible, not swallowed.
    """
    if legacy.feed != new.feed:
        raise ValueError(
            f"correlate() called across two feeds ({legacy.feed!r} vs "
            f"{new.feed!r}) -- that is a caller bug, not weak evidence")
    if legacy.business_date != new.business_date:
        raise ValueError(
            f"correlate() called across two business dates "
            f"({legacy.business_date} vs {new.business_date}) -- that is a "
            f"caller bug, not weak evidence")

    feed, bd = legacy.feed, legacy.business_date

    if legacy.producer_run_id and new.producer_run_id:
        if legacy.producer_run_id == new.producer_run_id:
            return Correlation(_key(TIER_PRODUCER_RUN, feed, bd,
                                    legacy.producer_run_id), TIER_PRODUCER_RUN)
        # Both sides claim a producer run id and they DISAGREE -- that is
        # not "no evidence", it is evidence the two deliveries are NOT the
        # same occurrence, and falling through to a weaker tier would risk
        # pairing them anyway on a coincidental filename/date match.
        return None

    if legacy.source_sha256 and new.source_sha256:
        if legacy.source_sha256.lower() == new.source_sha256.lower():
            return Correlation(_key(TIER_SOURCE_HASH, feed, bd,
                                    legacy.source_sha256.lower()),
                               TIER_SOURCE_HASH)
        return None

    if (legacy.source_filename and new.source_filename
            and legacy.source_system and new.source_system):
        if (legacy.source_filename == new.source_filename
                and legacy.source_system == new.source_system):
            return Correlation(_key(TIER_FILENAME, feed, bd,
                                    legacy.source_system,
                                    legacy.source_filename), TIER_FILENAME)
        return None

    if allow_business_date_only:
        return Correlation(_key(TIER_BUSINESS_DATE_ONLY, feed, bd),
                           TIER_BUSINESS_DATE_ONLY)
    return None
