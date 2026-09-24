"""Legacy/new correlation (Phase 8, section 6/41).

Every case here is pure: `correlate()` takes two `CorrelationEvidence`
values and returns a `Correlation` or `None`, no I/O.
"""
from __future__ import annotations

from datetime import date

from reporting_platform.migration.correlate import (
    TIER_FILENAME, TIER_PRODUCER_RUN, TIER_SOURCE_HASH, CorrelationEvidence,
    correlate,
)

BD = date(2026, 9, 17)


def _evidence(**kwargs):
    base = {"feed": "fo_trade", "business_date": BD}
    base.update(kwargs)
    return CorrelationEvidence(**base)


def test_shared_producer_run_id_is_the_strongest_correlation():
    legacy = _evidence(producer_run_id="DCM-849217")
    new = _evidence(producer_run_id="DCM-849217", source_filename="different.csv")
    result = correlate(legacy, new)
    assert result is not None
    assert result.tier == TIER_PRODUCER_RUN


def test_disagreeing_producer_run_ids_refuse_rather_than_fall_back():
    legacy = _evidence(producer_run_id="DCM-1", source_filename="same.csv",
                       source_system="DCM")
    new = _evidence(producer_run_id="DCM-2", source_filename="same.csv",
                    source_system="DCM")
    assert correlate(legacy, new) is None


def test_source_hash_correlates_when_no_producer_run_id():
    legacy = _evidence(source_sha256="ABCDEF")
    new = _evidence(source_sha256="abcdef")
    result = correlate(legacy, new)
    assert result is not None
    assert result.tier == TIER_SOURCE_HASH


def test_different_hashes_refuse():
    legacy = _evidence(source_sha256="aaaa")
    new = _evidence(source_sha256="bbbb")
    assert correlate(legacy, new) is None


def test_filename_and_source_system_is_the_third_tier():
    legacy = _evidence(source_filename="positions.csv", source_system="DCM")
    new = _evidence(source_filename="positions.csv", source_system="DCM")
    result = correlate(legacy, new)
    assert result is not None
    assert result.tier == TIER_FILENAME


def test_same_filename_different_transport_source_does_not_correlate():
    """Same filename, different Transport (section 41): the source system
    is part of the identity, not just the name."""
    legacy = _evidence(source_filename="positions.csv", source_system="DCM")
    new = _evidence(source_filename="positions.csv", source_system="OTHER-DCM")
    assert correlate(legacy, new) is None


def test_business_date_only_refuses_unless_explicitly_allowed():
    legacy = _evidence()
    new = _evidence()
    assert correlate(legacy, new) is None
    result = correlate(legacy, new, allow_business_date_only=True)
    assert result is not None


def test_file_version_is_never_part_of_the_evidence_model():
    """Section 6: `_file_version`/`file_version` must never be usable as
    producer identity. `CorrelationEvidence` simply has no such field, so
    this asserts the API surface rather than a behaviour."""
    assert not hasattr(CorrelationEvidence(feed="f", business_date=BD),
                       "file_version")


def test_mismatched_feed_or_date_is_a_caller_error_not_weak_evidence():
    legacy = _evidence(feed="fo_trade")
    new = _evidence(feed="ref_rating")
    try:
        correlate(legacy, new)
    except ValueError:
        pass
    else:
        raise AssertionError("expected a refusal for a cross-feed call")
