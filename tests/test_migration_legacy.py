"""The local fixture legacy adapter (Phase 8, section 25)."""
from __future__ import annotations

import json
import pathlib
import tempfile
from datetime import date

from reporting_platform.migration.legacy import LocalFixtureLegacySource


def _fixtures_dir(fixture: dict) -> pathlib.Path:
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-migration-fixtures-"))
    target = d / "fo_trade" / "2026-09-17"
    target.mkdir(parents=True)
    (target / "raw.json").write_text(json.dumps(fixture))
    return d


def test_missing_fixture_is_not_a_failure():
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-migration-fixtures-"))
    source = LocalFixtureLegacySource(d)
    assert source.fetch("fo_trade", date(2026, 9, 17), "raw") is None


def test_fixture_round_trips_evidence_and_rows():
    d = _fixtures_dir({
        "reference": "legacy-load-001",
        "source_system": "DCM",
        "source_filename": "positions.csv",
        "producer_run_id": "DCM-1",
        "rows": [{"id": "A", "amount": 100}],
        "aggregates": {"amount": 100},
    })
    source = LocalFixtureLegacySource(d)
    result = source.fetch("fo_trade", date(2026, 9, 17), "raw")
    assert result is not None
    assert result.reference == "legacy-load-001"
    assert result.evidence.producer_run_id == "DCM-1"
    assert result.effective_row_count() == 1
    assert result.aggregates["amount"] == 100


def test_row_count_falls_back_to_len_rows_when_not_declared():
    d = _fixtures_dir({"reference": "r1",
                       "rows": [{"id": "A"}, {"id": "B"}]})
    result = LocalFixtureLegacySource(d).fetch("fo_trade", date(2026, 9, 17), "raw")
    assert result.effective_row_count() == 2
