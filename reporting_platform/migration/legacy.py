"""The legacy-result adapter boundary (Phase 8, section 25).

"Legacy" here means the TRUE external enterprise estate this whole platform
replaces (SQL Server / stored procedures / the legacy ETL tool / the legacy
report server -- see docs/ARCHITECTURE.md's opening diagram) -- NOT the
Landing/Ready-v1 ingestion path inside this repo, which is a second entry
point of the NEW platform and stays "new" for Phase 8's purposes. See
docs/MIGRATION.md#terminology for why this distinction matters and where the
Phase 8 brief's own wording is easy to misread.

This local/public repo cannot reach a real SQL Server estate, and should not
pretend to: `LegacyResultSource` is the seam a future adapter plugs into,
and `LocalFixtureLegacySource` is the only implementation that ships here,
backed by small on-disk JSON fixtures under a configured directory. A real
enterprise adapter (`SqlServerLegacySource`, not built here) would implement
the same three-method interface against a live database/DCM query instead of
a file -- see the class docstring below for exactly what it would need to
supply and why each field matters to the comparison it feeds.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from reporting_platform.migration.correlate import CorrelationEvidence


@dataclass(frozen=True)
class LegacyResult:
    """What the legacy estate produced for one feed/business_date/checkpoint.

    `rows` is a list of business-column dicts -- SMALL, comparison-scale data
    (a fixture, or a bounded legacy query result), never the full production
    volume. At real enterprise scale an adapter would return summary
    statistics computed IN the legacy database (COUNT, key list, per-key
    hash) rather than materialising every row here; `rows` stays optional for
    exactly that reason -- `aggregates` alone is enough to run an
    aggregate-only comparison.
    """
    evidence: CorrelationEvidence
    reference: str  # the strongest legacy identifier the adapter can name
    rows: list[dict[str, Any]] | None = None
    row_count: int | None = None
    aggregates: dict[str, Any] = field(default_factory=dict)

    def effective_row_count(self) -> int | None:
        if self.row_count is not None:
            return self.row_count
        return len(self.rows) if self.rows is not None else None


class LegacyResultSource:
    """The adapter interface. One method a real SQL Server adapter needs.

    A PRODUCTION `SqlServerLegacySource` would implement `fetch` by running a
    parameterised, READ-ONLY query against the legacy reporting schema for
    the given (feed, business_date, checkpoint), returning:
      * `evidence` built from whatever legacy load-control identity exists
        (a filename/hash the legacy `stg` load recorded, or the DCM producer
        run id if the legacy load control captured it -- see
        docs/MIGRATION.md#legacy-provenance);
      * `reference`, a human-investigable string (e.g. a legacy load/batch id
        or a query snapshot id) -- NOT a new identity scheme this platform
        invents on the legacy estate's behalf;
      * either `rows` (bounded, comparison-scale) or `aggregates`
        (COUNT/SUM/etc. computed server-side, preferred at real volume).

    Connection details (server, credentials, database) belong to environment
    configuration read by that adapter, never hard-coded here or in this
    module -- see docs/MIGRATION.md#legacy-adapter.
    """

    def fetch(self, feed: str, business_date: date,
             checkpoint: str) -> LegacyResult | None:
        """Return the legacy result, or `None` if it is not available yet.

        `None` is NOT a failure -- it is "not yet comparable", which callers
        must distinguish from FAIL (see docs/VALIDATION.md's outcome
        semantics, reused unchanged for migration comparisons).
        """
        raise NotImplementedError


class LocalFixtureLegacySource(LegacyResultSource):
    """Local/dev/test adapter: legacy results as small JSON fixture files.

    Layout, one file per (feed, business_date, checkpoint):

        <fixtures_dir>/<feed>/<business_date>/<checkpoint>.json

        {
          "reference": "legacy-load-2026-09-17-001",
          "source_system": "DCM",
          "source_filename": "positions_20260917.csv",
          "source_sha256": "...",             # optional
          "producer_run_id": "DCM-849217",    # optional
          "rows": [ {"counterparty_id": "C1", "exposure": 100.0}, ... ],
          "aggregates": {"exposure": 100.0}   # optional, precomputed
        }

    This is the ONLY implementation shipped in this repo (section 25: "the
    smallest abstraction necessary"). It proves the comparison machinery end
    to end without any enterprise dependency, and documents exactly what a
    real adapter must supply.
    """

    def __init__(self, fixtures_dir: str | Path):
        self.fixtures_dir = Path(fixtures_dir)

    def fetch(self, feed: str, business_date: date,
             checkpoint: str) -> LegacyResult | None:
        path = (self.fixtures_dir / feed / business_date.isoformat()
               / f"{checkpoint}.json")
        if not path.is_file():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        evidence = CorrelationEvidence(
            feed=feed, business_date=business_date,
            source_system=data.get("source_system"),
            source_filename=data.get("source_filename"),
            source_sha256=data.get("source_sha256"),
            producer_run_id=data.get("producer_run_id"))
        return LegacyResult(
            evidence=evidence,
            reference=data["reference"],
            rows=data.get("rows"),
            row_count=data.get("row_count"),
            aggregates=data.get("aggregates") or {})
