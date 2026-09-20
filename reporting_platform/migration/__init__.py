"""Phase 8: dual-run migration and reconciliation.

THIS IS NOT A SECOND INGESTION PIPELINE. Every module here reads evidence
that Phases 0-7 already produce (Delivery/DeliveryManifest, Raw, prepared/
reporting tables, `registry.run`/`run_input`) and compares it against a
LEGACY result obtained through a small adapter interface -- it writes no
data anywhere except durable comparison evidence. See docs/MIGRATION.md.

Module map:
    context.resolve_migration_config  -- Feed config: mode/compare/acceptance
    correlate.py    -- same logical source delivery, legacy side vs new side
    legacy.py       -- LegacyResultSource adapter interface + local fixture
    comparators.py  -- row-count / key-set / canonical-hash / aggregate
    contract.py     -- comparison identity + contract-hash versioning
    new_side.py      -- Spark reader for the new-platform Raw/prepared/reporting
                       checkpoint (subprocess-only, like every Spark caller here)
    evidence.py     -- registry.migration_comparison read/write
    acceptance.py   -- derive READY/NOT_READY from comparison history
    run.py          -- orchestration entry points used by the CLI and the DAG
"""
