"""REQ-702's gate: which pin is worth reading, and what "readable" means.

Two rules here, both of them corrections to a check that reported green when
it should not have.

  * The gate is NOT AN AGE. It was `pin older than recent_partition_days`, on
    the grounds that maintenance rewrites partitions older than that. It
    rewrites the RECENT ones -- `compact()` scopes `>= cutoff` -- so a pin
    ages out of the compaction window rather than into it, and on a catalog
    between deliveries the window contains no data at all. The gate would have
    opened on a fixed date and reported green on files still identical to
    main's.
  * "Readable" is not `SELECT COUNT(*)`. Iceberg answers that out of its own
    manifests, so it returns the published number from a table whose parquet
    files have been deleted -- measured, by deleting one.

The divergence scan itself needs Spark and a catalog, so it is verified by
running it; see the module docstring. What is pinned here is the part that can
be: the object-storage half of the readability check, and the absence of any
way for the age gate to come back.
"""
from __future__ import annotations

from tests.support import REPO


def _module_source() -> str:
    return (REPO / "reporting_platform" / "monitoring"
            / "reproducibility.py").read_text(encoding="utf-8")


def _code_only(src: str) -> str:
    """The source with its module docstring removed.

    The prose still explains the age gate at length, and should -- an
    explanation of why a mechanism was wrong is the thing that stops somebody
    reinstating it. What must not come back is code that can compute an age.
    """
    import ast
    tree = ast.parse(src)
    doc = ast.get_docstring(tree, clean=False)
    return src.replace(doc, "") if doc else src


def test_the_gate_cannot_compute_an_age():
    code = _code_only(_module_source())
    assert "recent_partition_days" not in code
    assert "maintenance_config" not in code
    # `timedelta` is how the cutoff was built. Its absence is what makes the
    # rule above structural rather than a matter of somebody remembering it.
    assert "timedelta" not in code


def test_readability_is_not_only_a_count():
    """`COUNT(*)` proves the metadata chain resolves and nothing more, so the
    check must also ask object storage for the files the pin names."""
    code = _code_only(_module_source())
    assert "absent_objects" in code
    assert "missing_files" in code


def test_absent_objects_finds_a_collected_file():
    """The assertion that caught it: one pinned data file deleted from MinIO,
    which `SELECT COUNT(*)` at that pin answered right through."""
    from tests.fakes3 import FakeS3, install, uninstall
    from reporting_platform.monitoring.reproducibility import absent_objects

    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    try:
        base = "warehouse/raw/fo_trade_abc/data/_business_date_day=2026-08-06"
        s3.put(f"{base}/00000-a.parquet", "x")
        paths = {f"s3a://lakehouse/{base}/00000-a.parquet",
                 f"s3a://lakehouse/{base}/00001-b.parquet"}
        assert absent_objects(paths) == [f"{base}/00001-b.parquet"]
        s3.put(f"{base}/00001-b.parquet", "y")
        assert absent_objects(paths) == []
    finally:
        uninstall(monkey)


def test_a_scan_error_selects_the_pin_rather_than_skipping_it():
    """Fail TOWARD doing the check. A pin whose `.files` cannot be read is a
    symptom, and skipping it would silently pass over the one pin most likely
    to be broken -- while a table merely ABSENT at an old pin is history."""
    code = _code_only(_module_source())
    body = code[code.index("def _exclusive("):code.index("def divergence_scan(")]
    assert 'out["errors"]' in body
    assert 'bool(out["errors"])' in code[code.index("def _exclusive("):]
