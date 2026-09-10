"""Runs, reports and code identity: the pure parts of phase 5.

WHAT IS NOT COVERED HERE, deliberately, and it is the same line `test_registry`
drew: nothing that talks to Postgres. A fake database would agree with whatever
the code asked it, so `open_run`, `allocate_version` and `record_submission`
are verified by running them against the real one. What IS covered is
everything that decides what those functions are handed -- the tag shapes, the
report derivation, the code and manifest digests -- because every one of them
is a pure function of files in this repo, and every one of them is a place a
silent wrong answer would be written into a record nothing can rebuild.
"""
from __future__ import annotations

import datetime as dt
import pathlib

from tests.support import DAGS, REPO, config_dir


def _context():
    """common.context, with the dbt project and platform root pointed here."""
    import os
    import sys

    config_dir()                      # purges the module cache too
    os.environ["DBT_PROJECT_DIR"] = str(REPO / "dbt")
    os.environ["PLATFORM_ROOT"] = str(REPO)
    sys.path.insert(0, str(REPO))
    from reporting_platform.common import context
    return context


BD = dt.date(2026, 8, 1)


# ------------------------------------------------------------- the two tags
def test_a_publication_names_its_report():
    c = _context()
    assert (c.published_tag("counterparty_exposure_report", BD, "r1")
            == "published/counterparty_exposure_report/2026-08-01/r1")


def test_a_publication_with_no_report_is_refused():
    """The whole defect being corrected: `published/<bd>/<run>` was cut by an
    INGEST, so every check that read `published/` was reading ingests. A
    publication that cannot say which report it is for is not one."""
    c = _context()
    for bad in ("", None, "a/b"):
        try:
            c.published_tag(bad, BD, "r1")
        except ValueError:
            pass
        else:
            raise AssertionError(f"published_tag accepted report={bad!r}")


def test_an_ingest_pins_a_snapshot_and_it_names_the_feed():
    c = _context()
    assert c.snapshot_tag("fo_trade", BD, "r1") == "snapshot/fo_trade/2026-08-01/r1"


def test_the_two_shapes_cannot_be_confused_by_the_sweeps():
    """Each sweep must recognise its own and leave the other's alone --
    they answer to different windows, and a snapshot judged by the published
    window is the ten-year over-retention this split exists to end."""
    config_dir()
    import sys
    sys.path.insert(0, str(REPO))
    from reporting_platform.retention.retention import SNAPSHOT_RE, TAG_RE

    pub = "published/counterparty_exposure_report/2026-08-01/r1"
    snap = "snapshot/fo_trade/2026-08-01/r1"
    assert TAG_RE.match(pub) and not TAG_RE.match(snap)
    assert SNAPSHOT_RE.match(snap) and not SNAPSHOT_RE.match(pub)
    assert TAG_RE.match(pub).group("report") == "counterparty_exposure_report"
    assert SNAPSHOT_RE.match(snap).group("feed") == "fo_trade"
    # The old two-segment shape is still RECOGNISED: tags cut under it are
    # real pins, and a sweep that failed to recognise one would skip it
    # forever rather than judge it.
    old = TAG_RE.match("published/2026-08-01/r1")
    assert old and old.group("report") is None


# ---------------------------------------------------------------- reports
def test_reports_are_derived_from_the_dbt_exposures():
    """Not a `reports:` block in a new config file: the dbt project already
    declares them, and two declarations disagree the first time one moves."""
    c = _context()
    got = c.reports()
    assert set(got) == {"counterparty_exposure_report", "country_exposure_dashboard"}
    assert got["counterparty_exposure_report"]["models"] == [
        "counterparty_exposure", "exposure_change"]
    assert got["country_exposure_dashboard"]["owner"] == "Reporting"


def test_every_report_name_is_usable_as_a_tag_segment():
    """A '/' in a report name would make TAG_RE parse the tag into the wrong
    fields -- silently, and visibly only when retention resolved the wrong
    window for it."""
    c = _context()
    for name in c.reports():
        assert "/" not in name
        assert c.published_tag(name, BD, "r1").count("/") == 3


# ----------------------------------------------------------- code identity
def test_code_ref_prefers_what_the_deployment_supplies():
    import os

    c = _context()
    os.environ["PLATFORM_CODE_REF"] = "registry.example/platform:1.4.2"
    try:
        assert c.code_ref() == ("registry.example/platform:1.4.2", "deployed")
    finally:
        del os.environ["PLATFORM_CODE_REF"]


def test_code_ref_falls_back_to_a_digest_and_says_that_is_what_it_is():
    """A git SHA would be a LIE here: the code is bind-mounted from a working
    tree that may be dirty, and `.git` is not mounted into any container. The
    kind travels with the value so nobody reads a laptop digest as a release."""
    c = _context()
    value, kind = c.code_ref()
    assert kind == "tree-digest"
    assert len(value) == 16 and value == c.code_ref()[0]


def test_the_digests_actually_change_when_the_code_does(tmp=None):
    """A digest that does not move is worse than no digest: every run would
    claim to be the same code."""
    import tempfile

    c = _context()
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-code-"))
    (d / "a.py").write_text("x = 1\n")
    first = c._tree_digest([d], (".py",))
    (d / "a.py").write_text("x = 2\n")
    assert c._tree_digest([d], (".py",)) != first
    # And moving a file counts as a change: the path is hashed with the bytes.
    (d / "a.py").write_text("x = 1\n")
    assert c._tree_digest([d], (".py",)) == first
    (d / "a.py").rename(d / "b.py")
    assert c._tree_digest([d], (".py",)) != first


def test_the_manifest_ref_is_the_project_not_dbts_own_manifest():
    """Cosmos runs one dbt subprocess PER MODEL, each overwriting
    target/manifest.json with its own invocation id, so there is no single
    manifest for a run to record."""
    c = _context()
    assert len(c.dbt_manifest_ref()) == 16
    assert c.dbt_manifest_ref() == c.dbt_manifest_ref()


# ------------------------------------------------------- the run key itself
def test_the_run_key_is_derived_from_the_branch_and_carries_the_purpose():
    """Recomputing the slug in two tasks is how one run ends up as two rows;
    and the prepared and reporting builds slugify the SAME dataset-triggered
    Airflow run id, so the purpose has to be in the key."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_dbt_builds_probe", DAGS / "dbt_builds.py")
    # The module imports cosmos and airflow, which the test environment does
    # not have, so the helper is read out of the source rather than imported.
    source = (DAGS / "dbt_builds.py").read_text()
    body = source[source.index("def _run_key("):]
    body = body[:body.index("\ndef ", 1)]
    ns: dict = {}
    exec(compile(body, "dbt_builds._run_key", "exec"), ns)          # noqa: S102
    assert ns["_run_key"]("build/prepared/2026-09-06/abc-123") == "prepared-abc-123"
    assert ns["_run_key"]("build/reporting/2026-09-06/abc-123") == "reporting-abc-123"
    assert spec is not None


# --------------------------------------------------- the version diff (§11)
# Postgres again, so what is checked here is the SHAPE of the query rather
# than its result -- and the shape is where the two defects would be.
RUNS_PY = REPO / "reporting_platform" / "registry" / "runs.py"


def test_the_diff_left_joins_deliveries_rather_than_inner_joining_them():
    """`run_input` has no foreign key to `delivery` on purpose, so a delivery
    the registry cannot currently describe is a real possibility -- after a
    registry rebuild, or for one whose landing object retention removed. An
    INNER JOIN would drop it from BOTH sides equally, which turns a genuine
    difference into agreement: the diff would report "nothing changed" for
    exactly the case somebody is looking into."""
    body = RUNS_PY.read_text(encoding="utf-8")
    body = body[body.index("def diff("):]
    assert "LEFT JOIN registry.delivery" in body, "diff inner-joins deliveries"
    assert "INNER JOIN registry.delivery" not in body


def test_the_diff_reports_code_movement_separately_from_delivery_movement():
    """The interesting real case on this stack today is two versions of one
    date with IDENTICAL input sets and different `code_ref`s. A diff that only
    differenced deliveries would say "no change" about a rebuild that moved
    every figure, so the honest answer needs both halves."""
    body = RUNS_PY.read_text(encoding="utf-8")
    body = body[body.index("def diff("):]
    for key in ("code_ref_changed", "dbt_project_changed", "change_ref"):
        assert key in body, key


def test_the_diff_is_keyed_per_report_and_as_at_date_like_the_versions_are():
    """Decision 5 again. A diff keyed on run ids would compare two runs that
    published different dates and call the entire input set a change."""
    import inspect
    import sys
    sys.path.insert(0, str(REPO))
    from reporting_platform.registry import runs
    params = list(inspect.signature(runs.diff).parameters)
    assert params[:2] == ["report", "as_at_date"], params
    # Both versions default, because "the last two" is what anybody wants and
    # is the tedious thing to look up.
    sig = inspect.signature(runs.diff)
    assert sig.parameters["from_version"].default is None
    assert sig.parameters["to_version"].default is None
