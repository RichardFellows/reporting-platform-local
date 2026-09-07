"""Deployment provenance: the change that authorised the code, not the run.

A CHANGE IS A DEPLOYMENT EVENT. One ticket authorises a version, the pipeline
deploys it, and every run until the next deployment inherits it -- so the
standing change arrives from the ENVIRONMENT the chart set, and only an
exceptional publication (a restatement, an out-of-cycle rerun) carries a
per-run reference. These are separate fields because they are separate facts,
and the tests below pin that separation rather than the plumbing.

The drift check is the other half. A declared commit says what SHOULD be
running and a content digest says what IS; they diverge whenever the project is
writable at run time, which on this platform is exactly what the feed console
does. The console is a dev tool, so divergence in `dev` is normal and
divergence in `prod` is a fault -- and getting that the wrong way round would
either make dev unusable or make prod's attribution a guess.

No stack: environment variables and a digest of the checked-in dbt project.
"""
from __future__ import annotations

import os

from tests.support import config_dir


PROVENANCE_VARS = ("DBT_PROJECT_REF", "DBT_PROJECT_DIGEST",
                   "DEPLOYMENT_CHANGE_REF", "DEPLOYMENT_PIPELINE_REF")


def _context(env: str = "local", **overrides):
    """common.context with a fresh environment.

    `ENV` is bound at import time, so the variable has to be set before the
    module is imported -- which `config_dir()` guarantees by purging
    `reporting_platform` from sys.modules first.

    RESTORED BY `_restore()`, which every test calls in a finally. These tests
    share one process with every other module, and `REPORTING_ENV` selects the
    retention configuration -- so leaving it on `prod` here made eleven tests
    in two other modules fail on a config lookup, several modules later, with
    nothing pointing back to this one.
    """
    for key in PROVENANCE_VARS:
        os.environ.pop(key, None)
    os.environ["REPORTING_ENV"] = env
    os.environ.update(overrides)
    config_dir()
    from reporting_platform.common import context
    return context


def _restore():
    for key in PROVENANCE_VARS:
        os.environ.pop(key, None)
    os.environ["REPORTING_ENV"] = "local"


def test_nothing_declared_is_reported_as_nothing_not_guessed():
    try:
        ctx = _context()
        assert ctx.deployment_provenance() == {
            "dbt_project_ref": "", "deployment_change_ref": "",
            "deployment_pipeline_ref": ""}
        assert ctx.check_project_drift() == "", "nothing declared, nothing to check"
    finally:
        _restore()


def test_the_deployment_supplies_the_standing_change():
    try:
        ctx = _context(DBT_PROJECT_REF="a1b2c3d4",
                       DEPLOYMENT_CHANGE_REF="CHG-4471",
                       DEPLOYMENT_PIPELINE_REF="gitlab/pipelines/88213")
        assert ctx.deployment_provenance() == {
            "dbt_project_ref": "a1b2c3d4",
            "deployment_change_ref": "CHG-4471",
            "deployment_pipeline_ref": "gitlab/pipelines/88213"}
    finally:
        _restore()


def test_a_matching_digest_is_not_drift():
    try:
        ctx = _context()
        ctx = _context(DBT_PROJECT_DIGEST=ctx.dbt_manifest_ref())
        assert ctx.check_project_drift() == ""
    finally:
        _restore()


def test_drift_in_a_controlled_environment_refuses():
    """The project on disk is not the one deployed, so nothing may publish
    from it -- a figure attributed to a commit that did not produce it is
    worse than a failed build."""
    try:
        ctx = _context("prod", DBT_PROJECT_REF="a1b2c3d4",
                       DBT_PROJECT_DIGEST="deadbeefdead")
        try:
            ctx.check_project_drift()
        except RuntimeError as exc:
            assert "deadbeefdead" in str(exc) and "a1b2c3d4" in str(exc), (
                "the refusal must name both sides, or nobody can act on it")
        else:
            raise AssertionError("prod must refuse to run on a drifted project")
    finally:
        _restore()


def test_the_same_drift_in_dev_warns_and_continues():
    """`dev` is where the console writes models into the project. Refusing
    there would make the tool that edits the project unusable with it."""
    try:
        ctx = _context("dev", DBT_PROJECT_REF="a1b2c3d4",
                       DBT_PROJECT_DIGEST="deadbeefdead")
        message = ctx.check_project_drift()
        assert message and "deadbeefdead" in message
    finally:
        _restore()


def test_the_console_environments_are_not_controlled():
    """The guard above is only correct if `local` and `dev` stay out of the
    controlled set. Pinned because adding an environment there is a one-word
    change that would break the console silently."""
    try:
        ctx = _context()
        assert "dev" not in ctx.CONTROLLED_ENVIRONMENTS
        assert "local" not in ctx.CONTROLLED_ENVIRONMENTS
        assert set(ctx.CONTROLLED_ENVIRONMENTS) == {"uat", "prod"}
    finally:
        _restore()


def test_every_migrated_column_is_also_in_the_schema():
    """A fresh database and a migrated one must converge.

    `CREATE TABLE IF NOT EXISTS` does not reconcile columns, so a column added
    after a database was first created has to appear in BOTH -- in `SCHEMA` for
    a new database and in `MIGRATIONS` for an existing one. Declaring it in
    only one is the failure this catches: `ensure_schema()` succeeds either
    way, and the INSERT naming the column fails later, in a task, at publish.
    """
    try:
        import re

        from tests.support import REPO

        source = (REPO / "reporting_platform" / "registry" / "db.py").read_text()
        migrated = set(re.findall(
            r"ALTER TABLE registry\.run ADD COLUMN IF NOT EXISTS\s+(\w+)", source))
        assert migrated, "no migrations found -- has the pattern changed?"

        run_table = source.split("CREATE TABLE IF NOT EXISTS registry.run (")[1]
        run_table = run_table.split(");")[0]
        declared = set(re.findall(r"^\s{4}(\w+)\s+\w", run_table, re.M))
        missing = migrated - declared
        assert not missing, f"migrated but not declared in SCHEMA: {sorted(missing)}"
    finally:
        _restore()
