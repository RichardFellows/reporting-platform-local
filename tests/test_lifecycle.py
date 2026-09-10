"""The as-at date's lifecycle: locking, reopening, and what a lock costs.

Phase 6. A (report, as-at date) moves open -> locked -> submitted, and back to
reopened only deliberately. `open` is the ABSENCE of a row, so the state
machine is append-only and a date that has never been touched needs no seeding
-- which matters because the set of (report, date) pairs is derived, not
enumerated, and a table pre-populated with every pair would be a second list to
keep in step with the exposures.

WHAT THESE TESTS CAN AND CANNOT REACH. `registry/lifecycle.py` is Postgres,
and these are the config-level tests -- no stack. So this file covers the half
that is pure: the refusals `transition()` makes BEFORE it opens a connection,
the derivations in `common/context.py` the policy is read out of, and the
structural claims about the shipped dbt project that the whole design rests on.
The database half is verified by running it against the live registry, and the
session notes record that separately. A test that mocked psycopg2 to reach the
rest would assert that the mock behaves as written.
"""
from __future__ import annotations

import pathlib
from datetime import date

from tests.support import DAGS, config_dir

REPO = pathlib.Path(__file__).resolve().parent.parent
LIFECYCLE = REPO / "reporting_platform" / "registry" / "lifecycle.py"
DBT_BUILDS = DAGS / "dbt_builds.py"


def _ctx():
    config_dir()
    from reporting_platform.common import context
    return context


def _lifecycle():
    config_dir()
    from reporting_platform.registry import lifecycle
    return lifecycle


# ------------------------------------------------------- policy is declared
def test_every_exposure_declares_a_restatement_policy():
    """Open decision 2, settled as `fail`. There is no platform-wide default,
    so a report that declares nothing cannot be published for a locked date at
    all -- and this is the test that turns that from a latent outage into a
    failure at the point somebody adds an exposure. `restatement_policy()`
    raises, so reaching the end of the loop IS the assertion."""
    ctx = _ctx()
    for name in sorted(ctx.reports()):
        policy = ctx.restatement_policy(name)
        assert policy in ctx.RESTATEMENT_POLICIES, (name, policy)


def test_every_exposure_names_an_owner():
    """The approver check for reopening a SUBMITTED date compares against
    `report_owner()`. An exposure with an unnamed owner would make that check
    vacuous -- any `--approved-by` would match an empty string -- so the
    function refuses rather than returning one, and this pins that the shipped
    project does not rely on the refusal."""
    ctx = _ctx()
    for name in sorted(ctx.reports()):
        assert ctx.report_owner(name).strip(), name


def test_an_unknown_report_is_refused_not_defaulted():
    """A lock on a mistyped report name would record a control that protects
    nothing and leave the real date open."""
    ctx = _ctx()
    for fn in (ctx.report, ctx.report_owner, ctx.restatement_policy):
        try:
            fn("countrparty_exposure_report")
        except ValueError as exc:
            assert "no report" in str(exc) or "exposure" in str(exc), str(exc)
        else:
            raise AssertionError(f"{fn.__name__} accepted an unknown report")


# ------------------------------------------------------------- REQ-503
def test_nothing_in_the_lifecycle_branches_on_what_kind_of_report_it_is():
    """REQ-503: a daily internal dashboard and a quarterly regulatory return
    traverse the same machinery. The way that stops being true is a branch on
    an exposure's `type` or `maturity` appearing somewhere -- which reads as a
    small convenience and silently gives one class of report a different
    control. Same shape as test_supersession's grep for `known_as_of()`: the
    claim is structural, so it is checked structurally rather than asserted in
    a comment."""
    for path in (LIFECYCLE, DBT_BUILDS):
        text = path.read_text(encoding="utf-8")
        for token in ('"type"', "'type'", '"maturity"', "'maturity'",
                      ".type ==", "exposure_type"):
            assert token not in text, f"{path.name} branches on {token}"


def test_the_restatement_policy_is_read_from_the_exposure_not_a_new_list():
    """A report IS a dbt exposure (`context.reports()`), derived from the
    project the same way `managed_tables()` is. The failure this guards is a
    second registry of reports in feeds.yml or retention.yml, which would let
    the two disagree about which reports exist -- and the lifecycle would then
    protect the ones in the wrong list."""
    ctx = _ctx()
    assert ctx.reports(), "no exposures found -- is DBT_PROJECT_DIR set?"
    for name, meta in ctx.reports().items():
        assert ctx.restatement_policy(name) == meta["meta"]["restatement"], name


# ------------------------------------- refusals that need no database at all
def test_open_cannot_be_written_as_a_transition():
    """`open` is the absence of a row. Writing it would make the state machine
    two things at once -- absence AND a row -- and `state()` reads the newest
    row, so an explicit `open` would be indistinguishable from a reopen that
    lost its reason."""
    lc = _lifecycle()
    try:
        lc.transition("counterparty_exposure_report", date(2026, 8, 19),
                      lc.OPEN, actor="me", reason="because")
    except lc.LifecycleRefused as exc:
        assert "ABSENCE" in str(exc), str(exc)
    else:
        raise AssertionError("`open` was accepted as a written state")


def test_a_transition_without_an_actor_or_a_reason_is_refused():
    """This platform has no identity provider, so who decided and why is the
    only record there is. It is checked before the connection is opened, which
    is also why this test can run without one."""
    lc = _lifecycle()
    for actor, reason in (("", "because"), ("me", ""), ("  ", "  ")):
        try:
            lc.transition("counterparty_exposure_report", date(2026, 8, 19),
                          lc.LOCKED, actor=actor, reason=reason)
        except lc.LifecycleRefused as exc:
            assert "ACTOR" in str(exc) and "REASON" in str(exc), str(exc)
        else:
            raise AssertionError(f"accepted actor={actor!r} reason={reason!r}")


def test_locking_an_unknown_report_is_refused_before_anything_is_written():
    lc = _lifecycle()
    try:
        lc.lock("no_such_report", date(2026, 8, 19), actor="me", reason="x")
    except ValueError as exc:                      # includes LifecycleRefused
        assert "no report" in str(exc) or "exposure" in str(exc), str(exc)
    else:
        raise AssertionError("a lock on an unknown report was accepted")


def test_the_states_are_the_ones_the_cli_and_the_gate_agree_on():
    """Three modules name these strings -- the state machine, the registry CLI
    and the publish gate in dbt_builds.py. They are constants here so a fourth
    caller cannot introduce a fifth spelling."""
    lc = _lifecycle()
    assert lc.CLOSED == (lc.LOCKED, lc.SUBMITTED)
    assert lc.WRITABLE == (lc.LOCKED, lc.SUBMITTED, lc.REOPENED)
    assert lc.OPEN not in lc.WRITABLE


# --------------------------------------------- where the gate actually runs
def test_the_publish_gate_runs_before_the_merge_not_with_the_versioning():
    """The one thing about phase 6 that is easy to get wrong and impossible to
    see afterwards. `publish()` merges to `main` first and allocates versions
    second, so a gate placed with the versioning fires after `main` has already
    moved -- the refusal would be honest and useless. It has to sit between
    reading the input set (which is what makes the as-at date knowable) and the
    merge, so a refusal leaves `main` untouched and the branch retained."""
    text = DBT_BUILDS.read_text(encoding="utf-8")
    # `n.merge(` is the call that moves `main`. Matching on the bare word
    # "merge" would hit the module docstring and pass for the wrong reason.
    gate = text.index("check_publishable")
    merge = text.index("n.merge(")
    assert gate < merge, "the lifecycle gate is after the merge"
    # And after the input set is read -- that is what makes the as-at date and
    # the candidate delivery set knowable in the first place.
    assert text.index("run-inputs") < gate
    # And it must not swallow the refusal into a warning.
    assert "except lifecycle.LifecycleRefused" not in text
    assert "except LifecycleRefused" not in text
