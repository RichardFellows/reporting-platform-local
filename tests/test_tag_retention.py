"""Published-tag retention: the reproducibility window.

A published tag pins every data file its commit referenced, so how long a tag
lives is how long a published run stays REPRODUCIBLE. That is a different
question from how much history the tables serve, and it used to be answered
with the tables' own keep-set -- `keep_business_days: 10 / keep_month_ends:
80` -- which expired an ordinary daily pin after about a fortnight.

Two things these tests hold to, both of which were live defects:

  * the window is FLAT AGE in years, resolved per report, never a keep-set;
  * every tag for a date is judged on its own. The old sweep kept only the
    newest tag per COB date, and since the tag name carries no feed and
    `record_publication` runs in every per-feed ingest DAG, that deleted other
    feeds' publications for the same date.

Expiry cannot be exercised against the live catalog -- every real tag is days
old, and a ten-year window keeps everything for a decade, which is the point.
So the sweep is driven here against a fake catalog whose commit times are
whatever the test needs. What this canNOT tell you is whether Nessie deletes
the reference the same way; that is verified by running it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from tests.support import config_dir

BASE_YML = """
references:
  published_tags:
    default_keep_years: 10
    per_report: {}
"""


# Every fragment below overwrites the shipped retention.yml wholesale, and the
# shipped feeds.yml it is paired with names a retention class -- which is
# refused at LOAD if retention.yml does not declare it. So the declaration is
# prepended rather than repeated in each fragment: these tests are about tag
# windows, and a class list in each one would read as though it mattered here.
# The windows themselves are deliberately absent, so every class falls back to
# the prefix default and each fragment's `landing.keep_years` still means what
# it says.
CLASSES_YML = """
retention_classes:
  standard: {}
  operational: {}
"""


def _retention(references_yml: str = BASE_YML):
    """The retention module, pointed at a throwaway retention.yml."""
    import pathlib
    d = config_dir()
    if "retention_classes:" not in references_yml:
        references_yml = CLASSES_YML + references_yml
    (d / "retention.yml").write_text(references_yml, encoding="utf-8")
    import sys
    for name in [m for m in sys.modules if m.startswith("reporting_platform")]:
        del sys.modules[name]
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from reporting_platform.retention import retention
    return retention


class FakeNessie:
    """Just the two calls `expire_tags` makes. Anything else raises, so a
    test that starts depending on a third fails loudly."""

    def __init__(self, refs):
        self.refs = refs
        self.deleted: list[str] = []

    def list_references(self, prefix=None, fetch_all=False):
        return [r for r in self.refs
                if prefix is None or r["name"].startswith(prefix)]

    def delete_reference(self, name):
        self.deleted.append(name)


def _tag(name: str, *, days_ago: float | None = 1.0):
    """A tag ref shaped the way Nessie returns one with fetch=ALL."""
    meta = {}
    if days_ago is not None:
        when = datetime.now(timezone.utc) - timedelta(days=days_ago)
        meta = {"commitMetaOfHEAD": {"commitTime":
                                     when.isoformat().replace("+00:00", "Z")}}
    return {"type": "TAG", "name": name, "metadata": meta}


YEAR = 365.25


# ---------------------------------------------------------------- the window
def test_the_window_is_years_not_a_keep_set():
    """The defect this replaces: an ordinary daily publication used to lose
    its pin after ten more published COB dates, about a fortnight."""
    R = _retention()
    n = FakeNessie([_tag(f"published/2026-08-{d:02d}/run{d}", days_ago=d)
                    for d in range(1, 26)])
    assert R.expire_tags(n, dry_run=True) == []


def test_a_tag_past_its_window_is_removed():
    R = _retention()
    n = FakeNessie([_tag("published/2010-01-04/old", days_ago=11 * YEAR),
                    _tag("published/2026-08-01/new", days_ago=1)])
    assert R.expire_tags(n, dry_run=False) == ["published/2010-01-04/old"]
    assert n.deleted == ["published/2010-01-04/old"]


def test_a_dry_run_deletes_nothing():
    R = _retention()
    n = FakeNessie([_tag("published/2010-01-04/old", days_ago=11 * YEAR)])
    assert R.expire_tags(n, dry_run=True) == ["published/2010-01-04/old"]
    assert n.deleted == []


# ------------------------------------------------- every publication counts
def test_every_tag_for_a_date_is_kept():
    """The quieter half of the defect. `published/<bd>/<run_id>` carries no
    feed, and record_publication runs in every per-feed ingest DAG, so N feeds
    publishing one COB date cut N tags for it -- and the old sweep kept
    only the newest. Observed live: three tags for 2026-08-01, two of them
    scheduled for deletion while inside the keep-set."""
    R = _retention()
    n = FakeNessie([_tag("published/2026-08-01/fo_trade_run", days_ago=3),
                    _tag("published/2026-08-01/ref_cpty_run", days_ago=2),
                    _tag("published/2026-08-01/ref_rating_run", days_ago=1)])
    assert R.expire_tags(n, dry_run=True) == []


# ------------------------------------------------------------- per report
PER_REPORT_YML = """
references:
  published_tags:
    default_keep_years: 10
    per_report:
      short_lived: 1
      long_lived: 25
"""


def test_a_per_report_window_applies_to_that_report_only():
    R = _retention(PER_REPORT_YML)
    n = FakeNessie([
        _tag("published/short_lived/2024-01-04/r", days_ago=2 * YEAR),
        _tag("published/long_lived/2024-01-04/r", days_ago=2 * YEAR),
        _tag("published/2024-01-04/r", days_ago=2 * YEAR),
    ])
    assert R.expire_tags(n, dry_run=True) == \
        ["published/short_lived/2024-01-04/r"]


def test_an_unlisted_report_gets_the_default_not_an_error():
    """A newly added report must be OVER-retained, never dropped, until
    someone decides its period."""
    R = _retention(PER_REPORT_YML)
    n = FakeNessie([_tag("published/brand_new/2024-01-04/r", days_ago=2 * YEAR)])
    assert R.expire_tags(n, dry_run=True) == []


def test_both_tag_shapes_are_recognised():
    R = _retention()
    assert R.TAG_RE.match("published/2026-08-01/run").group("report") is None
    m = R.TAG_RE.match("published/daily_risk/2026-08-01/run")
    assert (m.group("report"), m.group("bd")) == ("daily_risk", "2026-08-01")


# ------------------------------------------------- what it refuses to touch
def test_an_unrecognised_name_under_published_is_left_alone():
    """This sweep removes only what it positively recognises -- the rule
    clean_working_branches already follows for hold/."""
    R = _retention()
    n = FakeNessie([{"type": "TAG", "name": "published/notes", "metadata": {}}])
    assert R.expire_tags(n, dry_run=True) == []


def test_a_branch_under_published_is_not_a_tag():
    R = _retention()
    n = FakeNessie([{"type": "BRANCH", "name": "published/2010-01-04/x",
                     "metadata": {}}])
    assert R.expire_tags(n, dry_run=True) == []


def test_a_tag_with_no_commit_time_falls_back_to_its_cob_date():
    """Conservative by construction: a publication cannot precede the date it
    reports on, so the COB date is never later than the commit time and
    can only keep a tag the commit time would also have kept."""
    R = _retention()
    n = FakeNessie([_tag("published/2026-08-01/recent", days_ago=None),
                    _tag("published/2005-08-01/ancient", days_ago=None)])
    assert R.expire_tags(n, dry_run=True) == ["published/2005-08-01/ancient"]


def test_an_unreadable_commit_time_falls_back_rather_than_expiring():
    R = _retention()
    n = FakeNessie([{"type": "TAG", "name": "published/2026-08-01/r",
                     "metadata": {"commitMetaOfHEAD":
                                  {"commitTime": "not-a-timestamp"}}}])
    assert R.expire_tags(n, dry_run=True) == []


# ------------------------------------------------------ fail closed on config
def _refuses(references_yml: str, needle: str):
    R = _retention(references_yml)
    n = FakeNessie([_tag("published/2026-08-01/r")])
    try:
        R.expire_tags(n, dry_run=True)
    except ValueError as exc:
        assert needle in str(exc), str(exc)
        assert n.deleted == []
    else:
        raise AssertionError(f"expected a refusal mentioning {needle!r}")


def test_a_missing_window_refuses_rather_than_defaulting():
    """The old keys are the realistic way this happens: an untouched
    retention.yml still carrying keep_business_days/keep_month_ends. Falling
    back to a guess would authorise deleting the evidence."""
    _refuses("references:\n  published_tags:\n"
             "    keep_business_days: 10\n    keep_month_ends: 80\n",
             "default_keep_years")


def test_a_zero_window_refuses():
    _refuses("references:\n  published_tags:\n    default_keep_years: 0\n",
             "expires every pin immediately")


def test_a_non_integer_window_refuses():
    _refuses("references:\n  published_tags:\n    default_keep_years: '10'\n",
             "not a whole number of years")


def test_a_bad_per_report_window_refuses():
    R = _retention("references:\n  published_tags:\n"
                   "    default_keep_years: 10\n    per_report:\n      r: 0\n")
    n = FakeNessie([_tag("published/r/2026-08-01/x")])
    try:
        R.expire_tags(n, dry_run=True)
    except ValueError as exc:
        assert "per_report['r']" in str(exc), str(exc)
        assert n.deleted == []
    else:
        raise AssertionError("a zero per-report window was accepted")


def test_a_shorter_per_report_window_is_allowed_but_warned():
    """A shorter period can be a real answer, so it is permitted -- but it is
    the direction that loses evidence, so it is logged rather than silent."""
    import logging
    R = _retention(PER_REPORT_YML)
    from reporting_platform.common.context import tag_retention_years
    records = []

    class Catch(logging.Handler):
        def emit(self, record): records.append(record.getMessage())

    lg = logging.getLogger("retention")
    lg.addHandler(h := Catch())
    try:
        assert tag_retention_years("short_lived") == 1
        assert tag_retention_years("long_lived") == 25
        assert tag_retention_years("unlisted") == 10
    finally:
        lg.removeHandler(h)
    assert any("short_lived" in m and "shorter" in m for m in records), records


# ------------------------------------------------- the reproducibility interlock
# A published tag pins the TABLES. Reproducing a published run also means
# showing its inputs, and landing/ is the only copy of what the upstream
# actually sent -- so a pin that outlives its landing evidence is one that
# cannot be fully honoured. This refuses where landing.keep_years()'s own
# interlock warns, because the loss is unrecoverable and the sweep is nightly
# and unattended.
INTERLOCK_YML = """
environments:
  local:
    landing: {{keep_years: {landing}}}
    raw: {{keep_business_days: 10, keep_month_ends: 80}}
references:
  published_tags:
    default_keep_years: {default}
    per_report: {per_report}
"""


def _interlock(landing: int, default: int = 10, per_report: str = "{}"):
    R = _retention(INTERLOCK_YML.format(landing=landing, default=default,
                                        per_report=per_report))
    return R.check_reproducibility_window


def test_landing_shorter_than_the_pin_refuses():
    try:
        _interlock(landing=8, default=10)()
    except ValueError as exc:
        # Named per (report, feed) now, not as one global pair of numbers:
        # with classes the answer to "how long is landing kept" depends on
        # which feed you are asking about, and a message that did not say
        # which would send you to the wrong line of retention.yml.
        assert "keeps its landing evidence 8 years" in str(exc), str(exc)
        assert "up to 10 years" in str(exc), str(exc)
        assert "fo_trade (class standard)" in str(exc), str(exc)
    else:
        raise AssertionError("landing expiring before the pins was accepted")


def _windows(result) -> set[tuple[int, int]]:
    """(landing years, tag years) for every (report, feed) pair checked.

    `check_reproducibility_window` returns what it checked rather than one
    number, because with retention classes there is no single landing window
    to return. These tests use the shipped feeds.yml, where every feed behind
    a report is `standard`, so the pairs collapse to one -- but asserting over
    the set rather than indexing row 0 keeps the test honest if a class is
    ever applied to a feed that IS behind a report.
    """
    return {(c["landing_keep_years"], c["tag_keep_years"])
            for c in result["checked"]}


def test_landing_equal_to_the_pin_is_accepted():
    assert _windows(_interlock(landing=10, default=10)()) == {(10, 10)}


def test_landing_longer_than_the_pin_is_accepted():
    assert _windows(_interlock(landing=20, default=10)()) == {(20, 10)}


def test_every_feed_behind_a_report_is_checked_not_just_one():
    """The interlock is per (report, feed) now. The shipped project has two
    exposures over three feeds, and a check that resolved one pair would pass
    while the other feed expired its evidence early."""
    checked = _interlock(landing=10, default=10)()["checked"]
    pairs = {(c["report"], c["feed"]) for c in checked}
    assert len(pairs) >= 3, pairs
    assert all(c["live_exposure"] for c in checked), checked


def test_a_feed_behind_no_report_is_not_bound_by_any_pin():
    """The whole point of classes. `ref_collateral` reaches no exposure, so
    nothing published is reproduced from it and no window binds it -- if this
    ever fails, either the lineage walk broke or a report started ref-ing it,
    and in the second case its class is now a real constraint."""
    result = _interlock(landing=10, default=10)()
    assert "ref_collateral" in result["unbound_feeds"], result["unbound_feeds"]


def test_the_longest_report_window_binds_not_the_default():
    """One report over-retaining is enough to require the evidence, so the
    interlock is measured against the maximum, not against the default."""
    try:
        _interlock(landing=10, default=10, per_report="{annual: 25}")()
    except ValueError as exc:
        assert "up to 25 years" in str(exc), str(exc)
    else:
        raise AssertionError("a per-report window past landing was accepted")


def test_a_shorter_report_window_does_not_relax_the_interlock():
    # `brief` is not a live exposure, so it binds every feed -- at 2 years,
    # which 10 clears. The default still applies to the real reports.
    assert _windows(_interlock(landing=10, default=10,
                               per_report="{brief: 2}")()) == {(10, 10), (10, 2)}


def test_a_per_report_entry_naming_no_exposure_binds_every_feed():
    """A report removed from the project keeps the tags it already cut, and
    `expire_tags` still resolves their window by the name in the tag -- so the
    entry is in force. Its lineage is gone, so the conservative answer is the
    only available one: every feed, including ones behind no live report."""
    result = _interlock(landing=10, default=10, per_report="{gone: 9}")()
    bound = {c["feed"] for c in result["checked"] if c["report"] == "gone"}
    assert "ref_collateral" in bound, bound
    assert result["unbound_feeds"] == [], result["unbound_feeds"]


# ------------------------------------------------------- per-environment windows
ENV_YML = """
environments:
  local: {landing: {keep_years: 10}}
  dev:   {landing: {keep_years: 1}}
references:
  published_tags:
    default_keep_years:
      local: 10
      dev: 1
    per_report: {}
"""


def test_the_window_can_be_per_environment():
    """landing.keep_years is per environment and dev deliberately shortens it.
    A globally fixed tag window would leave dev pinning runs for a decade
    whose evidence it discarded after a year -- and the interlock would then
    refuse every dev sweep."""
    import os
    for env, expected in (("local", 10), ("dev", 1)):
        before = os.environ.get("REPORTING_ENV")
        os.environ["REPORTING_ENV"] = env
        try:
            R = _retention(ENV_YML)
            from reporting_platform.common.context import tag_retention_years
            assert tag_retention_years() == expected, env
            assert _windows(R.check_reproducibility_window()) == \
                {(expected, expected)}, env
        finally:
            if before is None:
                os.environ.pop("REPORTING_ENV", None)
            else:
                os.environ["REPORTING_ENV"] = before


def test_a_map_with_no_entry_for_this_env_refuses():
    import os
    before = os.environ.get("REPORTING_ENV")
    os.environ["REPORTING_ENV"] = "uat"
    try:
        R = _retention(ENV_YML)
        n = FakeNessie([_tag("published/2026-08-01/r")])
        try:
            R.expire_tags(n, dry_run=True)
        except ValueError as exc:
            assert "no entry for env 'uat'" in str(exc), str(exc)
            assert n.deleted == []
        else:
            raise AssertionError("a missing env entry was silently defaulted")
    finally:
        if before is None:
            os.environ.pop("REPORTING_ENV", None)
        else:
            os.environ["REPORTING_ENV"] = before


def test_the_shipped_config_is_coherent_in_every_environment():
    """The one test here that pins the config this repo actually ships."""
    import os
    before = os.environ.get("REPORTING_ENV")
    try:
        for env in ("local", "dev", "uat", "prod"):
            os.environ["REPORTING_ENV"] = env
            R = _retention_shipped()
            # It refuses rather than returning a bad number, so reaching the
            # assertions at all is most of the test. They pin the shape.
            result = R.check_reproducibility_window()
            assert result["checked"], env
            for c in result["checked"]:
                assert c["landing_keep_years"] >= c["tag_keep_years"], (env, c)
    finally:
        if before is None:
            os.environ.pop("REPORTING_ENV", None)
        else:
            os.environ["REPORTING_ENV"] = before


def _retention_shipped():
    """The retention module against the REAL retention.yml."""
    import pathlib
    import sys
    config_dir()                       # copies the shipped retention.yml
    for name in [m for m in sys.modules if m.startswith("reporting_platform")]:
        del sys.modules[name]
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from reporting_platform.retention import retention
    return retention


# ----------------------------------------------------- snapshot tags (phase 5)
# `snapshot/<feed>/<bd>/<run>` is what an INGEST pins now. It used to be cut as
# `published/<bd>/<run>`, which meant an ingest was retained for the full
# reproducibility window and counted by every check that asks whether a
# PUBLICATION can still be read. These hold the split apart.
SNAP_YML = """
references:
  published_tags:
    default_keep_years: 10
    per_report: {}
  snapshot_tags:
    keep_years: 7
"""


def test_a_snapshot_tag_is_judged_by_its_own_window():
    R = _retention(SNAP_YML)
    n = FakeNessie([_tag("snapshot/fo_trade/2018-01-04/old", days_ago=8 * YEAR),
                    _tag("snapshot/fo_trade/2026-08-01/new", days_ago=1)])
    assert R.expire_snapshot_tags(n, dry_run=False) == \
        ["snapshot/fo_trade/2018-01-04/old"]
    assert n.deleted == ["snapshot/fo_trade/2018-01-04/old"]


def test_the_two_sweeps_do_not_touch_each_others_tags():
    """The whole point of the rename: a snapshot must not be kept for the
    published window, and a publication must not be expired on the snapshot
    one."""
    R = _retention(SNAP_YML)
    refs = [_tag("published/rep/2018-01-04/p", days_ago=8 * YEAR),
            _tag("snapshot/fo_trade/2018-01-04/s", days_ago=8 * YEAR)]
    # 8 years old: inside the 10-year published window, past the 7-year
    # snapshot one.
    assert R.expire_tags(FakeNessie(refs), dry_run=True) == []
    assert R.expire_snapshot_tags(FakeNessie(refs), dry_run=True) == \
        ["snapshot/fo_trade/2018-01-04/s"]


def test_every_snapshot_for_a_date_is_kept_on_its_own_merits():
    """N feeds landing one COB date cut N snapshot tags. The defect this
    inherits from the published sweep kept only the newest per date."""
    R = _retention(SNAP_YML)
    n = FakeNessie([_tag(f"snapshot/{f}/2026-08-01/r{i}", days_ago=1)
                    for i, f in enumerate(("fo_trade", "ref_rating",
                                           "ref_counterparty"))])
    assert R.expire_snapshot_tags(n, dry_run=True) == []


def test_a_name_under_snapshot_that_is_not_one_is_left_alone():
    R = _retention(SNAP_YML)
    n = FakeNessie([_tag("snapshot/hand-made", days_ago=40 * YEAR)])
    assert R.expire_snapshot_tags(n, dry_run=True) == []
    assert n.deleted == []


def test_an_unreadable_snapshot_window_refuses_rather_than_guessing():
    for bad in ("keep_years: 0", "keep_years: '7'", ""):
        R = _retention("references:\n  published_tags:\n"
                       "    default_keep_years: 10\n"
                       "  snapshot_tags:\n"
                       + (f"    {bad}\n" if bad else "    other: 1\n"))
        n = FakeNessie([_tag("snapshot/fo_trade/2010-01-04/x",
                             days_ago=20 * YEAR)])
        try:
            R.expire_snapshot_tags(n, dry_run=True)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted snapshot_tags {bad!r}")
        assert n.deleted == []


def test_the_shipped_snapshot_window_resolves_in_every_environment():
    import os

    from tests.support import CONFIG
    for env in ("local", "dev", "uat", "prod"):
        os.environ["REPORTING_ENV"] = env
        R = _retention((CONFIG / "retention.yml").read_text(encoding="utf-8"))
        n = FakeNessie([_tag("snapshot/fo_trade/2026-08-01/r", days_ago=1)])
        assert R.expire_snapshot_tags(n, dry_run=True) == []
    os.environ["REPORTING_ENV"] = "local"


def test_a_snapshot_window_longer_than_landing_is_allowed():
    """Unlike a published pin. Nothing is REPRODUCED from a snapshot, so it
    makes no claim on landing evidence and the interlock does not bind it --
    which is exactly why it may be set independently."""
    from tests.support import CONFIG
    R = _retention((CONFIG / "retention.yml").read_text(encoding="utf-8"))
    # The shipped config is coherent; the point is that the interlock reads
    # published_tags only. `snapshot_tags` is set independently and is not in
    # `checked` at all -- if it ever appears there, binding it has crept back.
    from reporting_platform.common.context import longest_tag_retention_years
    result = R.check_reproducibility_window()
    assert result["checked"]
    for c in result["checked"]:
        assert c["tag_keep_years"] <= longest_tag_retention_years(), c
