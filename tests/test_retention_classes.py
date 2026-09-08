"""Retention classes, and the one lateness expectation. Phase 7.

TWO SEPARATE CONFIG FILES ON PURPOSE. The class NAME is a per-feed key in
`feeds.yml` -- it is a property of the obligation the feed carries, and it is
edited by whoever onboards the feed. The class WINDOWS live in
`retention.yml`, per environment, and are edited by whoever owns retention
policy. Naming a class the windows file does not declare is refused at LOAD,
which is this repo's house style for config: `resolve_supersession_config`, an
undefined `convention` and an unreadable `tag_retention_years` all refuse
rather than falling back.

CLASSES GOVERN THE EVIDENCE PREFIXES ONLY -- `landing/` and `quarantine/`.
Table keep-sets stay per LAYER, because a table window says how much history
is queryable and `find_pending` derives one keep-set per feed from that feed's
own landing prefix. `PREFIX_CLASSES` is the closed list and `class_keep_years`
refuses a prefix outside it, so this cannot spread by accident.

`expected_by` is here rather than in its own file because it is the same
shape: a per-feed scalar, validated at load, inherited through `conventions:`,
and round-tripped by the console.
"""
from __future__ import annotations

from tests.support import REPO, config_dir, feeds_from, synthetic

CLASSES = """
retention_classes:
  standard: {}
  operational: {}
environments:
  local:
    landing:
      keep_years: 10
      classes:
        operational: {keep_years: 7}
    quarantine: {keep_years: 10}
    raw: {keep_business_days: 10, keep_month_ends: 80}
references:
  published_tags: {default_keep_years: 10, per_report: {}}
"""


def _ctx(feeds_yml: str | None = None, retention_yml: str = CLASSES):
    d = config_dir(feeds_yml)
    (d / "retention.yml").write_text(retention_yml, encoding="utf-8")
    from tests.support import _purge
    _purge()
    from reporting_platform.common import context
    return context, d


def _load(feeds_yml: str | None = None, retention_yml: str = CLASSES):
    """`_ctx` plus the load itself.

    `feeds()` is lazy and cached on the file's mtime, so importing the module
    proves nothing -- a refusal at LOAD means at the first `feeds()` call, and
    a test that only imported would pass whatever the config said.
    """
    ctx, d = _ctx(feeds_yml, retention_yml)
    ctx.feeds()
    return ctx, d


# ------------------------------------------------------------ the class name
def test_the_default_class_is_standard_and_is_always_declared():
    """`standard` is what every feed that says nothing gets, so it cannot be
    something retention.yml has to remember to declare -- a windows file that
    omitted it would refuse the whole config for a key nobody typed."""
    ctx, _ = _ctx(retention_yml="references:\n"
                                "  published_tags: {default_keep_years: 10}\n")
    assert "standard" in ctx.retention_classes()
    got, _ = feeds_from(synthetic())
    assert got["t_one"].retention_class == "standard"


def test_a_class_retention_yml_does_not_declare_is_refused_at_load():
    """Not defaulted, and not deferred to the sweep. A feed carrying a class
    nobody declared would be swept on the prefix default -- which is the
    LONGER window, so the failure would be silent storage rather than lost
    evidence, and nobody would ever find it."""
    try:
        _load(synthetic(feed_extra="    retention_class: gold\n"))
    except ValueError as exc:
        assert "gold" in str(exc) and "retention.yml" in str(exc), str(exc)
    else:
        raise AssertionError("an undeclared retention class was accepted")


def test_a_class_is_inherited_through_a_convention():
    """The point of `conventions:` is that variation is mostly per SOURCE
    SYSTEM, and a retention obligation usually is too -- one upstream's feeds
    carry the same one. `effective_defaults()` is the only implementation of
    that ordering and this pins that the new key goes through it."""
    ctx, _ = _ctx(synthetic(
        conventions="conventions:\n  src:\n    retention_class: operational\n",
        feed_extra="    convention: src\n"))
    assert ctx.feeds()["t_one"].retention_class == "operational"
    # And the feed still wins over its convention.
    ctx, _ = _ctx(synthetic(
        conventions="conventions:\n  src:\n    retention_class: operational\n",
        feed_extra="    convention: src\n    retention_class: standard\n"))
    assert ctx.feeds()["t_one"].retention_class == "standard"


# ---------------------------------------------------------- the class window
def test_a_class_with_no_window_falls_back_to_the_prefix_default():
    """Over-retaining is the safe direction, and it is the whole reason
    `quarantine:` ships with no `classes:` block at all. A class that shortens
    landing and says nothing about quarantine gets quarantine's own window."""
    ctx, _ = _ctx()
    assert ctx.class_keep_years("landing", "operational") == 7
    assert ctx.class_keep_years("landing", "standard") == 10
    assert ctx.class_keep_years("quarantine", "operational") == 10


def test_classes_govern_the_evidence_prefixes_only():
    """`PREFIX_CLASSES` is closed. A raw or prepared window is per LAYER --
    per-feed raw retention would fight `find_pending`, which derives one
    keep-set per feed from that feed's landing prefix, and the two would
    disagree about which COB dates still exist."""
    ctx, _ = _ctx()
    assert set(ctx.PREFIX_CLASSES) == {"landing", "quarantine"}
    for prefix in ("raw", "prepared", "reporting", "ready"):
        try:
            ctx.class_keep_years(prefix, "operational")
        except ValueError as exc:
            assert prefix in str(exc), str(exc)
        else:
            raise AssertionError(f"{prefix} accepted a retention class")


def test_an_unreadable_class_window_refuses_rather_than_guessing():
    for bad in ("keep_years: 0", "keep_years: '7'", "keep_years: -1"):
        ctx, _ = _ctx(retention_yml=CLASSES.replace("keep_years: 7", bad))
        try:
            ctx.class_keep_years("landing", "operational")
        except (ValueError, TypeError):
            pass
        else:
            raise AssertionError(f"accepted a class window of {bad!r}")


def test_the_shipped_config_declares_every_class_its_feeds_name():
    """The one test here that pins what this repo actually ships. It is the
    same check the load does, run over the real pair of files."""
    from tests.support import config_dir as real
    real()
    from reporting_platform.common.context import feeds, retention_classes
    declared = retention_classes()
    for name, fd in sorted(feeds().items()):
        assert fd.retention_class in declared, (name, fd.retention_class)


# ------------------------------------------------------------- expected_by
def test_expected_by_must_be_quoted_and_is_refused_if_it_is_not():
    """Unquoted `7:00` is SEXAGESIMAL in YAML 1.1 and loads as the integer 420.

    THE LEADING ZERO IS WHY THIS IS A TRAP RATHER THAN A TYPO. pyyaml's int
    resolver requires [1-9] first, so unquoted `07:00` survives as a string
    and unquoted `7:00` does not -- the config would work for every padded
    feed and break on the first one somebody wrote `9:00`. Both cases are
    asserted here so the pair cannot drift apart.

    Accepting the 420 would take an integer into `deadline()`, which splits on
    ':' -- failing somewhere else, later, naming neither the feed nor the file.
    """
    import yaml
    assert yaml.safe_load("a: 7:00") == {"a": 420}     # the actual behaviour
    assert yaml.safe_load("a: 07:00") == {"a": "07:00"}
    try:
        _load(synthetic(feed_extra="    expected_by: 7:00\n"))
    except ValueError as exc:
        assert "quote" in str(exc).lower() and "420" in str(exc), str(exc)
    else:
        raise AssertionError("an unquoted expected_by was accepted")
    # And the padded one, which loads as a string, is still a valid time.
    ctx, _ = _load(synthetic(feed_extra="    expected_by: 07:00\n"))
    assert ctx.feeds()["t_one"].expected_by == "07:00"


def test_expected_by_is_a_wall_clock_time_in_the_24_hour_day():
    ctx, _ = _load(synthetic(feed_extra='    expected_by: "07:30"\n'))
    assert ctx.feeds()["t_one"].expected_by == "07:30"
    for bad in ("24:00", "7:30", "07:60", "0730", "7am", "07:30:00"):
        try:
            _load(synthetic(feed_extra=f'    expected_by: "{bad}"\n'))
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted expected_by {bad!r}")


def test_no_expected_by_is_not_a_deadline_of_midnight():
    """A feed that has made no promise must not acquire one by omission. The
    lateness check skips it; a default of "00:00" would report every feed in
    the platform as late every day, which is how a monitor gets switched
    off."""
    ctx, _ = _ctx(synthetic())
    assert ctx.feeds()["t_one"].expected_by == ""


def test_the_deadline_is_the_day_after_the_cob_date():
    """Deliberately fixed at +1 rather than configurable, and deliberately in
    the FORGIVING direction: a COB date has to have ENDED before the
    extract can be taken, and a reference snapshot that legitimately arrives
    the same day is then judged against a later deadline than it needed. This
    check can under-report lateness and cannot invent it."""
    from datetime import date, datetime, timezone
    config_dir()
    from reporting_platform.monitoring.lateness import deadline
    assert deadline(date(2026, 8, 19), "07:30") == \
        datetime(2026, 8, 20, 7, 30, tzinfo=timezone.utc)


def test_a_backfill_is_one_event_and_not_n_missed_deadlines():
    """Ten dates that all arrived in one write is a bulk load -- a seed, a
    migration, a re-delivery of history after an outage. Reporting it as ten
    findings buries anything else the check found, which is how a monitor
    stops being read. It is a derivation with nothing to tune: more than one
    COB date, exactly one arrival day.

    The finding is DESCRIBED differently, never suppressed. `total_late` and
    `--fail-on-late` are unaffected, because a backfill of dates that were due
    weeks ago genuinely is late.
    """
    config_dir()
    from reporting_platform.monitoring.lateness import is_bulk_load
    one_day = [{"arrived": "2026-09-06T05:59:57+00:00"},
               {"arrived": "2026-09-06T06:37:11+00:00"}]
    assert is_bulk_load(one_day)
    assert not is_bulk_load(one_day[:1])            # one date is not a pattern
    assert not is_bulk_load(one_day[:1] + [{"arrived": "2026-09-05T05:00:00+00:00"}])


def test_the_lateness_check_needs_no_spark():
    """`received_at` is the landing object's LastModified, which the registry
    already holds -- so this runs in the Airflow task process directly rather
    than through `scripts/_spark_task.py`. A Spark import here would make a
    monitoring check cost a JVM start, and `platform_housekeeping` runs it
    nightly alongside the completeness check."""
    src = (REPO / "reporting_platform" / "monitoring" / "lateness.py").read_text(
        encoding="utf-8")
    for token in ("pyspark", "spark_session", "_spark_task"):
        assert token not in src, token
