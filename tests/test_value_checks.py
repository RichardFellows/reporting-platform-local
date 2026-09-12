"""The loader refuses what the console refuses, and they share the function.

THE ASYMMETRY THIS CLOSES, measured before it was closed: `ui/registry.validate`
rejected `cadence: fortnightly`, a multi-character `delimiter` and an unknown
`file_encoding`; the LOADER accepted all three. So the form refused what a hand
edit, a merge, or a migration script could still write -- and `fortnightly`
then behaved as `daily` with nothing raising anywhere, which for a weekly feed
reports every non-delivery day as a gap. That is the exact failure
`cadence:` was added to remove, reinstated by a typo the form would have caught.

Each check is asserted from BOTH SIDES here, because one shared function is
the only thing that keeps them from drifting apart again.

No stack. See docs/DECISIONS.md#feed-conventions
"""
from __future__ import annotations

import dataclasses

from tests.support import config_dir, feeds_from, registry_on, synthetic


def _load_raises(feed_extra: str) -> str:
    try:
        feeds_from(synthetic("", feed_extra))
    except Exception as exc:                                 # noqa: BLE001
        return str(exc)
    raise AssertionError(f"the loader accepted {feed_extra!r}")


def _form_raises(**overrides) -> str:
    d = config_dir(synthetic())
    registry = registry_on(d)
    payload = {"name": "trs_x", "description": "d", "source_system": "TRS",
               "filename_pattern": r"X_(?P<cob_date>\d{8})\.csv",
               "business_key": ["k"], "columns": ["k"], **overrides}
    try:
        registry.validate(registry.FeedSpec.from_payload(payload),
                          existing=set())
    except registry.FeedValidationError as exc:
        return " | ".join(exc.errors.values())
    raise AssertionError(f"the form accepted {overrides!r}")


# ----------------------------------------------------------------- cadence
def test_an_unknown_cadence_is_refused_at_load():
    """THE SILENT ONE. `find_gaps` is `if how == "weekly": ... else: <daily>`,
    so any other value is daily and nothing says so."""
    msg = _load_raises("    cadence: fortnightly\n")
    assert "cadence" in msg and "daily, weekly" in msg, msg
    assert "t_one.yml" in msg, msg


def test_an_unknown_cadence_is_refused_by_the_form():
    assert "daily, weekly" in _form_raises(cadence="fortnightly")


# ------------------------------------------------------------ schema_drift
def test_an_unknown_schema_drift_is_refused_at_load():
    """`ingest_feed` also checks this and keeps doing so -- but there it costs
    a delivery, and here it costs a parse."""
    msg = _load_raises("    schema_drift: Fail\n")
    assert "schema_drift" in msg and "warn, fail" in msg, msg


def test_an_unknown_schema_drift_is_refused_by_the_form():
    assert "warn, fail" in _form_raises(schema_drift="Fail")


# ------------------------------------------------------- expected_min_rows
def test_a_negative_row_floor_is_refused_at_load():
    """`row_count < -1` is never true, so a negative floor is a declared
    check that cannot fire. `0` is how you say there is no floor."""
    msg = _load_raises("    expected_min_rows: -5\n")
    assert "negative" in msg and "Use 0" in msg, msg


def test_a_negative_row_floor_is_refused_by_the_form():
    assert "negative" in _form_raises(expected_min_rows=-5)


# --------------------------------------------------- delimiter / quote_char
def test_a_multi_character_delimiter_is_refused_at_load():
    """It becomes Spark's `sep`, which takes one character. A longer one
    splits on neither and lands one column holding the whole row -- every
    declared column then reports as missing, and the load does not fail.

    The CONTROL FILE's delimiter was already checked at load and the feed's
    own was not, which was the wrong way round: the feed's is the one every
    delivery is read with.
    """
    msg = _load_raises('    delimiter: "||"\n')
    assert "delimiter" in msg and "not 1" in msg, msg


def test_a_multi_character_quote_char_is_refused_at_load():
    msg = _load_raises("""    quote_char: "''"\n""")
    assert "quote_char" in msg and "not 1" in msg, msg


def test_a_multi_character_delimiter_is_refused_by_the_form():
    assert "not 1" in _form_raises(delimiter="||")


# ----------------------------------------------------------- file_encoding
def test_an_unknown_encoding_is_refused_at_load():
    msg = _load_raises("    file_encoding: utf-99\n")
    assert "file_encoding" in msg and "utf-8" in msg, msg


def test_an_unknown_encoding_is_refused_by_the_form():
    assert "utf-8" in _form_raises(file_encoding="utf-99")


# ------------------------------------------------ what the FORM shows of it
def test_the_form_strips_the_filename_and_nothing_else():
    """THE REGRESSION NOTHING CAUGHT. `_FILE_PREFIX_RE` was the literal
    `feeds.yml: `, which stopped matching the moment the registry became a
    directory -- every form error then carried a path in front of it, and the
    whole suite stayed green because the assertions are all on what a message
    SAYS, not on what was stripped off the front.

    Both shapes matter: a NEW feed's error names the conventional relative
    path, and an EXISTING feed's names the absolute one `context.where()`
    actually read.
    """
    assert not _form_raises(cadence="fortnightly").startswith("feeds"), \
        "the relative path leaked into the form message"

    d = config_dir()
    registry = registry_on(d)
    from reporting_platform.common.context import feeds
    spec = dataclasses.replace(registry.spec_from_feed(feeds()["fo_trade"]),
                               cadence="nightly")
    try:
        registry.validate(spec, existing=set(feeds()), updating=True)
    except registry.FeedValidationError as exc:
        message = " ".join(exc.errors.values())
    assert message.startswith("`cadence:"), \
        f"an absolute path leaked into the form message: {message!r}"


# -------------------------------------------------- the defaults stay put
def test_a_key_nothing_declares_keeps_the_dataclass_default():
    """The checks run on DECLARED values only. Restating each default beside
    its check would be a second copy of every default, which is the failure
    this file is organised against -- so an absent key is left absent and the
    `Feed` default applies."""
    registry, _ = feeds_from(synthetic())
    fd = registry["t_one"]
    assert (fd.cadence, fd.schema_drift, fd.expected_min_rows) == \
        ("daily", "warn", 0)
