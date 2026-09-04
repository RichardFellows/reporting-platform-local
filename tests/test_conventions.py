"""`conventions:` resolution, and the failures that must not be silent.

See docs/DECISIONS.md#feed-conventions. Every `raises` case here is a thing
that, left unchecked, produces a working platform doing the wrong thing --
which is this repo's recurring failure and the reason the section is strict.
"""
from __future__ import annotations

from tests.support import feeds_from, synthetic


def _one(conventions="", feed_extra=""):
    return feeds_from(synthetic(conventions, feed_extra))[0]["t_one"]


def _raises(conventions="", feed_extra=""):
    try:
        _one(conventions, feed_extra)
    except ValueError as exc:
        return str(exc)
    except Exception as exc:                                   # noqa: BLE001
        # Anything but ValueError is a bug in the guard, not the guard firing.
        raise AssertionError(
            f"expected ValueError, got {type(exc).__name__}: {exc}") from exc
    raise AssertionError("expected a ValueError, none raised")


# ------------------------------------------------------------- resolution
def test_convention_supplies_values():
    fd = _one('conventions:\n  ref: {delimiter: "|", schema_drift: fail}\n',
              "    convention: ref\n")
    assert fd.delimiter == "|", fd.delimiter
    assert fd.schema_drift == "fail", fd.schema_drift
    assert fd.convention == "ref", fd.convention


def test_feed_block_overrides_convention():
    fd = _one('conventions:\n  ref: {delimiter: "|"}\n',
              '    convention: ref\n    delimiter: ";"\n')
    assert fd.delimiter == ";", fd.delimiter


def test_convention_overrides_defaults():
    fd = _one("conventions:\n  ref: {landing_prefix: other}\n",
              "    convention: ref\n")
    assert fd.landing_prefix == "other", fd.landing_prefix


def test_no_conventions_section_is_unchanged():
    fd = _one()
    assert fd.convention == "" and fd.delimiter == ","


# ----------------------------------------------------------- the failures
def test_undefined_convention_is_an_error():
    """Silent fallback to `defaults:` would give a feed configured subtly
    wrong rather than one that does not exist.

    ASSERTS ON THE FEED NAME, and that is the point of the assertion.
    `effective_defaults()` raises its own "convention is not defined" for the
    same input, so a looser check passes even with `_feeds_at`'s guard
    removed -- verified by removing it. Only the guard in `_feeds_at` knows
    which FEED named the missing convention, which is the half of the message
    worth having when feeds.yml has forty blocks in it.
    """
    msg = _raises('conventions:\n  ref: {delimiter: "|"}\n',
                  "    convention: rfe\n")
    assert "feed 't_one'" in msg, msg
    assert "not defined" in msg and "Available: ref" in msg, msg


def test_unknown_key_in_a_convention_is_an_error():
    # `delimeter:` would otherwise be dropped by the `allowed` filter and
    # never apply, with nothing anywhere saying so.
    msg = _raises('conventions:\n  ref: {delimeter: "|"}\n',
                  "    convention: ref\n")
    assert "unknown key" in msg and "delimeter" in msg, msg
    # The two keys a convention may not set are not offered as valid ones.
    valid = msg.split("Valid keys are:")[1]
    assert "name" not in valid.split(", "), valid
    assert "convention" not in valid.split(", "), valid


def test_convention_may_not_set_name():
    msg = _raises("conventions:\n  ref: {name: stolen}\n",
                  "    convention: ref\n")
    assert "may not set 'name'" in msg, msg


def test_conventions_do_not_chain():
    msg = _raises('conventions:\n  a: {delimiter: "|"}\n  b: {convention: a}\n',
                  "    convention: b\n")
    assert "may not set 'convention'" in msg and "do not chain" in msg, msg


def test_conventions_section_must_be_a_mapping():
    msg = _raises("conventions: [ref]\n")
    assert "must be a mapping" in msg, msg


def test_a_convention_must_be_a_mapping():
    msg = _raises('conventions:\n  ref: "|"\n', "    convention: ref\n")
    assert "must be a mapping" in msg, msg


# ------------------------------------------- what the SHIPPED config resolves to
def test_shipped_feeds_resolve_as_expected():
    """Pins the real feeds.yml, which is what the platform actually runs.

    `ref_src` supplies source_system and expected_min_rows to three feeds; a
    feed that names no convention is unaffected.
    """
    feeds, _ = feeds_from()
    for name in ("ref_counterparty", "ref_rating", "ref_collateral"):
        fd = feeds[name]
        assert fd.convention == "ref_src", (name, fd.convention)
        assert fd.source_system == "REF_SRC", (name, fd.source_system)
        assert fd.expected_min_rows == 10, (name, fd.expected_min_rows)
    assert feeds["fo_trade"].convention == ""
    assert feeds["fo_trade"].source_system == "FO_SRC"
    assert feeds["fo_trade"].expected_min_rows == 100
