"""Building a filename a feed's own pattern will match.

`common/filenames.py` is what the inbox gate uses to rename a legacy
upstream's file into one `landing/` accepts, and what the console uses to
generate a sample delivery. Its failure mode is silent by construction: a name
`parse_filename` does not match lands anyway, `find_pending` never sees it,
and the feed reports nothing pending forever with no error anywhere. The
module answers that with a round-trip check, and this pins the check as well
as the rendering.

Pure string work over `feeds.yml` -- no stack. What it cannot tell you is
whether the gate then writes the object under that name; that was verified by
dropping a file into `inbox/` and reading `landing/` back.
"""
from __future__ import annotations

from datetime import date

from tests.support import feeds_from

COB = date(2026, 8, 3)


def _mod():
    from reporting_platform.common import filenames
    return filenames


def _feed(pattern: str):
    """One feed whose `filename_pattern` is the thing under test."""
    yml = f"""
defaults:
  landing_prefix: landing
  delimiter: ","
feeds:
  - name: t_one
    description: d
    source_system: SRC
    filename_pattern: '{pattern}'
    business_key: [k]
    columns: [k, v]
"""
    fds, _ = feeds_from(yml)
    return fds["t_one"]


# ------------------------------------------------------------- rendering
def test_the_ordinary_shape():
    f = _feed(r'FEED_(?P<cob_date>\d{8})\.csv')
    assert _mod().render_filename(f, COB) == "FEED_20260803.csv"


def test_a_v1_delivery_does_not_carry_the_version_marker():
    """The optional group is rendered only when a version is asked for, so
    the first delivery of a date is `FEED_20260803.csv`, not `..._v1.csv`.
    """
    f = _feed(r'FEED_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv')
    assert _mod().render_filename(f, COB) == "FEED_20260803.csv"


def test_a_re_delivery_carries_it():
    f = _feed(r'FEED_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv')
    assert _mod().render_filename(f, COB, version=2) == "FEED_20260803_v2.csv"


def test_a_pattern_with_no_single_literal_form_is_refused():
    """`\\d+` outside a named group matches many strings and names none."""
    m = _mod()
    f = _feed(r'FEED_(?P<cob_date>\d{8})_\d+\.csv')
    try:
        m.render_filename(f, COB)
    except m.FilenameError as exc:
        assert "no single literal form" in str(exc)
    else:
        raise AssertionError("an unbuildable pattern must refuse")


def test_the_round_trip_is_what_makes_the_name_safe():
    """Every name returned has been fed back through the feed's own
    `parse_filename`. Pinned from the outside: whatever comes out parses to
    the date asked for.
    """
    f = _feed(r'FEED_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv')
    for version in (None, 2, 7):
        name = _mod().render_filename(f, COB, version=version)
        assert f.parse_filename(name)[0] == COB


# ------------------------------------------- the escape, inside a group
def test_an_escaped_paren_is_not_a_group_boundary():
    """THE OFF-BY-ONE. `\\)` is a literal parenthesis in the filename, and
    skipping only the backslash leaves the paren itself to be counted as
    structural -- so the group is closed at the wrong index.
    """
    m = _mod()
    assert m._closing_paren(r"(?:a\)b)", 0) == 7


def test_an_escaped_open_paren_does_not_unbalance_a_balanced_pattern():
    """Counted as structural, it never closes, and a perfectly good pattern
    is rejected as `unbalanced parentheses in filename_pattern`.
    """
    assert _mod()._closing_paren(r"(?:a\(b)", 0) == 7


def test_a_nested_group_still_closes_at_the_outer_paren():
    assert _mod()._closing_paren(r"(?:_(?P<version>\d+))?", 0) == 20


def test_a_genuinely_unbalanced_pattern_is_still_refused():
    m = _mod()
    try:
        m._closing_paren("(?:abc", 0)
    except m.FilenameError as exc:
        assert "unbalanced" in str(exc)
    else:
        raise AssertionError("an unclosed group must refuse")


def test_a_filename_containing_parentheses_round_trips():
    """End to end on a pattern whose literal name has parentheses in it."""
    f = _feed(r'FEED_(?P<cob_date>\d{8})(?:\(v(?P<version>\d+)\))?\.csv')
    m = _mod()
    assert m.render_filename(f, COB, version=3) == "FEED_20260803(v3).csv"
    assert f.parse_filename("FEED_20260803(v3).csv")[0] == COB


# ------------------------------------------------------ literal_from_regex
def test_a_control_file_name_is_built_from_its_regex_template():
    """`'positions\\.ctl'` is a REGEX: the backslash escapes the dot and is
    not a character in any filename, so the gate cannot use it as a key.
    """
    assert _mod().literal_from_regex(r"positions\.ctl", {},
                                     what="the control file name") == \
        "positions.ctl"


def test_a_named_group_is_filled_from_the_values_given():
    assert _mod().literal_from_regex(r"(?P<stem>[a-z]+)\.ctl",
                                     {"stem": "positions"},
                                     what="the control file name") == \
        "positions.ctl"


def test_a_group_with_no_value_refuses_and_says_what_it_knows():
    m = _mod()
    try:
        m.literal_from_regex(r"(?P<stem>x)\.ctl", {"other": "1"},
                             what="the control file name")
    except m.FilenameError as exc:
        assert "stem" in str(exc) and "other" in str(exc)
    else:
        raise AssertionError("an unfillable group must refuse")


def test_a_character_class_outside_a_group_is_refused():
    m = _mod()
    try:
        m.literal_from_regex(r"positions\d+\.ctl", {},
                             what="the control file name")
    except m.FilenameError as exc:
        assert "no single literal form" in str(exc)
    else:
        raise AssertionError("an unbuildable name must refuse")


# ==================================== reading a name, not building one =======
# `stem_pattern` and `sample_name` exist because `render_filename` is
# deliberately strict -- it refuses `\d`, `{8}` and `[A-Z]`, which is right for
# writing a name a feed must accept and much too strict for recognising one.

def _filenames():
    from reporting_platform.common import filenames
    return filenames


def test_stem_pattern_strips_a_literal_extension():
    f = _filenames()
    assert f.stem_pattern(r"MARGIN_(?P<cob_date>\d{8})\.csv") == r"MARGIN_(?P<cob_date>\d{8})"
    assert f.stem_pattern(r"POS_\d{8}\.TXT") == r"POS_\d{8}"
    assert f.stem_pattern(r"positions\.csv") == "positions"


def test_a_pattern_with_no_single_extension_has_no_stem_shape():
    """`\\.(csv|txt)` names two extensions, so nothing can say where the stem
    ends. The caller falls back to matching the control suffix alone -- which
    is today's behaviour -- and the load-time check refuses two such feeds."""
    f = _filenames()
    assert f.stem_pattern(r"A_(?P<cob_date>\d{8})\.(csv|txt)") is None
    assert f.stem_pattern("nodothere") is None


def test_sample_name_reads_what_render_filename_refuses():
    f = _filenames()
    assert f.sample_name(r"POS_\d{8}") == "POS_00000000"
    assert f.sample_name(r"POSITIONS_[A-Z]") == "POSITIONS_A"
    assert f.sample_name(r"MARGIN_(?P<cob_date>\d{8})") == "MARGIN_00000000"
    # An optional group is dropped: the FIRST delivery is the representative
    # one, which is the same choice render_filename makes.
    assert f.sample_name(r"A_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?") == "A_00000000"
    # Alternation takes the first branch.
    assert f.sample_name(r"(?:POS|POSITION)_\d{4}") == "POS_0000"


def test_sample_name_gives_up_rather_than_guessing():
    f = _filenames()
    assert f.sample_name(r"A_(?P<cob_date>\d{8}") is None      # unbalanced
    assert f.sample_name(r"A_.") is None                        # any character


def test_a_sampled_name_is_matched_by_the_pattern_it_came_from():
    """The property that makes it usable as a probe: it has to be a name the
    feed would really claim, or the collision check would compare fictions."""
    import re
    f = _filenames()
    for pattern in (r"POS_\d{8}", r"POSITIONS_[A-Z]", r"MARGIN_(?P<cob_date>\d{8})",
                    r"A_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?",
                    r"(?P<cob_date>\d{8})_positions"):
        sample = f.sample_name(pattern)
        assert sample is not None, pattern
        assert re.fullmatch(pattern, sample), (pattern, sample)
