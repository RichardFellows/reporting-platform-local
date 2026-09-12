"""The registry is a DIRECTORY, and the rules that makes load-bearing.

`feeds.yml` became `feeds/<name>.yml` plus `_defaults.yml` and
`conventions/<name>.yml`. Three things that were free in one file have to be
enforced now that it is many, and one thing that was trivially correct is the
easiest part of this to get subtly wrong -- the cache key.

No stack. Reads and writes throwaway config trees, like the rest of tests/.
See docs/DECISIONS.md#the-registry-is-a-directory
"""
from __future__ import annotations

import os

from tests.support import config_dir, feeds_from, synthetic


def _feeds(d):
    from reporting_platform.common.context import feeds
    return feeds()


def _raises(fn) -> str:
    try:
        fn()
    except Exception as exc:                                 # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"
    raise AssertionError("expected an error, got none")


# ------------------------------------------------- the filename is the identity
def test_a_feed_file_must_declare_the_name_it_is_called():
    """A file whose `name:` disagrees with its filename puts a spelling into
    play that nothing reconciles. The realistic way in is copying a file to
    start a new feed and changing one of the two."""
    d = config_dir(synthetic())
    (d / "feeds" / "t_one.yml").write_text(
        (d / "feeds" / "t_one.yml").read_text().replace("name: t_one",
                                                        "name: t_other"))
    msg = _raises(lambda: _feeds(d))
    assert "t_one.yml" in msg and "t_other" in msg, msg


def test_a_feed_filename_must_be_a_legal_name():
    """The name becomes a table, a DAG id and an S3 prefix, so the filename
    charset IS the identity charset -- a space or a capital is legal in a
    filename and in none of the four places the name has to work."""
    d = config_dir(synthetic())
    (d / "feeds" / "Trade Feed.yml").write_text("name: x\n")
    msg = _raises(lambda: _feeds(d))
    assert "Trade Feed" in msg and "lowercase" in msg, msg


def test_an_underscore_prefixed_file_is_not_a_feed():
    """`_defaults.yml` lives in the same directory as the feeds and is not
    one. Anything else `_`-prefixed is scratch, and must not become a feed
    with no columns that every sweep then reports on."""
    d = config_dir(synthetic())
    (d / "feeds" / "_scratch.yml").write_text("name: whatever\n")
    assert set(_feeds(d)) == {"t_one"}


def test_a_missing_registry_refuses_rather_than_resolving_to_nothing():
    """AN ABSENT REGISTRY IS NOT AN EMPTY ONE. A container without the config
    mounted would otherwise report that the platform ingests nothing, and
    every monitor and sweep would succeed having looked at no feeds -- the
    same failure `models_in` raises over, and the same shape as
    `#an-incomplete-keep-set-refuses`."""
    d = config_dir(synthetic())
    for path in (d / "feeds").rglob("*.yml"):
        path.unlink()
    (d / "feeds").rmdir()
    msg = _raises(lambda: _feeds(d))
    assert "no feed registry" in msg, msg


# ------------------------------------------------------------- the cache key
def _touch(path, content: str) -> None:
    """Rewrite a file and force its mtime forward.

    `os.utime` rather than trusting the write: the assertion below is about
    the cache noticing an mtime change, and a test that depended on two
    writes landing in different filesystem ticks would fail rarely and for
    the wrong reason.
    """
    path.write_text(content)
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns + 10**9, stat.st_mtime_ns + 10**9))


def test_editing_a_feed_file_is_picked_up():
    d = config_dir(synthetic())
    assert _feeds(d)["t_one"].delimiter == ","
    path = d / "feeds" / "t_one.yml"
    _touch(path, path.read_text() + "delimiter: '|'\n")
    assert _feeds(d)["t_one"].delimiter == "|", "the feed file was cached"


def test_editing_the_defaults_file_is_picked_up():
    d = config_dir(synthetic())
    path = d / "feeds" / "_defaults.yml"
    _touch(path, path.read_text().replace('delimiter: ","', "delimiter: ';'"))
    assert _feeds(d)["t_one"].delimiter == ";", "_defaults.yml was cached"


def test_editing_a_convention_file_is_picked_up():
    """THE ONE A DIRECTORY MTIME WOULD MISS, and the reason
    `layout.registry_files` stats every file instead.

    A directory's mtime moves when a file is added or removed and NOT when
    one is edited. Keyed on the directory, this edit would never reach
    Airflow's DAG file processor or the console -- a convention changed, no
    feed changing, and nothing reporting an error. That is exactly the bug
    `_load`'s mtime key was written to kill, which is why it gets a test
    rather than a comment.
    """
    d = config_dir(synthetic('conventions:\n  ref: {delimiter: "|"}\n',
                             "    convention: ref\n"))
    assert _feeds(d)["t_one"].delimiter == "|"
    path = d / "feeds" / "conventions" / "ref.yml"
    _touch(path, "delimiter: ';'\n")
    assert _feeds(d)["t_one"].delimiter == ";", "the convention file was cached"


def test_adding_a_feed_file_is_picked_up():
    d = config_dir(synthetic())
    _touch(d / "feeds" / "t_two.yml",
           "name: t_two\ndescription: d\nsource_system: SRC\n"
           "filename_pattern: 'B_(?P<cob_date>\\d{8})\\.csv'\n"
           "business_key: [k]\ncolumns: [k, v]\n")
    assert set(_feeds(d)) == {"t_one", "t_two"}


# --------------------------------------------------------------- origins()
def test_origins_names_the_tier_each_value_came_from():
    """`config show --origin` is what makes a three-tier merge reviewable, so
    the derivation behind it is pinned here rather than only exercised by
    eye. It reads `effective_defaults()` rather than re-walking the tiers --
    a report of where a value came from that could disagree with where it
    came from would be worse than no report."""
    d = config_dir(synthetic('conventions:\n  ref: {expected_min_rows: 7}\n',
                             "    convention: ref\n"))
    from reporting_platform.common.context import origins
    where = origins("t_one")
    assert where["columns"] == "feed"
    assert where["expected_min_rows"].endswith("conventions/ref.yml"), where
    assert where["delimiter"].endswith("_defaults.yml"), where
    # Nothing declared `cadence`; it is the dataclass default and says so
    # rather than being attributed to the nearest file that might have.
    assert where["cadence"] == "built-in", where
