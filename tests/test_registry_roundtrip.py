"""The feed console must not defeat a convention by saving a feed.

`ui/registry._block` omits a key whose value matches what the feed inherits.
Get that comparison wrong and every console save pins inherited values into
the individual feed block, where the convention can no longer change them --
in a diff that looks deliberate. See docs/DECISIONS.md#feed-conventions.
"""
from __future__ import annotations

import dataclasses
import difflib

from tests.support import config_dir, registry_on

# Keys the shipped `ref_src` convention supplies. None of these may appear in
# the block of a feed that inherits them.
SUPPLIED = ("source_system", "expected_min_rows")


def _setup():
    d = config_dir()
    registry = registry_on(d)
    from reporting_platform.common.context import feeds
    return d, registry, feeds


def _block_of(text: str, name: str) -> str:
    blk = text[text.index(f"- name: {name}"):]
    end = blk.find("\n  - name:")
    return blk if end == -1 else blk[:end]


def _changed(before: str, after: str) -> list[str]:
    return [l for l in difflib.unified_diff(before.splitlines(), after.splitlines(),
                                            lineterm="", n=0)
            if l[:1] in "+-" and l[1:2] not in "+-"]


def test_noop_save_pins_nothing():
    """A save that changes nothing must not write inherited keys into a block.

    Two pre-existing ruamel round-trip quirks show up regardless of
    conventions -- a dropped blank line between blocks, and a re-wrapped long
    description. They are not what this asserts on; what must not appear is a
    change to a key the convention supplies.
    """
    d, registry, feeds = _setup()
    before = (d / "feeds.yml").read_text()
    for name in feeds():
        registry.update(registry.spec_from_feed(feeds()[name]))
    after = (d / "feeds.yml").read_text()
    leaked = [l for l in _changed(before, after) if any(k in l for k in SUPPLIED)]
    assert not leaked, "convention-supplied keys leaked:\n" + "\n".join(leaked)


def test_resolved_feed_survives_a_roundtrip():
    d, registry, feeds = _setup()
    before = dataclasses.asdict(feeds()["ref_counterparty"])
    registry.update(registry.spec_from_feed(feeds()["ref_counterparty"]))
    after = dataclasses.asdict(feeds()["ref_counterparty"])
    assert before == after, f"\nBEFORE {before}\nAFTER  {after}"


def test_new_feed_inherits_without_repeating():
    d, registry, feeds = _setup()
    spec = dataclasses.replace(
        registry.spec_from_feed(feeds()["ref_counterparty"]),
        name="ref_newthing", description="A new reference feed.",
        filename_pattern=r'NEW_(?P<business_date>\d{8})\.csv')
    registry.add(spec)
    blk = _block_of((d / "feeds.yml").read_text(), "ref_newthing")
    assert "convention: ref_src" in blk, blk
    for key in SUPPLIED:
        assert key not in blk, f"{key} was pinned into the new block:\n{blk}"


def test_clearing_a_convention_pins_what_it_supplied():
    """The reverse direction, and the one that would lose data quietly.

    A feed whose convention is removed must keep the values it was
    inheriting. Letting it fall back to `defaults:` would, for a
    pipe-delimited feed, land one column holding the whole row -- and not
    fail.
    """
    d, registry, feeds = _setup()
    cleared = dataclasses.replace(
        registry.spec_from_feed(feeds()["ref_counterparty"]), convention="")
    registry.update(cleared)
    blk = _block_of((d / "feeds.yml").read_text(), "ref_counterparty")
    assert "convention:" not in blk, blk
    assert "source_system: REF_SRC" in blk, blk
    assert "expected_min_rows: 10" in blk, blk
