"""The console can create and edit an archive/control-gated feed.

Before this, ui/registry.FeedSpec had no `delivery` field at all: the
console could sniff a zip and tell you what it found, but the "new feed"
form silently had nowhere to put a delivery: block, so creating one
required a manual feeds.yml edit after the fact. See
docs/DECISIONS.md#the-sniffer's "A real gap this surfaced" and
docs/DELIVERY-SHAPES.md step 5.

Validation reuses context.resolve_delivery_config directly -- the same
function feeds.yml load calls -- so this file does not re-assert every rule
that already has coverage in tests/test_archive.py and tests/test_control.py.
What it covers is the ROUND TRIP: form payload -> FeedSpec -> written YAML
-> loaded Feed, and editing a feed to add or remove its delivery: block.
"""
from __future__ import annotations

from tests.support import config_dir, registry_on, synthetic

ARCHIVE_PAYLOAD = {
    "name": "cus_position",
    "description": "Custody positions, delivered zipped.",
    "source_system": "CUS",
    "filename_pattern": r"custodyPositions_(?P<business_date>\d{8})\.zip",
    "business_key": ["position_id"],
    "columns": ["position_id", "counterparty_id", "quantity"],
    "delivery": {"kind": "archive", "member_pattern": r"positions_.*\.csv"},
}

CONTROL_PAYLOAD = {
    "name": "trs_margin_call",
    "description": "Treasury margin calls, gated on a control file.",
    "source_system": "TRS",
    "filename_pattern": r"MarginCall_(?P<business_date>\d{8})\.csv",
    "business_key": ["margin_call_id"],
    "columns": ["margin_call_id", "amount"],
    "delivery": {"control": {"pattern": r"{stem}\.ctl",
                             "row_count": r"ROWS=(?P<rows>\d+)"}},
}


def _setup():
    d = config_dir(synthetic())
    registry = registry_on(d)
    return d, registry


# ------------------------------------------------------------ from_payload
def test_plain_kind_file_is_not_written():
    """kind: file is the implicit default -- writing it explicitly for the
    ordinary case would put `delivery: {kind: file}` in every feed the form
    creates, noise the shipped feeds have never carried."""
    _, registry = _setup()
    spec = registry.FeedSpec.from_payload({**ARCHIVE_PAYLOAD, "delivery": {"kind": "file"}})
    assert spec.delivery == {}, spec.delivery


def test_blank_control_fields_are_not_a_delivery_block():
    """A form that has the control inputs present but empty must not
    produce a spurious delivery: {control: {}}."""
    _, registry = _setup()
    spec = registry.FeedSpec.from_payload(
        {**CONTROL_PAYLOAD, "delivery": {"control": {"pattern": "", "row_count": ""}}})
    assert spec.delivery == {}, spec.delivery


def test_non_dict_delivery_is_ignored_not_an_error():
    _, registry = _setup()
    spec = registry.FeedSpec.from_payload({**ARCHIVE_PAYLOAD, "delivery": "nonsense"})
    assert spec.delivery == {}, spec.delivery


# ----------------------------------------------------------------- create
def test_archive_feed_creates_and_loads_correctly():
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(ARCHIVE_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["cus_position"]
    assert fd.delivery["kind"] == "archive", fd.delivery
    # Defaults resolve_delivery_config fills in, not written by the form.
    assert fd.delivery["business_date_from"] == "container", fd.delivery
    assert fd.delivery["parts"] == "concat", fd.delivery
    assert fd.delivery["member_pattern"] == r"positions_.*\.csv", fd.delivery


def test_control_feed_creates_and_loads_correctly():
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(CONTROL_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["trs_margin_call"]
    assert fd.delivery["control"]["pattern"] == r"{stem}\.ctl", fd.delivery
    assert fd.delivery["control"]["row_count"] == r"ROWS=(?P<rows>\d+)", fd.delivery


def test_plain_feed_writes_no_delivery_key_at_all():
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload({k: v for k, v in ARCHIVE_PAYLOAD.items()
                                           if k != "delivery"})
    registry.validate(spec, existing=set())
    registry.add(spec)
    assert "delivery:" not in (d / "feeds.yml").read_text()


# --------------------------------------------------------- validation reuse
def test_validate_rejects_control_combined_with_archive():
    """The SAME function feeds.yml load calls, not a second copy of the
    rules -- this asserts the reuse, not the rule (tests/test_control.py
    already covers the rule itself)."""
    _, registry = _setup()
    bad = {**ARCHIVE_PAYLOAD,
          "delivery": {"kind": "archive", "member_pattern": "x",
                       "control": {"pattern": r"{stem}\.ctl"}}}
    spec = registry.FeedSpec.from_payload(bad)
    try:
        registry.validate(spec, existing=set())
    except registry.FeedValidationError as exc:
        assert "delivery" in exc.errors, exc.errors
        assert "NOT BUILT" in exc.errors["delivery"], exc.errors
    else:
        raise AssertionError("expected FeedValidationError")


def test_validate_rejects_archive_with_no_member_pattern():
    _, registry = _setup()
    spec = registry.FeedSpec.from_payload({**ARCHIVE_PAYLOAD,
                                           "delivery": {"kind": "archive"}})
    try:
        registry.validate(spec, existing=set())
    except registry.FeedValidationError as exc:
        assert "member_pattern" in exc.errors["delivery"], exc.errors
    else:
        raise AssertionError("expected FeedValidationError")


# -------------------------------------------------------------------- edit
def test_editing_adds_a_delivery_block():
    d, registry = _setup()
    plain = {k: v for k, v in CONTROL_PAYLOAD.items() if k != "delivery"}
    spec = registry.FeedSpec.from_payload(plain)
    registry.validate(spec, existing=set())
    registry.add(spec)
    assert "delivery:" not in (d / "feeds.yml").read_text()

    updated = registry.FeedSpec.from_payload(CONTROL_PAYLOAD)
    registry.validate(updated, existing={"trs_margin_call"}, updating=True)
    registry.update(updated)
    text = (d / "feeds.yml").read_text()
    assert "delivery:" in text and "pattern: '{stem}\\.ctl'" in text, text


def test_editing_removes_an_existing_delivery_block():
    """The reverse direction, and the one that silently leaves stale config
    behind if get wrong: clearing the control fields on the form must
    actually delete the key, not leave the old one in place."""
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(CONTROL_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)
    assert "delivery:" in (d / "feeds.yml").read_text()

    plain = {k: v for k, v in CONTROL_PAYLOAD.items() if k != "delivery"}
    cleared = registry.FeedSpec.from_payload(plain)
    registry.validate(cleared, existing={"trs_margin_call"}, updating=True)
    registry.update(cleared)
    assert "delivery:" not in (d / "feeds.yml").read_text()


def test_editing_something_else_preserves_an_existing_delivery_block():
    """spec_from_feed round-trips delivery, so editing e.g. the description
    of an archive feed through the console must not silently drop its
    delivery: block -- the bug this guards is real: without delivery in
    spec_from_feed, ANY edit through the console would have deleted it."""
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(ARCHIVE_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["cus_position"]
    edited = registry.spec_from_feed(fd)
    edited.description = "Custody positions, zipped. Renamed via the console."
    registry.validate(edited, existing={"cus_position"}, updating=True)
    registry.update(edited)

    fd2 = feeds()["cus_position"]
    assert fd2.delivery["kind"] == "archive", fd2.delivery
    assert fd2.description == "Custody positions, zipped. Renamed via the console."
