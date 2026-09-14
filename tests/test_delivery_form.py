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

from tests.support import config_dir, feed_text, registry_on, synthetic

ARCHIVE_PAYLOAD = {
    "name": "cus_position",
    "description": "Custody positions, delivered zipped.",
    "source_system": "CUS",
    "filename_pattern": r"custodyPositions_(?P<cob_date>\d{8})\.zip",
    "business_key": ["position_id"],
    "columns": ["position_id", "counterparty_id", "quantity"],
    "delivery": {"kind": "archive", "member_pattern": r"positions_.*\.csv"},
}

CONTROL_PAYLOAD = {
    "name": "trs_margin_call",
    "description": "Treasury margin calls, gated on a control file.",
    "source_system": "TRS",
    "filename_pattern": r"MarginCall_(?P<cob_date>\d{8})\.csv",
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
    assert fd.delivery["cob_date_from"] == "container", fd.delivery
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
    assert "delivery:" not in feed_text(d, "cus_position")


# --------------------------------------------------------- validation reuse
def test_validate_accepts_control_combined_with_archive():
    """The SAME function feeds.yml load calls, not a second copy of the rules
    -- this asserts the reuse, not the rule (tests/test_control.py covers the
    rule itself). A container gated on a control file is built now, so the
    form has to accept what the loader accepts."""
    _, registry = _setup()
    payload = {**ARCHIVE_PAYLOAD,
               "delivery": {"kind": "archive", "member_pattern": r".*\.csv",
                            "control": {"pattern": r"{stem}\.ctl",
                                        "row_count": r"ROWS=(?P<rows>\d+)"}}}
    spec = registry.FeedSpec.from_payload(payload)
    registry.validate(spec, existing=set())          # no raise


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
    assert "delivery:" not in feed_text(d, "trs_margin_call")

    updated = registry.FeedSpec.from_payload(CONTROL_PAYLOAD)
    registry.validate(updated, existing={"trs_margin_call"}, updating=True)
    registry.update(updated)
    text = feed_text(d, "trs_margin_call")
    assert "delivery:" in text and "pattern: '{stem}\\.ctl'" in text, text


def test_editing_removes_an_existing_delivery_block():
    """The reverse direction, and the one that silently leaves stale config
    behind if get wrong: clearing the control fields on the form must
    actually delete the key, not leave the old one in place."""
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(CONTROL_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)
    assert "delivery:" in feed_text(d, "trs_margin_call")

    plain = {k: v for k, v in CONTROL_PAYLOAD.items() if k != "delivery"}
    cleared = registry.FeedSpec.from_payload(plain)
    registry.validate(cleared, existing={"trs_margin_call"}, updating=True)
    registry.update(cleared)
    assert "delivery:" not in feed_text(d, "trs_margin_call")


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


# --------------------------------------------------- a custom control format
# `control.format` is HOW the control file is read, and the console rewrites
# the whole feed block from the form payload -- so a key the form does not
# carry is a key the next save DELETES. A delimited control file quietly
# reverting to the regex reading is not a validation failure anywhere: the
# fields simply stop matching, at ingest, on a feed nobody touched.
DELIMITED_PAYLOAD = {
    **CONTROL_PAYLOAD,
    "name": "trs_margin_piped",
    "arrival": {
        "source_pattern": r"marginCalls\.csv",
        "control": {"pattern": r"{stem}\.ctl",
                    "format": {"kind": "delimited", "delimiter": "|"},
                    "cob_date": "BUSINESS_DATE"},
    },
    "delivery": {"control": {"pattern": r"{stem}\.ctl",
                             "format": {"kind": "delimited", "delimiter": "|"},
                             "row_count": "RECORD_COUNT"}},
}


def test_a_delimited_control_feed_creates_and_loads_correctly():
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(DELIMITED_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["trs_margin_piped"]
    assert fd.delivery["control"]["format"]["delimiter"] == "|", fd.delivery
    assert fd.delivery["control"]["row_count"] == "RECORD_COUNT", fd.delivery
    assert fd.arrival["control"]["cob_date"] == "BUSINESS_DATE", fd.arrival
    # Written once per block and identically, or check_gates_are_coherent
    # would refuse the feed the console had just saved.
    assert fd.arrival["control"]["format"] == fd.delivery["control"]["format"]


def test_the_written_format_carries_no_resolved_defaults():
    """`quote_char` and `header` come back filled in from the loader on every
    edit; writing them into the block would pin two values nobody chose."""
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(DELIMITED_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)
    text = feed_text(d, "trs_margin_piped")
    assert "quote_char" not in text, text
    assert "header:" not in text, text


def test_editing_something_else_preserves_the_control_format():
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(DELIMITED_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    edited = registry.spec_from_feed(feeds()["trs_margin_piped"])
    edited.description = "Treasury margin calls, piped control file."
    registry.validate(edited, existing={"trs_margin_piped"}, updating=True)
    registry.update(edited)

    fd = feeds()["trs_margin_piped"]
    assert fd.delivery["control"]["format"]["delimiter"] == "|", fd.delivery
    assert fd.arrival["control"]["format"]["delimiter"] == "|", fd.arrival


def test_a_headerless_format_round_trips_with_its_columns():
    d, registry = _setup()
    fmt = {"kind": "delimited", "delimiter": "|", "header": False,
           "columns": ["FEED", "BUSINESS_DATE", "RECORD_COUNT"]}
    payload = {
        **DELIMITED_PAYLOAD,
        "arrival": {**DELIMITED_PAYLOAD["arrival"],
                    "control": {**DELIMITED_PAYLOAD["arrival"]["control"],
                                "format": fmt}},
        "delivery": {"control": {**DELIMITED_PAYLOAD["delivery"]["control"],
                                 "format": fmt}},
    }
    spec = registry.FeedSpec.from_payload(payload)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["trs_margin_piped"]
    assert fd.delivery["control"]["format"]["columns"] == [
        "FEED", "BUSINESS_DATE", "RECORD_COUNT"], fd.delivery
    # `columns` alone is rejected at load, so the header flag has to travel
    # with it rather than being dropped as a non-default.
    assert fd.delivery["control"]["format"]["header"] is False, fd.delivery


def test_validate_reports_a_bad_format_against_the_form():
    """The SAME resolver feeds.yml load calls -- this asserts the reuse, not
    the rule (tests/test_control_format.py covers the rule)."""
    _, registry = _setup()
    bad = {**DELIMITED_PAYLOAD,
           "delivery": {"control": {"pattern": r"{stem}\.ctl",
                                    "format": {"kind": "delimited"},
                                    "row_count": "RECORD_COUNT"}}}
    spec = registry.FeedSpec.from_payload(bad)
    try:
        registry.validate(spec, existing=set())
    except registry.FeedValidationError as exc:
        assert "delimiter" in exc.errors["delivery"], exc.errors
    else:
        raise AssertionError("expected FeedValidationError")


# ------------------------------------------- a zip unpacked at the door -----
# `arrival.archive` had NO round trip at all: `_arrival_block` never wrote it,
# so editing an archive feed through the console dropped the block and turned
# a feed whose deliveries are unpacked at the door into one expecting a
# conformant CSV. The save then failed validation for a reason ("no way to
# find the COB date") that named neither the archive block nor the edit.

GATE_ARCHIVE_PAYLOAD = {
    "name": "cus_weekly",
    "description": "A weekly zip holding one complete file per COB date.",
    "source_system": "CUS",
    "filename_pattern": r"cus_weekly_(?P<cob_date>\d{8})\.csv",
    "business_key": ["position_id"],
    "columns": ["position_id", "quantity"],
    "arrival": {"source_pattern": r"weekly_\d{8}\.zip",
                "archive": {"member_pattern": r"POS_(?P<cob_date>\d{8})\.csv"}},
}


def test_an_archive_gate_feed_creates_and_loads_correctly():
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(GATE_ARCHIVE_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["cus_weekly"]
    assert fd.arrival["archive"]["member_pattern"] == r"POS_(?P<cob_date>\d{8})\.csv"
    assert "member_pattern" in feed_text(d, "cus_weekly")


def test_editing_an_archive_feed_keeps_its_archive_block():
    """THE ROUND TRIP, which is where this was lost: `spec_from_feed` hands
    back the resolved arrival block, so anything the writer does not write is
    deleted by the next save of an unrelated field."""
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(GATE_ARCHIVE_PAYLOAD)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    edited = registry.spec_from_feed(feeds()["cus_weekly"])
    edited.description = "Edited for an unrelated reason."
    registry.validate(edited, existing={"cus_weekly"}, updating=True)
    registry.update(edited)

    fd = feeds()["cus_weekly"]
    assert fd.arrival["archive"]["member_pattern"] == r"POS_(?P<cob_date>\d{8})\.csv"
    assert fd.needs_conforming is True, fd.arrival


def test_a_zip_of_control_gated_members_round_trips():
    """Both blocks and the archive block together -- the shape that used to
    load and then wait for ever on control files the gate had discarded."""
    d, registry = _setup()
    payload = {
        **GATE_ARCHIVE_PAYLOAD,
        "name": "cus_weekly_gated",
        "filename_pattern": r"cus_weekly_gated_(?P<cob_date>\d{8})\.csv",
        "arrival": {"source_pattern": r"weekly_\d{8}\.zip",
                    "control": {"pattern": r"{stem}\.ctl",
                                "cob_date": r"DATE=(?P<cob_date>\d{8})"},
                    "archive": {"member_pattern": r"POSITIONS_[A-Z]\.csv"}},
        "delivery": {"control": {"pattern": r"{stem}\.ctl",
                                 "row_count": r"ROWS=(?P<rows>\d+)"}},
    }
    spec = registry.FeedSpec.from_payload(payload)
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["cus_weekly_gated"]
    assert fd.arrival["archive"]["member_pattern"] == r"POSITIONS_[A-Z]\.csv"
    assert fd.arrival["control"]["cob_date"] == r"DATE=(?P<cob_date>\d{8})"
    assert fd.delivery["control"]["row_count"] == r"ROWS=(?P<rows>\d+)"


# --------------------------------- ...and the FORM can now create one -------
# The blocks above went in with the loader, the validator and the YAML writer
# all handling `arrival.archive` -- and the form with no input for it, so the
# CREATE direction was reachable only by hand-editing a feed file. These are
# the payload the form now sends, and the page source that sends it.


def test_the_create_direction_round_trips_from_the_form_payload():
    """Payload -> FeedSpec -> written YAML -> loaded Feed, keeping the block.

    `test_editing_an_archive_feed_keeps_its_archive_block` covers the edit
    direction; this is the one the console could not reach at all.
    """
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(GATE_ARCHIVE_PAYLOAD)
    assert spec.arrival["archive"] == {
        "member_pattern": r"POS_(?P<cob_date>\d{8})\.csv"}
    registry.validate(spec, existing=set())
    registry.add(spec)

    from reporting_platform.common.context import feeds
    fd = feeds()["cus_weekly"]
    assert fd.arrival["archive"]["member_pattern"] == r"POS_(?P<cob_date>\d{8})\.csv"
    assert fd.needs_conforming is True


def test_a_blank_member_pattern_is_no_archive_block_at_all():
    """The form sends `archive` only when the input has something in it.

    An `arrival.archive:` with no `member_pattern` is refused at load, so a
    form that sent an empty one every time would make every ordinary inbox
    feed unsaveable -- the same reason blanks are dropped everywhere else
    in `_arrival_from_payload`.
    """
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload({
        **GATE_ARCHIVE_PAYLOAD,
        "name": "cus_plain",
        "filename_pattern": r"cus_plain_(?P<cob_date>\d{8})\.csv",
        "arrival": {"source_pattern": r"weekly_(?P<cob_date>\d{8})\.csv",
                    "archive": {"member_pattern": "  "}},
    })
    assert "archive" not in spec.arrival
    registry.validate(spec, existing=set())


def test_the_form_has_a_member_pattern_input_that_it_sends():
    """The FORM must be able to create what the loader accepts.

    `ui/registry._arrival_from_payload` has read `archive.member_pattern`
    since the zip work landed and `_arrival_block` has written it back, but
    nothing ever put one in the payload. Asserted against the page source
    because that is where the gap was; there is no JS runtime in this suite.
    """
    import pathlib

    page = (pathlib.Path(__file__).resolve().parent.parent / "reporting_platform"
            / "ui" / "static" / "index.html").read_text()

    # the input exists, and is pre-filled from an existing feed's block, or
    # editing an archive feed through the form would blank it on every save
    assert "f?.arrival?.archive?.member_pattern" in page

    # ...and readArrival() -- not readDelivery() -- puts it in the payload.
    # The two zip mechanisms are different shapes and the COB date picks one.
    read = page[page.index("const readArrival"):]
    read = read[:read.index("\n  };")]
    assert "out.archive = {member_pattern:" in read, read
    assert "arrMemberPattern.value.trim()" in read, read


def test_the_form_says_the_landing_pattern_names_the_members():
    """The single most confusable thing about the shape, so the form says it.

    `filename_pattern` for an `arrival.archive` feed describes each MEMBER
    after renaming -- the container never lands. A form offering both a
    member pattern and a landing pattern with nothing distinguishing them
    invites a `\\.zip` in the second, which then matches nothing for ever
    and reports as a feed that simply never has anything pending.
    """
    import pathlib

    page = (pathlib.Path(__file__).resolve().parent.parent / "reporting_platform"
            / "ui" / "static" / "index.html").read_text()
    note = page[page.index("const membersNotTheZipNote"):]
    note = note[:note.index("});")]
    assert "MEMBER" in note, note
    # shown only while a member pattern is set, or it is a paragraph about
    # zips on the form of every feed that has nothing to do with one
    sync = page[page.index("function syncArrivalArchive"):]
    sync = sync[:sync.index("\n  }")]
    assert "membersNotTheZipNote.style.display = unpacking" in sync, sync
    assert "arrMemberPattern.value.trim()" in sync, sync


def test_the_form_does_not_manufacture_an_arrival_control_for_dated_members():
    """The form's own default path for this shape must be SAVEABLE.

    Ticking Arrival fills in `{stem}\\.ctl`, because a legacy sender that
    needs the door usually has a control file. A member pattern carrying its
    own date does not -- and `arrival.control` with no `delivery.control` is
    refused by `check_gates_are_coherent`, so the autofill would make the
    commonest way to reach this shape produce a feed the loader rejects, with
    the message landing on a section the person never touched. Pinned here
    because the payload below is what the browser was observed to POST.
    """
    import pathlib

    page = (pathlib.Path(__file__).resolve().parent.parent / "reporting_platform"
            / "ui" / "static" / "index.html").read_text()
    sync = page[page.index("function syncArrivalArchive"):]
    sync = sync[:sync.index("\n  }")]
    assert 'arrCtlPattern.value === AUTO_CTL' in sync, sync
    assert 'arrCtlPattern.value = ""' in sync, sync

    # ...and that payload loads.
    d, registry = _setup()
    spec = registry.FeedSpec.from_payload(GATE_ARCHIVE_PAYLOAD)
    registry.validate(spec, existing=set())

    # while the same payload WITH the autofill left in is exactly the refusal
    # the clearing avoids -- and it names `delivery.control`, not the archive.
    from reporting_platform.ui.registry import FeedValidationError
    with_control = registry.FeedSpec.from_payload({
        **GATE_ARCHIVE_PAYLOAD,
        "arrival": {**GATE_ARCHIVE_PAYLOAD["arrival"],
                    "control": {"pattern": r"{stem}\.ctl"}},
    })
    try:
        registry.validate(with_control, existing=set())
    except FeedValidationError as exc:
        assert "delivery.control" in str(exc.errors), exc.errors
    else:
        raise AssertionError("expected FeedValidationError")


# ------------------------- the console cannot write an unloadable registry ---
# A control-pattern collision is refused at LOAD, and load is the WHOLE
# registry -- so a feed saved with one does not break itself, it stops
# `feeds()` resolving at all and takes every other feed and every DAG with it.
# `add()` does not verify what it wrote, so the form is the only guard.

COLLIDING_PAYLOAD = {
    "name": "cus_margin_call",
    "description": "A second feed whose control files cannot be told apart.",
    "source_system": "CUS",
    # Same stem shape as CONTROL_PAYLOAD's feed, differing only by extension.
    "filename_pattern": r"MarginCall_(?P<cob_date>\d{8})\.txt",
    "business_key": ["margin_call_id"],
    "columns": ["margin_call_id", "amount"],
    "delivery": {"control": {"pattern": r"{stem}\.ctl",
                             "row_count": r"ROWS=(?P<rows>\d+)"}},
}


def _add(registry, payload):
    spec = registry.FeedSpec.from_payload(payload)
    registry.validate(spec, existing=set())
    registry.add(spec)
    return spec


def test_the_form_refuses_a_feed_whose_control_files_collide():
    d, registry = _setup()
    _add(registry, CONTROL_PAYLOAD)
    spec = registry.FeedSpec.from_payload(COLLIDING_PAYLOAD)
    try:
        registry.validate(spec, existing={"trs_margin_call"})
    except registry.FeedValidationError as exc:
        assert "delivery" in exc.errors, exc.errors
        assert "can claim the same control file" in exc.errors["delivery"], exc.errors
    else:
        raise AssertionError("expected FeedValidationError")


def test_the_registry_still_loads_after_that_refusal():
    """THE POINT OF REFUSING IN THE FORM. If the save had gone through, the
    next `feeds()` would raise and nothing would resolve -- not the other
    feed, not the DAGs, not the console that wrote it."""
    d, registry = _setup()
    _add(registry, CONTROL_PAYLOAD)
    spec = registry.FeedSpec.from_payload(COLLIDING_PAYLOAD)
    try:
        registry.validate(spec, existing={"trs_margin_call"})
    except registry.FeedValidationError:
        pass
    from reporting_platform.common.context import feeds
    assert "trs_margin_call" in feeds()


def test_a_distinguishable_second_control_feed_is_accepted():
    """The rule must not refuse the ordinary case: two source systems that
    both send `.ctl`, with names that can be told apart."""
    d, registry = _setup()
    _add(registry, CONTROL_PAYLOAD)
    fine = {**COLLIDING_PAYLOAD, "name": "cus_position",
            "filename_pattern": r"POS_(?P<cob_date>\d{8})\.csv"}
    _add(registry, fine)

    from reporting_platform.common.context import feeds
    assert {"trs_margin_call", "cus_position"} <= set(feeds())


def test_editing_a_control_feed_does_not_collide_with_itself():
    """The candidate replaces its own entry in the registry it is checked
    against -- comparing a feed with its unedited self would refuse every
    edit."""
    d, registry = _setup()
    _add(registry, CONTROL_PAYLOAD)
    from reporting_platform.common.context import feeds
    edited = registry.spec_from_feed(feeds()["trs_margin_call"])
    edited.description = "Edited."
    registry.validate(edited, existing={"trs_margin_call"}, updating=True)
    registry.update(edited)
    assert "trs_margin_call" in feeds()


def test_the_form_offers_a_control_block_for_an_archive():
    """The FORM must be able to create what the loader accepts.

    It hid the control fields whenever `kind: archive` was selected, and
    `readDelivery()` dropped the block outright, because the loader refused
    that combination. The loader accepts it now -- a container gated on a
    control file beside it in landing -- so hiding them would leave a feed
    shape creatable only by hand. Asserted against the page source because
    that is where the rule lives; there is no JS runtime in this suite.
    """
    import pathlib

    page = (pathlib.Path(__file__).resolve().parent.parent / "reporting_platform"
            / "ui" / "static" / "index.html").read_text()
    sync = page[page.index("function syncDeliveryVisibility"):]
    sync = sync[:sync.index("\n  }")]
    for field in ("controlPatternField", "ctlFormatField", "controlRowCountField",
                  "controlMd5Field"):
        line = next(l for l in sync.splitlines() if l.strip().startswith(field))
        assert "isArchive" not in line, line
    # the member pattern is still archive-only, which is the point of the shape
    assert "isArchive" in next(l for l in sync.splitlines()
                               if l.strip().startswith("memberPatternField"))

    read = page[page.index("const readDelivery"):]
    read = read[:read.index("\n  };")]
    # the archive branch must fall through to the control block, not return
    assert read.count("return") <= 2, read
    assert "out.control = control" in read, read


# ------------------ a sniffed zip of control-gated members fills the form ---
# `ingest/sniff.py` recognises members that carry their own control files and
# proposes the `arrival.archive` shape's control half. The form must put it
# where the loader reads it -- BOTH control blocks, the arrival section, and
# NOT `delivery.kind: archive`, which never looks inside a container.


def _page() -> str:
    import pathlib

    return (pathlib.Path(__file__).resolve().parent.parent / "reporting_platform"
            / "ui" / "static" / "index.html").read_text()


def test_the_form_prefills_both_control_blocks_from_a_member_control_proposal():
    page = _page()
    # the upload handler...
    handler = page[page.index("if (r.member_control) {"):]
    handler = handler[:handler.index("} else if (r.arrival_source_pattern)")]
    for line in ("hasArrival.checked = true", "arrMemberPattern.value = r.member_pattern_candidate",
                 'deliveryKind.value = "file"', "arrCtlPattern.value = mc.pattern",
                 "controlPattern.value = mc.pattern", "memberControlBits(r)"):
        assert line in handler, (line, handler)
    # ...and a draft from the unclaimed queue, which builds feedForm's shape
    draft = page[page.index("if (draft.member_control) {"):]
    draft = draft[:draft.index("} else if (draft.arrival_source_pattern)")]
    assert "draft.arrival.control = {pattern: mc.pattern, ...fmt}" in draft, draft
    assert "draft.delivery = {control: {pattern: mc.pattern, ...fmt}}" in draft, draft
    # the delivery.kind: archive pre-fill is skipped for this shape
    assert "if (r.archive_members && !r.member_control)" in page
    assert "if (draft.archive_members && !draft.member_control)" in page
    # fields are candidates in the note, never filled in -- no cob_date write
    assert "field_candidates" not in handler + draft


def test_the_payload_a_member_control_proposal_prefills_validates():
    """What the form posts from a REAL proposal, with the one field the note
    leaves to a human typed in from its candidates. Through the console's own
    validate(), which is the loader's resolvers plus check_gates_are_coherent."""
    import io
    import zipfile

    from reporting_platform.ingest import sniff

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("POS_A.dat", "position_id,quantity\nP1,10\nP2,20\n")
        zf.writestr("POS_A.ctl", "DATE=20260831\nROWS=2\n")
    p = sniff.propose_feed("weekly.zip", buf.getvalue())
    mc = p["member_control"]

    d, registry = _setup()
    payload = {
        "name": "cus_gated", "description": "d", "source_system": "CUS",
        "filename_pattern": r"cus_gated_(?P<cob_date>\d{8})\.csv",
        "business_key": ["position_id"], "columns": p["columns"],
        "arrival": {"source_pattern": p["arrival_source_pattern"],
                    "control": {"pattern": mc["pattern"]},
                    "archive": {"member_pattern": p["member_pattern_candidate"]}},
        "delivery": {"control": {"pattern": mc["pattern"]}},
    }
    # as pre-filled, before the human types the COB date: refused, and the
    # refusal is about the date source -- not about delivery.control
    try:
        registry.validate(registry.FeedSpec.from_payload(payload), existing=set())
    except registry.FeedValidationError as exc:
        assert "cob_date" in str(exc.errors), exc.errors
        assert "delivery.control" not in str(exc.errors), exc.errors
    else:
        raise AssertionError("expected FeedValidationError")

    payload["arrival"]["control"]["cob_date"] = mc["field_candidates"]["cob_date"][0]
    spec = registry.FeedSpec.from_payload(payload)
    registry.validate(spec, existing=set())
    registry.add(spec)
    from reporting_platform.common.context import feeds
    fd = feeds()["cus_gated"]
    assert fd.arrival["archive"]["member_pattern"] == r".*\.dat", fd.arrival
    assert fd.delivery["control"]["pattern"] == r"{stem}\.ctl", fd.delivery


def _member_control_handler() -> str:
    page = _page()
    handler = page[page.index("if (r.member_control) {"):]
    return handler[:handler.index("} else if (r.arrival_source_pattern)")]


def test_resniffing_an_existing_feed_fills_only_empty_fields():
    """Review finding: the member_control branch overwrote the member
    pattern, the delivery shape, both control patterns and the format --
    forcing text when the proposal had none -- while the row-count, md5 and
    COB-date fields kept values written under the old format. Every other
    pre-fill in the handler fills only an empty field."""
    handler = _member_control_handler()
    assert "if (!arrMemberPattern.value.trim())" in handler, handler
    assert 'if (!memberPattern.value.trim()) deliveryKind.value = "file"' in handler, handler
    assert 'memberPattern.value = ""' not in handler, handler
    assert "if (mc.pattern && ctlPatternsEmpty)" in handler, handler
    # the format moves only inside the block guarded by empty fields and an
    # unambiguous reading -- and nowhere else in the branch
    guard = handler.index("if (ctlFieldsEmpty && !mc.format_ambiguous)")
    assert handler.count("ctlFormat.value =") == 1, handler
    assert handler.index("ctlFormat.value =") > guard, handler
    for field in ("arrCtlDate", "controlRowCount", "controlMd5"):
        assert field in handler[:guard], (field, handler)
    # and what was kept is said, not silent
    assert "left as they are" in handler, handler


def test_why_the_format_must_not_flip_under_filled_fields():
    """The refusal a clobbered delimited feed would hit: its row count names
    a COLUMN, which under the text reading is a regex with no group."""
    _, registry = _setup()
    flipped = {**DELIMITED_PAYLOAD,
               "arrival": {**DELIMITED_PAYLOAD["arrival"],
                           "control": {"pattern": r"{stem}\.ctl",
                                       "cob_date": "BUSINESS_DATE"}},
               "delivery": {"control": {"pattern": r"{stem}\.ctl",
                                        "row_count": "RECORD_COUNT"}}}
    try:
        registry.validate(registry.FeedSpec.from_payload(flipped), existing=set())
    except registry.FeedValidationError as exc:
        assert "(?P<" in str(exc.errors), exc.errors
    else:
        raise AssertionError("expected FeedValidationError")


def test_the_arrival_autofill_answers_a_tick_not_a_sync():
    """Review finding: `syncArrivalVisibility()` filled `{stem}\\.ctl` into the
    ARRIVAL block whenever it ran -- on load, and after a sniff that could
    propose no control pattern -- leaving the delivery block empty. That form
    is refused by check_gates_are_coherent and nothing on it says why."""
    page = _page()
    sync = page[page.index("function syncArrivalVisibility"):]
    sync = sync[:sync.index("\n  }")]
    assert "AUTO_CTL" not in sync, sync
    listener = page[page.index('hasArrival.addEventListener("change"'):]
    listener = listener[:listener.index("});")]
    assert "arrCtlPattern.value = AUTO_CTL" in listener, listener
    # a draft with no proposed pattern writes no control block at all
    draft = page[page.index("if (draft.member_control) {"):]
    draft = draft[:draft.index("} else if (draft.arrival_source_pattern)")]
    assert draft.index("if (mc.pattern) {") < draft.index("draft.arrival.control"), draft
    # the plain undated-file path, which relied on the old autofill, fills it
    plain = page[page.index("} else if (r.arrival_source_pattern) {"):]
    plain = plain[:plain.index("\n    }")]
    assert "if (!arrCtlPattern.value.trim()) arrCtlPattern.value = AUTO_CTL" in plain, plain


def test_a_null_pattern_proposal_says_both_blocks_and_does_not_save_silently():
    """With no proposed pattern the form writes neither control block; the
    refusal is then about the COB date source, and the note says to write the
    same pattern into both blocks."""
    import io
    import zipfile

    from reporting_platform.ingest import sniff

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in (("A.csv", "position_id,quantity\nP1,1\n"),
                           ("A.ctl", "DATE=20260831\n"),
                           ("B.csv", "position_id,quantity\nP2,2\n"),
                           ("B.CTL", "DATE=20260901\n")):
            zf.writestr(name, body)
    p = sniff.propose_feed("weekly.zip", buf.getvalue())
    mc = p["member_control"]
    assert mc["pattern"] is None, mc
    assert "`arrival.control.pattern` and `delivery.control.pattern`" in mc["note"], mc

    _, registry = _setup()
    payload = {"name": "cus_nullpat", "description": "d", "source_system": "CUS",
               "filename_pattern": r"cus_nullpat_(?P<cob_date>\d{8})\.csv",
               "business_key": ["position_id"], "columns": p["columns"],
               "arrival": {"source_pattern": p["arrival_source_pattern"],
                           "archive": {"member_pattern": p["member_pattern_candidate"]}}}
    try:
        registry.validate(registry.FeedSpec.from_payload(payload), existing=set())
    except registry.FeedValidationError as exc:
        assert "cob_date" in str(exc.errors), exc.errors
        assert "delivery.control" not in str(exc.errors), exc.errors
    else:
        raise AssertionError("expected FeedValidationError")
