"""The inbox as the conformance gate.

`landing/` has a contract: every object in it is correctly named and
classified, so `parse_filename` answers for all of them and landing retention
can date all of them. A legacy upstream sending `positions.csv` with the date
inside `positions.ctl` does not satisfy it, and the answer is to make the
delivery conformant AT THE DOOR rather than teach the whole platform a second
shape.

What that means concretely, and what these tests hold to:

  * two names per delivery, and they are different strings -- `positions.csv`
    in the inbox, `trs_position_20260801.csv` in landing;
  * the rename is built FROM `filename_pattern` and fed back through
    `parse_filename`, so a name landing would not accept cannot be produced;
  * verification happens BEFORE anything lands, so a failure has nothing to
    roll back and the remedy is to reject rather than to abandon a branch;
  * the metadata sibling is the only record of what actually arrived, because
    the landed object carries the platform's name, not the upstream's.

No stack: these run against real files in a temp directory and pure functions.
Whether a real Airflow run behaves the same is verified by running it.
"""
from __future__ import annotations

import dataclasses
import json
from datetime import date, datetime, timezone
from pathlib import Path

from tests.support import config_dir

FEED = """
defaults:
  landing_prefix: landing
  ready_prefix: ready
  delimiter: ","

feeds:
  - name: trs_position
    description: Positions from a legacy sender that names nothing usefully.
    source_system: TRS
    filename_pattern: 'trs_position_(?P<business_date>\\d{8})(?:_v(?P<version>\\d+))?\\.csv'
    business_key: [position_id]
    expected_min_rows: 1
    arrival:
      source_pattern: 'positions\\.csv'
      control:
        pattern: '{stem}\\.ctl'
        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'
    delivery:
      kind: file
      control:
        pattern: '{stem}\\.ctl'
        row_count: 'ROWS=(?P<rows>\\d+)'
        md5: 'MD5=(?P<md5>[0-9a-fA-F]{32})'
    columns: [position_id, quantity]
"""

DATA = b"position_id,quantity\nP1,10\nP2,20\n"


def _feed(feeds_yml=FEED, name="trs_position"):
    config_dir(feeds_yml)
    from reporting_platform.common.context import feeds
    return feeds()[name]


def _conform():
    from reporting_platform.ingest import conform
    return conform


def _bad(feeds_yml):
    try:
        _feed(feeds_yml)
    except ValueError as exc:
        return str(exc)
    raise AssertionError("expected a ValueError")


def _ctl(bd="20260801", rows=2, md5=None):
    import hashlib
    md5 = md5 or hashlib.md5(DATA).hexdigest()
    return f"ReportingDate|{bd}\nROWS={rows}\nMD5={md5}\n"


# ---------------------------------------------------------------- the config
def test_arrival_block_resolves():
    fd = _feed()
    assert fd.needs_conforming is True
    assert fd.arrival["source_pattern"] == "positions\\.csv"
    # IDENTITY ONLY. row_count and md5 belong to delivery.control, so they are
    # checked once for every delivery rather than twice for one of the two
    # arrival paths.
    assert set(fd.arrival["control"]) == {"pattern", "business_date"}
    assert set(fd.delivery["control"]) == {"pattern", "row_count", "md5"}


def test_the_shipped_feeds_need_no_conforming():
    """Every feed that exists today, against the REAL feeds.yml. The gate has
    to be opt-in, or onboarding a conformant upstream would suddenly require
    a control file it has no reason to send."""
    config_dir()                      # the shipped config, not the fixture
    from reporting_platform.common.context import feeds

    registry = feeds()
    assert registry, "the shipped feeds.yml resolved to nothing"
    for name, feed in registry.items():
        assert feed.needs_conforming is False, name
        assert feed.claims_source("anything.csv") is False, name


def test_source_pattern_is_required():
    msg = _bad(FEED.replace("      source_pattern: 'positions\\.csv'\n", ""))
    assert "no `source_pattern`" in msg, msg


def test_the_date_must_come_from_exactly_one_place():
    both = FEED.replace("'positions\\.csv'",
                        "'positions_(?P<business_date>\\d{8})\\.csv'")
    assert "One fact, one source" in _bad(both)

    neither = FEED.replace(
        "        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'\n", "")
    assert "no way to find the business date" in _bad(neither)


def test_a_dated_source_name_needs_no_control_file():
    """Some legacy names are wrong without being dateless -- POS_20260801.TXT
    for a feed whose landing convention is trs_position_20260801.csv."""
    yml = FEED.replace("'positions\\.csv'", "'POS_(?P<business_date>\\d{8})\\.TXT'")
    for line in ("      control:\n",
                 "        pattern: '{stem}\\.ctl'\n",
                 "        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'\n",
                 "        row_count: 'ROWS=(?P<rows>\\d+)'\n",
                 "        md5: 'MD5=(?P<md5>[0-9a-fA-F]{32})'\n"):
        yml = yml.replace(line, "")
    fd = _feed(yml)
    assert fd.source_business_date("POS_20260801.TXT") == date(2026, 8, 1)

    plan = _conform().conform(fd, "POS_20260801.TXT", DATA)
    assert plan["landing_filename"] == "trs_position_20260801.csv", plan


def test_control_pattern_must_reference_stem():
    msg = _bad(FEED.replace("'{stem}\\.ctl'", "'fixed.ctl'"))
    assert "{stem}" in msg, msg


def test_each_control_regex_needs_its_named_group():
    assert "(?P<business_date>...)" in _bad(
        FEED.replace("business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'",
                     "business_date: 'ReportingDate\\|\\d{8}'"))
    assert "(?P<rows>...)" in _bad(FEED.replace("(?P<rows>\\d+)", "\\d+"))
    assert "(?P<md5>...)" in _bad(FEED.replace("(?P<md5>[0-9a-fA-F]{32})",
                                               "[0-9a-fA-F]{32}"))


def test_verification_keys_are_rejected_on_the_arrival_block():
    """They would be a second implementation of the same check on one of the
    two arrival paths, and would leave an approved sender writing straight to
    landing with WEAKER checking than a legacy feed -- the trusted path being
    the less verified one, which is backwards."""
    msg = _bad(FEED.replace(
        "        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'",
        "        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'\n"
        "        row_count: 'ROWS=(?P<rows>\\d+)'"))
    assert "unknown key" in msg and "row_count" in msg, msg


def test_unknown_arrival_key_is_rejected():
    msg = _bad(FEED.replace("      source_pattern:",
                            "      surce_pattern: 'x'\n      source_pattern:"))
    assert "unknown key" in msg and "surce_pattern" in msg, msg


def test_a_landing_pattern_with_no_date_is_rejected_naming_the_gate():
    """Already an error for any feed; this says which of the two patterns is
    the problem, which is the whole reason the check is repeated here."""
    msg = _bad(FEED.replace(
        "'trs_position_(?P<business_date>\\d{8})(?:_v(?P<version>\\d+))?\\.csv'",
        "'trs_position\\.csv'"))
    assert "nowhere to write the date" in msg, msg


# ------------------------------------------------------------- the two names
def test_the_two_names_are_different_questions():
    fd = _feed()
    assert fd.claims_source("positions.csv") is True
    assert fd.claims_source("trs_position_20260801.csv") is False
    assert fd.parse_filename("positions.csv") is None
    assert fd.parse_filename("trs_position_20260801.csv")[0] == date(2026, 8, 1)


def test_the_rename_round_trips_through_parse_filename():
    """The property that makes a silent failure structurally impossible."""
    fd = _feed()
    plan = _conform().conform(fd, "positions.csv", DATA,
                              control_filename="positions.ctl",
                              control_text=_ctl())
    landed = plan["landing_filename"]
    assert landed == "trs_position_20260801.csv", landed
    assert fd.parse_filename(landed)[0] == date(2026, 8, 1)


def test_routing_finds_the_control_file_by_stem():
    fd = _feed()
    c = _conform()
    assert c.is_source_control_file(fd, "positions.ctl") is True
    assert c.is_source_control_file(fd, "positions.csv") is False
    found = c.find_control(fd, "positions.csv",
                           ["positions.csv", "positions.ctl", "other.ctl"])
    assert found == "positions.ctl", found
    assert c.find_control(fd, "positions.csv", ["positions.csv"]) is None


# ------------------------------------------------------------ the gate
def test_a_missing_control_file_is_a_wait_not_a_failure():
    fd = _feed()
    c = _conform()
    try:
        c.conform(fd, "positions.csv", DATA)
    except c.NotReady as exc:
        assert "not a failed one" in str(exc), exc
    else:
        raise AssertionError("expected NotReady")


def test_the_gate_does_not_check_content_at_all():
    """Integrity is not the inbox's job. A delivery whose control file declares
    the wrong row count and a bogus checksum still conforms and still lands --
    landing/ is the evidence copy, and "the upstream sent us this on the 3rd"
    is exactly what it exists to prove. The ingest is what refuses."""
    fd = _feed()
    c = _conform()
    plan = c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                     control_text=_ctl(rows=99, md5="0" * 32))
    assert plan["landing_filename"] == "trs_position_20260801.csv", plan
    # Measured and recorded, never compared.
    assert plan["metadata"]["row_count"] == 2
    assert "checks" not in plan["metadata"]


def test_an_identity_failure_is_still_refused():
    """The one thing the gate must refuse: a delivery it cannot NAME. There is
    no landing key to write it to, so it cannot land at all."""
    fd = _feed()
    c = _conform()
    try:
        c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                  control_text="ROWS=2\n")            # no ReportingDate line
    except c.ConformanceError as exc:
        assert "not a timing problem" in str(exc), exc
    else:
        raise AssertionError("expected ConformanceError")


def test_a_control_file_that_does_not_parse_is_a_failure_not_a_wait():
    """It arrived and does not say what it was configured to say. That will
    not clear on its own, so it must not be reported as an ordinary wait."""
    fd = _feed()
    c = _conform()
    try:
        c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                  control_text="nothing useful here\n")
    except c.NotReady:
        raise AssertionError("a malformed control file is not a wait")
    except c.ConformanceError as exc:
        assert "not a timing problem" in str(exc), exc


def test_rows_are_counted_with_the_feeds_dialect_not_by_newlines():
    """A quoted field containing a newline is one row and two lines. Counting
    lines would reject a correct delivery, which is worse than the truncation
    the check exists to catch."""
    fd = _feed()
    c = _conform()
    tricky = b'position_id,quantity\n"P1\nstill P1",10\nP2,20\n'
    assert c.count_rows(fd, tricky) == 2, c.count_rows(fd, tricky)


def test_a_header_less_feed_counts_every_line():
    yml = FEED.replace('  delimiter: ","', '  delimiter: ","\n  header: false')
    fd = _feed(yml)
    c = _conform()
    assert c.count_rows(fd, b"P1,10\nP2,20\n") == 2


# --------------------------------------------------------------- metadata
def test_metadata_records_what_actually_arrived():
    """The landed object carries the PLATFORM's name. The upstream's name, and
    everything else about the delivery, survives only here."""
    fd = _feed()
    c = _conform()
    received = datetime(2026, 8, 1, 6, 31, 12, tzinfo=timezone.utc)
    plan = c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                     control_text=_ctl(), received_at=received)
    m = plan["metadata"]

    assert m["source_filename"] == "positions.csv"
    assert m["source_control_filename"] == "positions.ctl"
    assert m["landing_filename"] == "trs_position_20260801.csv"
    assert m["business_date"] == "2026-08-01"
    assert m["received_at"].startswith("2026-08-01T06:31:12")
    assert m["row_count"] == 2 and m["bytes"] == len(DATA)
    assert m["source_system"] == "TRS"
    assert m["landing_control_filename"] == "trs_position_20260801.ctl"
    # Normalised to ISO, not kept as the sender wrote it -- nothing is lost,
    # because `control_file_contents` below holds the raw line verbatim.
    assert m["declared"]["business_date"] == "2026-08-01"
    # NO `checks` key: the gate measures, it does not compare. Integrity is
    # delivery.control's job and it runs at ingest, once, for every delivery.
    assert "checks" not in m, m
    # NO verbatim copy of the control file. It used to be embedded here,
    # because the gate consumed it and it reached landing no other way. It is
    # promoted now, byte-identical, so a copy in the metadata would be a
    # second version of the same bytes to keep in step.
    assert "control_file_contents" not in m, m
    # The metadata must survive a JSON round-trip, which is how it is stored.
    assert json.loads(c.metadata_bytes(m).decode())["feed"] == "trs_position"


def test_the_control_file_is_promoted_not_consumed():
    """The delivery is the data file and its control file together, so landing
    holds the pair -- which is what lets delivery.control verify it there
    exactly as it verifies one an approved sender wrote straight in."""
    fd = _feed()
    c = _conform()
    plan = c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                     control_text=_ctl())
    assert plan["control_landing_filename"] == "trs_position_20260801.ctl", plan
    # The promoted name must satisfy delivery.control's OWN pattern, or it
    # lands beside the delivery and is never found.
    from reporting_platform.ingest import normalize as norm
    assert norm.is_control_file(fd, plan["control_landing_filename"])


def test_the_metadata_name_is_the_delivery_plus_a_suffix():
    fd = _feed()
    c = _conform()
    plan = c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                     control_text=_ctl())
    assert plan["metadata_filename"] == "trs_position_20260801.csv.meta.json"
    assert c.is_metadata_key(plan["metadata_filename"])
    assert c.delivery_of_metadata(plan["metadata_filename"]) \
        == plan["landing_filename"]


def test_landing_retention_dates_a_metadata_file_from_its_own_name():
    """No pairing lookup: the delivery's name is inside the metadata's, so an
    orphaned metadata object still expires instead of accumulating forever."""
    fd = _feed()
    from reporting_platform.retention import landing as land_ret
    assert land_ret._business_date(fd, "trs_position_20260801.csv") \
        == date(2026, 8, 1)
    assert land_ret._business_date(
        fd, "trs_position_20260801.csv.meta.json") == date(2026, 8, 1)
    assert land_ret._business_date(fd, "positions.csv") is None


# ------------------------------------------------------------- inbox routing
def test_route_prefers_an_already_conformant_name():
    """A feed can have both patterns. A file that already satisfies
    filename_pattern needs no rename, no control file and no verification --
    sending it down the legacy path would demand a control file a conformant
    sender has no reason to include."""
    _feed()
    from reporting_platform.ingest import inbox

    feed, reason, is_control = inbox.route("trs_position_20260801.csv")
    assert feed is not None and not is_control, (feed, reason)
    assert feed.parse_filename("trs_position_20260801.csv") is not None


def test_route_claims_the_legacy_name_and_its_control_file():
    _feed()
    from reporting_platform.ingest import inbox

    feed, reason, is_control = inbox.route("positions.csv")
    assert feed is not None and not is_control, (feed, reason)
    feed, reason, is_control = inbox.route("positions.ctl")
    assert feed is not None and is_control, (feed, reason)
    feed, reason, is_control = inbox.route("nobodys_file.csv")
    assert feed is None and "arrival.source_pattern" in reason, reason


# ------------------------------------------------- the console round-trip
def test_the_api_returns_every_block_the_form_can_edit():
    """A block the edit form can set must come back out of the feed endpoint.

    Not a cosmetic omission: the form populates itself from this response and
    posts back what it read, so a missing block means `readArrival()` sees an
    unchecked box, sends {}, and the next save DELETES the feed's arrival
    block -- in a diff that looks deliberate. `column_types` had exactly this
    bug once (see ui/app.py), which is why this asserts on the whole set
    rather than on `arrival` alone.

    Read out of `ui/app.py`'s SOURCE rather than by calling `_summary`,
    because these tests run on the host with nothing but pyyaml and ruamel
    installed (tests/README.md) and importing `ui.app` needs fastapi. Parsing
    the literal keys of the dict `_summary` returns is deterministic and
    catches the one thing worth catching: a FeedSpec field with no
    corresponding key in the response.
    """
    import ast
    import pathlib

    from reporting_platform.ui import registry

    source = pathlib.Path(registry.__file__).with_name("app.py").read_text()
    summary = next(n for n in ast.walk(ast.parse(source))
                   if isinstance(n, ast.FunctionDef) and n.name == "_summary")
    returned = next(n for n in ast.walk(summary)
                    if isinstance(n, ast.Return) and isinstance(n.value, ast.Dict))
    keys = {k.value for k in returned.value.keys
            if isinstance(k, ast.Constant)}

    editable = {f.name for f in dataclasses.fields(registry.FeedSpec)}
    missing = sorted(editable - keys)
    assert not missing, (
        f"ui/app.py:_summary omits {missing}, which the form can edit -- "
        f"editing such a feed through the console would silently drop them")


def test_a_console_save_round_trips_the_arrival_block():
    """`_arrival_block` writes the nested YAML by hand, so a key it forgets is
    dropped on the first console edit of the feed."""
    d = config_dir(FEED)
    from tests.support import registry_on
    registry = registry_on(d)
    from reporting_platform.common.context import feeds

    before = dict(feeds()["trs_position"].arrival)
    registry.update(registry.spec_from_feed(feeds()["trs_position"]))
    after = dict(feeds()["trs_position"].arrival)
    assert before == after, (before, after)

    text = (d / "feeds.yml").read_text()
    assert "source_pattern: 'positions\\.csv'" in text, text
    assert "business_date: 'ReportingDate" in text, text


def test_turning_arrival_off_removes_the_block():
    """The other direction: an unchecked box must genuinely remove it, not
    leave a half-empty block that fails validation at the next load."""
    d = config_dir(FEED)
    from tests.support import registry_on
    registry = registry_on(d)
    from reporting_platform.common.context import feeds

    spec = dataclasses.replace(
        registry.spec_from_feed(feeds()["trs_position"]),
        arrival=registry._arrival_from_payload({"control": {"pattern": "x"}}),
        filename_pattern="trs_position_(?P<business_date>\\d{8})\\.csv")
    registry.update(spec)
    assert feeds()["trs_position"].needs_conforming is False
    assert "arrival:" not in (d / "feeds.yml").read_text()


def test_arrival_control_without_delivery_control_is_rejected():
    """They are complementary, not alternatives. An earlier draft had this
    backwards and REJECTED the combination, because the inbox then consumed the
    control file. It promotes it now, so the real error is the other way round:
    a control file lands and nothing reads it, silently losing the row-count
    and checksum checks for a legacy feed."""
    yml = FEED
    for line in ("    delivery:\n", "      kind: file\n",
                 "      control:\n", "        pattern: '{stem}\\.ctl'\n",
                 "        row_count: 'ROWS=(?P<rows>\\d+)'\n",
                 "        md5: 'MD5=(?P<md5>[0-9a-fA-F]{32})'\n"):
        yml = yml.replace(line, "", 1) if line not in (
            "      control:\n", "        pattern: '{stem}\\.ctl'\n") else yml
    # Drop only the SECOND control block (delivery's); arrival keeps its own.
    head, sep, tail = yml.rpartition("      control:\n        pattern: '{stem}\\.ctl'\n")
    msg = _bad(head + tail if sep else yml)
    assert "no `delivery.control`" in msg, msg


def test_the_console_rejects_the_same_incoherence():
    d = config_dir(FEED)
    from tests.support import registry_on
    registry = registry_on(d)
    from reporting_platform.common.context import feeds

    spec = dataclasses.replace(
        registry.spec_from_feed(feeds()["trs_position"]), delivery={})
    try:
        registry.validate(spec, existing={"trs_position"}, updating=True)
    except registry.FeedValidationError as exc:
        assert "delivery" in exc.errors, exc.errors
        assert "no `delivery.control`" in exc.errors["delivery"], exc.errors
    else:
        raise AssertionError("the form must reject what the loader rejects")




# ------------------------------------------------------------------ archives
ARCHIVE_FEED = """
defaults:
  landing_prefix: landing
  ready_prefix: ready
  delimiter: ","

feeds:
  - name: cust_position
    description: A weekly zip holding one complete file per business date.
    source_system: CUST
    filename_pattern: 'cust_position_(?P<business_date>\\d{8})(?:_v(?P<version>\\d+))?\\.csv'
    business_key: [position_id]
    expected_min_rows: 1
    arrival:
      source_pattern: 'weekly_\\d{8}\\.zip'
      archive:
        member_pattern: 'POS_(?P<business_date>\\d{8})\\.csv'
    columns: [position_id, quantity]
"""


def _zip(members: dict) -> bytes:
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


ZIP_MEMBERS = {
    "POS_20260801.csv": "position_id,quantity\nP1,10\n",
    "POS_20260802.csv": "position_id,quantity\nP2,20\nP3,30\n",
    "checksums.txt": "ignore me",              # a zip routinely carries these
}


def test_archive_block_resolves():
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    from reporting_platform.ingest import conform as c
    assert c.is_archive(fd) is True
    assert fd.arrival["archive"]["member_pattern"] == "POS_(?P<business_date>\\d{8})\\.csv"


def test_member_pattern_must_capture_a_business_date():
    """Each member is landed as its OWN delivery, so each must say which day
    it is for. A date on the container instead means the members are parts of
    one delivery -- a different shape, and not built."""
    msg = _bad(ARCHIVE_FEED.replace("POS_(?P<business_date>\\d{8})\\.csv",
                                    "POS_\\d{8}\\.csv"))
    assert "captures no (?P<business_date>...)" in msg, msg
    assert "not built" in msg, msg


def test_member_pattern_is_required():
    yml = ARCHIVE_FEED.replace(
        "        member_pattern: 'POS_(?P<business_date>\\d{8})\\.csv'\n", "")
    assert "empty `arrival.archive:` block" in _bad(yml)


def test_an_archive_block_with_an_unknown_key_is_rejected():
    msg = _bad(ARCHIVE_FEED.replace("        member_pattern:",
                                    "        members: 'x'\n        member_pattern:"))
    assert "unknown key" in msg and "members" in msg, msg


def test_the_container_needs_no_date_of_its_own():
    """It is a transport wrapper, not a delivery. `source_pattern` here has no
    business_date group and that must be fine -- the rule requiring one applies
    to a plain file, whose name IS the delivery."""
    fd = _feed(ARCHIVE_FEED, name="cust_position")     # no raise
    assert fd.claims_source("weekly_20260803.zip") is True


def test_unpack_takes_claimed_members_and_skips_the_rest():
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    c = _conform()
    got = c.unpack(fd, "weekly_20260803.zip", _zip(ZIP_MEMBERS))
    assert [n for n, _ in got] == ["POS_20260801.csv", "POS_20260802.csv"], got


def test_an_archive_matching_nothing_is_rejected():
    """An archive that unpacks to nothing is a delivery problem, not an empty
    day -- landing zero rows would pass expected_min_rows by accident."""
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    c = _conform()
    try:
        c.unpack(fd, "weekly_20260803.zip", _zip({"nothing.txt": "x"}))
    except c.ConformanceError as exc:
        assert "unpacks to nothing" in str(exc), exc
    else:
        raise AssertionError("expected ConformanceError")


def test_a_member_naming_a_path_is_refused():
    """The standard archive traversal bug: a member called ../../x would be
    written outside this feed's landing prefix, into another feed's evidence."""
    # A STRICT member_pattern filters a path out before the guard is reached;
    # the guard exists for a permissive one, which is when it matters.
    fd = _feed(ARCHIVE_FEED.replace("'POS_(?P<business_date>\\d{8})\\.csv'",
                                    "'.*POS_(?P<business_date>\\d{8})\\.csv'"),
               name="cust_position")
    c = _conform()
    try:
        c.unpack(fd, "w.zip", _zip({"../POS_20260801.csv": "a,b\n1,2\n"}))
    except c.ConformanceError as exc:
        assert "contains a path" in str(exc), exc
    else:
        raise AssertionError("a path-bearing member must be refused")


def test_a_corrupt_container_is_rejected_cleanly():
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    c = _conform()
    try:
        c.unpack(fd, "weekly.zip", b"this is not a zip")
    except c.ConformanceError as exc:
        assert "not a readable zip" in str(exc), exc
    else:
        raise AssertionError("expected ConformanceError")


def test_each_member_lands_as_its_own_dated_delivery():
    """The whole point: one container in, N ordinary deliveries out, each
    named so parse_filename answers for it."""
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    c = _conform()
    container = _zip(ZIP_MEMBERS)
    taken = set()
    landed = []
    for name, body in c.unpack(fd, "weekly_20260803.zip", container):
        plan = c.conform_member(fd, "weekly_20260803.zip", container,
                                name, body, taken=taken)
        taken.add(plan["landing_filename"])
        landed.append(plan["landing_filename"])
        assert fd.parse_filename(plan["landing_filename"]) is not None

    assert landed == ["cust_position_20260801.csv",
                      "cust_position_20260802.csv"], landed


def test_the_container_is_recorded_but_never_landed():
    """The members are the evidence -- byte for byte what the upstream sent.
    The container's name and checksum are kept so what arrived is still
    provable without keeping an object nothing reads."""
    import hashlib
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    c = _conform()
    container = _zip(ZIP_MEMBERS)
    name, body = c.unpack(fd, "weekly_20260803.zip", container)[0]
    m = c.conform_member(fd, "weekly_20260803.zip", container, name, body)["metadata"]

    assert m["source_container"] == "weekly_20260803.zip"
    assert m["source_container_md5"] == hashlib.md5(container).hexdigest()
    assert m["source_filename"] == "POS_20260801.csv"
    assert m["business_date"] == "2026-08-01"
    assert m["row_count"] == 1


def test_two_members_for_one_date_do_not_overwrite_each_other():
    """`taken` has to advance as members land. Read once, both would render
    the unversioned name and the second would silently replace the first."""
    fd = _feed(ARCHIVE_FEED.replace("'POS_(?P<business_date>\\d{8})\\.csv'",
                                    "'POS_(?P<business_date>\\d{8})(?:_\\d+)?\\.csv'"),
               name="cust_position")
    c = _conform()
    container = _zip({"POS_20260801.csv": "position_id,quantity\nP1,10\n",
                      "POS_20260801_2.csv": "position_id,quantity\nP9,90\n"})
    taken, landed = set(), []
    for name, body in c.unpack(fd, "w.zip", container):
        plan = c.conform_member(fd, "w.zip", container, name, body, taken=taken)
        taken.add(plan["landing_filename"])
        landed.append(plan["landing_filename"])
    assert landed == ["cust_position_20260801.csv",
                      "cust_position_20260801_v2.csv"], landed


# --------------------------------------------------- the unchanged resend
# `taken` says a name is used. It does not say by WHAT, and versioning on that
# alone turned a retried transfer into a restatement: `_v2` is a `_source_file`
# the raw table has never seen, so `already_ingested` misses it,
# `next_file_version` gives it MAX+1, and `dedupe_rank` -- ranking
# `_file_version DESC` -- lets the copy supersede the delivery it copies.
# Identical rows, so nothing looks wrong; a restatement nobody made.
def _landed(**by_name):
    """A `landed_md5` for a fixed name -> md5 map. Anything else is unknown."""
    return lambda name: by_name.get(name)


def _md5(body: bytes) -> str:
    import hashlib
    return hashlib.md5(body).hexdigest()


def test_a_byte_identical_resend_is_not_a_new_version():
    fd, c = _feed(), _conform()
    first = c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                      control_text=_ctl(), taken=set())
    assert first["landing_filename"] == "trs_position_20260801.csv"

    try:
        c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                  control_text=_ctl(), taken={first["landing_filename"]},
                  landed_md5=_landed(**{"trs_position_20260801.csv": _md5(DATA)}))
    except c.DuplicateDelivery as exc:
        assert exc.landing_filename == "trs_position_20260801.csv"
    else:
        raise AssertionError("an unchanged resend was versioned instead of refused")


def test_a_duplicate_is_not_a_rejection():
    """It must not subclass ConformanceError: `inbox._promote` routes one of
    those to `.rejected/`, and nothing is wrong with an unchanged resend."""
    c = _conform()
    assert not issubclass(c.DuplicateDelivery, c.ConformanceError)
    assert not issubclass(c.DuplicateDelivery, c.NotReady)


def test_a_corrected_resend_still_versions():
    """The behaviour the versioning exists for, and the one the md5 check must
    not break: different bytes for a landed date are a real restatement."""
    fd, c = _feed(), _conform()
    corrected = b"position_id,quantity\nP1,11\nP2,20\n"
    plan = c.conform(fd, "positions.csv", corrected,
                     control_filename="positions.ctl", control_text=_ctl(),
                     taken={"trs_position_20260801.csv"},
                     landed_md5=_landed(**{"trs_position_20260801.csv": _md5(DATA)}))
    assert plan["landing_filename"] == "trs_position_20260801_v2.csv"


def test_an_unknown_md5_versions_rather_than_suppresses():
    """None means UNKNOWN -- no metadata sibling, an object landed before this
    existed -- and unknown must not read as "same". Landing is the evidence
    copy: a needless `_v2` costs an object, a suppressed restatement costs the
    evidence."""
    fd, c = _feed(), _conform()
    plan = c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                     control_text=_ctl(), taken={"trs_position_20260801.csv"},
                     landed_md5=_landed())
    assert plan["landing_filename"] == "trs_position_20260801_v2.csv"


def test_the_resend_of_a_later_version_is_caught_too():
    """v1 and v2 both landed, and v2 comes again. The walk has to compare
    every taken candidate, not only the unversioned one."""
    fd, c = _feed(), _conform()
    v2 = b"position_id,quantity\nP1,11\nP2,20\n"
    try:
        c.conform(fd, "positions.csv", v2, control_filename="positions.ctl",
                  control_text=_ctl(),
                  taken={"trs_position_20260801.csv",
                         "trs_position_20260801_v2.csv"},
                  landed_md5=_landed(**{"trs_position_20260801.csv": _md5(DATA),
                                        "trs_position_20260801_v2.csv": _md5(v2)}))
    except c.DuplicateDelivery as exc:
        assert exc.landing_filename == "trs_position_20260801_v2.csv"
    else:
        raise AssertionError("a resend of _v2 landed as _v3")


def test_a_control_declared_version_is_checked_too():
    """A declared version wins outright over the walk, so it needs its own
    comparison or the one path a sender controls stays unprotected."""
    fd = _feed(FEED.replace(
        "        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'",
        "        business_date: 'ReportingDate\\|(?P<business_date>\\d{8})'\n"
        "        version: 'VERSION\\|(?P<version>\\d+)'"))
    c = _conform()
    ctl = _ctl() + "\nVERSION|3"
    assert c.read_control(fd, ctl, "positions.ctl")["version"] == 3
    try:
        c.conform(fd, "positions.csv", DATA, control_filename="positions.ctl",
                  control_text=ctl,
                  taken={"trs_position_20260801_v3.csv"},
                  landed_md5=_landed(**{"trs_position_20260801_v3.csv": _md5(DATA)}))
    except c.DuplicateDelivery as exc:
        assert exc.landing_filename == "trs_position_20260801_v3.csv"
    else:
        raise AssertionError("a declared version skipped the md5 check")


def test_a_container_resent_whole_restates_nothing():
    """The expensive duplicate: every member of a re-sent zip is identical, so
    without the check one resend restates every business date it covers."""
    fd = _feed(ARCHIVE_FEED, name="cust_position")
    c = _conform()
    container = _zip(ZIP_MEMBERS)
    members = c.unpack(fd, "weekly_20260803.zip", container)

    taken, by_name = set(), {}
    for name, body in members:
        plan = c.conform_member(fd, "weekly_20260803.zip", container, name, body,
                                taken=taken)
        taken.add(plan["landing_filename"])
        by_name[plan["landing_filename"]] = _md5(body)

    duplicates = 0
    for name, body in members:
        try:
            c.conform_member(fd, "weekly_20260803.zip", container, name, body,
                             taken=taken, landed_md5=_landed(**by_name))
        except c.DuplicateDelivery:
            duplicates += 1
    assert duplicates == len(members), f"only {duplicates} of {len(members)} caught"
