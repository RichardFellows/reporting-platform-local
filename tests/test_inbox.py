"""inbox.py's pure logic: routing, and the .rejected/ backlog it feeds the
console's "unclaimed deliveries" queue from.

No S3, no Airflow: `route()` only reads `feeds()`, and `list_rejected`/
`read_rejected` only touch a local directory (`INBOX`, monkeypatched here to
a temp one). The polling loop (`sweep`) and the Airflow trigger it calls
are not covered -- those need MinIO and a running scheduler, and are
verified by running the stack.
"""
from __future__ import annotations

import pathlib
import tempfile

from tests.support import config_dir, synthetic

CONTROL_FEED = synthetic(feed_extra="""
    delivery:
      control:
        pattern: '{stem}\\.ctl'
""")


def _setup(feeds_yml=None):
    # config_dir(None) loads the REAL feeds.yml -- these tests want the
    # small "t_one" fixture instead, so the default here is `synthetic()`,
    # not None.
    config_dir(feeds_yml if feeds_yml is not None else synthetic())
    import reporting_platform.ingest.inbox as inbox
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-inbox-"))
    inbox.INBOX = d
    return inbox, d


# ------------------------------------------------------------------- route
def test_data_file_routes_to_its_feed():
    inbox, d = _setup()
    feed, reason, is_control = inbox.route("A_20260901.csv")
    assert feed is not None and feed.name == "t_one", (feed, reason)
    assert reason is None and is_control is False


def test_unrelated_filename_is_rejected():
    inbox, d = _setup()
    feed, reason, is_control = inbox.route("nothing_like_it.dat")
    assert feed is None, feed
    assert "matches no feed's filename_pattern" in reason, reason
    assert is_control is False


def test_control_file_routes_as_control():
    inbox, d = _setup(CONTROL_FEED)
    feed, reason, is_control = inbox.route("A_20260901.ctl")
    assert feed is not None and feed.name == "t_one", (feed, reason)
    assert is_control is True


def test_data_pattern_wins_over_control_pattern():
    """A feed's own data file must never be mistaken for its control file."""
    inbox, d = _setup(CONTROL_FEED)
    feed, reason, is_control = inbox.route("A_20260901.csv")
    assert feed is not None and is_control is False, (feed, is_control)


# ------------------------------------------------------------ .rejected/
def test_list_rejected_is_empty_with_no_folder():
    inbox, d = _setup()
    assert inbox.list_rejected() == []


def test_list_rejected_reports_the_current_rejection_reason():
    inbox, d = _setup()
    rej = d / ".rejected"
    rej.mkdir()
    (rej / "mystery.dat").write_bytes(b"whatever")
    out = inbox.list_rejected()
    assert len(out) == 1, out
    assert out[0]["filename"] == "mystery.dat"
    assert out[0]["bytes"] == 8
    assert out[0]["now_claimed_by"] is None
    assert out[0]["now_routes_as_control"] is False
    assert "matches no feed's filename_pattern" in out[0]["reason"]


def test_list_rejected_flags_a_file_a_later_config_change_would_now_claim():
    """feeds.yml can change after a file was rejected -- list_rejected
    re-runs route() rather than trusting a stored reason, so a file that
    would now land is flagged rather than offered up to sniff."""
    inbox, d = _setup()
    rej = d / ".rejected"
    rej.mkdir()
    (rej / "A_20260901.csv").write_bytes(b"k,v\n1,2\n")
    out = inbox.list_rejected()
    assert out[0]["now_claimed_by"] == "t_one", out


def test_list_rejected_skips_dotfiles_and_directories():
    inbox, d = _setup()
    rej = d / ".rejected"
    rej.mkdir()
    (rej / ".DS_Store").write_bytes(b"x")
    (rej / "subdir").mkdir()
    assert inbox.list_rejected() == []


def test_read_rejected_returns_the_bytes():
    inbox, d = _setup()
    rej = d / ".rejected"
    rej.mkdir()
    (rej / "mystery.dat").write_bytes(b"hello")
    assert inbox.read_rejected("mystery.dat") == b"hello"


def test_read_rejected_refuses_a_path_not_a_filename():
    inbox, d = _setup()
    for bad in ("../secrets", "a/b", "a\\b", "", ".", ".."):
        try:
            inbox.read_rejected(bad)
        except ValueError as exc:
            assert "not a bare filename" in str(exc), exc
        else:
            raise AssertionError(f"expected ValueError for {bad!r}")


def test_read_rejected_missing_file_is_a_clean_error():
    inbox, d = _setup()
    try:
        inbox.read_rejected("nope.csv")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("expected FileNotFoundError")


# =========================================== the writer, for every shape ====
# `_promote` is ONE function for every arrival shape now. It was two, and they
# had drifted: one treated a missing control file as a wait and the other
# could not express the idea, and the write order was written down twice. What
# is asserted here is the part that is not `conform.py`'s -- which objects are
# written and in what order, where the inbox copy goes, and what is triggered.
#
# No S3 and no Airflow: `put_landing_bytes` and `_trigger` are recorded
# instead. Whether MinIO accepts the same keys is verified by running it.

GATE_FEED = """
defaults:
  landing_prefix: landing
  ready_prefix: ready
  delimiter: ","

feeds:
  - name: trs_position
    description: A legacy sender that names nothing usefully.
    source_system: TRS
    filename_pattern: 'trs_position_(?P<cob_date>\\d{8})(?:_v(?P<version>\\d+))?\\.csv'
    business_key: [position_id]
    columns: [position_id, quantity]
    arrival:
      source_pattern: 'positions\\.csv'
      control:
        pattern: '{stem}\\.ctl'
        cob_date: 'DATE=(?P<cob_date>\\d{8})'
    delivery:
      kind: file
      control:
        pattern: '{stem}\\.ctl'
        row_count: 'ROWS=(?P<rows>\\d+)'
"""

ZIP_FEED = GATE_FEED.replace(
    "      source_pattern: 'positions\\.csv'",
    "      source_pattern: 'weekly\\.zip'").replace(
    "        cob_date: 'DATE=(?P<cob_date>\\d{8})'",
    "        cob_date: 'DATE=(?P<cob_date>\\d{8})'\n"
    "      archive:\n"
    "        member_pattern: 'POSITIONS_[A-Z]\\.csv'")

DATA = b"position_id,quantity\nP1,10\n"


def _wired(feeds_yml):
    """An inbox with object storage, the registry and Airflow recorded, not
    called. Returns (inbox, dir, feed, puts, triggers)."""
    inbox, d = _setup(feeds_yml)
    from reporting_platform.common.context import feeds
    from reporting_platform.registry import rejections

    puts: list[str] = []
    triggers: list[str] = []
    inbox.put_landing_bytes = lambda feed, name, body, content_type="": (
        puts.append(name) or f"landing/{feed.name}/{name}")
    inbox.list_landing = lambda feed: []
    inbox.landed_md5_lookup = lambda feed, keys: (lambda name: None)
    inbox._trigger = lambda feed, key: (
        triggers.append(key) or {"triggered": True, "dag_id": f"ingest_{feed.name}"})
    rejections.quarantine_quietly = lambda *a, **k: None
    return inbox, d, feeds()["trs_position"], puts, triggers


def _sweep(inbox, d):
    """Three passes: two to observe stability, one to act."""
    seen: dict = {}
    out: list = []
    for _ in range(3):
        out = inbox.sweep(seen)
    return out


def _zipped(members: dict) -> bytes:
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


def test_a_gated_delivery_writes_control_then_data_then_metadata():
    """THE ORDER IS THE POINT. The data file is what a triggered run acts on,
    so writing it after its control file means a run can never find a delivery
    whose control file has not landed."""
    inbox, d, fd, puts, triggers = _wired(GATE_FEED)
    (d / "positions.csv").write_bytes(DATA)
    (d / "positions.ctl").write_bytes(b"DATE=20260901\nROWS=1\n")

    results = _sweep(inbox, d)
    assert puts == ["trs_position_20260901.ctl",
                    "trs_position_20260901.csv",
                    "trs_position_20260901.csv.meta.json"], puts
    assert [r["status"] for r in results] == ["conformed"], results
    # BOTH inbox files move, and only after everything is written.
    assert (d / ".processed" / "trs_position" / "positions.csv").is_file()
    assert (d / ".processed" / "trs_position" / "positions.ctl").is_file()
    assert triggers == ["landing/trs_position/trs_position_20260901.csv"]


def test_a_delivery_waiting_on_its_control_file_is_left_alone():
    inbox, d, fd, puts, triggers = _wired(GATE_FEED)
    (d / "positions.csv").write_bytes(DATA)

    results = _sweep(inbox, d)
    assert results == [], results
    assert puts == [] and triggers == [], (puts, triggers)
    # Still in the inbox, waiting -- not moved, not rejected.
    assert (d / "positions.csv").is_file()
    assert not (d / ".processed").exists()
    assert not (d / ".rejected").exists()


def test_an_unnameable_delivery_takes_its_control_file_to_rejected():
    inbox, d, fd, puts, triggers = _wired(GATE_FEED)
    (d / "positions.csv").write_bytes(DATA)
    (d / "positions.ctl").write_bytes(b"no date in here at all\n")

    results = _sweep(inbox, d)
    assert [r["status"] for r in results] == ["rejected"], results
    assert puts == [] and triggers == [], (puts, triggers)
    assert (d / ".rejected" / "positions.csv").is_file()
    # On its own a control file is unreadable evidence, and the commonest
    # identity failure is that the two disagree.
    assert (d / ".rejected" / "positions.ctl").is_file()


def test_a_zip_of_gated_members_lands_three_objects_each():
    inbox, d, fd, puts, triggers = _wired(ZIP_FEED)
    (d / "weekly.zip").write_bytes(_zipped({
        "POSITIONS_A.csv": DATA, "POSITIONS_A.ctl": b"DATE=20260831\nROWS=1\n",
        "POSITIONS_B.csv": DATA, "POSITIONS_B.ctl": b"DATE=20260901\nROWS=1\n"}))

    results = _sweep(inbox, d)
    assert puts == ["trs_position_20260831.ctl",
                    "trs_position_20260831.csv",
                    "trs_position_20260831.csv.meta.json",
                    "trs_position_20260901.ctl",
                    "trs_position_20260901.csv",
                    "trs_position_20260901.csv.meta.json"], puts
    assert [r["status"] for r in results] == ["conformed", "conformed"], results
    assert len(triggers) == 2, triggers
    # ONE INBOX FILE, N DELIVERIES: the container moves once, and never lands.
    assert (d / ".processed" / "trs_position" / "weekly.zip").is_file()


def test_one_bad_member_does_not_stop_the_others():
    inbox, d, fd, puts, triggers = _wired(ZIP_FEED)
    (d / "weekly.zip").write_bytes(_zipped({
        "POSITIONS_A.csv": DATA, "POSITIONS_A.ctl": b"DATE=20260831\nROWS=1\n",
        "POSITIONS_B.csv": DATA}))          # no control file for B

    results = _sweep(inbox, d)
    assert [r["status"] for r in results] == ["conformed", "rejected"], results
    assert puts == ["trs_position_20260831.ctl",
                    "trs_position_20260831.csv",
                    "trs_position_20260831.csv.meta.json"], puts
    # The container is still processed -- a good member landed out of it.
    assert (d / ".processed" / "trs_position" / "weekly.zip").is_file()
    assert not (d / ".rejected").exists()


def test_no_trigger_lands_without_calling_airflow():
    """`--no-trigger` (the CI build tier) must not reach for Airflow at all:
    the stack it runs on has no webserver."""
    from reporting_platform.ingest import inbox

    before = inbox.TRIGGER
    inbox.TRIGGER = False
    try:
        out = inbox._trigger(object(), "landing/x/y.csv")
    finally:
        inbox.TRIGGER = before
    assert out == {"triggered": False,
                   "reason": "--no-trigger: the caller ingests"}, out
