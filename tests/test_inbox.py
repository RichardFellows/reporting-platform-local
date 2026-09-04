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
