"""The arrivals view: what the console makes of a delivery it did not record.

`reporting_platform/ui/arrivals.py` writes nothing. Every value it produces is
a projection of a registry row, an inbox filename or an Airflow run, and those
projections are what this pins -- the same split `test_registry.py` draws, for
the same reason: a fake database would agree with whatever the code asked it.

The two that matter most are the ones a wrong answer would be believed:

  * `checks()` puts a green pill next to a checksum. A verdict that said `ok`
    for a delivery nobody had compared, or for a multi-part one whose recorded
    hash is of the container rather than of the rows, is worse than no pill.
  * `_ingested_key()` decides which ingest run a delivery is shown as having.
    Attaching the wrong run to a delivery is how a page reports a failure as a
    success.

What these cannot tell you is whether the console renders them, whether
Airflow answers as it is assumed to, or whether the registry holds what it is
read for here. Those were verified by running them -- a file dropped into
`inbox/`, one landing and one quarantined, and both pages read back.
"""
from __future__ import annotations

from tests.support import feeds_from, synthetic

# The gate's own classes, restated here so a change to either list is a test
# failure rather than a silently narrower page.
from reporting_platform.registry.rejections import REASON_CLASSES


def _arrivals():
    """The module, imported AFTER support has pointed the config somewhere.

    `tests.support` purges every `reporting_platform` module to reset the
    config cache, so importing this at file scope would leave a stale one
    bound to the name.
    """
    from reporting_platform.ui import arrivals
    return arrivals


def _row(**over):
    """A registry delivery row, as `deliveries.recent` returns one."""
    row = {"feed": "t_one", "delivery_id": "A_20260801.csv",
           "source_system": "SRC", "cob_date": "2026-08-01",
           "received_at": "2026-08-01T06:00:00+00:00",
           "first_seen_at": "2026-08-01T06:05:00+00:00",
           "source_object": "landing/t_one/A_20260801.csv",
           "manifest_key": "ready/t_one/A_20260801.csv.json",
           "normalizer": "file/v1", "bytes": 33, "md5": "a" * 32,
           "schema_version": "abc123", "origin": "direct",
           "origin_uri": "s3://lakehouse/landing/t_one/A_20260801.csv",
           "source_filename": None, "source_container": None,
           "control_object": None, "declared_row_count": None,
           "declared_md5": None, "producer_run_id": None, "parts": 1}
    row.update(over)
    return row


# ------------------------------------------------------------------- checks
def test_checksum_matching_is_ok():
    feeds_from(synthetic())
    c = _arrivals().checks(_row(declared_md5="A" * 32, md5="a" * 32))
    # Case-insensitively: a control file is free to state its hex in either.
    assert c["md5"]["verdict"] == "ok", c


def test_checksum_differing_is_a_mismatch():
    feeds_from(synthetic())
    c = _arrivals().checks(_row(declared_md5="b" * 32, md5="a" * 32))
    assert c["md5"]["verdict"] == "mismatch", c


def test_nothing_declared_is_not_a_pass():
    """The failure this guards is a green tick on a feed that declares nothing.

    Most feeds have no control file at all, so `not_declared` is the common
    case; collapsing it into `ok` would put a checksum-verified pill on every
    delivery in the estate.
    """
    feeds_from(synthetic())
    c = _arrivals().checks(_row())
    assert c["md5"]["verdict"] == "not_declared", c
    assert c["row_count"]["verdict"] == "not_declared", c


def test_a_declared_row_count_is_answered_at_ingest_not_here():
    """Nothing counts rows without reading the file, and that is a Spark job.

    The verdict has to say so: reporting the DECLARED count as though it had
    been checked is the one reading of this column that would be believed and
    wrong.
    """
    feeds_from(synthetic())
    c = _arrivals().checks(_row(declared_row_count=2))
    assert c["row_count"] == {"declared": 2, "verdict": "at_ingest"}, c


def test_a_multi_part_delivery_is_not_comparable():
    """`ingest_feed._parts_md5` hashes the parts in order; the registry
    recorded the SOURCE object's hash. For one part those are the same bytes,
    which is what makes the comparison legitimate at all -- for several they
    are not, and the answer is that there isn't one."""
    feeds_from(synthetic())
    c = _arrivals().checks(_row(parts=3, declared_md5="a" * 32))
    assert c["md5"]["verdict"] == "not_comparable", c


# ------------------------------------------------------------- the two names
def test_a_promoted_delivery_shows_both_of_its_names():
    """Two filenames and they are different strings. The upstream knows only
    the first, so a page showing only the landed one makes the delivery
    unfindable by the name anyone would search for."""
    feeds_from(synthetic())
    a = _arrivals()._accepted(_row(source_filename="positions.20260801.txt",
                                   origin="inbox"))
    assert a["source_name"] == "positions.20260801.txt", a
    assert a["delivery_id"] == "A_20260801.csv", a
    assert a["renamed"] is True, a


def test_an_unrenamed_delivery_is_not_flagged_as_renamed():
    feeds_from(synthetic())
    a = _arrivals()._accepted(_row())
    assert a["source_name"] == "A_20260801.csv", a
    assert a["renamed"] is False, a


def test_parts_is_always_a_count():
    """`recent` returns the number and `by_id` the objects. One field name
    holding two shapes is how the page comes to render `[object Object]`."""
    arr = _arrivals()
    feeds_from(synthetic())
    listed = arr._accepted(_row(parts=2))
    detailed = arr._accepted(_row(parts=[{"part_no": 0, "object_key": "k",
                                          "bytes": 1}]))
    assert listed["parts"] == 2, listed
    assert detailed["parts"] == 1, detailed


def test_a_rejection_is_an_arrival():
    """Refused deliveries are merged into the same list on purpose: separate
    lists is how the console comes to show a morning that received something
    unreadable as a morning that received nothing."""
    feeds_from(synthetic())
    r = _arrivals()._refused(
        {"quarantine_key": "quarantine/_unclaimed/2026/08/x_y.txt",
         "feed": None, "source_filename": "y.txt",
         "received_at": "2026-08-01T06:00:00+00:00",
         "rejected_at": "2026-08-01T06:00:02+00:00",
         "reason_class": "unroutable", "reason": "no feed claims it",
         "bytes": 13, "md5": "c" * 32})
    assert r["outcome"] == "quarantined", r
    assert r["at"] == "2026-08-01T06:00:00+00:00", r
    assert r["feed"] is None, r
    assert r["reason_class"] in REASON_CLASSES, r


# ------------------------------------------------------- which run ingested
def test_a_run_matches_its_landing_key_its_manifest_and_its_parts():
    """`normalize` accepts either key, so both name this delivery -- and so
    does any of its parts, which is what a re-run of one archive member
    would carry."""
    arr = _arrivals()
    feeds_from(synthetic())
    row = _row(parts=[{"part_no": 0, "object_key": "ready/t_one/A/m1.csv",
                       "bytes": 1}])
    for key in ("landing/t_one/A_20260801.csv",
                "ready/t_one/A_20260801.csv.json",
                "ready/t_one/A/m1.csv"):
        assert arr._matches({"object_key": key}, row), key
    assert not arr._matches({"object_key": "landing/t_one/B.csv"}, row)


def test_a_run_with_no_key_matches_nothing():
    """The dangerous direction. A run whose key could not be resolved must
    attach to NO delivery -- attaching it to one is how a page reports another
    delivery's failure as this one's."""
    arr = _arrivals()
    feeds_from(synthetic())
    assert not arr._matches({"object_key": None}, _row())
    assert not arr._matches({}, _row())


def test_the_conf_is_read_before_anything_is_fetched():
    arr = _arrivals()
    feeds_from(synthetic())
    key = arr._ingested_key("ingest_t_one", {
        "run_id": "r1", "conf": {"object_key": "landing/t_one/A_20260801.csv"}})
    assert key == "landing/t_one/A_20260801.csv", key


def test_an_xcom_repr_is_parsed_and_rubbish_is_not_guessed_at():
    """Airflow 2.10's XCom endpoint returns the value as a Python REPR, not as
    JSON -- verified against this stack. So it is read with `literal_eval`,
    and anything that will not parse gives None rather than a key invented
    from a string."""
    arr = _arrivals()
    feeds_from(synthetic())
    calls = []

    class _Stub:
        AirflowError = arr.orchestration.AirflowError

        @staticmethod
        def xcom(dag_id, run_id, task_id, key="return_value"):
            calls.append((dag_id, run_id, task_id))
            return _Stub.value

    real = arr.orchestration
    arr.orchestration = _Stub
    try:
        _Stub.value = "{'object_key': 'ready/t_one/A.json', 'cob_date': None}"
        assert arr._ingested_key("d", {"run_id": "r"}) == "ready/t_one/A.json"
        _Stub.value = "not a literal at all"
        assert arr._ingested_key("d", {"run_id": "r"}) is None
        _Stub.value = None
        assert arr._ingested_key("d", {"run_id": "r"}) is None
    finally:
        arr.orchestration = real
    assert len(calls) == 3, calls
    assert calls[0][2] == "resolve_arrival", calls


# ---------------------------------------------------- classifying at the door
def _classify(filename, feeds_yml=None):
    """What the console labels a file the gate has already routed."""
    # The config has to be pointed somewhere BEFORE `inbox` is imported:
    # `context.CONFIG_DIR` is read at import time, and `feeds_from` purges
    # every reporting_platform module to reset it.
    feeds_from(feeds_yml if feeds_yml is not None else synthetic())
    from reporting_platform.ingest import inbox
    fd, reason, is_control = inbox.route(filename)
    if fd is None:
        return "ambiguous" if "more than one" in (reason or "") else "unroutable"
    return _arrivals()._classification(fd, filename, is_control)


def test_a_conformant_name_is_labelled_conformant():
    assert _classify("A_20260801.csv") == "conformant"


def test_an_upstream_name_is_labelled_gated():
    """A name only `arrival.source_pattern` claims: it goes through the gate
    and is promoted under a name `filename_pattern` describes."""
    yml = synthetic(feed_extra="""    arrival:
      source_pattern: 'pos_(?P<cob_date>\\d{8})\\.txt'
      control:
        pattern: '{stem}\\.ctl'
    delivery:
      control:
        pattern: '{stem}\\.ctl'
        row_count: 'ROWS=(?P<rows>\\d+)'
""")
    assert _classify("pos_20260801.txt", yml) == "gated"
    assert _classify("pos_20260801.ctl", yml) == "control"
    # And the conformant name still wins, which is route()'s own ordering:
    # a file landing already accepts needs no rename and no control file.
    assert _classify("A_20260801.csv", yml) == "conformant"


def test_a_name_nobody_claims_is_unroutable():
    assert _classify("whatever.txt") == "unroutable"


# ------------------------------------------------- how far back to ask Airflow
def test_the_run_limit_scales_with_the_rows_on_the_page():
    """A FIXED LIMIT MAKES `no run recorded` A LIE.

    The page size is the reader's choice and the row selector offers 250. A
    delivery whose run is older than the newest N matches nothing and renders
    as the one state this module promises means "Airflow trimmed its
    history", never "not ingested".
    """
    a = _arrivals()
    assert a._run_limit(100) > 100
    assert a._run_limit(250) >= 250 or a._run_limit(250) == a.RUN_LIMIT_CEILING


def test_the_run_limit_has_a_floor_and_a_ceiling():
    """A floor because one row on screen still wants its re-run history; a
    ceiling because this is an HTTP call to a scheduler, not a query.
    """
    a = _arrivals()
    assert a._run_limit(0) == a.RUN_LIMIT_FLOOR
    assert a._run_limit(1) == a.RUN_LIMIT_FLOOR
    assert a._run_limit(10_000) == a.RUN_LIMIT_CEILING


def test_the_run_limit_leaves_room_for_re_runs():
    """One run per row would push the oldest rows off the end as soon as any
    delivery had been ingested twice -- which is exactly the history somebody
    reading this page is looking for.
    """
    a = _arrivals()
    assert a._run_limit(40) >= 80


# ------------------------------------------------------------- the timestamps
def test_a_timestamp_is_normalised_to_utc_not_merely_formatted():
    """`recent()` merges two queries and sorts the result LEXICALLY, which is
    chronological only while every string carries the same offset. psycopg2
    renders a TIMESTAMPTZ in the server's timezone, which `registry/db.py`
    never sets.
    """
    from datetime import datetime, timedelta, timezone as tz
    a = _arrivals()
    east = tz(timedelta(hours=10))
    assert a._stamp(datetime(2026, 8, 4, 6, 0, tzinfo=east)) == \
        "2026-08-03T20:00:00+00:00"


def test_two_offsets_sort_chronologically_once_stamped():
    """The DST case, which is the one that actually reaches this: two rows
    hours apart whose raw ISO strings compare the wrong way round.
    """
    from datetime import datetime, timedelta, timezone as tz
    a = _arrivals()
    earlier = datetime(2026, 8, 4, 6, 0, tzinfo=tz(timedelta(hours=10)))
    later = datetime(2026, 8, 3, 21, 0, tzinfo=tz(timedelta(hours=0)))
    assert earlier.isoformat() > later.isoformat()          # the raw strings lie
    assert a._stamp(earlier) < a._stamp(later)              # stamped, they do not


def test_a_naive_or_absent_timestamp_is_left_alone():
    """Every column feeding this is NOT NULL, so `None` is defence against a
    row from a future schema rather than an expected case -- and it must not
    raise.
    """
    from datetime import datetime
    a = _arrivals()
    assert a._stamp(None) is None
    assert a._stamp("2026-08-03T20:00:00+00:00") == "2026-08-03T20:00:00+00:00"
    assert a._stamp(datetime(2026, 8, 3, 20, 0)) == "2026-08-03T20:00:00"
