"""The normalize stage: landing object -> manifest in `ready/`.

Step 2 of docs/DELIVERY-SHAPES.md. The success criterion for the pass-through
normalizer is that NOTHING OBSERVABLE CHANGES for a plain CSV feed, so most of
these assert on preservation rather than on new behaviour.

Runs against tests/fakes3.py, not MinIO. What that cannot tell you is whether
Spark reads the part correctly or whether the Nessie branch merges -- those
are verified by running the stack.
"""
from __future__ import annotations

import json

from tests.fakes3 import FakeS3, install, uninstall
from tests.support import config_dir

LANDED = "landing/fo_trade/TRADE_20260811.csv"


def _setup():
    config_dir()
    s3 = FakeS3()
    monkey: list = []
    install(monkey, s3)
    from reporting_platform.common.context import feeds
    from reporting_platform.ingest import normalize as norm
    fd = feeds()["fo_trade"]
    s3.put(LANDED, "trade_id,notional\nT1,100\n")
    return s3, monkey, fd, norm


# ------------------------------------------------------------- the manifest
def test_manifest_records_date_parts_and_format():
    s3, monkey, fd, norm = _setup()
    try:
        m = norm.normalize(fd, LANDED)
        assert m["business_date"] == "2026-08-11", m["business_date"]
        assert m["parts"] == [{"object_key": LANDED, "bytes": 25}], m["parts"]
        # Format is captured from feeds.yml AT NORMALIZE TIME, so an ingest
        # can be reproduced later even if the config has moved on.
        assert m["format"] == {"delimiter": ",", "quote_char": '"',
                               "header": True, "encoding": "utf-8"}, m["format"]
        assert m["normalizer"] == "file/v1"
    finally:
        uninstall(monkey)


def test_plain_csv_is_not_copied():
    """The common case must cost one small JSON object, not a second copy."""
    s3, monkey, fd, norm = _setup()
    try:
        norm.normalize(fd, LANDED)
        written = [k for k in s3.objects if k != LANDED]
        assert written == [f"ready/fo_trade/TRADE_20260811.csv.json"], written
        # The part still points into landing/ -- nothing was duplicated.
        m = norm.read_manifest(written[0])
        assert m["parts"][0]["object_key"] == LANDED
    finally:
        uninstall(monkey)


def test_manifest_never_records_ingestion_status():
    """A status flag here would be the `stg` load-control table rebuilt.

    See ingest/normalize.py's header and arrival.already_ingested.
    """
    s3, monkey, fd, norm = _setup()
    try:
        m = norm.normalize(fd, LANDED)
        for forbidden in ("ingested", "status", "processed", "state"):
            assert forbidden not in m, f"{forbidden!r} is derived state"
    finally:
        uninstall(monkey)


def test_renormalizing_is_byte_identical():
    """`ready/` is only a cache if rebuilding it changes nothing.

    Anything time-based in the manifest -- a `now()` timestamp, a uuid --
    would break this and make re-normalizing produce a new delivery.
    """
    s3, monkey, fd, norm = _setup()
    try:
        key = norm.write_manifest(fd, norm.normalize(fd, LANDED, write=False))
        first = s3.objects[key][0]
        second_manifest = norm.normalize(fd, LANDED, write=False)
        assert json.dumps(second_manifest, indent=2, sort_keys=True).encode() == first
    finally:
        uninstall(monkey)


def test_received_at_is_the_deliverys_time_not_the_runs():
    s3, monkey, fd, norm = _setup()
    try:
        m = norm.normalize(fd, LANDED, write=False)
        assert m["received_at"].startswith("2026-08-01T06:00"), m["received_at"]
    finally:
        uninstall(monkey)


def test_unroutable_filename_is_an_error_with_the_pattern_in_it():
    s3, monkey, fd, norm = _setup()
    try:
        s3.put("landing/fo_trade/nonsense.csv", "a,b\n")
        try:
            norm.normalize(fd, "landing/fo_trade/nonsense.csv")
        except ValueError as exc:
            assert "filename_pattern" in str(exc), exc
        else:
            raise AssertionError("expected a ValueError")
    finally:
        uninstall(monkey)


# -------------------------------------------------------------- reconcile
def test_reconcile_gives_every_landed_object_a_manifest():
    """The production arrival path is an agent doing PutObject directly.

    Nothing of ours runs then, so `ready/` has to be reconcilable from
    `landing/` rather than filled by whatever did the landing.
    """
    s3, monkey, fd, norm = _setup()
    try:
        s3.put("landing/fo_trade/TRADE_20260812.csv", "trade_id\nT2\n")
        report = norm.reconcile(fd)
        assert report["landed"] == 2, report
        assert len(report["created"]) == 2, report
        assert len(norm.list_manifests(fd)) == 2
    finally:
        uninstall(monkey)


def test_reconcile_is_idempotent():
    s3, monkey, fd, norm = _setup()
    try:
        norm.reconcile(fd)
        again = norm.reconcile(fd)
        assert again["created"] == [], again
    finally:
        uninstall(monkey)


def test_one_unroutable_file_does_not_block_the_others():
    """Raising here would let a single bad filename stop the night's load."""
    s3, monkey, fd, norm = _setup()
    try:
        s3.put("landing/fo_trade/TRADE_20260812.csv", "trade_id\nT2\n")
        # Not matched by the pattern, so `matching()` filters it before
        # reconcile even sees it -- assert the outcome, not the mechanism.
        s3.put("landing/fo_trade/junk.txt", "nope")
        report = norm.reconcile(fd)
        assert len(report["created"]) == 2, report
        assert report["failed"] == [], report
    finally:
        uninstall(monkey)


# ------------------------------------------------- what ingest resolves to
def test_ingest_accepts_a_manifest_key_or_a_landing_key():
    """`--object landing/...` is what every runbook in this repo tells you to
    type. It must keep working, and must not leave a queue entry behind."""
    s3, monkey, fd, norm = _setup()
    try:
        from reporting_platform.ingest.ingest_feed import resolve_delivery

        mkey = norm.write_manifest(fd, norm.normalize(fd, LANDED, write=False))
        from_manifest = resolve_delivery(fd, mkey)
        before = set(s3.objects)
        from_landing = resolve_delivery(fd, LANDED)
        assert from_manifest == from_landing, (from_manifest, from_landing)
        assert set(s3.objects) == before, "a manual ingest wrote to ready/"
    finally:
        uninstall(monkey)


def test_business_date_override_beats_the_manifest():
    s3, monkey, fd, norm = _setup()
    try:
        from datetime import date

        from reporting_platform.ingest.ingest_feed import resolve_delivery
        m = resolve_delivery(fd, LANDED, business_date=date(2026, 1, 2))
        assert m["business_date"] == "2026-01-02", m["business_date"]
    finally:
        uninstall(monkey)


def test_unparsable_name_with_an_explicit_date_still_ingests():
    """The documented escape hatch for a delivery the pattern cannot route."""
    s3, monkey, fd, norm = _setup()
    try:
        from datetime import date

        from reporting_platform.ingest.ingest_feed import resolve_delivery
        key = "landing/fo_trade/oddly_named.csv"
        s3.put(key, "trade_id\nT9\n")
        m = resolve_delivery(fd, key, business_date=date(2026, 3, 4))
        assert m["business_date"] == "2026-03-04"
        assert m["parts"][0]["object_key"] == key
        assert m["normalizer"] == "manual/v1"
    finally:
        uninstall(monkey)
