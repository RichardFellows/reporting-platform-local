"""The orphan sweep's keep-set, and its refusals.

`retention/orphan_storage.py` deletes a warehouse prefix that no Nessie
reference points at. Its input is a set of things NOT to delete, which makes
every way of getting a short answer a way of deleting live data -- and the
sweep runs unattended, from the nightly `platform_housekeeping` chain.

So what is pinned here is the arithmetic and the refusals, both out of fakes:
a Nessie stand-in returning entries (or failing), and an S3 stand-in listing
keys. What this cannot tell you is whether a real Nessie reports
`metadataLocation` in the shape assumed, or whether MinIO deletes what it is
asked to -- those were verified by running them.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from tests.support import config_dir

OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _mod():
    """The module, imported AFTER the config has been pointed somewhere.

    `sweep_orphan_prefixes` reads `maintenance.yml` for its age floor, and
    `tests.support` purges every `reporting_platform` module to reset the
    config cache -- so a file-scope import here would bind a stale one.
    """
    config_dir()
    from reporting_platform.retention import orphan_storage
    return orphan_storage


class _Gone(Exception):
    """What `Nessie._req` raises for a ref that 404s: an HTTPError carrying
    the response. Only the status code is read."""

    def __init__(self, status: int):
        self.response = type("R", (), {"status_code": status})()
        super().__init__(f"{status}")


class _FakeNessie:
    """Refs and their entries. A ref mapped to an exception raises it."""

    def __init__(self, refs: dict):
        self._refs = refs

    def list_references(self):
        return [{"name": name} for name in self._refs]

    def list_entries(self, ref: str):
        value = self._refs[ref]
        if isinstance(value, Exception):
            raise value
        return [{"content": {"metadataLocation": loc}} for loc in value]


def _meta(prefix: str) -> str:
    return f"s3a://lakehouse/{prefix}/metadata/00001-abc.metadata.json"


class _FakeS3:
    """Just enough of a boto3 client for `warehouse_table_prefixes`."""

    def __init__(self, keys: list[str], newest: datetime = OLD):
        self.keys = keys
        self.newest = newest
        self.deleted: list[str] = []

    def get_paginator(self, _name):
        client = self

        class _P:
            def paginate(self, Bucket, Prefix):        # noqa: N803
                return [{"Contents": [
                    {"Key": k, "Size": 10, "LastModified": client.newest}
                    for k in client.keys if k.startswith(Prefix)]}]
        return _P()

    def delete_object(self, Bucket, Key):              # noqa: N803
        self.deleted.append(Key)


# ------------------------------------------------------------ the live set
def test_the_live_set_spans_every_reference_not_just_main():
    """Reading only `main` would delete the working output of every open
    build branch, which is the whole reason this walks all of them.
    """
    m = _mod()
    live = m.live_table_prefixes(_FakeNessie({
        "main": [_meta("warehouse/raw/fo_trade_aaa")],
        "build/x": [_meta("warehouse/prepared/dim_cpty_bbb")],
    }))
    assert live == {"warehouse/raw/fo_trade_aaa",
                    "warehouse/prepared/dim_cpty_bbb"}


def test_a_reference_that_vanished_mid_sweep_is_tolerated():
    """A branch deleted between listing the refs and reading its entries
    genuinely has no live tables any more -- that is what this reclaims.
    """
    m = _mod()
    live = m.live_table_prefixes(_FakeNessie({
        "main": [_meta("warehouse/raw/fo_trade_aaa")],
        "build/gone": _Gone(404),
    }))
    assert live == {"warehouse/raw/fo_trade_aaa"}


def test_a_reference_that_could_not_be_read_refuses_the_whole_answer():
    """THE ONE THAT DELETES THE WAREHOUSE. A 500, a 401 or a timeout means
    the ref may hold live tables we simply did not see, and a short keep-set
    is a deletion order. It raises rather than returning what it managed.
    """
    m = _mod()
    try:
        m.live_table_prefixes(_FakeNessie({
            "main": [_meta("warehouse/raw/fo_trade_aaa")],
            "build/x": _Gone(500),
        }))
    except m.IncompleteLiveSet as exc:
        assert "build/x" in str(exc)
    else:
        raise AssertionError("an unreadable reference must refuse")


def test_an_entry_with_no_metadata_location_is_skipped_not_fatal():
    """Namespaces come back from `list_entries` too, and carry none."""
    m = _mod()

    class _N(_FakeNessie):
        def list_entries(self, ref):
            return [{"content": {}}, {},
                    {"content": {"metadataLocation": _meta("warehouse/raw/t_aaa")}}]

    assert m.live_table_prefixes(_N({"main": []})) == {"warehouse/raw/t_aaa"}


# --------------------------------------------------- the warehouse listing
def test_the_prefix_depth_follows_the_configured_root():
    """`REPORTING_WAREHOUSE` may be nested, and `_METADATA_RE` captures the
    WHOLE root into a live prefix. A hardcoded depth of 3 against
    `s3a://bucket/a/warehouse` yields `a/warehouse/<namespace>`, which can
    never equal a live prefix -- so every namespace reads as an orphan.
    """
    import os
    m = _mod()
    before = os.environ.get("REPORTING_WAREHOUSE")
    try:
        os.environ["REPORTING_WAREHOUSE"] = "s3a://lakehouse/a/warehouse"
        m._client = lambda: _FakeS3([
            "a/warehouse/raw/fo_trade_aaa/metadata/00001.metadata.json",
            "a/warehouse/raw/fo_trade_aaa/data/00001.parquet",
        ])
        found = m.warehouse_table_prefixes()
    finally:
        if before is None:
            os.environ.pop("REPORTING_WAREHOUSE", None)
        else:
            os.environ["REPORTING_WAREHOUSE"] = before
    assert list(found) == ["a/warehouse/raw/fo_trade_aaa"]
    assert found["a/warehouse/raw/fo_trade_aaa"]["objects"] == 2


def test_the_default_root_gives_the_prefix_the_live_set_uses():
    """The two halves must produce the same string or nothing ever matches."""
    m = _mod()
    m._client = lambda: _FakeS3(
        ["warehouse/raw/fo_trade_aaa/metadata/00001.metadata.json"])
    assert list(m.warehouse_table_prefixes()) == ["warehouse/raw/fo_trade_aaa"]
    assert m.live_table_prefixes(_FakeNessie(
        {"main": [_meta("warehouse/raw/fo_trade_aaa")]})) == \
        {"warehouse/raw/fo_trade_aaa"}


def test_an_object_shallower_than_a_table_prefix_is_ignored():
    """A stray key directly under the namespace is not a table directory."""
    m = _mod()
    m._client = lambda: _FakeS3(["warehouse/raw/stray.txt"])
    assert m.warehouse_table_prefixes() == {}


# ------------------------------------------------------------ the refusals
def _sweep_with(m, live, keys, newest=OLD):
    fake = _FakeS3(keys, newest)
    m._client = lambda: fake
    m.Nessie = lambda *a, **k: None
    if isinstance(live, Exception):
        def _raise(_n=None):
            raise live
        m.live_table_prefixes = _raise
    else:
        m.live_table_prefixes = lambda _n=None: set(live)
    return fake


def test_an_unreadable_catalog_refuses_the_sweep_rather_than_deleting():
    m = _mod()
    fake = _sweep_with(m, m.IncompleteLiveSet("nessie is down"),
                       ["warehouse/raw/t_aaa/metadata/1.json"])
    report = m.sweep_orphan_prefixes(dry_run=False)
    assert "nessie is down" in report["refused"]
    assert report["orphans"] == [] and fake.deleted == []


def test_an_empty_live_set_against_a_non_empty_warehouse_refuses():
    """The catalog answered, and answered that it holds no tables at all,
    while object storage holds some. Every prefix would qualify. That is not
    a state to act destructively on, whichever way it came about.
    """
    m = _mod()
    fake = _sweep_with(m, set(), ["warehouse/raw/t_aaa/metadata/1.json"])
    report = m.sweep_orphan_prefixes(dry_run=False)
    assert "no live table prefixes" in report["refused"]
    assert report["orphans"] == [] and fake.deleted == []


def test_an_empty_warehouse_is_not_a_refusal():
    """Nothing present and nothing live is a no-op, not an alarm."""
    m = _mod()
    _sweep_with(m, set(), [])
    report = m.sweep_orphan_prefixes(dry_run=True)
    assert "refused" not in report and report["orphans"] == []


# ----------------------------------------------------------- and the sweep
def test_a_prefix_no_reference_points_at_is_an_orphan():
    m = _mod()
    _sweep_with(m, {"warehouse/raw/live_aaa"},
                ["warehouse/raw/live_aaa/metadata/1.json",
                 "warehouse/raw/dead_bbb/metadata/1.json"])
    report = m.sweep_orphan_prefixes(dry_run=True)
    assert report["orphans"] == ["warehouse/raw/dead_bbb"]


def test_a_dry_run_deletes_nothing():
    m = _mod()
    fake = _sweep_with(m, set(["warehouse/raw/live_aaa"]),
                       ["warehouse/raw/live_aaa/metadata/1.json",
                        "warehouse/raw/dead_bbb/metadata/1.json"])
    m.sweep_orphan_prefixes(dry_run=True)
    assert fake.deleted == []


def test_a_prefix_younger_than_the_age_floor_is_never_touched():
    """It may belong to a write still in flight. Same floor as
    `remove_orphan_files`, and it absorbs clock skew too.
    """
    m = _mod()
    _sweep_with(m, {"warehouse/raw/live_aaa"},
                ["warehouse/raw/live_aaa/metadata/1.json",
                 "warehouse/raw/new_bbb/metadata/1.json"],
                newest=datetime.now(timezone.utc) - timedelta(hours=1))
    report = m.sweep_orphan_prefixes(dry_run=True)
    assert report["orphans"] == []
    assert report["skipped_too_new"] == ["warehouse/raw/new_bbb"]
