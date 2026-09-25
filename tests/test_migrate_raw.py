"""A declared feed gets its raw table before its first delivery.

`prepared_build` builds every prepared model on every run, and the model of a
feed with no raw table fails with `TABLE_OR_VIEW_NOT_FOUND` -- so until that
feed first delivered, write-audit-publish kept the branch and NO feed
published. `ingest.migrate_raw` now creates the absent tables, empty, and the
deploy steps run it. docs/DECISIONS.md#a-declared-feed-has-a-raw-table-before-it-delivers

Two halves, both without a stack:

  * what `migrate()` does with an absent, a present and an unreadable table,
    against a Spark and a Nessie that only record what they are asked;
  * that every deploy path runs it -- `airflow-init`, the chart's
    `platform-init` hook, the standalone runner's `setup` -- in EMBEDDED mode,
    because the first two do not wait for a Spark cluster.

What this cannot show is that the empty table is one the prepared models
build against. That was verified live; see the DECISIONS entry.
"""
from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import yaml

from reporting_platform.ingest import migrate_raw
from reporting_platform.ingest.ingest_feed import _PROVENANCE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
NOT_FOUND = ("[TABLE_OR_VIEW_NOT_FOUND] The table or view `raw`.`{}` cannot "
             "be found.")


def _feed(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, raw_namespace="raw",
                           raw_table=f"lakehouse.raw.{name}",
                           columns=["trade_id", "notional"])


class _Spark:
    """Answers `SELECT * ... LIMIT 0` from `tables`; records everything else.

    A CREATE on a branch (`raw.`x@migrate/...``) does NOT make the table
    exist on main -- only the merge does, which the fake Nessie applies.
    """

    def __init__(self, tables: dict[str, list] | None = None,
                 broken: set[str] = frozenset()):
        self.tables = dict(tables or {})
        self.broken = set(broken)
        self.statements: list[str] = []

    def sql(self, stmt: str):
        stmt = " ".join(stmt.split())
        self.statements.append(stmt)
        m = re.match(r"SELECT \* FROM (\S+) LIMIT 0", stmt)
        if m:
            table = m.group(1)
            if table in self.broken:
                raise RuntimeError("Connection refused: nessie:19120")
            if table not in self.tables:
                raise RuntimeError(NOT_FOUND.format(table.rsplit(".", 1)[1]))
            return SimpleNamespace(dtypes=self.tables[table])
        m = re.match(r"CREATE TABLE IF NOT EXISTS (\S+) ", stmt)
        if m and "@" not in m.group(1):
            self.tables.setdefault(m.group(1), [])
        return SimpleNamespace(dtypes=[])

    def stop(self):
        pass


class _Nessie:
    def __init__(self, spark: _Spark, main_has_commits: bool = True):
        self.spark = spark
        self.main_has_commits = main_has_commits
        self.calls: list[tuple] = []

    def _req(self, method, path):
        assert (method, path) == ("GET", "/trees/main/history")
        return {"logEntries": [{"commitMeta": {}}] if self.main_has_commits
                else []}

    def create_branch(self, name):
        self.calls.append(("create_branch", name))

    def merge(self, branch, into):
        self.calls.append(("merge", branch, into))
        # What the branch created is on main now.
        for stmt in self.spark.statements:
            m = re.match(r"CREATE TABLE IF NOT EXISTS (\S+)@", stmt)
            if m:
                self.spark.tables.setdefault(
                    m.group(1).replace("`", ""), [])

    def delete_reference(self, name):
        self.calls.append(("delete_reference", name))


def _run(spark: _Spark, nessie: _Nessie, fds, dry_run=False) -> dict:
    with mock.patch.object(migrate_raw, "feeds",
                           lambda: {fd.name: fd for fd in fds}), \
         mock.patch.object(migrate_raw, "spark_session",
                           lambda *a, **k: spark), \
         mock.patch.object(migrate_raw, "Nessie", lambda: nessie):
        return migrate_raw.migrate(dry_run=dry_run)


def _status(report: dict, feed: str) -> str:
    return next(f["status"] for f in report["feeds"] if f["feed"] == feed)


# ----------------------------------------------------- an absent table
def test_a_feed_with_no_raw_table_gets_one_on_a_branch_and_merged():
    """THE FIX. This used to report `absent` and create nothing, which left
    the feed's prepared model failing every build until its first delivery.
    """
    spark = _Spark()
    nessie = _Nessie(spark)
    report = _run(spark, nessie, [_feed("brand_new")])

    assert _status(report, "brand_new") == "created"
    assert report["created"] == 1
    creates = [s for s in spark.statements
               if s.startswith("CREATE TABLE IF NOT EXISTS")]
    assert len(creates) == 1 and "brand_new@migrate/brand_new/" in creates[0]
    ops = [c[0] for c in nessie.calls]
    assert ops == ["create_branch", "merge", "delete_reference"]
    assert nessie.calls[1][2] == "main"


def test_the_namespace_goes_on_main_before_the_branch_is_cut():
    """A namespace addressed `@branch` is created on main under a junk name
    (docs/DECISIONS.md#namespace-before-branch), so it is created first.
    """
    spark = _Spark()
    _run(spark, _Nessie(spark), [_feed("brand_new")])
    ns = [i for i, s in enumerate(spark.statements)
          if s.startswith("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")]
    table = [i for i, s in enumerate(spark.statements)
             if s.startswith("CREATE TABLE")]
    assert ns and table and ns[0] < table[0]
    assert "@" not in spark.statements[ns[0]]


def test_the_table_is_created_with_the_ingest_contract():
    """Same DDL `ingest()` would have run: declared columns as strings, the
    platform's own, provenance included, partitioned by COB date. A second
    definition would drift from the first.
    """
    spark = _Spark()
    _run(spark, _Nessie(spark), [_feed("brand_new")])
    ddl = next(s for s in spark.statements if s.startswith("CREATE TABLE"))
    for col in ("`trade_id` STRING", "`notional` STRING", "_cob_date DATE",
                "_delivery_id STRING", "_source_system STRING"):
        assert col in ddl, col
    assert "PARTITIONED BY (days(_cob_date))" in ddl


def test_a_fresh_catalog_is_bootstrapped_on_main_not_branched():
    """A `main` with no commits cannot take a branch+merge (Nessie refuses the
    merge against its sentinel hash), so the first table goes straight onto
    main -- the path the first ingest of a cold stack takes too.
    """
    spark = _Spark()
    nessie = _Nessie(spark, main_has_commits=False)
    report = _run(spark, nessie, [_feed("brand_new")])
    assert _status(report, "brand_new") == "created"
    assert nessie.calls == []
    assert "lakehouse.raw.brand_new" in spark.tables


def test_a_dry_run_creates_nothing_and_says_what_it_would():
    spark = _Spark()
    nessie = _Nessie(spark)
    report = _run(spark, nessie, [_feed("brand_new")], dry_run=True)
    assert _status(report, "brand_new") == "would_create"
    assert report["created"] == 0
    assert not [s for s in spark.statements if s.startswith("CREATE")]
    assert nessie.calls == []


# ------------------------------------------------ a table it cannot read
def test_a_table_it_could_not_read_is_not_a_table_to_create():
    """`_exists` used to be False on ANY error, harmless while absent meant
    skip. Now absent means CREATE, so a catalog that is down must raise
    rather than read as a feed to set up.
    """
    spark = _Spark(broken={"lakehouse.raw.fo_trade"})
    nessie = _Nessie(spark)
    try:
        _run(spark, nessie, [_feed("fo_trade")])
    except RuntimeError as exc:
        assert "Connection refused" in str(exc)
    else:
        raise AssertionError("an unreadable table was not an error")
    assert not [s for s in spark.statements if s.startswith("CREATE")]
    assert nessie.calls == []


# ------------------------------------------------ an existing table
def test_a_current_table_is_left_alone():
    fd = _feed("fo_trade")
    have = [(c, "string") for c in fd.columns + [
        c for c, _ in _PROVENANCE_COLUMNS]]
    spark = _Spark({fd.raw_table: have})
    nessie = _Nessie(spark)
    report = _run(spark, nessie, [fd])
    assert _status(report, "fo_trade") == "already_current"
    assert report["created"] == 0
    assert nessie.calls == []


# ------------------------------------------------ every deploy path runs it
def _embedded_migrate_segment(command: str) -> str:
    """The `&&`-separated step that runs migrate_raw, or an AssertionError."""
    steps = [" ".join(s.split()) for s in command.split("&&")]
    hits = [s for s in steps if "reporting_platform.ingest.migrate_raw" in s]
    assert len(hits) == 1, f"migrate_raw is not one step of: {steps}"
    return hits[0]


def _assert_embedded(step: str) -> None:
    # Neither init waits for the Spark cluster, and a standalone master with
    # no worker queues a job forever -- so it must not be the cluster's.
    assert step.startswith("PLATFORM_EXECUTION=embedded "), step
    assert re.search(r"SPARK_MASTER='?local\[\d+\]'?", step), step


def test_airflow_init_creates_the_raw_tables_embedded():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    command = compose["services"]["airflow-init"]["command"]
    step = _embedded_migrate_segment(command)
    _assert_embedded(step)
    # After the registry schema, as in the chart. Not strictly ordered by
    # need, but a reordering is a change somebody should make on purpose.
    assert command.index("registry schema") < command.index("migrate_raw")


def test_the_charts_init_hook_creates_the_raw_tables_embedded():
    """Read out of the Job's `args:` block, not the file: the template's
    header comment names migrate_raw too, and matching prose is how a check
    passes with the thing it checks deleted.
    """
    text = (ROOT / "deploy/helm/reporting-platform/templates/"
            "job-platform-init.yaml").read_text()
    m = re.search(r"\n\s+args:\n\s+- >\n(.*?)\n\s+envFrom:", text, re.S)
    assert m, "platform-init's args block moved; update this test"
    _assert_embedded(_embedded_migrate_segment(m.group(1)))


def test_the_standalone_runners_setup_creates_the_raw_tables():
    """`setup` is "what airflow-init does that this path needs". The runner
    service is embedded already, so it only has to call it.
    """
    from reporting_platform.pipeline import __main__ as pipeline

    called = []
    with mock.patch("reporting_platform.registry.db.ensure_schema"), \
         mock.patch.object(migrate_raw, "migrate",
                           lambda **k: called.append(k) or
                           {"created": 1, "migrated": 0,
                            "already_current": 0}), \
         mock.patch.object(pipeline.subprocess, "run"):
        out = pipeline.setup()
    assert called == [{}]
    assert out["raw_tables"]["created"] == 1


def test_housekeeping_still_runs_it_first_every_night():
    """What covers a feed saved from the console between deploys: the console
    writes config live, and nothing else would create its table before its
    first delivery.
    """
    src = (ROOT / "airflow/dags/platform_housekeeping.py").read_text()
    assert '_spark_subprocess("migrate-raw", mode)' in src
    from reporting_platform.common import spark_task
    assert spark_task.OPS["migrate-raw"].endswith(":op_migrate_raw")
