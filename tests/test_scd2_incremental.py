"""SCD2 incremental builds must agree with a full rebuild, run after run.

THE DEFECT THIS PINS. `ref_counterparty` and `ref_rating` merge on
`(key, effective_from)`, and dbt-spark's MERGE only updates and inserts. Once
`dedupe_rank` selects the newest delivery per COB date, a re-delivery that
drops a key -- or reverts its value -- RETRACTS a version an earlier
incremental run already wrote. Nothing re-derives that version's row, so the
merge never touches it: it stays current, the version before it stays closed,
and when the key next changes a second open version is inserted beside it.
`as_of()` then matches both and doubles the key's rows downstream.
See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date

HOW IT IS RUN. Each step renders the REAL model file with jinja2 (the
renderer in `test_dedupe_rank.py`) and executes it end to end in DuckDB,
twice: once as dbt's first build (`create table as`) into a full-rebuild
table, and once through dbt-spark's incremental flow into a table that has
seen every previous step -- the model as a view over `this`, then the MERGE
statement the project resolves for it. That statement is rendered from
`dbt/macros/*.sql` when the project overrides `spark__get_merge_sql`, and
otherwise is dbt-spark 1.8.0's own, written out below from its
`strategies.sql` for the one configuration these models use (a list
`unique_key`, no predicates, no merge_update/exclude columns). DuckDB runs
Spark's MERGE text as written, conditional clauses and `insert *` included.

What is shimmed, beyond `test_dedupe_rank.py`'s list: five Spark functions
DuckDB spells differently, renamed at call sites and defined as DuckDB macros
(`sha2`, `date_sub`, `trunc`, `to_date`, `element_at`). Nothing else is
rewritten.

What this cannot prove: that Iceberg on Spark executes the same MERGE the
same way on a Nessie branch. That is a live run.
"""
from __future__ import annotations

import re

import duckdb

from tests.test_dedupe_rank import DBT, PREPARED, _Config, _render

# (model, key columns, the attribute that changes, constant attributes)
MODELS = (
    ("ref_counterparty", ["counterparty_id"], "legal_name",
     {"country_code": "GB", "sector": "BANK", "parent_counterparty_id": None,
      "is_active": "Y"}),
    ("ref_rating", ["counterparty_id", "agency"], "rating",
     {"rating_date": "2026-01-01", "outlook": "STABLE"}),
)

FULL, INC = "full_build", "incremental_build"
DELETED = "0001-01-01"


# ------------------------------------------------------------ engine shims
_SHIMS = {
    "sha2": "create macro spark_sha2(s, n) as sha256(s)",
    "date_sub": "create macro spark_date_sub(d, n) as (d - n::integer)",
    "trunc": "create macro spark_trunc(d, fmt) as date_trunc('month', d)::date",
    "to_date": ("create macro spark_to_date(s, fmt) as (case fmt "
                "when 'yyyy-MM-dd' then try_strptime(s, '%Y-%m-%d') "
                "else try_strptime(s, '%Y%m%d') end)::date"),
    "element_at": "create macro spark_element_at(l, i) as list_extract(l, i)",
}


def _duck(sql: str) -> str:
    for fn in _SHIMS:
        sql = re.sub(rf"\b{fn}\(", f"spark_{fn}(", sql, flags=re.I)
    return sql


def _connect():
    con = duckdb.connect()
    for ddl in _SHIMS.values():
        con.execute(ddl)
    return con


# ------------------------------------------------------- dbt-spark, emulated
class _Column:
    def __init__(self, name):
        self.name = self.column = name
        self.quoted = f"`{name}`"


class _Adapter:
    def __init__(self, con):
        self.con = con

    def get_columns_in_relation(self, relation):
        return [_Column(r[0]) for r in
                self.con.execute(f"describe {relation}").fetchall()]


class _DbtSpark:
    """`dbt.<macro>` -- the internal namespace, where dbt-spark's own macros
    are reachable after a project macro overrides one."""

    def __init__(self, con, config):
        self.con, self.config = con, config

    @staticmethod
    def current_timestamp():
        return "current_timestamp"

    def spark__get_merge_sql(self, target, source, unique_key, dest_columns,
                             incremental_predicates):
        """dbt-spark 1.8.0 `spark__get_merge_sql`, for a list unique_key with
        no predicates and no merge_update_columns/merge_exclude_columns:
        update every destination column, insert *, no delete clause."""
        on = " and ".join(f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}"
                          for k in unique_key)
        cols = [c.quoted for c in _Adapter(self.con).get_columns_in_relation(target)]
        sets = ", ".join(f"{c} = DBT_INTERNAL_SOURCE.{c}" for c in cols)
        return (f"merge into {target} as DBT_INTERNAL_DEST\n"
                f"using {source} as DBT_INTERNAL_SOURCE\non {on}\n"
                f"when matched then update set {sets}\n"
                f"when not matched then insert *")


def _merge_sql(con, config, target, source):
    """What `get_merge_sql` dispatches to: the project's `spark__get_merge_sql`
    when one exists in dbt/macros, else dbt-spark's."""
    dbt = _DbtSpark(con, config)
    project = "".join(p.read_text(encoding="utf-8")
                      for p in (DBT / "macros").glob("*.sql"))
    if "macro spark__get_merge_sql" not in project:
        sql = dbt.spark__get_merge_sql(target, source, config["unique_key"], None, None)
    else:
        from tests.test_dedupe_rank import (_context, _environment,
                                            _macro_modules, call_macro)
        context = _context(incremental=True, this=target, config=config,
                           adapter=_Adapter(con), dbt=dbt)
        macro = _macro_modules(_environment(), context)["spark__get_merge_sql"]
        sql = str(call_macro(macro, target, source, config["unique_key"], None, None))
    return sql.replace("`", '"')


def _build(con, name: str, target: str, incremental: bool):
    text = (PREPARED / f"{name}.sql").read_text(encoding="utf-8").replace(
        f"source('raw', '{name}')", "source('raw', 'src')")
    config = _Config({})
    exists = con.execute(
        f"select count(*) from information_schema.tables where table_name = '{target}'"
    ).fetchone()[0]
    if not incremental or not exists:
        sql = _duck(_render(text, this=target, config=config))
        con.execute(f"create or replace table {target} as {sql}")
        return
    sql = _duck(_render(text, incremental=True, this=target, config=config,
                        adapter=_Adapter(con)))
    # dbt-spark: `create temporary view` of the model, then the merge. The view
    # is materialised here because DuckDB, unlike Spark's snapshot read, would
    # otherwise let the MERGE see its own writes through `this`.
    con.execute(f"create or replace temp table {target}__dbt_tmp as {sql}")
    con.execute(_merge_sql(con, config, target, f"{target}__dbt_tmp"))


# -------------------------------------------------------------------- data
def _deliver(con, keys, attr, constants, cob_date, version, rows, ingest_ts):
    """Append one delivery to `raw_src`. `rows` maps the first key to the
    changing attribute; any second key is a constant."""
    cols = keys + [attr] + list(constants)
    values = []
    for n, (key, value) in enumerate(rows.items(), 1):
        record = [key] + [f"{k.upper()}1" for k in keys[1:]] + [value] + list(constants.values())
        lits = ", ".join("null" if v is None else f"'{v}'" for v in record)
        values.append(f"(date '{cob_date}', {version}, {n}, {lits}, "
                      f"'landing/x_{cob_date}_v{version}.csv', 'b{version}', "
                      f"null::timestamp, timestamp '{ingest_ts}', "
                      f"'x_{cob_date}_v{version}.csv', 'sv1', 'X')")
    exists = con.execute("select count(*) from information_schema.tables "
                         "where table_name = 'raw_src'").fetchone()[0]
    if not exists:
        con.execute("create table raw_src (_cob_date date, _file_version int, "
                    "_row_number bigint, " + ", ".join(f"{c} varchar" for c in cols)
                    + ", _source_file varchar, _batch_id varchar, "
                    "_received_at timestamp, _ingest_ts timestamp, "
                    "_delivery_id varchar, _schema_version varchar, "
                    "_source_system varchar)")
    con.execute("insert into raw_src values " + ", ".join(values))


def _state(con, table, keys, attr):
    order = ", ".join(keys)
    return con.execute(
        f"select {order}, {attr}, effective_from::varchar, effective_to::varchar, "
        f"is_current from {table} order by {order}, effective_from").fetchall()


def _open_versions(con, table, keys):
    k = ", ".join(keys)
    return con.execute(f"select {k}, count(*) from {table} where is_current "
                       f"group by {k} having count(*) <> 1").fetchall()


def _overlaps(con, table, keys):
    on = " and ".join(f"a.{k} = b.{k}" for k in keys)
    return con.execute(
        f"select a.{keys[0]}, a.effective_from::varchar, b.effective_from::varchar "
        f"from {table} a join {table} b on {on} and a.effective_from < b.effective_from "
        f"and a.effective_to >= b.effective_from").fetchall()


# -------------------------------------------------------------------- tests
def _scenario(first_date: str):
    """The review's sequence. B is X from `first_date`; the 09-02 delivery
    changes it to Y; a re-delivery of 09-02 drops B; on 09-03 B is back as Y.
    C appears for the first time on 09-02 and the re-delivery drops it too, so
    its ONLY version is retracted -- nothing is left to re-derive for it at
    all, and a fix that works by rewriting what it re-derives cannot see it.

    `first_date` far from 09-02 keeps B's first version outside the lookback
    window of every later run (the replay has to start from it); near puts
    every version inside (the replay has to start from nothing)."""
    return [
        ("first build", [(first_date, 1, {"A": "a", "B": "X"}, "2026-09-01 06:00"),
                         ("2026-09-01", 1, {"A": "a", "B": "X"}, "2026-09-02 06:00")]),
        ("09-02 changes B", [("2026-09-02", 1, {"A": "a", "B": "Y", "C": "Z"},
                              "2026-09-03 06:00")]),
        ("09-02 re-delivered without B", [("2026-09-02", 2, {"A": "a"}, "2026-09-03 09:00")]),
        ("09-03 has B again", [("2026-09-03", 1, {"A": "a", "B": "Y"}, "2026-09-04 06:00")]),
    ]


def _run_scenario(first_date: str):
    failures = []
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for step, deliveries in _scenario(first_date):
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            _build(con, name, INC, incremental=True)
            _build(con, name, FULL, incremental=False)
            inc, full = _state(con, INC, keys, attr), _state(con, FULL, keys, attr)
            problems = []
            if inc != full:
                problems.append(f"incremental {inc}\n      full rebuild {full}")
            if _open_versions(con, INC, keys):
                problems.append(f"keys without exactly one open version: "
                                f"{_open_versions(con, INC, keys)}")
            if _overlaps(con, INC, keys):
                problems.append(f"overlapping versions: {_overlaps(con, INC, keys)}")
            if problems:
                failures.append(f"{name} [{first_date}] after '{step}':\n    "
                                + "\n    ".join(problems))
    return failures


def test_incremental_equals_full_rebuild_when_the_first_version_is_outside_the_window():
    failures = _run_scenario("2026-08-03")
    assert not failures, "\n".join(failures)


def test_incremental_equals_full_rebuild_when_every_version_is_inside_the_window():
    failures = _run_scenario("2026-08-31")
    assert not failures, "\n".join(failures)


def test_the_full_rebuild_itself_is_the_expected_history():
    """The reference the incremental build is compared against, pinned, so a
    change breaking both the same way cannot pass the comparison."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for _, deliveries in _scenario("2026-08-03"):
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
        _build(con, name, FULL, incremental=False)
        b = [r[len(keys):] for r in _state(con, FULL, keys, attr) if r[0] == "B"]
        assert b == [("X", "2026-08-03", "2026-09-02", False),
                     ("Y", "2026-09-03", "9999-12-31", True)], (name, b)


def test_a_redelivery_of_the_date_the_replay_starts_from_is_retracted_too():
    """The version the replay starts from is in scope as well. After step 2,
    08-03 -- B's first version, before the window -- is re-delivered without
    B. Keeping that version on the grounds that it predates the window would
    leave it overlapping the X version re-derived from 09-01; the full rebuild
    has no 08-03 version at all, and neither may the incremental one."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        steps = _scenario("2026-08-03")[:2] + [
            ("08-03 re-delivered without B",
             [("2026-08-03", 2, {"A": "a"}, "2026-09-03 09:00")])]
        for step, deliveries in steps:
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            _build(con, name, INC, incremental=True)
        _build(con, name, FULL, incremental=False)
        inc, full = _state(con, INC, keys, attr), _state(con, FULL, keys, attr)
        assert inc == full, (name, inc, full)
        assert not _open_versions(con, INC, keys) and not _overlaps(con, INC, keys)


def test_a_version_whose_cob_date_raw_no_longer_holds_is_never_retracted():
    """Absence of evidence is not a retraction.

    Retention prunes raw to month-ends, so a version's COB date can vanish
    from raw while the version is still right. The replay then cannot
    re-derive it -- and a retraction keyed on "not re-derived" alone would
    delete it and silently re-date the key to the next delivery raw still
    holds. After step 2, both of B's version dates are pruned from raw
    (08-03, where the replay starts, and 09-02, inside the window) and 09-03
    is delivered. Both versions must still be in the target.

    What the build does instead is the pre-existing failure of replaying from
    pruned raw, which is LOUD: a second open version, which the SCD2 tests
    refuse. Pinned too, so a later change cannot make it quiet by accident.
    """
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for _, deliveries in _scenario("2026-08-03")[:2]:
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            _build(con, name, INC, incremental=True)
        con.execute("delete from raw_src where _cob_date in "
                    "(date '2026-08-03', date '2026-09-02')")
        _deliver(con, keys, attr, constants, "2026-09-03", 1,
                 {"A": "a", "B": "Y"}, "2026-09-04 06:00")
        _build(con, name, INC, incremental=True)
        b = {(r[len(keys)], r[len(keys) + 1]) for r in _state(con, INC, keys, attr)
             if r[0] == "B"}
        assert {("X", "2026-08-03"), ("Y", "2026-09-02")} <= b, (name, sorted(b))
        assert _open_versions(con, INC, keys), (name, sorted(b))
