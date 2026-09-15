"""SCD2 incremental builds must agree with a full rebuild, run after run.

THE DEFECT THIS PINS. `ref_counterparty` and `ref_rating` merge on
`(key, effective_from)`, and dbt-spark's MERGE only updates and inserts. Once
`dedupe_rank` selects the newest delivery per COB date, a re-delivery that
drops a key -- or reverts its value -- RETRACTS a version an earlier
incremental run already wrote. Nothing re-derives that version's row, so the
merge never touches it: it stays current, the version before it stays closed,
and when the key next changes a second open version is inserted beside it.
`as_of()` then matches both and doubles the key's rows downstream.

The fix had two defects of its own, both found by the final review and both
pinned below: retracting the version the replay STARTS from left the one
before it closed or doubled, because that one was outside the replay; and the
replay scope compared RAW keys to the target's CLEANED keys, so a padded key
or a lower-case agency was never retracted.
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
(`sha2`, `date_sub`, `trunc`, `to_date`, `element_at`); and the MERGE's
`insert *` is run as DuckDB's `insert by name`, which is what Spark's means.
Two things DuckDB does not do are done by the harness: dbt-spark's
`process_schema_changes` (a column the view has and the target lacks is added
before the merge), and Spark's refusal of a MERGE in which one target row is
matched by several source rows (`_assert_merge_cardinality`), which DuckDB
1.5.5 silently accepts.

What this cannot prove: that Iceberg on Spark executes the same MERGE the
same way on a Nessie branch. That is a live run.
"""
from __future__ import annotations

import re

import duckdb

from tests.test_dedupe_rank import D1, D2, DBT, PREPARED, _Config, _render, _run

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


class _QueryResult:
    """A stand-in for the `agate.Table` real dbt's `run_query()` returns:
    `.rows` iterable and indexable per row, like `agate.Row`. Plain tuples
    from DuckDB already support positional indexing, so no row wrapper is
    needed beyond this."""

    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)


class _Adapter:
    def __init__(self, con):
        self.con = con

    def get_columns_in_relation(self, relation):
        return [_Column(r[0]) for r in
                self.con.execute(f"describe {relation}").fetchall()]

    def get_relation(self, database, schema, identifier):
        """`None` for a relation that does not exist -- the same signal real
        dbt gives `scd2_refuse_full_refresh_over_pruned_raw` for a first
        build. `database`/`schema` are accepted and ignored: DuckDB in this
        harness has one schema and no database level."""
        exists = self.con.execute(
            "select count(*) from information_schema.tables "
            "where table_name = ?", [identifier]).fetchone()[0]
        return identifier if exists else None

    def run_query(self, sql):
        return _QueryResult(self.con.execute(sql).fetchall())


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


class MergeCardinalityError(AssertionError):
    """What Spark/Iceberg raise and DuckDB 1.5.5 does not."""


def _assert_merge_cardinality(con, merge_sql: str):
    """SPARK'S RULE, ENFORCED HERE BECAUSE DUCKDB DOES NOT. Iceberg's MERGE
    refuses a target row matched by more than one source row; DuckDB 1.5.5
    silently applies one of them. The ON clause is read out of the rendered
    statement itself, so the check is the merge's own match."""
    m = re.search(r"merge into (\S+) as DBT_INTERNAL_DEST\s+using (\S+) as "
                  r"DBT_INTERNAL_SOURCE\s+on (.*?)\s+when ", merge_sql, re.S | re.I)
    assert m, merge_sql
    target, source, on = m.groups()
    bad = con.execute(
        f"select count(*) from (select DBT_INTERNAL_DEST.rowid from {target} as DBT_INTERNAL_DEST "
        f"join {source} as DBT_INTERNAL_SOURCE on {on} "
        f"group by DBT_INTERNAL_DEST.rowid having count(*) > 1)").fetchone()[0]
    if bad:
        raise MergeCardinalityError(
            f"{bad} target row(s) of {target} matched by more than one source row")


def _build(con, name: str, target: str, incremental: bool,
           invocation_id: str = "test-invocation", nessie_ref: str | None = None):
    text = (PREPARED / f"{name}.sql").read_text(encoding="utf-8").replace(
        f"source('raw', '{name}')", "source('raw', 'src')")
    config = _Config({})
    exists = con.execute(
        f"select count(*) from information_schema.tables where table_name = '{target}'"
    ).fetchone()[0]
    if not incremental or not exists:
        sql = _duck(_render(text, this=target, config=config,
                            invocation_id=invocation_id, nessie_ref=nessie_ref))
        con.execute(f"create or replace table {target} as {sql}")
        return
    sql = _duck(_render(text, incremental=True, this=target, config=config,
                        adapter=_Adapter(con), invocation_id=invocation_id,
                        nessie_ref=nessie_ref))
    # dbt-spark: `create temporary view` of the model, then the merge. The view
    # is materialised here because DuckDB, unlike Spark's snapshot read, would
    # otherwise let the MERGE see its own writes through `this`.
    con.execute(f"create or replace temp table {target}__dbt_tmp as {sql}")
    # ...and between the two, `process_schema_changes`: on_schema_change is
    # append_new_columns, so a column the view has and the target lacks is
    # added to the target before the merge is rendered.
    have = {r[0] for r in con.execute(f"describe {target}").fetchall()}
    for column, dtype, *_ in con.execute(f"describe {target}__dbt_tmp").fetchall():
        if column not in have:
            con.execute(f'alter table {target} add column "{column}" {dtype}')
    merge = _merge_sql(con, config, target, f"{target}__dbt_tmp")
    _assert_merge_cardinality(con, merge)
    # Spark resolves MERGE's `INSERT *` BY NAME; DuckDB's is positional, which
    # only differs once the target's column order has drifted from the view's
    # (a column dropped and re-added). DuckDB spells Spark's meaning this way.
    con.execute(re.sub(r"\binsert \*", "insert by name", merge, flags=re.I))


# -------------------------------------------------------------------- data
def _deliver(con, keys, attr, constants, cob_date, version, rows, ingest_ts,
             second_key="AGENCY1"):
    """Append one delivery to `raw_src`. `rows` maps the first key AS RAW HAS
    IT (padding included) to the changing attribute; any second key is
    `second_key`, also as raw has it."""
    cols = keys + [attr] + list(constants)
    values = []
    for n, (key, value) in enumerate(rows.items(), 1):
        record = [key] + [second_key for _ in keys[1:]] + [value] + list(constants.values())
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


def _inverted(con, table, keys):
    return con.execute(
        f"select {', '.join(keys)}, effective_from::varchar, effective_to::varchar "
        f"from {table} where effective_to < effective_from").fetchall()


def _overlaps(con, table, keys):
    # NULL-safe, like the models' own key match: a key that cleans to NULL is
    # one key, and `=` would hide its overlapping versions from this check.
    on = " and ".join(f"a.{k} is not distinct from b.{k}" for k in keys)
    return con.execute(
        f"select a.{keys[0]}, a.effective_from::varchar, b.effective_from::varchar "
        f"from {table} a join {table} b on {on} and a.effective_from < b.effective_from "
        f"and a.effective_to >= b.effective_from").fetchall()


def _markers(con, table):
    """A retraction marker (`scd2_retracted()`, DATE '0001-01-01') left behind
    in the target -- the MERGE is supposed to delete every one it emits, in
    the same statement, so none should ever be visible afterwards."""
    return con.execute(
        f"select * from {table} where effective_to = date '{DELETED}'").fetchall()


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
    return _run_steps(first_date, _scenario(first_date))


def _run_steps(label, steps, models=MODELS):
    """Build incrementally after every step, beside a full rebuild; report
    every step where they differ or the incremental table is malformed.
    A delivery is (cob_date, version, rows, ingest_ts[, second key])."""
    failures = []
    for name, keys, attr, constants in models:
        con = _connect()
        for step, deliveries in steps:
            for cob_date, version, rows, ts, *second in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts, *second)
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
            for table in (INC, FULL):
                inverted = _inverted(con, table, keys)
                if inverted:
                    problems.append(f"{table}: versions ending before they begin: {inverted}")
            if problems:
                failures.append(f"{name} [{label}] after '{step}':\n    "
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


def test_a_version_whose_cob_date_raw_no_longer_holds_is_seeded_from_the_target():
    """RENAMED from `..._is_never_retracted`: that name described the LOUD
    failure this pinned before the fix (a second open version, which the SCD2
    tests refuse) -- true, but not the point any more. `scd2_pruned_seed` now
    carries such a version forward from the target instead of leaving the
    replay unable to re-derive it, so the build is CORRECT, not merely loud.

    Retention prunes raw to month-ends, so a version's COB date can vanish
    from raw while the version is still right. After step 2, both of B's
    version dates are pruned from raw (08-03, where the replay starts, and
    09-02, inside the window) and 09-03 re-delivers B UNCHANGED (still Y).
    Both of B's versions must survive with their original values and
    boundaries, exactly one current, and the unchanged re-delivery must not
    open a third."""
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
        b = [r[len(keys):] for r in _state(con, INC, keys, attr) if r[0] == "B"]
        assert b == [("X", "2026-08-03", "2026-09-01", False),
                     ("Y", "2026-09-02", "9999-12-31", True)], (name, b)
        assert not _open_versions(con, INC, keys), (name, b)
        assert not _overlaps(con, INC, keys), (name, b)
        assert not _markers(con, INC), (name, "marker row left in target")


def test_only_the_replay_start_date_is_pruned():
    """The narrower case: 08-03 (where the replay starts) is pruned but 09-02
    (inside the window) is still in raw. B's 09-02 version must still be
    RE-DERIVED from raw as before -- an unchanged re-delivery of it must not
    open a spurious new version -- while 08-03 is seeded."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for _, deliveries in _scenario("2026-08-03")[:2]:
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            _build(con, name, INC, incremental=True)
        con.execute("delete from raw_src where _cob_date = date '2026-08-03'")
        _deliver(con, keys, attr, constants, "2026-09-03", 1,
                 {"A": "a", "B": "Y"}, "2026-09-04 06:00")
        _build(con, name, INC, incremental=True)
        b = [r[len(keys):] for r in _state(con, INC, keys, attr) if r[0] == "B"]
        assert b == [("X", "2026-08-03", "2026-09-01", False),
                     ("Y", "2026-09-02", "9999-12-31", True)], (name, b)
        assert not _open_versions(con, INC, keys), (name, b)
        assert not _overlaps(con, INC, keys), (name, b)
        assert not _markers(con, INC), (name, "marker row left in target")


def test_a_retained_redelivery_still_retracts_a_version_seeded_at_its_start():
    """The interaction the design direction calls out explicitly: the
    replay-start version is pruned (seeded, not re-derived), and a LATER,
    still-retained date is then re-delivered dropping the key entirely. That
    re-delivery must still retract the version it began (09-02, Y) and reopen
    the one before it (08-03, X) -- even though 08-03 itself is only in the
    replay because it was seeded, not because raw still holds it. C, whose
    only version was 09-02, has nothing left to seed or re-derive and is
    retracted outright."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for _, deliveries in _scenario("2026-08-03")[:2]:
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            _build(con, name, INC, incremental=True)
        con.execute("delete from raw_src where _cob_date = date '2026-08-03'")
        _deliver(con, keys, attr, constants, "2026-09-02", 2,
                 {"A": "a"}, "2026-09-03 09:00")
        _build(con, name, INC, incremental=True)
        # A's attribute value is case-folded by ref_rating's cleaning
        # (`upper(clean_string('rating'))`) but not by ref_counterparty's, so
        # only the dates and is_current -- not the value -- are compared.
        a = [r[len(keys) + 1:] for r in _state(con, INC, keys, attr) if r[0] == "A"]
        b = [r[len(keys):] for r in _state(con, INC, keys, attr) if r[0] == "B"]
        c = con.execute(f"select * from {INC} where {keys[0]} = 'C'").fetchall()
        assert a == [("2026-08-03", "9999-12-31", True)], (name, a)
        assert b == [("X", "2026-08-03", "9999-12-31", True)], (name, b)
        assert c == [], (name, c)
        assert not _open_versions(con, INC, keys), (name, b)
        assert not _overlaps(con, INC, keys), (name, b)
        assert not _markers(con, INC), (name, "marker row left in target")


def test_a_key_unchanged_for_weeks_survives_daily_pruning():
    """A `full_snapshot` feed re-delivers B unchanged every day and retention
    keeps only a short rolling window of raw -- the estate's actual shape,
    per CLAUDE.md: `full_snapshot` touches every key every day, and reference
    versions are mostly old. Once B's origin date (08-01) is pruned,
    `scd2_pruned_seed` is the only thing keeping its single open version
    alive at all; before this fix, every rebuild after that point stranded a
    second copy beside it. The target must end with exactly the one version
    it always had, audit columns aside."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for day in range(1, 21):
            cob_date = f"2026-08-{day:02d}"
            _deliver(con, keys, attr, constants, cob_date, 1,
                     {"A": "a", "B": "X"}, f"{cob_date} 06:00")
            _build(con, name, INC, incremental=True)
            # nightly retention: keep only the newest 6 COB dates in raw.
            con.execute("delete from raw_src where _cob_date < "
                        "(select max(_cob_date) from raw_src) - interval 5 day")
        b = [r[len(keys):] for r in _state(con, INC, keys, attr) if r[0] == "B"]
        assert b == [("X", "2026-08-01", "9999-12-31", True)], (name, b)
        assert not _open_versions(con, INC, keys), (name, b)
        assert not _overlaps(con, INC, keys), (name, b)
        assert not _markers(con, INC), (name, "marker row left in target")


def test_a_pruned_version_subsumed_by_a_retained_redelivery_is_retracted():
    """A pruned-date version that `scd2_pruned_seed` re-emits can still be
    made redundant by a RETAINED date's re-delivery, and that must retract
    it -- not strand it as a second current row.

    B is X from 08-03 (08-31 also delivers X, unchanged); 09-02 changes B to
    Y. Retention then prunes 08-03 and 09-02, keeping only the month-end
    08-31. 08-31 is re-delivered (v2) restating B as Y -- upstream's own
    correction, arriving at a date raw still holds -- and 09-04 repeats Y.
    The 09-02 version is now identical to what 08-31 v2 says: it must merge
    away, leaving X[08-03..08-30] then Y[08-31..] current, not a stranded
    third row at 09-02."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        for cob_date, version, rows, ts in [
                ("2026-08-03", 1, {"A": "a", "B": "X"}, "2026-08-04 06:00"),
                ("2026-08-31", 1, {"A": "a", "B": "X"}, "2026-09-01 06:00")]:
            _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
        _build(con, name, INC, incremental=True)
        _deliver(con, keys, attr, constants, "2026-09-02", 1,
                 {"A": "a", "B": "Y"}, "2026-09-03 06:00")
        _build(con, name, INC, incremental=True)
        con.execute("delete from raw_src where _cob_date in "
                    "(date '2026-08-03', date '2026-09-02')")
        _deliver(con, keys, attr, constants, "2026-08-31", 2,
                 {"A": "a", "B": "Y"}, "2026-09-05 06:00")
        _deliver(con, keys, attr, constants, "2026-09-04", 1,
                 {"A": "a", "B": "Y"}, "2026-09-05 06:05")
        _build(con, name, INC, incremental=True)
        b = [r[len(keys):] for r in _state(con, INC, keys, attr) if r[0] == "B"]
        assert b == [("X", "2026-08-03", "2026-08-30", False),
                     ("Y", "2026-08-31", "9999-12-31", True)], (name, b)
        assert not _open_versions(con, INC, keys), (name, b)
        assert not _overlaps(con, INC, keys), (name, b)
        assert not _markers(con, INC), (name, "marker row left in target")


# ------------------------------------------- the final review's two findings
# Reproduced by the reviewer's probe before the fix; every case below printed
# DIFF on 2ae6052 except the unpadded control.

def _history_before_the_replay_start(third):
    """B is V from 06-01, W from 07-01, X from 08-03, Y from 09-02 -- so when
    08-03 is re-delivered, the lookback window starts 08-30 and the replay
    starts at 08-03 (X, the version in force then). W, the version BEFORE it,
    is outside the replay, and is the one the full rebuild extends. V, before
    that, is out of every scope: its raw date is still there, so a retraction
    scope that reached past the seed would delete it."""
    return [
        ("V, W, then X", [("2026-06-01", 1, {"A": "a", "B": "V"}, "2026-06-02 06:00"),
                          ("2026-07-01", 1, {"A": "a", "B": "W"}, "2026-07-02 06:00"),
                          ("2026-08-03", 1, {"A": "a", "B": "X"}, "2026-08-04 06:00"),
                          ("2026-09-01", 1, {"A": "a", "B": "X"}, "2026-09-02 06:00")]),
        ("09-02 changes B to Y", [("2026-09-02", 1, {"A": "a", "B": "Y"}, "2026-09-03 06:00")]),
        third,
    ]


def test_dropping_the_replay_start_version_reopens_the_one_before_it():
    """Finding 1, C-drop. Before the fix the X version was retracted and W
    stayed closed at 08-02: a gap in which `as_of()` matches nothing."""
    failures = _run_steps("C-drop", _history_before_the_replay_start(
        ("08-03 re-delivered without B",
         [("2026-08-03", 2, {"A": "a"}, "2026-09-03 09:00")])))
    assert not failures, "\n".join(failures)


def test_reverting_the_replay_start_version_extends_the_one_before_it():
    """Finding 1, C-revert. Before the fix the re-delivery's W became a second
    W version back to back with the first; the full rebuild has one."""
    failures = _run_steps("C-revert", _history_before_the_replay_start(
        ("08-03 re-delivered reverting B to W",
         [("2026-08-03", 2, {"A": "a", "B": "W"}, "2026-09-03 09:00")])))
    assert not failures, "\n".join(failures)


def test_a_padded_raw_key_is_matched_to_the_cleaned_target_key():
    """Finding 2. The target holds `clean_string` keys; a replay scope built
    from raw keys never matched ' B' to 'B', so no marker was emitted, the
    retracted version stayed current, and B's next change opened a second."""
    failures = _run_steps("padded", [
        ("B clean, then padded", [("2026-08-03", 1, {"A": "a", "B": "X"}, "2026-09-01 06:00"),
                                  ("2026-09-01", 1, {"A": "a", " B": "X"}, "2026-09-02 06:00")]),
        ("09-02 changes B", [("2026-09-02", 1, {"A": "a", " B": "Y"}, "2026-09-03 06:00")]),
        ("09-02 re-delivered without B", [("2026-09-02", 2, {"A": "a"}, "2026-09-03 09:00")]),
        ("09-03", [("2026-09-03", 1, {"A": "a", " B": "X"}, "2026-09-04 06:00")]),
    ])
    assert not failures, "\n".join(failures)


def _padded_consistently(pad):
    return [
        ("first build", [("2026-08-03", 1, {"A": "a", pad + "B": "X"}, "2026-09-01 06:00"),
                         ("2026-09-01", 1, {"A": "a", pad + "B": "X"}, "2026-09-02 06:00")]),
        ("09-02 changes B", [("2026-09-02", 1, {"A": "a", pad + "B": "Y"}, "2026-09-03 06:00")]),
        ("09-02 re-delivered without B", [("2026-09-02", 2, {"A": "a"}, "2026-09-03 09:00")]),
        ("09-03", [("2026-09-03", 1, {"A": "a", pad + "B": "Y"}, "2026-09-04 06:00")]),
    ]


def test_a_key_padded_in_every_delivery_is_retracted_like_a_clean_one():
    """Finding 2, the review's padded-consistent case, beside its control."""
    failures = (_run_steps("padded-consistent", _padded_consistently(" "))
                + _run_steps("unpadded-control", _padded_consistently("")))
    assert not failures, "\n".join(failures)


def test_an_agency_in_another_case_is_matched_to_the_cleaned_target_agency():
    """Finding 2 on `ref_rating`'s second key, which the model cleans with
    `upper(clean_string(...))`: raw sends ' agency1' where the target holds
    'AGENCY1'."""
    rating = [m for m in MODELS if m[0] == "ref_rating"]
    failures = _run_steps("agency case", [
        ("first build", [("2026-08-03", 1, {"A": "a", "B": "X"}, "2026-09-01 06:00", "AGENCY1"),
                         ("2026-09-01", 1, {"A": "a", "B": "X"}, "2026-09-02 06:00", " agency1")]),
        ("09-02 changes B", [("2026-09-02", 1, {"A": "a", "B": "Y"}, "2026-09-03 06:00", " agency1")]),
        ("09-02 re-delivered without B", [("2026-09-02", 2, {"A": "a"}, "2026-09-03 09:00", " agency1")]),
        ("09-03", [("2026-09-03", 1, {"A": "a", "B": "X"}, "2026-09-04 06:00", "agency1")]),
    ], models=rating)
    assert not failures, "\n".join(failures)


# ------------------------------------- the rank on the SCD2 replay, in place
def _replay_case(con, name, keys, attr, constants):
    """On D1, v1 carries A and B and v2 carries only A. `this` holds A from D2
    and again from 09-10 (current), and B from 06-01, so the lookback window
    starts 09-07 and each key replays from its last version before that: A
    from D2, B from 06-01. D1's replayed rows must therefore be nothing at
    all -- A's are before its replay start and B's are the delivery v2
    replaced. 09-08 puts both keys inside the window, so both are touched."""
    for cob_date, version, rows, ts in [
            (D1, 1, {"A": "a", "B": "b"}, "2026-09-02 06:00"),
            (D1, 2, {"A": "a"}, "2026-09-03 06:00"),
            (D2, 1, {"A": "a", "B": "b"}, "2026-09-03 06:00"),
            ("2026-09-08", 1, {"A": "a", "B": "b"}, "2026-09-09 06:00")]:
        _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
    second = ", 'AGENCY1' as agency" if len(keys) > 1 else ""
    con.execute(f"""
        create table this_table as
        select effective_from::date as effective_from, is_current,
               k as counterparty_id {second}
        from (values ('{D2}', 'A', false), ('2026-09-10', 'A', true),
                     ('2026-06-01', 'B', true)) t(effective_from, k, is_current)
    """)
    return (PREPARED / f"{name}.sql").read_text(encoding="utf-8").replace(
        f"source('raw', '{name}')", "source('raw', 'src')")


REPLAYED = "select cob_date::varchar, counterparty_id, source_file_version from replayed order by 1, 2"


def test_scd2_incremental_replay_drops_a_key_the_newest_delivery_omitted():
    """`this_table`'s two anchor dates (A's current version from 09-10, B's
    only version from 06-01) have no matching delivery anywhere in this
    fixture's `raw_src` -- indistinguishable, to `scd2_pruned_seed`, from a
    date retention has since pruned. Both are therefore seeded from
    `this_table` rather than left unrepresented: `source_file_version` reads
    NULL because `this_table`'s minimal schema (effective_from, is_current,
    the key) has no such column, exactly as `in_target` intends for a column
    the seed source lacks."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        text = _replay_case(con, name, keys, attr, constants)
        sql = _duck(_render(text, incremental=True, adapter=_Adapter(con)))
        got = _run(con, sql, "replayed", REPLAYED)
        assert (D1, "B", 1) not in got, (name, got)
        assert got == [("2026-06-01", "B", None),
                       (D2, "A", 1), (D2, "B", 1),
                       ("2026-09-08", "A", 1), ("2026-09-08", "B", 1),
                       ("2026-09-10", "A", None)], (name, got)


def test_scd2_full_refresh_replay_agrees():
    for name, keys, attr, constants in MODELS:
        con = _connect()
        text = _replay_case(con, name, keys, attr, constants)
        got = _run(con, _duck(_render(text)), "replayed", REPLAYED)
        assert got == [(D1, "A", 2), (D2, "A", 1), (D2, "B", 1),
                       ("2026-09-08", "A", 1), ("2026-09-08", "B", 1)], (name, got)


def test_scd2_as_of_decides_newest_among_what_was_known():
    """`newest_file_version()` must filter on `known_as_of()` too. Without it
    the aggregate names v2 as newest for D1 while the model's own WHERE has
    already removed v2's rows -- and D1 loses A and B both."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        text = _replay_case(con, name, keys, attr, constants)
        got = _run(con, _duck(_render(text, knowledge_time="2026-09-02 12:00")),
                   "replayed", REPLAYED)
        assert got == [(D1, "A", 1), (D1, "B", 1)], (name, got)


# ------------------------------------------ the second review's five issues
def test_a_key_that_cleans_to_null_is_kept_like_the_full_rebuild():
    """Issue 1. `clean_string` turns '', 'NULL' and 'N/A' into NULL. A replay
    scope joined on `=` matched nothing for a NULL key, so the row vanished on
    every incremental run while a full rebuild kept it -- and a `not_null`
    test on the key then passed on incremental builds only. With the
    null-safe comparison the NULL-key version is replayed like any other, and
    the MERGE matches it instead of inserting a second copy each run."""
    failures = []
    for spelling in ("N/A", "", "NULL"):
        failures += _run_steps(f"null key {spelling!r}", _scenario("2026-08-03")[:2] + [
            ("09-03 adds an unkeyed row",
             [("2026-09-03", 1, {"A": "a", "B": "Y", spelling: "Q"}, "2026-09-04 06:00")]),
            ("09-04 repeats it",
             [("2026-09-04", 1, {"A": "a", "B": "Y", spelling: "Q"}, "2026-09-05 06:00")]),
            ("09-04 re-delivered as R",
             [("2026-09-04", 2, {"A": "a", "B": "Y", spelling: "R"}, "2026-09-05 09:00")]),
        ])
    assert not failures, "\n".join(failures)


def test_two_raw_spellings_of_one_key_in_one_file_keep_the_last():
    """Issue 2. The rank partitioned by the RAW key, so ' B' and 'B' in one
    file were both "last in file" for the same cleaned key: two versions with
    one effective_from, one ending before it began -- and on Spark a MERGE
    cardinality violation. The rank is on the cleaned key; the later row wins."""
    failures = _run_steps("two spellings", [
        ("first build", [("2026-08-03", 1, {"A": "a", "B": "X"}, "2026-09-01 06:00")]),
        ("09-02 has B twice", [("2026-09-02", 1, {"A": "a", "B": "Y", " B": "Z"},
                                "2026-09-03 06:00")]),
        ("09-03 has B twice the other way round",
         [("2026-09-03", 1, {"A": "a", " B": "Z", "B": "W"}, "2026-09-04 06:00")]),
    ])
    assert not failures, "\n".join(failures)
    for name, keys, attr, constants in MODELS:
        con = _connect()
        _deliver(con, keys, attr, constants, "2026-09-02", 1,
                 {"A": "a", "B": "Y", " B": "Z"}, "2026-09-03 06:00")
        _build(con, name, FULL, incremental=False)
        b = [r[len(keys):] for r in _state(con, FULL, keys, attr) if r[0] == "B"]
        assert b == [("Z", "2026-09-02", "9999-12-31", True)], (name, b)


def test_an_agency_spelled_twice_in_one_file_keeps_the_last():
    """Issue 2 on `ref_rating`'s second key: 'moodys' and 'MOODYS' in one
    file are one cleaned agency."""
    name, keys, attr, constants = [m for m in MODELS if m[0] == "ref_rating"][0]
    con = _connect()
    _deliver(con, keys, attr, constants, "2026-09-02", 1, {"B": "A"}, "2026-09-03 06:00", "moodys")
    con.execute("update raw_src set _row_number = 1")
    _deliver(con, keys, attr, constants, "2026-09-02", 1, {"B": "BBB"}, "2026-09-03 06:00", "MOODYS")
    con.execute("update raw_src set _row_number = 2 where agency = 'MOODYS'")
    _build(con, name, FULL, incremental=False)
    assert _state(con, FULL, keys, attr) == [
        ("B", "MOODYS", "BBB", "2026-09-02", "9999-12-31", True)], _state(con, FULL, keys, attr)


def test_a_reopened_seed_row_carries_this_runs_audit_columns():
    """Issue 3. The seed row is copied from the target and then CHANGED by the
    merge -- its effective_to reopens -- so it must say which run and branch
    changed it, as every re-derived row does, not the run that wrote it
    first. Its delivery provenance (source_file, source_batch_id) is the
    target's, because the delivery that began the version has not changed."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        steps = _history_before_the_replay_start(
            ("08-03 re-delivered without B",
             [("2026-08-03", 2, {"A": "a"}, "2026-09-03 09:00")]))
        for n, (_, deliveries) in enumerate(steps, 1):
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            _build(con, name, INC, incremental=True,
                   invocation_id=f"run{n}", nessie_ref=f"build/run{n}")
        w = con.execute(
            f"select effective_to::varchar, dbt_invocation_id, nessie_ref, source_file "
            f"from {INC} where counterparty_id = 'B' and effective_from = date '2026-07-01'"
        ).fetchall()
        assert w == [("2026-08-31", "run3", "build/run3",
                      "landing/x_2026-07-01_v1.csv")], (name, w)


def test_the_harness_refuses_a_merge_spark_would_refuse():
    """Issue 4. DuckDB 1.5.5's MERGE applies one of several source rows that
    match a target row; Spark/Iceberg raise. The harness must raise too, or
    the SCD2 tests could pass on a merge that fails on the cluster."""
    con = duckdb.connect()
    con.execute("create table t as select 'B' as k, date '2026-09-02' as effective_from")
    con.execute("create table s as select * from (values ('B', date '2026-09-02'), "
                "('B', date '2026-09-02')) x(k, effective_from)")
    merge = ("merge into t as DBT_INTERNAL_DEST\n using s as DBT_INTERNAL_SOURCE\n"
             " on DBT_INTERNAL_SOURCE.k = DBT_INTERNAL_DEST.k\n"
             " when matched then update set k = DBT_INTERNAL_SOURCE.k")
    try:
        _assert_merge_cardinality(con, merge)
    except MergeCardinalityError:
        pass
    else:
        raise AssertionError("two source rows matched one target row and nothing refused it")


def test_the_seed_reads_null_for_a_column_the_target_does_not_have_yet():
    """Issue 4. A column the model has just gained is not in the target when
    the view is analysed -- dbt-spark adds it afterwards -- so the seed must
    read NULL for it, not the column. Simulated by dropping `sector` (or
    `outlook`) from the target before the run that seeds and reopens W."""
    for name, keys, attr, constants in MODELS:
        dropped = "sector" if name == "ref_counterparty" else "outlook"
        con = _connect()
        steps = _history_before_the_replay_start(
            ("08-03 re-delivered without B",
             [("2026-08-03", 2, {"A": "a"}, "2026-09-03 09:00")]))
        for n, (_, deliveries) in enumerate(steps):
            for cob_date, version, rows, ts in deliveries:
                _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
            if n == len(steps) - 1:
                con.execute(f"alter table {INC} drop column {dropped}")
            _build(con, name, INC, incremental=True)
        _build(con, name, FULL, incremental=False)
        assert _state(con, INC, keys, attr) == _state(con, FULL, keys, attr), name
        w = con.execute(f"select {dropped} from {INC} where counterparty_id = 'B' "
                        f"and effective_from = date '2026-07-01'").fetchall()
        assert w == [(None,)], (name, w)


# ------------------------------------- --full-refresh over pruned raw (REQ)
# scd2_refuse_full_refresh_over_pruned_raw, called from scd2_replay's
# non-incremental branch. Renders the real model file directly (not through
# _build) so the guard's RuntimeError -- raised at RENDER time, before any
# SQL runs -- can be asserted on without needing the rendered SQL to be
# valid or executed.

def _model_text(name):
    return (PREPARED / f"{name}.sql").read_text(encoding="utf-8").replace(
        f"source('raw', '{name}')", "source('raw', 'src')")


def _target_with_history(con, name, keys, attr, constants):
    """B is X from 08-03, Y from 09-02 -- built the ordinary incremental way,
    into INC, which is the "target table name that exists" the guard needs
    to have something to check."""
    for cob_date, version, rows, ts in [
            ("2026-08-03", 1, {"A": "a", "B": "X"}, "2026-08-04 06:00"),
            ("2026-09-02", 1, {"A": "a", "B": "Y"}, "2026-09-03 06:00")]:
        _deliver(con, keys, attr, constants, cob_date, version, rows, ts)
        _build(con, name, INC, incremental=True)


def test_full_refresh_refuses_when_raw_no_longer_explains_the_target():
    """(a) An existing target plus a pruned origin date refuses, naming the
    override var."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        _target_with_history(con, name, keys, attr, constants)
        con.execute("delete from raw_src where _cob_date = date '2026-08-03'")
        try:
            _render(_model_text(name), this=INC, adapter=_Adapter(con))
        except RuntimeError as e:
            assert "scd2_rebuild_from_pruned_raw" in str(e), (name, e)
            assert "2026-08-03" in str(e), (name, e)
        else:
            raise AssertionError(
                f"{name}: --full-refresh over pruned raw did not refuse")


def test_full_refresh_proceeds_when_the_override_var_is_set():
    """(b) The same pruned scenario, with `scd2_rebuild_from_pruned_raw: true`
    -- must not raise."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        _target_with_history(con, name, keys, attr, constants)
        con.execute("delete from raw_src where _cob_date = date '2026-08-03'")
        _render(_model_text(name), this=INC, adapter=_Adapter(con),
                scd2_rebuild_from_pruned_raw=True)


def test_full_refresh_proceeds_when_raw_holds_every_version_date():
    """(c) An existing target with raw still holding every version's origin
    date -- nothing pruned, must not raise."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        _target_with_history(con, name, keys, attr, constants)
        _render(_model_text(name), this=INC, adapter=_Adapter(con))


def test_full_refresh_proceeds_with_no_target_relation():
    """(d) No target relation at all (a first build) -- nothing to lose,
    must not raise, however pruned raw already is."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        _deliver(con, keys, attr, constants, "2026-09-02", 1,
                 {"A": "a", "B": "Y"}, "2026-09-03 06:00")
        _render(_model_text(name), this="not_built_yet", adapter=_Adapter(con))


def test_full_refresh_refuses_for_a_knowledge_time_as_of_build_too():
    """An as-of build is always --full-refresh on a branch from main, where
    the target exists -- and is exactly as re-dated by pruned raw as an
    ordinary rebuild, so it gets no exemption."""
    for name, keys, attr, constants in MODELS:
        con = _connect()
        _target_with_history(con, name, keys, attr, constants)
        con.execute("delete from raw_src where _cob_date = date '2026-08-03'")
        try:
            _render(_model_text(name), this=INC, adapter=_Adapter(con),
                    knowledge_time="2026-09-10")
        except RuntimeError as e:
            assert "scd2_rebuild_from_pruned_raw" in str(e), (name, e)
        else:
            raise AssertionError(
                f"{name}: --full-refresh as-of build over pruned raw did not refuse")
