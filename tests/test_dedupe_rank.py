"""`full_snapshot`: the newest delivery for a COB date restates ALL of it.

A key the newest delivery omits must be ABSENT. `dedupe_rank` used to
partition by `(_cob_date, <business keys>)`, which picks the newest version
of each KEY instead of the newest FILE, and a dropped key kept its row from
the delivery that replaced it -- for as long as the platform existed, with the
uniqueness test green, because uniqueness holds either way.
See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date

THE SQL UNDER TEST IS THE REAL SQL, RENDERED, NOT A HAND COPY. A copy of the
rank in a test agrees with itself forever. So the macros in
`dbt/macros/engine.sql` and the model files themselves are rendered with
jinja2 -- the template engine dbt is built on -- and the CTEs up to `deduped`
are cut out of the rendered model and run against a real DuckDB. Revert the
macro to the per-key partition and these fail.

What is emulated, and it is deliberately little:

  * dbt's context: `source()`/`ref()` name a DuckDB table, `config()` records
    its arguments and renders nothing, `is_incremental()`/`this`/`var()` are
    set per test, and `return()` hands a macro's value back to its caller the
    way dbt's `MacroGenerator` does (`_DbtContext`).
  * Spark's identifier quote. `ident()` emits backticks and DuckDB reads
    double quotes; the rendered SQL has its backticks swapped, and nothing
    else is rewritten.

What this CANNOT prove is the materialisation: that `insert_overwrite` on
Iceberg through dbt-spark replaces exactly the COB dates the select returns.
That is dbt-spark's SQL on Spark's engine, and it was verified on a Nessie
branch instead. What is pinned here is its precondition -- each date the
incremental select returns, it returns WHOLE -- and the config that selects
the strategy.
"""
from __future__ import annotations

import pathlib
import re

import duckdb
import jinja2

from tests.support import REPO

DBT = REPO / "dbt"
ENGINE = DBT / "macros" / "engine.sql"
PREPARED = DBT / "models" / "prepared"
REPORTING = DBT / "models" / "reporting"

D1, D2, D0 = "2026-09-01", "2026-09-02", "2026-08-20"


# ------------------------------------------------------------- rendering
class _Exceptions:
    @staticmethod
    def raise_compiler_error(msg):
        raise RuntimeError(msg)


class _Modules:
    re = re


class _Dbt:
    @staticmethod
    def current_timestamp():
        return "current_timestamp"


class _Return(Exception):
    """dbt's `return()`: a macro hands back a value instead of its text."""

    def __init__(self, value):
        super().__init__()
        self.value = value


def _return(value):
    raise _Return(value)


class _DbtContext(jinja2.runtime.Context):
    """Every macro call goes through `Context.call`, so this is where a
    `return()` raised inside one becomes that call's value -- what dbt's
    `MacroGenerator` does."""

    def call(__self, __obj, *args, **kwargs):  # noqa: N805 -- jinja2's signature
        if __obj is _return:
            # `{{ return(x) }}` is itself a call through here; catching it at
            # this level would render x as the macro's text instead.
            raise _Return(args[0])
        try:
            return super().call(__obj, *args, **kwargs)
        except _Return as returned:
            return returned.value


def _environment():
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    env.context_class = _DbtContext
    return env


def call_macro(macro, *args, **kwargs):
    """Call a macro from Python the way dbt does, honouring `return()`."""
    try:
        return macro(*args, **kwargs)
    except _Return as returned:
        return returned.value


def _macro_modules(env, context):
    """Every project macro file the models call into, as jinja2 modules.

    `engine.sql` first; any other file (`merge.sql`) is rendered with the
    engine's macros already in its globals, the way dbt puts every project
    macro in one namespace.
    """
    macros: dict = {}
    for path in [ENGINE] + sorted(p for p in (DBT / "macros").glob("*.sql")
                                  if p.name not in ("engine.sql", "naming.sql")):
        module = env.from_string(path.read_text(encoding="utf-8"),
                                 globals=dict(context, **macros)).module
        macros.update({name: getattr(module, name)
                       for name in dir(module) if not name.startswith("_")})
    return macros


def _context(*, incremental=False, this="this_table", knowledge_time=None,
             config=None, adapter=None, dbt=None,
             invocation_id="test-invocation", nessie_ref=None):
    variables = {"knowledge_time": knowledge_time, "lookback_days": 3,
                 "nessie_ref": nessie_ref}
    return dict(
        var=lambda name, default=None: (variables[name] if name in variables
                                        and variables[name] is not None
                                        else default),
        is_incremental=lambda: incremental,
        this=this,
        exceptions=_Exceptions(),
        modules=_Modules(),
        dbt=dbt or _Dbt(),
        invocation_id=invocation_id,
        **{"return": _return},
        config=config if config is not None else _Config({}),
        adapter=adapter,
    )


class _Config(dict):
    """`config` as a model sees it: callable in the model, `.get` in a macro."""

    def __call__(self, **kwargs):
        self.update(kwargs)
        return ""


def _render(template_text: str, *, incremental: bool = False,
            this: str = "this_table", knowledge_time: str | None = None,
            config: "_Config | None" = None, adapter=None,
            invocation_id: str = "test-invocation",
            nessie_ref: str | None = None) -> str:
    """Render a model (or any text using the project macros) as dbt would."""
    config = config if config is not None else _Config({})
    context = _context(incremental=incremental, this=this,
                       knowledge_time=knowledge_time, config=config,
                       adapter=adapter, invocation_id=invocation_id,
                       nessie_ref=nessie_ref)
    env = _environment()
    model_globals = dict(context, **_macro_modules(env, context))
    model_globals.update(
        source=lambda schema, table: f"{schema}_{table}",
        ref=lambda name: f"prepared_{name}",
        config=config,
    )
    sql = env.from_string(template_text, globals=model_globals).render()
    return sql.replace("`", '"')


def _ctes(sql: str) -> dict[str, str]:
    """The top-level CTEs of a rendered model, in order, name -> body.

    A paren scanner rather than a regex because the bodies nest. Quoted
    strings and `--` comments are skipped so a parenthesis in either cannot
    unbalance it.
    """
    sql = re.sub(r"--[^\n]*", "", sql)
    head = re.search(r"\bwith\b", sql, re.I)
    assert head, "rendered model has no WITH"
    out: dict[str, str] = {}
    pos = head.end()
    name_re = re.compile(r"\s*,?\s*(\w+)\s+as\s*\(", re.I)
    while True:
        m = name_re.match(sql, pos)
        if not m:
            break
        depth, i, quote = 1, m.end(), None
        while depth:
            ch = sql[i]
            if quote:
                if ch == quote:
                    quote = None
            elif ch in "'\"":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        out[m.group(1)] = sql[m.end():i - 1]
        pos = i
    return out


def _run(con, sql: str, upto: str, select: str):
    """Every CTE of `sql` up to and including `upto`, then `select`."""
    ctes = _ctes(sql)
    assert upto in ctes, f"no CTE {upto!r} in {list(ctes)}"
    names = list(ctes)[:list(ctes).index(upto) + 1]
    body = ",\n".join(f"{n} as ({ctes[n]})" for n in names)
    return con.execute(f"with {body}\n{select}").fetchall()


def _model(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------- the data
def _raw_trades(con, table="raw_fo_trade", key="trade_id"):
    """One COB date, two deliveries; the second DROPS T2 and repeats T1.

    v1 arrives on 09-02 06:00 and v2 on 09-03 06:00. v1 predates provenance,
    so its `_received_at` is NULL and `known_as_of()` must fall back to
    `_ingest_ts`. D2 has a single delivery and must be untouched by D1's
    re-delivery -- the rank is per COB date, not across the table.
    """
    con.execute(f"""
        create or replace table {table} as
        select _cob_date::date as _cob_date, _file_version::int as _file_version,
               _row_number::bigint as _row_number, k as {key}, mtm,
               _received_at::timestamp as _received_at,
               _ingest_ts::timestamp as _ingest_ts
        from (values
          ('{D1}', 1, 1, 'T1', '10', null,               '2026-09-02 06:05'),
          ('{D1}', 1, 2, 'T2', '20', null,               '2026-09-02 06:05'),
          ('{D1}', 1, 3, 'T1', '11', null,               '2026-09-02 06:05'),
          ('{D1}', 2, 1, 'T1', '12', '2026-09-03 06:00', '2026-09-03 06:05'),
          ('{D1}', 2, 2, 'T1', '13', '2026-09-03 06:00', '2026-09-03 06:05'),
          ('{D2}', 1, 1, 'T3', '30', '2026-09-03 06:00', '2026-09-03 06:05')
        ) t(_cob_date, _file_version, _row_number, k, mtm, _received_at, _ingest_ts)
    """)


DEDUPED = "select _cob_date::varchar, {key}, _file_version, mtm from deduped order by 1, 2"


def _prepared_date_models():
    """The two date-partitioned prepared models, each with its own key."""
    return [(PREPARED / "fo_trade.sql", "raw_fo_trade", "trade_id"),
            (PREPARED / "ref_collateral.sql", "raw_ref_collateral", "collateral_id")]


# ------------------------------------------- (a) the item's case, real rank
def test_a_key_the_newest_delivery_omits_is_absent():
    for path, table, key in _prepared_date_models():
        con = duckdb.connect()
        _raw_trades(con, table, key)
        got = _run(con, _render(_model(path)), "deduped", DEDUPED.format(key=key))
        assert got == [(D1, "T1", 2, "13"),     # v2, last occurrence in file
                       (D2, "T3", 1, "30")], (path.name, got)


def test_the_newest_delivery_is_per_cob_date_not_per_table():
    """A rank that took the newest version across the whole table would wipe
    every date that was not re-delivered."""
    con = duckdb.connect()
    _raw_trades(con)
    got = _run(con, _render(_model(PREPARED / "fo_trade.sql")), "deduped",
               "select distinct _cob_date::varchar from deduped order by 1")
    assert got == [(D1,), (D2,)], got


def test_the_macro_alone_says_the_same_thing():
    """The rank as `dedupe_rank` renders it, outside any model -- so a model
    that stopped calling it is told apart from a macro that regressed."""
    con = duckdb.connect()
    _raw_trades(con)
    rank = _render("{{ dedupe_rank(['trade_id']) }}")
    got = con.execute(f"""
        select trade_id, _file_version, mtm from (
          select *, {rank} as _rn from raw_fo_trade where _cob_date = '{D1}')
        where _rn = 1""").fetchall()
    assert got == [("T1", 2, "13")], got


def test_the_incremental_select_returns_each_date_whole():
    """THE PRECONDITION `insert_overwrite` RESTS ON. dbt-spark replaces the
    cob_date partitions the incremental select returns; a date returned
    partially would be truncated to the part, and a date outside the lookback
    must not be returned at all or it is rewritten from nothing.

    `this` holds a max cob_date of D2, so the window is D2 - 3 days: D1 and D2
    are in it, D0 is not.
    """
    for path, table, key in _prepared_date_models():
        con = duckdb.connect()
        _raw_trades(con, table, key)
        con.execute(f"""insert into {table} values
            ('{D0}', 1, 1, 'T0', '1', null, '2026-08-21 06:05')""")
        con.execute(f"create table this_table as select date '{D2}' as cob_date")
        sql = _render(_model(path), incremental=True)
        got = _run(con, sql, "deduped", DEDUPED.format(key=key))
        assert got == [(D1, "T1", 2, "13"), (D2, "T3", 1, "30")], (path.name, got)


# ------------------------------------------------------------- (b) as-of
def test_as_of_before_the_redelivery_keeps_the_first_deliverys_population():
    """Computed AFTER `known_as_of()`: at 09-02 12:00 only v1 existed, so v1
    is the newest delivery and T2 is in it. Ranking before the filter would
    pick v2 as newest, filter it out, and return nothing for D1."""
    for path, table, key in _prepared_date_models():
        con = duckdb.connect()
        _raw_trades(con, table, key)
        sql = _render(_model(path), knowledge_time="2026-09-02 12:00")
        got = _run(con, sql, "deduped", DEDUPED.format(key=key))
        assert got == [(D1, "T1", 1, "11"),     # v1, last occurrence in file
                       (D1, "T2", 1, "20")], (path.name, got)


def test_as_of_after_the_redelivery_matches_the_ordinary_build():
    con = duckdb.connect()
    _raw_trades(con)
    path = PREPARED / "fo_trade.sql"
    as_of = _run(con, _render(_model(path), knowledge_time="2026-09-04"),
                 "deduped", DEDUPED.format(key="trade_id"))
    now = _run(con, _render(_model(path)), "deduped", DEDUPED.format(key="trade_id"))
    assert as_of == now


# ------------------------------- counterparty_exposure's own read of raw
def test_delivered_means_in_the_newest_delivery_for_the_date():
    """`counterparty_exposure.delivered` reads raw directly and used to group
    every version together, so a counterparty a re-delivery dropped still
    counted as delivered and `reference_carried_forward` never fired."""
    con = duckdb.connect()
    con.execute(f"""
        create table raw_ref_counterparty as
        select _cob_date::date as _cob_date, _file_version::int as _file_version,
               _row_number::bigint as _row_number, counterparty_id,
               _received_at::timestamp as _received_at,
               _ingest_ts::timestamp as _ingest_ts
        from (values
          ('{D1}', 1, 1, 'CP1', null, '2026-09-02 06:00'),
          ('{D1}', 1, 2, 'CP2', null, '2026-09-02 06:00'),
          ('{D1}', 2, 1, 'CP1', null, '2026-09-03 06:00')
        ) t(_cob_date, _file_version, _row_number, counterparty_id, _received_at, _ingest_ts)
    """)
    text = _model(REPORTING / "counterparty_exposure.sql")
    select = "select cob_date::varchar, counterparty_id from delivered order by 1, 2"

    ctes = _ctes(_render(text))
    got = con.execute(f"with delivered as ({ctes['delivered']}) {select}").fetchall()
    assert got == [(D1, "CP1")], got

    ctes = _ctes(_render(text, knowledge_time="2026-09-02 12:00"))
    got = con.execute(f"with delivered as ({ctes['delivered']}) {select}").fetchall()
    assert got == [(D1, "CP1"), (D1, "CP2")], got


# ------------------------------------------------------------ (d) scaffold
def _scaffolded(name: str, key: str) -> str:
    from reporting_platform.ui.registry import FeedSpec
    from reporting_platform.ui.scaffold import render_model

    spec = FeedSpec(name=name, description="Test feed.", source_system="t",
                    filename_pattern=r"X_(?P<cob_date>\d{8})\.csv",
                    business_key=[key], columns=[key, "mtm"])
    return render_model(spec, {key: "string", "mtm": "string"})


def test_the_scaffold_emits_the_strategy_and_no_unique_key():
    text = _scaffolded("t_new", "trade_id")
    config = text[:text.index("}}")]
    assert "incremental_strategy='insert_overwrite'" in config, config
    assert "partition_by=['cob_date']" in config, config
    assert "unique_key" not in config, config


def test_a_scaffolded_model_drops_a_key_the_newest_delivery_omits():
    """The template, rendered and run -- a new feed must not be the one model
    with the old rank."""
    con = duckdb.connect()
    _raw_trades(con, "raw_t_new", "trade_id")
    got = _run(con, _render(_scaffolded("t_new", "trade_id")), "deduped",
               DEDUPED.format(key="trade_id"))
    assert got == [(D1, "T1", 2, "13"), (D2, "T3", 1, "30")], got


# ------------------------------------------------- the strategy, per model
def _config(path: pathlib.Path) -> str:
    text = _model(path)
    return text[:text.index("}}")]


def test_every_date_partitioned_model_overwrites_and_every_scd2_model_merges():
    """A cob_date-partitioned model on MERGE keeps every row a re-delivery
    dropped; an SCD2 model on insert_overwrite would truncate each
    effective_from_month it returns to the keys this run touched. So the
    partition decides the strategy, and each model says it."""
    models = sorted(PREPARED.glob("*.sql")) + sorted(REPORTING.glob("*.sql"))
    assert len(models) >= 7, models
    for path in models:
        config = _config(path)
        if "partition_by=['cob_date']" in config:
            assert "incremental_strategy='insert_overwrite'" in config, path.name
            assert "unique_key" not in config, path.name
        else:
            assert "partition_by=['effective_from_month']" in config, path.name
            assert "incremental_strategy='merge'" in config, path.name
            assert "unique_key" in config, path.name


def test_the_project_default_is_insert_overwrite():
    """For a hand-written model that states nothing -- the default must be the
    strategy that fits the default cob_date partition."""
    import yaml

    project = yaml.safe_load((DBT / "dbt_project.yml").read_text(encoding="utf-8"))
    models = project["models"]["reporting_platform"]
    assert models["+incremental_strategy"] == "insert_overwrite"
    for layer in ("prepared", "reporting"):
        assert models[layer]["+partition_by"] == ["cob_date"]


def test_every_scd2_model_decides_newest_from_the_unjoined_aggregate():
    """The SCD2 models rank every raw row now -- the replay scope is applied
    after cleaning (`scd2_replay`) -- so a window would see whole dates too.
    They still decide "newest" from `newest_file_version()`, because the
    retraction guard reads that CTE and a key-scoped filter reintroduced
    before the rank must not quietly make the default window wrong again.
    Their replay is tested end to end in `test_scd2_incremental.py`."""
    scd2 = [p for p in sorted(PREPARED.glob("*.sql")) if "scd2_replay(" in _model(p)]
    assert len(scd2) == 2, scd2
    for path in scd2:
        text = _model(path)
        assert "newest_file_version(" in text, path.name
        assert "newest_version='_newest_file_version'" in text, path.name
