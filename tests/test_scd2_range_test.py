"""`dbt_utils.mutually_exclusive_ranges` must be bound on the EXCLUSIVE end.

`effective_to` (`scd2_effective_to` in `dbt/macros/engine.sql`) is an
INCLUSIVE upper bound: the next version's `effective_from` minus one day. The
dbt_utils test's own arithmetic assumes an EXCLUSIVE one --
`coalesce(lower_bound < upper_bound, false)` (strict, `zero_length_range_allowed`
defaults false) and, per row, `coalesce(upper_bound {op} next_lower_bound,
is_last_record, false)` where `op` is `=` for `gaps: not_allowed` and `<=` for
`gaps: allowed` -- so bound directly on `effective_to` the test either refuses
a correct one-day version (`gaps: not_allowed`) or passes a same-day overlap
(`gaps: allowed`, since inclusive `05 <= 05` holds for [09-01,09-05] then
[09-05,09-10]). See the comment above the test in
`dbt/models/prepared/_prepared.yml` for the full reasoning.

The fix: `upper_bound_column: date_add(effective_to, 1)` (the EXCLUSIVE end)
with `gaps: not_allowed`. Two things this test pins:

  1. STATIC: every `dbt_utils.mutually_exclusive_ranges` on an SCD2 model in
     `dbt/models/prepared/_prepared.yml` is configured that way -- reads the
     YAML, does not hand-copy it.
  2. DYNAMIC: the macro's own arithmetic, described above, evaluated in
     DuckDB with the upper-bound expression and the gaps operator READ FROM
     THAT SAME YAML (never hard-coded), over four cases: a one-day version
     (must pass), a same-day overlap (must fail), a gap (must fail), and an
     open version ending `9999-12-31` (must pass, and must not need
     collecting into a Python datetime, which is where a year-10000 date
     breaks -- this test only COUNTS rows, exactly like dbt does).

NO SPARK. `date_add(effective_to, 1)` is Spark spelling (`dbt/macros/engine.sql`
uses `date_sub`, never `- INTERVAL 1 DAY`, for the same reason: the interval
form returns a TIMESTAMP in Spark). DuckDB 1.5.5 accepts `date_add(<date>,
<int>)` directly and returns a DATE (verified by hand at the time this was
written), so the YAML's expression is used AS-IS, with no translation layer.
A DuckDB that ever rejected it should fail this test loudly rather than
have the test quietly rewrite the expression: a translated spelling would be
verifying different SQL than the one dbt actually renders.
"""
from __future__ import annotations

import duckdb
import yaml

from tests.support import repo_file

PREPARED_YML = "dbt/models/prepared/_prepared.yml"

# gaps: -> the comparison operator dbt_utils' generated SQL uses between
# `upper_bound` and the next row's `lower_bound` (`next_lower_bound`).
_GAPS_OP = {"not_allowed": "=", "allowed": "<="}


def _mutually_exclusive_ranges_configs() -> list[dict]:
    """Every `dbt_utils.mutually_exclusive_ranges` test config in the YAML.

    Walks the whole parsed structure rather than assuming where a table-level
    test sits, so a block moved from `tests:` to a differently-shaped config
    (or added on a fourth SCD2 model later) is still found.
    """
    text = repo_file(PREPARED_YML).read_text(encoding="utf-8")
    doc = yaml.safe_load(text)
    found: list[dict] = []

    def walk(node):
        if isinstance(node, dict):
            cfg = node.get("dbt_utils.mutually_exclusive_ranges")
            if isinstance(cfg, dict):
                found.append(cfg)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(doc)
    return found


def _range_test_fails(con, rows: list[tuple[str, str]], gaps: str,
                       upper_bound_expr: str,
                       zero_length_range_allowed: bool) -> int:
    """Re-derive dbt_utils.mutually_exclusive_ranges' own row count.

    `rows`: (effective_from, effective_to) pairs for ONE partition key.
    Mirrors the macro exactly (see this module's docstring): a row fails
    when NOT (lower < upper [or <=, if zero-length allowed] AND upper `op`
    next_lower [or it is the last record by (lower desc, upper desc)]).
    """
    con.execute(
        "create or replace table v as select * from (values " + ",".join(
            f"('K','{f}'::date,'{e}'::date)" for f, e in rows) + ") t(k,f,e)")
    op = _GAPS_OP[gaps]
    lb_cmp = "<=" if zero_length_range_allowed else "<"
    ub = upper_bound_expr.replace("effective_to", "e")
    return con.execute(f"""
        select count(*) from (
            select f, {ub} as ub,
                   lead(f) over (partition by k order by f, e)         as next_lower,
                   row_number() over (partition by k order by f desc, e desc) = 1
                                                                        as is_last
            from v
        )
        where not (coalesce(f {lb_cmp} ub, false)
               and coalesce(ub {op} next_lower, is_last, false))
    """).fetchone()[0]


def test_scd2_range_tests_bound_on_exclusive_end():
    configs = _mutually_exclusive_ranges_configs()
    assert configs, (
        f"no dbt_utils.mutually_exclusive_ranges test found in {PREPARED_YML} "
        f"-- this test has nothing to check; if the SCD2 tests moved, update "
        f"the walker above")
    # ref_counterparty and ref_rating today; a third SCD2 table would add one.
    assert len(configs) >= 2, (
        f"expected at least 2 mutually_exclusive_ranges blocks (ref_counterparty, "
        f"ref_rating), found {len(configs)}: {configs}")

    for cfg in configs:
        upper = cfg.get("upper_bound_column")
        gaps = cfg.get("gaps")
        zero_ok = cfg.get("zero_length_range_allowed", False)
        assert upper == "date_add(effective_to, 1)", (
            f"upper_bound_column is {upper!r}, not the EXCLUSIVE "
            f"'date_add(effective_to, 1)' -- effective_to itself is INCLUSIVE "
            f"(scd2_effective_to), so binding on it directly either refuses a "
            f"correct one-day version or passes a same-day overlap. Config: {cfg}")
        assert gaps == "not_allowed", (
            f"gaps is {gaps!r}, not 'not_allowed'. Config: {cfg}. Nothing "
            f"legitimate produces a gap between one key's SCD2 versions -- see "
            f"this module's docstring and docs/DECISIONS.md"
            f"#a-snapshot-re-delivery-restates-the-whole-date -- so a gap is a "
            f"defect the test must catch.")
        assert zero_ok is not True, (
            f"zero_length_range_allowed: true makes the test pass a "
            f"zero/negative-length range, which is never correct here. Config: {cfg}")


def test_scd2_range_test_arithmetic():
    configs = _mutually_exclusive_ranges_configs()
    assert configs, f"no dbt_utils.mutually_exclusive_ranges test found in {PREPARED_YML}"

    overlap = [("2026-09-01", "2026-09-05"), ("2026-09-05", "2026-09-10")]
    one_day = [("2026-09-01", "2026-09-02"),
               ("2026-09-03", "2026-09-03"),
               ("2026-09-04", "9999-12-31")]
    gap = [("2026-09-01", "2026-09-02"), ("2026-09-05", "9999-12-31")]
    open_only = [("2026-09-01", "9999-12-31")]

    con = duckdb.connect()
    for cfg in configs:
        upper = cfg.get("upper_bound_column")
        gaps = cfg.get("gaps")
        zero_ok = cfg.get("zero_length_range_allowed", False)
        ub_expr = upper  # used as-is: see the module docstring on translation

        def fails(rows):
            return _range_test_fails(con, rows, gaps, ub_expr, zero_ok)

        assert fails(one_day) == 0, (
            f"config {cfg} refuses a correct one-day SCD2 version "
            f"([09-03, 09-03])")
        assert fails(overlap) != 0, (
            f"config {cfg} passes a same-day overlap ([09-01,09-05] then "
            f"[09-05,09-10]) -- as_of() would match both and double every "
            f"joined exposure row for that day")
        assert fails(gap) != 0, (
            f"config {cfg} passes a genuine gap between two of one key's "
            f"versions -- nothing legitimate produces one (see this module's "
            f"docstring), so it must fail")
        assert fails(open_only) == 0, (
            f"config {cfg} refuses a single open version ending "
            f"9999-12-31 -- the sentinel `scd2_effective_to` uses for every "
            f"currently-open version")
