"""`exposure_change` on a date pair where one counterparty DISAPPEARS.

The model was driven from the current date's rows, so a counterparty with
exposure yesterday and none today produced no row at all, and its `REMOVED`
branch (`cur.total_mtm is null`) could never fire for one -- only, wrongly,
for a counterparty that was present with an unknown total (todo 16, plan #21).

`REMOVED` means ABSENT ENTIRELY: a row on the previous retained date and none
today. `counterparty_exposure` is aggregated from trades, so no live trades
means no row. The removed counterparty's row lands in TODAY's partition --
the model is insert_overwrite per cob_date.

THE SQL UNDER TEST IS THE REAL MODEL, rendered with the project's macros and
run in DuckDB, as tests/test_dedupe_rank.py does it. See that module for what
is emulated.
"""
from __future__ import annotations

from decimal import Decimal

import duckdb

from tests.test_dedupe_rank import REPORTING, _duck, _model, _render, _shim

D0, D1, D2 = "2026-08-31", "2026-09-01", "2026-09-02"   # D0 is a month end


def _exposure(con):
    """A, B, C on D1; on D2, A is unchanged, B is GONE, C's total became
    unknown and D is new. D0 is the prior month end for both."""
    con.execute("""
        create or replace table prepared_counterparty_exposure as
        select cob_date::date as cob_date, counterparty_id,
               'name ' || counterparty_id as legal_name, 'GB' as country_code,
               'BANK' as sector, total_mtm::decimal(28,4) as total_mtm,
               total_notional::decimal(28,4) as total_notional,
               trade_count::bigint as trade_count
        from (values
          ('%(d0)s', 'A',  80, 800, 1),
          ('%(d0)s', 'B',  40, 400, 1),
          ('%(d1)s', 'A', 100, 1000, 2),
          ('%(d1)s', 'B',  50, 500, 3),
          ('%(d1)s', 'C',  10, 100, 1),
          ('%(d2)s', 'A', 100, 1000, 2),
          ('%(d2)s', 'C', null, 100, 1),
          ('%(d2)s', 'D',   5,  50, 1)
        ) t(cob_date, counterparty_id, total_mtm, total_notional, trade_count)
    """ % {"d0": D0, "d1": D1, "d2": D2})


def _run(sql: str, select: str):
    con = _shim(duckdb.connect())
    _exposure(con)
    return con.execute(f"with m as ({_duck(sql)}) {select}").fetchall()


def _model_sql():
    return _render(_model(REPORTING / "exposure_change.sql"))


def test_a_counterparty_that_disappears_is_removed_in_todays_partition():
    got = _run(_model_sql(), f"""
        select counterparty_id, change_category, current_mtm, prior_mtm,
               mtm_change, trade_count_change, prior_month_end_mtm,
               mtm_change_since_month_end
        from m where cob_date = date '{D2}' order by 1""")
    by_key = {r[0]: r for r in got}
    assert set(by_key) == {"A", "B", "C", "D"}, got
    b = by_key["B"]
    assert b[1] == "REMOVED", b
    assert b[2] is None, b                      # nothing current to show
    assert b[3] == Decimal("50"), b             # what it had
    assert b[4] == Decimal("-50"), b            # the whole of it went
    assert b[5] == -3, b
    assert b[6] == Decimal("40"), b             # prior month end still joins
    assert b[7] == Decimal("-40"), b


def test_the_other_categories_are_unchanged_by_it():
    got = dict(_run(_model_sql(), f"""
        select counterparty_id, change_category from m
        where cob_date = date '{D2}'"""))
    assert got == {"A": "UNCHANGED", "B": "REMOVED",
                   # an MTM that became unknown is a change, not UNCHANGED --
                   # and not REMOVED: C is still here
                   "C": "CHANGED",
                   "D": "NEW"}, got


def test_nothing_is_removed_when_nothing_disappeared():
    # D1 against D0: A and B both still present, C new.
    got = dict(_run(_model_sql(), f"""
        select counterparty_id, change_category from m
        where cob_date = date '{D1}'"""))
    assert got == {"A": "CHANGED", "B": "CHANGED", "C": "NEW"}, got


def test_each_date_is_one_row_per_counterparty():
    """insert_overwrite needs each date returned whole and once: a key must
    not appear as both a present row and a REMOVED one."""
    got = _run(_model_sql(), """
        select cob_date, counterparty_id, count(*) from m
        group by 1, 2 having count(*) > 1""")
    assert got == [], got
