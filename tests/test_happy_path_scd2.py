"""Execute the actual happy-path SCD2 model through full and incremental builds."""
from tests.test_scd2_incremental import _connect, _deliver, _build


def test_happy_path_history_and_corrected_delivery():
    con = _connect()
    constants = dict(desk_code=' rates ', currency='gbp',
                     effective_date='2026-09-14', is_active='Y', description='Example')
    steps = [('2026-09-14', 1, '1250.50'),
             ('2026-09-15', 1, '1250.50'),
             ('2026-09-16', 1, '1500.00'),
             ('2026-09-16', 2, '1250.50'),
             ('2026-09-17', 1, '1600.00')]
    query = ("select position_id, amount, effective_from::varchar, "
             "effective_to::varchar, is_current from {} order by effective_from")
    for day, version, amount in steps:
        _deliver(con, ['position_id'], 'amount', constants, day, version,
                 {'HP001': amount}, day + ' 12:00')
        con.execute('create or replace view raw_qa_happy_position as select * from raw_src')
        _build(con, 'qa_happy_position_scd2', 'full_build', False)
        _build(con, 'qa_happy_position_scd2', 'incremental_build', True)
        actual = con.execute(query.format('incremental_build')).fetchall()
        assert actual == con.execute(query.format('full_build')).fetchall()
        assert len(actual) == (2 if amount in ('1500.00', '1600.00') else 1)
    assert [(r[2], r[3], r[4]) for r in actual] == [
        ('2026-09-14', '2026-09-16', False),
        ('2026-09-17', '9999-12-31', True)]
    assert con.execute('select distinct desk_code, currency, is_active from incremental_build').fetchall() == [('RATES', 'GBP', True)]
    con.close()
