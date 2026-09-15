"""Read-only assertions for tests/fixtures/happy_path after UI ingestion."""
from datetime import date
from decimal import Decimal
import json

from scripts.duckdb_console import connect


def main():
    con = connect()
    raw = con.execute("""
        select position_id, desk_code, amount, currency, effective_date,
               is_active, description
        from lakehouse.raw.qa_happy_position
        where _cob_date = DATE '2026-09-14' order by position_id
    """).fetchall()
    assert raw == [
        ('HP001', ' rates ', '1250.50', 'gbp', '2026-09-14', 'Y', 'First dummy position'),
        ('HP002', 'credit', '249.50', 'GBP', '2026-09-14', 'N', 'Second dummy position'),
        ('HP003', 'rates', '-100.00', 'usd', '2026-09-14', 'true', 'Quoted | pipe'),
    ], raw
    prepared = con.execute("""
        select position_id, desk_code, amount, currency, effective_date, is_active,
               description
        from lakehouse.prepared.qa_happy_position
        where cob_date = DATE '2026-09-14' order by position_id
    """).fetchall()
    assert prepared == [
        ('HP001', 'RATES', Decimal('1250.50'), 'GBP', date(2026, 9, 14), True, 'First dummy position'),
        ('HP002', 'CREDIT', Decimal('249.50'), 'GBP', date(2026, 9, 14), False, 'Second dummy position'),
        ('HP003', 'RATES', Decimal('-100.00'), 'USD', date(2026, 9, 14), True, 'Quoted | pipe'),
    ], prepared
    reporting = con.execute("""
        select currency, position_count, total_amount, active_position_count
        from lakehouse.reporting.qa_happy_position_summary
        where cob_date = DATE '2026-09-14' order by currency
    """).fetchall()
    assert reporting == [('GBP', 2, Decimal('1500.00'), 1),
                         ('USD', 1, Decimal('-100.00'), 1)], reporting
    schemas = {}
    for layer, table in [('raw', 'qa_happy_position'),
                         ('prepared', 'qa_happy_position'),
                         ('reporting', 'qa_happy_position_summary')]:
        schemas[layer] = con.execute(f'DESCRIBE lakehouse.{layer}.{table}').fetchall()
    raw_types = {row[0]: row[1] for row in schemas['raw']}
    assert all(raw_types[c] == 'VARCHAR' for c in (
        'position_id', 'desk_code', 'amount', 'currency', 'effective_date',
        'is_active', 'description')), raw_types
    prepared_types = {row[0]: row[1] for row in schemas['prepared']}
    assert prepared_types['amount'] == 'DECIMAL(18,2)', prepared_types
    assert prepared_types['effective_date'] == 'DATE', prepared_types
    assert prepared_types['is_active'] == 'BOOLEAN', prepared_types
    print(json.dumps({'status': 'PASS', 'catalog_ref': 'main', 'raw': raw,
                      'prepared': prepared, 'reporting': reporting,
                      'schemas': schemas}, default=str, indent=2))
    con.close()


if __name__ == '__main__':
    main()
