# Pipe-delimited onboarding fixture

Dummy feed: `qa_happy_position`; COB date: `2026-09-14`.

Upload the CSV and its `.ctl` sibling together through the feed console.
Both files are UTF-8 without BOM, with LF line endings and pipe delimiters.
The CSV uses double-quote escaping and includes a quoted pipe in one value.

## Feed settings

- Source system: `QA`
- Filename pattern: `qa_happy_position_(?P<cob_date>\d{8})(?:_v(?P<version>\d+))?\.csv`
- Business key: `position_id`
- Minimum rows: `1`
- Delivery control pattern: `{stem}\.ctl`
- Control format: delimited, `|`, header present
- Row count column: `RECORD_COUNT`
- MD5 column: `CHECKSUM`

The filename supplies delivery identity. The control file gates integrity;
its date and version columns document the fixture but need no arrival mapping.

## Expected published values

Raw must retain three rows and all seven business columns as strings,
including the whitespace around ` rates ` and `Quoted | pipe` as one value.

| position_id | desk_code | amount | currency | effective_date | is_active |
|---|---|---:|---|---|---|
| HP001 | RATES | 1250.50 | GBP | 2026-09-14 | true |
| HP002 | CREDIT | 249.50 | GBP | 2026-09-14 | false |
| HP003 | RATES | -100.00 | USD | 2026-09-14 | true |

Prepared types: string identifiers/codes/text, decimal(18,2) amount,
date effective_date, boolean is_active. The reporting aggregation by currency
must return GBP: 2 positions, 1500.00; USD: 1 position, -100.00.

Live execution results are recorded separately; this fixture alone does not
prove ingestion or publication succeeded.

## Headerless control variant

`qa_headerless_position_20260915.csv` has two pipe-delimited rows. Its control
file contains exactly one headerless record:

```text
qa_headerless_position_20260915.csv|2|e503b1d9efa39929629b06ee8348f1c7
```

The feed declares the control columns in positional order as `FILENAME`,
`RECORD_COUNT`, and `CHECKSUM`, with `header: false`. The row-count and MD5
fields gate ingestion; the filename is retained as sender context.

## SCD2 prepared version

`prepared.qa_happy_position_scd2` reads the same raw deliveries and applies
the same conformance as `prepared.qa_happy_position`. It tracks changes to
desk, amount, currency, effective date, active status and description by
`position_id`. Repeated values do not create versions. Missing positions
carry forward; corrected deliveries can retract a superseded change.

`effective_from` and `effective_to` are inclusive COB dates. The source's
`effective_date` is a separate business attribute. Current versions have
`is_current = true` and `effective_to = '9999-12-31'`. Delivery provenance
identifies the delivery that began the version.

With only the original fixture loaded, expect three current versions starting
on `2026-09-14`, with the values above. Query current data with:

```sql
select * from lakehouse.prepared.qa_happy_position_scd2
where is_current;
```

For historical data, filter a date between `effective_from` and `effective_to`.
Build with `dbt build --select qa_happy_position_scd2 --vars
'nessie_ref: <build-branch>'` on a build branch, following the project's
write-audit-publish process.
