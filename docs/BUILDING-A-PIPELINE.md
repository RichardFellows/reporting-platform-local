# Building a pipeline, end to end

One feed, four tables, in the order you build them. Every step is verifiable on
its own — do not batch them and then debug the whole thing at once.

```
delivery + control file
        │
        ▼   ingest (arrival-driven, not scheduled)
   raw.<feed>                 45 business days, no month-ends
        │
        ▼   dbt, 1:1 conform
   prepared.<feed>            45 business days + 80 month-ends
        │
        ▼   dbt, SCD2
   prepared.<feed>_scd2       one row per VERSION
        │
        ▼   dbt, point-in-time
   reporting.<mart>           a few columns, per business date
```

The retention difference is the point of the shape: raw is a verbatim copy kept
only long enough to re-derive prepared and answer *what did the file say*;
prepared is typed and deduplicated, so 80 month-ends there cost a fraction of
the same history in raw.

Prerequisites: the stack is up (`docs/QUICKSTART.md`) and you have one real
delivery file to hand.

---

## 1. Register the feed

`reporting_platform/config/feeds.yml`, or the **New feed** form on
<http://localhost:8082>, which writes the same thing.

```yaml
  - name: treasury_margin_call          # <source_system>_<feed>
    description: Margin calls per counterparty and call type, from TREASURY.
    source_system: TREASURY
    filename_pattern: 'marginCalls_(?P<business_date>\d{8})(?:_v(?P<version>\d+))?\.txt'
    delimiter: '|'
    business_key: [margin_call_id]
    expected_min_rows: 10
    columns:
      - margin_call_id: Margin Call Id
      - counterparty_id: Cpty Ref
      - call_amount: Call Amount (USD)
      - currency
      - call_date: Call Date
    column_types:
      call_amount: decimal
      call_date: date
    control:
      filename_pattern: 'marginCalls_(?P<business_date>\d{8})\.ctl'
      filename_template: 'marginCalls_{business_date}.ctl'
      delimiter: '|'
      keys:
        business_date: ReportingDate
        rows: Rows
        md5: MD5
```

**The name is five things at once** — the raw table, the DAG id, the landing
prefix, the dbt source table and the prepared model — which is why prefixing it
with the source system prefixes all five and none can drift.

**Column names.** Write `- platform_name: Source Header` wherever the file's
header is not a usable identifier, and a bare `- currency` where it is. The
rename happens once, at ingest, so raw onwards sees only identifiers — a space
in a column name reaches SQL through dbt macros, where `PARTITION BY Call Date`
is a syntax error, not a quoting inconvenience. Drift is still reported in the
file's names.

**Control file.** Both patterns are needed: `filename_pattern` to *recognise*
one arriving, `filename_template` to *construct* the name when pairing from a
delivery. `keys` maps the sender's labels onto `rows`, `md5` and
`business_date`; anything unmapped (`CreatedDate`) is ignored. Add
`digest_encoding: base64` if the digest is not hex, and
`rows_include_header: true` if the count includes the header line.

**`column_types` is not cosmetic.** It is what the prepared model's
`safe_cast` uses *and* what the sample-data generator reads. Leave a `decimal`
column undeclared and the generator produces a string, `safe_cast` nulls the
column, and nothing fails — because nulling is what `safe_cast` is for.

Verify, and fix a `None` here rather than later:

```bash
docker compose exec -T airflow python -c "
from reporting_platform.common.context import feeds
f = feeds()['treasury_margin_call']
print(f.raw_table, f.file_header, f.parse_filename('marginCalls_20260901.txt'))"
```

## 2. Declare raw to dbt

`dbt/models/raw/_sources.yml`, under `tables:` — arrival-only tests, because
raw is all strings and a range test here would be a string comparison:

```yaml
      - name: treasury_margin_call
        description: Margin calls per counterparty and call type, from TREASURY.
        columns:
          - name: margin_call_id
            tests: [not_null]
          - name: counterparty_id
            tests: [not_null]
```

Nothing else is needed for raw: `ingest_feed.py` builds the DDL from `columns`,
creates the table, and retention covers it because it is in `feeds.yml`. The
**45-day window** comes from the `raw` layer policy in
`reporting_platform/config/retention.yml`; it is per layer, not per table.

> A delivery older than that window is landed and **never ingested** —
> deliberately, or the next ingest would undo the last retention run.

## 3. The prepared 1:1 model

`dbt/models/prepared/treasury_margin_call.sql`. Same grain as raw, conformed
and typed — restating what the feed said, holding no opinion:

```sql
{{ config(materialized='incremental',
          unique_key=['business_date', 'margin_call_id'],
          partition_by=['business_date'],
          tags=['prepared']) }}

with raw_rows as (
    select *, {{ dedupe_rank(['margin_call_id']) }} as _rn
    from {{ source('raw', 'treasury_margin_call') }}
    where {{ incremental_window('_business_date', 'business_date') }}
),
deduped as (select * from raw_rows where _rn = 1)

select
    _business_date                                as business_date,
    {{ clean_string('margin_call_id') }}          as margin_call_id,
    {{ clean_string('counterparty_id') }}         as counterparty_id,
    {{ safe_cast(clean_string('call_amount'), 'DECIMAL(18,2)') }} as call_amount,
    {{ clean_string('currency') }}                as currency,
    {{ parse_date(clean_string('call_date')) }}   as call_date,
    _source_file                                  as source_file,
    _file_version                                 as source_file_version,
    {{ audit_columns() }}
from deduped
```

Four things that are not optional:

- **`incremental_window` takes TWO arguments in prepared** — raw's
  `_business_date` and the modelled `business_date`. One argument makes Spark
  bind the unqualified name to the outer query and the build fails with
  `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY`, *only on the incremental path* —
  so the first build passes and the second does not.
- **`dedupe_rank(business_key)`** is what picks the latest `_file_version`.
  Omit it and a re-delivery doubles the rows.
- **`partition_by=['business_date']`** is a retention requirement, not a
  performance one: without it retention deletes become full-table rewrites.
- **`safe_cast`, never a bare `CAST`.** A bad value must land as NULL and fail
  a *test*; a load must not fail on unparseable input.

Its longer retention — 45 days **plus 80 month-ends** — is the `prepared`
policy, and applies to every table in the layer.

Tests in `dbt/models/prepared/_prepared.yml`:

```yaml
models:
  - name: treasury_margin_call
    columns:
      - name: margin_call_id
        tests: [not_null]
      - name: call_amount
        tests:
          - dbt_utils.accepted_range: {min_value: 0, inclusive: true}
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [business_date, margin_call_id]
```

The uniqueness test is what proves `dedupe_rank` works — without it a broken
dedupe is invisible.

## 4. The SCD2 model, built from prepared

`dbt/models/prepared/treasury_margin_call_scd2.sql`. One row per **version**,
not per business date:

```sql
{{ config(materialized='incremental',
          unique_key=['margin_call_id', 'effective_from'],
          partition_by=['effective_from_month'],
          tags=['prepared']) }}

with

{% if is_incremental() %}
{#
  date_column='business_date': the default is raw's `_business_date`, and this
  model reads the PREPARED table, where that name does not survive.
#}
{{ scd2_incremental_scope(ref('treasury_margin_call'), ['margin_call_id'],
                          date_column='business_date') }},
{% endif %}

source_rows as (

    select p.*
    from {{ ref('treasury_margin_call') }} p
    {#
      BOTH joins. `touched` is the entities that moved recently, including ones
      never seen before; `replay_from` is where each already-open version
      began. Filtering on replay_from alone would silently drop every NEW
      entity, because a new one has no open version to replay from.
    #}
    {% if is_incremental() %}
    join touched t on t.margin_call_id = p.margin_call_id
    left join replay_from r on r.margin_call_id = p.margin_call_id
    where p.business_date >= coalesce(r.from_date, date '1900-01-01')
    {% endif %}

),

versioned as (

    select
        *,
        {{ scd2_hash(['counterparty_id', 'call_amount', 'currency',
                      'call_date']) }} as _row_hash
    from source_rows

),

{{ scd2_changes('versioned', ['margin_call_id']) }}

select
    margin_call_id,
    counterparty_id,
    call_amount,
    currency,
    call_date,
    {{ scd2_columns(['margin_call_id']) }}
from kept
```

- **`date_column='business_date'`.** The default is raw's `_business_date`;
  a model built off a *prepared* table must say so, and getting it wrong fails
  only on the incremental path — so the first build passes and the second does
  not.
- **The CTE-emitting macros do not punctuate themselves.** Both
  `scd2_incremental_scope` and `scd2_changes` emit CTEs *without* a trailing
  comma, so the caller adds one when another CTE follows — note the `}},`
  inside the `{% if is_incremental() %}` block above — and omits it when the
  model ends on the macro, as it does here. They used to carry the comma, which
  made them impossible to end a `with` chain on: `select … from kept` produced
  `),` then `select`, and Spark reported `[PARSE_SYNTAX_ERROR]` pointing at the
  projection rather than at the macro forty lines above.
- **Join BOTH `touched` and `replay_from` on the incremental path.** `touched`
  is the entities that moved recently, *including ones never seen before*;
  `replay_from` is where each already-open version began. Filtering on
  `replay_from` alone silently drops every NEW entity, because a new one has no
  open version to replay from — and it fails silently, since the rows simply do
  not appear.
- **Hash only attributes that are genuinely slow-moving.** If one of them
  changes on every delivery, every entity versions every day and the SCD2 table
  is the same size as the 1:1 one — you have paid the complexity and saved
  nothing, while everything still looks like it works. Measured during the
  build of this document: with a per-delivery date in the hash, the SCD2 table
  held 36 rows against the 1:1 table's 36; holding that column steady, the next
  delivery added 12 rows to the 1:1 table and 2 to the SCD2.
- **`scd2_hash` covers the BUSINESS attributes only.** Include `source_file`,
  `_batch_id` or anything build-related and the hash changes on every
  delivery, minting a new version daily and rebuilding the exact duplication
  the model exists to remove — while looking like it works.
- **`partition_by=['effective_from_month']`**, because an SCD2 row has no
  `business_date`. Retention detects the shape from the columns and range-
  deletes instead of dropping partitions; nothing needs configuring.

The `mutually_exclusive_ranges` test is **not optional** on an SCD2 table —
it is what catches two versions in force at once, which `as_of()` would
silently double:

```yaml
  - name: treasury_margin_call_scd2
    columns:
      - name: margin_call_id
        tests: [not_null]
    tests:
      - dbt_utils.mutually_exclusive_ranges:
          lower_bound_column: effective_from
          upper_bound_column: effective_to
          partition_by: margin_call_id
          gaps: allowed
```

```yaml
          zero_length_range_allowed: true
```

Two settings, and both are about `effective_to` being an **inclusive** upper
bound:

- **`gaps: allowed`** — contiguous versions look like a one-day gap to the
  test's arithmetic. `allowed` still fails on any overlap, which is the
  condition that matters.
- **`zero_length_range_allowed: true`** — an entity that changes on consecutive
  business days produces a version lasting exactly one day, where
  `effective_from == effective_to`. That is correct data, and dbt_utils rejects
  it as a zero-length range unless told otherwise. A feed that changes daily
  hits this on its first build.

Recreate the singular "exactly one open version" test now too — its body is in
[ADDING-A-MODEL.md](ADDING-A-MODEL.md).

## 5. The reporting model

`dbt/models/reporting/margin_call_exposure.sql`. A few columns from the SCD2,
addressed point-in-time:

```sql
{{ config(materialized='incremental',
          unique_key=['business_date', 'margin_call_id'],
          partition_by=['business_date'],
          tags=['reporting']) }}

with dates as (
    select distinct business_date
    from {{ ref('treasury_margin_call') }}
    where {{ incremental_window('business_date') }}
)

select
    d.business_date,
    s.margin_call_id,
    s.counterparty_id,
    s.call_amount
from dates d
join {{ ref('treasury_margin_call_scd2') }} s
  on {{ as_of('s', 'd.business_date') }}
```

- **`incremental_window` takes ONE argument in reporting** — both columns are
  already `business_date`.
- **The date spine comes from the prepared 1:1 table**, because an SCD2 table
  has no business dates of its own. This is what turns one-row-per-version back
  into one-row-per-date, and `as_of()` is what expresses it: `effective_to` is
  `9999-12-31` on the open version, so no null-handling branch is needed.
- Selecting `where is_current` instead would give you today's picture with no
  `business_date` — which conflicts with the reporting layer's `partition_by`
  and leaves retention nothing to delete by. Use the spine.

Add its grain tests to `_reporting.yml`, then:

```bash
docker compose exec -T airflow dbt parse --project-dir /opt/platform/dbt \
  --profiles-dir /opt/platform/dbt --target spark_local
```

`dbt parse` costs about five seconds, touches no Spark, and catches a bad
`ref()`, a macro typo and malformed YAML — most of what goes wrong.

## 6. Run it

```bash
# 1. deliver: drop the file AND its control file into ./inbox
cp marginCalls_20260901.txt marginCalls_20260901.ctl inbox/

# The delivery lands and waits; the CONTROL FILE triggers the ingest.
docker compose logs -f inbox

# 2. or, without the watcher
docker compose exec -T airflow python -m scripts.land_feeds --feed treasury_margin_call
docker compose exec -T airflow python -m scripts.bulk_ingest

# 3. build the three dbt models on a throwaway branch
branch=$(docker compose exec -T airflow python -m scripts._open_build_branch | tr -d '\r\n')
MSYS_NO_PATHCONV=1 docker compose exec -T airflow dbt build \
  --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt \
  --target spark_local --select path:models/prepared path:models/reporting \
  --vars "{nessie_ref: $branch}"

# 4. or let the DAGs do it, which also exercises branch-and-merge
docker compose exec -T airflow airflow dags unpause ingest_treasury_margin_call
docker compose exec -T airflow airflow dags trigger prepared_build -r first_build
```

**Build the models in dependency order the first time** — the SCD2 model reads
the 1:1 table, and the reporting model reads both. `dbt build` handles that from
the `ref()`s; a manual `--select` of one model alone does not.

## 7. Check it

```bash
docker compose exec -T airflow python -m scripts.duckdb_console --tables
docker compose exec -T airflow python -m reporting_platform.retention.retention \
  --all-managed --dry-run
```

The retention dry run is the one worth reading: it reports which mode it chose
per table, so you can see the SCD2 table getting a range delete and the others a
partition delete, and confirm the 45-day and 80-month-end windows are what you
meant.

---

## What you did not have to touch

- **No DAG file.** `feed_ingest.py` generates one DAG per entry in `feeds.yml`;
  Cosmos renders one task per dbt model on every parse.
- **No maintenance or retention registration.** Both are keyed by layer, and
  the table set is derived from `feeds.yml` and the model directory.
- **No `PREPARED_TABLES`.** It no longer exists —
  see [DECISIONS.md#managed-tables-are-derived](DECISIONS.md#managed-tables-are-derived).
