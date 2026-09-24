{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['prepared', 'transactional']
  )
}}

{#
  INSERT_OVERWRITE, NOT MERGE, AND NO unique_key. An incremental run
  REWRITES every COB date its select returns and leaves every other date
  alone. A merge never deletes, so a key a re-delivery dropped used to
  survive every run after the one that first wrote it.

  Correct only if each date the select returns, it returns WHOLE -- a
  partial date would be truncated to the part. Its select admits raw
  by `_cob_date` alone (the lookback window and the as-of filter), so each
  date it returns is every row of that date's newest delivery.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}

{#
  Trade positions, conformed and typed.

  Note what is NOT here: no joins to counterparty, no exposure calculation, no
  report-specific filtering. prepared is conformed source data. Anything that
  encodes a report's opinion belongs in reporting, where the lineage shows
  which report owns it.
#}

{% set output_columns = ['cob_date', 'trade_id', 'counterparty_id', 'book', 'product_type', 'currency', 'notional', 'mtm_value', 'trade_date', 'maturity_date'] %}

with raw_rows as (

    {#
      EVERY raw row the window and the as-of filter admit, unranked. The
      rank happens after cleaning, on the CLEANED key -- see `ranked_rows`.
      Cleaning filters nothing, so dedupe_rank's newest delivery per date is
      decided over the same rows it always was.
    #}
    select *
    from {{ source('raw', 'fo_trade') }}
    where {{ incremental_window('_cob_date', 'cob_date') }}
      and {{ known_as_of() }}

),

cleaned as (

    select
        _cob_date                                         as cob_date,
        {{ clean_string('trade_id') }}                    as trade_id,
        {{ clean_string('counterparty_id') }}             as counterparty_id,
        {{ clean_string('book') }}                        as book,
        upper({{ clean_string('product_type') }})         as product_type,
        upper({{ clean_string('currency') }})             as currency,

        -- TRY_CAST, not CAST: an unparseable notional must land as NULL and
        -- fail the not_null test, rather than abort the whole build.
        {{ safe_cast(clean_string('notional'), 'decimal(28,4)') }}   as notional,
        {{ safe_cast(clean_string('mtm_value'), 'decimal(28,4)') }}  as mtm_value,

        {{ parse_date(clean_string('trade_date')) }}      as trade_date,
        {{ parse_date(clean_string('maturity_date')) }}   as maturity_date,

        _source_file                                      as source_file,
        _file_version                                      as source_file_version,
        {{ source_provenance() }}
        {{ audit_columns() }},
        -- carried for the rank below, which needs the cleaned key
        _cob_date,
        _file_version,
        _row_number

    from raw_rows

),

ranked_rows as (

    {#
      THE IN-FILE DEDUPE IS ON THE CLEANED KEY. Ranked on the raw key, ' T1'
      and 'T1' in one file were each "last in file", both survived, and
      became two rows with one (cob_date, trade_id) -- which the uniqueness
      test then refused, naming uniqueness rather than the padded key.
    #}
    select
        *,
        {{ dedupe_rank(['trade_id']) }} as _rn
    from cleaned

),

deduped as (

    select
        {%- for c in prepared_output_columns(output_columns) %}
        {{ ident(c) }}{{ ',' if not loop.last }}
        {%- endfor %}
    from ranked_rows
    where _rn = 1

)

select
    *,
    case
        when maturity_date is null then null
        when maturity_date < cob_date then true
        else false
    end as is_matured
from deduped
