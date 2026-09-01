{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'trade_id'],
    partition_by=['business_date'],
    tags=['prepared', 'transactional']
  )
}}

{#
  Trade positions, conformed and typed.

  Note what is NOT here: no joins to counterparty, no exposure calculation, no
  report-specific filtering. prepared is conformed source data. Anything that
  encodes a report's opinion belongs in reporting, where the lineage shows
  which report owns it.
#}

with raw_rows as (

    select
        *,
        {{ dedupe_rank(['trade_id']) }} as _rn
    from {{ source('raw', 'fo_trade') }}
    where {{ incremental_window('_business_date', 'business_date') }}

),

deduped as (
    select * from raw_rows where _rn = 1
),

typed as (

    select
        _business_date                                    as business_date,
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
        {{ audit_columns() }}

    from deduped

)

select
    *,
    case
        when maturity_date is null then null
        when maturity_date < business_date then true
        else false
    end as is_matured
from typed
