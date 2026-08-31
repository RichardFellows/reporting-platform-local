{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'collateral_id'],
    partition_by=['business_date'],
    tags=['prepared', 'reference']
  )
}}

{#
  Collateral positions held against counterparty exposure, from the collateral management system.

  SCAFFOLDED by the feed console from the registry entry -- conforming and
  typing only. Anything that restates what the feed already says (a validity
  flag derived from its own effective/expiry dates, a status normalisation)
  belongs here and should be added deliberately. Anything that is a report
  opinion -- utilisation, breach flags, anything needing a join -- belongs in
  `reporting`, where it can be joined to exposure.
#}

with raw_rows as (

    select
        *,
        {{ dedupe_rank(['collateral_id']) }} as _rn
    from {{ source('raw', 'collateral') }}
    where {{ incremental_window('_business_date', 'business_date') }}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
        _business_date                                                 as business_date,
        {{ clean_string('collateral_id') }}                            as collateral_id,
        {{ clean_string('counterparty_id') }}                          as counterparty_id,
        upper({{ clean_string('collateral_type') }})                   as collateral_type,
        {{ safe_cast(clean_string('market_value'), 'DECIMAL(18,2)') }} as market_value,
        upper({{ clean_string('currency') }})                          as currency,
        {{ parse_date(clean_string('valuation_date')) }}               as valuation_date,
        {{ safe_cast(clean_string('haircut_pct'), 'DECIMAL(18,2)') }}  as haircut_pct,
        case
            when upper({{ clean_string('is_eligible') }}) in ('Y', 'YES', 'TRUE', '1') then true
            when upper({{ clean_string('is_eligible') }}) in ('N', 'NO', 'FALSE', '0') then false
            else null
        end                                                            as is_eligible,
        _source_file                                                   as source_file,
        _file_version                                                  as source_file_version,
        {{ audit_columns() }}

    from deduped

)

select * from cleaned
