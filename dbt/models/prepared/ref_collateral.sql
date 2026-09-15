{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['prepared', 'reference']
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
  Collateral positions held against counterparty exposure, from the collateral management system.

  SCAFFOLDED by the feed console from the registry entry -- conforming and
  typing only. Anything that restates what the feed already says (a validity
  flag derived from its own effective/expiry dates, a status normalisation)
  belongs here and should be added deliberately. Anything that is a report
  opinion -- utilisation, breach flags, anything needing a join -- belongs in
  `reporting`, where it can be joined to exposure.
#}

with raw_rows as (

    select *
    from {{ source('raw', 'ref_collateral') }}
    where {{ incremental_window('_cob_date', 'cob_date') }}
      and {{ known_as_of() }}

),

cleaned as (

    {#
      Every admitted raw row, cleaned ONCE. The rank runs on this in
      `ranked_rows` below -- see its comment, and ref_counterparty's
      `ranked_rows` (dbt/models/prepared/ref_counterparty.sql), which this
      mirrors.
    #}
    select
        _cob_date                                                      as cob_date,
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
      THE IN-FILE DEDUPE IS ON THE CLEANED KEY -- see ref_counterparty's
      `ranked_rows` comment: ranked on the raw key, ' T1' and 'T1' in one
      file were each "last in file" for their own spelling and both
      survived as two rows with the same (cob_date, cleaned collateral_id).
    #}
    select
        *,
        {{ dedupe_rank(['collateral_id']) }} as _rn
    from cleaned

),

deduped as (

    select
        cob_date,
        collateral_id,
        counterparty_id,
        collateral_type,
        market_value,
        currency,
        valuation_date,
        haircut_pct,
        is_eligible,
        source_file,
        source_file_version,
        {{ source_provenance_columns() }}
        source_batch_id,
        dbt_invocation_id,
        nessie_ref,
        dbt_updated_at
    from ranked_rows
    where _rn = 1

)

select * from deduped
