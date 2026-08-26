{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'limit_id'],
    partition_by=['business_date'],
    tags=['prepared', 'reference']
  )
}}

{#
  Primary credit limits from GCIS2, conformed and typed.

  Nothing here is a report opinion: the amount becomes a number, the dates
  become dates, and the status strings gcis2 varies between releases are
  normalised once. Utilisation, headroom and breach flags belong in
  `reporting`, where they can be joined to exposure.

  `is_current` is the one derivation, and it is a restatement of what the feed
  already says rather than new logic: the limit is in force on the business
  date it was delivered for. It is computed here so that every consumer asks
  the question the same way -- the alternative is each report writing its own
  BETWEEN, which is precisely how the legacy estate ended up with limits that
  disagreed between screens.
#}

with raw_rows as (

    select
        *,
        {{ dedupe_rank(['limit_id']) }} as _rn
    from {{ source('raw', 'primary_limits') }}
    where {{ incremental_window('_business_date', 'business_date') }}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
        _business_date                                      as business_date,
        {{ clean_string('limit_id') }}                      as limit_id,
        {{ clean_string('counterparty_id') }}               as counterparty_id,
        upper({{ clean_string('limit_type') }})             as limit_type,

        -- DECIMAL, not DOUBLE: these are monetary amounts that get summed and
        -- compared against exposure, and binary floating point makes those
        -- comparisons non-reproducible. TRY_CAST via safe_cast() so an
        -- unparseable amount lands as NULL and fails the not_null test below,
        -- rather than failing the load.
        {{ safe_cast(clean_string('limit_amount'), 'DECIMAL(18,2)') }}
                                                            as limit_amount,
        upper({{ clean_string('currency') }})               as currency,
        {{ parse_date(clean_string('effective_date')) }}    as effective_date,
        {{ parse_date(clean_string('expiry_date')) }}       as expiry_date,

        -- gcis2 sends the status in whatever case the release happens to use,
        -- and an open-ended limit arrives with an empty expiry rather than a
        -- far-future one. Normalise both once, here.
        upper({{ clean_string('status') }})                 as status,

        _source_file                                        as source_file,
        _file_version                                       as source_file_version,
        {{ audit_columns() }}

    from deduped

)

select
    *,
    case
        when status is null then null
        when status <> 'ACTIVE' then false
        when effective_date is not null and business_date < effective_date then false
        -- A null expiry is an open-ended limit, not a missing value.
        when expiry_date is not null and business_date > expiry_date then false
        else true
    end                                                     as is_current
from cleaned
