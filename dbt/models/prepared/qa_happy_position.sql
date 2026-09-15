{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['prepared', 'reference']
  )
}}

{#
  Dummy QA positions for pipe CSV and control-file end-to-end verification.

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
        {{ dedupe_rank(['position_id']) }} as _rn
    from {{ source('raw', 'qa_happy_position') }}
    where {{ incremental_window('_cob_date', 'cob_date') }}
      and {{ known_as_of() }}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
        _cob_date                                                as cob_date,
        {{ clean_string('position_id') }}                        as position_id,
        upper({{ clean_string('desk_code') }})                   as desk_code,
        {{ safe_cast(clean_string('amount'), 'DECIMAL(18,2)') }} as amount,
        upper({{ clean_string('currency') }})                    as currency,
        {{ parse_date(clean_string('effective_date')) }}         as effective_date,
        case
            when upper({{ clean_string('is_active') }}) in ('Y', 'YES', 'TRUE', '1') then true
            when upper({{ clean_string('is_active') }}) in ('N', 'NO', 'FALSE', '0') then false
            else null
        end                                                      as is_active,
        {{ clean_string('description') }}                        as description,
        _source_file                                             as source_file,
        _file_version                                            as source_file_version,
        {{ source_provenance() }}
        {{ audit_columns() }}

    from deduped

)

select * from cleaned
