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

{% set output_columns = ['cob_date', 'position_id', 'desk_code', 'amount', 'currency', 'effective_date', 'is_active', 'description'] %}

with raw_rows as (

    {#
      EVERY raw row the window and the as-of filter admit, unranked. The
      rank happens after cleaning, on the CLEANED key -- see `ranked_rows`
      (plan #13; this model was scaffolded before that fix).
    #}
    select *
    from {{ source('raw', 'qa_happy_position') }}
    where {{ incremental_window('_cob_date', 'cob_date') }}
      and {{ known_as_of() }}

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
        {{ audit_columns() }},
        -- carried for the rank below, which needs the cleaned key
        _cob_date,
        _file_version,
        _row_number


    from raw_rows

),

ranked_rows as (

    {# THE IN-FILE DEDUPE IS ON THE CLEANED KEY: ' B' and 'B' are one key. #}
    select
        *,
        {{ dedupe_rank(['position_id']) }} as _rn
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

select * from deduped
