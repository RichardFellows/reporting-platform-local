{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    scd2_retractions=true,
    unique_key=['position_id', 'effective_from'],
    partition_by=['effective_from_month'],
    tags=['prepared', 'reference', 'scd2']
  )
}}

{% set business_columns = ['position_id', 'desk_code', 'amount', 'currency', 'effective_date', 'is_active', 'description'] %}

with

{{ newest_file_version(source('raw', 'qa_happy_position')) }}

raw_rows as (

    select
        r.*,
        nv._newest_file_version
    from {{ source('raw', 'qa_happy_position') }} r
    join newest_file_version nv on nv._newest_cob_date = r._cob_date
    where {{ known_as_of() }}

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
        _source_file                                                as source_file,
        _file_version                                               as source_file_version,
        {{ source_provenance() }}
        {{ audit_columns() }},
        -- carried for the rank below, which needs the cleaned key
        _cob_date,
        _file_version,
        _row_number,
        _newest_file_version

    from raw_rows

),

ranked_rows as (

    select
        *,
        {{ dedupe_rank(['position_id'],
                       newest_version='_newest_file_version') }} as _rn
    from cleaned

),

{{ scd2_replay('ranked_rows', ['position_id'], business_columns) }}

versioned as (

    select
        *,
        {{ scd2_hash(['desk_code', 'amount', 'currency', 'effective_date', 'is_active', 'description']) }}    as _row_hash
    from replayed

),

{{ scd2_changes('versioned', ['position_id']) }}

ranged as (

    select
        {%- for c in scd2_output_columns(business_columns) %}
        {{ ident(c) }},
        {%- endfor %}
        {{ scd2_columns(['position_id']) }}

    from kept

)

select * from ranged
{{ scd2_retractions('ranged', ['position_id'], business_columns) }}
