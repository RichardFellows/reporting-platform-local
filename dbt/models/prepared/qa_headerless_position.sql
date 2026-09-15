{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['prepared', 'reference']
  )
}}

{#
  Dummy QA feed with a headerless pipe-delimited control file.

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
    from {{ source('raw', 'qa_headerless_position') }}
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
        {{ safe_cast(clean_string('amount'), 'DECIMAL(18,2)') }} as amount,
        upper({{ clean_string('currency') }})                    as currency,
        _source_file                                             as source_file,
        _file_version                                            as source_file_version,
        {{ source_provenance() }}
        {{ audit_columns() }}

    from deduped

)

select * from cleaned
