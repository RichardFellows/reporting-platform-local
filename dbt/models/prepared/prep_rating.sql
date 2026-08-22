{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'counterparty_id', 'agency'],
    partition_by=['business_date'],
    tags=['prepared', 'reference']
  )
}}

{#
  Agency ratings, conformed, with a numeric rank so downstream can order and
  compare. The rank mapping is deliberately explicit rather than a seed file:
  it is a small, stable, auditable piece of business logic and reviewers can
  see it in the diff.
#}

with raw_rows as (

    select
        *,
        {{ dedupe_rank(['counterparty_id', 'agency']) }} as _rn
    from {{ source('raw', 'rating') }}
    where {{ incremental_window('_business_date', 'business_date') }}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
        _business_date                                  as business_date,
        {{ clean_string('counterparty_id') }}           as counterparty_id,
        upper({{ clean_string('agency') }})             as agency,
        upper({{ clean_string('rating') }})             as rating,
        {{ parse_date(clean_string('rating_date')) }}   as rating_date,
        {{ clean_string('outlook') }}                   as outlook,
        _source_file                                    as source_file,
        _file_version                                   as source_file_version,
        {{ audit_columns() }}

    from deduped

)

select
    *,
    case upper(rating)
        when 'AAA' then 1  when 'AA+' then 2  when 'AA'  then 3  when 'AA-' then 4
        when 'A+'  then 5  when 'A'   then 6  when 'A-'  then 7
        when 'BBB+' then 8 when 'BBB' then 9  when 'BBB-' then 10
        when 'BB+' then 11 when 'BB'  then 12 when 'BB-' then 13
        when 'B+'  then 14 when 'B'   then 15 when 'B-'  then 16
        when 'CCC+' then 17 when 'CCC' then 18 when 'CCC-' then 19
        when 'CC'  then 20 when 'C'   then 21 when 'D'   then 22
        else null
    end                                                  as rating_rank,
    case
        when upper(rating) in ('AAA','AA+','AA','AA-','A+','A','A-','BBB+','BBB','BBB-')
            then 'INVESTMENT_GRADE'
        when upper(rating) is null then null
        else 'SUB_INVESTMENT_GRADE'
    end                                                  as grade_band
from cleaned
