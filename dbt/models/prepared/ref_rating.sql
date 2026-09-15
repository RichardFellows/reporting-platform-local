{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    scd2_retractions=true,
    unique_key=['counterparty_id', 'agency', 'effective_from'],
    partition_by=['effective_from_month'],
    tags=['prepared', 'reference', 'scd2']
  )
}}

{#
  Agency ratings, conformed, with a numeric rank so downstream can order and
  compare. The rank mapping is deliberately explicit rather than a seed file:
  it is a small, stable, auditable piece of business logic and reviewers can
  see it in the diff.

  SCD2, one row per (counterparty, agency) VERSION. A rating moves perhaps
  once a year and an outlook a little more often, but the feed restated every
  rating on every delivery: 5,580 rows to express roughly 1,300 versions.
  Consumers join point-in-time with as_of(). See docs/ARCHITECTURE.md.

  THE GRAIN INCLUDES agency. A counterparty holds one version per agency, and
  they move independently -- Moody's downgrading does not close the S&P
  version. The key columns below are therefore (counterparty_id, agency) and
  getting that wrong would interleave two agencies' histories into one chain.

  MERGE, NOT insert_overwrite; a key the newest delivery omits does not close
  the version in force, but does retract a version the replaced delivery
  began (`scd2_retractions`) -- all for the reasons ref_counterparty's header
  states.
#}

{% set business_columns = ['counterparty_id', 'agency', 'rating', 'rating_date', 'outlook', 'rating_rank', 'grade_band'] %}

with

{{ newest_file_version(source('raw', 'ref_rating')) }}

raw_rows as (

    {# Every raw row, unranked, for the reasons ref_counterparty's copy of
       this states. #}
    select
        r.*,
        nv._newest_file_version
    from {{ source('raw', 'ref_rating') }} r
    join newest_file_version nv on nv._newest_cob_date = r._cob_date
    where {{ known_as_of() }}

),

cleaned as (

    select
        _cob_date                                       as cob_date,
        {{ clean_string('counterparty_id') }}           as counterparty_id,
        upper({{ clean_string('agency') }})             as agency,
        upper({{ clean_string('rating') }})             as rating,
        {{ parse_date(clean_string('rating_date')) }}   as rating_date,
        {{ clean_string('outlook') }}                   as outlook,
        _source_file                                    as source_file,
        _file_version                                   as source_file_version,
        {{ source_provenance() }}
        {{ audit_columns() }},
        -- carried for the rank below, which needs the cleaned key
        _cob_date,
        _file_version,
        _row_number,
        _newest_file_version

    from raw_rows

),

ranked as (

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

),

ranked_rows as (

    {#
      THE IN-FILE DEDUPE IS ON THE CLEANED KEY. Ranked on the raw key, ' B'
      and 'B' (or 'moodys' and 'MOODYS') in one file were each "last in file"
      and both survived as one cleaned key: two versions with one
      effective_from, one ending before it began.
    #}
    select
        *,
        {{ dedupe_rank(['counterparty_id', 'agency'],
                       newest_version='_newest_file_version') }} as _rn
    from ranked

),

{{ scd2_replay('ranked_rows', ['counterparty_id', 'agency'], business_columns, source('raw', 'ref_rating')) }}

{#
  rating_rank and grade_band are DERIVED from `rating` and are deliberately
  not hashed -- they cannot change without it changing, and hashing them would
  only make the change detector slower and its column list misleading about
  what is actually a source fact.
#}
versioned as (

    select
        *,
        {{ scd2_hash(['rating', 'rating_date', 'outlook']) }}     as _row_hash
    from replayed

),

{{ scd2_changes('versioned', ['counterparty_id', 'agency']) }}

ranged as (

    select
        {%- for c in scd2_output_columns(business_columns) %}
        {{ ident(c) }},
        {%- endfor %}
        {{ scd2_columns(['counterparty_id', 'agency']) }}

    from kept

)

{#
  On an incremental run, plus one marker row per version this replay
  covers and no longer derives -- a change a re-delivery dropped or
  reverted. The merge deletes those (macros/merge.sql); without them it
  would leave the retracted version current.
#}
select * from ranged
{{ scd2_retractions('ranged', ['counterparty_id', 'agency'], business_columns) }}
