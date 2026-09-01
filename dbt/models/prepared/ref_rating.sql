{{
  config(
    materialized='incremental',
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
#}

with

{% if is_incremental() %}
{{ scd2_incremental_scope(source('raw', 'ref_rating'), ['counterparty_id', 'agency']) }}
{% endif %}

raw_rows as (

    select
        r.*,
        {{ dedupe_rank(['r.counterparty_id', 'r.agency']) }} as _rn
    from {{ source('raw', 'ref_rating') }} r
    {% if is_incremental() %}
    join touched t
      on t.counterparty_id = r.counterparty_id and t.agency = r.agency
    left join replay_from p
      on p.counterparty_id = r.counterparty_id and p.agency = r.agency
    where r._business_date >= coalesce(p.from_date, date '1900-01-01')
    {% endif %}

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
    from ranked

),

{{ scd2_changes('versioned', ['counterparty_id', 'agency']) }}

ranged as (

    select
        counterparty_id,
        agency,
        rating,
        rating_date,
        outlook,
        rating_rank,
        grade_band,

        source_file,
        source_file_version,
        source_batch_id,
        dbt_invocation_id,
        nessie_ref,
        dbt_updated_at,

        {{ scd2_columns(['counterparty_id', 'agency']) }}

    from kept

)

select * from ranged
