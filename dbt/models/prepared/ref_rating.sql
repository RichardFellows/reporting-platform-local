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

{#
  rating_rank and grade_band are DERIVED from `rating` and are deliberately
  not hashed -- they cannot change without it changing, and hashing them would
  only make the change detector slower and its column list misleading about
  what is actually a source fact.
#}
{% set rating_rank %}
case upper(rating)
            when 'AAA' then 1  when 'AA+' then 2  when 'AA'  then 3  when 'AA-' then 4
            when 'A+'  then 5  when 'A'   then 6  when 'A-'  then 7
            when 'BBB+' then 8 when 'BBB' then 9  when 'BBB-' then 10
            when 'BB+' then 11 when 'BB'  then 12 when 'BB-' then 13
            when 'B+'  then 14 when 'B'   then 15 when 'B-'  then 16
            when 'CCC+' then 17 when 'CCC' then 18 when 'CCC-' then 19
            when 'CC'  then 20 when 'C'   then 21 when 'D'   then 22
            else null
        end
{% endset %}
{% set grade_band %}
case
            when upper(rating) in ('AAA','AA+','AA','AA-','A+','A','A-','BBB+','BBB','BBB-')
                then 'INVESTMENT_GRADE'
            when upper(rating) is null then null
            else 'SUB_INVESTMENT_GRADE'
        end
{% endset %}

{{ scd2_prepared(
    source_name='ref_rating',
    keys=['counterparty_id', 'agency'],
    cleaning={
        'counterparty_id': clean_string('counterparty_id'),
        'agency':          'upper(' ~ clean_string('agency') ~ ')',
        'rating':          'upper(' ~ clean_string('rating') ~ ')',
        'rating_date':     parse_date(clean_string('rating_date')),
        'outlook':         clean_string('outlook'),
    },
    hashed=['rating', 'rating_date', 'outlook'],
    derived={
        'rating_rank': rating_rank,
        'grade_band':  grade_band,
    },
) }}
