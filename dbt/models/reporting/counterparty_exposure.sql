{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'counterparty_id'],
    partition_by=['business_date'],
    tags=['reporting', 'core']
  )
}}

{#
  Counterparty exposure — the shared spine of the reporting layer.

  This is the model that makes "shared data lineage" concrete: the two models
  below both ref() this one rather than re-deriving exposure from prepared.
  If the exposure definition changes, it changes in ONE place and every report
  moves together. That is the specific failure the legacy estate has today —
  the same measure computed slightly differently in several stored procedures,
  with no way to tell which is authoritative.

  Replaces the legacy report server's "Counterparty Exposure Summary" report set.
#}

with trades as (

    select *
    from {{ ref('trade') }}
    where {{ incremental_window('business_date') }}
      and coalesce(is_matured, false) = false

),

counterparties as (
    select * from {{ ref('counterparty') }}
),

-- One rating per counterparty per date: the most conservative (highest rank)
-- across agencies. Documented here because it is a business rule, not a
-- technical one, and report owners need to be able to find it.
worst_rating as (

    select
        business_date,
        counterparty_id,
        max(rating_rank)                                        as worst_rating_rank,
        min(case when grade_band = 'SUB_INVESTMENT_GRADE' then 0 else 1 end) as is_investment_grade_flag
    from {{ ref('rating') }}
    group by business_date, counterparty_id

),

aggregated as (

    select
        t.business_date,
        t.counterparty_id,
        count(*)                                                as trade_count,
        sum(t.notional)                                         as total_notional,
        sum(t.mtm_value)                                        as total_mtm,
        sum(case when t.mtm_value > 0 then t.mtm_value else 0 end) as positive_mtm,
        count(distinct t.book)                                  as book_count,
        count(distinct t.product_type)                          as product_type_count,
        min(t.trade_date)                                       as earliest_trade_date,
        max(t.maturity_date)                                    as latest_maturity_date,
        max(t.source_batch_id)                                  as source_batch_id
    from trades t
    group by t.business_date, t.counterparty_id

)

select
    a.business_date,
    a.counterparty_id,
    c.legal_name,
    c.country_code,
    c.sector,
    c.parent_counterparty_id,
    c.is_active,

    a.trade_count,
    a.total_notional,
    a.total_mtm,
    a.positive_mtm,
    a.book_count,
    a.product_type_count,
    a.earliest_trade_date,
    a.latest_maturity_date,

    r.worst_rating_rank,
    case when r.is_investment_grade_flag = 1 then true
         when r.is_investment_grade_flag = 0 then false
         else null end                                          as is_investment_grade,

    {{ audit_columns('a.source_batch_id') }}

from aggregated a

-- LEFT JOIN, not INNER: a counterparty missing from the reference feed must
-- still appear in the exposure report with nulls, so the gap is visible.
-- An INNER JOIN would silently drop exposure, which is the more dangerous
-- failure in a risk report.
left join counterparties c
       on c.business_date    = a.business_date
      and c.counterparty_id  = a.counterparty_id
left join worst_rating r
       on r.business_date    = a.business_date
      and r.counterparty_id  = a.counterparty_id
