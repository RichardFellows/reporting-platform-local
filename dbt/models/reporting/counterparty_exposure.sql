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

{#
  WHICH COUNTERPARTIES WERE ACTUALLY DELIVERED ON EACH DATE.

  This reads `raw` from the reporting layer, which is unusual here and is the
  point: `prepared.counterparty` is SCD2 now and deliberately holds no record
  of a delivery that restated an unchanged value. The delivery record still
  exists exactly once, in raw, and copying it into prepared to avoid this join
  would rebuild the 2,400-row table SCD2 just removed.

  It exists because SCD2 silently HEALS a missing delivery: a version's range
  spans the gap, so the point-in-time join finds the counterparty on a day its
  feed never arrived. That is what README:186 and ARCHITECTURE:153 promise
  ("carries forward the last good version"), and it is the opposite of what
  the LEFT JOIN comment below used to promise. Carrying forward silently is
  the part nobody wants -- so it is carried forward and FLAGGED.

  Narrow and grouped: two columns, no attributes, so this is a cheap scan.
#}
delivered as (

    select
        _business_date                          as business_date,
        {{ clean_string('counterparty_id') }}   as counterparty_id
    from {{ source('raw', 'counterparty') }}
    group by 1, 2

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

    {#
      The gap, as data rather than as absence. NULL attributes used to be the
      only signal that a reference feed had not arrived; this says so
      explicitly, and `reference_effective_from` says how old the value in force
      is, which a NULL could never express.
    #}
    (d.counterparty_id is null)                             as reference_carried_forward,
    c.effective_from                                            as reference_effective_from,

    r.worst_rating_rank,
    case when r.is_investment_grade_flag = 1 then true
         when r.is_investment_grade_flag = 0 then false
         else null end                                          as is_investment_grade,

    {{ audit_columns('a.source_batch_id') }}

from aggregated a

-- LEFT JOIN, not INNER: a counterparty missing from the reference feed must
-- still appear in the exposure report. An INNER JOIN would silently drop
-- exposure, which is the more dangerous failure in a risk report.
--
-- This used to say "with nulls, so the gap is visible", which was true of the
-- snapshot and contradicted README:186 and ARCHITECTURE:153, both of which
-- promise the last good version is carried forward. Under SCD2 it IS carried
-- forward -- and `reference_carried_forward` above is what keeps the gap
-- visible, more usefully than a NULL did.
-- POINT-IN-TIME, not equality: `counterparty` is SCD2 now, holding one row
-- per version rather than one per business date. as_of() expands to a
-- `between effective_from and effective_to` predicate; effective_to is 9999-12-31 on the
-- open version, so the current row matches every date at or after its
-- effective_from with no null-handling branch here.
left join counterparties c
       on c.counterparty_id  = a.counterparty_id
      and {{ as_of('c', 'a.business_date') }}
left join delivered d
       on d.counterparty_id  = a.counterparty_id
      and d.business_date    = a.business_date
left join worst_rating r
       on r.business_date    = a.business_date
      and r.counterparty_id  = a.counterparty_id
