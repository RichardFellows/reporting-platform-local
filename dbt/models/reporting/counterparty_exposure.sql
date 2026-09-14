{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['reporting', 'core']
  )
}}

{#
  INSERT_OVERWRITE, NOT MERGE, AND NO unique_key. An incremental run
  REWRITES every COB date its select returns and leaves every other date
  alone. A merge never deletes, so a key a re-delivery dropped used to
  survive every run after the one that first wrote it.

  Correct only if each date the select returns, it returns WHOLE -- a
  partial date would be truncated to the part. Each date it returns
  is aggregated from ALL of that date's unmatured trades in `fo_trade` --
  the lookback window and `is_matured` are the only row filters -- and every
  other CTE is a lookup joined onto those.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}

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
    from {{ ref('fo_trade') }}
    where {{ incremental_window('cob_date') }}
      and coalesce(is_matured, false) = false

),

counterparties as (
    select * from {{ ref('ref_counterparty') }}
),

{#
  WHICH COUNTERPARTIES WERE ACTUALLY DELIVERED ON EACH DATE.

  This reads `raw` from the reporting layer, which is unusual here and is the
  point: `prepared.ref_counterparty` is SCD2 now and deliberately holds no record
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

  DELIVERED MEANS IN THE NEWEST DELIVERY FOR THE DATE, AS KNOWN AT THE
  KNOWLEDGE TIME. This used to group every version of a date together, so a
  counterparty a re-delivery dropped still counted as delivered and was never
  flagged -- the same per-key reading `dedupe_rank` had, reached without
  calling it. Hence the macro and `known_as_of()` here, in the one reporting
  model that reads raw.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}
delivered as (

    select
        _cob_date                               as cob_date,
        {{ clean_string('counterparty_id') }}   as counterparty_id
    from (
        select
            *,
            {{ dedupe_rank(['counterparty_id']) }} as _rn
        from {{ source('raw', 'ref_counterparty') }}
        where {{ known_as_of() }}
    ) newest
    where _rn = 1
    group by 1, 2

),

-- One rating per counterparty per date: the most conservative (highest rank)
-- across agencies. Documented here because it is a business rule, not a
-- technical one, and report owners need to be able to find it.
--
-- `rating` is SCD2 now, so there is no cob_date to group by: the dates
-- come from the exposure side and the ratings are joined point-in-time onto
-- them. `dates` is the set of (cob_date, counterparty_id) pairs the
-- report is being built for, taken from the trades themselves so that a
-- counterparty with no trades on a date contributes no rating row -- which is
-- what the old equality join did implicitly.
dates as (
    select distinct cob_date, counterparty_id from trades
),

worst_rating as (

    select
        d.cob_date,
        d.counterparty_id,
        max(r.rating_rank)                                      as worst_rating_rank,
        min(case when r.grade_band = 'SUB_INVESTMENT_GRADE' then 0 else 1 end) as is_investment_grade_flag,
        -- How recent the ratings behind this row are. NOT flagged as
        -- "carried forward" the way counterparty is: rating is a WEEKLY feed
        -- (cadence: weekly in feeds.yml), so a COB date with no delivery
        -- is the design rather than a gap, and flagging it would be noise on
        -- three days in four. A date is still useful -- it distinguishes a
        -- rating set last week from one set two years ago.
        max(r.effective_from)                                   as rating_as_of
    from dates d
    join {{ ref('ref_rating') }} r
      on r.counterparty_id = d.counterparty_id
     and {{ as_of('r', 'd.cob_date') }}
    group by d.cob_date, d.counterparty_id

),

aggregated as (

    select
        t.cob_date,
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
    group by t.cob_date, t.counterparty_id

)

select
    a.cob_date,
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
    r.rating_as_of,
    case when r.is_investment_grade_flag = 1 then true
         when r.is_investment_grade_flag = 0 then false
         else null end                                          as is_investment_grade,

    {{ audit_columns('a.source_batch_id') }}

from aggregated a

-- LEFT JOIN, not INNER: a counterparty missing from the reference feed must
-- still appear in the exposure report. An INNER JOIN would silently drop
-- exposure, which is the more dangerous failure in a risk report.
-- The last good version IS carried forward under SCD2, and
-- `reference_carried_forward` above is what keeps that gap visible.
-- POINT-IN-TIME, not equality: `counterparty` is SCD2 now, holding one row
-- per version rather than one per COB date. as_of() expands to a
-- `between effective_from and effective_to` predicate; effective_to is 9999-12-31 on the
-- open version, so the current row matches every date at or after its
-- effective_from with no null-handling branch here.
left join counterparties c
       on c.counterparty_id  = a.counterparty_id
      and {{ as_of('c', 'a.cob_date') }}
left join delivered d
       on d.counterparty_id  = a.counterparty_id
      and d.cob_date    = a.cob_date
left join worst_rating r
       on r.cob_date    = a.cob_date
      and r.counterparty_id  = a.counterparty_id
