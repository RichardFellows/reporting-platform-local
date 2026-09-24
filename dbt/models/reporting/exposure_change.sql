{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['reporting', 'change-detection']
  )
}}

{#
  INSERT_OVERWRITE, NOT MERGE, AND NO unique_key. An incremental run
  REWRITES every COB date its select returns and leaves every other date
  alone. A merge never deletes, so a key a re-delivery dropped used to
  survive every run after the one that first wrote it.

  Correct only if each date the select returns, it returns WHOLE -- a
  partial date would be truncated to the part. Each date it returns
  carries ALL of that date's rows in `counterparty_exposure`: the lookback
  window is the only filter, and `date_sequence` is an inner join that every
  one of those dates satisfies.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}

{#
  Day-on-day change in counterparty exposure.

  This is the model that justifies the whole architecture. The reporting
  emphasis across the estate is on highlighting CHANGE over time, and on
  the legacy RDBMS that meant either keeping wide history in the reporting schema or
  running comparisons against an archive. Here it is a self-join over the
  retained COB dates, on data that is already partitioned by date.

  It also demonstrates why the retention rule matters operationally: with
  10 business days retained, day-on-day comparison always works; month-on-month
  comparison only works because the 80 month-end dates are also retained. If
  someone "simplifies" retention to a rolling 10 days, this model silently
  starts returning nulls for the month-end comparison rather than failing.
  Hence the not_null test with severity: warn on prior_month_end_mtm.
#}

with current_exposure as (

    select *
    from {{ ref('counterparty_exposure') }}
    where {{ incremental_window('cob_date') }}

),

-- Rank the retained dates so "previous COB date" means the previous
-- date WE HAVE, not calendar yesterday. Matches the retention semantics.
date_sequence as (

    select
        cob_date,
        lag(cob_date) over (order by cob_date) as prior_cob_date
    from (
        select distinct cob_date from {{ ref('counterparty_exposure') }}
    ) d

),

month_end_dates as (

    select
        cob_date,
        row_number() over (
            partition by extract(year from cob_date), extract(month from cob_date)
            order by cob_date desc
        ) as rn
    from (select distinct cob_date from {{ ref('counterparty_exposure') }}) m

),

prior_month_end as (

    select
        c.cob_date,
        max(m.cob_date) as prior_month_end_date
    from (select distinct cob_date from {{ ref('counterparty_exposure') }}) c
    left join month_end_dates m
           on m.rn = 1
          and m.cob_date < c.cob_date
    group by c.cob_date

),

-- EVERY (date, counterparty) PAIR THE REPORT COVERS: today's counterparties,
-- plus the previous date's that are ABSENT today. The model used to be driven
-- from today's rows alone, so a counterparty that disappeared produced no row
-- at all and `REMOVED` could never be assigned -- the most interesting row
-- in a change report, silently omitted, tests green (todo 16).
--
-- REMOVED means ABSENT ENTIRELY: exposure on the previous date and no row in
-- today's `counterparty_exposure`. That table is aggregated from trades, so a
-- counterparty with no live trades has no row and IS removed; one whose
-- reference data was carried forward (`reference_carried_forward`) still has
-- trades, still has a row, and is not. Decided with the report's owner.
--
-- A removed counterparty's row belongs to TODAY's partition -- this model is
-- insert_overwrite per cob_date, and the missing keys are taken only for the
-- dates `current_exposure` returns, so each date is still returned whole.
keys as (

    select cob_date, counterparty_id
    from current_exposure

    union all

    select ds.cob_date, prev.counterparty_id
    from (select distinct cob_date from current_exposure) w
    join date_sequence ds
      on ds.cob_date = w.cob_date
    join {{ ref('counterparty_exposure') }} prev
      on prev.cob_date = ds.prior_cob_date
    where not exists (
        select 1 from current_exposure cur
        where cur.cob_date = ds.cob_date
          and cur.counterparty_id = prev.counterparty_id
    )

)

select
    k.cob_date,
    k.counterparty_id,
    coalesce(cur.legal_name, prev.legal_name)                as legal_name,
    coalesce(cur.country_code, prev.country_code)            as country_code,
    coalesce(cur.sector, prev.sector)                        as sector,

    -- A removed counterparty has no current row: its current values are
    -- NULL (there is nothing to show), and its CHANGE is the whole of what it
    -- had -- current treated as zero. A present counterparty keeps the
    -- original arithmetic, so a null current total still yields a null change.
    cur.total_mtm                                            as current_mtm,
    prev.total_mtm                                           as prior_mtm,
    {{ removed_as_zero('cur', 'total_mtm') }} - coalesce(prev.total_mtm, 0)
                                                             as mtm_change,
    case
        when prev.total_mtm is null or prev.total_mtm = 0 then null
        else ({{ removed_as_zero('cur', 'total_mtm') }} - prev.total_mtm) / abs(prev.total_mtm)
    end                                                      as mtm_change_pct,

    cur.total_notional                                       as current_notional,
    prev.total_notional                                      as prior_notional,
    {{ removed_as_zero('cur', 'total_notional') }} - coalesce(prev.total_notional, 0)
                                                             as notional_change,

    cur.trade_count                                          as current_trade_count,
    prev.trade_count                                         as prior_trade_count,
    {{ removed_as_zero('cur', 'trade_count') }} - coalesce(prev.trade_count, 0)
                                                             as trade_count_change,

    me.total_mtm                                             as prior_month_end_mtm,
    {{ removed_as_zero('cur', 'total_mtm') }} - coalesce(me.total_mtm, 0)
                                                             as mtm_change_since_month_end,

    -- Classification the report actually presents. Thresholds are business
    -- rules and belong here, in the mart, not in the BI tool — otherwise they
    -- change when the BI tool changes.
    --
    -- CHANGED is `is distinct from`, not `abs(diff) > 0`: the two agree for
    -- two known values, and differ where it matters -- an MTM that became
    -- unknown (every trade's mtm_value null) is a change, not UNCHANGED.
    case
        when prev.counterparty_id is null then 'NEW'
        when cur.counterparty_id is null then 'REMOVED'
        when abs(cur.total_mtm - coalesce(prev.total_mtm, 0)) > 1000000 then 'MATERIAL_CHANGE'
        when cur.total_mtm is distinct from prev.total_mtm then 'CHANGED'
        else 'UNCHANGED'
    end                                                      as change_category,

    -- STRING, not VARCHAR: see the note in audit_columns() (macros/engine.sql).
    cast('{{ invocation_id }}' as string)                    as dbt_invocation_id,
    {{ dbt.current_timestamp() }}                            as dbt_updated_at

from keys k

join date_sequence ds
  on ds.cob_date = k.cob_date

left join current_exposure cur
       on cur.cob_date = k.cob_date
      and cur.counterparty_id = k.counterparty_id

left join {{ ref('counterparty_exposure') }} prev
       on prev.cob_date   = ds.prior_cob_date
      and prev.counterparty_id = k.counterparty_id

left join prior_month_end pme
       on pme.cob_date = k.cob_date

left join {{ ref('counterparty_exposure') }} me
       on me.cob_date   = pme.prior_month_end_date
      and me.counterparty_id = k.counterparty_id
