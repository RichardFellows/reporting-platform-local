{{
  config(
    materialized='incremental',
    unique_key=['cob_date', 'counterparty_id'],
    partition_by=['cob_date'],
    tags=['reporting', 'change-detection']
  )
}}

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

)

select
    cur.cob_date,
    cur.counterparty_id,
    cur.legal_name,
    cur.country_code,
    cur.sector,

    cur.total_mtm                                            as current_mtm,
    prev.total_mtm                                           as prior_mtm,
    cur.total_mtm - coalesce(prev.total_mtm, 0)              as mtm_change,
    case
        when prev.total_mtm is null or prev.total_mtm = 0 then null
        else (cur.total_mtm - prev.total_mtm) / abs(prev.total_mtm)
    end                                                      as mtm_change_pct,

    cur.total_notional                                       as current_notional,
    prev.total_notional                                      as prior_notional,
    cur.total_notional - coalesce(prev.total_notional, 0)    as notional_change,

    cur.trade_count                                          as current_trade_count,
    prev.trade_count                                         as prior_trade_count,
    cur.trade_count - coalesce(prev.trade_count, 0)          as trade_count_change,

    me.total_mtm                                             as prior_month_end_mtm,
    cur.total_mtm - coalesce(me.total_mtm, 0)                as mtm_change_since_month_end,

    -- Classification the report actually presents. Thresholds are business
    -- rules and belong here, in the mart, not in the BI tool — otherwise they
    -- change when the BI tool changes.
    case
        when prev.counterparty_id is null then 'NEW'
        when cur.total_mtm is null then 'REMOVED'
        when abs(cur.total_mtm - coalesce(prev.total_mtm, 0)) > 1000000 then 'MATERIAL_CHANGE'
        when abs(cur.total_mtm - coalesce(prev.total_mtm, 0)) > 0 then 'CHANGED'
        else 'UNCHANGED'
    end                                                      as change_category,

    -- STRING, not VARCHAR: see the note in audit_columns() (macros/engine.sql).
    cast('{{ invocation_id }}' as string)                    as dbt_invocation_id,
    {{ dbt.current_timestamp() }}                            as dbt_updated_at

from current_exposure cur

join date_sequence ds
  on ds.cob_date = cur.cob_date

left join {{ ref('counterparty_exposure') }} prev
       on prev.cob_date   = ds.prior_cob_date
      and prev.counterparty_id = cur.counterparty_id

left join prior_month_end pme
       on pme.cob_date = cur.cob_date

left join {{ ref('counterparty_exposure') }} me
       on me.cob_date   = pme.prior_month_end_date
      and me.counterparty_id = cur.counterparty_id
