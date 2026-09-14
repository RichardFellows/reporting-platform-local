{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['cob_date'],
    tags=['reporting']
  )
}}

{#
  INSERT_OVERWRITE, NOT MERGE, AND NO unique_key. An incremental run
  REWRITES every COB date its select returns and leaves every other date
  alone. A merge never deletes, so a key a re-delivery dropped used to
  survive every run after the one that first wrote it.

  Correct only if each date the select returns, it returns WHOLE -- a
  partial date would be truncated to the part. Each date it returns
  is grouped from ALL of that date's rows in `counterparty_exposure`; the
  lookback window is the only filter.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}

{#
  Country-level rollup.

  ref()s counterparty_exposure rather than trade, deliberately. The
  aggregate must reconcile to the detail report by construction — if both
  derived from prepared independently, they would eventually disagree and
  someone would spend a week finding out why.
#}

select
    cob_date,
    coalesce(country_code, 'UNKNOWN')                as country_code,
    count(distinct counterparty_id)                  as counterparty_count,
    sum(trade_count)                                 as trade_count,
    sum(total_notional)                              as total_notional,
    sum(total_mtm)                                   as total_mtm,
    sum(positive_mtm)                                as positive_mtm,
    sum(case when is_investment_grade = false then total_mtm else 0 end)
                                                     as sub_investment_grade_mtm,
    count(distinct case when is_investment_grade = false then counterparty_id end)
                                                     as sub_investment_grade_counterparties,
    -- STRING, not VARCHAR: bare VARCHAR is rejected by Spark 3.x, which wants
    -- an explicit length. Same rule as audit_columns() in macros/engine.sql --
    -- not calling that macro here only because this model aggregates away the
    -- batch source it emits.
    cast('{{ invocation_id }}' as string)            as dbt_invocation_id,
    {{ dbt.current_timestamp() }}                    as dbt_updated_at

from {{ ref('counterparty_exposure') }}
where {{ incremental_window('cob_date') }}
group by cob_date, coalesce(country_code, 'UNKNOWN')
