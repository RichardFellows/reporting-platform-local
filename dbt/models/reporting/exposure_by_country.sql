{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'country_code'],
    partition_by=['business_date'],
    tags=['reporting']
  )
}}

{#
  Country-level rollup.

  ref()s counterparty_exposure rather than trade, deliberately. The
  aggregate must reconcile to the detail report by construction — if both
  derived from prepared independently, they would eventually disagree and
  someone would spend a week finding out why.
#}

select
    business_date,
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
where {{ incremental_window('business_date') }}
group by business_date, coalesce(country_code, 'UNKNOWN')
