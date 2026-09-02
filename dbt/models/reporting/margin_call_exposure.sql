{{ config(materialized='incremental',
          unique_key=['business_date', 'margin_call_id'],
          partition_by=['business_date'],
          tags=['reporting']) }}

with dates as (
    select distinct business_date
    from {{ ref('treasury_margin_call') }}
    where {{ incremental_window('business_date') }}
)

select
    d.business_date,
    s.margin_call_id,
    s.counterparty_id,
    s.call_amount
from dates d
join {{ ref('treasury_margin_call_scd2') }} s
  on {{ as_of('s', 'd.business_date') }}
