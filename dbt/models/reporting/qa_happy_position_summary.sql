{{ config(materialized='incremental', incremental_strategy='insert_overwrite',
          partition_by=['cob_date'], tags=['reporting', 'qa']) }}

-- Dummy reporting consumer used to verify feed onboarding end to end.
select
    cob_date,
    currency,
    count(*) as position_count,
    sum(amount) as total_amount,
    sum(case when is_active then 1 else 0 end) as active_position_count
from {{ ref('qa_happy_position') }}
where {{ incremental_window('cob_date') }}
group by cob_date, currency
