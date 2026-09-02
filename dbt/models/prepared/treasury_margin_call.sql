{{ config(materialized='incremental',
          unique_key=['business_date', 'margin_call_id'],
          partition_by=['business_date'],
          tags=['prepared']) }}

with raw_rows as (
    select *, {{ dedupe_rank(['margin_call_id']) }} as _rn
    from {{ source('raw', 'treasury_margin_call') }}
    where {{ incremental_window('_business_date', 'business_date') }}
),
deduped as (select * from raw_rows where _rn = 1)

select
    _business_date                                as business_date,
    {{ clean_string('margin_call_id') }}          as margin_call_id,
    {{ clean_string('counterparty_id') }}         as counterparty_id,
    {{ safe_cast(clean_string('call_amount'), 'DECIMAL(18,2)') }} as call_amount,
    {{ clean_string('currency') }}                as currency,
    {{ parse_date(clean_string('call_date')) }}   as call_date,
    _source_file                                  as source_file,
    _file_version                                 as source_file_version,
    {{ audit_columns() }}
from deduped
