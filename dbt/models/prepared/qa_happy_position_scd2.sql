{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    scd2_retractions=true,
    unique_key=['position_id', 'effective_from'],
    partition_by=['effective_from_month'],
    tags=['prepared', 'reference', 'scd2']
  )
}}

{{ scd2_prepared(
    source_name='qa_happy_position',
    keys=['position_id'],
    cleaning={
        'position_id':    clean_string('position_id'),
        'desk_code':      'upper(' ~ clean_string('desk_code') ~ ')',
        'amount':         safe_cast(clean_string('amount'), 'DECIMAL(18,2)'),
        'currency':       'upper(' ~ clean_string('currency') ~ ')',
        'effective_date': parse_date(clean_string('effective_date')),
        'is_active':      yes_no_flag(clean_string('is_active')),
        'description':    clean_string('description'),
    },
    hashed=['desk_code', 'amount', 'currency', 'effective_date', 'is_active', 'description'],
) }}
