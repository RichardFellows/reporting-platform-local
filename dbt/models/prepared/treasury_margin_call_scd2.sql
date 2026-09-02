{{ config(materialized='incremental',
          unique_key=['margin_call_id', 'effective_from'],
          partition_by=['effective_from_month'],
          tags=['prepared']) }}

with

{% if is_incremental() %}
{#
  date_column='business_date': the default is raw's `_business_date`, and this
  model reads the PREPARED table, where that name does not survive.
#}
{{ scd2_incremental_scope(ref('treasury_margin_call'), ['margin_call_id'],
                          date_column='business_date') }},
{% endif %}

source_rows as (

    select p.*
    from {{ ref('treasury_margin_call') }} p
    {#
      BOTH joins. `touched` is the entities that moved recently, including ones
      never seen before; `replay_from` is where each already-open version
      began. Filtering on replay_from alone would silently drop every NEW
      entity, because a new one has no open version to replay from.
    #}
    {% if is_incremental() %}
    join touched t on t.margin_call_id = p.margin_call_id
    left join replay_from r on r.margin_call_id = p.margin_call_id
    where p.business_date >= coalesce(r.from_date, date '1900-01-01')
    {% endif %}

),

versioned as (

    select
        *,
        {{ scd2_hash(['counterparty_id', 'call_amount', 'currency',
                      'call_date']) }} as _row_hash
    from source_rows

),

{{ scd2_changes('versioned', ['margin_call_id']) }}

select
    margin_call_id,
    counterparty_id,
    call_amount,
    currency,
    call_date,
    {{ scd2_columns(['margin_call_id']) }}
from kept
