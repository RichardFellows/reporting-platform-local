{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'counterparty_id'],
    partition_by=['business_date'],
    tags=['prepared', 'reference']
  )
}}

{#
  Counterparty reference, conformed.

  Shared by every downstream report — this is the single conformed
  representation the legacy estate never had. Each of the legacy apps built
  its own counterparty view and they diverged; the whole point of putting this
  in prepared and referencing it with ref() is that divergence becomes visible
  in the lineage graph.
#}

with raw_rows as (

    select
        *,
        {{ dedupe_rank(['counterparty_id']) }} as _rn
    from {{ source('raw', 'counterparty') }}
    where {{ incremental_window('_business_date', 'business_date') }}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
        _business_date                                              as business_date,
        {{ clean_string('counterparty_id') }}                       as counterparty_id,
        {{ clean_string('legal_name') }}                            as legal_name,
        upper({{ clean_string('country_code') }})                   as country_code,
        {{ clean_string('sector') }}                                as sector,
        {{ clean_string('parent_counterparty_id') }}                as parent_counterparty_id,

        -- Upstream sends Y/N/1/0/true/false depending on the release.
        -- Normalise once, here, rather than in every consuming report.
        case
            when upper({{ clean_string('is_active') }}) in ('Y', 'YES', 'TRUE', '1') then true
            when upper({{ clean_string('is_active') }}) in ('N', 'NO', 'FALSE', '0') then false
            else null
        end                                                         as is_active,

        _source_file                                                as source_file,
        _file_version                                                as source_file_version,
        {{ audit_columns() }}

    from deduped

)

select * from cleaned
