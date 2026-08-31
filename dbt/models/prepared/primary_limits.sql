{{
  config(
    materialized='incremental',
    unique_key=['limit_id', 'effective_from'],
    partition_by=['effective_from_month'],
    tags=['prepared', 'reference', 'scd2']
  )
}}

{#
  Primary credit limits from GCIS2, conformed and typed.

  Nothing here is a report opinion: the amount becomes a number, the dates
  become dates, and the status strings gcis2 varies between releases are
  normalised once. Utilisation, headroom and breach flags belong in
  `reporting`, where they can be joined to exposure.

  SCD2, one row per limit VERSION. A limit is a standing object that gets
  restated, not a new record each day -- 5,680 rows expressed roughly 940
  versions. Consumers join point-in-time with as_of().

  THE `is_current` COLUMN IS GONE, and that needs explaining because it was
  load-bearing. It meant "this limit is in force on the business date it was
  delivered for", and it cannot survive this change for two reasons: it is a
  function of business_date, which an SCD2 row does not have, and the name now
  means something else entirely -- on every SCD2 table `is_current` marks the
  live VERSION of the record.

  Its reason for existing was right, though: "computed here so that every
  consumer asks the question the same way -- the alternative is each report
  writing its own BETWEEN, which is precisely how the legacy estate ended up
  with limits that disagreed between screens." So the definition survives as
  the `limit_in_force(alias, date)` macro in macros/engine.sql. It moved from
  a column to a call; it did not become each report's problem.

  TWO SEPARATE DATE RANGES NOW SIT ON THIS ROW and they are not the same thing:

    * effective_date / expiry_date -- when the LIMIT applies. A business fact
      the upstream sends, and what limit_in_force() reads.
    * effective_from / effective_to -- when this VERSION of the record was the
      one being reported. Platform bookkeeping, and what as_of() reads.

  A limit can be recorded (effective_from) long before it applies
  (effective_date). Conflating them is the obvious way to get this wrong.
#}

with

{% if is_incremental() %}
{{ scd2_incremental_scope(source('raw', 'primary_limits'), ['limit_id']) }}
{% endif %}

raw_rows as (

    select
        r.*,
        {{ dedupe_rank(['r.limit_id']) }} as _rn
    from {{ source('raw', 'primary_limits') }} r
    {% if is_incremental() %}
    join touched t on t.limit_id = r.limit_id
    left join replay_from p on p.limit_id = r.limit_id
    where r._business_date >= coalesce(p.from_date, date '1900-01-01')
    {% endif %}

),

deduped as (
    select * from raw_rows where _rn = 1
),

cleaned as (

    select
        _business_date                                      as business_date,
        {{ clean_string('limit_id') }}                      as limit_id,
        {{ clean_string('counterparty_id') }}               as counterparty_id,
        upper({{ clean_string('limit_type') }})             as limit_type,

        -- DECIMAL, not DOUBLE: these are monetary amounts that get summed and
        -- compared against exposure, and binary floating point makes those
        -- comparisons non-reproducible. TRY_CAST via safe_cast() so an
        -- unparseable amount lands as NULL and fails the not_null test below,
        -- rather than failing the load.
        {{ safe_cast(clean_string('limit_amount'), 'DECIMAL(18,2)') }}
                                                            as limit_amount,
        upper({{ clean_string('currency') }})               as currency,
        {{ parse_date(clean_string('effective_date')) }}    as effective_date,
        {{ parse_date(clean_string('expiry_date')) }}       as expiry_date,

        -- gcis2 sends the status in whatever case the release happens to use,
        -- and an open-ended limit arrives with an empty expiry rather than a
        -- far-future one. Normalise both once, here.
        upper({{ clean_string('status') }})                 as status,

        _source_file                                        as source_file,
        _file_version                                       as source_file_version,
        {{ audit_columns() }}

    from deduped

),

versioned as (

    select
        *,
        {{ scd2_hash(['counterparty_id', 'limit_type', 'limit_amount',
                      'currency', 'effective_date', 'expiry_date',
                      'status']) }}                           as _row_hash
    from cleaned

),

{{ scd2_changes('versioned', ['limit_id']) }}

ranged as (

    select
        limit_id,
        counterparty_id,
        limit_type,
        limit_amount,
        currency,
        effective_date,
        expiry_date,
        status,

        source_file,
        source_file_version,
        source_batch_id,
        dbt_invocation_id,
        nessie_ref,
        dbt_updated_at,

        {{ scd2_columns(['limit_id']) }}

    from kept

)

select * from ranged
