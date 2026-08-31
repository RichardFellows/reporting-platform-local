{{
  config(
    materialized='incremental',
    unique_key=['counterparty_id', 'effective_from'],
    partition_by=['effective_from_month'],
    tags=['prepared', 'reference', 'scd2']
  )
}}

{#
  Counterparty reference, conformed — SCD2.

  Shared by every downstream report — this is the single conformed
  representation the legacy estate never had. Each of the legacy apps built
  its own counterparty view and they diverged; the whole point of putting this
  in prepared and referencing it with ref() is that divergence becomes visible
  in the lineage graph.

  ONE ROW PER VERSION, NOT PER BUSINESS DATE. This table held 2,400 rows
  expressing 68 distinct attribute versions: 60 counterparties that changed 8
  times between them across 40 retained business dates, restated in full every
  day. Consumers join point-in-time with `as_of()` instead of on equality.

  Two things this deliberately does NOT change:

    * `raw` and `landing` still hold every delivery, 1:1 with what arrived.
      This is where the provenance of a RESTATEMENT now lives -- see the
      lineage note in docs/ARCHITECTURE.md. The audit chain moved, it did not
      shorten.
    * The table keeps its NAME. `PREPARED_TABLES` in common/context.py,
      managed_tables(), retention and maintenance are all keyed by name, and
      renaming this to `counterparty_history` would have meant touching every
      one of them to express nothing.

  The unique key is (counterparty_id, effective_from) and NOT effective_to,
  which is what lets the incremental merge UPDATE a previously-open row to
  close it rather than inserting a second one alongside.
#}

with

{% if is_incremental() %}

{#
  Which entities moved in this run's window. On a first build this whole CTE
  is absent and every entity is replayed.
#}
touched as (

    select distinct counterparty_id
    from {{ source('raw', 'counterparty') }}
    where _business_date >= (
        select coalesce(max(_inc.effective_from), date '1900-01-01')
               - interval {{ var('lookback_days', 3) }} day
        from {{ this }} as _inc
    )

),

{#
  THE CRUX OF THIS MODEL.

  A touched entity's currently-open version may have begun months before the
  lookback window, and the whole of it has to be re-derived so that lead() can
  see the new value and CLOSE it. Replaying only the last few business dates
  would append a new version and leave the previous one still claiming
  effective_to = 9999-12-31 -- two versions in force at once, and `as_of()`
  would then match both and silently DOUBLE every exposure row for it.

  Nothing inside this model can detect that state. The
  dbt_utils.mutually_exclusive_ranges test in _prepared.yml is what catches it,
  which is why that test is not optional here.

  Closed versions are deliberately NOT replayed: they start before from_date,
  so they are never rewritten and their source_file / dbt_invocation_id stay
  frozen at the build that created them.
#}
replay_from as (

    select counterparty_id, min(effective_from) as from_date
    from {{ this }}
    where is_current
    group by counterparty_id

),

{% endif %}

raw_rows as (

    select
        r.*,
        {{ dedupe_rank(['r.counterparty_id']) }} as _rn
    from {{ source('raw', 'counterparty') }} r
    {% if is_incremental() %}
    join touched t on t.counterparty_id = r.counterparty_id
    left join replay_from p on p.counterparty_id = r.counterparty_id
    where r._business_date >= coalesce(p.from_date, date '1900-01-01')
    {% endif %}

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
        _file_version                                               as source_file_version,
        {{ audit_columns() }}

    from deduped

),

{#
  Business attributes only -- see the scd2_hash macro for what including an
  audit column would do.
#}
versioned as (

    select
        *,
        {{ scd2_hash(['legal_name', 'country_code', 'sector',
                      'parent_counterparty_id', 'is_active']) }}    as _row_hash
    from cleaned

),

changes as (

    select
        *,
        lag(_row_hash) over (partition by counterparty_id
                             order by business_date)                as _prev_hash
    from versioned

),

{#
  One row per CHANGE. A delivery that restates an unchanged counterparty
  produces nothing here, which is the entire point.
#}
kept as (
    select * from changes
    where _prev_hash is null or _prev_hash <> _row_hash
),

ranged as (

    select
        counterparty_id,
        legal_name,
        country_code,
        sector,
        parent_counterparty_id,
        is_active,

        {#
          The delivery on which this value FIRST appeared, not the most recent
          one to repeat it. That is the more useful question, and the
          restatements are still in raw.
        #}
        source_file,
        source_file_version,
        source_batch_id,
        dbt_invocation_id,
        nessie_ref,
        dbt_updated_at,

        business_date                                               as effective_from,
        {{ scd2_effective_to('business_date', ['counterparty_id']) }}
                                                                    as effective_to,
        lead(business_date) over (partition by counterparty_id
                                  order by business_date) is null    as is_current,

        {#
          business_date no longer exists as a column, so it cannot be the
          partition column any more. Retention deletes by a range predicate
          against this instead of dropping a partition -- the one genuinely
          invasive consequence of this change. See docs/RETENTION.md.
        #}
        trunc(business_date, 'MM')                                  as effective_from_month

    from kept

)

select * from ranged
