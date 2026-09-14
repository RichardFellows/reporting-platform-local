{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    scd2_retractions=true,
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

  ONE ROW PER VERSION, NOT PER COB DATE. This table held 2,400 rows to
  express 70 versions: 60 counterparties that changed 10 times between them
  across 40 retained COB dates, restated in full every day. Consumers
  join point-in-time with `as_of()` instead of on equality.

  (70, not the 68 distinct attribute tuples the table contains: two names
  reverted to an earlier value, which is a third version rather than a
  duplicate of the first. `count(distinct ...)` is a lower bound on an SCD2
  row count, never the answer.)

  Two things this deliberately does NOT change:

    * `raw` and `landing` still hold every delivery, 1:1 with what arrived.
      This is where the provenance of a RESTATEMENT now lives -- see the
      lineage note in docs/ARCHITECTURE.md. The audit chain moved, it did not
      shorten.
    * The table keeps its NAME. `managed_tables()` in common/context.py,
      managed_tables(), retention and maintenance are all keyed by name, and
      renaming this to `counterparty_history` would have meant touching every
      one of them to express nothing.

  The unique key is (counterparty_id, effective_from) and NOT effective_to,
  which is what lets the incremental merge UPDATE a previously-open row to
  close it rather than inserting a second one alongside.

  MERGE, NOT insert_overwrite, unlike every date-partitioned model. The
  partition is `effective_from_month` and an incremental run re-derives only
  the touched keys, so overwriting the months it returns would truncate
  them to those keys.

  A KEY ABSENT FROM A DATE'S NEWEST DELIVERY IS TWO DIFFERENT THINGS, and
  they are treated differently:

    * It does not CLOSE the version in force. An absent counterparty is a
      gap to show, not a retirement to infer: the version carries forward
      and `counterparty_exposure` flags it (`reference_carried_forward`).
    * It does RETRACT a version that the replaced delivery itself began. If
      the 09-02 delivery changed a name and its re-delivery omits the
      counterparty, the change never happened: the 09-02 version goes and
      the one before it is open again. `scd2_retractions` below emits the
      marker rows and `scd2_retractions=true` gives this model the merge in
      macros/merge.sql that deletes on them. Without both, the retracted
      version stays current, and the key's next change opens a second one.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}

{% set business_columns = ['counterparty_id', 'legal_name', 'country_code', 'sector', 'parent_counterparty_id', 'is_active'] %}

with

{{ newest_file_version(source('raw', 'ref_counterparty')) }}

raw_rows as (

    {#
      EVERY raw row, ranked. The replay scope is applied after cleaning
      (`scd2_replay`), so that its keys are compared to the target's CLEANED
      keys. `nv` decides the newest delivery per COB date from raw unjoined;
      see `newest_version` on dedupe_rank. The retraction guard reads the
      same CTE.

      known_as_of() applies on the FULL-REFRESH path, which is the only path
      an as-of build is allowed to take (the macro refuses an incremental
      one). It compiles to `1 = 1` when no knowledge_time is set.
    #}
    select
        r.*,
        {{ dedupe_rank(['r.counterparty_id'],
                       newest_version='nv._newest_file_version') }} as _rn
    from {{ source('raw', 'ref_counterparty') }} r
    join newest_file_version nv on nv._newest_cob_date = r._cob_date
    where {{ known_as_of() }}

),

cleaned as (

    select
        _cob_date                                                   as cob_date,
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
        {{ source_provenance() }}
        {{ audit_columns() }},
        _rn

    from raw_rows

),

{{ scd2_replay('cleaned', ['counterparty_id'], business_columns) }}

{#
  Business attributes only -- see the scd2_hash macro for what including an
  audit column would do.
#}
versioned as (

    select
        *,
        {{ scd2_hash(['legal_name', 'country_code', 'sector',
                      'parent_counterparty_id', 'is_active']) }}    as _row_hash
    from replayed

),

{{ scd2_changes('versioned', ['counterparty_id']) }}

ranged as (

    {#
      `source_file` is the delivery on which this value FIRST appeared, not
      the most recent one to repeat it. That is the more useful question,
      and the restatements are still in raw.
    #}
    select
        {%- for c in scd2_output_columns(business_columns) %}
        {{ ident(c) }},
        {%- endfor %}
        {{ scd2_columns(['counterparty_id']) }}

    from kept

)

{#
  On an incremental run, plus one marker row per version this replay
  covers and no longer derives -- a change a re-delivery dropped or
  reverted. The merge deletes those (macros/merge.sql); without them it
  would leave the retracted version current.
#}
select * from ranged
{{ scd2_retractions('ranged', ['counterparty_id'], business_columns) }}
