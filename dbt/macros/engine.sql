{#
  Shared SQL constructs for the Spark/Iceberg build.

  THIS FILE USED TO BE A PORTABILITY LAYER, carrying a DuckDB branch beside
  every Spark one and claiming "the project's tests must pass on both
  targets". Session 5 established that DuckDB cannot be a second build engine
  here on three independent counts -- it cannot address a Nessie branch (so no
  write-audit-publish), it silently drops `partition_by` (so it cannot
  reproduce the partition spec retention depends on), and it cannot INSERT to
  a partitioned table without an explicit override.

  So the DuckDB branches were dead code carrying a promise nothing tested, and
  an untested promise in this repo is a liability rather than optionality.
  They are gone. **Spark is the build engine.** DuckDB remains a reader
  against published `main` for analysts and for `dbt show`, which compiles
  these macros but materialises nothing.

  WHAT THIS FILE IS STILL FOR, and it is the more important half: keeping
  engine-specific constructs in ONE place. That was never really about having
  two engines. Bug #8 put a bare `CAST(x AS VARCHAR)` in `audit_columns()`
  which Spark rejects outright, and all three reporting models were found to have
  had copy-pasted their own inline version, so fixing the macro never reached
  them. The centralisation is what stops that; the second engine was never
  what made it worth doing.
#}

{# Safe cast that yields NULL rather than erroring on bad input.
   Raw is all strings by design, so every prepared model needs this. #}
{% macro safe_cast(col, type) %}
  TRY_CAST({{ col }} AS {{ type }})
{% endmacro %}


{# Trim and null-normalise a raw string column. Upstream CSVs use a mix of
   '', ' ', 'NULL' and 'N/A' for absent values; normalise once, here. #}
{% macro clean_string(col) %}
  NULLIF(NULLIF(NULLIF(TRIM({{ col }}), ''), 'NULL'), 'N/A')
{% endmacro %}


{# Parse a date held as text. Feeds deliver yyyyMMdd or yyyy-MM-dd.

   Spark's TO_DATE returns NULL on a pattern that does not match, so the
   COALESCE picks whichever format the feed used. #}
{% macro parse_date(col) %}
  COALESCE(TO_DATE({{ col }}, 'yyyy-MM-dd'), TO_DATE({{ col }}, 'yyyyMMdd'))
{% endmacro %}


{# The incremental predicate every prepared/reporting model shares.

   Reprocesses a trailing window rather than only the newest date, so a
   late-arriving correction for an earlier date is picked up without a full
   rebuild. lookback_days is set in dbt_project.yml. #}
{% macro incremental_window(date_column='business_date', target_column=None) %}
  {%- if is_incremental() -%}
    {#
      The alias is load-bearing. Without it the unqualified column inside the
      aggregate is ambiguous -- it exists in both the outer query's source and
      in `this` -- and Spark binds it to the OUTER table, turning this into a
      correlated subquery and failing the build:

        [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE]
        Expressions referencing the outer query are not supported outside of
        WHERE/HAVING clauses

      The two column names are also NOT the same on both sides, which is why
      target_column exists. The outer query filters the SOURCE column -- raw
      carries `_business_date` -- while `this` is the prepared table, whose
      modelled column is `business_date`. Assuming one name for both is what
      made the unqualified version bind to the outer table in the first place.
      The reporting layer happens to have matching names on both sides, so it
      never showed the problem.

      This went unnoticed for a long time because it only runs on the
      INCREMENTAL path: every earlier build was against a branch where the
      prepared tables did not exist yet, so is_incremental() was false and the
      `1 = 1` branch ran instead. The first build after publishing to main --
      i.e. the first steady-state run, which is what production does every day
      -- is what exposed it.
    #}
    {%- set tgt = target_column or date_column -%}
    {{ date_column }} >= (
      SELECT COALESCE(MAX(_inc.{{ tgt }}), DATE '1900-01-01')
             - INTERVAL {{ var('lookback_days', 3) }} DAY
      FROM {{ this }} AS _inc
    )
  {%- else -%}
    1 = 1
  {%- endif -%}
{% endmacro %}


{# Standard audit columns on every prepared/reporting model.
   Lineage back to the exact ingest batch is what makes a published figure
   explainable six months later. #}
{% macro audit_columns(batch_source='_batch_id') %}
  {{ batch_source }}                          AS source_batch_id,
  -- STRING, not VARCHAR: Spark 3.x requires an explicit length on
  -- CHAR/VARCHAR and rejects the bare form with DATATYPE_MISSING_SIZE. That
  -- was a real defect here, and the same cast had been copy-pasted into three
  -- reporting models where fixing this macro could not reach it.
  CAST('{{ invocation_id }}' AS STRING)       AS dbt_invocation_id,
  CAST('{{ var("nessie_ref", "main") }}' AS STRING)  AS nessie_ref,
  {{ dbt.current_timestamp() }}               AS dbt_updated_at
{% endmacro %}


{# Latest file version, and deduplication within it.

   Re-deliveries land as a new _file_version rather than overwriting, so every
   prepared model must select the newest version for each business date. And
   within a version, upstream occasionally repeats a business key; we take the
   last occurrence in file order, which matches the legacy ETL tool's behaviour.

   Centralised here so the rule cannot drift between models — this is exactly
   the kind of logic that was copy-pasted across legacy stored procedures and
   then diverged. #}
{% macro dedupe_rank(partition_keys) %}
  ROW_NUMBER() OVER (
    PARTITION BY _business_date, {{ partition_keys | join(', ') }}
    ORDER BY _file_version DESC, _row_number DESC
  )
{% endmacro %}

{#
  ---------------------------------------------------------------- SCD2
  Slowly-changing-dimension helpers. Used by the prepared reference models
  that store one row per VERSION rather than one row per business date, and
  by the reporting models that join to them point-in-time.

  See docs/ARCHITECTURE.md for why only reference tables are shaped this way:
  `trade` measured 9.7% redundancy against `counterparty`'s 97%, so versioning
  a transaction table costs complexity and saves nothing.
#}

{% macro as_of(alias, business_date_expr) %}
  {#
    Point-in-time join predicate against an SCD2 table.

    `effective_to` is DATE '9999-12-31' on the open version rather than NULL, so
    this needs no `or effective_to is null` branch -- which every consumer would
    otherwise have to remember, and which is silently wrong when forgotten
    (the current version simply stops matching and exposure loses its
    reference data).
  #}
  {{ business_date_expr }} between {{ alias }}.effective_from and {{ alias }}.effective_to
{% endmacro %}


{% macro scd2_hash(columns) %}
  {#
    The change detector: a hash over the BUSINESS attributes only.

    A macro rather than an inline expression specifically so the column list is
    a deliberate argument at the call site. Include `_source_file`, `_batch_id`,
    `dbt_invocation_id` or `dbt_updated_at` -- all of which sit right beside
    the business columns in these models -- and the hash changes on every
    delivery and every build, minting a new version daily and rebuilding the
    exact duplication the model exists to remove. It would look like it was
    working.

    coalesce to '' so a NULL is a value rather than poisoning the whole hash,
    and cast everything so booleans and dates compare stably.
  #}
  sha2(concat_ws('||'
    {%- for c in columns %},
    coalesce(cast({{ c }} as string), '')
    {%- endfor %}), 256)
{% endmacro %}


{% macro scd2_effective_to(order_column, partition_columns) %}
  {#
    Close each version at the day before the next one starts.

    `date_sub`, NOT `- INTERVAL 1 DAY`: the interval form returns a TIMESTAMP
    in Spark, and the column has to stay a DATE or `as_of()` compares a date to
    a timestamp on every joined row.
  #}
  coalesce(
    date_sub(lead({{ order_column }}) over (
      partition by {{ partition_columns | join(', ') }}
      order by {{ order_column }}), 1),
    DATE '9999-12-31')
{% endmacro %}

{% macro scd2_incremental_scope(source_relation, key_columns) %}
  {#
    The two CTEs every SCD2 model needs on its incremental path, so the logic
    exists once rather than once per reference table.

    `replay_from` IS THE LOAD-BEARING HALF. A touched entity's currently-open
    version can have begun months or years before the lookback window, and the
    whole of it must be re-derived for lead() to see the new value and CLOSE
    it. Replaying only the last few business dates appends a new version and
    leaves the previous one still claiming effective_to = 9999-12-31 -- two
    versions in force at once, which as_of() then matches BOTH of, silently
    doubling every joined row. The mutually_exclusive_ranges test is what
    catches that, and is not optional on any table using this.
  #}
  {%- set keys = key_columns | join(', ') -%}
  touched as (

      select distinct {{ keys }}
      from {{ source_relation }}
      where _business_date >= (
          select coalesce(max(_inc.effective_from), date '1900-01-01')
                 - interval {{ var('lookback_days', 3) }} day
          from {{ this }} as _inc
      )

  ),

  replay_from as (

      select {{ keys }}, min(effective_from) as from_date
      from {{ this }}
      where is_current
      group by {{ keys }}

  ),
{% endmacro %}


{% macro scd2_changes(source_cte, key_columns) %}
  {#
    Collapse a per-business-date stream into one row per CHANGE. A delivery
    that restates an unchanged entity produces nothing, which is the point.
    Expects `{{ source_cte }}` to carry `_row_hash` and `business_date`.
  #}
  changes as (

      select
          *,
          lag(_row_hash) over (partition by {{ key_columns | join(', ') }}
                               order by business_date)          as _prev_hash
      from {{ source_cte }}

  ),

  kept as (
      select * from changes
      where _prev_hash is null or _prev_hash <> _row_hash
  ),
{% endmacro %}


{% macro scd2_columns(key_columns) %}
  {#
    The four columns that make a row a VERSION. One definition, so the three
    reference tables cannot drift in how they express validity -- as_of()
    depends on all of them meaning the same thing everywhere.
  #}
  business_date                                             as effective_from,
  {{ scd2_effective_to('business_date', key_columns) }}     as effective_to,
  lead(business_date) over (partition by {{ key_columns | join(', ') }}
                            order by business_date) is null  as is_current,
  {#
    business_date is gone as a column, so it cannot be the partition column.
    Retention deletes by a range predicate against this instead of dropping a
    partition -- see docs/RETENTION.md.
  #}
  trunc(business_date, 'MM')                                as effective_from_month
{% endmacro %}


{% macro limit_in_force(alias, business_date_expr) %}
  {#
    Is this limit in force on the given date?

    THIS USED TO BE AN `is_current` COLUMN ON prepared.primary_limits, and it
    could not survive SCD2: it is a function of business_date, and an SCD2 row
    has no business_date. Two names would also have collided, since `is_current`
    now means "this is the live VERSION of the record" on every SCD2 table.

    It is a macro rather than nothing, because the model's docstring was right
    about why it existed: "it is computed here so that every consumer asks the
    question the same way -- the alternative is each report writing its own
    BETWEEN, which is precisely how the legacy estate ended up with limits that
    disagreed between screens." A macro keeps that single definition; only its
    shape moved from a column to a call.

    Note the limit's OWN effective_date/expiry_date are business facts about
    the limit, entirely separate from the record's effective_from/effective_to.
  #}
  case
      when {{ alias }}.status is null then null
      when {{ alias }}.status <> 'ACTIVE' then false
      when {{ alias }}.effective_date is not null
           and {{ business_date_expr }} < {{ alias }}.effective_date then false
      -- A null expiry is an open-ended limit, not a missing value.
      when {{ alias }}.expiry_date is not null
           and {{ business_date_expr }} > {{ alias }}.expiry_date then false
      else true
  end
{% endmacro %}
