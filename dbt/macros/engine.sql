{#
  Shared SQL constructs for the Spark/Iceberg build.

  THIS FILE USED TO BE A PORTABILITY LAYER, carrying a DuckDB branch beside
  every Spark one and claiming "the project's tests must pass on both
  targets". Session 5 established that DuckDB cannot be a second build engine
  here on three independent counts -- it cannot address a Nessie branch (so no
  write-audit-publish), it silently drops `partition_by` (so it cannot
  reproduce the partition spec retention depends on), and it cannot INSERT to
  a partitioned table without an explicit override.1".

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
