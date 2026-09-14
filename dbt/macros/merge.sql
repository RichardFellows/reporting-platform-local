{#
  dbt-spark's MERGE, with a DELETE clause for a model that asks for one.

  dbt-spark 1.8's `spark__get_merge_sql` emits `when matched then update` and
  `when not matched then insert *` and nothing else, and has no configuration
  that adds a delete. That is right for a merge whose source restates every
  row it covers, and wrong for the SCD2 models: a re-delivery that drops or
  reverts a change RETRACTS a version an earlier run wrote, nothing the replay
  re-derives matches it, and an update-or-insert leaves it current for ever --
  two open versions as soon as the key next changes.

  So a model with `scd2_retractions=true` in its config gets a merge that
  deletes the rows its source marks with `scd2_retracted()` (emitted by
  `scd2_retractions` in engine.sql), updates the rest, and inserts only rows
  that are not markers. ONE statement, so the deletion is decided against the
  same snapshot of the target as everything else and lands in the same
  commit: there is no state between a delete and a merge for a failure to
  leave behind. Every clause is one Iceberg documents for Spark MERGE INTO --
  conditional `WHEN MATCHED ... THEN DELETE`, `WHEN MATCHED THEN UPDATE`,
  conditional `WHEN NOT MATCHED ... THEN INSERT *` -- and needs the Iceberg
  SQL extensions `profiles.yml` already loads.

  EVERY OTHER MODEL gets dbt-spark's own SQL, unchanged: `dbt.` reaches the
  internal namespace, where the adapter's macro still lives after this one
  shadows it for dispatch (the root project is searched first).

  The update list is the target's columns, as dbt-spark computes it -- after
  on_schema_change has added any new ones.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}
{% macro spark__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {%- if not config.get('scd2_retractions', false) -%}
    {{ return(dbt.spark__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates)) }}
  {%- endif -%}

  {%- if unique_key is string or unique_key is none or 'effective_from' not in unique_key -%}
    {{ exceptions.raise_compiler_error(
         "scd2_retractions needs a unique_key list including effective_from, got "
         ~ unique_key ~ ". A retraction matches the version it removes by "
         ~ "(key, effective_from).") }}
  {%- endif -%}

  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- for key in unique_key -%}
    {%- do predicates.append('DBT_INTERNAL_SOURCE.' ~ key ~ ' = DBT_INTERNAL_DEST.' ~ key) -%}
  {%- endfor -%}
  {%- set update_columns = adapter.get_columns_in_relation(target) | map(attribute='quoted') | list -%}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

      when matched and DBT_INTERNAL_SOURCE.effective_to = {{ scd2_retracted() }} then delete

      when matched then update set
        {%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}

      when not matched and DBT_INTERNAL_SOURCE.effective_to <> {{ scd2_retracted() }} then insert *
{% endmacro %}
