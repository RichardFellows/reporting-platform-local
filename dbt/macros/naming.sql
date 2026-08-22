{#
  Schema naming.

  dbt's default generate_schema_name concatenates the profile's schema with the
  per-layer `+schema:` from dbt_project.yml, so `schema: lakehouse` plus
  `+schema: prepared` produces one schema literally named
  `lakehouse_prepared`.

  That default exists for shared-warehouse analytics teams: every developer
  targets a personal schema (`dbt_alice`), custom schemas namespace within it,
  and `dbt_alice_marts` cannot collide with `dbt_bob_marts`. This platform does
  not separate that way -- isolation comes from the Nessie branch and catalog
  (and REPORTING_ENV for config), every developer runs their own local stack,
  and CI builds on its own `ci/mr-*` branch. So the prefix guards against a
  collision the architecture already prevents, while breaking the layer model
  in docs/ARCHITECTURE.md: `raw` is created literally by ingest_feed.py, which
  left `lakehouse.raw.trade` sitting beside
  `lakehouse.lakehouse_prepared.prep_trade`.

  Returning the custom schema verbatim gives lakehouse.raw /
  lakehouse.prepared / lakehouse.reporting -- one consistent namespace per
  layer, matching both the design docs and what ingest already does.

  Note this is the SCHEMA half of a two-level `schema.table` name. The catalog
  name comes from spark.sql.defaultCatalog in profiles.yml (REPORTING_CATALOG,
  `lakehouse` by default), because dbt-spark cannot address a three-level
  catalog.schema.table reference at all --
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
