{% macro removed_as_zero(alias, column) %}
  {#-
    A measure on the CURRENT side of a change comparison, with a REMOVED
    counterparty's value taken as zero -- it has no current row, so its change
    is the whole of what it had. A PRESENT counterparty keeps its own value,
    null included, so a null total still yields a null change rather than a
    fabricated one. Used by exposure_change; the key column is the row's
    presence test. See the `keys` CTE there.
  -#}
  case when {{ alias }}.counterparty_id is null then 0 else {{ alias }}.{{ column }} end
{%- endmacro %}
