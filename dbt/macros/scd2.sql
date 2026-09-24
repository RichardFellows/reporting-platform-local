{% macro scd2_prepared(source_name, keys, cleaning, hashed, derived={}) %}
  {#-
    THE WHOLE SCD2 PREPARED MODEL, from raw to the merge's input, as one
    macro. The three SCD2 models (ref_counterparty, ref_rating,
    qa_happy_position_scd2) repeated this sequence by hand, and it is the
    subtlest logic in the platform: a model copied from one of them and
    edited is how a fourth one would get it subtly wrong. Each of them is now
    its config, its documentation and one call. Moved onto this macro with a
    before/after EXCEPT in both directions returning 0 rows, full-refresh and
    incremental (plan #15).
    See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date and
    #scd2-is-one-macro.

      source_name  the raw table, e.g. 'ref_rating'
      keys         the business key, a LIST -- ref_rating's is two columns
      cleaning     column -> SQL, IN OUTPUT ORDER, keys first: how each raw
                   column becomes a conformed one. The expressions are built
                   by the caller from clean_string() and friends.
      hashed       the columns whose change opens a new version. Source facts
                   only: never a key, never a derived column (it cannot change
                   without its input changing), never an audit column (see
                   scd2_hash).
      derived      column -> SQL over the CLEANED columns, appended after
                   them (ref_rating's rating_rank and grade_band). Optional.

    The business columns -- the output order scd2_output_columns fixes -- are
    `cleaning`'s keys then `derived`'s, so the order cannot drift from the
    expressions that produce it.

    The sequence, and why each step is where it is:

      newest_file_version  the newest delivery per COB date, from raw
                           UNJOINED (dedupe_rank's `newest_version`), after
                           known_as_of() and nothing else.
      raw_rows             EVERY raw row, unranked, filtered by known_as_of()
                           only: ranking, the replay scope and the retraction
                           markers all happen after cleaning, so each uses the
                           CLEANED key.
      cleaned              the conformed columns, with _cob_date, _file_version,
                           _row_number and _newest_file_version carried for
                           the rank.
      derived              (optional) columns computed from cleaned ones.
      ranked_rows          THE IN-FILE DEDUPE IS ON THE CLEANED KEY. Ranked on
                           the raw key, ' B' and 'B' in one file were each
                           "last in file" and both survived as one cleaned key:
                           two versions with one effective_from, one ending
                           before it began.
      scd2_replay          on an incremental run, the touched keys from the
                           version in force before their earliest touched date.
      versioned            the change detector's hash.
      scd2_changes         keep a row only where the hash changed.
      ranged               the output columns and the SCD2 range columns.
      scd2_retractions     on an incremental run, one marker row per version
                           this replay covers and no longer derives; the merge
                           (macros/merge.sql) deletes on them.
  -#}
  {%- set business_columns = [] -%}
  {%- for c in cleaning %}{% do business_columns.append(c) %}{% endfor -%}
  {%- for c in derived %}{% do business_columns.append(c) %}{% endfor -%}
  {%- for k in keys -%}
    {%- if k not in cleaning -%}
      {{ exceptions.raise_compiler_error(
           "scd2_prepared: key '" ~ k ~ "' has no cleaning expression -- "
           ~ "every key must be conformed before the rank that dedupes on it.") }}
    {%- endif -%}
    {%- if k in hashed -%}
      {{ exceptions.raise_compiler_error(
           "scd2_prepared: key '" ~ k ~ "' is in `hashed`. A key identifies a "
           ~ "version; it cannot be a change within one.") }}
    {%- endif -%}
  {%- endfor -%}
  {%- for h in hashed -%}
    {%- if h not in cleaning -%}
      {{ exceptions.raise_compiler_error(
           "scd2_prepared: hashed column '" ~ h ~ "' is not a cleaned column. "
           ~ "Only source facts are hashed; a derived column cannot change "
           ~ "without its input changing.") }}
    {%- endif -%}
  {%- endfor %}

with

{{ newest_file_version(source('raw', source_name)) }}

raw_rows as (

    select
        r.*,
        nv._newest_file_version
    from {{ source('raw', source_name) }} r
    join newest_file_version nv on nv._newest_cob_date = r._cob_date
    where {{ known_as_of() }}

),

cleaned as (

    select
        _cob_date                                                   as cob_date,
        {%- for c, expression in cleaning.items() %}
        {{ expression }}                                            as {{ ident(c) }},
        {%- endfor %}
        _source_file                                                as source_file,
        _file_version                                               as source_file_version,
        {{ source_provenance() }}
        {{ audit_columns() }},
        -- carried for the rank below, which needs the cleaned key
        _cob_date,
        _file_version,
        _row_number,
        _newest_file_version

    from raw_rows

),

{%- set rank_from = 'cleaned' %}
{%- if derived %}
{%- set rank_from = 'derived' %}

derived as (

    select
        *,
        {%- for c, expression in derived.items() %}
        {{ expression }}                                            as {{ ident(c) }}{{ ',' if not loop.last }}
        {%- endfor %}
    from cleaned

),
{%- endif %}

ranked_rows as (

    select
        *,
        {{ dedupe_rank(keys, newest_version='_newest_file_version') }} as _rn
    from {{ rank_from }}

),

{{ scd2_replay('ranked_rows', keys, business_columns) }}

versioned as (

    select
        *,
        {{ scd2_hash(hashed) }}                                     as _row_hash
    from replayed

),

{{ scd2_changes('versioned', keys) }}

ranged as (

    select
        {%- for c in scd2_output_columns(business_columns) %}
        {{ ident(c) }},
        {%- endfor %}
        {{ scd2_columns(keys) }}

    from kept

)

select * from ranged
{{ scd2_retractions('ranged', keys, business_columns) }}
{%- endmacro %}


{% macro yes_no_flag(expression) %}
  {#- Upstream sends Y/N/1/0/true/false depending on the release. Normalised
      once, here, rather than in every consuming report. Unrecognised is NULL,
      so a not_null test can see it. -#}
  case
            when upper({{ expression }}) in ('Y', 'YES', 'TRUE', '1') then true
            when upper({{ expression }}) in ('N', 'NO', 'FALSE', '0') then false
            else null
        end
{%- endmacro %}
