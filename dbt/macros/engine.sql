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
{% macro ident(name) -%}
  {#-
    Quote a COLUMN NAME so any name works, including one with a space in it.

    Which macros take a name and which take an EXPRESSION is the distinction
    that matters here, and it is not cosmetic. `dedupe_rank`, `scd2_hash`,
    `scd2_effective_to` and `as_of` are handed identifiers and interpolate them
    into SQL, so `PARTITION BY Trade Id` is a syntax error rather than a
    quoting inconvenience -- those call this. `safe_cast`, `clean_string` and
    `parse_date` are handed expressions and nest inside each other
    (`safe_cast(clean_string('x'), 'DECIMAL(18,2)')`), so quoting their
    argument would break every existing model; those do not.

    Column names reaching the prepared layer are normally ordinary identifiers
    anyway, because `feeds.yml` maps awkward source headers to clean names at
    ingest -- see docs/DECISIONS.md#source-column-names and
    docs/DECISIONS.md#identifiers-in-macros. This is the second
    line of defence, for a name that is still awkward and for the models that
    read raw directly.

    Already-quoted and qualified names are passed through: a caller that wrote
    `t.business_date` or `` `Trade Id` `` meant it.
  -#}
  {%- set text = name | string | trim -%}
  {%- if '`' in text or '.' in text or '(' in text -%}{{ text }}
  {%- else -%}{{ '`' ~ text ~ '`' }}{%- endif -%}
{%- endmacro %}


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


{#
  --------------------------------------------------- knowledge time (as-of)
  REQ-300/REQ-301. "What did we believe on date X", answered by the same
  models rather than by a second code path.

  A ROW'S KNOWLEDGE TIME IS ITS DELIVERY'S ARRIVAL TIME, and the platform has
  two clocks for that. `_received_at` is when the delivery landed and is the
  right one -- but it was ADDED, NEVER BACKFILLED, so it is NULL for every row
  ingested before that change. `_ingest_ts` is when the platform loaded the
  rows, has been on every raw table since the beginning, and is never null.
  The two differ by however long a delivery waited to be ingested (a Friday
  arrival loaded on Monday), so the fallback is not a synonym -- it is the
  later, more conservative of the two, and using it can only ever include a
  row in an as-of query slightly earlier than the truth, never exclude one it
  should have shown.

  DEFAULTS TO NOW, expressed as `1 = 1`. With no `knowledge_time` var the
  predicate compiles away entirely, so no model changes behaviour on the day
  this lands and the ordinary nightly build is byte-identical to before.

  PREPARED ONLY. Reporting models read `ref()`s, not raw, and carry no
  arrival clock of their own; an as-of reporting build is a build of the whole
  chain on one branch with the var set, which is why the var reaches every
  model rather than being a per-call-site argument.
#}
{% macro known_as_of() %}
  {%- set kt = var('knowledge_time', none) -%}
  {%- if kt is none or kt | string | trim == '' -%}
    1 = 1
  {%- else -%}
    {%- set kt = kt | string | trim -%}
    {%- if not modules.re.match('^\\d{4}-\\d{2}-\\d{2}([ T]\\d{2}:\\d{2}(:\\d{2})?)?$', kt) -%}
      {{ exceptions.raise_compiler_error(
           "knowledge_time must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]', got "
           ~ kt ~ ". It is interpolated into SQL, and a value that is not a "
           ~ "timestamp would either fail deep in the build or silently "
           ~ "compare against NULL and return nothing.") }}
    {%- endif -%}
    {%- if is_incremental() -%}
      {#
        AN AS-OF BUILD MUST NOT WRITE INTO THE PUBLISHED TABLE. On the
        incremental path dbt MERGEs into `this`, so a build restricted to what
        was known last month would delete or overwrite rows built from
        everything known today -- a silent restatement backwards, in the
        published table, with a green run. Full-refresh on a throwaway branch
        is the only correct way to materialise one.
      #}
      {{ exceptions.raise_compiler_error(
           "knowledge_time=" ~ kt ~ " on an INCREMENTAL run of " ~ this ~
           ". An as-of build must be --full-refresh on a throwaway Nessie "
           ~ "branch: merging as-of rows into the published table would "
           ~ "restate it backwards. Add --full-refresh, or drop the var.") }}
    {%- endif -%}
    coalesce(_received_at, _ingest_ts) <= TIMESTAMP '{{ kt }}'
  {%- endif -%}
{% endmacro %}


{# Standard audit columns on every prepared/reporting model.
   Lineage back to the exact ingest batch is what makes a published figure
   explainable six months later. #}
{#
  ---------------------------------------------------- delivery provenance
  Where the row CAME FROM, as opposed to what this build did with it.

  Distinct from `audit_columns` and the distinction is the point: those
  describe the dbt invocation that wrote the row -- batch, invocation id,
  Nessie ref, timestamp -- and change every time the model is rebuilt. These
  describe the DELIVERY, and do not change however often the row is rebuilt
  from it. REQ-303 and REQ-304.

  `prepared` previously dropped all four by selecting named columns, so a
  typed value could be traced to the file it came from (`_source_file` was
  kept) but not to the delivery, the contract it was read against, or when it
  arrived. Retrofitting that across ten teams' models later is not a day of
  work, which is why it is here before there are ten teams' models.

  ONE PAIR LIST, TWO MACROS. `source_provenance()` projects them out of raw;
  `source_provenance_columns()` names them again for the SCD2 models, which
  enumerate their output columns explicitly rather than `select *`. A fifth
  provenance column added to the list below reaches both.
#}
{% macro provenance_pairs() %}
  {#-
    (alias, expression). All but the first are a bare raw column; the first is
    `delivery_ref()`, which is where the phase-3 fallback lives -- see that
    macro. Anything added here reaches `source_provenance()` and
    `source_provenance_columns()` together.
  -#}
  {{ return([('delivery_id',    delivery_ref()),
             ('received_at',    '_received_at'),
             ('schema_version', '_schema_version'),
             ('source_system',  '_source_system')]) }}
{% endmacro %}


{% macro delivery_ref() %}
  {#-
    WHICH DELIVERY A RAW ROW CAME FROM, reaching back past the change that
    started recording it.

    `_delivery_id` was ADDED, NEVER BACKFILLED (docs/DECISIONS.md#provenance-
    is-added-not-backfilled): adding a column in Iceberg touches no data
    files, so every row ingested before it reads NULL and no as-of query, run
    record or evidence check could see those rows' deliveries at all.

    `_source_file` is the fallback, and it is not a synonym. It is the PART --
    the object the rows were actually read from -- and `already_ingested`
    depends on it staying the part, which is exactly why `_delivery_id` needed
    a column of its own. For `kind: file`, which is every delivery in this
    catalog, the part IS the landing object and the delivery id is its
    BASENAME, so the fallback is exact once the prefix is stripped. It is not
    a full key with the prefix left on: `_delivery_id` holds a bare filename,
    and coalescing the two without stripping would mix two namespaces in one
    column and silently break every join and group-by over it.

    THE ONE SHAPE IT IS NOT EXACT FOR is an archive ingested before the
    provenance columns existed, where the part is an extracted member under
    `ready/` and its basename is a member name rather than the delivery. No
    feed in this catalog is `kind: archive`, so no such row exists -- but the
    limit is written here rather than left to be discovered, because the row
    it would produce looks perfectly ordinary.
  -#}
  coalesce(_delivery_id, element_at(split(_source_file, '/'), -1))
{% endmacro %}


{# Emits a TRAILING COMMA: it is always placed before `audit_columns()`,
   which is always last in the select. #}
{% macro source_provenance() %}
  {%- for alias, expression in provenance_pairs() %}
  {{ expression }}                            AS {{ alias }},
  {%- endfor %}
{% endmacro %}


{# The same columns by their prepared-layer names, for a model that lists its
   output columns rather than selecting *. Also trailing-comma'd. #}
{% macro source_provenance_columns() %}
  {%- for alias, _expression in provenance_pairs() %}
  {{ alias }},
  {%- endfor %}
{% endmacro %}


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
{% macro dedupe_rank(partition_keys, mode='full_snapshot') %}
  {#-
    `mode` IS THE FEED'S DECLARED SUPERSESSION, not a switch with a
    convenient default. `full_snapshot` -- each delivery restates the whole
    population for its business date -- is what this macro has always
    implemented and what every feed does, and it was implemented without ever
    being stated. Stating it is what lets `supersession:` in feeds.yml refuse
    a feed this cannot serve, instead of ranking a delta feed as though it
    were a snapshot and silently dropping every key its newest file omits.

    The other two modes are validated at load in
    `context.resolve_supersession_config` and rejected there with a reason;
    this is the second line of defence, for a model written by hand with a
    mode nothing checked.
  -#}
  {%- if mode != 'full_snapshot' -%}
    {{ exceptions.raise_compiler_error(
         "dedupe_rank implements supersession mode 'full_snapshot' only, got '"
         ~ mode ~ "'. See `supersession:` in feeds.yml and "
         ~ "context.SUPERSESSION_NOT_BUILT for what each other mode would "
         ~ "require.") }}
  {%- endif -%}
  ROW_NUMBER() OVER (
    PARTITION BY _business_date,
      {%- for k in partition_keys %} {{ ident(k) }}{{ ',' if not loop.last }}{% endfor %}
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
    coalesce(cast({{ ident(c) }} as string), '')
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
    date_sub(lead({{ ident(order_column) }}) over (
      partition by
      {%- for c in partition_columns %} {{ ident(c) }}{{ ',' if not loop.last }}{% endfor %}
      order by {{ ident(order_column) }}), 1),
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
  {%- set keys = [] -%}
  {%- for c in key_columns %}{%- do keys.append(ident(c)) %}{%- endfor -%}
  {%- set keys = keys | join(', ') -%}
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
          lag(_row_hash) over (partition by
            {%- for c in key_columns %} {{ ident(c) }}{{ ',' if not loop.last }}{% endfor %}
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
  lead(business_date) over (partition by
    {%- for c in key_columns %} {{ ident(c) }}{{ ',' if not loop.last }}{% endfor %}
                            order by business_date) is null  as is_current,
  {#
    business_date is gone as a column, so it cannot be the partition column.
    Retention deletes by a range predicate against this instead of dropping a
    partition -- see docs/RETENTION.md.
  #}
  trunc(business_date, 'MM')                                as effective_from_month
{% endmacro %}
