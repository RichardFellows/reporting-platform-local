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
    `t.cob_date` or `` `Trade Id` `` meant it.
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
{% macro incremental_window(date_column='cob_date', target_column=None) %}
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
      carries `_cob_date` -- while `this` is the prepared table, whose
      modelled column is `cob_date`. Assuming one name for both is what
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

  PREPARED ONLY, with one exception. Reporting models read `ref()`s, not raw,
  and carry no arrival clock of their own; an as-of reporting build is a build
  of the whole chain on one branch with the var set, which is why the var
  reaches every model rather than being a per-call-site argument. The
  exception is `counterparty_exposure`'s `delivered` CTE, which reads
  `raw.ref_counterparty` directly and so needs this filter as much as a
  prepared model does -- "the newest delivery for a date" means the newest
  one KNOWN at the knowledge time.
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
        incremental path dbt writes into `this` -- a MERGE on the SCD2 models,
        an overwrite of every COB date it selects on the date-partitioned ones
        (docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date) --
        so a build restricted to what
        was known last month would delete or overwrite rows built from
        everything known today -- a silent restatement backwards, in the
        published table, with a green run. Full-refresh on a throwaway branch
        is the only correct way to materialise one.
      #}
      {{ exceptions.raise_compiler_error(
           "knowledge_time=" ~ kt ~ " on an INCREMENTAL run of " ~ this ~
           ". An as-of build must be --full-refresh on a throwaway Nessie "
           ~ "branch: writing as-of rows into the published table would "
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


{# The newest delivery for each COB date, and deduplication within it.

   Re-deliveries land as a new _file_version rather than overwriting, so every
   prepared model must select the newest DELIVERY for each COB date -- the
   whole of it, and nothing from the delivery it replaced. And within that
   delivery, upstream occasionally repeats a business key; we take the last
   occurrence in file order, which matches the legacy ETL tool's behaviour.

   Centralised here so the rule cannot drift between models — this is exactly
   the kind of logic that was copy-pasted across legacy stored procedures and
   then diverged.

   See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date #}
{% macro dedupe_rank(partition_keys, mode='full_snapshot', newest_version=none) %}
  {#-
    `mode` IS THE FEED'S DECLARED SUPERSESSION, not a switch with a
    convenient default. `full_snapshot` -- each delivery restates the whole
    population for its COB date -- is the one mode this macro implements and
    what every feed does. Stating it is what lets `supersession:` in feeds.yml
    refuse a feed this cannot serve, instead of ranking a delta feed as though
    it were a snapshot and silently dropping every key its newest file omits.

    The other two modes are validated at load in
    `context.resolve_supersession_config` and rejected there with a reason;
    this is the second line of defence, for a model written by hand with a
    mode nothing checked.

    1 FOR EXACTLY ONE ROW PER KEY OF THE NEWEST DELIVERY, NULL FOR EVERY ROW
    OF AN OLDER ONE. Callers keep `where _rn = 1`.

    THIS USED TO PARTITION BY (_cob_date, <business keys>) alone, which selects
    the newest version of each KEY rather than the newest FILE: a key the
    newest delivery omitted kept its row from the delivery that one replaced.
    Every statement of intent said newest file, so the macro was wrong. The
    gate on `_file_version` is the fix; the ROW_NUMBER inside it is only the
    in-file dedupe, which is why it partitions by `_file_version` as well.

    `newest_version` IS WHERE "NEWEST" IS DECIDED, and the default is correct
    under one condition only: every COB date the enclosing query keeps, it
    keeps WHOLE. The default is a window over the rows the query's WHERE has
    already admitted -- so it is computed after `known_as_of()`, which is the
    point (an as-of build must rank against the deliveries that existed at its
    knowledge time), and after `incremental_window()`, which admits whole
    dates. A query that keeps only SOME keys of a date -- as the SCD2 models'
    `touched` join once did, before the rank -- would find the newest version
    among those keys' rows only, and a date whose touched keys were all
    dropped by the newest delivery would keep the old one. Such a query
    passes `newest_version='<alias>._newest_file_version'`, joined from
    `newest_file_version()` below, which reads raw unjoined. The SCD2 models
    still do: they rank every raw row now and scope the replay after
    cleaning, but the retraction guard reads the same CTE.
  -#}
  {%- if mode != 'full_snapshot' -%}
    {{ exceptions.raise_compiler_error(
         "dedupe_rank implements supersession mode 'full_snapshot' only, got '"
         ~ mode ~ "'. See `supersession:` in feeds.yml and "
         ~ "context.SUPERSESSION_NOT_BUILT for what each other mode would "
         ~ "require.") }}
  {%- endif -%}
  CASE WHEN _file_version = {% if newest_version is none -%}
      MAX(_file_version) OVER (PARTITION BY _cob_date)
    {%- else -%}
      {{ newest_version }}
    {%- endif %}
  THEN ROW_NUMBER() OVER (
    PARTITION BY _cob_date, _file_version,
      {%- for k in partition_keys %} {{ ident(k) }}{{ ',' if not loop.last }}{% endfor %}
    ORDER BY _row_number DESC
  ) END
{% endmacro %}


{% macro newest_file_version(source_relation) %}
  {#-
    The newest delivery per COB date, as a CTE named `newest_file_version`
    with columns `_newest_cob_date` and `_newest_file_version`. Emits a
    TRAILING COMMA, like `scd2_replay`.

    For a query whose rows for a date are a SUBSET of that date's keys -- see
    `newest_version` on `dedupe_rank` above. Filtered by `known_as_of()` and by
    nothing else: not by the lookback window and not by `touched`, because
    which delivery is newest for a date does not depend on which keys this run
    happens to be replaying.

    The columns are prefixed so that joining this cannot make `_cob_date` or
    `_file_version` ambiguous in the query that ranks.
  -#}
  newest_file_version as (

      select _cob_date          as _newest_cob_date,
             max(_file_version) as _newest_file_version
      from {{ source_relation }}
      where {{ known_as_of() }}
      group by _cob_date

  ),
{% endmacro %}

{#
  ---------------------------------------------------------------- SCD2
  Slowly-changing-dimension helpers. Used by the prepared reference models
  that store one row per VERSION rather than one row per COB date, and
  by the reporting models that join to them point-in-time.

  See docs/ARCHITECTURE.md for why only reference tables are shaped this way:
  `trade` measured 9.7% redundancy against `counterparty`'s 97%, so versioning
  a transaction table costs complexity and saves nothing.
#}

{% macro as_of(alias, cob_date_expr) %}
  {#
    Point-in-time join predicate against an SCD2 table.

    `effective_to` is DATE '9999-12-31' on the open version rather than NULL, so
    this needs no `or effective_to is null` branch -- which every consumer would
    otherwise have to remember, and which is silently wrong when forgotten
    (the current version simply stops matching and exposure loses its
    reference data).
  #}
  {{ cob_date_expr }} between {{ alias }}.effective_from and {{ alias }}.effective_to
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

{% macro scd2_key_match(left, right, key_columns) -%}
  {#-
    `left.k IS NOT DISTINCT FROM right.k and ...` over the key columns: the
    ONE comparison of a cleaned SCD2 key, used by every join in
    `scd2_replay`, by the markers in `scd2_retractions`, and by the MERGE's
    `on` in macros/merge.sql.

    NULL-SAFE, DELIBERATELY. `clean_string` turns '', 'NULL' and 'N/A' into
    NULL, and a full rebuild versions the NULL-key rows like any other key:
    window partitions group NULLs together. With `=` the incremental path
    matched nothing for them -- the rows silently vanished from incremental
    builds while a full rebuild kept them, so the `not_null` test on the key
    passed on exactly the builds that publish -- and the merge's `on` would
    re-insert a NULL-key version every run. A NULL key is still a defect
    `not_null` exists to fail; the point is that incremental and full builds
    agree, so the test sees it on both.

    NOT ENGINE-SPECIFIC. `IS NOT DISTINCT FROM` is in Spark 3.5.3's grammar
    (SqlBaseParser.g4, `predicate`: `IS NOT? kind=DISTINCT FROM`) and
    AstBuilder turns it into `EqualNullSafe`, the same expression as `<=>`;
    DuckDB accepts it and rejects `<=>`. So the standard spelling is used.
  -#}
  {%- for c in key_columns %}{{ left }}.{{ ident(c) }} is not distinct from {{ right }}.{{ ident(c) }}{{ ' and ' if not loop.last }}{% endfor -%}
{%- endmacro %}


{% macro scd2_refuse_full_refresh_over_pruned_raw(source_relation) %}
  {#-
    A FULL REFRESH of an SCD2 model reads only whatever raw currently holds,
    so a version whose ORIGIN COB date retention has since pruned re-dates to
    the next date raw still holds -- not a correction, a silent restatement
    of when it began. `scd2_pruned_seed` (above) is what protects the
    INCREMENTAL path from exactly this; nothing protected `--full-refresh`
    until this guard. See docs/DECISIONS.md
    #a-snapshot-re-delivery-restates-the-whole-date for the measured diff a
    rebuild produces once retention has pruned raw.

    ONLY AT EXECUTE TIME. `execute` is false while dbt is only PARSING the
    project -- `dbt parse`/`dbt ls`, which is how the build DAGs are
    rendered every time Cosmos parses them (#cosmos-load-bearing-settings)
    -- and `adapter`/`run_query` reach nothing then. Guarding on `execute`
    first means parsing the project never runs a query.

    ONLY WHEN THERE IS SOMETHING TO LOSE. `adapter.get_relation` returns
    NONE for a relation that does not exist yet: a first build, with no
    history a rebuild could re-date. An EMPTY existing target reads as
    nothing to lose too -- the query below simply finds no rows to check.

    THE SAME DATE-LEVEL PREDICATE `scd2_pruned_seed` USES -- does raw hold
    ANY row at all for this date -- not `min(effective_from)` against
    `min(_cob_date)` in raw: retention keeps month-ends, so the two minimums
    can agree while every mid-month date between them is gone. Deliberately
    NOT filtered by `known_as_of()`: retention deletes rows outright, it does
    not depend on when they were known, so whether a date is still in raw
    cannot depend on a `knowledge_time` this run happens to set.

    A READ THAT FAILS REFUSES. `run_query` raising propagates as an ordinary
    Jinja error here, uncaught, because a build that could not ask the
    question must never answer "nothing pruned" -- the same rule
    `#an-incomplete-keep-set-refuses` states for the orphan sweep applies
    exactly as much to a guard as to that sweep: a subject it could not READ
    is not a subject that is EMPTY.

    THE OVERRIDE IS EXPLICIT, modelled on `known_as_of()`'s own refusal
    below: `--vars '{scd2_rebuild_from_pruned_raw: true}'`, spelled exactly
    that way, is the only way past this. The message names the var and
    names restoring the table from a Nessie tag or commit as the
    alternative; it does not mention restoring from landing, because that is
    not built.

    APPLIES TO A KNOWLEDGE-TIME (AS-OF) BUILD TOO, deliberately.
    `known_as_of()` already refuses an INCREMENTAL run with `knowledge_time`
    set; a `--full-refresh` as-of build takes this same non-incremental
    branch and reads the same raw, so it is exactly as re-dated by a pruned
    origin as an ordinary rebuild and gets no exemption.
  -#}
  {%- if execute and not var('scd2_rebuild_from_pruned_raw', false) -%}
    {%- set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}
    {%- if existing is not none -%}
      {%- set pruned = run_query(
            "select distinct _t.effective_from from " ~ this ~ " as _t where not exists ("
            ~ "select 1 from " ~ source_relation ~ " as _raw where _raw._cob_date = _t.effective_from"
            ~ ") order by _t.effective_from") -%}
      {%- if pruned.rows | length > 0 -%}
        {%- set examples = [] -%}
        {%- for row in pruned.rows[:5] %}{%- do examples.append(row[0] | string) %}{%- endfor -%}
        {{ exceptions.raise_compiler_error(
             "--full-refresh of " ~ this ~ " would re-date every version beginning on one of "
             ~ (pruned.rows | length) ~ " COB date(s) raw no longer holds (e.g. "
             ~ (examples | join(', ')) ~ ") to the next date raw still holds -- a silent "
             ~ "restatement of when they began, not a correction. This refuses a "
             ~ "--full-refresh knowledge_time (as-of) build for the same reason: it reads "
             ~ "the same pruned raw. Add --vars '{scd2_rebuild_from_pruned_raw: true}' to "
             ~ "rebuild anyway, or restore " ~ this ~ " from a Nessie tag or commit instead.") }}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}


{% macro scd2_replay(stage_cte, key_columns, business_columns, source_relation) %}
  {#
    THE ROWS AN SCD2 MODEL VERSIONS, as a CTE named `replayed`: `cob_date`
    then `scd2_output_columns(business_columns)`, one row per key per COB
    date. Emits a TRAILING COMMA. `stage_cte` is the model's own cleaned
    stream over EVERY raw row, carrying `_rn` from `dedupe_rank` ranked on
    that CLEANED key -- so every key compared below, and the in-file dedupe
    before it, is the model's cleaned key, by the model's one definition of
    that cleaning. (Ranked on the raw key, ' B' and 'B' in one file were both
    "last in file" for one cleaned key: two versions with one effective_from.)

    THAT IS WHY IT COMES AFTER CLEANING, not before. The target holds cleaned
    keys (`clean_string`, and `upper` on `ref_rating`'s agency). The replay
    scope used to be built from raw keys and joined to the target, and a raw
    ' B' or a lower-case agency matched nothing: no retraction, a stranded
    current version, and two open versions at the key's next change.

    FULL REFRESH: the newest delivery's rows for every date.

    INCREMENTAL, per key the lookback window touches:

      * `scd2_window` -- where the lookback starts: the newest effective_from,
        less `lookback_days`.
      * `touched` -- every key the stage carries on or after it, in ANY
        delivery (not ranked), so a key the newest delivery dropped is still
        touched and can have its version retracted.
      * `replay_from` -- the start of the version in force when the window
        starts, the last one that began before it; NULL when every version
        began inside it, which reads as "from the beginning". Raw rows are
        replayed from this date, so every version from it onward is
        re-derived or, if its delivery was replaced, retracted
        (`scd2_retractions`).
      * `scd2_seed_from` -- the version BEFORE the replay start, when there is
        one. It is not re-derived from raw: it is taken from the TARGET, as
        the first row of the replay, so lead() can recompute its end. That is
        what reopens it when the version after it is retracted, and what lets
        a re-delivery that REVERTS to its value extend it instead of starting
        a duplicate beside it. It also means the replay never needs that
        version's own COB date in raw, which retention may have pruned.
      * `scd2_pruned_seed` -- every version FROM the replay start onward whose
        own COB date raw no longer holds AT ALL: the same "does raw have a
        delivery for this date" question `scd2_retractions` used to guard
        its marker with (`newest_file_version`), asked here at seed time
        instead. Such a version is taken from the TARGET, exactly like
        `scd2_seed_from`'s row -- never re-derived, because raw has nothing
        to say about it -- and, ordinarily, carried forward unchanged. It CAN
        still be marked for retraction, though not for lacking raw evidence:
        if an adjacent, RETAINED delivery restates the identical value,
        `scd2_changes` collapses the seeded row away as redundant, and
        `scd2_retractions` deletes the now-superseded target row rather than
        stranding it beside the version that subsumed it (see that macro). A
        version whose date raw DOES hold is excluded here and left to the
        ordinary raw branch above, so a re-delivery of that date can still
        retract or revert it as before; the two branches partition the scope
        by date, so a (key, cob_date) pair is never produced by both.

    WHAT IS HONOURED, then: re-deliveries of every COB date from each touched
    key's replay start onward THAT RAW STILL HOLDS. A re-delivery of an
    EARLIER date is outside the replay, exactly as a date outside the lookback
    window is for the `cob_date`-partitioned models, and needs a full
    refresh. A date inside the replay that retention has since pruned is not
    a re-delivery at all -- there is nothing to deliver -- so it is carried
    forward from the target by `scd2_pruned_seed` instead, unless a later,
    retained re-delivery has since made that carried-forward version
    redundant (`scd2_retractions`).

    The mutually_exclusive_ranges and scd2_exactly_one_current_version tests
    catch a replay that gets this wrong, and are not optional on any table
    using this.
    See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
  #}
  {%- set cols = scd2_output_columns(business_columns) -%}
  {%- if cols[-4:] != ['source_batch_id', 'dbt_invocation_id', 'nessie_ref', 'dbt_updated_at'] -%}
    {{ exceptions.raise_compiler_error(
         "scd2_replay renders the seed's last four columns with audit_columns(), "
         ~ "so scd2_output_columns() must end with its four, got " ~ cols[-4:]) }}
  {%- endif -%}
  {%- set keys = [] -%}
  {%- for c in key_columns %}{%- do keys.append(ident(c)) %}{%- endfor -%}
  {%- if not is_incremental() -%}
  {%- do scd2_refuse_full_refresh_over_pruned_raw(source_relation) -%}
  replayed as (

      select cob_date{% for c in cols %}, {{ ident(c) }}{% endfor %}
      from {{ stage_cte }}
      where _rn = 1

  ),
  {%- else %}
  {#- Which output columns the target already has. A column the model has
      just gained is added by on_schema_change AFTER this view is analysed,
      so the seed reads NULL for it rather than failing on a missing column. -#}
  {%- set in_target = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list %}
  scd2_window as (

      select coalesce(max(effective_from), date '1900-01-01')
             - interval {{ var('lookback_days', 3) }} day as window_start
      from {{ this }}

  ),

  touched as (

      select distinct {{ keys | join(', ') }}
      from {{ stage_cte }}
      where cob_date >= (select window_start from scd2_window)

  ),

  replay_from as (

      {#- A CROSS JOIN rather than a scalar subquery inside the aggregate. -#}
      select {% for k in keys %}_v.{{ k }} as {{ k }}, {% endfor %}
             max(case when _v.effective_from < _w.window_start
                      then _v.effective_from end)                  as from_date
      from {{ this }} as _v
      join touched as _t on {{ scd2_key_match('_t', '_v', key_columns) }}
      cross join scd2_window as _w
      group by {% for k in keys %}_v.{{ k }}{{ ', ' if not loop.last }}{% endfor %}

  ),

  scd2_seed_from as (

      select {% for k in keys %}_v.{{ k }} as {{ k }}, {% endfor %}
             max(_v.effective_from)                                as seed_from
      from {{ this }} as _v
      join replay_from as _p on {{ scd2_key_match('_p', '_v', key_columns) }}
      where _v.effective_from < _p.from_date
      group by {% for k in keys %}_v.{{ k }}{{ ', ' if not loop.last }}{% endfor %}

  ),

  scd2_pruned_seed as (

      {#- Every version FROM the replay start onward whose own COB date raw no
          longer holds a delivery at all -- the SAME predicate
          `scd2_retractions`' guard reads (`newest_file_version`, date-level),
          asked here instead so the version is seeded rather than left for a
          replay that cannot re-derive it and a retraction guard that (rightly)
          refuses to delete it. A date raw DOES still hold is excluded by the
          `not exists`, so it is left to the ordinary raw branch in `replayed`
          below -- the two can never emit the same (key, cob_date), because
          this CTE and that branch partition the scope by exactly that
          existence check. -#}
      select _old.*
      from {{ this }} as _old
      join replay_from as _p on {{ scd2_key_match('_p', '_old', key_columns) }}
      where _old.effective_from >= coalesce(_p.from_date, date '1900-01-01')
        and not exists (
            select 1 from newest_file_version as _nv
            where _nv._newest_cob_date = _old.effective_from
        )

  ),

  replayed as (

      select _s.cob_date{% for c in cols %}, _s.{{ ident(c) }}{% endfor %}
      from {{ stage_cte }} as _s
      join touched as _t on {{ scd2_key_match('_t', '_s', key_columns) }}
      left join replay_from as _p on {{ scd2_key_match('_p', '_s', key_columns) }}
      where _s._rn = 1
        and _s.cob_date >= coalesce(_p.from_date, date '1900-01-01')

      union all

      {# The seed's own columns come from the target, except the four audit
          columns: the merge CHANGES this row (its effective_to), so it names
          this run and branch, as every re-derived row does. source_batch_id
          stays the target's -- the delivery that began the version. #}
      select _q.effective_from as cob_date
             {%- for c in cols[:-4] %},
             {% if c | lower in in_target %}_q.{{ ident(c) }}{% else %}null{% endif %} as {{ ident(c) }}
             {%- endfor %},
             {{ audit_columns('_q.source_batch_id' if 'source_batch_id' in in_target else 'null') }}
      from {{ this }} as _q
      join scd2_seed_from as _f
        on {{ scd2_key_match('_f', '_q', key_columns) }}
       and _q.effective_from = _f.seed_from

      union all

      {# A pruned-date version, seeded the same way as the row above it: the
         target's own columns, this run's audit columns, source_batch_id kept
         from the delivery that began the version. Never re-derived, never a
         retraction candidate -- raw has nothing to say about this date. #}
      select _u.effective_from as cob_date
             {%- for c in cols[:-4] %},
             {% if c | lower in in_target %}_u.{{ ident(c) }}{% else %}null{% endif %} as {{ ident(c) }}
             {%- endfor %},
             {{ audit_columns('_u.source_batch_id' if 'source_batch_id' in in_target else 'null') }}
      from scd2_pruned_seed as _u

  ),
  {%- endif %}
{% endmacro %}


{% macro scd2_retracted() -%}
  {#- The effective_to that marks a row as a RETRACTION rather than a version:
      `scd2_retractions` writes it and the merge deletes on it. A date no real
      range can end on, so it can never be a real version's value. -#}
  DATE '0001-01-01'
{%- endmacro %}


{% macro prepared_output_columns(business_columns) %}
  {#-
    The column ORDER of a prepared model's cleaned output, defined once: the
    business columns, the delivery the row came from, the provenance and
    audit columns -- the order `cleaned` writes them in. A model that ranks
    AFTER cleaning carries `_cob_date`, `_file_version` and `_row_number`
    through for the rank, and Spark 3.5 has no `SELECT * EXCEPT`, so it
    projects this list to drop them. `insert_overwrite` into an existing
    table must see the columns it always did, in that order.
  -#}
  {%- set cols = business_columns + ['source_file', 'source_file_version'] -%}
  {%- for alias, _expression in provenance_pairs() %}{% do cols.append(alias) %}{% endfor -%}
  {%- do cols.extend(['source_batch_id', 'dbt_invocation_id', 'nessie_ref', 'dbt_updated_at']) -%}
  {{ return(cols) }}
{% endmacro %}


{% macro scd2_output_columns(business_columns) %}
  {#-
    The column ORDER of an SCD2 model's output, defined once: the prepared
    order above, then what `scd2_columns` appends. `scd2_replay` projects
    it, `ranged` projects it, and `scd2_retractions` unions against it
    POSITIONALLY -- Spark 3.5 has no UNION BY NAME -- so they must not be able
    to disagree.
  -#}
  {{ return(prepared_output_columns(business_columns)) }}
{% endmacro %}


{% macro scd2_retractions(final_cte, key_columns, business_columns) %}
  {#-
    On the incremental path: one MARKER row per version in the target that
    this run's replay covers and did not re-derive -- a version whose source
    delivery has since been replaced by one that drops or reverts it. The
    merge (macros/merge.sql) deletes the matching target row; on a full
    refresh nothing renders, because nothing is in a target to retract.

    Only the key, effective_from and the `scd2_retracted()` marker are real
    values. Every other column is NULL, and read from nothing: a column the
    model has just gained is not in the target yet when this is analysed
    (on_schema_change adds it after the temporary view exists), so reading
    one from `this` would fail the first run after it was added.

    ONE BOUND, not two, since item 22: THE REPLAY'S SCOPE -- touched keys,
    from `replay_from` onward, the identical join `scd2_pruned_seed` uses.
    Both sides are cleaned keys (`scd2_replay`).

    A version in that scope, "not re-derived" USED TO MEAN "not re-derived
    from raw", and a version whose COB date raw no longer holds at all could
    never be re-derived -- so this carried a second bound, only where raw
    still held a delivery for the version's date, or every pruned version
    would have been deleted and the key silently re-dated to the next
    retained delivery.

    THAT SECOND BOUND IS GONE, because `scd2_replay`'s `scd2_pruned_seed` now
    means every version in scope is ALWAYS represented going into
    `scd2_changes` -- from raw where raw still holds the date, from the
    target where it does not. So "not re-derived" now means "not re-derived
    AND not carried forward either", which can only happen one way: the
    version was carried forward BY `scd2_pruned_seed`, but its row was then
    collapsed by `scd2_changes` because an adjacent, retained delivery
    restates the identical value -- upstream's own correction subsuming a
    version retention had already pruned the evidence for. That must retract
    it, or it strands as a second current row beside the version that
    subsumed it. Absence of evidence is still not a retraction: nothing here
    is ever deleted merely because raw does not hold its date, only when a
    delivery raw DOES hold makes it redundant.
    See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
  -#}
  {%- if is_incremental() %}
  union all

  select
    {%- for c in scd2_output_columns(business_columns)
                 + ['effective_from', 'effective_to', 'is_current', 'effective_from_month'] %}
    {% if c in key_columns or c == 'effective_from' -%}
      _old.{{ ident(c) }}
    {%- elif c == 'effective_to' -%}
      {{ scd2_retracted() }}
    {%- else -%}
      null
    {%- endif %}                                            as {{ ident(c) }}{{ ',' if not loop.last }}
    {%- endfor %}
  from {{ this }} as _old
  join replay_from as _p on {{ scd2_key_match('_p', '_old', key_columns) }}
  where _old.effective_from >= coalesce(_p.from_date, date '1900-01-01')
    and not exists (
        select 1 from {{ final_cte }} as _new
        where {{ scd2_key_match('_new', '_old', key_columns) }}
          and _new.effective_from = _old.effective_from
    )
  {%- endif %}
{% endmacro %}


{% macro scd2_changes(source_cte, key_columns) %}
  {#
    Collapse a per-COB-date stream into one row per CHANGE. A delivery
    that restates an unchanged entity produces nothing, which is the point.
    Expects `{{ source_cte }}` to carry `_row_hash` and `cob_date`.
  #}
  changes as (

      select
          *,
          lag(_row_hash) over (partition by
            {%- for c in key_columns %} {{ ident(c) }}{{ ',' if not loop.last }}{% endfor %}
                               order by cob_date)          as _prev_hash
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
  cob_date                                                  as effective_from,
  {{ scd2_effective_to('cob_date', key_columns) }}          as effective_to,
  lead(cob_date) over (partition by
    {%- for c in key_columns %} {{ ident(c) }}{{ ',' if not loop.last }}{% endfor %}
                            order by cob_date) is null  as is_current,
  {#
    cob_date is gone as a column, so it cannot be the partition column.
    Retention deletes by a range predicate against this instead of dropping a
    partition -- see docs/RETENTION.md.
  #}
  trunc(cob_date, 'MM')                                     as effective_from_month
{% endmacro %}
