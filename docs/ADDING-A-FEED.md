# Adding a feed

Worked example: a `treasury_margin_call` feed from the `treasury` source system,
delivering `marginCalls_20260801.csv`. The lowerCamelCase filename is
deliberate — it is what makes the per-feed `filename_pattern` earn its keep.

**Five files, in this order.** The order matters only in that each step is
verifiable on its own — do not batch them and then debug the whole thing at
once.

> **There is a UI for this.** The feed console at <http://localhost:8082>
> writes all five files from one form, with the filename pattern derived from a
> real filename and every trap below turned into a validation message. See
> [FEED-UI.md](FEED-UI.md). Read this document anyway: the console does what it
> describes and the diff it produces is meant to be reviewed as an ordinary
> change.

---

## 1. `reporting_platform/config/feeds.yml` — the registry

This is the single source of truth. It creates the raw table, the Airflow DAG,
the asset that triggers `prepared_build`, and the retention/maintenance
entries. Nothing else in the platform needs to learn the feed's name.

```yaml
  - name: treasury_margin_call
    description: Margin calls per counterparty and call type, from treasury.
    source_system: TREASURY
    filename_pattern: 'marginCalls_(?P<business_date>\d{8})(?:_v(?P<version>\d+))?\.csv'
    business_key: [margin_call_id]
    expected_min_rows: 10
    columns: [margin_call_id, counterparty_id, call_type, call_amount,
              currency, effective_date, due_date, status]
```

Four things that are easy to get wrong:

- **`name` is a table name, an Airflow DAG id and an S3 prefix at once.**
  `treasury_margin_call` gives `lakehouse.raw.treasury_margin_call`, DAG
  `ingest_margin_call` and landing prefix `landing/treasury_margin_call/`. Use
  lowercase with underscores regardless of what the upstream calls its files.
- **`filename_pattern` must yield a `business_date` named group**, and it is
  matched with `re.fullmatch`, not `search` — a pattern that does not cover
  the whole filename silently matches nothing, and the feed simply never has
  anything pending. The `(?:_v(?P<version>\d+))?` group is optional but you
  want it: it is how a re-delivery lands as a new `_file_version` instead of
  a duplicate. The pattern is per-feed precisely so a source system with its
  own naming convention (`marginCalls_`, lowerCamelCase, unlike the other
  three feeds) does not force that convention on anyone else.
- **A column can declare the name it has in the FILE.** Real deliveries are
  headed `Trade Id`, `Cpty Ref`, `Notional (USD)`; the platform needs
  identifiers, because column names reach SQL through dbt macros. Write
  `- trade_id: "Trade Id"` and the rename happens once, at ingest, so raw
  onwards sees only identifiers. A bare `- notional` means the header is
  already usable, which is most columns. Drift is still reported in the
  file's names. See
  [DECISIONS.md#source-column-names](DECISIONS.md#source-column-names).
- **`columns` is the declared contract, and it is *not* discovered from the
  file.** It becomes the raw DDL, all `STRING`. A column in the file but not
  here lands in the `_extra_columns` map; a column here but not in the file
  lands as NULL. Both are reported as drift.
- **`expected_min_rows` aborts the ingest below that count**, leaving the
  branch for inspection and `main` untouched. Set it to something a genuinely
  empty or truncated delivery would fail, not to the expected row count.

Optional, and worth a thought rather than a default:

| Key | When |
|---|---|
| `convention: <name>` | Another feed from this source system already exists and shares its delivery arrangement. Inherits everything the convention sets; anything the convention supplies is then left OUT of this block, so it stays in one place. Naming one that is not defined is an error at load, not a silent fallback. See [DECISIONS.md#feed-conventions](DECISIONS.md#feed-conventions). |
| `cadence: weekly` | The feed does not deliver every business date. Without it the completeness check infers the calendar from the other feeds and reports every non-delivery day as a gap. |
| `completeness: false` | Monthly or ad-hoc. Opts out of the gap check entirely. |
| `schema_drift: fail` | Abort the load on an extra *or* missing column instead of landing and warning. The default `warn` is usually right — a rejected file is a file nobody looks at. |
| `column_types:` | A column whose prepared-layer treatment is not what its *name* implies. `haircut_pct` reads as a string to the inference but should be `decimal`; `settlement_ccy` is a code, not free text. Only list the disagreements — anything absent falls back to the inference. It is what the feed console writes when you change a type on the form, and what the sample-data generator reads, so the two cannot drift apart. Raw is still all strings; this describes the **prepared** model. |
| `delivery:` | The delivery is a zip (`kind: archive`, plus `member_pattern`) or gates on a second, control file (`control: {pattern, row_count}`) rather than one plain CSV. See [DELIVERY-SHAPES.md](DELIVERY-SHAPES.md) for the shapes, and [DECISIONS.md#archive-normalizer](DECISIONS.md#archive-normalizer) / [#control-file-gate](DECISIONS.md#control-file-gate) for what each key actually does. The console validates it with the exact function feeds.yml load does, so a typo here fails in the form rather than at the next Airflow parse. |

Verify before moving on:

```powershell
docker compose exec -T airflow python -c "from reporting_platform.common.context import feeds; f=feeds()['treasury_margin_call']; print(f.raw_table, f.asset_uri, f.parse_filename('marginCalls_20260801.csv'))"
```

A `None` from `parse_filename` means the regex is wrong. Fix it here, not later.

## 2. `dbt/models/raw/_sources.yml` — declare raw to dbt

```yaml
      - name: treasury_margin_call
        description: Margin calls per counterparty and call type, from TREASURY.
        columns:
          - name: margin_call_id
            tests: [not_null]
          - name: counterparty_id
            tests: [not_null]
```

Source-level tests run against raw *before* any modelling, so keep them to
"this arrived at all" checks. Everything about types, ranges and values
belongs in step 4 — raw is all strings by design, so a range test here would
be a string comparison.

Add a per-table `freshness:` block only if the feed's cadence differs from
the source default of 26h warn / 50h error (see how `rating` does it).

`models/raw/` holds no `.sql` and never will — dbt does not build the raw
layer, `ingest_feed.py` does. The folder exists so the tree mirrors the layer
model; the file's own header explains it. Do not add a matching `raw:` key
under `models:` in `dbt_project.yml`: it configures models, there are none,
and dbt warns on every invocation.

## 3. `dbt/models/prepared/<feed>.sql` — the prepared model

**Name the model after the feed, with no layer prefix.** Feed
`treasury_margin_call` gives model `treasury_margin_call`, which materialises as
`prepared.treasury_margin_call` alongside `raw.treasury_margin_call` — same name, the
namespace carries the layer. These were `prep_*` and `rpt_*` once; the prefix
repeated what the namespace already said. dbt keeps models and sources in
separate namespaces, so a model named `trade` and a source `raw.fo_trade` do not
collide.

Copy the nearest existing model rather than starting blank —
`counterparty` for reference data, `trade` for transactional. The
skeleton is fixed and the three macro calls are not optional:

```sql
{{ config(materialized='incremental', unique_key=['business_date','margin_call_id'],
          partition_by=['business_date'], tags=['prepared','reference']) }}

with raw_rows as (
    select *, {{ dedupe_rank(['margin_call_id']) }} as _rn
    from {{ source('raw', 'treasury_margin_call') }}
    where {{ incremental_window('_business_date', 'business_date') }}
),
deduped as (select * from raw_rows where _rn = 1),
cleaned as (
    select
        _business_date                       as business_date,
        {{ clean_string('margin_call_id') }}       as margin_call_id,
        {{ safe_cast(clean_string('call_amount'), 'DECIMAL(18,2)') }} as call_amount,
        {{ parse_date(clean_string('due_date')) }}                  as due_date,
        _source_file                         as source_file,
        _file_version                        as source_file_version,
        {{ audit_columns() }}
    from deduped
)
select * from cleaned
```

- **`incremental_window('_business_date', 'business_date')` takes two
  arguments here and one in reporting.** The source column is raw's
  `_business_date`; the target column is the modelled `business_date`. Passing
  one argument makes Spark bind the unqualified name to the outer query and
  the build fails with `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY`. It only
  fires on the *incremental* path, so a first build against a fresh branch
  will not show it — the first build after publishing to `main` will.
- **`dedupe_rank(business_key)` is what picks the latest `_file_version`.**
  Omit it and a re-delivery doubles the rows.
- **`partition_by=['business_date']` is a retention requirement, not a
  performance one.** Without it retention deletes become full-table rewrites.
- **Use `safe_cast` (`TRY_CAST`), never a bare `CAST`.** A bad value must land
  as NULL and fail a *test*; a load must not fail on unparseable input.
- **Never inline SQL that `macros/engine.sql` already has a macro for.** The
  centralisation is the point: a bare `CAST(x AS VARCHAR)` copy-pasted into
  three models is the defect that macro file exists to prevent.

Derivations are allowed but should restate what the feed already says, not
invent policy. `is_current` here is "in force on the date delivered for";
utilisation and breach flags would be reporting-layer opinions.

## 4. `dbt/models/prepared/_prepared.yml` — the tests

**This is the step that makes write-audit-publish mean anything.** A feed with
no tests builds green forever and publishes whatever it is given.

```yaml
  - name: treasury_margin_call
    columns:
      - name: counterparty_id
        tests:
          - not_null
          - relationships: {to: ref('counterparty'), field: counterparty_id}
      - name: call_amount
        tests:
          - not_null
          - dbt_utils.accepted_range: {min_value: 0, inclusive: true}
      - name: call_type
        tests:
          - accepted_values: {values: ['INITIAL', 'VARIATION']}
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [business_date, margin_call_id]
```

At minimum: `not_null` on the business key, a
`unique_combination_of_columns` on `[business_date, <business_key>]`, and a
`relationships` test on any foreign key. The uniqueness test is what proves
`dedupe_rank` is doing its job — without it a broken dedupe is invisible.

Use `severity: warn` for a test that flags something to investigate rather
than something that should block publication (see `rating.rating_rank`).

## 5. Sample data

Local-stack only, but skip it and the feed has nothing to run against.

**You do not write a generator.** `scripts/generate_feeds.py` hand-writes
generators for the four original feeds only — `trade`, `counterparty`,
`rating`, `treasury_margin_call` — because their *pathologies* are the point and a
definition-driven generator cannot produce them: the two injected failures, the
schema drift, the absent delivery, the non-overlapping SCD2 ranges
`dbt_utils.mutually_exclusive_ranges` tests, the cadence difference between a
daily feed and a weekly one.

Every **other** feed in `feeds.yml` is generated from its own definition by
`reporting_platform/ui/sampledata.py`, the console's generator, which
`generate_feeds.py` calls after the hand-written four. So one command seeds all
of them:

```powershell
docker compose exec -T airflow python -m scripts.generate_feeds --out /opt/platform/seed
```

Look for `<feed>: N files (from feed definition)` in the output. That generator
does three things a naive one would not, each the difference between a file
that tests something and one that does not:

- **It generates for business dates the other feeds delivered on.** A
  `relationships` test compares against reference data on the *same*
  `business_date`, so rows dated where `counterparty` has nothing are
  guaranteed to fail a test that has found nothing wrong with the feed. It
  therefore runs *after* the hand-written four and raises on an empty `seed/`.
- **Foreign keys are drawn from the real reference data**, read out of the
  other feed's seed CSVs. Random `CP#####` values would fail `relationships`
  for reasons that say nothing about the feed under test.
- **It varies the representations the platform exists to normalise** — dates
  alternate between `yyyy-MM-dd` and `yyyyMMdd`, booleans cycle Y/N/true/1/0 —
  so `parse_date` and the boolean CASE are actually exercised.

Values are a function of `(entity, epoch)` rather than `(entity, date)`, so
reference data holds still between deliveries instead of churning every
morning. See `common/volatility.py` and
[DECISIONS.md#generated-data-must-hold-still](DECISIONS.md#generated-data-must-hold-still).

**If you call `sampledata.generate()` directly, pass `types=`.** Without it it
re-guesses from the column *name*, so a column declared `decimal` in
`column_types:` gets a non-numeric value, the prepared model's `safe_cast`
nulls the whole column, and **nothing fails** — because nulling is what
`safe_cast` is for. `scaffold.resolve_types(feed)` is the value to pass. See
[DECISIONS.md#resolve-types-is-authoritative](DECISIONS.md#resolve-types-is-authoritative).

```python
from reporting_platform.common.context import feeds
from reporting_platform.ui import sampledata, scaffold
feed = feeds()["treasury_margin_call"]
sampledata.generate(feed, days=0, types=scaffold.resolve_types(feed))
```

The alternative is the console's **Data** tab, which uploads a real CSV — the
better option once you have one, because it is the actual file shape rather
than a plausible one.

---

## Running it

```powershell
# 1. sample data -> landing
docker compose exec -T airflow python -m scripts.land_feeds --feed treasury_margin_call

# 2. what would be ingested -- MANIFEST keys under ready/, not landing keys.
#    This also reconciles ready/ from landing/, which is what gives a file
#    pushed straight into the bucket a manifest at all.
docker compose exec -T airflow python -m scripts._spark_task pending treasury_margin_call

# 3. landing -> raw, every pending file
docker compose exec -T airflow python -m scripts.bulk_ingest

# 4. raw -> prepared on a throwaway branch, with tests
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt --target spark_local --select path:models/prepared --vars "{nessie_ref: $branch}"

# 5. the new DAG is created PAUSED -- it will never fire until you do this
docker compose exec -T airflow airflow dags unpause ingest_margin_call
```

Step 5 is the one that gets forgotten. `airflow dags list` shows a paused DAG
identically to a running one except for a single boolean column.

> **Adding a dbt model on its own** — a new mart with no new feed behind it —
> is [ADDING-A-MODEL.md](ADDING-A-MODEL.md): two files, and no DAG edit either,
> because Cosmos renders the build graph from the dbt project.

## What you did not have to touch

Worth knowing, because it is where the effort would otherwise go:

- **No DAG file.** `feed_ingest.py` generates one DAG per entry in
  `feeds.yml`.
- **No maintenance registration.** `managed_tables()` derives the prepared and
  reporting sets from the dbt project directory, so writing step 3 registers
  the table. This used to be a sixth file, and was the one step with no error
  if you skipped it -- see
  [DECISIONS.md#managed-tables-are-derived](DECISIONS.md#managed-tables-are-derived).
- **No `prepared_build` schedule.** It ORs the asset of every feed, derived
  from `feeds()`, so a new feed triggers rebuilds automatically.
- **No retention or maintenance policy.** Both are keyed by layer.
- **No raw DDL.** `ingest_feed.ensure_raw_table` builds it from `columns`.

## Two things that will surprise you

**A landed file is not ingested directly; it is normalized first.** The
`normalize` task turns it into a manifest under `ready/` -- the business date,
the objects holding the rows, and how to read them -- and `ingest` consumes
that. For an ordinary CSV nothing is copied and nothing about the feed changes,
but it is why `pending` returns a `ready/...json` key. The single-file CLI form
below still takes a landing key and normalizes on the fly. See
[DECISIONS.md#ready-is-a-derived-index](DECISIONS.md#ready-is-a-derived-index).

**A delivery outside the retention keep-set is landed but never ingested.**
`find_pending` computes the raw keep-set (10 business days + 80 month-ends)
from the dates present in *landing* and skips everything else, so it does not
re-ingest history that retention has already expired. `marginCalls_20260801.csv`
is a Saturday: it is in landing, no other feed delivered on that date, and it
is neither a month-end nor within the last 10 business days — so it is
correctly skipped. That is not a bug, and forcing it in only gives the next
retention run something to delete:

```powershell
# only if you actually want it
docker compose exec -T airflow python -m reporting_platform.ingest.ingest_feed --feed treasury_margin_call --object landing/treasury_margin_call/marginCalls_20260801.csv
```

**A new daily feed reports completeness gaps until it has history.** The check
compares against business dates *other* feeds delivered on. Backfilling
landing, as step 1 above does, is what closes them.
