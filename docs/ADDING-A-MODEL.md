# Adding a dbt model

The companion to [ADDING-A-FEED.md](ADDING-A-FEED.md). A feed brings data *in*;
a model turns it into something someone reads.

**Two files, and no DAG edit.** The build DAGs are rendered from the dbt
project by Astronomer Cosmos on every parse, so a new `.sql` becomes a new
Airflow task on its own — verified live: a model added to
`models/prepared/` appeared as `dbt.<name>_run` in `prepared_build` within one
parse interval, with no restart and nothing to register.

That is the same property `feeds.yml` gives the ingest DAGs, and it is the
answer to "where do I wire this up?" — you don't.

---

## Which layer

| | `prepared` | `reporting` |
|---|---|---|
| Reads from | `source('raw', …)` | `ref()` of prepared, or of another reporting model |
| Job | conform, type, deduplicate — **restate** what the feed said | aggregate, join, compare — **assert** something the business cares about |
| One per | feed | question someone asks |

The rule that keeps them apart: **`prepared` may not hold an opinion.**
"In force on the date delivered for" is a restatement and belongs there;
"breached its limit" is a policy and belongs in `reporting`. If you find
yourself encoding a threshold in `prepared`, it is a reporting model.

A `prepared` model is nearly always one-per-feed and is covered by
[ADDING-A-FEED.md](ADDING-A-FEED.md) steps 3 and 4. This page is mostly about
the reporting layer, where models are added without a new feed behind them.

---

## 1. `dbt/models/<layer>/<name>.sql`

Copy the nearest existing model rather than starting from blank —
`exposure_by_country` for a rollup, `exposure_change` for a period comparison,
`counterparty_exposure` for a join across prepared tables.

```sql
{{
  config(
    materialized='incremental',
    unique_key=['business_date', 'country_code'],
    partition_by=['business_date'],
    tags=['reporting']
  )
}}

select
    business_date,
    ...
from {{ ref('counterparty_exposure') }}
where {{ incremental_window('business_date') }}
group by business_date, ...
```

Five things that are not optional:

- **`partition_by=['business_date']` is a retention requirement, not a
  performance one.** Retention deletes by business date; without the partition
  those deletes become full-table rewrites. Every managed table leads its
  partition spec with `business_date`. See [RETENTION.md](RETENTION.md).
- **`incremental_window(...)` takes one argument in `reporting` and two in
  `prepared`.** In prepared the source column is raw's `_business_date` and the
  target is the modelled `business_date`, so both must be named. In reporting
  both are already `business_date`. Passing one argument in prepared makes
  Spark bind the unqualified name to the outer query and the build fails with
  `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY` — and only on the *incremental*
  path, so a first build against a fresh branch will not show it. The first
  build after publishing to `main` will.
- **`unique_key` must actually be unique**, and a
  `dbt_utils.unique_combination_of_columns` test on the same columns is what
  proves it. `incremental_strategy: merge` is set project-wide; a non-unique
  key silently merges rows together.
- **Never inline SQL that `macros/engine.sql` already has a macro for**
  (`safe_cast`, `clean_string`, `parse_date`, `dedupe_rank`, `audit_columns`).
  Centralising them is the point — a bare `CAST(x AS VARCHAR)` copy-pasted into
  three models is the defect that file exists to prevent. Note also that Spark
  3.x rejects bare `VARCHAR` without a length: use `string`.
- **Aggregates must `ref()` the detail model, not re-derive from `prepared`.**
  `exposure_by_country` reads `counterparty_exposure` so the rollup reconciles
  to the detail *by construction*. Two independent derivations eventually
  disagree, and finding out why costs a week.

## 2. `dbt/models/<layer>/_<layer>.yml` — the tests

**This is the step that makes write-audit-publish mean anything.** A model with
no tests builds green forever and publishes whatever it is given; the merge to
`main` is gated on `dbt test`, so an untested model has effectively opted out
of the gate.

```yaml
  - name: exposure_by_country
    description: >
      What it is, and who reads it.
    columns:
      - name: country_code
        tests: [not_null]
      - name: counterparty_count
        tests:
          - dbt_utils.accepted_range: {min_value: 1, inclusive: true}
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [business_date, country_code]
```

At minimum: `not_null` on the grain columns, a
`unique_combination_of_columns` on the full grain, and — for an aggregate — a
`dbt_utils.equality` or row-count relationship back to the model it summarises,
which is what catches a join that started dropping rows.

Use `severity: warn` for something to investigate rather than something that
should block publication. `counterparty_exposure.legal_name` is the worked
example: a counterparty present in trades but missing from the reference feed
is worth surfacing, but blocking the entire risk report over it is worse than
publishing it with a null.

## 3. If it is a `prepared` model: `PREPARED_TABLES`

Add the model name to `PREPARED_TABLES` in
`reporting_platform/common/context.py` (or `REPORTING_TABLES` for a reporting
one). **This is the one step with no error if you skip it** — the table simply
never gets compacted, its snapshots never expire, and retention never trims it.
It grows quietly. See [ADDING-A-FEED.md](ADDING-A-FEED.md) step 5.

---

## Checking it before you build

`dbt parse` compiles the whole project without touching Spark, in about five
seconds. It catches a bad `ref()`, a macro typo and malformed YAML — which is
most of what goes wrong — long before a build would:

```powershell
docker compose exec -T airflow dbt parse --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt
```

Then confirm Cosmos picked it up. This is the whole "no DAG edit" claim, and it
is one command:

```powershell
docker compose exec -T airflow airflow tasks list prepared_build
```

Your model should be there as `dbt.<name>_run`. If it is not, wait one parse
interval — Cosmos caches the rendered graph against a hash of the project
files, so the new task appears when the scheduler next parses, not instantly.

## Building it

On a throwaway branch, which is the point of the platform — never straight at
`main`:

```powershell
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt --target spark_local --select exposure_by_country+ --vars "{nessie_ref: $branch}"
```

`--select <model>+` builds the model and everything downstream of it, which is
what you want: a change to a shared model like `counterparty_exposure` moves
every mart that `ref()`s it, and building it alone proves nothing about them.

Or let the DAG do it, which also exercises the branch-and-merge:

```powershell
docker compose exec -T airflow airflow dags trigger reporting_build -r my_change_1
```

A failed model leaves the `build/*` branch in place holding the exact bad data
— query it by passing `nessie_ref` — and `main` untouched. `build/*` branches
are swept after 120h.

## What you did not have to touch

- **No DAG file.** Cosmos renders the graph from the project.
- **No task dependencies.** They come from your `ref()`s.
- **No retention or maintenance policy.** Both are keyed by *layer*, not by
  table — though see step 3 for the one list that is hand-maintained.
- **No schedule.** `reporting_build` runs when `prepared_build` publishes.

---

## Related

| | |
|---|---|
| [ADDING-A-FEED.md](ADDING-A-FEED.md) | the six files a new *feed* touches |
| [ARCHITECTURE.md](ARCHITECTURE.md) | § *How the dbt builds become Airflow tasks* — why Cosmos is configured the way it is |
| [FEED-UI.md](FEED-UI.md) | the console on :8082, which scaffolds a prepared model from a form |
