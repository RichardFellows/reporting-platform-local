# Adding a column to an existing feed

The third of the three "how do I change this" pages, and **the one you will
reach for most**. [ADDING-A-FEED.md](ADDING-A-FEED.md) is for a feed that does
not exist yet and [ADDING-A-MODEL.md](ADDING-A-MODEL.md) for a question nobody
was asking yet; this is for the ordinary event in the life of a feed that has
been delivering for months — the upstream extends its extract, and one more
column starts arriving.

**Three files, one command, and the command is the part that is easy to
forget** — because the thing it changes, the raw table, is the one piece of
this that is not in the git diff.

Read order: change the config, migrate the table, extend the model.

> **The commands below are PowerShell**, because that is what the Windows
> stack this was built on uses. Only two constructs differ elsewhere: a
> continuation is `` ` `` in PowerShell and `\` in bash, and capturing the
> build branch is
>
> ```bash
> branch=$(docker compose exec -T airflow python -m scripts._open_build_branch | tr -d '\r')
> ```
>
> instead of `$branch = (...).Trim()`. `$branch` then reads the same in both.
> On Windows, use Git Bash for anything with single-quoted JSON in it —
> PowerShell mangles the quoting.

---


## 0. What is already true before you start

The column is probably already arriving. Ingest does not refuse a column it
was not told about: `reconcile_schema` puts it in the `_extra_columns` map and
reports it as drift, which is a warning on the ingest task and a value you can
read back:

```powershell
docker compose exec -T airflow python -m scripts.duckdb_console `
  "select _extra_columns from lakehouse.raw.ref_rating limit 5"
```

That is worth doing first. It tells you the column's name **as the file spells
it**, which is what step 1 needs, and it tells you how long it has been
arriving — every delivery that carried it still has its values in that map,
which is what makes a backfill possible later even though nothing backfills
now.

---

## 1. `reporting_platform/config/feeds.yml`

Add it to the feed's `columns:`, at the end.

```yaml
  - name: ref_rating
    convention: ref_src
    ...
    columns:
      - counterparty_id
      - agency
      - rating
      - rating_date
      - outlook
      - watch_status          # new
```

If the file's header does not spell it as a legal identifier, rename it here
rather than downstream — `- watch_status: "Watch Status"` — so raw onwards is
ordinary identifiers and no macro has to quote one. See
[DECISIONS.md#source-column-names](DECISIONS.md#source-column-names).

Two things change as a side effect, both intended:

- **`Feed.schema_version` changes.** It is a digest of the ordered column
  contract, so every row ingested from now on is stamped with a different one
  and a value read years later can be traced to the column list in force when
  it landed. There is nothing to bump by hand.
- **The column stops landing in `_extra_columns`** and starts landing in a
  column of its own. History keeps what it has.

The feed console (<http://localhost:8082>) writes this same edit from a form
and round-trips the file with its comments intact, if you would rather see the
diff than type it.

---

## 2. Migrate the raw table

**This is the step with no file to remind you.** `ensure_raw_table` builds the
DDL from `columns`, but it is `CREATE TABLE IF NOT EXISTS` — a no-op on a
table that already exists — so the table does not follow the config on its
own.

```powershell
docker compose exec -T airflow python -m reporting_platform.ingest.migrate_raw --dry-run
docker compose exec -T airflow python -m reporting_platform.ingest.migrate_raw
```

```json
{ "feed": "ref_rating", "status": "would_add", "columns": ["watch_status"] }
```

It runs on a branch and merges, is idempotent, and commits nothing for a feed
already current. Run it **before** the next ingest or build.

You can skip it and nothing breaks *for that feed*: `ingest()` calls
`ensure_raw_schema` on its own branch, so the first delivery after the change
adds the column itself and reports it as `columns_added`. What you cannot skip
it for is **the feeds that do not deliver that day** — that is exactly the
failure `migrate_raw` was written for, one requirement earlier, and
`platform_housekeeping` runs it first every night for the same reason.

---

## 3. `dbt/models/prepared/<feed>.sql`

Raw now has the column; nothing reads it yet. `prepared` names its columns
explicitly — the `select *` is only in the CTE that reads the source — so add
the line, typed:

```sql
        {{ clean_string('watch_status') }}                 as watch_status,
```

Use the same macro the neighbouring columns use: `clean_string` for text,
`safe_cast(clean_string('x'), 'decimal(28,4)')` for a number,
`parse_date(clean_string('x'))` for a date. `safe_cast` is `try_cast` — an
unparseable value must land as NULL and fail a *test*, not abort the build.

**Expect NULLs in history, and do not test for their absence.** The column was
added as metadata, not backfilled, so every row ingested before step 2 reads
NULL. A `not_null` test on it fails the whole build on data that is correct.
If the column matters enough to test, test it on the dates that have it:

```yaml
      - name: watch_status
        tests:
          - not_null:
              config:
                where: "cob_date >= '2026-08-27'"
```

---

## 4. Build it

```powershell
$branch = (docker compose exec -T airflow python -m scripts._open_build_branch).Trim()
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt `
  --profiles-dir /opt/platform/dbt --target spark_local `
  --select path:models/prepared --vars "{nessie_ref: $branch}"
```

An ordinary incremental run is enough to get the **column**:
`dbt_project.yml` sets `on_schema_change: append_new_columns`, so dbt alters
the target table rather than building green around a column that is silently
absent.

**It does not populate the rows that run does not touch**, which is the part
worth expecting. Measured on a branch: the merge wrote 9 SCD2 versions, those
carried the value, and the other 1,412 rows stayed NULL. What that means
depends on the model's shape:

| Model | What an incremental run populates |
|---|---|
| COB-date incremental (`fo_trade`) | the lookback window, and no further back |
| SCD2 (`ref_rating`, `ref_counterparty`) | only versions cut from here on — **every current row reads NULL until its entity next changes** |

For a current-state dimension that is usually not what you want, so decide
deliberately:

```powershell
# only if history has to carry the value
docker compose exec -T airflow dbt build --project-dir /opt/platform/dbt `
  --profiles-dir /opt/platform/dbt --target spark_local --full-refresh `
  --select ref_rating --vars "{nessie_ref: $branch}"
```

`--full-refresh` is now a choice about **data**, not the only way to get the
**column**. It can only reach as far back as raw does, and raw reads NULL
before step 2 — the values that arrived before the column was declared are in
`_extra_columns`, and getting them out is a backfill nobody has written.

---

## Removing a column is not the mirror image of this

Take it out of `columns:` and the raw table **keeps** it: filled with NULL on
every subsequent ingest, reported as `columns_orphaned` on the ingest result
and on `migrate_raw`, and never dropped.

That is deliberate. Renaming a column in `feeds.yml` is character-for-character
a removal plus an addition, so acting on the removal would delete the history
of a column that was only renamed. Dropping it is a deliberate
`ALTER TABLE ... DROP COLUMN` by someone who knows which of the two edits it
was. See
[DECISIONS.md#a-declared-column-migrates-itself](DECISIONS.md#a-declared-column-migrates-itself).

Until it is dropped or re-declared it also shows up as `unresolved` in
`python -m reporting_platform.lineage --columns`, which exits 1 — the same
condition seen through the other derivation, on purpose.

---

## What you did not have to touch

- **No raw DDL, and no migration script.** Steps 1 and 2 are the whole schema
  change.
- **No `dbt/models/raw/_sources.yml` edit.** The source declares only the
  columns carrying tests — the business key — not the full column list.
- **No DAG edit,** for the usual reason: the ingest DAGs are generated from
  `feeds.yml` and the build DAGs rendered from the dbt project.
- **No backfill.** History reads NULL, and the values that did arrive before
  the column was declared are still in `_extra_columns`.

## See also

- [ADDING-A-FEED.md](ADDING-A-FEED.md) — a feed that does not exist yet
- [ADDING-A-MODEL.md](ADDING-A-MODEL.md) — a model with no new feed behind it
- [DECISIONS.md#a-declared-column-migrates-itself](DECISIONS.md#a-declared-column-migrates-itself)
  — why adding is automatic and removing never is
