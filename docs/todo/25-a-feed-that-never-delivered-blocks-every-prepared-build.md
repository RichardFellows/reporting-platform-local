# A declared feed that has never delivered blocks every prepared build

**Value** high · **Effort** ½–1 day, most of it the decision · **Branch** `fix/undelivered-feed-blocks-prepared`

## What is wrong (verified 2026-09-24 against `f1493ca`)

`prepared_build` builds **every** prepared model on each run
(`select="path:models/prepared"`, `airflow/dags/dbt_builds.py`). The model
of a feed whose raw table does not exist yet fails. `keep_failed_branch`
then keeps the branch and `publish` never runs, so **none** of the
prepared models are published, including those of feeds that did deliver.
`reporting_build` never fires either, because the prepared asset never
updates.

On the local stack as of this date, three declared feeds have no raw
table on `main`, and all 11 `prepared_build` runs have failed:

```bash
curl -s http://localhost:19120/api/v2/trees/main/entries |
  python3 -c "import json,sys; print(sorted('.'.join(e['name']['elements']) for e in json.load(sys.stdin)['entries']))"
#  ['raw', 'raw.fo_trade', 'raw.qa_happy_position', 'raw.ref_rating']

docker compose exec -T airflow airflow dags list-runs -d prepared_build -o plain | awk 'NR>1{print $3}' | sort | uniq -c
#  11 failed

run=$(docker compose exec -T postgres psql -U platform -d airflow -Atc \
  "select run_id from dag_run where dag_id='prepared_build' order by start_date desc limit 1")
docker compose exec -T airflow bash -c \
  "grep -h TABLE_OR_VIEW_NOT_FOUND '/opt/airflow/logs/dag_id=prepared_build/run_id=$run/'task_id=dbt.*/attempt=*.log"
#  [TABLE_OR_VIEW_NOT_FOUND] The table or view `raw`.`ref_counterparty` cannot be found.
#  [TABLE_OR_VIEW_NOT_FOUND] The table or view `raw`.`ref_collateral` cannot be found.
#  [TABLE_OR_VIEW_NOT_FOUND] The table or view `raw`.`qa_headerless_position` cannot be found.
```

Nothing creates a raw table before a feed's first ingest.
`ensure_raw_table` runs only inside an ingest, and `ingest/migrate_raw.py`
skips a feed with no table on purpose ("the first ingest creates it").

## Why it matters

The ordinary way to onboard a feed is to add its config and its prepared
model, then wait for the first delivery. From that moment until it
arrives, no feed publishes. The README states the opposite: "A late feed
does not block the feeds that did arrive." The CI build tier does not
catch this because its seed delivers every feed.

## What done looks like

- [ ] Decide the mechanism (see below) and record it in `DECISIONS.md`.
- [ ] A feed declared in `config/feeds/` and never delivered does not stop
      the other feeds' prepared models from publishing.
- [ ] Verified live: add a feed and its model without delivering it,
      trigger `ingest_fo_trade`, and see `prepared_build` and
      `reporting_build` succeed with `dataset_triggered__` run ids.
- [ ] A config-level test that fails on today's behaviour, if the
      mechanism allows one.

## Options, not yet decided

1. **Create an empty raw table for every declared feed at deploy time**
   (`airflow-init`, and the Helm `job-platform-init`), from the same
   contract `ensure_raw_table` uses. The model builds against zero rows.
   The cost is that completeness then reads `no data` instead of `no table`
   for a feed that has never delivered, and `MONITORING.md` treats those as
   different answers. The monitor would need another way to tell "never
   delivered" apart.
2. **Guard in the model or the source**: skip or empty-select when the raw
   relation is absent. The prepared table is then missing, and every
   reporting model that `ref()`s it fails in the same way one layer down.
3. **Build only what the triggering asset feeds**. This is closest to the
   stated design (no feed blocks another), but `any_of()` does not tell the
   run which asset fired, and the lifecycle gate and input set assume one
   build per layer.

## Watch out for

- `CLAUDE.md`: a subject that could not be READ is not a subject that is
  EMPTY. Option 1 turns one into the other on purpose. Say so wherever it
  becomes visible (completeness, COB Status, lineage `not derivable`).
- `migrate_raw` deliberately does not create tables. Do not make it create
  them without re-reading `#provenance-is-added-not-backfilled`.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/25-a-feed-that-never-delivered-blocks-every-prepared-build.md.
Reproduce it live (the commands are in the item). Then propose one of the
three options with its cost to the completeness monitor, and wait for a
decision before implementing it.
```
