# `dedupe_rank` keeps keys a `full_snapshot` re-delivery dropped

**Value** high · **Effort** half a day to two days, depending on the decision · **Branch** `fix/dedupe-rank-full-snapshot`

## What is wrong (verified 2026-09-14)

`full_snapshot` is documented as "each delivery restates the whole population
for its COB date, newest `_file_version` wins". The macro does not select the
newest FILE. It partitions by the COB date **and the business key**, so it
selects the newest version of each KEY — and a key the newest file omits keeps
its row from the older file:

```bash
sed -n '312,316p' dbt/macros/engine.sql
#   ROW_NUMBER() OVER (
#     PARTITION BY _cob_date, <business keys>
#     ORDER BY _file_version DESC, _row_number DESC

python3 - <<'PY'
import duckdb
con = duckdb.connect()
con.execute("""create table raw as select * from (values
  ('2026-09-01', 1, 1, 'T1'), ('2026-09-01', 1, 2, 'T2'),   -- v1: T1, T2
  ('2026-09-01', 2, 1, 'T1')                               -- v2 restates: T1 only
) t(_cob_date, _file_version, _row_number, trade_id)""")
print(con.execute("""select trade_id, _file_version from (
  select *, row_number() over (partition by _cob_date, trade_id
            order by _file_version desc, _row_number desc) _rn from raw)
  where _rn = 1 order by trade_id""").fetchall())
PY
#  [('T1', 2), ('T2', 1)]    <- T2 survives, from the file v2 replaced
```

No prepared model compensates: `_rn = 1` is the only filter in all four, and
`_file_version` is only carried through as `source_file_version`
(`grep -rn '_file_version' dbt/models/prepared/`).

## Why it matters

Two ways, and the second is the larger.

* **The built mode is wrong.** A snapshot re-delivery that drops a cancelled
  trade, a closed counterparty or a withdrawn rating leaves it in `prepared`,
  and in every report built from it, with nothing raising. The scaffolded
  tests cannot see it: `_prepared.yml:142` says uniqueness over
  `[cob_date, <business key>]` "is enough to prove dedupe_rank works", and
  uniqueness holds under either reading.
* **The refusal protecting the unbuilt modes names the wrong failure.** Five
  places say a delta feed "deduped as a snapshot silently loses every key its
  newest file omits" — `CLAUDE.md:241`, `dbt/macros/engine.sql:298`,
  `docs/REQUIREMENTS.md:69`, `reporting_platform/common/context.py:243`, and
  `docs/ADDING-A-FEED.md:106`. Under the macro as written that cannot happen:
  it already takes the union of a date's deliveries. That changes the premise
  of item 07, which should be re-read once this is decided.

## What done looks like

- [ ] **A decision, written down in `DECISIONS.md`**: is the macro wrong, or
      is the documented meaning of `full_snapshot` wrong? Check the seed
      before deciding — whether any `_v2` delivery in `seed/` or `seed_clean/`
      omits a key its `_v1` carried, and what the published marts currently
      show for it.
- [ ] If the macro is wrong: it selects rows of the newest file per COB date,
      and a re-delivery that drops a key REMOVES that key from `prepared` on
      an incremental run, not only on `--full-refresh`.
- [ ] If the docs are wrong: the five sites above, and the `supersession:`
      refusal text, stop describing a failure the code cannot produce and
      name deletion as what a delta feed actually lacks.
- [ ] A test with one COB date, two deliveries, the second omitting a key —
      asserting whichever answer was decided. Against the real engine
      (DuckDB, the way `test_sniff.py` does it), not a stand-in.

## Watch out for

* **Fixing the rank is not enough on its own.** `dbt_project.yml` sets
  `+incremental_strategy: merge`, and a merge with no delete clause never
  removes a row already in the target. A key merged in by an earlier run stays
  whatever the rank now says. Whatever the fix is has to rewrite the COB date,
  not merge into it — verify on a Nessie branch, not by reading the SQL.
* `ref_counterparty` and `ref_rating` feed SCD2 dimensions. A key dropped from
  a snapshot should close its validity interval; check what they do with a key
  that stops appearing before and after.
* `known_as_of()` filters in the same `where` as the rank. Whatever "newest
  file" becomes must be computed AFTER that filter, or an as-of build ranks
  against deliveries that did not exist at its knowledge time.
* `tests/test_doc_claims.py` gates quoted error text. If the refusal message
  changes, any doc quoting it must change in the same commit.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/09-dedupe-rank-keeps-keys-a-snapshot-dropped.md,
then DECISIONS.md#supersession-is-declared-not-assumed.

dedupe_rank partitions by (_cob_date, business key), so under full_snapshot a
key the newest delivery omits survives from the older one. Run the item's
DuckDB snippet to see it. Do NOT start by editing the macro: first establish
from the seed and the published marts whether anything depends on the current
behaviour, then write down whether the macro or the documented meaning of
full_snapshot is wrong, and bring that decision back before building.

Remember the merge strategy: a corrected rank does not delete rows an earlier
incremental run already merged. Verify any fix on a throwaway Nessie branch.
```
