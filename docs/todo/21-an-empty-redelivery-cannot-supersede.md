# A re-delivery with no rows cannot supersede anything

**Value** medium · **Effort** ½–1 day (a design) · **Branch** `feat/empty-delivery-supersedes`

## What is wrong (verified 2026-09-14)

```bash
grep -n 'MAX(_file_version) OVER\|max(_file_version)' dbt/macros/engine.sql
#  "newest delivery" is the max _file_version among RAW ROWS for the date
grep -n 'expected_min_rows: int = 0' reporting_platform/common/context.py
grep -rn 'expected_min_rows' reporting_platform/config/feeds/
#  fo_trade.yml: 100;  conventions/ref_src.yml: 10 (the three ref_* feeds inherit it)
```

Under item 09's decision the newest delivery restates its whole COB date. But
"newest delivery" is computed from raw ROWS (`dedupe_rank`'s window and
`newest_file_version()`), and a delivery with zero rows writes no raw row. A
header-only re-delivery ingests (`expected_min_rows` defaults to 0), gets
`_file_version` 2, and is invisible: version 1 still wins,
`insert_overwrite` rewrites the date with version 1's population, and the
SCD2 models retract nothing.

It is not even ingested once. `already_ingested()` (`ingest/arrival.py`)
decides what is done by reading `_source_file` back out of raw, and a zero-row
ingest writes no row carrying it — so `find_pending` offers the same delivery
again on every run, and it is re-ingested (branch, empty append, merge) every
time until retention drops its date:

```bash
grep -n 'def already_ingested' -A14 reporting_platform/ingest/arrival.py | grep -n '_source_file'
```

`DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date` names the rank half as a
risk; nothing enforces or implements it. The four current feeds set floors
(100 on `fo_trade`, 10 inherited from the `ref_src` convention), so they refuse an empty file at ingest; a scaffolded feed on the
default does not.

## Why it matters

An empty snapshot is a real statement — "no open positions today", "every
facility closed" — and it is the one restatement this mechanism cannot
express. Worse, it is silent: the date keeps the previous delivery's rows and
nothing says a newer delivery was ignored.

## What done looks like

- [ ] A decision in `DECISIONS.md`: is an empty `full_snapshot` delivery an
      empty date, or a refusal? If a refusal, `expected_min_rows: 0` on a
      `full_snapshot` feed is refused at LOAD (or its default changes), with
      the reason. If an empty date, "newest delivery" has to come from
      something that records a delivery without rows — `registry.delivery`
      is an index, not a ledger, so check what it may carry before using it.
- [ ] Either way, `already_ingested` stops treating an empty delivery as
      pending for ever — an "empty date" answer has to change it as well as
      the rank.
- [ ] A test of whichever answer, in the `tests/test_dedupe_rank.py` harness,
      and one that an empty delivery is ingested once.
- [ ] **Not yet reproduced by running an ingest** — the analysis above is
      from the code. Land a header-only re-delivery on a throwaway branch
      first and confirm both halves before designing.

## Prompt for a new session

```text
Read CLAUDE.md, docs/todo/21-an-empty-redelivery-cannot-supersede.md and
DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date. A zero-row
re-delivery writes no raw rows, so the newest-delivery rank never sees it.
Decide whether an empty full_snapshot delivery is an empty date or a refusal,
bring the decision back, then build it.
```
