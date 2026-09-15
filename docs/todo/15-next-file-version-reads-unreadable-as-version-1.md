# `next_file_version` treats an unreadable raw table as version 1

**Value** high · **Effort** 2–4 hours · **Branch** `fix/next-file-version-unreadable`

## What is wrong (verified 2026-09-15, after 09 merged)

```bash
grep -n 'def next_file_version' -A10 reporting_platform/ingest/ingest_feed.py
#  try:
#      row = spark.sql("SELECT COALESCE(MAX(_file_version), 0) ... WHERE _cob_date = ...")
#      return int(row["v"]) + 1
#  except Exception:
#      return 1
```

Any failure reading the raw table — Nessie down, a catalog error, a
permissions problem, a schema the query cannot compile against — becomes
"this COB date has never been delivered", and the delivery is written as
`_file_version = 1`.

## Why it matters

It is `CLAUDE.md`'s rule exactly: a subject it could not READ reported as
EMPTY. The `COALESCE(..., 0)` already handles the genuinely empty case, so the
`except` only ever catches the unreadable one.

It is a correctness defect, not a cosmetic one, since item 09's decision
(`dedupe_rank` selects the newest `_file_version` per COB date, whole). A
re-delivery mis-numbered as version 1 alongside an existing version 1 ties
with the delivery it was meant to replace, and a version 2 that already exists
outranks it — the correction silently loses. Before 09 it could at worst
reorder per-key winners; after it, it decides which whole file is the date.

A MISSING table is not the exception either. The one call site
(`ingest_feed.py`, in the branch ingest) runs `next_file_version` straight
after `ensure_raw_table` and `ensure_raw_schema` have created and reconciled
that table on the branch, so by then a `TABLE_OR_VIEW_NOT_FOUND` means a wrong
ref or a wrong name — also unreadable, not a first delivery.

## What done looks like

- [ ] Every exception propagates and fails the ingest, naming the table and
      the ref. If a caller is ever added that can run before the table
      exists, it handles that itself.
- [ ] A test with a stand-in `spark.sql` that raises, asserting the ingest
      fails rather than writing version 1.
- [ ] **Concurrent ingests of one COB date get the same version.** Each
      ingest reads `MAX(_file_version)` on its OWN branch, forked from main,
      so two in flight at once both compute the same `MAX+1`. Airflow's
      `lakehouse_write` pool (one slot) serialises the DAG path, but
      `scripts/bulk_ingest.py` runs ingests in its own subprocesses outside
      Airflow, where no pool applies — so the invariant is already not
      guaranteed. After 09 the two files tie. Either make the version
      allocation safe at merge time, or make `bulk_ingest` refuse to run
      alongside the DAGs, and write down which; do NOT add a comment
      claiming the pool guarantees it.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/15-next-file-version-reads-unreadable-as-version-1.md.
next_file_version's `except Exception: return 1` turns an unreadable table
into a first delivery. Make every read failure fail the ingest (the
one call site runs after the table is created on the branch), test it, and
decide how concurrent ingests of one COB date are kept from sharing a version
-- the lakehouse_write pool does not cover scripts/bulk_ingest.py.
```
