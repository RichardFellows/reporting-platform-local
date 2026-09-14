# `next_file_version` treats an unreadable raw table as version 1

**Value** high once 09 lands · **Effort** 1–2 hours · **Branch** `fix/next-file-version-unreadable`

## What is wrong (verified 2026-09-14)

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

It becomes a correctness defect, not a cosmetic one, under item 09's decision
(`dedupe_rank` selects the newest `_file_version` per COB date, whole). A
re-delivery mis-numbered as version 1 alongside an existing version 1 ties
with the delivery it was meant to replace, and a version 2 that already exists
outranks it — the correction silently loses. Before 09 it could at worst
reorder per-key winners; after it, it decides which whole file is the date.

A table that does not exist yet (`TABLE_OR_VIEW_NOT_FOUND`, a feed's first
delivery) is the one failure that legitimately means "version 1" — the same
three-way split the completeness check already makes (`no data` / `no table` /
`unreadable`).

## What done looks like

- [ ] `TABLE_OR_VIEW_NOT_FOUND` → 1; any other exception propagates and fails
      the ingest, naming the table.
- [ ] A test for each branch (a stand-in `spark.sql` raising each kind), in
      `tests/`.
- [ ] Check the other callers of the `_file_version` sequence while there:
      `_file_version` is `MAX+1` at ingest time, which is only a safe order
      because the `lakehouse_write` pool has one slot — write that down where
      the function is, since the pool size is now load-bearing for
      supersession.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/15-next-file-version-reads-unreadable-as-version-1.md.
next_file_version's `except Exception: return 1` turns an unreadable table
into a first delivery. Split "no table" from "unreadable" the way the
completeness check does, and test both.
```
