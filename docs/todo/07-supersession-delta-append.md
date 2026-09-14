# `supersession: delta_append`

**Value** high, if a delta feed is real · **Effort** multi-day · **Branch** `feat/supersession-delta-append`

> **Read [09](09-dedupe-rank-keeps-keys-a-snapshot-dropped.md) first.** Verified
> 2026-09-14: `dedupe_rank` partitions by `(_cob_date, business key)`, so it
> already keeps a key the newest delivery omits — the union-per-COB-date
> behaviour this item describes building. The failure quoted below, "a delta
> feed deduped as a snapshot silently loses every key its newest file omits",
> is not one the macro as written can produce. What this item should ask for
> depends on what 09 decides; nothing below has been rewritten to match yet.

## What exists now

```bash
grep -n 'SUPERSESSION_NOT_BUILT' -A6 reporting_platform/common/context.py | head -20
grep -rn 'delta_append' --include=*.sql dbt/macros/   # no hits: dedupe_rank
                                                      # does not know the mode
```


`supersession:` is declared per feed and validated at load.
`full_snapshot` is the only built mode — each delivery restates the whole
population, newest `_file_version` wins, which is exactly what `dedupe_rank`
implements. `delta_append` and `correction` are recognised and **refused**:

```
feeds.yml: feed 'x' `supersession.mode: delta_append` is described in the
requirements (REQ-202) but NOT BUILT -- each delivery carries only what
changed, so a COB date's population is the UNION of its deliveries rather
than the newest one [...]
```

The refusal is the feature: a delta feed deduped as a snapshot silently loses
every key its newest file omits, with nothing raising anywhere.

## What building it involves

This is a design piece, not a fill-in:

* **`dedupe_rank` must rank ACROSS versions rather than select the newest.** A
  COB date's population becomes the union of its deliveries, most recent value
  per business key.
* **Deletes need a tombstone convention the feed does not have.** In a snapshot
  feed a key disappearing IS the delete; in a delta feed it means "unchanged".
  Without a tombstone, nothing can ever be removed — decide whether that is a
  declared column, a control-file field, or unsupported.
* **`correction` is a third shape**, not a variant: a delivery restating keys
  of an EARLIER COB date crosses the partition `dedupe_rank` ranks within, so
  the corrected date has to be rebuilt rather than the delivered one.

## What done looks like

- [ ] `SUPERSESSION_NOT_BUILT` loses the entry, `SUPERSESSION_MODES` gains it.
- [ ] `dedupe_rank` takes the mode and implements the union ranking.
- [ ] A tombstone decision, written down in `DECISIONS.md` whichever way it
      goes — including "not supported, and here is what that costs".
- [ ] `tests/test_supersession.py` covers a two-delivery date where the second
      omits a key present in the first, and asserts the key survives.
- [ ] `tests/test_doc_claims.py` will fail until every doc that calls
      `delta_append` NOT BUILT is updated — that is the gate working.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/07-supersession-delta-append.md, then
DECISIONS.md#supersession-is-declared-not-assumed.

Build `supersession.mode: delta_append`. Start with the tombstone question --
how a delta feed expresses a DELETE -- because the answer decides the rest of
the design, and "not supported" is an acceptable answer if it is written down
with its cost. Do not start by editing dedupe_rank.

Note that tests/test_doc_claims.py will fail while the docs still call it NOT
BUILT; that is the gate doing its job, and those docs are part of the change.
```
