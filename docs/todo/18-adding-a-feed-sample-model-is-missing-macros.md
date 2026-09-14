# The sample prepared model in `ADDING-A-FEED.md` misses two macros every real one calls

**Value** low–medium · **Effort** 30 minutes · **Branch** `docs/adding-a-feed-sample-model`

## What is wrong (verified 2026-09-14)

```bash
awk '/^```sql/{f=1;next} /^```/{f=0} f' docs/ADDING-A-FEED.md | grep -c 'known_as_of\|source_provenance'
#  0
grep -l 'known_as_of' dbt/models/prepared/*.sql | wc -l
grep -n 'known_as_of\|source_provenance' reporting_platform/ui/scaffold.py | head
```

Every prepared model, and the console's scaffold template, filters raw with
`known_as_of()` and projects the provenance columns through
`source_provenance()`. The hand-written example in `ADDING-A-FEED.md` does
neither.

## Why it matters

A model copied from the doc builds green and is silently wrong twice: an
as-of build (`--vars '{knowledge_time: ...}'`) reads every delivery regardless
of the knowledge time, and `registry.run_input` — derived from the prepared
models' `delivery_id` — has nothing to read for that feed. Neither fails.

## What done looks like

- [ ] The sample matches what `ui/scaffold.py` emits today (strategy, rank,
      `known_as_of()`, `source_provenance()`), or is replaced by a pointer to
      the scaffold plus the one thing the doc needs to show.
- [ ] Check the doc's sample AFTER item 09 merges: 09 changes the strategy
      and the rank, and the sample must follow.
- [ ] Consider a test that renders the doc's SQL block the way
      `tests/test_dedupe_rank.py` renders models, so the sample cannot drift
      again.

## Prompt for a new session

```text
Read docs/todo/18-adding-a-feed-sample-model-is-missing-macros.md. Make the
sample prepared model in docs/ADDING-A-FEED.md match ui/scaffold.py's
template, and if cheap, gate it with a test that renders the block.
```
