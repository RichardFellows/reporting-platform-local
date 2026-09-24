# A reporting publication tags `main`'s head, not the commit its merge made

**Value** medium · **Effort** 1–2 hours · **Branch** `fix/published-tag-merge-commit`

## What is wrong (reasoned from the code 2026-09-24 against `81e9d53`, not reproduced)

```bash
grep -n 'merged = n.get_reference\|create_tag(tag, from_ref="main")' reporting_platform/transform/wap.py
#  220:    merged = n.get_reference("main")["reference"]["hash"]
#  231:                n.create_tag(tag, from_ref="main")
grep -n 'resultantTargetHash' reporting_platform/ingest/ingest_feed.py
#  the ingest already takes the hash from the merge's own response
```

`wap.publish` merges the build branch, then reads `main`'s head **again** and
uses it twice. It is recorded as the run's `merged_hash`, and every
`published/<report>/<as_at>/<run key>` tag is cut from `main` with no hash.
That head is the publication's merge commit only while nothing else merges
in between. This is the defect
[DECISIONS.md#a-snapshot-tag-names-its-merge-commit](../DECISIONS.md#a-snapshot-tag-names-its-merge-commit)
fixed for snapshot tags, and it is still here for published tags.

In Airflow, `publish` holds the `lakehouse_write` pool across the merge and
the tags, and so do the ingest tasks. At the default of one slot, nothing
can merge in between. Outside that, something can:

- `LAKEHOUSE_WRITE_SLOTS` above 1;
- `python -m reporting_platform.transform build reporting`, the standalone
  `runner`, `scripts.bulk_ingest` and the `ingest` CLI, none of which enter
  the pool.

When it happens, the published tag, which is the reproducibility pin kept
for years, pins an ingest or build that is not part of the publication. The
run record's `merged_hash` then names a commit the run did not make.

## What done looks like

- [ ] `publish` takes `resultantTargetHash` from `Nessie.merge`'s response,
      as `ingest_feed._merge_ingest_branch` does, and uses it for both
      `merged_hash` and `create_tag(..., hash=...)`.
- [ ] A result with no hash is not tagged at the head as a fallback. It is
      recorded on the run as an error, as `steps.record_snapshot` does.
- [ ] A test with a fake Nessie whose `main` moves between the merge and
      the tag, showing the tag names the merge commit.
- [ ] `docs/ARCHITECTURE.md#the-ref-graph`'s caveat about this removed.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/45-published-tags-are-cut-at-mains-head.md.
Make wap.publish tag and record the commit its own merge made, as the
snapshot tag already does. Test it with a main that moves in between.
```
