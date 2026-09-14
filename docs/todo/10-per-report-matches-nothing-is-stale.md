# `DECISIONS.md` says `per_report` matches nothing; the reporting build makes it match

**Value** low–medium · **Effort** 15 minutes · **Branch** `docs/per-report-amended`

## What is wrong (verified 2026-09-14)

```bash
grep -n 'matches nothing' docs/DECISIONS.md
#  2565: **`per_report` matches nothing today**, and it is written down as a forward
#        hook rather than presented as a working mechanism: no publication yet knows
#        which report it is for, so every tag resolves to the default.

grep -rn 'published_tag(' --include=*.py reporting_platform airflow | grep -v 'def '
#  airflow/dags/dbt_builds.py:416:  tag = published_tag(report, as_at, run_id)
sed -n '2174p' reporting_platform/common/context.py
#  return f"published/{report}/{cob_date:%Y-%m-%d}/{run_id}"
```

The reporting build cuts one `published/<report>/<cob_date>/<run_id>` tag per
report, and `TAG_RE` (`retention/retention.py:55`) reads the `report` group
out of it — so a `per_report` entry is honoured. `docs/RETENTION.md:422` and
`DECISIONS.md#an-ingest-is-not-a-publication` already say so. The entry
`#published-tags-are-the-reproducibility-window` still says the opposite.

Found while removing `RETENTION.md`'s paraphrase of that entry (item 06):
exactly the drift that item predicted, already present.

## Why it matters

The paragraph ends by saying that naming a mechanism that does not exist is
the failure this repo keeps rediscovering. The mechanism exists now; the
paragraph is the stale half. Someone deciding whether to set a report's window
reads that it will be ignored.

## What done looks like

- [ ] An `> **Amended.**` block on that paragraph, saying what changed and
      pointing at `#an-ingest-is-not-a-publication`. **Not** a rewrite:
      `DECISIONS.md`'s preamble ("Keeping an entry honest as the code moves")
      labels superseded reasoning rather than deleting it, and the reasoning
      for accepting the report segment early is why retention honoured the
      tags on the day they started appearing.
- [ ] `grep -rn 'matches nothing\|no publication yet knows' docs/ CLAUDE.md`
      finds nothing else stating it as current.
- [ ] `python3 -m tests.run` passes. Nothing here should move it —
      `test_doc_claims` only gates paragraphs claiming something is NOT BUILT
      and quoted error text, and this paragraph is neither.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/10-per-report-matches-nothing-is-stale.md, then
DECISIONS.md's preamble section "Keeping an entry honest as the code moves".

DECISIONS.md#published-tags-are-the-reproducibility-window says per_report
matches nothing, but dbt_builds.py cuts published/<report>/<cob_date>/<run_id>
and retention reads the report out of it. Add an Amended block rather than
rewriting the paragraph, and grep for the claim elsewhere.
```
