# Two places say retention removes superseded versions; nothing does

**Value** low–medium · **Effort** 30 minutes · **Branch** `docs/superseded-versions-are-kept`

## What is wrong (verified 2026-09-14)

```bash
grep -n -i 'superseded' README.md reporting_platform/ingest/ingest_feed.py
#  README.md:535: ... and retention removes superseded versions later, after a grace period.
#  reporting_platform/ingest/ingest_feed.py:11:  superseded ones later. This preserves ...
grep -rln -i '_file_version\|supersed' reporting_platform/retention/ reporting_platform/maintenance/
#  reporting_platform/retention/landing.py     <- says the OPPOSITE, see below
sed -n 8,10p reporting_platform/retention/landing.py
#  ... Superseded re-deliveries are kept too, because the interesting question
#  is usually about the first one.
```

No retention or maintenance code prunes an older `_file_version` from raw, and
the landing sweep keeps superseded re-deliveries deliberately. The only
"grace period" is the table keep-set, which is by COB date, not by version.

## Why it matters

Someone sizing storage, or answering "can we still see the original file we
received for that date?", reads that the older version goes away after a grace
period. It does not — in raw or in landing — and landing's own docstring says
that is policy.

## What done looks like

- [ ] `README.md` and the `ingest_feed.py` module docstring say what happens:
      every version stays in raw for the table keep-set and in landing for
      `keep_years`; `prepared` reads only the newest.
- [ ] `grep -rn -i 'superseded' docs/ README.md CLAUDE.md reporting_platform/`
      finds nothing else describing a version sweep as current.
- [ ] If a version sweep is WANTED, that is a new item with a decision in it,
      not a docs fix.

## Prompt for a new session

```text
Read docs/todo/17-docs-say-retention-removes-superseded-versions.md. Confirm
nothing prunes old _file_versions, then correct README.md and
ingest/ingest_feed.py's docstring to what actually happens.
```
