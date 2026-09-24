# `ingest_attempt_id` cuts the Airflow run id mid-token

**Value** low · **Effort** 1 hour · **Branch** `fix/ingest-attempt-id-slug`

## What is wrong (verified 2026-09-24 against `81e9d53`)

```bash
REPORTING_CONFIG_DIR=$PWD/reporting_platform/config python3 -c "
from reporting_platform.common.context import ingest_attempt_id as a
print(a('manual__2026-08-14T06:02:11.408312+00:00', 1))"
#  -14T060211.4083120000-a1
curl -s http://localhost:19120/api/v2/trees | grep -o 'snapshot/qa_happy_position/[^"]*'
#  snapshot/qa_happy_position/2026-09-14/osition-snaptag-dag-1-a1
```

`context.ingest_attempt_id` keeps the last 21 characters of the Airflow run
id. For the `manual__<timestamp>` ids that the inbox and the console trigger
with, that drops the year and month and leaves a leading `-`, so every ingest
branch and snapshot tag reads `…/-14T060211.4083120000-a1`. For a hand-set
run id, it cuts the id mid-word (`osition-snaptag-dag-1`). `wap.branch_name`
was changed from exactly this (`run_id[-24:]`, "cut mid-token … unreadable")
to a slug. The ingest side never was.

The ids are still unique in practice, because the branch already carries the
feed and COB date. The cost is readability of the refs a person reads when
diagnosing, and the tag names kept for years.

## What done looks like

- [ ] The attempt id is a readable slug of the whole run id (as
      `wap.branch_name` does), still unique per attempt, still ending
      `-a<n>`.
- [ ] Old tags stay recognisable: anything that parses
      `snapshot/<feed>/<bd>/<run>` (grep for `snapshot/`) still accepts
      them.
- [ ] The example names in `docs/ARCHITECTURE.md#the-ref-graph` updated.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/46-ingest-attempt-id-slices-mid-token.md.
Make ingest_attempt_id produce a readable, unique slug, without breaking
anything that parses existing snapshot tags.
```
