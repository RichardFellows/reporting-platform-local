# `ingest_attempt_id` cuts a `manual__` or hand-set Airflow run id mid-token

**Value** low · **Effort** 1 hour · **Branch** `fix/ingest-attempt-id-slug`

## What is wrong (verified 2026-09-24 against `81e9d53`)

```bash
REPORTING_CONFIG_DIR=$PWD/reporting_platform/config python3 -c "
from reporting_platform.common.context import ingest_attempt_id as a
print(a('console__20260814T060211408312', 1))
print(a('manual__2026-08-14T06:02:11.408312+00:00', 1))"
#  20260814T060211408312-a1        <- the inbox and the console: fine
#  -14T060211.4083120000-a1        <- Airflow UI / CLI trigger
curl -s http://localhost:19120/api/v2/trees | grep -o 'snapshot/qa_happy_position/[^"]*'
#  snapshot/qa_happy_position/2026-09-14/osition-snaptag-dag-1-a1   <- a hand-set run id
```

`context.ingest_attempt_id` keeps the last 21 characters of the Airflow run
id. **The common paths are unaffected.** The inbox and the feed console
trigger through `common/airflow_api.trigger`, whose run id is
`console__%Y%m%dT%H%M%S%f` (`airflow_api.py:107`). Its last 21 characters
are exactly the timestamp, so those refs read `…/20260814T060211408312-a1`.

The mid-token names come only from runs whose id is longer or shaped
differently:
- a run triggered from Airflow's own UI or CLI, which gets
  `manual__<iso time>`: `…/-14T060211.4083120000-a1`, with the year and
  month dropped and a leading `-`;
- a hand-set `-r` id: `osition-snaptag-dag-1-a1`.

`wap.branch_name` was changed away from this kind of slicing
(`run_id[-24:]`, "cut mid-token … unreadable") to a slug.

The ids are still unique in practice, because the branch already carries
the feed and COB date. The cost is readability of refs from manual and test
runs, and those tags are kept for years.

## What done looks like

- [ ] The attempt id is a readable slug of the whole run id (as
      `wap.branch_name` does), still unique per attempt, still ending
      `-a<n>`. A `console__` id keeps the name it has today, or changes
      it on purpose.
- [ ] Old tags stay recognisable: anything that parses
      `snapshot/<feed>/<bd>/<run>` (grep for `snapshot/`) still accepts
      them.
- [ ] The `manual__` example in `docs/ARCHITECTURE.md#the-ref-graph`'s table,
      and its link to this file, updated.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/46-ingest-attempt-id-slices-mid-token.md.
Make ingest_attempt_id produce a readable, unique slug for manual__ and
hand-set run ids, without breaking anything that parses existing snapshot
tags.
```
