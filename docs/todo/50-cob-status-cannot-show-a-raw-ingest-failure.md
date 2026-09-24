# COB Status reads a failed or refused raw ingest as PROCESSING, and cannot see a failed validation at all

**Value** medium · **Effort** ½–1 day · **Branch** `fix/cob-status-raw-failure`

## What is wrong (checked 2026-09-24 against `81e9d53`, reasoned from the code, not reproduced live)

`feed_status.status_of` checks `has_delivery` (any `registry.delivery` row)
**before** the receipt's `failed`. `registry.delivery` is written at
normalize (`ingest/normalization.py`, `register_v2_quietly`), which comes
before `ingest_raw`. So when `ingest_raw` fails, and when it is refused (a
declared md5 or row count mismatch, the row floor, schema drift under
`fail`), the feed reads `PROCESSING` and never `FAILED`. It stays that way
until something commits:

```bash
REPORTING_CONFIG_DIR=$PWD/reporting_platform/config python3 -c "
from datetime import date, datetime, timezone
from reporting_platform.monitoring.feed_status import status_of
print(status_of(expected=True, committed=False, has_delivery=True,
      receipt_status='failed', expected_by='07:00', cob_date=date(2026,9,21),
      now=datetime(2026,9,23,tzinfo=timezone.utc)))"
#  PROCESSING
```

`status_of`'s docstring intends rule 2 for an *earlier* Transport's failure
("regardless of what any earlier failed Transport for the same date did"),
but the same rule also swallows the *same* Transport's failure one step
later. `OPERATIONAL-CONTROL-PLANE.md` §6 says a FAILED row that never
surfaces is "exactly the case an operator most needs to see".

Two related gaps, from the same reading:

- `build_report` drops receipts whose `feed` is NULL, and a receipt gets a
  feed only at `create_delivery`. So a `validate_transport` failure never
  appears as `FAILED`, and the feed reads `WAITING` or `MISSING`.
- `ingest_raw` writes the receipt only when it fails. A retry that then
  succeeds leaves the receipt at `failed`. COB Status still says `COMPLETE`,
  because `committed` wins, but `registry.transport_receipt` itself is wrong.

## What done looks like

- [ ] Reproduced live first: a Transport whose declared md5 does not match,
      carried through `transport_ingest`. Record what the COB Status page
      shows for its feed.
- [ ] A decision on the precedence. For example, only count a failed
      receipt as outranked by a `registry.delivery` row whose
      `delivery_id` is different, or newer. Write it down in `status_of`'s
      docstring and in `OPERATIONAL-CONTROL-PLANE.md` §5, and update the
      flowchart there.
- [ ] Either give a `validate_transport` failure a place on the page, or
      state in §5 that it is deliberately not shown.
- [ ] A test in the `status_of`/`feed_entry` style for each case.

## Watch out for

Status is derived, never stored (`#the-registry-records-observations-not-verdicts`).
The fix belongs in the derivation, not in a new column.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/50-cob-status-cannot-show-a-raw-ingest-failure.md.
Reproduce it live first, then decide the precedence in feed_status.status_of.
```
