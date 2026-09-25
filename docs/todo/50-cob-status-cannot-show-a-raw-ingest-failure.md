# A failed or refused raw ingest never reads FAILED on COB Status, and inside reconcile's window its receipt is reset

**Value** medium · **Effort** ½–1 day · **Branch** `fix/cob-status-raw-failure`

## What is wrong (checked 2026-09-24 against `81e9d53`; reasoned from the code, NOT reproduced live — reproduce it live first)

When `ingest_raw` fails, or is refused (a declared md5 or row count
mismatch, the row floor, schema drift under `fail`), the operator never sees
`FAILED` for that feed. Two mechanisms each cause this on their own.

**1. Inside reconcile's window, the failed receipt is overwritten.**
`transport_reconcile`'s `sync_receipts` sets it back to `normalized` and
clears `failure_reason` within about one 20-minute interval, whether or not
anything retried it. This applies to a COB date inside
`TRANSPORT_RECONCILE_WINDOW_DAYS` (default 7, counted back from today), or
to any date under a full sweep. The mechanism, and why nothing re-triggers
the ingest, is in
[`OPERATIONAL-CONTROL-PLANE.md` §6](../OPERATIONAL-CONTROL-PLANE.md#a-failed-receipt-is-reset-by-reconcile).
The cause is in `registry/transports.py`: `sync_from_progress` writes
`normalized` for every candidate past normalization, and `record_stage`
accepts any stage on a `failed` row (`OR status = 'failed'`). A failure for
an older COB date, such as a backfill or a replay, keeps its `failed`
receipt, but (2) still hides it.

**2. Precedence hides it even before the reset.** `feed_status.status_of`
checks `has_delivery` (any `registry.delivery` row) before the receipt's
`failed`. `registry.delivery` is written at normalize (`ingest/normalization.py`,
`register_v2_quietly`), so the feed already reads `PROCESSING`:

```bash
REPORTING_CONFIG_DIR=$PWD/reporting_platform/config python3 -c "
from datetime import date, datetime, timezone
from reporting_platform.monitoring.feed_status import status_of
print(status_of(expected=True, committed=False, has_delivery=True,
      receipt_status='failed', expected_by='07:00', cob_date=date(2026,9,21),
      now=datetime(2026,9,23,tzinfo=timezone.utc)))"
#  PROCESSING
```

Inside the window, fixing (2) alone would show `FAILED` for at most one
interval, until (1) erases it.

**3. A failure after a commit is hidden too.** `status_of` checks
`committed` first. Once any Delivery for the date has committed, a later
one for the same date that fails reads `COMPLETE`. That later Delivery could
be a correction whose md5 does not match. It has the same shape as (2), one
step earlier:

```bash
# status_of(expected=True, committed=True, has_delivery=True,
#           receipt_status='failed', ...)  ->  COMPLETE
```

**Related:** `build_report` drops receipts whose `feed` is NULL. A receipt
gets a feed only from `create_delivery` or `sync_receipts`, so a
`validate_transport` failure never reads `FAILED`; the feed reads `WAITING`
or `MISSING`. When nothing discovered the Transport first, as in a manual
replay, the failure writes no receipt row at all, because `record_stage` is
`UPDATE`-only.

## What done looks like

- [ ] Reproduced live first: a Transport whose declared md5 does not match,
      carried through `transport_ingest`, for a COB date inside the window.
      Record the receipt and the COB Status page right after the failure,
      and again after the next `transport_reconcile` run. Then do the same
      for a correction that fails after the date committed (3).
- [ ] A decision on what may overwrite a `failed` receipt, for example that
      `sync_from_progress` skips rows that are `failed`. Record it in
      `registry/transports.py`'s header and in §6, and update the §6 state
      diagram.
- [ ] A decision on `status_of`'s precedence: `committed` and
      `has_delivery` against a failed receipt for a *different*, newer
      `delivery_id`. Update `status_of`'s docstring, §5 and the §5
      flowchart.
- [ ] Either give a `validate_transport` failure a place on the page, or
      say in §5 that it is deliberately not shown.
- [ ] Tests for each case, in the `status_of`/`feed_entry` style, plus one
      for `record_stage`/`sync_from_progress` on a `failed` row.

## Watch out for

Status is derived, never stored (`#the-registry-records-observations-not-verdicts`).
The receipt is an execution record and may be mutable. COB status may not.
"Successful retry must not stay stuck reading FAILED forever" (§6) is a real
requirement too. Whatever stops reconcile erasing a failure must still let a
genuinely successful retry move the row on. The precedence in (3) exists
for a reason: a correction must not be reported FAILED because of what it
corrected. So compare `delivery_id`s; do not simply reorder the checks.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/50-cob-status-cannot-show-a-raw-ingest-failure.md.
Reproduce it live first (receipt + COB Status before and after one
transport_reconcile run, for a COB date inside the window), then decide both
the overwrite rule and the status_of precedence.
```
