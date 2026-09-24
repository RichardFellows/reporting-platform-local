# A failed or refused raw ingest never reads FAILED on COB Status, and its receipt is reset within 20 minutes

**Value** medium · **Effort** ½–1 day · **Branch** `fix/cob-status-raw-failure`

## What is wrong (checked 2026-09-24 against `81e9d53`; reasoned from the code, NOT reproduced live — reproduce it live first)

When `ingest_raw` fails, or is refused (a declared md5 or row count
mismatch, the row floor, schema drift under `fail`), the operator never sees
`FAILED` for that feed. Two mechanisms each cause this on their own.

**1. The failed receipt is overwritten.** `transport_steps.ingest_raw`
records `failed` on the receipt. But `transport_reconcile` runs every 20
minutes (`schedule=timedelta(minutes=20)`), and its `sync_receipts` task
calls `registry/transports.py::sync_from_progress`. That function calls
`record_stage(..., "normalized")` for every Transport in
`candidates_by_feed`. `transport_reconcile.discover_transport_progress`
puts a Transport there when it has a NormalizationManifest, whether or not
it reached Raw. `record_stage`'s `WHERE` accepts any stage on a row whose
status is `failed` (`OR status = 'failed'`) and sets `failure_reason` to
NULL. So within about one interval the receipt reads `normalized` again,
with no reason, whether or not anything retried it. After that the only
durable record of the failure is the `registry.validation_result` row
(`control_id=raw_ingestion`). The retrigger does not help either:
`trigger_pending` re-triggers under the same `run_id_for(transport_id)`,
which deduplicates against the failed run.

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

Fixing (2) alone would show `FAILED` for at most about 20 minutes, until
(1) erases it. The feed then reads `PROCESSING` with nothing behind it.
`OPERATIONAL-CONTROL-PLANE.md` §6 calls a FAILED row that never surfaces
"exactly the case an operator most needs to see".

A related gap: `build_report` drops receipts whose `feed` is NULL, and a
receipt only gets a feed at `create_delivery`. So a `validate_transport`
failure never reads `FAILED`; the feed reads `WAITING` or `MISSING`. When
nothing discovered the Transport first, as in a manual replay, the failure
writes no receipt row at all, because `record_stage` is `UPDATE`-only.

## What done looks like

- [ ] Reproduced live first: a Transport whose declared md5 does not match,
      carried through `transport_ingest`. Record the receipt and the COB
      Status page right after the failure, and again after the next
      `transport_reconcile` run.
- [ ] A decision on what may overwrite a `failed` receipt. For example,
      `sync_from_progress` could skip rows that are `failed`, or only
      advance a failed row that a real stage write has reached. Record it in
      `registry/transports.py`'s header and in §6, and update the §6 state
      diagram.
- [ ] A decision on `status_of`'s precedence between `has_delivery` and a
      failed receipt for the same `delivery_id`. Update `status_of`'s
      docstring, §5 and the §5 flowchart.
- [ ] Either give a `validate_transport` failure a place on the page, or
      say in §5 that it is deliberately not shown.
- [ ] Tests for each case, in the `status_of`/`feed_entry` style, plus one
      for `record_stage`/`sync_from_progress` on a `failed` row.

## Watch out for

Status is derived, never stored (`#the-registry-records-observations-not-verdicts`).
The receipt is an execution record and may be mutable. COB status may not.
"Successful retry must not stay stuck reading FAILED forever" (§6) is a real
requirement too. Whatever stops reconcile erasing a failure must still let a
genuinely successful retry move the row on.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/50-cob-status-cannot-show-a-raw-ingest-failure.md.
Reproduce it live first (receipt + COB Status before and after one
transport_reconcile run), then decide both the overwrite rule and the
status_of precedence.
```
