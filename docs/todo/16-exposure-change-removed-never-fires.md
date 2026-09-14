# `exposure_change` has a `'REMOVED'` category that can never be assigned

**Value** medium · **Effort** half a day · **Branch** `fix/exposure-change-removed`

## What is wrong (verified 2026-09-14)

```bash
grep -n "'REMOVED'\|from current_exposure cur\|join" dbt/models/reporting/exposure_change.sql
#  when cur.total_mtm is null then 'REMOVED'
#  from current_exposure cur
#  join date_sequence ds ...
#  left join {{ ref('counterparty_exposure') }} prev ...
```

The model is driven FROM `cur` (the current date's `counterparty_exposure`
rows) and left-joins the previous date. A counterparty present on the previous
date and absent today has no `cur` row, so it produces no output row at all —
and `cur.total_mtm is null` is true only for a counterparty that IS present
today with a null total, which is not "removed".

## Why it matters

A counterparty whose exposure disappeared between two COB dates — often the
most interesting row in a change report — is silently omitted, while the
presented categories imply it would be flagged. Nothing fails: the model and
its tests are green.

Item 09 makes "absent from the newest delivery" a real, deliberate outcome for
the snapshot feeds, so a key vanishing between dates is now something the
platform means rather than an artefact.

## What done looks like

- [ ] Decide what `REMOVED` should mean against `counterparty_exposure`'s own
      semantics (it carries reference data forward and flags it — check what
      a counterparty with no live trades looks like there before assuming).
- [ ] Previous-date counterparties missing today produce a `REMOVED` row
      (a full outer join, or a union of the missing keys), with a dbt test that
      builds a date pair where one disappears.
- [ ] Verify on a throwaway Nessie branch; the model is `insert_overwrite`
      per `cob_date` once 09 lands, so a `REMOVED` row must belong to the
      CURRENT date's partition.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/16-exposure-change-removed-never-fires.md and
dbt/models/reporting/exposure_change.sql. The 'REMOVED' branch cannot fire
because the query is driven from the current date. Decide what REMOVED means
against counterparty_exposure, make it fire, and prove it on a Nessie branch.
```
