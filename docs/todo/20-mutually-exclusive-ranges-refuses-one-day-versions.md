# `mutually_exclusive_ranges` refuses a legitimate one-day SCD2 version

**Value** medium · **Effort** 1 hour · **Branch** `fix/scd2-zero-length-ranges`

## What is wrong (verified 2026-09-14)

```bash
grep -n 'mutually_exclusive_ranges' -A5 dbt/models/prepared/_prepared.yml
#  lower_bound_column: effective_from / upper_bound_column: effective_to
#  partition_by: counterparty_id          (and counterparty_id || '|' || agency)
#  gaps: allowed                          <- no zero_length_range_allowed
docker compose exec -T airflow sh -c \
  'grep -rn "zero_length_range_allowed=False" /opt/platform/run/packages/dbt_packages/dbt_utils/macros/generic_tests/mutually_exclusive_ranges.sql'
#  {% test mutually_exclusive_ranges(..., gaps='allowed', zero_length_range_allowed=False) %}
```

dbt_utils' test defaults to `zero_length_range_allowed: false`, which requires
`lower_bound < upper_bound` STRICTLY. This project's `effective_to` is
INCLUSIVE (`scd2_columns`: the next version's `effective_from` less one day),
so a value in force for exactly one COB date is written `09-03 → 09-03` and
the test refuses it.

Seen live while verifying item 09: a counterparty whose value changed on
09-03 and again on 09-04 left `[Q, 2026-09-03, 2026-09-03]`, and
`dbt_utils_mutually_exclusive_ranges_ref_counterparty_...` failed with 1 row —
on the incremental table AND on a full refresh over the same raw, which was
identical. Nothing overlapped.

## Why it matters

A one-day value is ordinary for a daily reference feed (a correction the
sender reverses the next day; 09's retraction can also re-date a version to a
single day). Each one fails the build's tests, so the reporting build refuses
to publish over a history that is right — and the likeliest reaction to a test
that cries wolf is to stop reading it, which is what would hide a REAL overlap.

## What done looks like

- [ ] Both `mutually_exclusive_ranges` tests (`ref_counterparty`,
      `ref_rating`) set `zero_length_range_allowed: true`, with a comment
      saying `effective_to` is inclusive.
- [ ] Check `gaps: allowed` against the same convention: with inclusive ends,
      contiguous versions have `upper_bound < next_lower_bound` by one day, so
      `gaps: not_allowed` (which requires `=`) could never pass either — say
      in the comment why `allowed` is the only usable setting, and that it
      therefore does not catch a real gap.
- [ ] Prove it on a throwaway Nessie branch: a one-day version passes, and an
      overlap (two open versions for one key) still fails.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/20-mutually-exclusive-ranges-refuses-one-day-versions.md.
effective_to is inclusive, so dbt_utils' mutually_exclusive_ranges refuses a
one-day SCD2 version. Allow zero-length ranges on both SCD2 models, and prove
on a Nessie branch that a one-day version passes and an overlap still fails.
```
