# `fo_trade` and `ref_collateral` dedupe on the raw key, then clean it

**Value** low–medium · **Effort** 1–2 hours · **Branch** `fix/rank-on-cleaned-key`

## What is wrong (verified 2026-09-14)

```bash
grep -n "dedupe_rank\|clean_string('trade_id')\|clean_string('collateral_id')" \
  dbt/models/prepared/fo_trade.sql dbt/models/prepared/ref_collateral.sql
#  fo_trade.sql:36:        {{ dedupe_rank(['trade_id']) }} as _rn
#  fo_trade.sql:51:        {{ clean_string('trade_id') }} as trade_id,
#  ref_collateral.sql:38:  {{ dedupe_rank(['collateral_id']) }} as _rn
#  ref_collateral.sql:53:  {{ clean_string('collateral_id') }} as collateral_id,
```

The in-file dedupe partitions by the RAW key and cleaning happens afterwards,
so one file carrying ` T1` and `T1` keeps both rows as `_rn = 1`, and they
become two rows with the same `(cob_date, trade_id)`. Item 09 fixed exactly
this in the two SCD2 models (`ranked_rows` ranks the cleaned key) and left
these two alone because their path had already been verified live.

## Why it matters

The scaffolded `unique_combination_of_columns` test on `[cob_date, key]`
catches it, so the build refuses to publish rather than publishing a
duplicate — but the refusal names uniqueness, not "the sender padded a key",
and the fix is the legacy "last occurrence in file wins" rule the macro
documents, which is not what happens.

## What done looks like

- [ ] Both models rank on the cleaned key, through the model's one cleaning
      definition, as `ref_counterparty`'s `ranked_rows` does; the scaffold
      template (`ui/scaffold.py`) does the same.
- [ ] A test in `tests/test_dedupe_rank.py` with ` T1` then `T1` in one file,
      both orders, asserting the later row wins.
- [ ] Re-verify the `insert_overwrite` path on a Nessie branch (item 09's
      `fo_trade` procedure), since the select changes shape.

## Prompt for a new session

```text
Read CLAUDE.md and docs/todo/23-date-partitioned-models-rank-the-raw-key.md.
Make fo_trade, ref_collateral and the scaffold rank on the cleaned key the way
ref_counterparty's ranked_rows does, test it, and re-verify fo_trade's
insert_overwrite on a Nessie branch.
```
