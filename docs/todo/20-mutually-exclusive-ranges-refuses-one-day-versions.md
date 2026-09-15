# `mutually_exclusive_ranges` refuses a correct one-day SCD2 version and passes a same-day overlap

**Value** high · **Effort** 1–2 hours · **Branch** `fix/scd2-range-test-exclusive-end`

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

**And it misses the overlap it exists to catch.** With `gaps: allowed` the
test requires only `upper_bound <= next_lower_bound`. Inclusive ends mean
`[09-01, 09-05]` then `[09-05, 09-10]` OVERLAP on 09-05, and `05 <= 05`
passes. `as_of()` joins with `between effective_from and effective_to`
(`engine.sql`), so on that day both versions match and every joined exposure
row doubles. The comment above the test in `_prepared.yml` ("`allowed` still
fails on any OVERLAP") is wrong in the same way. Simulated with the macro's
own arithmetic:

```bash
python3 - <<'SIM'
import duckdb
con = duckdb.connect()
def fails(rows, gaps_op, upper, zero_ok):
    con.execute("create or replace table v as select * from (values " + ",".join(
        f"('B','{f}'::date,'{e}'::date)" for f, e in rows) + ") t(k,f,e)")
    lb = "<=" if zero_ok else "<"
    return con.execute(f"""select count(*) from (select f, {upper} ub,
        lead(f) over (partition by k order by f,e) nl,
        row_number() over (partition by k order by f desc, e desc)=1 is_last from v)
        where not(coalesce(f {lb} ub,false) and coalesce(ub {gaps_op} nl, is_last, false))""").fetchone()[0]
overlap = [('2026-09-01','2026-09-05'), ('2026-09-05','2026-09-10')]
oneday  = [('2026-09-01','2026-09-02'), ('2026-09-03','2026-09-03'), ('2026-09-04','9999-12-30')]
gap     = [('2026-09-01','2026-09-02'), ('2026-09-05','9999-12-30')]
for name, cfg in {"today: allowed, effective_to": ("<=", "e", False),
                  "allowed + zero_length_range_allowed": ("<=", "e", True),
                  "not_allowed, effective_to + 1 day": ("=", "e + interval 1 day", False)}.items():
    print(f"{name:38s} overlap={fails(overlap,*cfg)} one-day={fails(oneday,*cfg)} gap={fails(gap,*cfg)}")
SIM
#  today: allowed, effective_to           overlap=0 one-day=1 gap=0   <- wrong both ways
#  allowed + zero_length_range_allowed    overlap=0 one-day=0 gap=0   <- passes everything
#  not_allowed, effective_to + 1 day      overlap=1 one-day=0 gap=1
```

## Why it matters

Today the test is wrong in both directions: it fails the reporting build over
a correct history, and it passes the one defect — two versions in force on
the same day — that silently doubles exposure. The obvious fix,
`zero_length_range_allowed: true`, makes it pass everything.

## What done looks like

- [ ] Both tests (`ref_counterparty`, `ref_rating`) bound on the EXCLUSIVE
      end: `upper_bound_column` is inserted into the SQL as written (the
      `ref_rating` block already passes an expression as `partition_by`), so
      `upper_bound_column: date_add(effective_to, 1)` with
      `gaps: not_allowed`. That accepts one-day versions and fails overlaps
      and gaps. Check that `DATE '9999-12-31' + 1 day` does not overflow or
      go NULL on Spark for the open version — the macro computes it for the
      last record too — and handle it if it does.
- [ ] Decide `gaps: not_allowed` deliberately: a key absent from a snapshot
      does not close its version (`#a-snapshot-re-delivery-restates-the-whole-date`),
      so versions should be contiguous and a gap is a defect — confirm
      nothing legitimately produces one before making it fail.
- [ ] The `_prepared.yml` comments say what the test catches, and what it
      does not.
- [ ] Prove it on a throwaway Nessie branch: a one-day version passes; a
      same-day overlap fails.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/20-mutually-exclusive-ranges-refuses-one-day-versions.md.
effective_to is inclusive, so dbt_utils' mutually_exclusive_ranges refuses a
correct one-day SCD2 version AND passes a same-day overlap. Bound the test on
effective_to + 1 day with gaps: not_allowed, and prove on a Nessie branch that
a one-day version passes and a same-day overlap fails.
```
