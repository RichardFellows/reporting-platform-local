# The SCD2 replay reads raw that retention has pruned

**Value** high · **Effort** 1–2 days · **Branch** `fix/scd2-replay-from-target`

## What is wrong (verified 2026-09-14)

```bash
python3 -m tests.run test_scd2_incremental 2>&1 | grep no_longer_holds
#  ok    test_scd2_incremental.test_a_version_whose_cob_date_raw_no_longer_holds_is_never_retracted
sed -n '/def test_a_version_whose_cob_date_raw_no_longer_holds_is_never_retracted/,/^def /p' tests/test_scd2_incremental.py | head -30
grep -n 'limit that remains' -A10 docs/DECISIONS.md
```

`ref_counterparty` and `ref_rating` replay each touched key from its
`replay_from` date by re-reading RAW. Retention prunes raw to 10 business days
plus month-ends (`retention.yml`), while the SCD2 versions live far longer.
Once the date a key's replay starts from is pruned, the replay re-derives the
key from its first RETAINED delivery as a new version beside the one it could
not re-derive: two open versions, and `scd2_exactly_one_current_version` /
`mutually_exclusive_ranges` fail the build — every run, until someone
intervenes. The test above pins that this stays LOUD (item 09 added a guard
so it is never turned into a silent deletion); it does not fix it.

The same root cause makes `--full-refresh` of either model destructive once
retention has run: a rebuild from pruned raw re-dates every mid-month version
older than the keep-set. Not reproduced live — no retention has run on this
estate — but it follows from the same read.

## Why it matters

Nothing has pruned raw here yet — `platform_housekeeping` is paused and has
never run — which is the only reason the stack is green. And the failure does
not wait for a counterparty to CHANGE: `scd2_replay` treats a key as touched
if the stage carries it anywhere in the lookback window, and a
`full_snapshot` feed delivers every key every day, so every key is touched on
every build. Any key whose version began on a now-pruned date gets two open
versions on the very next build — the pinned test re-delivers B with an
UNCHANGED value and still gets two. Reference versions are mostly old, so the
first build after the first housekeeping that prunes raw breaks both SCD2
models almost wholesale, and the obvious fix a person reaches for —
`--full-refresh` — rewrites history.

## What done looks like

- [ ] The replay's starting state comes from the TARGET (the version in force
      at `replay_from`, already seeded for the version before it by item 09),
      not from raw that may be gone, so an incremental run never depends on a
      pruned date.
- [ ] **Without losing 09's retraction of that version.** `DECISIONS.md`'s
      "limit that remains" paragraph says seeding the start version from the
      target "would change which re-deliveries can retract it, and is not
      done here". Read it first: a re-delivery of the start version's own
      date must still be able to retract it
      (`test_a_redelivery_of_the_date_the_replay_starts_from_is_retracted_too`
      and the C-drop tests must stay green), and the answer must say how.
- [ ] `test_a_version_whose_cob_date_raw_no_longer_holds_is_never_retracted`
      is changed to assert the build is CORRECT, not loud.
- [ ] A decision on `--full-refresh` for SCD2 models after retention: refuse
      it (a guard like `known_as_of`'s), or document the restore path.
- [ ] Verified on a Nessie branch with raw rows deleted to simulate retention.

## Prompt for a new session

```text
Read CLAUDE.md, docs/todo/22-scd2-replay-reads-pruned-raw.md and the SCD2
section of DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date. The
SCD2 replay re-reads raw from replay_from, and retention prunes raw. Seed the
replay from the target instead, flip the pinned test from loud to correct, and
decide what --full-refresh of an SCD2 model may do once raw is pruned.
```
