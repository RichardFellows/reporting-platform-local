# One-shot `inbox --dry-run` prints `inbox empty` with a file in the inbox

**Value** medium · **Effort** 1–2 hours · **Branch** `fix/inbox-one-shot-dry-run`

## What is wrong (verified 2026-09-14)

```bash
docker compose stop inbox
printf 'a,b\n1,2\n' > inbox/zz_probe_dryrun.csv
docker compose run --rm --no-deps -T inbox python -m reporting_platform.ingest.inbox --dry-run
#  inbox empty
rm -f inbox/zz_probe_dryrun.csv; docker compose start inbox
```

`STABLE_POLLS = 2` (`ingest/inbox.py`) means a file must be seen unchanged
across three observations before it is judged, and the one-shot path in
`main()` sweeps twice. A file it has not been able to judge YET is reported
with the same words as no file at all.

A second, smaller gap on the same path: when a dry run does get far enough to
route a container unpacked at the gate (`arrival.archive`), it reports only
`would conform` for the zip — never the members it would unpack or the
control file each would be dated from. Finding that out currently means
calling `conform.plan_arrival` by hand.

## Why it matters

This is `CLAUDE.md`'s rule about a subject that could not be READ being
reported as EMPTY. `CLAUDE.md`'s quick reference offers this exact command as
the way to see what the inbox would do, and an operator checking why a file
has not moved is told there is nothing there.

## What done looks like

- [ ] A one-shot dry run either observes a file enough times to judge it, or
      says plainly that it saw N file(s) not yet stable — never `inbox empty`
      while the directory holds a candidate.
- [ ] A dry run of an `arrival.archive` container lists each planned member,
      its landing name and its control file (or the refusal).
- [ ] A test for each, in `tests/test_inbox.py`.

## Watch out for

The stability wait exists so a file still being written is not picked up.
Do not shorten it for the watcher to make the one-shot path look right —
fix what the one-shot path SAYS, or make it wait.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/12-inbox-one-shot-dry-run-says-empty.md. Run
its reproduction (stop the watcher first, restart it after). Make the one-shot
dry run tell "not yet stable" apart from "empty", and make a dry run list the
members an arrival.archive container would unpack.
```
