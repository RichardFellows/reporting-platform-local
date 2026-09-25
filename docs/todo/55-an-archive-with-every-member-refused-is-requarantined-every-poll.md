# An `arrival.archive` container whose every member is refused is re-quarantined on every poll

**Value** medium · **Effort** 1–2 hours · **Branch** `fix/inbox-all-members-refused`

## What is wrong (verified 2026-09-24 against `81e9d53`)

`inbox._promote` decides where the inbox copy goes from all of its outcomes
together (`ingest/inbox.py`, the `WHERE THE INBOX COPY GOES` block after the
outcome loop). It sends the file to `.rejected/` only if the file *itself* was
refused (`outcome.source_name == path.name`), and to `.processed/` only if
`written` is non-zero. A refused archive **member** has source name
`<zip>!<member>`, so it sets neither. The container therefore takes the
`else` branch meant for "Waiting, or every write failed": it stays in
`inbox/`. Its `seen` entry is not cleared, so the next poll plans it again and
quarantines the same member again, with a new `quarantine/` object and a new
`registry.rejection` row every time.

Reproduced with `tests/test_inbox.py`'s own harness (no S3, no Airflow):

```bash
cat > /tmp/repro55.py <<'EOF'
from tests.test_inbox import ZIP_FEED, DATA, _wired, _zipped
inbox, d, fd, puts, triggers = _wired(ZIP_FEED)
from reporting_platform.registry import rejections   # after _wired
q = []
rejections.quarantine_quietly = lambda *a, **k: q.append(a[1])
(d / "weekly.zip").write_bytes(_zipped({"POSITIONS_B.csv": DATA}))  # no .ctl
seen = {}
for i in range(6):
    print(i, [(r["file"], r["status"]) for r in inbox.sweep(seen)])
print("still in inbox:", (d / "weekly.zip").is_file(), "quarantined:", len(q))
EOF
PYTHONPATH=$PWD REPORTING_CONFIG_DIR=$PWD/reporting_platform/config python3 /tmp/repro55.py 2>/dev/null
#  0 []
#  1 []
#  2 [('weekly.zip!POSITIONS_B.csv', 'rejected')]
#  3 [('weekly.zip!POSITIONS_B.csv', 'rejected')]
#  4 [('weekly.zip!POSITIONS_B.csv', 'rejected')]
#  5 [('weekly.zip!POSITIONS_B.csv', 'rejected')]
#  still in inbox: True quarantined: 4
```

`test_one_bad_member_does_not_stop_the_others` covers one bad member beside
a good one, and there the container moves because the good member counts as
`written`. Nothing covers a container where *no* member lands.

## Why it matters

A refusal cannot clear by waiting. The planner says so itself when it turns a
member's `NotReady` into `Refused` (`_plan_archive`: "a container is complete
the moment it arrives"). Yet here the watcher waits on it forever. In the
meantime `quarantine/` and `registry.rejection` gain one copy per poll interval
for as long as the file sits there, and the Arrivals page and `registry
rejections` show one refusal as hundreds. Retention sizes `quarantine/` in
years.

## What done looks like

- [ ] A container whose every member is `Refused` leaves `inbox/` (a
      `Duplicate` member already counts as `written` and moves it). It
      goes to `.rejected/`, because nothing out of it landed. Decide
      whether the container's own bytes are also quarantined, or only the
      members' as now, and write down why.
- [ ] Each member is quarantined once, not once per poll.
- [ ] A test in `tests/test_inbox.py` asserting both, beside
      `test_one_bad_member_does_not_stop_the_others`.
- [ ] The inbox outcome diagram in `docs/DELIVERY-SHAPES.md` ("What the gate
      does with one file") loses its `(todo 55)` label.

## Watch out for

"Every write failed" (MinIO down) must still leave the file in place. It is
the one case in that `else` branch that the next poll can fix. Count refusals
apart from upload failures rather than widening `refused_itself`.

## Prompt for a new session

```text
Read CLAUDE.md, then
docs/todo/55-an-archive-with-every-member-refused-is-requarantined-every-poll.md.
Reproduce with its script, then fix inbox._promote's destination decision so
a container whose members were all refused leaves the inbox once. Add the
test, and update the DELIVERY-SHAPES.md diagram.
```
