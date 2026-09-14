# Sniffing an undated plain file pre-fills a form the loader refuses

**Value** low–medium · **Effort** 1 hour · **Branch** `fix/undated-sniff-both-control-blocks`

## What is wrong (verified 2026-09-14)

A plain file with no 8-digit date in its name is proposed as the arrival
shape (`propose_feed` sets `arrival_source_pattern`). The console then
pre-fills `{stem}\.ctl` into the ARRIVAL control pattern only — in both the
upload handler and `newFeed(draft)`:

```bash
grep -n 'else if (r.arrival_source_pattern)' -A7 reporting_platform/ui/static/index.html
#  ... if (!arrCtlPattern.value.trim()) arrCtlPattern.value = AUTO_CTL;
grep -n 'else if (draft.arrival_source_pattern)' -A5 reporting_platform/ui/static/index.html
#  ... control: {pattern: "{stem}\\.ctl"}}      <- arrival only, no delivery block
```

What that form posts is refused at load:

```bash
docker compose exec -T airflow python - <<'PY'
from reporting_platform.ui import registry
from reporting_platform.common.context import feeds
p = {"name": "prb_undated", "description": "d", "source_system": "PRB",
     "filename_pattern": r"prb_undated_(?P<cob_date>\d{8})\.csv", "business_key": ["position_id"],
     "columns": ["position_id", "qty"],
     "arrival": {"source_pattern": r"positions\.csv",
                 "control": {"pattern": r"{stem}\.ctl", "cob_date": r"(?P<cob_date>\d{8})"}}}
try:
    registry.validate(registry.FeedSpec.from_payload(p), existing=set(feeds())); print("ACCEPTED")
except registry.FeedValidationError as e:
    print("REFUSED:", e.errors)
PY
#  REFUSED: {'delivery': "... sets `arrival.control` but no `delivery.control` ..."}
```

The note shown after the sniff asks only for the landing pattern and the COB
date, so the refusal arrives with nothing on the page having pointed at it.

## Why it matters

It is the form's own default path producing a feed `check_gates_are_coherent`
refuses — the same class item 02 fixed for dated members, and item 08 fixed
for member control files (the `member_control` branch fills BOTH blocks).
This branch was left as it was because it predates both.

## What done looks like

- [ ] The undated-file branch fills the same pattern into both control
      blocks, in the upload handler and in `newFeed(draft)`, and only where
      empty (as the `member_control` branch does).
- [ ] The note says both blocks read the same promoted control file.
- [ ] A test in `tests/test_delivery_form.py` in the style of
      `test_the_payload_a_member_control_proposal_prefills_validates`: the
      pre-filled payload is refused only for the COB date, not for
      `delivery.control`.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/13-undated-file-sniff-prefills-an-unsaveable-form.md,
then the member_control branch of the sniff handler in ui/static/index.html,
which already fills both control blocks. Make the undated plain-file branch
do the same, and verify by sniffing an undated csv through the console at
http://localhost:8092 and saving it.
```
