# An unpaired marker file can be the member sniffed and the member pattern proposed

**Value** low–medium · **Effort** 1 hour · **Branch** `fix/sniff-skips-unpaired-control-names`

## What is wrong (verified 2026-09-14)

```bash
python3 - <<'PY'
import io, zipfile
from reporting_platform.ingest import sniff
b = io.BytesIO()
with zipfile.ZipFile(b, "w") as z:
    z.writestr("BATCH.done", "")
    z.writestr("positions.csv", "id,q\n1,2\n")
r = sniff.propose_feed("c_20260901.zip", b.getvalue())
print(r["sniffed_member"], r["member_pattern_candidate"])
PY
#  BATCH.done .*\.done
```

Item 08 taught the sniffer to take PAIRED control members (`X.ctl` beside `X`)
out of the member it sniffs. A container-level marker with no data twin —
`BATCH.done`, `_SUCCESS.ok` — is deliberately not claimed as a member's
control file, and with no pairs the proposal is exactly what it was before 08.
So when such a marker sorts first, it is the file sniffed: the columns, types
and delimiter proposed are read from an empty or one-line marker.

## Why it matters

Both halves of the proposal come from the marker: the columns, and the
member pattern — `.*\.done`, because `.done` and `.csv` tie at one member
each and the unpaired rule breaks ties by name order. A human who accepts it
gets a feed that unpacks the marker and ignores the data. Nothing on the page
says the sniffed member is not a data file.

## What done looks like

- [ ] With no pairs, a member whose name has a control suffix is not chosen
      as the sniffed member when another member exists — and does not win a
      tie for `member_pattern_candidate` either. This IS a change to the
      unpaired proposal, which item 08 kept byte-identical on purpose — say
      so in `DECISIONS.md#the-sniffer` rather than doing it quietly.
- [ ] A test in `tests/test_sniff.py`.

## Prompt for a new session

```text
Read CLAUDE.md, docs/todo/19-sniffer-can-propose-a-marker-file.md and
DECISIONS.md#the-sniffer. Run the snippet. Stop a lone BATCH.done from being
the member sniffed or the member pattern proposed, and record the change to
the unpaired proposal in DECISIONS.md#the-sniffer.
```
