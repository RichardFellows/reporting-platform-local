# The sniffer cannot propose a zip whose members have control files

**Value** low–medium · **Effort** 2–3 hours · **Branch** `feat/sniff-member-control`

## What is wrong (verified 2026-09-13)

`sniff_archive` proposes `member_pattern` by grouping members on their most
common extension. Given a container of control-gated members it proposes:

```python
_member_pattern_candidate(["POSITIONS_A.csv", "POSITIONS_A.ctl",
                           "POSITIONS_B.csv", "POSITIONS_B.ctl"])
# -> '.*\.csv'
```

which is right — but by luck, not by understanding. It has no notion that the
`.ctl` members are control files, so:

* it cannot propose the `arrival.control` half that shape needs, and a human
  gets a proposal that loads and then refuses (`arrival.control` without
  `delivery.control`, or no COB date source at all);
* a sender that ships control content with a claimed extension (`.ctl.csv`, or
  plain `.csv`) gets a `member_pattern` that swallows its control files. The
  gate itself is safe — `unpack` excludes anything matching the control
  pattern — but only once a control pattern has been declared, which is what
  the sniffer is supposed to help write.

## What done looks like

- [ ] `sniff_archive` notices paired stems: members whose names differ only by
      extension, where one extension is in a small set of likely control
      suffixes (`.ctl`, `.trl`, `.done`, `.ok`) or whose content parses as a
      short key/value or single-row delimited file.
- [ ] When it finds them, `propose_feed` proposes `arrival.control.pattern`
      (`'{stem}\.ctl'`) alongside `member_pattern`, and says plainly in the
      note that `delivery.control` is required with it.
- [ ] When it does not, nothing changes.
- [ ] `tests/test_sniff.py` covers both.

## Watch out for

Propose, never decide — the module's existing discipline. Business-key
candidates are surfaced as a note and never auto-selected, and a control-file
guess deserves the same treatment: it changes how every delivery for that feed
is gated.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/08-sniffer-has-no-notion-of-member-control-files.md,
then DECISIONS.md#the-sniffer.

Teach sniff_archive to recognise per-member control files inside a container
and to propose the arrival.control half of that feed shape. Keep the module's
rule that it proposes and a human decides. tests/test_sniff.py imports duckdb
at module level, which is why CI installs it -- keep that true.
```
