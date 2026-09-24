# Diagram the inbox gate's four outcomes

**Value** low · **Effort** 1 hour · **Branch** `docs/inbox-gate-outcomes` · *Nice to have*

## What is missing (checked 2026-09-24 against `f1493ca`)

The inbox conformance gate is drawn in `DELIVERY-SHAPES.md` and
`DELIVERY-WALKTHROUGHS.md` with two outcomes, lands or quarantined. The
code has four, and `inbox.py` acts on exactly these:

```bash
grep -nE '^class (Planned|Refused|Duplicate|Waiting)' reporting_platform/ingest/conform.py
#  718:class Planned   738:class Refused   752:class Duplicate   762:class Waiting
```

`Duplicate` (already landed, so do nothing) and `Waiting` (for example, a
control file not yet arrived) are the two that confuse whoever is watching a
file sit in `inbox/`. Neither appears in any diagram. It is also where
docs/todo/12's "not yet stable" state belongs.

## What done looks like

- [ ] One `flowchart` in `docs/DELIVERY-SHAPES.md`: a file at the door →
      `route()` (already conformant / claimed by a feed's
      `arrival.source_pattern` / unroutable) → the shape's planner in
      `conform.ARRIVAL_SHAPES` → one of the four outcomes, each labelled
      with what `inbox.py` then does (land + `.meta.json`, quarantine +
      `registry.rejection` + `.rejected/`, leave in place, move to
      `.processed/`), taken from `inbox.py`, not from the prose.
- [ ] The stability wait (`STABLE_POLLS`) drawn before the planner, since
      it is a fifth thing a file can be doing.
- [ ] Rendered before committing.

## Why the value is low

This is the legacy path. New feeds onboard onto Transport
(`docs/ADDING-A-FEED.md`). Do it if the inbox is still what operators
support day to day.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/31-diagram-the-inbox-gate-outcomes.md. Read
ingest/inbox.py and conform.ARRIVAL_SHAPES, and draw the outcome flowchart
it describes in docs/DELIVERY-SHAPES.md. Render before committing.
```
