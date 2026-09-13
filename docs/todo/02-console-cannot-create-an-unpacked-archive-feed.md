# The console cannot create a feed whose zip is unpacked at the gate

**Value** high · **Effort** 2–3 hours · **Branch** `feat/console-arrival-archive`

## What is wrong (verified 2026-09-13)

The feed form has the arrival fields for a legacy sender —
`arrival.source_pattern`, `arrival.control.pattern`, `arrival.control.cob_date`
— but **no input for `arrival.archive.member_pattern`**:

```bash
grep -n 'archive' reporting_platform/ui/static/index.html
# only the delivery.kind dropdown and syncDeliveryVisibility; no arrival.archive
```

Everything behind the form already supports it. `ui/registry._arrival_from_payload`
reads `archive.member_pattern` from a payload, `_arrival_block` writes it back,
and `validate()` accepts it — all added when the zip mechanisms were built. The
UI simply never sends one, so the shapes documented as **4a** and **4c** in
`docs/DELIVERY-WALKTHROUGHS.md` are creatable only by hand-editing a feed file.

## Why it matters

This is the same class of gap as the one fixed in PR #16: the loader and the
form's validator accept a shape the form cannot express, so the console is
quietly a second-class way to onboard. It is also the shape most likely to be
onboarded through the console, because a zip of dated members is exactly what
somebody sniffs first.

## What done looks like

- [ ] A `memberPattern` input under the arrival section, shown when the feed
      has an `arrival:` block, with the same placeholder discipline as its
      neighbours (`POS_(?P<cob_date>\d{8})\.csv`).
- [ ] `readArrival()` emits `archive: {member_pattern: ...}` when it is set.
- [ ] A round-trip test in `tests/test_delivery_form.py`: payload → FeedSpec →
      written YAML → loaded `Feed` keeps `arrival.archive.member_pattern`.
      (`test_editing_an_archive_feed_keeps_its_archive_block` covers the edit
      direction already; this is the create direction from the form's payload.)
- [ ] Check the form's own guidance text: an `arrival.archive` feed needs
      `filename_pattern` to describe the MEMBERS after renaming, never the zip,
      which is the single most confusable thing about the shape.

## Watch out for

* `arrival.archive` + `arrival.control` requires `delivery.control` too
  (`check_gates_are_coherent`), so a form that offers the first two without the
  third produces a feed the loader refuses. Validation already says so — make
  sure the message reaches the right field.
* The member's COB date comes from `member_pattern` OR the member's control
  file, never both, never neither (`resolve_arrival_config`).

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/02-console-cannot-create-an-unpacked-archive-feed.md.

The feed console can edit an arrival.archive feed but cannot create one: the
form has no member_pattern input under the arrival section, though
ui/registry.py and validate() have handled it since the zip work landed.

Add the field and the payload wiring, with a round-trip test. The console runs
on http://localhost:8092 on this machine (.env overrides the documented 8082);
`docker compose up -d feed-ui` and create a feed through the real form to check
it, then delete it.
```
