# Diagram the Transport receipt's stages and how COB Feed Status is derived

**Value** medium · **Effort** 1–2 hours · **Branch** `docs/status-diagrams` · *Nice to have*

## What is missing (checked 2026-09-24 against `f1493ca`)

Operators read two status vocabularies, and neither is drawn anywhere:

```bash
grep -n 'STAGES\|^FAILED' reporting_platform/registry/transports.py
#  STAGES = ("discovered", "validated", "delivered", "normalized")
#  FAILED = "failed"
grep -n '^STATUSES' -A1 reporting_platform/monitoring/feed_status.py
#  ("NOT_EXPECTED", "WAITING", "RECEIVED", "PROCESSING", "COMPLETE", "FAILED", "MISSING")
grep -c mermaid docs/OPERATIONAL-CONTROL-PLANE.md
#  0
```

## What done looks like

- [ ] **`registry.transport_receipt`: a `stateDiagram-v2`**, because it is
      a stored, mutable status. It shows the four stages in order, with
      `failed` reachable from each. It also shows the `stage_rank` rule: a
      retried or racing task cannot move the status backwards, but
      `failed` can always be recorded, and a successful retry moves it on
      again (`transports.py` header). It shows which `transport_ingest`
      task writes each transition, and that "reached Raw" is **not** a
      receipt stage. That fact is `registry.delivery_committed`.
- [ ] **COB Feed Status: a decision `flowchart`, NOT a state diagram.**
      The status is derived fresh on every request by
      `feed_status.status_of()`, and nothing moves between states. Draw the
      precedence exactly as the code checks it:
      not expected → `NOT_EXPECTED`; committed → `COMPLETE`; normalized
      delivery → `PROCESSING`; failed receipt → `FAILED`; any receipt →
      `RECEIVED`; past `expected_by` → `MISSING`; else `WAITING`. Label the
      two edges that surprise people: a failed-then-superseded date is
      `COMPLETE`, and no `expected_by` means `WAITING` forever, not
      `MISSING`.
- [ ] Both in `docs/OPERATIONAL-CONTROL-PLANE.md`. The receipt diagram is
      also linked from `AIRFLOW-ORCHESTRATION.md`.
- [ ] Mention in the caption that a Delivery ingested before
      `_delivery_id` existed reads `PROCESSING` until its table is rebuilt
      (`CLAUDE.md`, registry section).

## Watch out for

Drawing COB status as a state machine would claim a lifecycle that does not
exist. It is the mistake `#the-registry-records-observations-not-verdicts`
exists to prevent. If a diagram can't be drawn without implying stored
state, the prose was right to leave it undrawn.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/29-diagram-transport-receipt-and-cob-status.md.
Read registry/transports.py and monitoring/feed_status.status_of, then draw
the two diagrams it describes in docs/OPERATIONAL-CONTROL-PLANE.md. Render
them before committing.
```
