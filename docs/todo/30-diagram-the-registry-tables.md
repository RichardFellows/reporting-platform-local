# Diagram the registry's tables: which are rebuildable, which are events

**Value** medium · **Effort** 2–3 hours · **Branch** `docs/registry-diagram` · *Nice to have*

## What is missing (checked 2026-09-24 against `f1493ca`)

The registry now holds 14 tables. The rule that matters most about them is
which ones can be **rebuilt from object storage** (observations) and which
**cannot** (events). That rule is stated in prose in several places, and the
one diagram (`ARCHITECTURE.md`, "The registry: what is recorded, and what
stays derived") predates half the tables.

```bash
grep -oE 'CREATE TABLE IF NOT EXISTS [a-z_.]+' reporting_platform/registry/db.py | awk '{print $NF}'
#  delivery delivery_part normalization_part rejection run run_input
#  report_version submission as_at_transition submission_item
#  validation_result migration_comparison transport_receipt delivery_committed
```

## What done looks like

- [ ] A Mermaid `erDiagram` (or a grouped `flowchart` if `erDiagram` gets
      too dense) in `docs/REGISTRY.md` with the tables grouped by kind:
  - **observations, rebuildable** by `deliveries.reconcile()`: `delivery`,
    `delivery_part`, `normalization_part`, `rejection`,
    `delivery_committed` (and say how each is rebuilt; `delivery_committed`
    comes from `reconcile-committed`, which only works where
    `_delivery_id` exists);
  - **events, not rebuildable**: `run`, `run_input`, `report_version`,
    `as_at_transition`, `submission`, `submission_item`,
    `transport_receipt`, `validation_result`, `migration_comparison`.
- [ ] The **absent** foreign key drawn as absent: `run_input` does NOT
      reference `delivery` (`#a-run-is-the-first-thing-the-registry-cannot-rebuild`).
      Draw it as a dotted "matches by value" line, not a relationship.
- [ ] Which process writes each table (normalize, ingest, `wap.publish`,
      the lifecycle CLI, `transport_steps`, `migration_reconcile`).
- [ ] Every table and column taken from `registry/db.py`'s `SCHEMA` and
      `MIGRATIONS`, not from prose. Mark the ARCHITECTURE.md diagram as
      superseded or update it, so there are not two diagrams that disagree.

## Watch out for

Do not add verdict columns (`status`, `ingested`) to `delivery` in the
diagram to "make it clearer". Their absence is the design
(`#the-registry-records-observations-not-verdicts`).

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/30-diagram-the-registry-tables.md. Draw the
registry diagram it describes in docs/REGISTRY.md from registry/db.py's
SCHEMA and MIGRATIONS, grouped into rebuildable observations and
non-rebuildable events. Reconcile it with the existing ARCHITECTURE.md
diagram. Render before committing.
```
