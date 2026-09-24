# Diagram write-audit-publish as a Nessie commit graph

**Value** medium · **Effort** 1–2 hours · **Branch** `docs/nessie-ref-graph` · *Nice to have*

## What is missing (checked 2026-09-24 against `f1493ca`)

Write-audit-publish is the idea the whole platform leans on. Every doc draws
it as a sequence diagram (README, `PIPELINE.md` §4–5). Nothing draws it as
what it is in the catalog: a graph of branches, merges and tags. That view
is the one someone needs when they look at
`curl -s http://localhost:19120/api/v2/trees` and try to work out what they
are seeing.

```bash
grep -rn 'gitGraph' README.md docs/    # nothing
```

## What done looks like

- [ ] A Mermaid `gitGraph` in `docs/ARCHITECTURE.md` (write-audit-publish
      section), linked from the README's write-audit-publish section,
      showing:
  - two ingests, each `ingest/<feed>/<cob_date>/<run>-a<n>`, merged to
    `main` and tagged `snapshot/<feed>/<cob_date>/<run_id>` **on the merge
    commit** (not on a later head of `main`);
  - a retried ingest, whose `-a1` branch is kept after failing and whose
    `-a2` succeeds (`#a-refusal-is-not-retried`);
  - a `build/prepared/…` branch that fails its tests and is kept, with
    `main` unmoved;
  - a `build/reporting/…` branch merged and tagged
    `published/<report>/<as_at>/<run_id>` once per report (two exposures);
  - a note that retention sweeps abandoned `build/*` branches (120 h) and
    that `published/` tags are kept for years
    (`#published-tags-are-the-reproducibility-window`).
- [ ] Every ref name checked against the code that makes it:
      `context.branch_name`, `context.ingest_attempt_id`,
      `context.snapshot_tag`/`published_tag`, `transform/wap.branch_name`.
- [ ] Rendered with mermaid-cli and on GitHub before merging.

## Watch out for

- `gitGraph` accepts slash-separated branch names if they are quoted
  (`branch "ingest/fo_trade/2026-08-13/r1-a1"`). This was checked with
  mermaid-cli 11. GitHub's renderer can lag behind, so check it there too.
- `gitGraph` has no way to show a branch being *deleted*. Say in the
  caption that merged branches are deleted and kept ones are not, rather
  than drawing a merged branch as though it lives on.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/28-diagram-the-nessie-ref-graph.md. Draw the
gitGraph it describes in docs/ARCHITECTURE.md, taking every ref name from
the code, and link it from the README. Render it before committing.
```
