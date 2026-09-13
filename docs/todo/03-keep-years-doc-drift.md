# `keep_years: 8` in the docs; the config says 10

**Value** medium · **Effort** 15 minutes · **Branch** `docs/keep-years-drift`

## What is wrong (verified 2026-09-13)

`reporting_platform/config/retention.yml` sets `landing.keep_years: 10`
(`operational: 7`, and `1` in the `dev` profile). Four places in the docs say
8, and one says 10:

```bash
grep -rn 'keep_years: 8\|keep_years` (8' --include=*.md .
#  docs/DELIVERY-SHAPES.md:90   | `landing/<feed>/` | evidence ... | `keep_years: 8` |
#  docs/PIPELINE.md:23          LAND[... keep_years: 8 ...]        (the mermaid diagram)
#  docs/PIPELINE.md:103         kept for `keep_years` (8 in the default profile, 1 in `dev`)
#  docs/DECISIONS.md:1584       | `landing/<feed>/` | evidence, byte for byte | `keep_years: 8` |
grep -n 'keep_years' docs/PIPELINE.md
#  docs/PIPELINE.md:230         | `landing/` | `keep_years` — 10 by default ...
```

So `PIPELINE.md` contradicts itself 130 lines apart, and three files carry a
number the platform stopped using when the published-tag interlock raised it
(`DECISIONS.md#published-tags-are-the-reproducibility-window`).

## Why it matters

It is small, but it is the concrete cost of the same fact being written in four
places — and the number governs how long the evidence copy is kept, which is
the one prefix nothing can rebuild.

## What done looks like

- [ ] One place states the number, and `retention.yml` is the authority.
      Prefer `docs/RETENTION.md` for the value; the others say "see
      retention.yml" or drop the figure entirely, since none of them is the
      right place to learn a policy value.
- [ ] `PIPELINE.md` no longer disagrees with itself.
- [ ] Consider a test in the shape of `tests/test_versions.py`, which already
      pins one value across five files — the same trick would work here, and
      would stop the next drift. Only worth it if the number stays duplicated.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/03-keep-years-doc-drift.md.

landing.keep_years is 10 in reporting_platform/config/retention.yml, and four
doc locations say 8 while a fifth says 10. Fix the drift by stating the number
once and pointing at it from the rest -- do not simply change 8 to 10 in four
places, which recreates the problem.

This is one of three small verified-wrong things; docs/todo/04 and 05 are the
others, and they make sense as a single PR.
```
