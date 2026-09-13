# `_defaults.yml` says conventions do not chain; `parent:` chains them

**Value** medium · **Effort** 10 minutes · **Branch** `docs/keep-years-drift` (same PR)

## What is wrong (verified 2026-09-13)

```bash
grep -n 'DO NOT CHAIN' reporting_platform/config/feeds/_defaults.yml
#  43: # `convention` itself, and conventions DO NOT CHAIN. Naming one that does not
```

A convention may name a `parent:`, so the tier is a chain — global, vendor,
system, feed — with an undefined parent and a cycle both errors at load. That
is in `CLAUDE.md`, in `DECISIONS.md#feed-conventions`, and in
`context.resolve_conventions`. The comment in the file every feed team opens
says the opposite.

## Why it matters

`CLAUDE.md`'s first rule is that a claim is worth what its last execution
proved, and this repo has a documented habit of settings and comments that
describe a mechanism other than the working one. This is that, in the most-read
config file.

## What done looks like

- [ ] The comment describes `parent:` chaining, and the two load errors it
      brings (undefined parent, cycle).
- [ ] Grep for the same claim elsewhere: `grep -rn "not chain\|DO NOT CHAIN"`.
- [ ] `python -m tests.run` still passes (`tests/test_conventions.py` covers
      the behaviour; this is a comment fix, so nothing should move).

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/04-conventions-do-not-chain-comment.md.

reporting_platform/config/feeds/_defaults.yml line 43 states that conventions
do not chain. They do, via `parent:`. Correct the comment to describe the
chain and its two load-time errors, and grep for the same claim elsewhere.
```
