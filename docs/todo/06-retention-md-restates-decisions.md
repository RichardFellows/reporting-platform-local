# `RETENTION.md` paraphrases `DECISIONS.md`

**Value** medium · **Effort** 2–3 hours · **Branch** `docs/retention-dedupe`

## What is wrong (measured 2026-09-13, after the PR #16 trim)

Cross-file near-duplicate detection over the whole doc set puts this pair at
the top: **2,683 characters across 9 paragraph pairs**, several near identical.
It was ~3,800 across 11 before the trim, which cut `DECISIONS.md` but not
`RETENTION.md`.

| Overlap | Subject |
|---|---|
| 80% | `snapshot_tags` stays outside the interlock |
| 79% | a `per_report` entry naming no live exposure binds every feed |
| 64% | age is the commit time, not the COB date |
| 56% | never sweep a manifest whose parts are not ingested |
| 53% | `check_reproducibility_window()` refuses |
| 46% | the interlock is per (report, feed), and that is a relaxation |

They are **paraphrases, not quotes**, which is the problem: you cannot diff
them, so a change lands in one and the other keeps saying the old thing.

Measure it with this — it is the whole detector, and it works on any pair of
files in the set:

```python
# python3 - <<'PY'  (from the repo root)
import re, itertools, pathlib
def blocks(path):
    text = re.sub(r"```.*?```", "\n\n", pathlib.Path(path).read_text(), flags=re.S)
    return [" ".join(b.split()) for b in re.split(r"\n\s*\n", text)
            if len(" ".join(b.split())) >= 120 and not b.lstrip().startswith("#")]
def shingles(b, n=6):
    w = re.findall(r"[a-z_]+", b.lower())
    return {tuple(w[i:i + n]) for i in range(max(0, len(w) - n + 1))}
A, B = "docs/DECISIONS.md", "docs/RETENTION.md"
for ba in blocks(A):
    sa = shingles(ba)
    if len(sa) < 8: continue
    for bb in blocks(B):
        sb = shingles(bb)
        if len(sb) < 8: continue
        ov = len(sa & sb) / min(len(sa), len(sb))
        if ov >= 0.30:
            print(f"{ov:.0%}\n  A: {ba[:150]}\n  B: {bb[:150]}\n")
```

## Why it matters

It is the clearest remaining case of what the trim in PR #16 was about, and
`RETENTION.md` was not touched by it. It also cuts against the split the repo
states: `CLAUDE.md` the rule, `DECISIONS.md` the reasoning, the topic doc what
the thing IS and how to run it.

## What done looks like

- [ ] `RETENTION.md` keeps the operational half — what the windows are, which
      command sweeps what, what a dry run prints, the refusals an operator will
      hit — and states conclusions rather than re-arguing them.
- [ ] Where it currently re-argues, it links the anchor instead. Every anchor
      it needs already exists.
- [ ] Re-run the detector below afterwards; the pair should fall well down the
      list.
- [ ] `tests/test_doc_claims.py` still passes.

## Watch out for

Do not delete the reasoning outright — move the reader to it. The policy set in
`DECISIONS.md`'s preamble is that superseded reasoning is labelled, not
deleted, and that a rule with no why is a rule someone removes.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/06-retention-md-restates-decisions.md, then
DECISIONS.md's preamble section "Keeping an entry honest as the code moves".

docs/RETENTION.md restates ~11 paragraphs of DECISIONS.md in paraphrase.
Reduce it to what an operator needs -- the windows, the commands, the
refusals -- and link the anchors for the reasoning. Measure the overlap before
and after so the change is reportable.
```
