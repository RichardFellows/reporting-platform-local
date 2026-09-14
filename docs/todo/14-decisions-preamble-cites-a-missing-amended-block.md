# `DECISIONS.md`'s preamble cites an `Amended.` block that never existed

**Value** low · **Effort** 15 minutes · **Branch** `docs/preamble-amended-example`

## What is wrong (verified 2026-09-14)

```bash
grep -n 'see the `> \*\*Amended.\*\*` block under' -A1 docs/DECISIONS.md
#  see the `> **Amended.**` block under
#  [#console-delivery-support](#console-delivery-support).
awk '/^## console-delivery-support/{f=1;next} /^## /{f=0} f' docs/DECISIONS.md | grep -c 'Amended'
#  0
git log -S'block under' --oneline -- docs/DECISIONS.md
```

The preamble ("Keeping an entry honest as the code moves") gives, as its
example of a NOT BUILT claim that went false, an `> **Amended.**` block under
`#console-delivery-support`. That section has no such block, and per the
history search never did. The first real `> **Amended.**` block in the file
is the one item 10 added under `#published-tags-are-the-reproducibility-window`.

## Why it matters

The preamble is how an entry is kept honest. Its one worked example of the
convention points at nothing, so the first person to follow it finds no model
to copy — which is what happened to the session that closed item 10.

## What done looks like

- [ ] Either the NOT BUILT story it refers to is found (search the history of
      `#console-delivery-support` for the claim that went false) and marked
      in place with an `> **Amended.**` block, or the preamble's sentence
      points at a real example and says what that example shows.
- [ ] `python3 -m tests.run` still passes (`test_doc_claims` reads NOT BUILT
      paragraphs).

## Prompt for a new session

```text
Read docs/todo/14-decisions-preamble-cites-a-missing-amended-block.md and
DECISIONS.md's preamble. Use git log -S / -G on docs/DECISIONS.md to find
what the preamble's example was meant to be, then either write that
Amended block or repoint the sentence at a real one.
```
