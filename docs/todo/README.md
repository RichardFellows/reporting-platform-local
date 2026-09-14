# Work items

One file per item. Each states what is wrong, **with the command that shows
it**, what done looks like, and a prompt to paste into a new session.

Everything here was found by working on the platform rather than by reading it,
and each was re-verified on 2026-09-13 against `main` at `0354304`. If an item
looks stale, run its verification command first — the platform moves, and an
item that no longer reproduces should be deleted rather than worked.

| | Item | Value | Effort |
|---|---|---|---|
| [03](03-keep-years-doc-drift.md) | `keep_years: 8` in the docs; the config says 10 | medium | 15 min |
| [04](04-conventions-do-not-chain-comment.md) | `_defaults.yml` says conventions do not chain; `parent:` chains them | medium | 10 min |
| [05](05-test-versions-fails-in-the-container.md) | `python -m tests.run` fails inside the container | medium | 20 min |
| [06](06-retention-md-restates-decisions.md) | `RETENTION.md` paraphrases `DECISIONS.md` | medium | 2–3 hours |
| [07](07-supersession-delta-append.md) | `supersession: delta_append` | high, if a delta feed is real | multi-day |
| [08](08-sniffer-has-no-notion-of-member-control-files.md) | The sniffer cannot propose a zip whose members have control files | low–medium | 2–3 hours |

## Where to start

**03 + 04 + 05 are one small PR.** Three separately verified wrong things, an
hour in total, and each is the kind this repo treats as worse than a bug: a
comment that lies, a number that disagrees with the config it describes, and a
test suite that fails where its own README says to run it.

## Done

**02, the console can create a feed whose zip is unpacked at the gate** —
a member-pattern input in the form's arrival section, `readArrival()` sending
`archive: {member_pattern: ...}`, and the guidance saying what that makes
`filename_pattern` mean (each MEMBER after renaming, never the zip). Ticking
Arrival no longer manufactures an `arrival.control` when the members carry
their own dates, which was the form's own default path producing a feed
`check_gates_are_coherent` refuses. Covered by `tests/test_delivery_form.py`;
verified by creating one through the real form and routing a container to it.

**01, CI: a second tier that runs `dbt parse` and imports the DAGs** —
`.github/workflows/parse.yml`, with `scripts/check_dag_imports.py` and
`tests/test_ci_pins.py`. Deleted rather than ticked, per the rule above: the
item no longer reproduces, and what it was asking for is described in
`CLAUDE.md` where it is maintained.

## Adding an item

Keep the shape: what is wrong **and how to see it**, why it matters, what done
looks like, what to watch out for, and a prompt. An item nobody can verify in
one command is an opinion, and this directory is not for those.
