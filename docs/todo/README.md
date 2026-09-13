# Work items

One file per item. Each states what is wrong, **with the command that shows
it**, what done looks like, and a prompt to paste into a new session.

Everything here was found by working on the platform rather than by reading it,
and each was re-verified on 2026-09-13 against `main` at `0354304`. If an item
looks stale, run its verification command first — the platform moves, and an
item that no longer reproduces should be deleted rather than worked.

| | Item | Value | Effort |
|---|---|---|---|
| [01](01-ci-dbt-parse-and-dag-import.md) | CI: a second tier that runs `dbt parse` and imports the DAGs | high | half a day |
| [02](02-console-cannot-create-an-unpacked-archive-feed.md) | The console cannot create a feed whose zip is unpacked at the gate | high | 2–3 hours |
| [03](03-keep-years-doc-drift.md) | `keep_years: 8` in the docs; the config says 10 | medium | 15 min |
| [04](04-conventions-do-not-chain-comment.md) | `_defaults.yml` says conventions do not chain; `parent:` chains them | medium | 10 min |
| [05](05-test-versions-fails-in-the-container.md) | `python -m tests.run` fails inside the container | medium | 20 min |
| [06](06-retention-md-restates-decisions.md) | `RETENTION.md` paraphrases `DECISIONS.md` | medium | 2–3 hours |
| [07](07-supersession-delta-append.md) | `supersession: delta_append` | high, if a delta feed is real | multi-day |
| [08](08-sniffer-has-no-notion-of-member-control-files.md) | The sniffer cannot propose a zip whose members have control files | low–medium | 2–3 hours |

## Where to start

**01** has the most lasting value: it closes the tier `CLAUDE.md` itself lists
as ungated, and the failures it catches — a DAG that does not import, dbt
broken by a dependency resolution — are invisible until the scheduler parses.

**03 + 04 + 05 are one small PR.** Three separately verified wrong things, an
hour in total, and each is the kind this repo treats as worse than a bug: a
comment that lies, a number that disagrees with the config it describes, and a
test suite that fails where its own README says to run it.

**02** is a half-shipped feature rather than a defect: the loader, the
validator and the YAML writer all accept a feed shape the form cannot express.

## Adding an item

Keep the shape: what is wrong **and how to see it**, why it matters, what done
looks like, what to watch out for, and a prompt. An item nobody can verify in
one command is an opinion, and this directory is not for those.
