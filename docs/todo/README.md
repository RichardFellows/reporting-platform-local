# Work items

One file per item. Each states what is wrong, **with the command that shows
it**, what done looks like, and a prompt to paste into a new session.

Everything here was found by working on the platform rather than by reading it,
and each was re-verified on 2026-09-13 against `main` at `0354304`. If an item
looks stale, run its verification command first — the platform moves, and an
item that no longer reproduces should be deleted rather than worked.

| | Item | Value | Effort |
|---|---|---|---|
| [07](07-supersession-delta-append.md) | `supersession: delta_append` | high, if a delta feed is real | multi-day |
| [08](08-sniffer-has-no-notion-of-member-control-files.md) | The sniffer cannot propose a zip whose members have control files | low–medium | 2–3 hours |

## Where to start

**08 is the only remaining code item that is not conditional.** 07 is a design
question before it is a build, and the answer changed — see under "Done".

## Done

**06, `RETENTION.md` restated `DECISIONS.md` in paraphrase** — measured at 9
paragraph pairs and 2,683 characters by the detector in the item file, **now
0 and 0**. The operational half stays: the windows, which command sweeps what,
what a dry run prints, and the refusals an operator actually hits. The
argument moved to eight `DECISIONS.md` anchors, every one verified to exist.

Conclusions an operator cannot act on without the why kept a one-line why and
then the link — what `check_reproducibility_window()` refuses on and what to
change, why an unresolvable `ref()` raises rather than shortening the list,
why a `per_report` entry naming no live exposure binds every feed. The
interlock section became three properties an operator meets in the output
rather than three paragraphs re-arguing the design.

Two things were fixed rather than moved, both verified against the code and
not the prose: the interlock runs *before* the dry-run branch, so a dry run
hits it too (`retention.py:1072`); and the **Non-prod** section claimed
retention was "identical across environments" while proposing
`keep_business_days: 5`/`keep_month_ends: 3` as a future aspiration — `dev`
has had exactly those values, plus `landing.keep_years: 1` and a matching
`published_tags.dev: 1`, since the `environments:` block was written. The
caution that section exists to give is kept.

**03 + 04 + 05, the small PR** — one number stated once, one comment that
lied, and a suite that failed where its README says to run it.

*03:* `landing.keep_years` now has exactly one home, `RETENTION.md`'s landing
section, naming `retention.yml` as the authority (10 in `local`/`uat`/`prod`,
7 for `operational`, 1 in `dev`). The five other sites carry a pointer and no
figure; `PIPELINE.md` no longer disagrees with itself, and its quarantine row
— a sixth bare figure the item did not list — points at quarantine's own key
rather than landing's. No test was added: the number is no longer duplicated,
so there is nothing for one to pin. Two keys in `RETENTION.md`'s config sample
(`superseded_grace_days`, `latest_version_only`) turned out to be read by
nothing at all and went with it.

*04:* both sites corrected — `_defaults.yml` and `common/context.py`, where
the comment contradicted the `CONVENTION_FORBIDDEN` entry two lines below it.
Each now describes the `parent:` chain, shallow at every link, and the five
things that are errors at LOAD. Checked against `_chain` and the seven tests
in `test_conventions.py` that cover the chain and those five errors, not
against the prose.

*05:* the item named one file and offered a mount. Measured, it was **nine
paths across three modules** — `test_ci_pins` and `test_doc_claims` fail there
too, and `test_doc_claims` shells out to `git ls-files` with no `.git` to
read. Mounting the set would put this repo's docs, CI config and git history
inside the runtime image of six services to satisfy a test, so they skip
instead and name the path: the container holds the PACKAGE and these tests
read the REPO. `support.repo_file()` raises `Skipped` **only where there is no
checkout** — in one, a missing file is an `AssertionError` — so neither CI
tier can skip, and a skip never touches the exit code. Verified by moving
`.env.example` aside and watching it fail rather than skip. Container:
`512 passed, 0 failed, 14 skipped`.

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
