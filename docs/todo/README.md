# Work items

One file per item. Each states what is wrong, **with the command that shows
it**, what done looks like, and a prompt to paste into a new session.

Everything here was found by working on the platform rather than by reading it.
Each item file carries the date its "What is wrong" was last verified:
12–19 on 2026-09-14 against `main` at `fdb0784`, and 20–24 against `cea4500`. 21 is reasoned from the code and says so. If an item looks stale, run its verification command first — the
platform moves, and an item that no longer reproduces should be deleted rather
than worked.

| | Item | Value | Effort |
|---|---|---|---|
| [07](07-supersession-delta-append.md) | `supersession: delta_append` | high, if a delta feed is real | multi-day |
| [12](12-inbox-one-shot-dry-run-says-empty.md) | One-shot `inbox --dry-run` prints `inbox empty` with a file in the inbox | medium | 1–2 hours |
| [13](13-undated-file-sniff-prefills-an-unsaveable-form.md) | Sniffing an undated plain file pre-fills a form the loader refuses | low–medium | 1 hour |
| [14](14-decisions-preamble-cites-a-missing-amended-block.md) | `DECISIONS.md`'s preamble cites an `Amended.` block that never existed | low | 15 min |
| [17](17-docs-say-retention-removes-superseded-versions.md) | Two places say retention removes superseded versions; nothing does (README fixed; docstring left) | low–medium | 15 min |
| [19](19-sniffer-can-propose-a-marker-file.md) | An unpaired marker file can be the member sniffed and the member pattern proposed | low–medium | 1 hour |
| [21](21-an-empty-redelivery-cannot-supersede.md) | A re-delivery with no rows cannot supersede anything | medium | ½–1 day |
| [24](24-spark-workers-run-python-3-8.md) | The Spark workers run Python 3.8; every driver runs 3.11 | medium | 1–2 hours |
| [25](25-a-feed-that-never-delivered-blocks-every-prepared-build.md) | A declared feed that has never delivered blocks every prepared build | high | ½–1 day |
| [26](26-tests-run-on-a-host-fails-without-reporting-config-dir.md) | `python -m tests.run` on a host fails 12 tests unless `REPORTING_CONFIG_DIR` is set | medium | 1 hour |
| [27](27-make-lineage-points-at-the-notebook-port.md) | `make lineage` says to serve dbt docs on the notebook's port | low | 15–30 min |
| [28](28-diagram-the-nessie-ref-graph.md) | *Nice to have:* write-audit-publish as a Nessie commit graph | medium | 1–2 hours |
| [29](29-diagram-transport-receipt-and-cob-status.md) | *Nice to have:* diagram the Transport receipt stages and COB Feed Status derivation | medium | 1–2 hours |
| [30](30-diagram-the-registry-tables.md) | *Nice to have:* diagram the registry tables, rebuildable vs events | medium | 2–3 hours |
| [31](31-diagram-the-inbox-gate-outcomes.md) | *Nice to have:* diagram the inbox gate's four outcomes | low | 1 hour |

## Where to start

**25 first.** Until it is fixed, onboarding any feed stops
every other feed publishing until the new one first delivers. It needs a
decision before code, and the item lays out the three options.

**07** is no longer blocked: 09 decided that `full_snapshot` selects the
newest delivery per COB date, so 07's premise holds as written. It is
multi-day design work and only worth starting if a delta feed is real; read
its banner and
[DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date](../DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date)
first.

**25–31** were found reviewing the README on 2026-09-24. 25–27 are bugs,
reproduced before they were written down. 28–31 are diagrams, independent of
each other and of everything else.

**12–24** were found working 08–10. 12–19, 23 and 24 were reproduced before
they were written down; **21** is reasoned from the code and says so — reproduce
it first. **21–24** came out of 09's reviews and live runs. The rest are
independent. (**16**, `exposure_change`'s `REMOVED`, is fixed: plan #21.)

## Done

**22, the SCD2 replay read raw that retention had pruned** — every version
in the replay's scope whose COB date raw no longer holds at all is now
SEEDED FROM THE TARGET (`scd2_pruned_seed` in `scd2_replay`), never
re-derived, so an incremental run never depends on a pruned date. 09's
retraction of the start version is kept because the seed fires only on
ABSENCE: a re-delivery of the start date is a row raw holds, so that date
is replayed from raw exactly as before and can still retract it
(`test_a_redelivery_of_the_date_the_replay_starts_from_is_retracted_too`,
the C-drop tests and the new
`test_a_retained_redelivery_still_retracts_a_version_seeded_at_its_start`
are green). That made `scd2_retractions`' "only where raw still holds the
date" guard redundant, and code review found that keeping it stranded a
seeded version a retained re-delivery had made redundant; it is gone, and
`test_a_pruned_version_subsumed_by_a_retained_redelivery_is_retracted` pins
it. `--full-refresh` of an SCD2 model now REFUSES once the target holds a
version whose origin date raw has lost
(`scd2_refuse_full_refresh_over_pruned_raw`, as-of builds included), naming
`--vars '{scd2_rebuild_from_pruned_raw: true}'` and a restore from a Nessie
tag as the ways past. The pinned test now asserts the build is correct.
Eight `test_scd2_incremental` tests fail against `main`'s macros and pass
here; `python -m tests.run`: 1009 passed.
*Verified live* on throwaway branches, the branch's dbt project copied into
the airflow container at `/tmp/todo22-dbt` (removed afterwards; `main`
stayed at `523b887`). `ref_rating` full-refreshed from 36 raw dates, then
raw before 2026-07-20 deleted: `main`'s code failed
`mutually_exclusive_ranges` with 206 rows and left 88 keys with two open
versions; the branch's passed every test with the table row-for-row
unchanged. A new 2026-08-20 delivery changing one such key opened exactly
one version. `--full-refresh` refused naming 29 pruned dates; with the
override it rebuilt 199 versions from 1,402, all re-dated to 2026-07-20 or
later — the destruction the guard exists to stop, now measured rather than
reasoned. Recorded in
[DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date](../DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date).
*What the item got wrong.* Nothing material; its `--full-refresh` claim,
reasoned from the code, reproduced exactly.

**15, `next_file_version` read an unreadable raw table as version 1** — its
`except Exception: return 1` is gone. Any read failure now raises, naming the
feed, the table as addressed (ref included) and the COB date, and fails the
ingest. `COALESCE(MAX(_file_version), 0)` was already answering the genuinely
empty case, and the one call site runs after `ensure_raw_table` has created
the table on the branch, so a missing table there is a wrong ref or name too.
Reproduced on `main` first. Against the live catalog, `fo_trade` 2026-08-19
correctly got 2, but a nonexistent ref and a nonexistent table both got **1**,
tying with the delivery already there. `tests/test_next_file_version.py`
failed on `main` ("an unreadable raw table returned a version instead of
raising").
*Concurrent ingests*: measured, not assumed. Two branches cut from one base
both computed `_file_version=2`. The first merged, and Nessie refused the
second with `409 REFERENCE_CONFLICT` naming `raw.fo_trade`: its default
NORMAL per-key merge mode stops the tie from ever reaching `main`. The pool
plays no part (`bulk_ingest` and the CLI never enter it). Written down as
[`#a-merge-conflict-not-the-pool-keeps-file-version-unique`](../DECISIONS.md#a-merge-conflict-not-the-pool-keeps-file-version-unique),
along with what would break it. `tests/test_nessie_merge.py` pins the merge
body to an allowlist, so `defaultKeyMergeMode`, `keyMergeModes` and
`returnConflictAsResult` stay out. `_merge_ingest_branch` turns the bare 409
into a message naming the table, the version and the branch. It blames a
concurrent write only when Nessie's own message names this table's key
(`tests/test_merge_conflict.py`). The conflict is transient, so it is a plain
`RuntimeError`, not a refusal. The next attempt cuts its own branch
(`ingest_attempt_id`, merged since this branch began), re-reads the version
and merges. `registry/db.py`'s header no longer credits the pool.
*Verified live* with the branch's code in the airflow container, on
throwaway refs (`probe/todo15/*`, deleted afterwards; `main` stayed at
`523b887`). A wrong ref raised `could not read _file_version from
lakehouse.raw.`fo_trade@no-such-branch` for cob_date 2026-08-19: Nessie ref
'no-such-branch' does not exist`. Two branches each appended a row as
`_file_version=2`. The first merged into the stand-in for `main`, and the
second was refused with the new message. `python -m tests.run`: 1000 passed.
*What the item got wrong.* It expected the two concurrent files to tie after
merging. They never merge together.

**20, the SCD2 range test refused correct one-day versions and passed
same-day overlaps** — both `dbt_utils.mutually_exclusive_ranges` tests in
`dbt/models/prepared/_prepared.yml` (`ref_counterparty`, `ref_rating`) now
bound on the exclusive end, `upper_bound_column: date_add(effective_to, 1)`,
with `gaps: not_allowed`. `effective_to` is inclusive, and dbt_utils'
arithmetic assumes an exclusive end. The YAML comment says what the test
catches (an overlap, including a same-day one, a zero- or negative-length
range, and a gap) and what it does not (a missing or duplicate open version,
which `scd2_exactly_one_current_version` owns). Before `gaps: not_allowed`
was turned on, the change checked that nothing legitimately produces a gap.
`scd2_effective_to` is contiguous by construction, a retraction reopens the
version before it, and `apply_scd2_retention` deletes only `NOT is_current AND
effective_to < cutoff`, so it removes a prefix of a key's history, never a
middle version. `tests/test_scd2_range_test.py` reads the YAML and re-derives
the macro's arithmetic in DuckDB over a one-day version, a same-day overlap,
a gap and an open version. It failed on the old YAML.

*Verified live* on a throwaway Nessie branch. `raw.ref_counterparty` and
`raw.ref_rating` were created with ingest's own `ensure_raw_table` and
`ensure_raw_schema` and seeded with SQL. Both models were built, and each got
a key whose value changed on 09-03 and again on 09-04. With `main`'s YAML the
range tests failed on correct data (`FAIL 1` and `FAIL 4`, one per one-day
version). With the branch's YAML, the same tables passed (21 of 21).
`ref_counterparty` was then given a same-day overlap (a version ending 09-03
next to one starting 09-03), and `ref_rating` a gap (one version deleted). The
branch's YAML failed each with 1 row. `main`'s YAML still counted only the
one-day rows (`FAIL 1` and `FAIL 3`) and missed both defects. On Spark 3.5.3,
`date_add(DATE '9999-12-31', 1)` is a non-NULL DATE (`+10000-01-01`) that
compares correctly. Only collecting it into a Python `datetime` fails (`year
10000 is out of range`), and dbt only counts the failing rows. `main`'s hash
was the same before and after, and the branch was deleted.

*What the item got wrong.* Not much. Its DuckDB simulation gave the open
version `9999-12-30`, so it never exercised the real `9999-12-31` sentinel
that `date_add` has to step past. The branch's test uses the real one.

**09, `dedupe_rank` kept keys a `full_snapshot` re-delivery dropped** — the
owner's decision was that the macro was wrong and the documented meaning
stands: the newest delivery for a COB date is that date's population, and a
key it omits is absent from `prepared`, incrementally. Written down as
[`#a-snapshot-re-delivery-restates-the-whole-date`](../DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date),
with an `> **Amended.**` block on `#supersession-is-declared-not-assumed`,
which had said the macro "has always implemented" what it did not. Measured
first: the seed's one re-delivery (`fo_trade` 2026-08-13 `_v2`) has the same
400 keys as `_v1`, and nothing was published on `main`, so nothing depended on
the per-key reading.

Three changes, because the rank alone removes nothing. `dedupe_rank` gates on
the newest `_file_version` per COB date, computed after `known_as_of()`; the
five `cob_date`-partitioned models are `insert_overwrite` (project default and
per model, `unique_key` removed), because dbt-spark's MERGE has no delete
clause; and the two SCD2 models, which stay `merge`, take "newest" from
`newest_file_version()`, an unjoined aggregate, because a window over their
`touched`-scoped rows gets it wrong. The scaffold emits the new strategy, and
its generated comment no longer says uniqueness proves the dedupe works.
`tests/test_dedupe_rank.py` renders the real macro and models with jinja2 and
runs them in DuckDB; putting the per-key partition back fails seven of its
tests. `config.yml` installs `jinja2` for it, and the clean-virtualenv run of
that dependency set passed.

*Verified live, `prepared.fo_trade` only*, by the orchestrator on a throwaway
Nessie branch from `main` (deleted afterwards; `main`'s hash unchanged). An
extra COB date inside the lookback window (08-18) and one outside it (08-10)
were added to raw on the branch, the model built, and a synthetic
`_file_version` 2 for 2026-08-19 omitting 49 of its 400 keys appended. The
INCREMENTAL run's log showed `set spark.sql.sources.partitionOverwriteMode =
DYNAMIC` then `insert overwrite prepared.fo_trade`, no `merge into`.
Afterwards 08-19 held 351 rows, all version 2, with none of the 49 keys; 08-18
was rewritten whole (400 rows, the second run's invocation id); 08-10 was
untouched (the first run's); and the only new snapshot was one `overwrite`
with `replace-partitions=true`, `changed-partition-count=2`, 751 added, 800
deleted. A full-refresh as-of build at a knowledge time before version 2 gave
all three dates version 1's 400 keys; the same var on an incremental run was
refused with the compiler error. **The three reporting models were not run
live**; they use the same materialisation.

*Code review then found a HIGH defect in the SCD2 half, fixed in later commits
on this branch.* With the rank selecting the newest delivery, an incremental
run of `ref_counterparty`/`ref_rating` could no longer retract a version a
replaced delivery had begun: the replay started at the current version, found
nothing, and dbt-spark's merge cannot delete — so the retracted version stayed
current, and the key's next change opened a second one. Reproduced first in
`tests/test_scd2_incremental.py` (rendered models through dbt-spark's
incremental flow in DuckDB, beside a full rebuild; it failed on the rank
change alone). Fixed with marker rows in the model and a project
`spark__get_merge_sql` that deletes on them in the same MERGE, a replay that
starts at the version in force when the window starts, and a guard that a
date raw no longer holds is never a retraction; options (a)–(c) and why each
was rejected are in the DECISIONS entry. The review was also right about two
sentences: `ref_counterparty`'s header said a dropped key's version simply
"carries forward", and the DECISIONS entry's first draft called the stranding
"not new" — the per-key rank never stranded a version. Both are rewritten.

*The SCD2 retraction verified live* on two throwaway Nessie branches, with
`raw.ref_counterparty` created on the branch by ingest's own
`ensure_raw_table`/`ensure_raw_schema`. Four incremental runs: A=a and B=X
from 2026-08-03; 09-02 changes B to Y and adds C; 09-02 re-delivered with A
only; 09-03 brings B back as Y. dbt sent the project's merge — `when matched
and DBT_INTERNAL_SOURCE.effective_to = DATE '0001-01-01' then delete` and the
conditional insert — and after the re-delivery the table held exactly
`[A, a, 08-03, open]` and `[B, X, 08-03, open]`: the 09-02 version and C
retracted, B's earlier version reopened, Iceberg's snapshot recording 4
records deleted and 2 added. After B's return: `[B, X, 08-03 → 09-02]`,
`[B, Y, 09-03, open]` — one open version per key, no overlaps, no marker
rows, and all 9 of `ref_counterparty`'s dbt tests passed, including
`mutually_exclusive_ranges`. Both states compared IDENTICAL to a full refresh
over the same raw (the post-retraction one on the second branch). A throwaway
`merge` model with no `scd2_retractions` got dbt-spark's own
`when matched then update set *` and the right rows, so the override
delegates. `main`'s hash was the same before and after, and neither branch
survived. **Not run live:** `ref_rating` (same macros, host-tested) and the
three reporting models. **That run predates the two fixes below** (both re-run live further down) and
exercised neither.

*A final review then found two medium defects in the SCD2 fix*, both
reproduced by the reviewer's probe and by the orchestrator, and both now
tests in `tests/test_scd2_incremental.py` that failed on `2ae6052`:

- **The version before the replay start was never reopened.** The replay
  started at the version in force when the lookback window starts, and a
  re-delivery of THAT date could retract it — but the version before it was
  outside the replay, so it stayed closed (a drop left a gap no `as_of()`
  matches) or doubled (a revert left two back-to-back versions). Fixed in
  `scd2_replay`: the target's version before the replay start heads the
  replayed rows, so lead() reopens or extends it, and it is never read from
  raw, whose copy of its date retention may have pruned. (The replay's START
  version was still re-derived from raw, which retention prunes — fixed by
  todo 22, below.) The tests' history
  carries a version before that one too, whose raw date is still there, so
  a retraction scope that reached past the seed would be caught deleting it.
- **The replay scope compared RAW keys to the target's CLEANED keys**, in
  the markers and in the `replay_from` join, on both models: a raw ` B`, or
  `ref_rating`'s agency in another case, matched nothing, nothing was
  retracted, and a second open version followed. Fixed by ranking every raw
  row and applying the whole replay scope to the model's own cleaned stream
  (`cleaned`, or `ranked` for `ref_rating`), so there is one cleaning and no
  copy of it. Nothing else joins `touched` to the target.

The probe prints EQUAL for every case, padded and control. The review's
third, low finding is recorded as a risk in the DECISIONS entry, not fixed: a
delivery with no rows cannot supersede anything, because "newest" is read
off raw rows — item [21](21-an-empty-redelivery-cannot-supersede.md).

*A second review of those fixes confirmed the seed and cleaned-key logic*
through the harness against six further cases (two retractions in one run,
several dates reverted at once, a replay-start drop plus a new day, a longer
drop/revert chain, the newest version retracted with a seed present, a new key
added then dropped), and found five more issues, each now a test that failed
first:

- **A key that cleans to NULL was dropped by incremental builds only**
  (medium, a regression of the cleaned-key fix). `clean_string` maps `''`,
  `'NULL'` and `'N/A'` to NULL, and every join of the cleaned key used `=`,
  so an `'N/A'` row vanished from incremental builds while a full rebuild kept
  it — hiding it from the `not_null` test on exactly the builds that publish.
  Every join of the cleaned key, and the SCD2 merge's `on`, is now
  `IS NOT DISTINCT FROM` through one macro, `scd2_key_match`; Spark 3.5.3
  parses it to `EqualNullSafe`, and DuckDB accepts it where it rejects `<=>`.
- **The in-file dedupe ranked the RAW key** (low, pre-existing): ` B` and
  `B` in one file gave two versions with one `effective_from`, one inverted.
  The SCD2 models rank in `ranked_rows`, after cleaning, on the cleaned key.
  **Found, not fixed:** `fo_trade` and `ref_collateral` rank raw `trade_id`
  and `collateral_id` the same way, so ` T1` and `T1` in one file would both
  survive; their uniqueness tests would fail that build rather than publish
  it. Fixed since (plan #13): both, and the scaffold, rank in `ranked_rows`
  after cleaning.
- **A reopened seed row kept an earlier run's audit columns** (low): it now
  takes this run's `dbt_invocation_id`, `nessie_ref` and `dbt_updated_at`
  through `audit_columns()`, and keeps the target's `source_batch_id`.
- **The harness trusted DuckDB where Spark differs** (low): it now refuses a
  MERGE in which one target row matches several source rows, as Spark/Iceberg
  does and DuckDB 1.5.5 does not; runs dbt-spark's `append_new_columns` step
  before the merge; and runs `insert *` by name, as Spark means it. A test
  pins the seed's NULL for a column the target does not have yet.
- **`scd2_incremental_scope` was still named** in `_prepared.yml` and two older
  DECISIONS entries; all now say `scd2_replay`.

*Both reviews' fixes verified live* on `a5cc128`, five throwaway Nessie
branches, `ref_counterparty`, every build incremental unless it says full
refresh:

- **The first sequence again** (the retraction, B's return): the same states
  as before, 9/9 dbt tests, IDENTICAL to a full refresh at both steps.
- **The version before the replay start** (B: V 06-01, W 07-01, X 08-03,
  Y 09-02; then 08-03 re-delivered without B): W reopened to 07-01 → 08-31,
  X re-dated to 09-01 → 09-01, V untouched, Y open — IDENTICAL to a full
  refresh.
- **A padded key** (`B` on 08-03, ` B` after; 09-02 re-delivered without it;
  ` B` back on 09-03): only `[A, a, 08-03, open]` and `[B, X, 08-03, open]`
  at both steps, IDENTICAL to a full refresh.
- **Two spellings in one file, and a key that cleans to NULL:** 09-02's `B`
  then ` B` gave ONE 09-02 version, the later row's `Z`, and Spark accepted
  the MERGE; an `N/A` row on 09-03 appeared as a NULL-key version, was
  MATCHED rather than re-inserted on 09-04, and closed when a 09-04
  re-delivery changed its value. dbt's log shows `is not distinct from` in
  the replay's joins and in the MERGE's `on`. IDENTICAL to a full refresh.
  `not_null_ref_counterparty_counterparty_id` failed with 2 rows on the
  incremental table, as intended.

`main`'s hash was the same before and after, and no branch survived.

**One thing the procedure did not predict:** on that last branch
`mutually_exclusive_ranges` failed too, with 1 row. It is not this change:
dbt_utils' test defaults to `zero_length_range_allowed: false`, which
requires `effective_from < effective_to` strictly, and this project's
`effective_to` is inclusive, so any value in force for exactly one COB date
(`Q`, 09-03 → 09-03) is refused. The full refresh over the same raw is
identical, so it fails there too. Filed as item
20 (done, see its entry above), which also found
the test passes a same-day overlap.

One thing the run found that the host tests could not: the procedure's first
seeding helper built rows with `createDataFrame`, which needs Python workers
on the cluster, and those run 3.8 against the driver's 3.11
(`PYTHON_VERSION_MISMATCH`; item [24](24-spark-workers-run-python-3-8.md)). SQL `INSERT ... SELECT` literals, which stay in
the JVM, worked. The platform's own ingest does not hit this; a notebook or
script that does will.

What the item file got wrong. **Its SCD2 watch-out contradicted the design**:
it said a key dropped from a snapshot "should close its validity interval",
and the SCD2 models deliberately do not — an absent counterparty is carried
forward and flagged, and `scd2_exactly_one_current_version` expects it. That
is unchanged (retracting a version is a different thing; see above). **Its doc-gate watch-out did not apply**: `test_doc_claims`
matches a quoted message only in the form `` `"..."` ``, and every doc quoting
the `supersession:` refusal quotes it as an indented block, which it never
reads; and its NOT BUILT check exempts any paragraph containing "supersede"
as history, which is most paragraphs about supersession. The refusal text
did not change here, but nothing would have caught it if it had. **It missed a site**: `counterparty_exposure.delivered` grouped
`raw.ref_counterparty` across every version without calling `dedupe_rank`, so
a dropped counterparty still counted as delivered and was never flagged; it
ranks through the macro and filters on `known_as_of()` now. And **it asked for
`seed_clean/` to be checked, which was empty** in the checkout measured.

**08, the sniffer had no notion of member control files** —
`ingest/sniff.py` recognises members that carry their own control file and
proposes the `arrival.archive` shape's control half as `member_control`:
the `{stem}...` pattern, the control format, and candidate fields. The
console fills the arrival section and BOTH control blocks from it and leaves
`deliveryKind` at plain file. Covered by 23 tests in `tests/test_sniff.py` and
7 in `tests/test_delivery_form.py`; written up in
`DECISIONS.md#the-sniffer`.

*Choices.* Pairs are recognised **by name only**: a `ctl`/`trl`/`done`/`ok`
extension anywhere after the stem of another member (`POS_A.ctl`,
`POS_A.ctl.csv`, `POS_A.csv.done`). The item offered content detection too, and
it was refused as a classifier. A short key/value or one-row delimited file is
also what a one-row data delivery for a quiet day looks like, so treating it as
a control file would drop a real member without a word. Content is read only
once a pair is found, to propose how the file is read. `row_count` and `md5`
are offered only where the value **equals** the paired member's row count or
md5. `cob_date` is offered wherever every file holds a real date. Every
candidate is read back through `ingest/control.py` before it is offered.
Fields stay in the note and are never filled in, because which date is the COB
date is the business-key problem again. So a pre-filled form is refused until a
human types one, and the refusal names the date source.

*What the item got wrong.* **It said the current proposal was "right — but by
luck".** It is right only alphabetically. `.csv` sorts before `.ctl`; `.dat`,
`.txt`, `.done` and an upper-case `.CTL` do not. For those, the CONTROL file
was the member sniffed (one column named `date_20260901`) and `.*\.ctl` the
member pattern proposed. **It asked for `arrival.control.pattern` alongside
`member_pattern`, and that is not enough to load.** The container's derived
`filename_pattern` (`…\.zip`) is the landing pattern of `delivery.kind:
archive`; under `arrival.archive` it has to name each member, and a `\.zip`
there matches nothing for ever. The console also forced `deliveryKind:
archive`, which never reads a control file inside the container. So the
container's name now becomes `arrival_source_pattern` and `filename_pattern`
is withdrawn. The item also missed three stale statements on this path:
`propose_feed`'s comment that the gate does not unpack archives, the console's
claim that an undated container "cannot be onboarded", and the arrival
section's help saying the control file is "consumed at the door" (it is
PROMOTED). All three were corrected.

*Verified live.* Through `POST /api/sniff` on the console, a probe feed saved
with the form's payload, `config check` and `config show --origin`, and the
gate's own `plan_arrival` over the dropped container. Each `.dat` member was
paired with its `.ctl` and dated from it, and nothing was landed. The probe was
deleted, and `git diff main -- reporting_platform/config dbt` is empty. Unpaired
proposals were compared output for output against `main`'s sniffer and are
identical. The orchestrator re-verified it independently. It used a DATED
container of `.csv` members with delimited `.ctl.csv` controls, saved a probe
feed through the console, and planned both members dated from their control
files. A container missing one member's control file was refused.

*Code review found six issues, all fixed in a second commit on this branch.*
Extensionless data members (`POSA` beside `POSA.ctl`) were not paired, and got
`.*\.ctl` back. A two-line KEY|VALUE file was read as a two-column table.
Control files were decoded as UTF-8, not the proposed encoding, and indented
lines failed their read-back. Member bytes were held together and re-parsed
per candidate. Re-sniffing an existing feed overwrote its configured values
and format. And with no proposable pattern, the form auto-filled an arrival
control pattern alone, which cannot be saved and said nothing about why. A second
review found three more, fixed in a third commit. Ticking Arrival before
uploading counted the tick's `{stem}\.ctl` as a pattern already set, so the
proposed one reached neither control block. Extensionless member-pattern ties
depended on name order. And `A=1,B=2` over `C=3,D=4` was reported as an
ambiguous "KEY,VALUE" table, although its separator is not the delimiter.
A third review caught that fix going too far: it dropped the table reading
on the SHAPE of the separator, and `Time:UTC|Rows` over `T08:00|3` -- a `|`
table whose `Rows` is the member's row count -- lost a real reading with no
warning. The table is now dropped only when no field can be read under it.
The orchestrator re-verified the extensionless case end to end as well: a
probe feed from `POSA`/`POSA.ctl` members saved, and `plan_arrival` planned
both members dated from their indented control files.

*Found, not fixed* — now item [12](12-inbox-one-shot-dry-run-says-empty.md);
the same work also found items
[13](13-undated-file-sniff-prefills-an-unsaveable-form.md) and
[19](19-sniffer-can-propose-a-marker-file.md). **`inbox --dry-run` without `--loop` prints `inbox empty`
with a file in the inbox.** `STABLE_POLLS = 2` needs three observations and
once-off mode sweeps twice. To reproduce, stop the watcher, drop any file in
`./inbox`, then run `docker compose run --rm --no-deps -T inbox python -m
reporting_platform.ingest.inbox --dry-run`. Even with the file routed, its
dry run reports `would conform` for a container and never lists the members
or their control files.

**10, `DECISIONS.md` said `per_report` matches nothing** — an
`> **Amended.**` block on that paragraph of
`#published-tags-are-the-reproducibility-window`, not a rewrite: the reporting
build's `publish` cuts `published/<report>/<cob_date>/<run_id>` per report,
`expire_tags` reads the window out of `TAG_RE`'s `report` group through
`tag_retention_years`, and the block points at
`#an-ingest-is-not-a-publication`. It keeps why accepting the report segment
early mattered, and says why every tag still gets the default in practice:
`per_report` is empty, not unmatched. Checked in the code, not the prose —
`check_reproducibility_window` sizes the interlock per report through the
same resolver.

The item's line numbers all still held. Its **scope** did not: its grep
covered `docs/` and `CLAUDE.md` only, and the same claim was live in
`config/retention.yml`, in the comment on `default_keep_years` ("nothing
cutting a tag yet knows which report it is for") — in the same block as the
`per_report` comment, which already said it matches. Corrected the same way,
minimally. Every other hit is either history written as history
(`feed_ingest.record_snapshot`'s docstring, the comment on `TAG_RE`,
`#an-ingest-is-not-a-publication`'s own "could never match") or an unrelated
"matches nothing" about filename patterns. `RETENTION.md` was already right.

**11, `.claude/worktrees/` was not ignored** — `.gitignore` gains
`.claude/worktrees/`, with a comment saying what creates it and why it is not
`.claude/`. The item's command now prints nothing. Verified more strongly than
it asked: with two worktree agents checked out under `.claude/worktrees/` at
the time, `git status --porcelain` showed only the `.gitignore` edit, and
`git check-ignore` matches `.claude/worktrees/x` but not
`.claude/settings.json`. One thing the item got wrong: it listed repo-root
`grep -r` hits among the symptoms, and ignoring the directory does not fix
that one — `grep` never reads `.gitignore`, and with a worktree live
`grep -rl keep_years .claude` still returns its copy of `CLAUDE.md`. The
comment says so and names `--exclude-dir=.claude` or `git grep` instead.

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
`.env.example` aside and watching it fail rather than skip. Container, at the
time: `512 passed, 0 failed, 14 skipped`.

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
