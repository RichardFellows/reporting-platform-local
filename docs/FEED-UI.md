# The feed console

<http://localhost:8082> — a small web UI for onboarding a feed and running it
through the platform: `seed/` → landing → `raw` → `prepared` → `reporting`.

It is a front end for `docs/ADDING-A-FEED.md` and the run sequence at the end
of it. Read that document first: this console does exactly what it describes
and nothing else, and every trap it warns about is a validation message or a
label here.

```powershell
docker compose up -d feed-ui
docker compose logs --tail 20 feed-ui
```

---

## What it is not

**It is not a second source of truth.** Every read goes through
`common.context.feeds()`; every write goes back into
`reporting_platform/config/feeds.yml` and the dbt project. A feed added here
produces the same diff as one added by hand — check `git diff` after using it,
because that diff is the actual deliverable and it is meant to be reviewed.

**It is not a scheduler.** It triggers the ingest DAG and then watches. The
cascade into `prepared` and `reporting` is the platform's own: the ingest DAG
emits its feed's asset, `prepared_build` is scheduled on the OR of every raw
asset, `reporting_build` on prepared's. A second sequencer living in a UI
could disagree with that one, so there isn't one.

**It is not authenticated.** Same as Airflow's admin/admin and MinIO's
minioadmin on this stack. It writes source files and triggers builds, so
anything shared needs a real identity layer in front of it.

## Adding a feed

The **New feed** form writes all five files:

| | File | Written by |
|---|---|---|
| 1 | `reporting_platform/config/feeds.yml` | `ui/registry.py` |
| 2 | `dbt/models/raw/_sources.yml` | `ui/scaffold.py` |
| 3 | `dbt/models/prepared/<feed>.sql` | `ui/scaffold.py` |
| 4 | `dbt/models/prepared/_prepared.yml` | `ui/scaffold.py` |
| 5 | sample data | the **Data** tab, by upload |

There is no step registering the table for maintenance. There used to be, and
it was the one with no error if it was skipped — the table just never got
compacted, its snapshots never expired and retention never trimmed it.
`managed_tables()` now derives that set from the dbt project, so writing the
model registers it. See
[DECISIONS.md#managed-tables-are-derived](DECISIONS.md#managed-tables-are-derived).

### The filename pattern

The field most likely to be wrong, and it fails **silently**: `find_pending`
matches with `re.fullmatch`, so a pattern that covers only part of the
filename matches nothing and the feed reports nothing pending, forever.

Type a real delivered filename into **Example delivered filename** and press
**Derive pattern**. `COLLATERAL_20260819.csv` gives:

```
COLLATERAL_(?P<business_date>\d{8})(?:_v(?P<version>\d+))?\.csv
```

**Test** runs it the way arrival will and says which part failed. A pattern
that only partially matches is reported as such rather than as "no match",
because those two have completely different fixes.

### Columns and types

`columns` is the **declared contract**, not something discovered from the
file: a column in the file but not declared lands in `_extra_columns`, one
declared but absent lands as NULL, and both are reported as drift. Upload a
CSV next to the column list to read its header, then edit.

The per-column type says what the **prepared model** should do with the
column — raw stays all strings by design. It is inferred from the column name
(`*_date` → `parse_date`, `*_type`/`*_code`/`currency` → uppercased, `is_*` →
the Y/N/1/0/true normalisation, money words → `safe_cast(..., DECIMAL(18,2))`)
and every one is editable, before you create and afterwards.

**A type you change is recorded in `feeds.yml`**, under an optional
`column_types:` key, and only where it disagrees with the inference:

```yaml
    column_types:
      haircut_pct: decimal
```

A feed nobody has corrected has no such key and no diff. That sparseness is
the point: the block records *decisions*, not restatements of the default.

This used to say "the registry holds no types", and the type was thrown away
the moment it had been used to scaffold the model — the API re-inferred from
the column name on every read. The cost was not theoretical. A column typed
`decimal` in the form got `safe_cast(..., DECIMAL(18,2))` in the model, while
**Generate**, re-inferring `string` from the name `haircut_pct`, produced
values that could not cast. The column published as 100% NULL, and the build
went green — `safe_cast` is *meant* to land NULL rather than fail, and the
scaffolded tests deliberately do not cover a column's domain. Measured before
the fix: 75 rows, 0 non-null. One resolved map (`scaffold.resolve_types`) now
feeds the API, the scaffold and the generator, so they cannot disagree.

### What the scaffold deliberately does not generate

`accepted_range` and `accepted_values`. Both are statements about a feed's
domain that only its owner can make, and guessing produces the worst outcome
available: `min_value: 0` is right for a notional and wrong for an MTM, and a
scaffold whose tests fail on correct data teaches people to ignore failing
tests. The generated test block carries a comment saying to add them.

What it does generate is the minimum from `ADDING-A-FEED.md`: `not_null` on
the business key, `unique_combination_of_columns` over
`[business_date, <business key>]` — which is what proves `dedupe_rank` works —
and a `relationships` test on `counterparty_id` when the `counterparty` model
exists.

### Does dbt accept what was written?

The scaffold renders the model as text. **`dbt parse` is what proves dbt can
read it**, and the console runs it automatically after every scaffold, plus
on demand from **Validate dbt project**.

It takes about five seconds and needs no Spark, no cluster and no warehouse
connection. It builds the manifest, so it resolves every `ref()` and
`source()`, renders every model's Jinja and validates the schema YAML:

```
✗ dbt parse failed (exit 2)
  Compilation Error
    Model 'model.reporting_platform.collateral' depends on a node named
    'counterparty_typo' which was not found
```

**Parsing is structural and the UI says so.** It does not compile SQL against
the catalog, so a column that is not in raw, a value that will not cast, or a
test that fails on real data all still surface first in `prepared_build`.

It is also **project-wide** — dbt builds one manifest — so a failure may be
another feed's model rather than the one on screen.

The scaffold status pills are a **presence** check: the file exists, the entry
is in the YAML. Four green pills are not a claim that dbt accepts them, which
is why the parse is a separate, explicit answer.

Validation is skipped only when a scaffolding step itself failed — the
scaffold is then known to be incomplete, and a parse error would be a second,
vaguer report of the same problem. It still runs when every step reported
`skipped`: "the files were already there" says nothing about whether dbt can
read them.

### Nothing is ever overwritten

Re-running the scaffold (**Fill missing dbt files**) reports `skipped` for
anything already present. The derivations that make a prepared model worth
reading are hand-written, and a regenerate that clobbered them would destroy
exactly the work the scaffold is asking for.

## What the page keeps up to date

**Every panel that can go stale is re-read when a run or job finishes.** It did
not used to be: the feed pills and the run history were rendered once, so the
console would sit showing an amber `DAG not parsed yet` directly above three
successful runs *of that very DAG*, and `no runs yet` in the history table
below them. Both were true when drawn and false a minute later, and a page that
contradicts itself on one screen teaches you to disbelieve the pills that are
right. `refreshFeedViews()` now re-reads the DAG pill, the scaffold pills, the
stage strip and the history whenever a watched run or job reaches a terminal
state, coalescing a burst of them into one refresh.

**The stage strip is state, not decoration.** `seed → landing → raw → prepared
→ reporting` used to be five fixed labels. Each box now carries this feed's
actual position — file and object counts, and the last run state of each DAG —
and is coloured accordingly, so "where am I in the loop?" is answered without
reading anything else on the page. It comes from
`GET /api/feeds/<name>/state`, which is deliberately **Spark-free**: it is
redrawn after every run, and a strip that cost thirty seconds of cluster time
to draw would just get switched off.

**All output renders below all controls.** Job and run output used to appear
directly under the button that produced it, which reads better right up until
three run cards render and push *Test this feed* 75px down the page mid-click.
Everything now lands in one **Output** region beneath every control on the tab,
capped and scrolling, so nothing you are about to click moves.

## Adding columns quickly

Three ways, and the type is guessed in all of them:

| | |
|---|---|
| **paste a header row** | Comma or space separated, then Enter. Existing columns are left alone. |
| **read the header from a CSV** | Uploads the file and reads its first line. |
| **+ column** | One row at a time. |

`+ column` used to be the only one that did **not** guess: hand-typed columns
stayed `string` while the identical list read from a CSV came back correctly
typed. The guess now runs on blur for any column name however it arrived — and
a type *you* set is never overwritten by it.

## Deleting a feed

Type-the-name to confirm, and a **checkbox next to the button** for whether the
prepared model goes too.

That checkbox used to be a second `confirm()` dialog, raised *after* the
type-the-name gate had already passed — and Cancel or Escape there returned
`false`, which did not cancel anything: it deleted the feed and kept the file.
The one key a hesitant person reaches for was the one that committed. The
choice is now visible before you commit, and the confirmation prompt states
which way it is set.

Deleting never touches data. `lakehouse.raw.<name>` and everything built from
it stay exactly where they are — but they leave `managed_tables()`, so
retention and maintenance stop covering them and they grow untended.

## Generating sample data

A feed defined five minutes ago has nothing to test against, and
`generate_feeds.py` cannot help — its generators are hand-written per feed.
**Generate** on the Data tab emits deliveries from the definition itself: one
CSV per business date, into `seed/<feed>/`, named so the feed's own pattern
matches it (and refused if it would not — see `sampledata.filename_for`).

Three things it does that a naive generator would not, each of which is the
difference between a file that tests something and one that does not:

- **Dates come from what the OTHER feeds have in `seed/`**, not from today. A
  `relationships` test compares against reference data on the *same*
  business_date, so rows dated where `counterparty` has nothing would fail a
  test that has found nothing wrong with the feed.
- **Foreign keys are drawn from the real reference data**, read out of the
  other feed's seed CSVs — no Spark, no catalog. Random `CP#####` values fail
  `relationships` for reasons that say nothing about the feed under test.
- **Representations vary**: dates alternate `yyyy-MM-dd` / `yyyyMMdd`,
  booleans cycle `Y/N/true/false/1/0`. A generator emitting one clean format
  would leave `parse_date` and the boolean CASE — the code most likely to be
  wrong — untested.

**Values hold still.** A generated cell is drawn from its own
(row, column, epoch, version) stream, not from a per-file one, so a row keeps
its values across deliveries and changes only when its epoch rolls — months,
for most column types. Generate five days of a feed and you get five files
that differ in name and little else, which is what reference data looks like.

That is a fix, not a feature: it used to seed one RNG per business date, so
every value in every row changed on every delivery. A feed created here looked
like the most volatile market data imaginable rather than like the reference
data most new feeds are, and the change detection the prepared layer exists to
do had nothing to detect but noise. Date columns are anchored to the epoch
start for the same reason — anchoring them on the business date slid them
forward a day per delivery and defeated the whole thing for any feed with a
date in it. See `reporting_platform/common/volatility.py`.

Output is still fully deterministic: the same inputs give the same file across
restarts, and `version` remains part of the key so a `_v2` is a genuine
restatement.

## Proving a feed

**Test this feed** on the Run tab runs `dbt build` — the model **and** its
tests — for one feed, on its own Nessie branch, outside Airflow, streaming the
output into the page.

**It never merges.** The Airflow builds open a branch, build, test and merge to
`main` on success; that is publication. This is a dev proving a definition, and
publishing as a side effect of pressing "test" is exactly the surprise
write-audit-publish exists to prevent. On success the branch is deleted; on
failure it is kept, holding the exact bad data, the way `keep_failed_branch`
does in the DAG. `main` is untouched either way.

```
✗ failed   dbt build --select collateral
  2 of 7 FAIL 2 accepted_values_collateral_collateral_type__CASH__GOVT_BOND
  Done. PASS=6 WARN=0 ERROR=1 SKIP=0 TOTAL=7
  Branch build/feed-test/collateral/… kept for inspection — main is untouched.
```

**One build at a time.** Every Spark application here caps itself at 2 of the
worker's 6 cores, and Airflow's builds are serialised through the
`lakehouse_write` pool. A console that allowed five would be the one component
able to starve that pool from outside it, and the symptom — a DAG run stuck at
"Initial job has not accepted any resources" — points nowhere near the console.
A second request returns 409 with the running job's id, and the UI attaches to
it rather than telling you to try again.

Downstream reporting models are **not** built by default. A new feed usually
has none, and for one that does, a failure downstream is a different question
from "is my feed definition right".

### The whole loop, with no external file

```
New feed  →  Generate  →  Land selected  →  Ingest all pending  →  Test this feed
```

Verified end to end on a feed invented from nothing: six files written, dbt
parses in 6s, two deliveries generated against real counterparty ids, landed,
ingested as two runs, then `PASS=6 WARN=0 ERROR=0` on a throwaway branch that
cleaned itself up.

## Running a feed

**Data tab** — upload a delivery into `seed/<feed>/`, then land it. An upload
whose filename does not match the pattern is **refused**, with the reason: a
non-matching file in `seed/` is invisible work, since it lands and is then
never ingested. Landing goes oldest business date first, which matters on a
first load — a trade file landed ahead of its counterparty file fails the
`relationships` test on reference data that simply has not arrived yet.

**Run tab** — **Ingest next arrival** triggers one run, which ingests **one**
delivery. That is the platform's design (a feed is processed as it is
received, so the unit of work is a delivery), and it surprises anyone who has
just landed a backfill. **Ingest all pending** resolves the outstanding set and
triggers one run per object; `max_active_runs=1` drains them in order.

A generated DAG is created **paused** — it accepts a trigger and never runs
it. The console unpauses before triggering and tells you it did.

### Costs the console is explicit about

Two buttons say *runs Spark*, because they start an application on the shared
cluster holding 2 of the worker's 6 cores:

- **Check pending** and **Ingest all pending** call `find_pending`, which reads
  the raw table's own `_source_file` values.

Everything else on the page — seed listings, landed objects, pattern matching,
drift comparison — is answered from the filesystem and S3, so the page itself
never starts a JVM. **Validate dbt project** is the one other slow button at
about five seconds, and it is a dbt process only: no Spark, no cluster, no
warehouse connection.

### Pending can legitimately be empty

`find_pending` also skips deliveries outside the retention keep-set (10
business days plus 80 month-ends, computed from the dates present in
*landing*). A landed file for an expired date is not new, and re-ingesting it
would only give the next retention run something to delete.

## Editing and deleting

Editing rewrites one block in `feeds.yml` in place, keeping its position and
comments, and keeping any key the console does not manage — a hand-tuned
`arrival_timeout_hours` survives an edit here. A field returned to its default
has its override **removed** rather than left behind.

**Renaming is not offered.** The name is the raw table, the DAG id, the S3
prefix, the dbt source, the model and the `PREPARED_TABLES` entry at once.

**Delete removes the registry entry only.** The data stays exactly where it
is — and stops being in `managed_tables()`, so retention and maintenance stop
covering it and it grows untended. The dbt source entry, test block and
`PREPARED_TABLES` entry are left for you, because each lives in a file with
other feeds' content in it.

## When Airflow is down

The console still works: registering, scaffolding, uploading and landing need
nothing from the scheduler. The header pill reports Airflow separately from
its scheduler, deliberately — the REST API is served by the **webserver**, so a
dead scheduler answers every call perfectly while nothing anyone triggers ever
starts.

## How a new feed reaches the scheduler

`feeds()` is cached on `feeds.yml`'s **mtime**, not just its name (see
`common/context.py`). Without that, Airflow's DAG file processor — which
reuses worker processes across parses — would hold the pre-edit registry: the
feed would be written, `feed_ingest.py` re-parsed, and no `ingest_<feed>` DAG
would appear, with nothing reporting an error. In practice the new DAG shows
up within about 30 seconds. Until it does, the console says *DAG not parsed
yet* rather than pretending something is wrong.

## Layout

```
reporting_platform/ui/
  app.py            HTTP surface -- thin, decides nothing
  registry.py       feeds.yml, round-tripped so comments survive
  scaffold.py       the dbt files + PREPARED_TABLES
  feeddata.py       seed/, landing, uploads, drift comparison
  dbt_check.py      `dbt parse` -- does dbt accept the scaffolded files?
  sampledata.py     generate a delivery from the definition itself
  feedtest.py       `dbt build` for one feed, on a throwaway branch, never merged
  jobs.py           background jobs + streamed logs for the long ones
  orchestration.py  Airflow REST client
  static/index.html the whole front end, no build step
```
