# Data Retention

## Two delete modes

Retention has two paths, chosen per table by **detecting** whether it carries
`effective_from`/`effective_to`/`is_current` (`retention.is_scd2`). Detected rather
than configured: this file already records a hand-maintained table list that
drifted to five tables against the DAG's nine, and a list of which tables are
SCD2 would drift the same way — except the failure would be worse, since
retention would run the wrong delete against a table it believed was a
snapshot. The chosen mode is reported in the result JSON, so it is visible.

**Snapshot tables** — `DELETE ... WHERE business_date IN (...)`. Because
`business_date` leads the partition spec this is an Iceberg metadata
operation, the analogue of partition switching. It rewrites no data files.
This is why `partition_by=['business_date']` is described throughout this
document as a retention requirement.

**SCD2 tables** (`prepared.ref_counterparty`, `prepared.ref_rating`,
`prepared.ref_counterparty`) — that requirement does not apply,
because there is no `business_date` to partition by. Instead:

- a **current** version is never expired, however old — it is the answer to
  "what is this now", and dropping it would empty the dimension
- a **closed** version is expired only once its whole range sits before the
  oldest retained business date

This is a **row-level delete**: it produces delete files, and reclaiming them
is `rewrite_data_files` in the maintenance job rather than a metadata drop.
That is the real cost of SCD2 and it is deliberately not hidden.

In exchange the problem is much smaller. The three reference tables hold 2,412
rows where the snapshots held 13,660, and a dry run currently expires **none**
of them: the keep-set reaches back 80 month-ends and the entire version
history fits inside it. Retention on an SCD2 dimension bounds history, not
volume.

## What we are replacing

The legacy the legacy RDBMS reporting layer kept **10 working days plus 80 month-end
dates**, enforced by a nightly batch that used partition switching to move
expired partitions out and drop them. Partition switching was effectively free:
a metadata operation, no data movement, storage reclaimed immediately.

Nothing in Iceberg is free in that way, and the difference matters. On Iceberg,
"deleting" data is a two-stage process, and getting stage two wrong is the most
common way a lakehouse ends up costing more than the database it replaced.

## The two-stage model

**Stage 1 — logical expiry.** A `DELETE FROM` (or partition drop) writes a new
snapshot in which the rows are absent. Old snapshots still reference the old
files. **Nothing is reclaimed.** The table is correct; the bucket is not smaller.

**Stage 2 — physical expiry.** Something must delete the data files no longer
referenced by any surviving snapshot. Only now does storage drop.

On a plain Iceberg catalog that is `expire_snapshots`. **On Nessie it is not.**
NessieCatalog sets `gc.enabled=false`, so `expire_snapshots` and
`remove_orphan_files` both refuse to delete anything — correctly, because data
files are shared across references. **Nessie GC performs stage 2 here**, and it
is the only mechanism that can decide safely. Verified end to end: a real run
took the warehouse from 22.79 MB / 2,224 objects to 20.81 MB / 1,888, deleting
373 files with 0 failures.

**And a Nessie-specific third condition:** a file is only truly unreferenced if
no *Nessie reference* — no branch, no tag — still points at a commit that
contains it. A `published/2019-03-29` tag kept for audit will pin every file
that snapshot needed, indefinitely. **Tag retention is data retention.** This is
the single most important thing to get right, and the thing most likely to be
missed in review.

So the full chain is:

```
delete rows  →  delete stale Nessie tags  →  nessie gc  →  (deferred deletes)
```

Tags first: a tag pins every file its commit referenced, so GC before tag expiry
collects nothing while reporting success. `expire_snapshots` and
`remove_orphan_files` do not appear because Nessie disables them; see
`MAINTENANCE.md`.

## Policy configuration

All policy lives in `reporting_platform/config/retention.yml`. No retention rule is
hard-coded in a DAG.

Policy is keyed by environment, selected at runtime by `REPORTING_ENV`. `local`,
`uat` and `prod` share one anchor today (see "Non-prod" below); `dev` is
shortened separately.

```yaml
environments:
  local:   &full
    landing:
      keep_business_days: 10
      keep_month_ends_years: 8
      latest_version_only: true
      superseded_grace_days: 5

    raw:
      keep_business_days: 10
      keep_month_ends: 80
      snapshot_retention_days: 7
      snapshot_retain_last: 5

    prepared:
      keep_business_days: 10
      keep_month_ends: 80
      snapshot_retention_days: 7
      snapshot_retain_last: 5

    reporting:
      keep_business_days: 10
      keep_month_ends: 80
      snapshot_retention_days: 30
      snapshot_retain_last: 10

  uat: *full
  prod: *full
  dev:  # shortened — see "Non-prod"
```

`snapshot_retain_last` is a floor that applies regardless of age, so an idle
table always keeps a rollback point. `superseded_grace_days` delays removal of
a superseded `_file_version` so a bad re-delivery can still be investigated.

Nessie reference retention lives in the same file under a separate top-level
`references:` key — `published_tags` and `working_branches`. That separation is
deliberate: tag retention is data retention (see above), but it is not a
per-layer property.

**Published tags are sized in years, not by keep-set**, and that is the one
place where the shape of the policy differs from everything else in this file.
See *The reproducibility window* below.

### `keep_business_days`

The N most recent **business dates actually present in the table**, not the last
N calendar days. If upstream skipped a day, we keep 10 real dates, not 9 plus a
gap. This matches the legacy behaviour and is what report users expect when they
ask for "the last two weeks".

Business dates are read from the table itself
(`SELECT DISTINCT business_date`), not from a calendar table. This deliberately
avoids maintaining a holiday calendar for every jurisdiction in scope.

### `keep_month_ends`

The last available business date **in each month**, not the calendar last day of
the month. 30 March 2029 is a Friday; 31 March is a Saturday; the month-end
snapshot is the 30th. Deriving this from observed dates rather than a calendar
is again the safer choice.

`keep_month_ends: 80` ≈ 6 years 8 months, matching the legacy figure exactly.
The stated desire to extend to 8 years of *all* dates is a separate,
much larger commitment — see "Open question: extended retention" below.

### Landing: everything, for ten years

**The evidence copy, and it does not follow the table rule.** Every CSV every
feed has ever delivered is kept for `landing.keep_years` (10) and then removed —
including superseded re-deliveries. `TRADE_20260813.csv` and
`TRADE_20260813_v2.csv` both live out their ten years.

Raised from eight when the published-tag window was corrected: landing must
outlast the pins, or a published run stays reproducible after the evidence it
was built from is gone. `retention.py` refuses to sweep if it does not — see
*The reproducibility window*.

That is deliberate, and it is the opposite of what an earlier draft of this
document specified. Landing exists to answer *"what did the file we actually
received say?"* — a question normally asked after a restatement, about a
business date the table layers expired years ago. Sampling it by keep-set, or
dropping superseded versions, destroys exactly the evidence it exists to
preserve, and saves the cheapest bytes in the estate: flat CSV on object
storage.

`reporting_platform/retention/landing.py`, run as the last step of
`retention.run()` and therefore nightly:

```bash
docker compose exec -T airflow python -m reporting_platform.retention.landing --dry-run
docker compose exec -T airflow python -m reporting_platform.retention.landing
```

Three properties worth knowing:

- **Age means business date, not upload time.** A file re-delivered late
  carries an old business date and a recent `LastModified`; the data in it is
  still ten years old, and retention is a question about the data.
- **An object whose name matches no feed pattern is never deleted.** It is
  counted and warned about, not swept. Deleting something unidentifiable out
  of the evidence prefix is not this job's call.
- **`keep_years` must be ≥ the raw layer's window** (`keep_month_ends / 12`,
  currently 6.7 years). `find_pending` derives its retention keep-set from the
  business dates present in *landing*, precisely so a date expired from the
  table is recognised as expired rather than re-ingested. Truncate
  landing below the raw window and live month-ends start looking expired.
  `landing.py` warns; it does not refuse, because the failure is gradual and
  an operator shortening landing in a sandbox should not be blocked.
- **`keep_years` must also be ≥ the longest published-tag window**, and *that*
  one refuses. See *The reproducibility window*.

### Ready: the work queue, for a week

`ready/` holds one manifest per delivery, plus any parts a normalizer derived
(see
[DECISIONS.md#ready-is-a-derived-index](DECISIONS.md#ready-is-a-derived-index)).
**It is a cache**: everything in it can be rebuilt by re-normalizing from
`landing/`, which is what makes it safe to delete from and why its window is
`ready.keep_days` (7) rather than years.

```bash
docker compose exec -T airflow python -m reporting_platform.retention.ready --dry-run
docker compose exec -T airflow python -m reporting_platform.retention.ready --apply
```

Unlike `landing:`, this window constrains nothing — nothing computes a
keep-set from `ready/`, and it has no correctness floor either. What protects
a delivery waiting on a late control file is the rule below, not this number.

**One rule, and it is the reason this is its own module: a manifest whose parts
are not yet in the raw table is never swept, at any age.** That is a read of
`already_ingested`, not a status flag in the manifest — the manifest never
records derived state. Sweeping an un-ingested delivery is not data loss, since
landing still holds the object, but nothing would re-normalize it on its own,
so it is a *silent* drop, which is worse than a loud one. The sweep reports
those as `held_uningested`.

A part that points back into `landing/` is the evidence copy and is never
deleted here; only parts under `ready/` are.

Until session 5 none of this existed: the `landing:` block was four keys no
code read, and this section described behaviour that had never run. The policy it
described — `latest_version_only`, a `superseded_grace_days`
window — has been replaced rather than implemented, for the evidential reason
above.

### Quarantine: refused deliveries, for ten years

`quarantine/` holds what an upstream sent that the conformance gate would not
accept — an unroutable name, a name two feeds claim, a delivery that cannot be
given a landing name at all — and `registry.rejection` says what was wrong with
it (see
[DECISIONS.md#quarantine-is-where-a-refused-delivery-goes](DECISIONS.md#quarantine-is-where-a-refused-delivery-goes)).

```bash
docker compose exec -T airflow python -m reporting_platform.retention.quarantine --dry-run
```

**Kept the way `landing:` is** — flat age, everything, no keep-set — because it
answers the same question from the other side: *what did they actually send
us?*, asked about a delivery that never arrived. A rejected file is frequently
the whole explanation for a missing business date, and the explanation is
needed for as long as the date it is missing from.

**Its own key, not a reference to landing's**, even though the value is the
same today. Landing's window has two hard floors — the raw keep-set and the
published-tag interlock. This one has neither: nothing is reproduced from a
delivery that never landed, so shortening it is a policy call somebody can make
on its own.

**Dated from the object's own key**, `quarantine/<feed>/<yyyy>/<mm>/
<timestamp>_<name>`, rather than from the filename. Landing dates a delivery by
parsing its name and refuses to delete what it cannot parse; nothing here is
parsable by contract — *not being nameable* is a common reason a file is in
quarantine — so the platform puts the date in when it writes the object. A key
whose folders and timestamp disagree is still left alone.

**The rows are not swept.** `registry.rejection` is small, and keeping it after
the bytes expire is what leaves "has this upstream sent us something broken
before?" answerable.

### The reproducibility window

**A published tag pins every data file its commit referenced.** So how long a
tag lives is how long a published run can still be *reproduced* — a different
question from how much history the tables serve, and it must not be answered
with the tables' policy.

It was, until this was corrected. `references.published_tags` carried
`keep_business_days: 10` and `keep_month_ends: 80` — the numbers the table
layers use. In practice that meant:

- an ordinary daily publication lost its pin once ten more business dates had
  been published, roughly a fortnight;
- month-end pins lasted 80 ÷ 12 ≈ 6.7 years, short of the assumed period;
- and within a *retained* date, only the newest tag survived. The tag name
  carries no feed (`published/<business_date>/<run_id>`) and
  `record_publication` runs in every per-feed ingest DAG, so N feeds
  publishing one business date cut N tags for it and N−1 were deleted the same
  night. Observed on the live catalog: three tags for `2026-08-01`, all inside
  the keep-set, two of them scheduled for deletion.

The policy is now **flat age in years, resolved per report**, on the same
reasoning `landing:` already uses — sampling evidence by keep-set destroys
exactly what it exists to preserve:

```yaml
references:
  published_tags:
    default_keep_years:          # bare number, or per REPORTING_ENV
      local: 10
      dev: 1
      uat: 10
      prod: 10
    per_report: {}               # <report>: <years>
```

Ten years is **provisional**: the regulatory period is not confirmed, and
over-retaining costs storage while under-retaining costs the evidence
permanently, so it is set to the longest plausible value rather than the
assumed seven.

`per_report` matches nothing today — no publication yet knows which report it
is for, so every tag resolves to the default. It is written down as a forward
hook, and `TAG_RE` accepts `published/<report>/<business_date>/<run_id>`
alongside today's shape, so the day a publication does name its report,
retention honours it rather than silently applying the default.

**Age is measured from the commit time**, not the business date: a retention
period runs from when the record was made, and a restatement published today
for an old business date is a new record that must survive its own full
window. The business date is the fallback when a tag carries no readable
commit time, and it is conservative by construction — a publication cannot
precede the date it reports on.

#### The interlock

`check_reproducibility_window()` **refuses** to run retention when
`landing.keep_years` is shorter than the longest published-tag window. A tag
pins the *tables*; reproducing a published run also means showing its inputs,
and `landing/` is the only copy of what the upstream actually sent.

It refuses where `landing.keep_years()`'s own interlock only warns, and the
difference is what the failure costs: landing running short of the raw window
degrades `find_pending` gradually and is fixed by raising it, whereas deleting
landing evidence a live pin depends on is not recoverable, and the sweep that
would do it runs nightly and unattended.

**This is not the full REQ-602 interlock**, and the gap is worth stating. The
complete rule is "retention must not delete anything a published *run* depends
on", which needs a run record enumerating its delivery set. Until that exists,
the window comparison catches the configuration that guarantees the loss; it
cannot catch one delivery expiring early inside an otherwise coherent window.

#### Verified

Against the running Nessie and MinIO, not inferred:

- A tag's own state stays live at **any** GC cutoff. `mark-live` at `NONE`,
  `P30D` and `P0D` gave `numContents` 171, 171 and 41: the identify phase
  walks each reference from its HEAD and stops at "the first non-live commit",
  so the cutoff bounds how much history *behind* a reference stays live, never
  whether the reference's own state does. The tag's lifetime is therefore the
  only thing that decides reproducibility.
- All five published tags on the catalog pin distinct commits, none equal to
  `main`.
- Reading `lakehouse.raw.fo_trade` at the oldest of them returns its rows.
- The old policy would have deleted two of the five that night; the new one
  keeps all five.

**Not verified**: the full reclamation chain across the deferral window —
`maintain.py`, snapshot expiry, GC identify, then the deferred-delete pass —
with a tagged snapshot surviving it. No sweep or delete phase was run.

#### The standing check

`reporting_platform/monitoring/reproducibility.py` reads every managed table at
a published pin and fails the night if one that exists can no longer be read.
It runs as the last task of `platform_housekeeping`, *after* the maintenance
chain, so it observes the state that chain left behind; and it only counts a
pin older than `recent_partition_days`, because a younger one still shares its
files with `main` and would resolve whether pinning worked or not. See
[DECISIONS.md#reproducibility-is-exercised-not-asserted](DECISIONS.md#reproducibility-is-exercised-not-asserted).

```bash
docker compose exec -T airflow python -m reporting_platform.monitoring.reproducibility
docker compose exec -T airflow python -m reporting_platform.monitoring.reproducibility --tag published/2026-08-01/<run_id>
```

## Implementation

`reporting_platform/retention/retention.py` executes, per table:

1. Read the distinct business-date values. Note the column name differs by
   layer: `raw` carries the ingest metadata column `_business_date`, while
   `prepared` and `reporting` carry a modelled `business_date`. `retention.py`
   selects between them per layer — see `run()`.
2. Compute the keep-set: last N business dates ∪ last M month-end dates.
3. `DELETE FROM <table> WHERE <date_column> IN (<expiry-set>)`, where the
   expiry set is the observed dates minus the keep-set. Because that column is
   the partition column, Iceberg resolves this to a partition-level metadata
   delete — the closest analogue to partition switching, and it does not
   rewrite data files.
4. `CALL system.expire_snapshots(table, older_than, retain_last)`.
5. Report reclaimed bytes.

Steps 1–3 run on a Nessie branch and are merged, so the expiry itself is a
reviewable commit. Steps 4–5 run against `main` because snapshot expiry is not
a branchable operation.

Tag expiry runs separately in `retention_tags`. See *The reproducibility
window* below for what it does and why it is not the keep-set.

### Nessie GC

Between tag expiry and `expire_snapshots` sits `nessie_gc()`, which collects
content unreachable from *any* Nessie reference — the thing per-table snapshot
expiry structurally cannot see. It is disabled by default and its cutoff is
interlocked against `snapshot_retention_days`; the mechanics are in
`docs/MAINTENANCE.md` under "Nessie GC". Ordering matters for the same reason
it does everywhere else here: run it before tags are expired and the tags still
pin the content, so it collects nothing while reporting success.

**Identification and removal are two steps, a window apart.** The sweep runs
with `--defer-deletes`: it *records* the files it would remove and deletes
nothing, so there is a review period before anything irreversible happens.
Executing those records used to be `nessie-gc deferred-deletes` typed by a
human, which meant in practice that nothing was ever reclaimed unattended.
`deferred_deletes()` now runs in the same nightly chain and actions every
live-set older than `nessie_gc.deferred_delete_after_hours` — per environment,
because the window is really "how long a human needs to notice" and a laptop
is not prod.

Two consequences worth holding on to:

- **Reclamation is lagged, not immediate.** Tonight's run deletes what a sweep
  some days ago identified. A night that reclaims nothing is therefore a
  perfectly correct night, and no assertion should treat it as a fault — see
  `storage_report`'s docstring for what can honestly be asserted instead.
- **Do not point a reference backwards while deletes are outstanding.** A file
  recorded as unreachable is deleted later. Reassigning a ref to an old hash,
  or branching from a pre-sweep commit, resurrects content the next pass will
  then delete. This was equally true of the manual step; a longer window
  widens the exposure.

### Working-branch cleanup

`clean_working_branches()` deletes `ingest/*` and `build/*` branches older than
`references.working_branches.abandoned_after_hours`. That window is **per
prefix**, because one number was serving two different needs and serving the
more important one badly:

| prefix | window | why |
|---|---|---|
| `build/` | 120h | A build that fails at 22:00 on a Friday must still be there on Monday morning. Under a global 48h it was swept on Sunday night — the evidence `keep_failed_branch` exists to preserve, destroyed at precisely the moment nobody had yet looked at it. 120h covers a weekend with slack. |
| `ingest/` | 48h | Nobody diagnoses a crashed ingest branch individually; the feed is re-ingested. Holding it longer only pins files GC would otherwise collect. |

Keep both **≤ the shortest `snapshot_retention_days`** (7d = 168h) or branch
retention and snapshot retention work against each other — a branch outliving
the snapshots its commits reference. `retention.py` logs a warning if that is
violated; it is a storage cost, not a correctness one, so it does not refuse.

#### Holding a branch for investigation

**Rename it out of `ingest/`/`build/`, into `hold/`.** The sweep matches only
those two prefixes, so `hold/` is exempt *by construction* — there is no
special case in the sweep that could be forgotten or broken. This is the
supported way to keep a branch under investigation indefinitely.

It is not free. A live branch pins every file its commits reference, so Nessie
GC cannot collect them for as long as the hold lasts. Holding is a deliberate
act with an ongoing storage cost, which is why it is a rename a person performs
rather than a policy value that could quietly apply to everything.

Three properties matter and all three were once broken (see bugs
#10 and #31):

- **Branch age requires `fetch=ALL`.** Nessie returns only `type`/`name`/`hash`
  from `GET /trees` unless asked for more; the `metadata.commitMetaOfHEAD.
  commitTime` the age check reads is absent otherwise. Without it the age
  comparison silently sees `None` and every branch looks arbitrarily old — the
  function deleted branches an ingest or build was actively writing to.
- **A branch with no commits of its own has no age.** `commitMetaOfHEAD`
  describes whatever commit the branch points at, and a freshly opened branch
  points at its *base*. So the age read for it is main's age, not the
  branch's. On a quiet main — a long weekend, or a platform that publishes
  weekly — a branch opened seconds ago reads as days old, and the next sweep
  deletes it out from under a running build. Nessie exposes no
  branch-creation timestamp, so `numCommitsAhead == 0` is treated as unknown
  age and the branch is left. The cost is that an empty abandoned branch is
  never swept; the watchdog's working-branch count is what catches those
  accumulating.
- **Unknown age fails safe.** If a commit time still cannot be determined the
  branch is kept and a warning logged. Deleting a branch is destructive and
  unrecoverable; a branch kept a night too long costs storage, one deleted
  mid-write costs the run.

This is the one part of retention that can destroy work in progress rather than
merely expired data, so it deserves the extra caution.

## Partitioning is a retention decision

`business_date` must be the leading partition field on every table in `raw`,
`prepared` and `reporting`. If it is not, retention deletes become row-level
deletes: read every file, rewrite it without the expired rows, leave delete
files behind. That turns a metadata operation into a full table rewrite every
single night.

Use `days(business_date)` (Iceberg's identity-ish day transform) rather than a
derived `yyyymm` string, so partition pruning works for range predicates too.

Do not add a second high-cardinality partition field "for query performance"
without measuring. Small files are a bigger problem in this estate than
partition pruning, given typical feed volumes.

## Non-prod

Today non-prod holds production data, on-prem, restricted to prod-authorised
users. Retention policy is therefore currently *identical* across environments.

When masking/subsetting arrives, retention should shorten in non-prod
(`keep_business_days: 5`, `keep_month_ends: 3`) — but note that shortening
retention in non-prod removes your ability to reproduce a production month-end
issue in a lower environment. Budget for a "restore a month-end into non-prod"
procedure rather than assuming the data will be there.

## Open question: extended retention

Retaining *all* dates for 8 years, rather than 10 days plus month-ends, is
roughly a 20–25× increase in retained rows for a daily feed. Before committing:

- Is the driver regulatory (a defined obligation) or "we might want it"?
  These have very different answers.
- Does it need to be *queryable*, or *recoverable*? Queryable means Iceberg and
  full cost. Recoverable means the landing copy on cheaper storage and a
  documented rehydration procedure — far cheaper, and adequate for most
  "prove what we published" questions.
- The recommendation on the table would be: extend **landing** retention to 8
  years of everything (it is flat CSV, the cheap copy — note that landing
  objects are currently stored uncompressed, so costing this properly means
  either assuming raw CSV volume or adding compression at the landing step),
  and keep the
  Iceberg layers on 10 days + 80 month-ends. Rehydrate on demand. That gets most
  of the value at a fraction of the cost — but it is a proposal, not a
  conclusion, and it depends on the answer to the first question.
