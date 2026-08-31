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

**SCD2 tables** (`prepared.counterparty`, `prepared.rating`,
`prepared.primary_limits`) — that requirement does not apply,
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

### Landing: everything, for eight years

**The evidence copy, and it does not follow the table rule.** Every CSV every
feed has ever delivered is kept for `landing.keep_years` (8) and then removed —
including superseded re-deliveries. `TRADE_20260813.csv` and
`TRADE_20260813_v2.csv` both live out their eight years.

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
  still eight years old, and retention is a question about the data.
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

Until session 5 none of this existed: the `landing:` block was four keys no
code read, and this section described behaviour that had never run. The policy it
described — `latest_version_only`, a `superseded_grace_days`
window — has been replaced rather than implemented, for the evidential reason
above.

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

Tag expiry runs separately in `retention_tags`, driven by the same keep-set
logic applied to `published/<business_date>/<run_id>` tag names.

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
