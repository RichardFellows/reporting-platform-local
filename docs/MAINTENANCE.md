# Iceberg Table Maintenance

the legacy RDBMS gave us index maintenance, statistics updates and autogrow for free
(or at least, someone else's problem). Iceberg gives none of it. A lakehouse
with no maintenance job degrades predictably: small files accumulate, manifest
lists grow, planning time rises, and eventually a query that took 4 seconds
takes 4 minutes with no change to the data volume.

This is the single most common operational failure mode for teams arriving from
a database background, and it is worth stating plainly at design review.

## The five operations

All are Iceberg stored procedures, invoked via `CALL` from **Spark**. DuckDB
cannot perform any of them.

### 1. `rewrite_data_files` — compaction

Combines many small files into fewer right-sized files.

```sql
CALL lakehouse.system.rewrite_data_files(
  table => 'raw.trade',
  strategy => 'sort',
  sort_order => 'business_date, counterparty_id',
  where => 'business_date >= date "2026-08-01"',
  options => map('target-file-size-bytes','268435456',
                 'min-input-files','5',
                 'partial-progress.enabled','true')
);
```

Notes that matter:

- **Always scope with `where`.** An unscoped compaction on a table with 80
  month-ends will rewrite six years of data. Scope to recently-written
  partitions.
- `sort` strategy costs more than `binpack` but pays back on every subsequent
  read. Use `sort` for `prepared` and `reporting`; `binpack` is fine for `raw`.
- `partial-progress.enabled` commits in batches, so a failure halfway does not
  waste the whole run.
- Target file size: 256 MB is a sane default. For feeds of this size many
  partitions will be far smaller than one file; that is fine and compaction will
  correctly do nothing.

### 2. `rewrite_manifests`

Reorganises manifest files so that manifest-level partition pruning works.
Cheap, and worth running whenever compaction ran.

```sql
CALL lakehouse.system.rewrite_manifests('raw.trade');
```

### 3. `expire_snapshots`

Removes old snapshots and the data files only they referenced. On a plain
Iceberg catalog this is the operation that reclaims storage.

> **It does not work under Nessie, and must not be made to.** NessieCatalog
> sets `gc.enabled=false` on every table it creates, so Iceberg refuses:
> `Cannot expire snapshots: GC is disabled (deleting files may corrupt other
> tables)`. That guard is correct — under Nessie many references share the same
> data files, and no single table pointer knows what another branch or tag
> still needs. Forcing `gc.enabled=true` removes the guard, not the hazard.
>
> **Under Nessie, Nessie GC is what reclaims storage** (see "Nessie GC" below).
> `retention.py` detects `gc.enabled=false` and skips both this and
> `remove_orphan_files`, recording why, so the calls remain valid against a
> non-Nessie catalog. This was verified the hard way —

See `RETENTION.md` — it interacts with Nessie tag retention and must run after
tag expiry, not before.

```sql
CALL lakehouse.system.expire_snapshots(
  table => 'raw.trade',
  older_than => TIMESTAMP '2026-08-05 00:00:00',
  retain_last => 5
);
```

`retain_last` is a floor: never expire below this many snapshots regardless of
age, so an idle table always keeps a rollback point.

### 4. `remove_orphan_files`

Deletes files in the table's storage location that no metadata references —
the debris of failed writes.

```sql
CALL lakehouse.system.remove_orphan_files(
  table => 'raw.trade',
  older_than => TIMESTAMP '2026-08-08 00:00:00'
);
```

**Handle with care.** If `older_than` is set too recent, this will delete files
belonging to a write that is currently in flight, corrupting the table. Two
rules, both non-negotiable:

- `older_than` must be at least 3 days ago, and comfortably longer than the
  longest-running write in the estate;
- it must never run concurrently with ingest.

> **Enforced by a single shared pool.** `lakehouse_write` has one slot and is
> held by *every* task that writes: ingest (`feed_ingest.py`), dbt builds
> (`dbt_builds.py`), and both acting tasks of `platform_housekeeping.py`.
>
> It has to be one pool. An Airflow task belongs to exactly one pool, so a
> separate one-slot `iceberg_maintenance` pool does **not** exclude maintenance
> from writers — the two simply run in parallel. That was the original
> arrangement and it left this window open while looking deliberate; the pool
> has since been retired. If you find yourself adding a second pool "so
> maintenance doesn't queue", you are reintroducing the bug.
>
> The cost is real and accepted: a feed arriving mid-compaction waits. That is
> the right trade here — maintenance runs after the last publication of the
> day, and a late feed is late, not lost.

Clock skew between the Spark driver and the object store is a real cause of
incidents here. Three days of headroom absorbs it.

### 5. `rewrite_position_delete_files`

Only relevant for merge-on-read tables. Our `prepared` incremental models use
`merge` and will accumulate positional deletes; this compacts them back into the
data files.

```sql
CALL lakehouse.system.rewrite_position_delete_files('prepared.trade');
```

## Nessie GC

Iceberg's `expire_snapshots` only knows about the snapshots reachable from the
table pointer it is given. Nessie holds many pointers — every branch, every tag.
Nessie's own GC is what identifies content unreachable from *any* live
reference.

### How it runs

**There is no server-side GC endpoint and no REST call for this.** Nessie GC is
an external CLI (`nessie-gc`), published as a GitHub release asset — not on
Maven Central under that name. `Dockerfile.airflow` downloads it to
`/opt/platform/lib/nessie-gc.jar`, pinned to the same version as the Nessie server
in `docker-compose.yml`. Keep those equal.

`reporting_platform/retention/retention.py`'s `nessie_gc()` drives it, and policy
lives under `nessie_gc:` in `retention.yml`. It is **disabled by default**: it
deletes data files and is the one step in the chain with no undo.

The tool has no `--dry-run`. Its safety model is three stages, and this is
mapped onto the job's `dry_run` flag:

| | what runs | what is deleted |
|---|---|---|
| `dry_run=True` | `mark-live` only | nothing, ever |
| `dry_run=False`, `defer_deletes: true` (default) | `mark-live` + `sweep --defer-deletes` | nothing — files are *recorded* |
| `dry_run=False`, `defer_deletes: false` | `mark-live` + `sweep` | files deleted immediately |

So even a live run removes nothing by default. Review what it found with
`nessie-gc list-deferred`, then `nessie-gc deferred-deletes` to actually
delete. Deferred deletes require the JDBC live-set store (they are impossible
with `--inmemory`), which is why a `nessie_gc` database exists in
`scripts/init-postgres.sql`.

### The cutoff is a safety interlock, not a tuning knob

`default_cutoff` decides how far back each reference's commit log counts as
live. Content referenced *only* by commits older than it is collectable.

**It must be at least the longest `snapshot_retention_days`** — currently 30,
on `reporting`. A shorter cutoff collects files that surviving snapshots still
reference, breaking time travel and the scheduled BI extracts that
`snapshot_retention_days: 30` exists to protect. `retention.py` refuses to run
when this is violated rather than warning, because the damage is
unrecoverable. A cutoff not expressible in days (a commit count, an ISO
instant) cannot be checked, so it warns and proceeds on the operator's word.

Note the interaction with branch retention: every live branch keeps its commits
live, so a long `abandoned_after_hours` directly holds storage that GC would
otherwise reclaim. Branch hold, snapshot retention and GC cutoff are three
windows on the same data and should be reasoned about together.

Order of operations, nightly:

```
1. delete merged/abandoned ingest+build branches   (branch hygiene)
2. delete expired published/* tags                 (per retention policy)
3. nessie gc                                       (mark unreferenced content)
4. expire_snapshots per table
5. remove_orphan_files per table (older_than >= 3d)
```

Getting 2 and 3 the wrong way round is harmless. Getting 4 before 2 means the
tag still pins the files and you reclaim nothing while believing you did — the
storage graph flatlines and nobody notices for a quarter.

## Triggering: metrics, not a fixed schedule

Blind nightly compaction of every table wastes cluster time on tables nobody
wrote to. `reporting_platform/maintenance/maintain.py` reads Iceberg metadata tables first
and only acts where thresholds are breached:

| Metric | Source | Threshold → action |
|---|---|---|
| file count in recent partitions | `<table>.files` | > 50 files/partition → compact |
| avg file size | `<table>.files` | < 32 MB → compact |
| snapshot count | `<table>.snapshots` | > 100 → expire |
| manifest count | `<table>.manifests` | > 20 → rewrite_manifests |
| positional delete file count | `<table>.delete_files` | > 10 → rewrite deletes |

The thresholds are in `reporting_platform/config/maintenance.yml`. Every run emits the
metrics whether or not it acts, so you get a time series showing degradation
before it becomes a support call. Feed these to the existing Elastic/ECS
observability stack rather than inventing a new dashboard.

## Concurrency and the maintenance window

Maintenance rewrites files that queries may be reading. Iceberg's snapshot
isolation means readers are safe (they hold a snapshot), but a long-running
reader combined with aggressive `expire_snapshots` can produce
`FileNotFoundException` on the reader side.

Mitigations, in order of preference:

1. `snapshot_retention_days` comfortably exceeds the longest report runtime
   (hence 30 days on `reporting`, where scheduled BI extracts live).
2. Maintenance runs in a defined window after the last publication of the day.
3. `max_active_runs=1` on `platform_housekeeping` prevents self-collision, and
   the single-slot `lakehouse_write` pool prevents collision with any writer.

Do not attempt to run maintenance concurrently with ingest to "save time". The
requirement that each feed processes on arrival means arrivals are spread
through the day; the maintenance window is what makes that safe.
