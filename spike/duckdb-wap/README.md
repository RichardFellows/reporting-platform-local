# Spike: DuckDB → Iceberg on a Nessie branch, write-audit-publish

**Question.** Can DuckDB write to a Nessie *branch* over Iceberg REST, so that
a build can be audited before it is published — the same write-audit-publish
this platform gets from Spark, at a fraction of the start-up cost?

**Answer.** Yes, and not out of the box. DuckDB 1.5.5 writes to Nessie
happily, isolates a branch correctly, and the gate works in plain SQL. But the
branch is unreachable through the documented path, and the way it is
unreachable is the finding: `ATTACH` **succeeds** and the branch reads as an
**empty catalog**. One ~40-line rewriting proxy makes it work. Whether that is
a bridge worth owning is the decision this spike exists to inform; the last
section argues it is not, yet.

Everything below was run against the live stack. Re-run it:

```bash
docker compose cp spike/duckdb-wap/nessie_ref_proxy.py airflow:/tmp/
docker compose exec -d airflow python /tmp/nessie_ref_proxy.py

docker compose cp spike/duckdb-wap/poc.sql airflow:/tmp/
docker compose cp spike/duckdb-wap/run.py airflow:/tmp/
docker compose exec -T airflow python /tmp/run.py            # the WAP cycle

docker compose cp spike/duckdb-wap/constraints.py airflow:/tmp/
docker compose exec -T airflow python /tmp/constraints.py    # the findings
```

## Phase 0: what the spec assumed, and what was already here

The spec's prerequisites were mostly already satisfied, which changed where
the time went.

| assumed | actual |
|---|---|
| DuckDB ≥ 1.4.2 | **1.5.5**, host and container |
| "confirm Nessie has a warehouse configured" | already does — `nessie.catalog.default-warehouse=warehouse`, added for `scripts/duckdb_console.py`. See `docs/DECISIONS.md#nessie-iceberg-rest` |
| "stand up MinIO on localhost:9000" | already here, `minio:9000` inside the network, `19000` on the host |
| MinIO on `localhost:9000`, Nessie on `localhost:19120` | **the spike must run inside a container**: Nessie vends `http://minio:9000` as the S3 endpoint to its clients, and the host cannot resolve that name |

One correction to the spec's Phase 1: **no S3 secret is needed to create or
write a table.** The catalog vends credentials for its own data files, for
writes as well as reads. A secret is needed only for a direct
`read_parquet('s3://…')`, which is not the catalog's business.

## Phase 1: baseline on main — works, unmodified

`ATTACH`, `CREATE SCHEMA`, `CREATE TABLE`, `INSERT`, `SELECT`. No surprises.
This is the same path `scripts/duckdb_console.py` already uses, minus its
`READ_ONLY`.

## Phase 2: the seam

This is the whole spike. DuckDB does two individually reasonable things:

1. it calls `<ENDPOINT>/v1/config?warehouse=<attach name>`, and
2. it builds every later URL as `<ENDPOINT>/v1/<defaults.prefix>/…` — appending
   to **the endpoint it was given**, not to the `uri` override the config
   response carries.

Nessie's prefix is `{ref}|{warehouse}`, and it *will* vend a branch-scoped
one — ask `/iceberg/etl_x/v1/config` and it answers `prefix:
etl_x%7Cwarehouse`. So the ref ends up in the URL **twice**. Measured through
a logging proxy, this is exactly what DuckDB sends:

```
GET /iceberg/v1/config?warehouse=warehouse                    -> 200
GET /iceberg/v1/main%7Cwarehouse/namespaces                   -> 200
GET /iceberg/v1/main%7Cwarehouse/namespaces/raw/tables        -> 200      main: fine

GET /iceberg/etl_x/v1/config?warehouse=warehouse              -> 200      prefix: etl_x%7Cwarehouse
GET /iceberg/etl_x/v1/etl_x%7Cwarehouse/namespaces            -> 404      <- the ref, twice
```

**And the 404 is rendered as an empty catalog.** `ATTACH` returns success,
`information_schema.tables` is empty, and every query says the branch has no
such table. Nothing anywhere says "404", "branch" or "prefix". A pipeline
built on this would report a clean run having written nothing.

Things that do **not** work, all confirmed by `constraints.py`:

- `ENDPOINT '…/iceberg/etl_x|warehouse'` (the spec's first suggestion) — the
  raw `|` makes DuckDB's own HTTP client reject the URL: `BadRequest_400`.
- `ENDPOINT '…/iceberg/etl_x%7Cwarehouse'` — attaches, and is empty, same as
  the bare form.
- `ATTACH 'etl_x|warehouse'` — Nessie answers the *config* call with
  `500 Warehouse 'etl_x|warehouse' is not known`. The `?warehouse=` parameter
  takes a warehouse name only; the ref is not selectable there.
- `PREFIX '…'` / `WAREHOUSE '…'` as `ATTACH` options — `Unhandled options
  found`. There is no override. (`MAX_TABLE_STALENESS` and
  `ACCESS_DELEGATION_MODE` *are* accepted, so the option list is not the
  problem.)

**The fix is one rewrite**, and the spec predicted its shape.
`nessie_ref_proxy.py` strips the ref segment from every path except
`/v1/config`, which must pass through untouched or Nessie vends `main`'s
prefix and the write lands on the wrong branch:

```
/iceberg/etl_x/v1/config          ->  /iceberg/v1/config          (unchanged)
/iceberg/etl_x/v1/etl_x%7Cwh/...  ->  /iceberg/v1/etl_x%7Cwh/...  (stripped)
```

With it: `tables=['raw.fo_trade', 'wap_poc.trades']` on the branch, and
isolation holds — 6 rows on the branch while `main` still reads 1.

Note DuckDB ignores the `uri` override in the config response, which is what
makes a proxy viable at all: it keeps talking to whatever host you pointed it
at, so nothing redirects it back to Nessie behind the proxy's back.

## Phases 3–6: the cycle, both ways

```
=== Write the BAD fixture on the branch.
    branch after write, and main during it: branch_rows=6, main_rows=1
=== Audit.
    result: null_ids=1, dupe_ids=2, bad_notional=0
    GATE: FAIL -> discard
=== The gate failed, so discard the branch and prove main is untouched.
    nessie: drop etl_wap_poc -> ok
    main after the discard: rows=1
=== Now the happy path: the good fixture, a passing gate, and a publish.
    GATE: PASS -> publish
    nessie: merge etl_wap_good -> ok
    main after the merge: rows=4
```

The failure path is the claim, and it holds: a bad build is written, audited,
refused and discarded without `main` moving.

Two notes on the audit query as the spec wrote it. `count(*) -
count(DISTINCT trade_id)` counts a NULL id as a duplicate too, because
`count(DISTINCT)` ignores NULLs — hence `dupe_ids=2` for one duplicate and one
NULL. Harmless for a gate that requires every column to be zero; misleading if
anyone reads the number. And the gate is only as good as the rules: this one
would not notice a wrong `notional`, only a non-positive one.

## Phase 5's predicted trap: not observed

The spec expected merged rows to be invisible until `DETACH`/`ATTACH` or a low
`MAX_TABLE_STALENESS`. Measured on a connection attached to `main` *before*
the merge and queried after it: **it saw the new rows immediately**, without
`DETACH`, without a fresh connection, without touching staleness. Worth
re-checking on a faster loop before relying on it, but the trap did not bite
here.

## Constraints, measured

| | unpartitioned (`wap_poc.trades`) | partitioned (`raw.fo_trade`) |
|---|---|---|
| `INSERT` | ok | refused, unless `SET ignore_target_file_size_for_partitioned_tables=true` — **then ok** |
| `UPDATE` | ok | refused; **with the override on, `INTERNAL Error: IcebergDelete multi_file_list is NULL`** |
| `DELETE` | ok | ok, override or not |

Three things fall out of that table.

**Partitioned append works**, contrary to the spec's expectation and to
`scripts/duckdb_console.py`'s docstring, which says DuckDB "refuses INSERT and
UPDATE on a partitioned table by default". True by default; there is now an
override, and with it a real `raw.fo_trade` went 400 → 403 rows on a branch
with the `_cob_date` partitioning intact. So this is not limited to an
append-only *unpartitioned* silver layer.

**The override you need for INSERT is what breaks a bare UPDATE.** With it
off, UPDATE is cleanly refused. With it on, UPDATE reaches an internal error
that **invalidates the whole DuckDB database** — every subsequent statement on
that connection returns `FATAL Error: … database has been invalidated`. In an
Airflow task that is the process gone mid-write, with the branch left
half-written. Recoverable, since the branch is throwaway, but the failure is a
crash rather than a rollback.

**`MERGE INTO`, however, works — including on the partitioned table.** That
matters more than the line above, because `MERGE` is what dbt's `merge`
incremental strategy emits and a bare `UPDATE` is not. Measured against the
real Spark-created `raw.fo_trade` on a branch with the override on: the merge
applied, one row changed on the branch, `main` still read zero. So the broken
operation is narrower than "updating a partitioned table" — it is the bare
`UPDATE` statement specifically.

**DELETE needs no opt-in on a partitioned table, and never did.** The
destructive operation is the one with no guard — already noted in
`duckdb_console.py`, and still true.

## The Airflow scenario: concurrent branches

Three branches cut from the same `main` hash, then merged in order:

```
merge probe_x (first in):              ok
merge probe_y (same table as probe_x): 409 Conflict: "The following keys have been
                                       changed in conflict: 'wap_poc.trades'"
merge probe_z (a different table):     ok
```

**Nessie conflicts per table key, not per repository and not per row.** Two
ingest branches appending to the *same* table from the same base always
conflict, however disjoint their rows; two touching different tables never do.
For a per-feed DAG writing a per-feed table that is fine. For anything where
two runs append to one table — a shared reporting mart, a backfill running
beside the nightly — the second merge fails and its work must be re-run on a
rebased branch. That is a real design constraint on this pattern, not a
DuckDB one: the platform's Spark path has it too, and serialises through the
`lakehouse_write` pool for exactly this reason.

## Upstream: all of it is filed, and most of it is already fixed

Searched after the fact, which is the wrong order but the more useful one —
everything above was measured first, so the issues below are corroboration
rather than something taken on trust.

**The seam is a filed bug, fixed a month before this was written.**

> [duckdb/duckdb-iceberg#1145](https://github.com/duckdb/duckdb-iceberg/issues/1145)
> — *REST catalog: `uri` override from /v1/config is ignored; prefix appended
> to ATTACH ENDPOINT breaks Nessie branch-scoped endpoints*. Opened
> 2026-07-05, closed COMPLETED 2026-07-26.

Its body describes what Phase 2 above describes: the doubled path, the 404,
the silently empty catalog, and the same workaround — *"Reaching another
branch currently requires an external path-rewriting proxy."*

> **Fixed by [duckdb/duckdb-iceberg#1230](https://github.com/duckdb/duckdb-iceberg/pull/1230)
> — "Honor REST catalog URI overrides"**, merged 2026-07-26.

The diff is visible in `src/catalog/rest/iceberg_catalog.cpp`. On `main`,
`base_uri` is taken from `overrides["uri"]` and `GetBaseUrl()` uses it; on
`v1.5-variegata`, `uri` is still `attach_options_p.endpoint` and `SetHost(uri)`
uses that. The bug is in source, on the shipping branch.

**Which is exactly why this estate still reproduces it.** The extension here is
`45163a28` (2026-07-09, `core`), 17 days older than the fix. Forcing the
nightly (`FORCE INSTALL iceberg FROM core_nightly`) gives `6561bfca` — byte
identical to the head of `v1.5-variegata`, because the nightly for DuckDB
1.5.x is built from the 1.5 maintenance branch. The branch attach was re-tested
on it without the proxy and was still empty. **None of the relevant fixes are
on that branch:**

| fix | merged to | on `v1.5-variegata`? |
|---|---|---|
| [#1230](https://github.com/duckdb/duckdb-iceberg/pull/1230) honor `uri` overrides | `main`, 2026-07-26 | no |
| [#1150](https://github.com/duckdb/duckdb-iceberg/pull/1150) re-enable `target-file-size-bytes` for partitioned inserts ([#1057](https://github.com/duckdb/duckdb-iceberg/issues/1057)) | `main`, 2026-07-06 | no |
| [#1232](https://github.com/duckdb/duckdb-iceberg/pull/1232) parse the catalog `prefix` once | `main`, 2026-07-26 | no |

So **the proxy and the file-size override are both version artefacts, not
architectural limits.** They are what this release line requires, not what the
approach requires.

**Branches will not become a first-class feature, though.**

> [duckdb/duckdb-iceberg#1252](https://github.com/duckdb/duckdb-iceberg/issues/1252)
> — *[FEATURE] Support for Iceberg branches and tags* (open)

The maintainer is explicit: *"read support should be trivial, but write
support isn't something I'm willing to commit to yet"*, and *"I'm not
implementing nessie-specific endpoints … They require these parameters to be
passed to the REST endpoint in a way that doesn't conform to the spec."* So
expect no `REF`/`BRANCH` attach option. Branch access works only as a side
effect of spec-conformant URI handling — which is what #1230 restores.

Also open, and separate:
[#375](https://github.com/duckdb/duckdb-iceberg/issues/375) (an older Nessie
ATTACH failure about URL-encoding the `|`, which #1145's author explicitly
distinguishes from the doubled-path bug) and
[#784](https://github.com/duckdb/duckdb-iceberg/issues/784) (*Support Create
or replace table* — the third finding in the constraints table above).

## Could the dbt project use this?

Asked separately, measured the same way. The branch problem is no longer the
obstacle; three others are.

**What clears.** dbt-duckdb's `Attachment` accepts `type` plus arbitrary
`options`, so `TYPE ICEBERG / ENDPOINT / AUTHORIZATION_TYPE` all fit in
`profiles.yml`, and the endpoint could carry `{{ var('nessie_ref') }}` exactly
as the Spark profile carries `spark.sql.catalog.lakehouse.ref`. `MERGE INTO`
works on the real partitioned tables, so `incremental_strategy: merge` — the
default on every model here — is not the blocker. `DELETE` works, so
`delete+insert` is a fallback; dbt-duckdb ships both materializations. And
`pip install --dry-run dbt-duckdb` moves exactly one package: no
cosmos-style dependency trap.

**What blocks.**

1. **`partition_by` is silently dropped.** dbt-duckdb handles it only in the
   `external` materialization; `table.sql` and `incremental.sql` never read
   it ([dbt-duckdb#581](https://github.com/duckdb/dbt-duckdb/issues/581) is
   the DuckLake equivalent). Every prepared and reporting table would come out
   unpartitioned, with a green build — and `docs/RETENTION.md` depends on
   `cob_date` leading the spec so expiry is a metadata delete rather than a
   full rewrite. The *engine* is not the problem: DuckDB-Iceberg accepts
   `CREATE TABLE … PARTITIONED BY`. This is an adapter gap.
2. **`dbt run` hangs for minutes with an Iceberg catalog attached** —
   [dbt-duckdb#640](https://github.com/duckdb/dbt-duckdb/issues/640). The
   `information_schema.tables` listing dbt runs on every invocation took 90+
   seconds against a populated catalog for that reporter, whose users
   abandoned the integration over it. Not measured here; the catalog in this
   estate has one table.
3. **Six Spark-only constructs in `dbt/macros/engine.sql`, reaching every
   model**: `sha2`, `date_sub`, `trunc(date,'MM')`, `element_at`,
   `TO_DATE(col, fmt)` and the backtick quoting from `ident()`. All six fail
   on DuckDB; `TRY_CAST`, `CAST(… AS STRING)`, `INTERVAL n DAY` and the window
   functions are fine. Mechanical to fix — but `engine.sql`'s own header
   records that its DuckDB branches were deleted because "an untested promise
   in this repo is a liability rather than optionality", so putting them back
   re-opens that decision and needs a test that actually runs.

Two more, both already filed against dbt-duckdb:
[#754](https://github.com/duckdb/dbt-duckdb/issues/754) — `generate_schema_name`
prefixes `target.schema` onto custom schemas for an attached Iceberg REST
catalog, which is precisely what `dbt/macros/naming.sql` overrides for the
Spark path (fix in [#755](https://github.com/duckdb/dbt-duckdb/pull/755)) —
and the CTAS handling in
[#747](https://github.com/duckdb/dbt-duckdb/pull/747) /
[#725](https://github.com/duckdb/dbt-duckdb/pull/725), the latter written
around the same `CREATE OR REPLACE` gap found above.

## Verdict

The PoC's claim is proven: **DuckDB can do write-audit-publish on a Nessie
branch, and a failing audit leaves `main` untouched.** It is fast — the whole
cycle above runs in a couple of seconds against a cluster that takes ~22s just
to start a SparkSession.

What it costs today:

1. **A proxy on the catalog path.** Not conceptually hard, but it is a
   component in front of Nessie that every writer must go through, and it
   exists only because two projects disagree about where a prefix comes from.
   It will break silently — as an empty catalog — if either side changes.
2. **A crash instead of a refusal** on the one operation combination a
   partitioned table needs.
3. **Experimental on both sides**: Nessie's IRC support is labelled
   experimental, and the DuckDB Iceberg writer is young enough to have an
   internal error one flag away from the happy path.

So: not a replacement for the Spark build path, and nothing here changes
`docs/DECISIONS.md#duckdb-is-not-an-engine`. The interesting near-term use is
narrower and does not need the proxy at all — **auditing a branch someone else
wrote**. `dbt_builds.py` already opens a branch and runs the tests on Spark;
a DuckDB reader attached to that branch could run the cheap assertions in
seconds. That needs the branch to be *readable*, which is the same seam, but
read-only removes constraints 2 and 3 entirely.

**That revisit already has a date on it.** This section originally ended
"worth revisiting when DuckDB's iceberg extension either honours the `uri`
override or exposes a prefix option". It honours it on `main` as of
2026-07-26; it is the 1.5 release line that does not. So the next step is not
to design around the proxy but to re-run `constraints.py` against an extension
built from `main` (or the first release that carries #1230) and see how much of
the list above simply disappears. If both #1230 and #1150 hold, what is left is
dbt-duckdb's, not DuckDB's — and `dbt-duckdb#640` is then the one that would
actually stop you.

## Files

| file | what it is |
|---|---|
| `poc.sql` | the cycle, phase by phase, hand-runnable |
| `run.py` | executes `poc.sql` and performs its `-- @nessie` steps; refuses to merge after a failed gate |
| `constraints.py` | the probes behind every claim above |
| `nessie_ref_proxy.py` | the ~40-line rewrite that makes a branch reachable |

Nothing here is wired into the platform. The spike creates a `wap_poc`
namespace and `spike/duckdb-wap/*.parquet` under the lakehouse bucket, and
removes neither — `cleanup.py` does.

**Version boundary.** Everything measured here is DuckDB 1.5.5 with iceberg
extension `45163a28` (`core`), and `6561bfca` (`core_nightly`, the head of
`v1.5-variegata`) behaves identically. Re-measure before trusting any of it on
a build that carries
[#1230](https://github.com/duckdb/duckdb-iceberg/pull/1230).
