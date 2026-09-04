# Delivery shapes

**Status: steps 1-4 built, step 5 partially built** (the sniffer backend;
the console wiring is not). Sections in the future tense describe work that
does not exist yet. When a step lands, its section moves to the present
tense and the reasoning moves to [DECISIONS.md](DECISIONS.md) — step 1 at
[#feed-conventions](DECISIONS.md#feed-conventions), step 2 at
[#ready-is-a-derived-index](DECISIONS.md#ready-is-a-derived-index), step 4 at
[#control-file-gate](DECISIONS.md#control-file-gate), the sniffer at
[#the-sniffer](DECISIONS.md#the-sniffer). Step 3's reasoning has not moved
yet — its section below is still the write-up.

Today a feed is [five files and no DAG edit](ADDING-A-FEED.md). That is cheap
enough — until the delivery is a zip with the date on the container and no date
on the CSVs inside it, or a file that must not be read until its control file
lands. This is about making those cost the same five files.

The short version: **`landing/` is doing two jobs, and splitting them is what
makes every awkward delivery shape cheap.**

---

## What already works, so nobody builds it twice

**Pipe-delimited, tab-delimited, unusual quoting, non-UTF-8 encodings are one
line each.** `delimiter`, `quote_char`, `header` and `file_encoding` are
per-feed keys with global defaults in `feeds.yml`, they reach Spark's reader
unchanged (`read_landing`, `ingest_feed.py:105`), and the console exposes all
four on the form with `unescape_char` so `\t` can be typed literally
(`ui/registry.py:92`). A pipe feed is `delimiter: "|"` and nothing else.

**`.csv.gz` needs no unpacking.** Hadoop's input formats decompress gzip and
bzip2 transparently, so a single gzipped CSV already reads through the same
path. **A zip is not a compression codec, it is an archive**, and Spark has no
reader for one. That distinction is why archives need a new stage and `.gz`
does not.

**Re-delivery versioning needs nothing.** `_file_version` is computed from the
raw table (`next_file_version`, `ingest_feed.py:281`), not from the filename —
the `(?:_v(?P<version>\d+))?` group is parsed and then **discarded** by
`ingest()`. Its only job is to make the pattern match a corrected file at all.
An archive re-delivered under its original name versions correctly with no new
mechanism.

## The assumption everything else breaks against

**One landed object is one file, is one delivery, and its NAME carries the
business date.**

`Feed.parse_filename` (`common/context.py:142`) does `re.fullmatch` on a
filename and returns `(business_date, version)`. It has **14 call sites across
7 modules**, and they are not all the ones you would guess:

| Module | What it decides |
|---|---|
| `ingest/arrival.py` (3) | what is pending, and which dates landing believes exist |
| `ingest/ingest_feed.py` (1) | the business date of the write |
| `ingest/inbox.py` (1) | which feed claims a dropped file |
| `retention/landing.py` (2) | **what may be deleted from the evidence prefix** |
| `ui/feeddata.py` (4) | seed listing, landed listing, delivery, upload validation |
| `ui/sampledata.py` (3) | that a generated filename is one the platform can route |
| `scripts/land_feeds.py` (1) | which seed files to upload |

A zip breaks that three ways at once: the date is on the container and not the
member; one object yields *many* units of work; and there is no URI Spark can
read. A control file breaks it differently — the object that arrives is not the
object to ingest, and "ready" stops meaning "stopped growing"
([DECISIONS.md#inbox-is-polled](DECISIONS.md#inbox-is-polled)).

More regex does not fix this. One concept has to become three: **what arrived**,
**when it is ready**, and **what business date it is for**.

---

## The shape: three prefixes, three jobs, three lifetimes

`landing/` currently serves as both the immutable evidence copy **and** the work
queue. Those have opposite requirements, and the tension is already visible in
the code: `sweep_landing` will not delete an object whose name it cannot parse
(`retention/landing.py:94`) because it is evidence, which means anything the
platform does not recognise accumulates in the work queue forever.

Split them.

| Prefix | Job | Lifetime | Deletion rule |
|---|---|---|---|
| `landing/<feed>/` | **evidence** — exactly what the upstream sent, byte for byte | `keep_years: 8` | never on a guess; unparseable means keep |
| `ready/<feed>/` | **work queue** — a manifest per delivery, plus any derived parts | days | freely, once ingested; rebuildable from landing |

`landing/` keeps its current semantics and its current retention sweep
untouched. `ready/` is new, is a **cache**, and everything in it can be
reconstructed by re-running normalization against `landing/`. That property is
what makes it safe to delete from, and it is the property to protect: the
moment something in `ready/` is not reconstructable, it has quietly become a
third copy of the data.

A **normalize** stage sits between them, and `ready_prefix` joins
`landing_prefix` in `feeds.yml` defaults (`config/feeds.yml:11`).

## The manifest

Normalization writes one JSON manifest per delivery into `ready/<feed>/`:

```json
{ "feed": "treasury_margin_call",
  "business_date": "2026-08-01",
  "delivery_id": "20260801T063112-a1b2c3",
  "received_at": "2026-08-01T06:31:12Z",
  "source_object": "landing/treasury_margin_call/marginCalls_20260801.zip",
  "parts": [{"object_key": "ready/treasury_margin_call/.../part1.csv",
             "bytes": 41203, "member": "part1.csv"}],
  "format": {"delimiter": "|", "quote_char": "\"", "header": true,
             "encoding": "utf-8"},
  "declared_row_count": 4211,
  "normalizer": "archive/v1" }
```

Three decisions in that object carry most of the design.

**For a plain CSV, normalize writes a manifest and does NOT copy the bytes.**
`parts[].object_key` points straight back into `landing/`. So the common case
costs one small JSON object rather than a second copy of every delivery, and
there is still exactly one code path downstream: `ingest()` reads
`manifest.parts` and neither knows nor cares whether that points into
`landing/` or `ready/`. A normalizer copies bytes only when it actually
transforms them.

**`format` is recorded, and ingest reads it from the manifest rather than live
from `feeds.yml`.** That makes an ingest reproducible — you can say what
delimiter was actually used for a delivery six months ago, which is the same
question `landing/` exists to answer about the bytes. Correcting a wrong
delimiter becomes "fix `feeds.yml`, re-normalize", which is cheap precisely
because `ready/` is a cache.

**Ingestion status is NOT in the manifest, and must never be.**
`already_ingested` derives its ledger from `_source_file` in the raw table
specifically so that it *cannot* drift from reality; its docstring
(`arrival.py:63`) names the legacy `stg` load-control tables as the failure
being avoided. A manifest carrying `"ingested": true` is that table, rebuilt
under a new name.

The line to hold: **the manifest records observations about an event that
happened** — what arrived, what the control file declared, what encoding was
detected — facts not recomputable once the container is gone. It never records
**derived state**, which stays derived.

---

## 1. `conventions:` — the lever that reduces work — **BUILT**

`feeds.yml` had exactly two tiers: global `defaults:` and per-feed. The
variation being described is neither — it is **per source system**. Treasury
sends zips with a control file; the reference system sends pipe files. That
knowledge had nowhere to live, so it got retyped into every feed block and
drifted between them.

```yaml
conventions:
  treasury_zip:
    delimiter: "|"
    schema_drift: fail
    delivery:
      kind: archive
      business_date_from: container

feeds:
  - name: treasury_margin_call
    convention: treasury_zip
    business_key: [margin_call_id]
    columns: [...]
```

Merge order is `defaults -> convention -> feed`, resolved in
`context.effective_defaults()` — the **only** implementation of that ordering,
because the feed console needs the same answer to decide which keys to leave
out of a block it writes. Three things are errors at load rather than silent
fallbacks: a feed naming an undefined convention, an unknown key inside a
convention, and a convention setting `name` or `convention`. The reasoning for
each is in [DECISIONS.md#feed-conventions](DECISIONS.md#feed-conventions).

The three `REF_SRC` feeds use it today, which is the point of converting them
rather than shipping an empty section: this file's history is full of settings
that were read by no code for months.

The payoff is not YAML brevity. It is that **the awkward source system is
onboarded once, with real thought and a real test**, and feeds 2..40 from it
are six lines that cannot get the awkward part wrong.

## 2. `ready/`, the manifest, and a pass-through normalizer — **BUILT**

The whole stage, before a single interesting normalizer. The reasoning now
lives at
[DECISIONS.md#ready-is-a-derived-index](DECISIONS.md#ready-is-a-derived-index);
what follows is what was built.

A **normalize** task sits between `resolve_arrival` and `ingest` in
`airflow/dags/feed_ingest.py`. It is plain Python — `zipfile`, boto3, `json`,
no Spark — so it is an ordinary Airflow task; if it ever grows a Spark call it
goes through `scripts/_spark_task.py` like everything else
([DECISIONS.md#spark-in-a-subprocess](DECISIONS.md#spark-in-a-subprocess)).

With `kind: file` — the default, and what every existing feed gets — the
normalizer resolves the business date from the filename exactly as
`parse_filename` did, writes a manifest whose single part references the
landing object, and copies nothing. **Observable behaviour is unchanged**, and
that was the entire success criterion.

Verified on the live stack: a 120-row delivery landed, `pending` returned
`ready/fo_trade/TRADE_20260903.csv.json`, ingesting that manifest wrote 120
rows with `_source_file = landing/fo_trade/TRADE_20260903.csv`, and the next
`pending` came back empty. A full `ingest_fo_trade` DAG run went green with
`normalize` taking 0.12s. Re-delivery still versions from the table
(`_file_version: 2`), and `--object landing/...` still works.

Downstream of the manifest boundary, nothing changes: same reader, same branch
per delivery, same `_source_file` ledger, same merge. `ingest()` already
accepts an explicit `business_date` that takes precedence over the parsed one
(`ingest_feed.py:248`), so the manifest date flows in through a parameter that
exists today.

### What this does and does not do to `parse_filename`

It does not delete it, and the count is worth stating honestly: about **4 of
the 14** call sites go away — the ingest hot path and two in `find_pending`.
`inbox` still routes by pattern at arrival (`inbox.py:75`), landing retention
still needs it to decide what is ours (`retention/landing.py:94`), and the
console, sample-data and seed-landing uses are about local files *before*
landing and legitimately keep it.

**The win is not the call-site count.** It is that the business date is derived
**once**, at normalize, and read thereafter — instead of seven modules
independently re-running the same regex with the standing ability to disagree.

### Does normalize want its own image?

Not for this step, and the reason the current one-image rule gives is worth
correcting while it is being relied on.
[DECISIONS.md#feed-ui-same-image](DECISIONS.md#feed-ui-same-image) says a
slimmer image "would have to duplicate the platform package and could then be
built against a different version of it". Locally that is not what happens:
`feed-ui`, `inbox` and `watchdog` all **bind-mount** `./reporting_platform`,
so the source is already shared and cannot skew. What a second image would
actually duplicate is the **dependency pinning**. The argument holds in the
cluster, where the package is baked in, and not on the laptop.

Normalize needs `boto3`, `pyyaml` and stdlib `zipfile` — all already
installed — so splitting now costs a build and buys nothing.

The trigger to split is **step 3 and 4**, where format handling wants
`openpyxl`, `chardet` and friends. Those have no business sitting in an image
that also carries Spark, dbt and Cosmos, and the OpenShift shape makes the
cost concrete: normalize becomes a KubernetesPodOperator, one pod per
delivery, pulling 2.7GB to unzip a file. When that happens, put the pins in
one shared requirements file so the two images cannot drift — the manifest is
a contract between normalize and ingest, and skew across it is precisely the
silent failure this whole design is trying to remove.

### `ready/` retention

A new `ready:` block in `retention.yml`, in days. It has none of the coupling
the `landing:` block carries — that one must be `>=` the raw window because
`find_pending` computes its keep-set from landing, and the config comment says
so. `ready/` is rebuildable, so the only floor is operational: it must comfortably
exceed `arrival_timeout_hours` (26h, deliberately longer than a day), or a
delivery can be swept between normalization and a late ingest. Seven days.

One rule, and it uses the derived ledger rather than a status flag: **never
sweep a manifest whose parts are not in `already_ingested`.** A manifest swept
before ingest is not data loss — landing still has the container — but nothing
would re-normalize it automatically, so it is a silent drop, which is worse
than a loud one.

## 3. The archive normalizer — **BUILT**, for `concat`/`container` only

The reasoning now lives at
[DECISIONS.md#archive-normalizer](DECISIONS.md#archive-normalizer); what
follows is what was built.

```yaml
delivery:
  kind: archive               # file | archive
  member_pattern: '.*\.csv'   # which members belong to this feed
  business_date_from: container   # container | member | path -- only container is built
  parts: concat               # concat | separate -- only concat is built
```

The normalizer reads the container from `landing/`, explodes matching members
into `ready/<feed>/<stem>/`, and writes one manifest whose `parts` list them.
This is the first normalizer that actually copies bytes; the derived copies
live in `ready/`, where short retention and rebuildability apply, never under
`landing/`. `business_date_from: member`/`path` and `parts: separate` are
recognised keys that raise a "NOT BUILT" error naming the gap rather than
being silently accepted (`context.NOT_BUILT`).

**Not yet verified on the live stack.** `tests/test_archive.py` exercises real
zip bytes through Python's `zipfile`, but against `tests/fakes3.py`; no feed
in `feeds.yml` uses `kind: archive` yet, so this has not been driven through
MinIO, Spark and a real `ingest` end to end. That is the next thing to do
before the BUILT marker above is fully earned.

## 4. Control files — **BUILT**

The reasoning now lives at
[DECISIONS.md#control-file-gate](DECISIONS.md#control-file-gate); what
follows is what was built.

```yaml
delivery:
  control:
    pattern: '{stem}\.ctl'          # a REGEX template, {stem} substituted in
    row_count: 'ROWS=(?P<rows>\d+)' # optional; a pure gate needs no row_count
```

Only on top of `kind: file`; combining `control:` with `kind: archive` is
rejected at load as NOT BUILT, alongside archive's own unbuilt corners.
`normalize()` will not emit a manifest until a sibling in the same landing
folder matches `pattern`, and reads `declared_row_count` out of it where
`row_count` is set. A missing control file raises `NotReady`, a new
exception distinct from every other normalization failure, and `reconcile()`
counts it separately in `awaiting_control` rather than `failed`.

The declared row count is an equality check next to `expected_min_rows`
(`ingest_feed.py:404`), not a replacement for it — the floor still catches a
truncated file on a feed with no control file at all.

**A late control file does not fail the run.** `feed_ingest.py`'s
`normalize_task` catches `NotReady` and skips rather than propagating it into
`DEFAULT_ARGS`' two retries at `RETRY_DELAY`, which would otherwise turn "not
here yet" into a hard failure in well under a minute. The safety-net poll
path already built for every other feed — `find_pending`, reached from
`resolve_arrival`'s no-conf fallback, or `scripts.bulk_ingest` — is what
picks the delivery up once the control file actually lands, however long
that takes. `arrival_timeout_hours` is **not** what does this: it is a
config field nothing reads, and a first pass at this section claimed
otherwise before that was checked against the code.

`inbox.route()` needed a second check for this to work locally at all: a
control file matches no feed's `filename_pattern` and would otherwise be
rejected to `.rejected/` and never reach `landing/`, permanently starving
the delivery it belongs to.

Not yet verified on the live stack — same caveat as step 3, and for the same
reason: no feed in `feeds.yml` uses `delivery.control` yet.

## 5. Onboard from a real file — **PARTIALLY BUILT**: the sniffer backend

The reasoning now lives at
[DECISIONS.md#the-sniffer](DECISIONS.md#the-sniffer); what follows is what
was built and what was not.

The console already derives the filename pattern from one example
(`derive_pattern`, `ui/registry.py:278`) and columns from an uploaded CSV
(`columns_from_csv`, `ui/feeddata.py:136`). `reporting_platform/ingest/sniff.py`
is a **sniffer**: propose mode, not a normalizer that writes anything. Given a
real delivered file it proposes delimiter, quote, header, encoding and
per-column types by calling DuckDB's own `sniff_csv()` -- a real, tested CSV
sniffer already a dependency here via `scripts/duckdb_console.py` and
`notebooks/explore.py` -- rather than hand-rolled frequency analysis, plus a
uniqueness scan on top for business-key candidates. Headers become
identifiers through the existing `platform_names`.

**Infer types from values, not from column names.** `infer_type`
(`ui/scaffold.py:59`) guesses from the name, and the repo already documents
what that produces: a column typed `decimal` whose generated sample data is a
string, `safe_cast` nulls the column, the build goes green, 75 rows and 0
non-null
([DECISIONS.md#resolve-types-is-authoritative](DECISIONS.md#resolve-types-is-authoritative)).
With a real file in hand the guess can simply be right.

Verified against real data on the live stack: pointed at a landed
`fo_trade` delivery in MinIO through `s3://lakehouse/...`, using the same
DuckDB connection `scripts/duckdb_console.connect()` builds
(`REPORTING_DUCKDB_S3_SECRET`, now also set for `feed-ui`, not only
`notebook`) -- correct delimiter, correct per-column types read from real
values, not names.

**Not built**: archive-layout sniffing (a zip's own member list, unlike a
plain CSV's header, needs its own proposal shape); date-source detection
across container/member/path; and the console side entirely -- no button, no
form pre-fill, no unclaimed-deliveries queue. `inbox` still moves an
unclaimed file to `.rejected/` (`ingest/inbox.py`) and landing's sweep still
only counts unrecognised objects rather than surfacing them as "landed but
not normalized" for one queryable backlog. That is the moment a new feed
actually appears in real life -- not as a ticket, as a file nobody expected
-- and it is still a ticket today.

---

## Order, and what each step is worth

| # | Step | Why here |
|---|---|---|
| 1 | `conventions:` tier | **Built.** No new runtime concept. Makes 2-5 cheap to express. Useful even if nothing else is built. |
| 2 | `ready/` + manifest + normalize stage, pass-through only | **Built.** The architecture. Behaviour-preserving, verified against the live stack before anything new depends on it. |
| 3 | Archive normalizer | **Built**, not yet live-verified. The zip case. First normalizer that copies bytes. |
| 4 | Control-file normalizer | **Built**, not yet live-verified. Readiness in one place, and the exact-count assertion. |
| 5 | Sniffer + unclaimed queue | **Sniffer backend built**, live-verified; console wiring and the unclaimed queue not built. A normalizer in propose mode. Turns onboarding from a form into a reviewable diff. |

Steps 3-5 are each *one normalizer* because step 2 built the stage. That is the
whole reason step 2 exists as its own change rather than arriving underneath
the zip work.

## Three things that will bite

**`find_pending` has to straddle both prefixes.** Candidates come from `ready/`
manifests, but the retention keep-set must still be computed from the dates
observed in **`landing/`** (`retention_keep_dates`, `arrival.py:85`) — landing
is the only place holding every date after raw has expired them, which is
exactly what the `landing:` block in `retention.yml` warns about in its own
comment. Compute the keep-set from `ready/` and it silently narrows to the
cache window, and live business dates start looking expired. The tempting fix —
give manifests an eight-year lifetime so the keep-set can come from them — is
the control-table-drift trap again, wearing a different hat.

**The sample-data generator has to grow an archive mode.**
`ui/sampledata.py` builds a filename and then checks it against
`parse_filename` (`sampledata.py:109`), which is the guard that catches a
pattern nobody can route. An archive feed with no generator has nothing to run
against locally, and per [ADDING-A-FEED.md](ADDING-A-FEED.md) that is the step
whose omission leaves a feed that looks complete and has never executed.

**`kind: file` must stay the untouched default through every step.** Four feeds
in `feeds.yml`, every seed, every DAG and every test depend on today's
behaviour. The value of step 2 is entirely that it changes nothing observable.
