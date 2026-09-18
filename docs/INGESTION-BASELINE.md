# Ingestion baseline (Phase 0)

This document records the current, working ingestion path that a future
redesign must preserve or deliberately replace. It describes the repository
implementation and tests, not the intended future architecture. In
particular, this phase does not add DCM integration, a new acquisition
service, a transport contract, a new delivery identifier, or new manifest
types.

Code and tests are the primary evidence. `docs/ADDING-A-FEED.md`,
`docs/DECISIONS.md`, and `docs/DELIVERY-SHAPES.md` remain useful explanations,
but claims below were checked against the paths named here.

## Current architecture

There are two supported ways into Landing:

```text
producer/DFS                         approved object writer
    |                                       |
    v                                       |
local inbox (optional conformance gate)     |
    |                                       |
    +-------------------+-------------------+
                        v
                 S3/MinIO landing/
                        |
             Airflow resolve + normalize
                        v
             ready/ manifest (+ archive parts)
                        |
                  Spark raw ingest
                        v
               Iceberg raw.<feed>
                        |
             Airflow asset scheduling
                        v
              dbt prepared -> tests
                        v
              dbt reporting -> tests
```

The local inbox is the current optional conformance gate; it is not a model
for a new DFS/SFTP poller. The future DCM follow-on upload belongs outside
this Phase 0 baseline.

Phase 1 adds the DCM boundary under the distinct `received/` prefix. Phase 2
now interprets a validated Transport as an immutable DeliveryManifest under
`deliveries/`, resolving Feed and business identity without renaming or
copying source bytes. This additive boundary still does not enter Landing,
Ready, raw, or Airflow. See `docs/TRANSPORT-CONTRACT.md` and
`docs/DELIVERY-CONTRACT.md`.

### Implementation flow

| Stage | Input and output | Responsible code | Authority, retry, and evidence |
| --- | --- | --- | --- |
| Feed definition | Per-feed YAML in `reporting_platform/config/feeds/`, resolved with `_defaults.yml` and `conventions/` into a `Feed` | `reporting_platform/common/context.py`: `feeds()`, `Feed`, `Feed.parse_filename`, `Feed.schema_version` | Resolved `Feed` configuration is the runtime contract. Configuration loading is mtime-cached. Configuration tests and `tests/test_feed_config.py` protect it. |
| Inbox stability and routing (optional) | A file in the bind-mounted `/opt/platform/inbox`; after two unchanged size/mtime observations it is processed, rejected, or left waiting | `reporting_platform/ingest/inbox.py`: `route`, `sweep`, `_promote`; `reporting_platform/ingest/conform.py`: `plan_arrival`, `conform`, `conform_member` | The watcher is a `while` loop with a sleep. A control-gated file remains in the inbox until its sibling exists. The inbox copy moves only after object writes succeed. `tests/test_inbox.py`, `tests/test_conform.py`, and `tests/test_control.py` cover this path. |
| Conformance | Legacy producer bytes and names become a filename matching `Feed.filename_pattern`; data and control bytes are uploaded to Landing and the original names/observations go in `<delivery>.meta.json` | `conform._free_name`, `conform.metadata_bytes`, `inbox._promote`, `arrival.put_landing_bytes` | Data bytes are not rewritten. Name collisions with different content allocate `_vN`; unchanged retransmissions are recognised by MD5. The sidecar is provenance, not the data object. |
| Landing | A conformant object at `landing/<feed>/<filename>` plus an optional control object and optional `.meta.json` | Direct writers use `arrival.put_landing*`; the gate uses `inbox._promote` | Landing objects are the retained delivery evidence. “Immutable” is a producer/platform contract: S3 Object Lock or bucket versioning is not enforced here, and a same-key `PutObject` can overwrite bytes. Tests protect conformance behaviour, not infrastructure-level immutability. |
| Arrival resolution | A DAG parameter containing a Landing or manifest key, or the next result from `arrival.find_pending` | `airflow/dags/feed_ingest.py`: `resolve_arrival`; `reporting_platform/ingest/arrival.py`: `find_pending` | Before selecting pending work, the fallback poll reconciles `ready/`. It then removes deliveries whose manifest part keys already appear as raw `_source_file`. `tests/test_find_pending.py` covers this. |
| Normalization | A conformant Landing object; output is manifest v1 in `ready/<feed>/<delivery_id>.json` | `reporting_platform/ingest/normalize.py`: `_gate`, `_normalize_file`, `_normalize_archive`, `normalize`, `reconcile` | Plain files are not copied: their part points back to Landing. Archives stay in Landing while safe, matching members are extracted below `ready/`. The control gate runs before extraction. A repeat with unchanged Landing state and config produces identical JSON and stable part keys. `tests/test_normalize.py`, `tests/test_control*.py`, and `tests/test_archive.py` cover these properties. |
| Delivery registry | A normalized manifest plus Landing listing/sidecar observations; output is `registry.delivery` and `registry.delivery_part` | `reporting_platform/registry/deliveries.py`: `observations`, `register`, `reconcile`, `coverage` | Inline registration is best-effort. Reconciliation is the correctness path and uses the same observation projection. `(feed, delivery_id)` is the primary key, so reconciliation does not duplicate an observation. `tests/test_registry.py` covers the boundary. |
| Raw ingestion | Ordered manifest parts plus captured format instructions; output is an Iceberg raw table | `reporting_platform/ingest/ingest_feed.py`: `resolve_delivery`, `read_landing`, `reconcile_schema`, `ingest`; invoked by `feed_ingest.py:ingest_task` | Spark reads each physical part, adds raw provenance, writes on a Nessie branch, validates count/checksum/minimum rows, then merges. The DAG's normal pending path suppresses already-seen part keys. A direct explicit replay is not independently guarded; see Known limitations. Tests cover parsing/schema/provenance, while `scripts/verify_happy_path.py` checks the live stack. |
| Prepared build | A raw Airflow Asset update; output is typed/conformed Iceberg prepared models | `airflow/dags/dbt_builds.py`; `dbt/models/prepared/`; macros in `dbt/macros/engine.sql` | Cosmos runs the selected dbt graph with tests after all models. Work happens on a Nessie branch and merges only after the build and tests pass. `tests/test_happy_path_scd2.py` executes representative model SQL in DuckDB. |
| Reporting build | Prepared Asset update; output is reporting models and published registry run/version information | `airflow/dags/dbt_builds.py`; `dbt/models/reporting/`; dbt schema tests | The same branch/test/merge pattern applies. The QA smoke path ends at `reporting.qa_happy_position_summary`; `scripts/verify_happy_path.py` asserts exact live output and types. |

The generated per-feed ingest DAG is
`resolve_arrival -> normalize -> ingest -> report_drift -> record_snapshot`.
Prepared and reporting builds are separate asset-scheduled DAGs, rather than
extra tasks inside the ingest DAG.

## Current identifiers

| Identifier | Current semantics |
| --- | --- |
| `Feed.name` | Stable configuration key used in Landing/Ready prefixes, raw table names, generated ingest DAG IDs, dbt sources/models, and registry keys. |
| `delivery_id` | The basename of the conformant Landing source object, including its extension and any `_vN` correction suffix. `_normalize_file` and `_normalize_archive` derive it from `source_object`; the manual explicit-date fallback does the same. For an archive delivery it identifies the retained container, not a member. It is not a transport-independent identifier. |
| `source_object` | Full object key for the retained source delivery in Landing. For a plain file it is also the only manifest part; for an archive it is the container while parts point to extracted Ready objects. |
| `_source_file` | Raw row provenance for the physical object Spark read. It is a Landing key for a plain file and a Ready member key for an archive. `arrival.already_ingested` currently derives ingestion state from distinct values of this column. |
| `_delivery_id` | Raw row provenance copied from manifest `delivery_id`. It was added without backfilling historic rows. dbt's `delivery_ref()` therefore falls back to the basename of `_source_file` for old data; that fallback is exact for current catalog file feeds but not for historic archive rows. |
| `schema_version` / `_schema_version` | A 12-character SHA-1 digest of the ordered platform/source column mapping in the resolved `Feed`. It identifies the declared raw column contract used to read a delivery, not an inferred file schema and not prepared-layer types. |
| `registry.run.run_id` | Identifier for a prepared or reporting build/publication run. It is not the raw ingest `run_id`/batch identifier. |
| `registry.run_input` | The distinct `(feed, delivery_id)` values present in the models a successful build published. It has deliberately no foreign key to `registry.delivery`, so rebuilding delivery observations cannot cascade away run history. It records published data provenance, not every delivery scanned. |

COB/business date and the optional producer version are parsed by
`Feed.parse_filename` from the conformant Landing filename. The control file
may declare a business date/version, but conformance first turns that into a
filename satisfying `Feed.filename_pattern`; downstream normalization parses
the Landing name.

## Current storage responsibilities

### `inbox/`

A local, bind-mounted handoff and conformance workspace. It waits for stable
files and required controls, preserves failed originals under `.rejected/`,
and moves accepted inputs under `.processed/`. It is not object-storage
evidence and is not required for approved direct-to-Landing writers.

There are two archive concepts. `delivery.kind: archive` retains one archive
as the Landing delivery and extracts its parts during normalization.
`arrival.archive` is an inbox shape: the gate unpacks a producer container and
promotes each matching member as its own delivery; that container does not
become the Landing delivery, although its original name is recorded in
sidecar provenance.

### `landing/`

Long-lived evidence bytes. A direct writer must already use a conformant
filename. The inbox gate may rename before upload while preserving byte
content. Its `.meta.json` sibling records `source_filename`, optional
`source_container`, observations such as MD5/size, and gate metadata. Thus the
producer filename is preserved only when the gate created a sidecar; for a
direct writer, the Landing basename is the earliest known name.

### `ready/`

A derived normalization cache. It contains one manifest per accepted
delivery and, only for materialising normalizers such as archives, extracted
parts. Plain data remains in Landing. Retention may delete ingested extracted
parts while keeping the manifest, so “rebuildable” and “always fully
materialized” are not synonyms.

Rebuilding a deleted Ready entry requires:

- the Landing source object and its LastModified value;
- the current resolved feed configuration and filename pattern;
- the Landing control sibling when the feed is control-gated; and
- archive bytes for materialising archive members.

Those inputs regenerate the same manifest and stable member keys. The sidecar
is not needed to construct the manifest, but it is needed to reconstruct the
original producer-name provenance in the registry.

### Raw Iceberg

The raw table stores source business values as strings plus `_cob_date`,
`_source_file`, `_file_version`, `_ingest_ts`, `_row_number`, `_batch_id`,
`_delivery_id`, `_received_at`, `_schema_version`, and `_source_system`. The physical part key
in `_source_file` is currently both provenance and the normal-path ingestion
ledger.

### Postgres registry

`registry.delivery` and `registry.delivery_part` index accepted observations
from Landing/Ready. They do not contain an `ingested`, `status`, `processed`,
or verdict field. They can be reconstructed by normalization plus delivery
reconciliation using object-storage evidence. Rebuild preserves delivery
ordering by `received_at`, but database-generated `sequence_no` values are
not stable identities.

The whole registry is not rebuildable from object storage. Build runs,
published report versions, submissions, and lifecycle transitions record
acts that are not encoded in Landing. The rebuildable claim applies to the
delivery observation tables.

## Manifest v1

`normalize.normalize` obtains `received_at` and byte sizes from object
metadata, parses identity/date/version from the Landing name, reads any
control declarations, and captures the feed format. It sorts archive members
and uses stable keys. No UUID or normalization clock is included, so the same
Landing state and resolved config serialize byte-identically with sorted JSON
keys.

The current manifest intentionally mixes two concerns:

- Source observations: `feed`, `delivery_id`, `cob_date`, `received_at`,
  `source_object`, control object and declared values, part member names and
  sizes, and normalizer identity.
- Consumption instructions: ordered `parts`, parser `format`,
  `checksum_objects`, and the normalizer/version semantics used to materialize
  them.

It is therefore rebuildable, but it is neither a pure receipt nor a separate
normalization plan.

## Confirmed invariants

These behaviours have implementation and regression evidence and should not
change accidentally during later refactoring:

1. Landing retains the accepted source bytes. Plain normalization does not
   make a second data copy; archive normalization retains the source archive.
2. A required control file gates normalization. No Ready parts or manifest
   are produced while it is missing; declared row count/checksum metadata is
   captured after it arrives.
3. Unsafe archive member paths are rejected and member order/Ready keys are
   deterministic.
4. Ready manifests and archive parts can be removed and rebuilt from unchanged
   required inputs; the new deletion/reconciliation tests assert byte-for-byte
   reconstruction.
5. Delivery registration records observations, uses `(feed, delivery_id)` as
   its natural key, and reconciliation does not create duplicate rows or
   mutable ingestion verdicts.
6. Normal pending detection derives completed physical inputs from raw
   `_source_file`; it does not consult an `ingested=true` registry flag.
7. Raw rows retain both physical provenance (`_source_file`) and current
   logical provenance (`_delivery_id`) with COB date, file version, schema
   contract, source system, arrival time, and batch metadata.
8. Prepared models carry delivery/source/schema provenance forward, and dbt
   model tests gate the Nessie merge before reporting is published.

## Representative regression paths

| Shape | Representative evidence |
| --- | --- |
| Plain CSV | `fo_trade` in `tests/test_normalize.py`: Landing is the manifest part, no duplicate data object, deterministic and now deletion/rebuild-tested. |
| Control-gated file | `qa_happy_position` in `tests/test_control.py`, `tests/test_control_format.py`, and the live `scripts/verify_happy_path.py`: wait, discover control, capture declarations, then reach raw/prepared/reporting. |
| Archive | Synthetic `cus_position` real ZIP bytes in `tests/test_archive.py`: retained container, matching extracted members, stable manifest/parts, path rejection, and full Ready deletion/rebuild. No production catalog feed currently declares `delivery.kind: archive`. |
| Raw provenance | `tests/test_provenance.py`, `tests/test_raw_schema.py`, and the live verifier assert `_source_file`, `_delivery_id`, COB date, file version, schema version, and source system. |
| Registry retry | `tests/test_registry.py` asserts one observation per `(feed, delivery_id)`, common projection, reconciliation, and absence of verdict state. |
| dbt | `tests/test_happy_path_scd2.py`, dbt schema tests, DAG import checks, and the live QA summary cover the prepared/reporting path. |

## Known limitations and redesign pressure points

These are confirmed constraints or gaps, not policies introduced by this
phase.

- Delivery identity is equivalent to the conformant Landing basename in
  `normalize._normalize_file`, `normalize._normalize_archive`, registry keys,
  raw `_delivery_id`, and dbt provenance. It is not independent of storage
  naming.
- Direct Landing filenames must satisfy `Feed.filename_pattern`.
  `arrival.matching`, `normalize`, and `Feed.parse_filename` enforce that
  relationship. The inbox gate handles legacy names by renaming them first.
- COB date and file version are derived from the Landing filename. An explicit
  manual COB override exists in `ingest_feed.resolve_delivery`, but it is an
  operator escape hatch rather than normal delivery metadata.
- Producer filenames survive only in gate-written `.meta.json` sidecars and
  registry `source_filename`/`source_container`. Direct writers have no
  separate original-name field.
- Manifest v1 combines receipt observations with ingestion instructions.
- Normal pending/already-ingested detection compares every manifest part with
  raw `_source_file`, not a logical delivery identifier. A stable extracted
  member key is therefore required to avoid archive re-ingestion.
- The ordinary reconciliation/pending path is retry-safe. Calling
  `ingest(feed, explicit_key)` directly bypasses pending detection and has no
  same-delivery guard; it can allocate another `_file_version` and duplicate
  raw input. Current semantics are therefore undefined for forced replay, and
  this phase deliberately adds no test that invents a new policy.
- Landing immutability is not enforced by storage configuration. The code
  trusts approved direct writers not to replace a key; only the inbox gate's
  MD5/version logic protects its own path.
- Ready reconstruction depends on current feed configuration. A historical
  snapshot of the configuration is not stored independently; the manifest
  captures parsing format after normalization but cannot rebuild itself once
  it has been deleted.
- `registry.delivery` observations are rebuildable; exact `sequence_no`
  values and the run/version/submission/lifecycle tables are not.
- Polling, waiting, retry, and sequencing are partly custom: the inbox watcher
  performs stable-file polling and sleeps; `scripts/bulk_ingest.py` chunks and
  launches subprocesses; the UI triggers one DAG run per key; and
  `reporting_platform/ui/jobs.py` maintains local background-process status,
  timeout, cancellation, and log streaming. No replacement is made here.

## Candidate future changes

Later phases can use these seams without treating them as Phase 0 work:

| Confirmed pressure point | Likely later concern |
| --- | --- |
| Filename is delivery identity; COB/version are parsed from it | Introduce transport-independent receipt/delivery identity and explicit observed metadata after the DCM-to-S3 boundary is defined. |
| Original producer name is sidecar-only on gated arrivals | Define which uploader observations are authoritative and how they reach a receipt contract. |
| Manifest combines observation and consumption | Separate a delivery receipt from normalization/ingestion instructions, with migration compatibility for v1. |
| `_source_file` is the pending ledger | Move retry/deduplication to a logical delivery boundary while retaining physical source provenance. |
| Ready rebuild uses current config | Decide how normalization versions/config snapshots are retained and reproduced. |
| Custom polling and process status | Map coordination to Airflow-native sensors, datasets/assets, retries, and task state where appropriate. DCM remains the DFS monitor; do not create another poller. |
| Landing immutability is contractual | Define uploader write/idempotency and storage-retention controls at the transport phase. |

Any such change must keep the representative paths above green or explicitly
replace their contracts with versioned migrations.
