# Delivery contract (Phase 2)

This contract defines the reporting platform's first business interpretation
of accepted Transport evidence. It is additive to the legacy
Landing → Ready → Raw path and does not feed that path yet.

```text
received/<transport-id>/_COMPLETE.json
        │ validate marker, bytes and SHA-256
        ▼
resolve explicit external Feed id
        │ resolve date/version from original evidence
        ▼
deliveries/<source>/<transport-id>/delivery-manifest.json
```

## Transport and Delivery are different facts

A Transport says what DCM transferred: stable TransportID, original object
names and bytes, timestamps, hashes, roles, and the external feed id. A
Delivery says what the reporting platform believes that accepted Transport
represents: one Feed, business date, optional producer version, the Feed
contract used, and producer assertions.

Source evidence remains below `received/`. Delivery creation neither copies
nor renames data/control objects and does not write `landing/` or `ready/`.

## External Feed mapping

Feeds may declare source-owned identifiers:

```yaml
source_identifiers:
  DCM: qa-happy-position
```

`resolve_transport_feed()` looks up exactly the pair
`(Transport.source, Transport.legacy_feed_id)`. It never guesses from a
filename and never reads DCM XML. Duplicate pairs are rejected when the Feed
registry loads; an unknown pair fails Delivery creation. Feeds without a
`source_identifiers` block remain valid and continue to support the legacy
path.

The console does not offer authoring controls for these Phase 2 fields yet,
but it round-trips hand-authored `source_identifiers`, `delivery_identity`,
and delivery-side control identity fields on unrelated edits.

## DeliveryID

DeliveryID v1 is `dlv_` plus the first 32 hexadecimal characters of SHA-256
over a domain separator, Transport source, and TransportID. It is opaque to
business consumers and is independent of producer filename, business date,
file version, Ready keys, and Airflow run ids.

This deterministic identity gives the required occurrence semantics:

- the same accepted `(source, TransportID)` always yields the same DeliveryID;
- a different TransportID is a distinct Delivery, even with identical names
  and bytes; and
- no mutable id allocation registry is required for retry safety.

The source is included so two transport systems may use the same opaque id
without collapsing two occurrences.

## Business identity

`delivery_identity` is a small ordered list, not a rules engine. Its supported
sources are `control` and `filename`; the default is `[filename]` for backward
compatibility. The Phase 2 QA mapping uses:

```yaml
delivery_identity: [control, filename]
```

Control identity uses the existing `ingest/control.py` readers and the
resolved `delivery.control` block. That block now permits `cob_date` and
`version` alongside its existing `row_count` and `md5` declarations. The
declared Transport control object is read directly; it is not conformed or
looked up in Landing. Filename identity calls `Feed.parse_filename()` on each
original data basename.

All configured evidence is inspected. Ordering selects the recorded
provenance when sources agree; it does not hide contradictory evidence.
Delivery creation refuses:

- different dates or versions across data objects;
- different declarations across controls;
- disagreement between control and filename; and
- a missing business date after every configured source is inspected.

Version is recorded when configured evidence supplies it. The current
`Feed.parse_filename()` contract treats an omitted optional filename version
as version 1, preserving existing semantics.

The `identity` object records the selected source, source object, all objects
supporting the selected value, and resolution order.

## DeliveryManifest v1

One JSON document is stored at:

```text
deliveries/<transport-source>/<transport-id>/delivery-manifest.json
```

Its fields are:

```yaml
delivery_manifest_version: 1
delivery_id: dlv_...
feed: qa_happy_position
transport:
  source: DCM
  transport_id: dcm-1234-98765
  external_feed_id: qa-happy-position
  completion_marker: received/dcm-1234-98765/_COMPLETE.json
business_date: 2026-09-17
file_version: 2                 # omitted when unavailable
timestamps:
  source_observed_at: 2026-09-17T05:42:17Z
  received_at: 2026-09-17T05:43:02Z
feed_contract:
  schema_version: abc123
  identity_sources: [control, filename]
  filename_pattern: ...
  control: {...}
identity:
  resolution_order: [control, filename]
  business_date_source: control
  business_date_source_object: received/dcm-1234-98765/positions.ctl
  business_date_evidence_objects: [...]
source_files:
  - role: data
    original_filename: positions_final_FINAL2.zip
    object_key: received/dcm-1234-98765/positions_final_FINAL2.zip
    bytes: 184273921
    sha256: ...
producer_assertions:
  business_date: 2026-09-17
  file_version: 2
  row_count: 4521847
  md5: ...
  producer_run_id: DCM-849217
```

`received_at` is Transport `uploaded_at`: receipt at the durable transport
boundary, not the time somebody later interpreted it. `source_observed_at`
retains DCM's observation time. `feed_contract.schema_version` snapshots the
current `Feed.schema_version`; the other contract fields snapshot the identity
configuration needed to explain the interpretation.

There is no mutable processing status, ingestion verdict, normalization
state, or publication state in this manifest.

## Producer assertions and platform observations

Transport file size/SHA-256 are DCM transfer declarations which the platform
re-measures before creating a Delivery. Control business date, version, row
count, and MD5 are producer assertions parsed with existing configured
semantics. `producer_run_id` is the assertion carried by Transport.

The manifest does not claim that a row count was ingested or that an MD5 was
validated against parsed content. Spark content validation remains outside
Phase 2.

## Create-once and retry behaviour

The manifest write uses the object-store create-only precondition. Before a
write, an existing document is read and verified against the newly validated
immutable Transport. An identical retry returns it without a write. A race
accepts only byte-identical interpretation; a conflicting winner fails.

Historical interpretation wins over today's Feed configuration: if a valid
manifest already exists, retry verifies its Transport binding and returns its
snapshotted Feed/date/schema interpretation without regenerating it. A
manifest whose DeliveryID, transport identity, evidence list, timestamps,
completion marker, or producer run id conflicts with Transport is rejected
and never overwritten.

Application create-only behaviour is not bucket WORM. Object Lock, versioning,
IAM and retention remain deployment controls.

## Registry relationship

Phase 2 does not change `registry.delivery`. That table is currently keyed by
the legacy `(feed, delivery_id)`, projects Ready manifest v1, and is coupled to
Landing-side observations and parts. Extending its schema and reconcile path
without a consumer would be an invasive migration and would risk presenting
new Deliveries as Raw-ingestible when they are not.

The desired later relationship remains:

```text
Transport → DeliveryManifest → rebuildable registry.delivery observation
```

DeliveryManifest creation has no registry dependency. Phase 3 should add an
additive evidence reconciliation projection before any inline registration,
preserve legacy rows and `run_input`, and add no verdict/status column.

Phase 3 implemented that rebuildable projection and Phase 5 now follows its
`manifest_key` from a published run input. The historical design constraint
above remains: `run_input` has no foreign key to the projection, so a missing
or rebuilding observation cannot invalidate run history.

## Relationship to Ready manifest v1

DeliveryManifest v1 is separate from Ready manifest v1. The former records an
accepted occurrence and identity while referencing immutable `received/`
evidence. The latter remains a legacy normalization/consumption plan under
`ready/<feed>/`, derived from conformant Landing filenames.

No compatibility copy bridges them. The existing filename-derived legacy
DeliveryID, `arrival.find_pending`, normalization, archive extraction, Raw
provenance, `delivery_ref()`, and Airflow trigger shape are unchanged.

## Deferred work

Phase 3 now owns DeliveryManifest-to-normalization, archive handling from
`received/`, and an additive registry projection. New DeliveryManifests add a
`feed_contract.normalization` snapshot containing the resolved kind, parser
format, columns/source mapping, and archive member pattern. Existing immutable
manifests are not backfilled. See `NORMALIZATION-CONTRACT.md`.

Phase 6 owns orchestration/discovery: `transport_ingest`'s `create_delivery`
task calls `create_delivery()` unchanged from the description above, and
`transport_reconcile` walks `manifest_key()` as one stage of its
durable-evidence progress check. See `docs/AIRFLOW-ORCHESTRATION.md`.

Later phases still own Raw and dbt provenance migration, supersession/
restatement, content-result persistence, historical backfill, and production
object-store controls.
