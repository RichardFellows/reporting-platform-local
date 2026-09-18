# Raw ingestion contract (Phase 4)

Phase 4 makes the Delivery-centered path consumable by Raw while preserving
the legacy path:

```text
Transport -> DeliveryManifest -> NormalizationManifest v2 -> Spark -> Raw Iceberg
landing/<feed>/<file> -> Ready manifest v1 -----------------------> Spark -> Raw Iceberg
```

The entry points are explicit. `ingest(feed, key, ...)` retains Ready v1 and
manual Landing compatibility. `ingest_normalized_delivery(key, ...)` accepts a
NormalizationManifest v2 key alone; `scripts._spark_task ingest-v2` is the
process-isolated adapter an Airflow task can call later. Phase 4 adds no event,
sensor, schedule, or DAG migration.

## Delivery identity and physical provenance

For v2, `_delivery_id` is the opaque `dlv_...` identity of the accepted
Delivery. Every row read from every part of that Delivery carries the same
value. It is never a filename, TransportID, Ready part, or run id.

`_source_file` is the object Spark actually read. A plain Delivery therefore
records its `received/<transport-id>/<original name>` key. An archive records
one `ready/<feed>/<delivery-id>/part-NNNN.csv` key per member; those rows have
different `_source_file` values and one `_delivery_id`.

The remaining row provenance is:

| Raw field | Meaning and source |
| --- | --- |
| `_cob_date` | Frozen `business_date` from Delivery/Normalization evidence; never parsed from a part name. |
| `_schema_version` | Historical Delivery schema contract recorded in Normalization v2, not today's Feed digest. |
| `_source_system` | Business source system snapshotted with the normalization contract; Transport mechanism such as DCM is not substituted. |
| `_received_at` | Durable transport receipt time from Delivery evidence. |
| `_ingest_ts` | UTC execution observation taken when Raw ingestion constructs the write. This repository's established name is `_ingest_ts` (the conceptual `_ingested_at`). |
| `_batch_id` | This Raw execution's run id, not Delivery identity. |

New DeliveryManifests also snapshot `source_system`, `expected_min_rows`, and
`schema_drift` with parser format, columns, and source-column mapping. Immutable
Phase 2/early Phase 3 manifests are not backfilled. If those older snapshots
lack the additive fields, first Phase 4 ingestion uses the current Feed only
for the missing source/control values; their recorded business date, schema
version, format, columns, and mapping still win.

## The Raw ledger

`already_ingested_delivery(feed, delivery_id)` queries committed Raw `main`
for `_delivery_id`. The v2 domain operation runs this guard itself, so an
explicit retry is an idempotent no-op. It refreshes the Iceberg table before
the query because chunked ingestion deliberately reuses a Spark session across
Nessie merges.

Two Deliveries with the same producer basename, or with identical bytes, have
different DeliveryIDs and ingest independently. Content hash and
`_source_file` are not v2 deduplication keys. Legacy pending discovery keeps
its existing `_source_file` ledger until the legacy path is retired.

No `ingested` flag is written to DeliveryManifest, NormalizationManifest, or
Postgres. `registry.delivery` answers which evidence has been observed; Raw
answers which Delivery has been ingested.

## Validation and commit boundary

The shared writer retains strict parser validation, historical column mapping,
schema-drift policy, minimum row count, producer-declared exact row count, and
producer MD5 verification. Assertions, observations, and results remain
separate: the manifest holds what the producer declared, Spark measures parsed
rows/bytes, and ingestion either accepts or raises without mutating evidence.

Each attempt creates a Nessie branch. Schema reconciliation, reads, counts,
checksum checks, and the Iceberg append occur there; only `Nessie.merge` makes
the result visible on `main`. A parse/control failure or a failure after branch
append but before merge therefore remains absent from the DeliveryID ledger
and eligible for a new attempt. Failed branches remain for inspection under
the existing operational policy.

The single-slot `lakehouse_write` pool serializes production writers. Phase 4
does not introduce a mutable claim/status table or redesign writer concurrency.

## `_file_version`

`_file_version` remains the platform-assigned ordering number for a Feed and
business date (`MAX + 1` on the isolated ingest branch). Existing dbt
full-snapshot/SCD2 logic uses it to select the latest Raw restatement. The
optional producer `file_version` in Delivery evidence is retained in the
manifests but is not substituted into this platform ordering field.

Thus `_file_version` remains an ordering mechanism; it is not the identity of
an accepted Delivery and does not compete with `_delivery_id`.

## Deferred boundaries

Prepared/Reporting provenance, `registry.run_input`, and dbt lineage migration
remain Phase 5. Airflow-native discovery and orchestration remain Phase 6.
Legacy historical rows are not rewritten or backfilled, Ready v1 is not
removed, and no supersession/SCD2 policy is redesigned here.
