# Normalization contract (Phase 3)

Phase 3 adds a Delivery-centered normalization path beside, not in place of,
the legacy Landing path:

```text
received/<transport-id>/...                         long-lived evidence
        |
deliveries/<source>/<transport-id>/delivery-manifest.json
        |
ready/<feed>/<delivery-id>/                        rebuildable cache
    normalization-manifest.json
    part-0001.csv                                  archives only
```

`DeliveryManifest v1` is immutable historical evidence and identity. It says
what Transport was accepted, which Feed and business date it resolved to, and
which `received/` objects contain the producer evidence.

`NormalizationManifest v2` is a rebuildable consumption plan. It says how a
later Raw reader should parse that Delivery and which ordered objects hold its
rows. It references the DeliveryManifest rather than copying its identity
evidence. Legacy Ready manifest v1 is unchanged. Phase 4 now consumes this v2
manifest through the explicit `ingest_normalized_delivery()` entry point while
the v1 Raw path remains operational; see `RAW-INGESTION-CONTRACT.md`.

## Manifest v2

The JSON document contains:

```json
{
  "normalization_manifest_version": 2,
  "delivery_id": "dlv_...",
  "feed": "qa_happy_position",
  "delivery_manifest": "deliveries/DCM/dcm-123/delivery-manifest.json",
  "business_date": "2026-09-17",
  "file_version": 2,
  "received_at": "2026-09-17T05:43:02Z",
  "schema_version": "abc123",
  "source_system": "QA",
  "normalizer": "file/v2",
  "format": {"delimiter": ",", "quote_char": "\"", "header": true,
             "encoding": "utf-8"},
  "normalization_contract": {},
  "contract_source": "delivery_manifest",
  "parts": [],
  "checksum_objects": [],
  "declared_row_count": 4521847,
  "declared_md5": "...",
  "source_object": "received/dcm-123/original.csv"
}
```

`normalization_contract` is the exact resolved contract used: contract
version, delivery kind, parser format, declared platform columns, source
column mapping, business source system, ingestion controls, and archive member
pattern where applicable. `format` is also
top-level as the stable field shape consumed by the Phase 4 Raw adapter.

Identity is never parsed again. `business_date`, optional `file_version`,
`received_at`, `schema_version`, producer row count and producer MD5 all come
from the DeliveryManifest. Normalization does not list, find, or parse a
sibling control file. `checksum_objects` names the producer data object the
sender hashed: the plain file or the original archive, never extracted parts.

For newly created Delivery evidence, `source_system`, `expected_min_rows`, and
`schema_drift` are snapshotted as ingestion semantics. DCM remains the
Transport source and is not silently used as the business source system.

## Plain files

`file/v2` writes only the small manifest. Its one part points directly to the
original `received/` object and records `materialized: false` plus the original
producer filename as metadata. No Landing compatibility object and no Ready
data copy is created. The object size and SHA-256 are checked against the
DeliveryManifest; a filename need not match `Feed.filename_pattern`.

## Archives

`archive/v2` reads the original zip directly from `received/`, checks its size
and SHA-256, and leaves it unchanged. Matching flat members are sorted by
their original member name and written as deterministic
`part-0001<suffix>`, `part-0002<suffix>`, and so on under the DeliveryID Ready
prefix. The original member name is metadata, never storage identity.

Members with paths are refused, an invalid zip fails, and an archive with no
matching member fails. The producer MD5 continues to cover the received
archive; extracted members are platform-derived cache objects.

## Reproducibility and compatibility

New DeliveryManifests snapshot the normalization contract inside
`feed_contract.normalization`. A later Feed YAML change therefore cannot alter
a rebuild. Existing Phase 2 manifests are not mutated. If that nested snapshot
is absent, first normalization explicitly uses today's resolved Feed contract
and records `contract_source: current_feed_compatibility` plus the complete
contract in NormalizationManifest v2. Once that v2 manifest exists, its
recorded contract wins on retry.

The compatibility limitation is deliberate and visible: if both Ready v2 and
an old Phase 2 manifest's cache are deleted, a rebuild must use current Feed
configuration because no historical normalization snapshot exists. There is
no honest way to reconstruct configuration that was never recorded.

## Idempotency and rebuilding

Manifest and archive-part bytes contain no run clock or random identifier.
Writes are create-only; an existing key is accepted only when its bytes are
identical. That supports identical retries and recovery after a partial
archive extraction while detecting conflicting cache contents. Deleting the
DeliveryID Ready directory and rerunning from DeliveryManifest plus
`received/` recreates identical keys and bytes.

Ready remains short-lived derived state. No `normalized` flag is added to the
DeliveryManifest or registry.

## Registry projection

Phase 3 may project the chain into the existing `registry.delivery` natural
key `(feed, delivery_id)`. For v2, `manifest_key` is the immutable
DeliveryManifest key and `source_object` is the producer object in
`received/`. Registration is best-effort; `reconcile_v2()` walks object-store
manifests and is the correctness path. Its upsert makes repetition safe and it
does not modify `run_input`.

V2 parts live in the additive `registry.normalization_part` table. They carry
`materialized`, optional `source_member`, key and size. Legacy
`registry.delivery_part` is unchanged: it still means objects whose key can
join to today's Raw `_source_file`. Keeping the tables separate avoids making
a Phase 4 Raw-provenance claim in Phase 3.

## Phase 4 boundary

Phase 3 stopped at an ingestion-ready plan. Phase 4 adds explicit v2 Raw
consumption and DeliveryID-led idempotency without changing the legacy v1
queue or the generated Airflow ingest DAG. Phase 5 maps the resulting
`_delivery_id` unchanged into Prepared and run inputs while retaining the
legacy `delivery_ref()` fallback. Phase 6 adds Airflow-native discovery and
orchestration: `transport_ingest`'s `normalize_delivery` task calls
`normalize_delivery()` unchanged from the description above, and
`transport_reconcile` walks this manifest's key
(`normalization.manifest_key()`) as one stage of its durable-evidence
progress check. See `docs/AIRFLOW-ORCHESTRATION.md`.
