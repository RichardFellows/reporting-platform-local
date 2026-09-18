# DCM to S3 Transport Contract v1

This contract is the boundary between DCM's existing acquisition job and the
reporting platform. DCM watches DFS, decides that the source delivery is
complete, and performs a follow-on upload. The platform begins with completed
S3-compatible object-storage evidence; it does not watch DFS, poll SFTP, or
infer whether a file has stopped changing.

Transport is not Delivery. Phase 1 stores and validates acquisition evidence
only. It does not map `legacy_feed_id` to a feed, create a DeliveryID, derive a
COB date, write current Landing/Ready, trigger Airflow, or ingest raw data.
The existing flow documented in `docs/INGESTION-BASELINE.md` remains active
and unchanged.

## Storage boundary

The configurable prefix is `REPORTING_RECEIVED_PREFIX`, defaulting to
`received`. One completed transfer has this shape:

```text
received/
  <transport-id>/
    <original data filename>
    <additional original data filename>   # zero or more
    <original control filename>           # zero or more
    _COMPLETE.json                        # uploaded last
```

Every original filename is a basename and its bytes are stored unchanged.
The platform never renames, modifies, or unpacks source objects in
`received/`. This differs from current `landing/<feed>/...`, whose basename
must already conform to `Feed.filename_pattern` or be conformed by the inbox.
Nothing copies a Phase 1 Transport into that legacy shape.

## TransportID

`transport_id` is a stable, caller-supplied identifier for one DCM transfer
event, such as `dcm-1234-98765`. The reporting platform treats its content as
opaque and only requires it to be one safe S3 path segment.

It is not a filename, DeliveryID, business date, or Airflow run ID. Retrying
the same DCM execution uses the same TransportID. A correction or restatement
is a new DCM execution and therefore a new TransportID. Separate transports
may preserve the same original filename without collision because their
prefixes differ.

## `_COMPLETE.json` v1

The UTF-8 JSON document is strictly parsed. Unknown fields are rejected so a
producer cannot believe it supplied evidence the consumer silently ignored.
`producer_run_id` is optional; every other top-level field shown below is
required.

```json
{
  "transport_contract_version": 1,
  "transport_id": "dcm-1234-98765",
  "source": "DCM",
  "legacy_feed_id": "1234",
  "source_observed_at": "2026-09-17T05:42:17Z",
  "uploaded_at": "2026-09-17T05:43:02Z",
  "producer_run_id": "DCM-849217",
  "files": [
    {
      "role": "data",
      "original_filename": "positions_final_FINAL2.zip",
      "object_key": "received/dcm-1234-98765/positions_final_FINAL2.zip",
      "bytes": 184273921,
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    },
    {
      "role": "control",
      "original_filename": "positions_final_FINAL2.ctl",
      "object_key": "received/dcm-1234-98765/positions_final_FINAL2.ctl",
      "bytes": 214,
      "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    }
  ]
}
```

Supported roles are `data` and `control`. At least one data object is
required; multiple data and control objects are allowed. Timestamps must be
ISO-8601 values with a UTC offset. SHA-256 values are 64 hexadecimal
characters. Existing MD5-based feed/control behaviour is unrelated and is not
changed by this contract.

Each `object_key` must be exactly
`<received-prefix>/<transport-id>/<original_filename>`. This pins the declared
original basename to the stored evidence key and rules out renaming, nested
paths, prefix escape, and `_COMPLETE.json` declaring itself as source data.

## Completion and acceptance

DCM writes in this order:

1. Upload every original data object.
2. Upload every original control object, when present.
3. Verify the uploaded objects.
4. Upload `_COMPLETE.json` last.

An object without its marker is not returned by
`list_completed_transports()`. Marker existence is the uploader's completion
assertion, not proof that the assertion is correct. Before acceptance,
`read_validated_transport()`:

- strictly parses the JSON and accepts only contract version 1;
- verifies required fields, roles, safe basenames, unique declarations, and
  exact TransportID-scoped keys;
- checks that every referenced object exists;
- compares its actual byte length with `bytes`; and
- streams and calculates SHA-256 from its bytes, rather than trusting an S3
  ETag.

Validation only reads storage and is safe to repeat. Errors identify the
TransportID and object key, but never include source contents or credentials.
A visible marker whose referenced object is absent or different fails
validation and is not an accepted transport.

## Retry and immutability semantics

The application contract is append-only:

- source objects and `_COMPLETE.json` are created conditionally, never
  overwritten by the simulator;
- an existing completed TransportID is read and fully revalidated;
- an identical retry returns that existing Transport, making no writes; and
- changed source bytes or conflicting source metadata under the same
  TransportID fail.

`uploaded_at` records the first successfully published marker. A retry may
calculate a later upload time, but the accepted marker is not rewritten and
its original `uploaded_at` is returned. Source observations and all file
declarations must still agree exactly.

A failed upload can leave unmarked source objects. A retry may reuse an
identical object, but refuses a conflicting one before publishing a marker.
Only the marker makes the set visible as completed.

This is application behaviour, not infrastructure-enforced immutability. S3
versioning, Object Lock/WORM, IAM denial of overwrite/delete, and retention
policy are production deployment decisions still to be made. Application
checks cannot prevent a separately authorised storage principal from
replacing evidence.

## What DCM must implement

The future .NET follow-on action must:

- derive a stable TransportID from its existing feed/job/execution identity;
- reuse it when retrying that execution and allocate a new one for a genuine
  correction;
- preserve source basenames and bytes beneath that TransportID;
- calculate byte counts and SHA-256 for every source object;
- supply the required observations and optional producer run ID;
- verify its uploads; and
- create `_COMPLETE.json` only after all source objects are durable.

DCM does not need to conform filenames to reporting-platform feed patterns.
It must not upload a marker that references a missing, partially uploaded, or
differently hashed object.

## Local DCM simulator

`scripts/simulate_dcm_transport.py` models the producer side for local MinIO.
It takes explicit files and never watches a directory:

```powershell
docker compose exec -T feed-ui python -m scripts.simulate_dcm_transport `
  --transport-id dcm-1234-98765 `
  --legacy-feed-id 1234 `
  --source-observed-at 2026-09-17T05:42:17Z `
  --producer-run-id DCM-849217 `
  --data /opt/platform/inbox/positions_final_FINAL2.zip `
  --control /opt/platform/inbox/positions_final_FINAL2.ctl
```

Repeat `--data` or `--control` for multi-object transports. The simulator
reads the caller's basenames and bytes, calculates SHA-256, preflights existing
objects, uploads absent source objects, validates the complete set, and writes
the marker last with an S3 create-only precondition. Repeating the command is
an idempotent retry when evidence and source observations match.

The receiving API lives in `reporting_platform/ingest/transport.py`; the
simulator implementation is isolated in
`reporting_platform/ingest/dcm_simulator.py`. Neither is called by an Airflow
DAG in Phase 1.

The accepted-Transport consumer is now specified in
[`DELIVERY-CONTRACT.md`](DELIVERY-CONTRACT.md). It resolves the explicit
external Feed id and creates a separate immutable DeliveryManifest without
copying evidence into Landing.

## Deferred decisions

Phase 2 defines Transport-to-Delivery in `docs/DELIVERY-CONTRACT.md`. Later
phases still own orchestration, Delivery-aware normalization/raw provenance,
registry reconciliation, and deployment-level object-store controls.
