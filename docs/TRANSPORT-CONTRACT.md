# DCM to S3 Transport Contract v2

This contract is the boundary between DCM's existing acquisition job and the
reporting platform. DCM watches DFS, decides that the source delivery is
complete, and performs a follow-on upload. The platform begins with completed
S3-compatible object-storage evidence; it does not watch DFS, poll SFTP, or
infer whether a file has stopped changing.

Transport is not Delivery. This contract stores and validates acquisition
evidence only. It does not map `legacy_feed_id` to a feed, create a
DeliveryID, derive processing state, write current Landing/Ready, trigger
Airflow, or ingest raw data — see [`DELIVERY-CONTRACT.md`](DELIVERY-CONTRACT.md)
for what happens to an *accepted* Transport.

## What v2 changes, and why

v1 stored one flat prefix per transport (`received/<transport-id>/...`) with
no business date or source classification in the contract at all. v2 adds two
producer-supplied fields — `cob_date` and `source_system` — and reorganises
the S3 layout under them, deliberately resembling DCM's own existing
DFS/delivery organisation by COB date and source system:

```text
received/
  cob_date=2026-09-21/
    source_system=RISK_ENGINE_X/
      dcm-1234-849217/
        positions_20260921.csv
        positions_20260921.ctl
        _COMPLETE.json
```

This is what makes *bounded* reconciliation possible: a periodic sweep can
list `cob_date=<date>/` partitions for a recent window instead of the whole
Transport history forever. See "Reconciliation implications" below.

**v2 markers are the only thing this platform publishes going forward. v1
markers already in a bucket remain permanently readable** — there is no tool
in this platform that rewrites historical `received/` evidence, so both
shapes may legitimately coexist. See "v1 compatibility" below.

## Field semantics

### Producer-supplied (DCM authoritatively knows these)

| Field | Meaning |
|---|---|
| `legacy_feed_id` | DCM's own feed identity. Authoritative — the reporting platform's Feed mapping (`Feed.source_identifiers`) resolves against this, never against a filename. |
| `producer_run_id` | DCM's own execution/run identity. **Required in v2** (optional in v1): the deterministic TransportID formula needs it, so an omitted value has no meaning any more — see "TransportID" below. |
| `cob_date` | The business date this delivery represents. Producer-supplied business context, **not** inferred from a filename or an upload timestamp — DCM already knows the COB date it is delivering for; deriving it a second time from evidence the platform can see would be a second, potentially disagreeing, source of truth. |
| `source_system` | DCM's classification of the producing system (e.g. `RISK_ENGINE_X`) — provenance/classification, not a routing key. **Naming collision, deliberately not resolved**: `Feed.source_system` (e.g. `QA`) already names something different — the feed's own domain classification in `feeds.yml`. The two are unrelated concepts that happen to share a name at different layers; this contract does not rename either to avoid the collision, and `DeliveryManifest` does not (yet) cross-reference them — see "Deferred: DeliveryManifest does not yet snapshot Transport's cob_date/source_system" below. |
| `source_observed_at` | When DCM observed the source delivery complete. |
| files (`data`/`control`) | The original source files and their roles, exactly as DCM has them. |

### Derived (the publisher computes these; DCM must not construct them)

`transport_id`, the S3 prefix, `original_filename` (from the path DCM gave),
`object_key`, `bytes`, `sha256`, `uploaded_at`, `transport_contract_version`,
and `source` (defaults to `"DCM"`).

## TransportID

`transport_id` is derived deterministically:

```text
dcm-{legacy_feed_id}-{producer_run_id}
```

— more precisely `{source.lower()}-{legacy_feed_id}-{producer_run_id}`, since
`source` defaults to `"DCM"` but is not hard-coded. `legacy_feed_id`,
`producer_run_id` and `source` must each already be a safe token (letters,
digits, `.`, `_`, `-`) — the publisher validates and refuses rather than
hashing or otherwise silently re-encoding an unsafe value. A hash-based
encoding was considered and rejected: this format keeps `transport_id`
human-debuggable in an S3 console or a DCM operator's log, which a digest
would defeat, and it matches the contract's own worked example.

The same `(source, legacy_feed_id, producer_run_id)` always derives the same
`transport_id` — retrying the same DCM execution is automatically idempotent
with no state DCM has to remember and pass back. A genuine correction or
restatement must use a new `producer_run_id`; nothing here is ever derived
from a timestamp.

To the reporting platform, `transport_id` remains opaque beyond being one
safe path segment — nothing downstream parses it apart.

**Known, accepted limitation.** `transport_id` does not include `cob_date` or
`source_system` (per the formula above), but they ARE part of the S3 path. A
caller that reuses `producer_run_id` while declaring a *different* `cob_date`
or `source_system` is therefore **not** detected as a conflicting retry — it
publishes a second, independent marker at a different partition instead.
Catching this would need an unbounded reverse lookup by `transport_id` across
every COB partition, which conflicts directly with the bounded-reconciliation
goal this version exists to serve. This is accepted as a DCM-side contract
obligation — retry the same execution with the same `cob_date`/
`source_system` every time — rather than solved here. Every other
producer-declared field changing under the same `transport_id` (a different
`source_observed_at`, different file bytes, a different `legacy_feed_id`
feeding a different derived id, …) still fails as a conflicting retry; see
"Retry and idempotency semantics".

## `_COMPLETE.json` v2

Strictly parsed; unknown fields are rejected. All fields below are required
(v2 has no optional field — contrast v1, where `producer_run_id` alone was
optional).

```json
{
  "transport_contract_version": 2,
  "transport_id": "dcm-1234-849217",
  "source": "DCM",
  "legacy_feed_id": "1234",
  "producer_run_id": "849217",
  "cob_date": "2026-09-21",
  "source_system": "RISK_ENGINE_X",
  "source_observed_at": "2026-09-22T01:13:00Z",
  "uploaded_at": "2026-09-22T01:14:22Z",
  "files": [
    {
      "role": "data",
      "original_filename": "positions_20260921.csv",
      "object_key": "received/cob_date=2026-09-21/source_system=RISK_ENGINE_X/dcm-1234-849217/positions_20260921.csv",
      "bytes": 184273921,
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  ]
}
```

Supported roles are `data` and `control`. At least one data object is
required; multiple data and control objects are allowed. `cob_date` must be a
real ISO-8601 calendar date written exactly `YYYY-MM-DD`. `source_system` must
be one safe path segment. Timestamps must be ISO-8601 with a UTC offset.
SHA-256 values are 64 hexadecimal characters.

**The marker is authoritative transport metadata; the S3 path is physical
organisation only.** The reporting platform validates that a marker's
`cob_date`/`source_system`/`transport_id` and the key it was read from agree
— rejecting a marker filed under a path it does not claim — rather than ever
*inferring* `cob_date` or `source_system` from the path alone. Each
`object_key` must still be exactly
`<received-prefix>/cob_date=<cob_date>/source_system=<source_system>/<transport-id>/<original_filename>`,
pinning every declared object to the marker's own declared identity.

## Publishing semantics

The publisher (`reporting_transport.publisher.publish_transport`) implements
this sequence:

```text
validate producer inputs
        |
derive stable TransportID/prefix
        |
for each declared file:
  already exists remotely?  --yes--> hash locally, verify against remote
        |no
  upload it, hashing what is actually streamed (one read, see below)
        |
verify EVERY declared object's evidence against object storage
  (never trust the upload response or an S3 ETag)
        |
existing marker for this TransportID?  --yes--> compare; identical returns
  it unchanged, conflicting fails
        |no
construct marker
        |
conditionally create _COMPLETE.json LAST (create-only)
```

This is a deliberate reordering of the "check existing → upload missing →
verify → publish" shape from v1, not a different set of guarantees: the
outcome (append-only, source objects never silently overwritten, marker never
overwritten, conflicting retry fails, `uploaded_at` fixed at first
publication) is identical. The change is *when* local hashing happens per
file, and it exists to fix a real correctness/efficiency gap — see next.

### Hashing and mutation safety

v1's simulator computed a file's SHA-256 with one full local read, then
opened the file again as the upload body — two reads, and a window between
them in which the file could change without the second read noticing before
upload. v2's publisher uploads a file it doesn't yet have evidence for through
a small wrapper (`_HashingReader`) that computes SHA-256 and byte count *from
what is actually streamed to object storage*, in the same read pass as the
upload — one disk read instead of two for the common case (first publish of a
large file), and the declared evidence can never describe bytes the upload
itself did not send.

That still leaves a source file re-written *after* the read completes but
before the object is durable. This is closed by the unconditional final step
above: every declared object's actual size and SHA-256 are re-derived from
what object storage holds *right now*, for every file, every time, before
`_COMPLETE.json` is ever written. A marker is therefore never published
whose SHA-256 describes different bytes than what is durably stored — the
guarantee holds even if the two-read window above were never closed at all;
closing it is an efficiency and a belt-and-braces improvement, not the sole
mechanism enforcing correctness.

Byte counts and hashes are always derived from streamed bytes, never from an
S3 ETag — ETag is not guaranteed to be an MD5 (multipart uploads, some
S3-compatible backends) and is never SHA-256.

## Retry and idempotency semantics

Append-only, exactly as v1:

- source objects and `_COMPLETE.json` are created conditionally
  (`IfNoneMatch: *`), never overwritten by the publisher;
- an existing completed TransportID is read and fully revalidated;
- an identical retry returns the existing Transport, making no writes;
- changed source bytes or conflicting declared metadata under the same
  TransportID fail with `TransportConflictError` rather than overwriting; and
- `uploaded_at` records the first successfully published marker and is never
  rewritten by a later retry.

A failed upload can leave unmarked source objects; a retry reuses an
identical one and refuses a conflicting one before ever publishing a marker.
Only the marker makes the set visible as completed
(`list_completed_transports`).

**Correction/restatement.** A genuine correction is a new `producer_run_id`
(see "TransportID"), which derives a new `transport_id` and therefore
publishes an entirely independent Transport at its own path — the platform
never mutates or supersedes a previously accepted Transport in place. Whether
a correction under the same COB date becomes a superseding Delivery is a
question for [`DELIVERY-CONTRACT.md`](DELIVERY-CONTRACT.md) and the feed's own
`supersession:` declaration, not for this contract.

This is application behaviour, not infrastructure-enforced immutability. S3
versioning, Object Lock/WORM, IAM denial of overwrite/delete, and retention
policy remain deployment decisions.

## v1 compatibility

`reporting_transport.contract.parse_transport` accepts both
`transport_contract_version: 1` and `2` **permanently** — this is a read-only
compatibility path, not a migration window with an end date. `Transport.
cob_date`/`Transport.source_system` are `None` for a parsed v1 marker and
always set for v2. There is no tool that rewrites an already-published v1
marker into v2 shape, and none is planned: a Transport's evidence is
immutable once published, and "migrating" it would mean fabricating a
`cob_date`/`source_system` the original delivery never declared.

Practical implications:

- `list_completed_transports(cob_dates=None)` (the default) finds both
  shapes, listing the whole `received/` prefix.
- `list_completed_transports(cob_dates=[...])` — the bounded form —
  **only** finds v2 markers, because v1 markers have no `cob_date=` segment
  to be found by. A v1 marker is therefore reachable only through the
  unbounded listing; see "Reconciliation implications" below.
- The publisher (`reporting_transport.publisher`) only ever **writes** v2. No
  code path in this repository writes a v1 marker any more.

## Authentication and configuration

`reporting_transport.storage.StorageConfig` is explicit, reusable storage
configuration — endpoint, bucket, region, received prefix — deliberately
**not** a credentials bag: authentication always goes through the standard
AWS/boto3 credential-provider chain (environment variables, shared
credentials/config files, an instance/pod IAM role, …), never a
command-line argument, so nothing in this package or its CLI can leak a
secret into a log or a process list.

`StorageConfig.from_env()` resolves, in order, `REPORTING_TRANSPORT_*`
variables, falling back to the same names `docker-compose.yml` already sets
for the rest of this platform so the local simulator needs no new
configuration:

| `StorageConfig` field | `REPORTING_TRANSPORT_*` | RPL fallback |
|---|---|---|
| `bucket` | `REPORTING_TRANSPORT_BUCKET` | derived from `REPORTING_WAREHOUSE` |
| `endpoint_url` | `REPORTING_TRANSPORT_S3_ENDPOINT` | `S3_ENDPOINT` |
| `region` | `REPORTING_TRANSPORT_REGION` | `AWS_REGION` |
| `received_prefix` | `REPORTING_TRANSPORT_RECEIVED_PREFIX` | `REPORTING_RECEIVED_PREFIX` (default `received`) |

A standalone DCM deployment with none of the RPL-side variables set should
set the `REPORTING_TRANSPORT_*` names explicitly, or pass the equivalent CLI
flags (`--bucket`, `--s3-endpoint`, `--region`, `--received-prefix`), which
always take priority over both.

## CLI invocation

`reporting_transport` is directly invocable and is the reference producer a
real DCM environment should call as a subprocess:

```bash
python -m reporting_transport publish \
  --legacy-feed-id 1234 \
  --producer-run-id 849217 \
  --cob-date 2026-09-21 \
  --source-system RISK_ENGINE_X \
  --source-observed-at 2026-09-22T01:13:00Z \
  --data "\\dfs\...\positions.csv" \
  --control "\\dfs\...\positions.ctl"
```

`--data`/`--control` are repeatable, for multi-object transports. Storage
configuration is resolved from the environment as above, or overridden with
`--bucket`/`--s3-endpoint`/`--region`/`--received-prefix`.

Exactly one JSON object is printed to stdout, always — success or failure —
and never contains source-file contents or credentials:

```json
{
  "status": "published",
  "transport_contract_version": 2,
  "transport_id": "dcm-1234-849217",
  "...": "... the rest of the accepted marker fields ..."
}
```

`status` is `"published"` (this call created the marker) or
`"already_published"` (an idempotent retry — every field, `uploaded_at`
included, describes the ORIGINAL publication). An error instead prints
`{"status": "error", "error_type": ..., "message": ..., "exit_code": ...}`,
and the same short message (never file contents or credentials) is echoed to
stderr for a human operator.

### Exit codes

Stable and documented, so DCM automation can branch on the code without
parsing message text:

| Code | Meaning | `error_type` |
|---|---|---|
| 0 | Newly published, or an idempotent retry — both are success | *(none; see `status`)* |
| 1 | Unexpected error not covered below | `unexpected_error` |
| 2 | Invalid input or a marker/contract violation | `invalid_input` |
| 3 | Transport conflict — a conflicting retry under the same TransportID | `transport_conflict` |
| 4 | Evidence mismatch — a declared object is missing or its stored bytes disagree | `evidence_mismatch` |
| 5 | Storage/auth/connectivity failure | `storage_error` |

"Newly published" and "already published" share exit code 0 deliberately: a
DCM scheduler checking `$? -eq 0` should treat an idempotent retry as success,
not a failure requiring alerting. The finer distinction is carried in
`status` for anything that wants it.

## Local MinIO example

```powershell
docker compose exec -T feed-ui python -m scripts.simulate_dcm_transport `
  --legacy-feed-id qa-happy-position `
  --producer-run-id local-dev-1 `
  --cob-date 2026-09-14 `
  --source-system QA `
  --source-observed-at 2026-09-17T05:42:17Z `
  --data /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.csv `
  --control /opt/platform/tests/fixtures/happy_path/qa_happy_position_20260914.ctl
```

`scripts/simulate_dcm_transport.py` is a thin wrapper, not a second
implementation: it builds a `StorageConfig` from RPL's own environment
(`S3_ENDPOINT`, `REPORTING_WAREHOUSE`, `REPORTING_RECEIVED_PREFIX`) and calls
`reporting_transport.publisher.publish_transport` — the identical function
the CLI above calls. Repeating the command is an idempotent retry.

## Production DCM example

A real DCM follow-on action invokes the same package as a subprocess, no RPL
import required — `reporting_transport` has no dependency on the rest of this
repository, only `boto3` and the standard library:

```powershell
python -m reporting_transport publish `
  --legacy-feed-id 1234 --producer-run-id 849217 `
  --cob-date 2026-09-21 --source-system RISK_ENGINE_X `
  --source-observed-at 2026-09-22T01:13:00Z `
  --bucket enterprise-lakehouse --s3-endpoint https://s3.internal.example `
  --data "\\dfs\risk\positions_20260921.csv" `
  --control "\\dfs\risk\positions_20260921.ctl"
```

Credentials are supplied through the environment (an IAM role, or
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` set by
whatever secret store DCM's host already uses) — never on this command line.

## Reconciliation implications

The `cob_date=`/`source_system=` partitioning exists specifically so a
periodic reconciliation sweep is not forced to re-list every Transport ever
published. `reporting_platform.ingest.transport.list_completed_transports`
takes an optional `cob_dates` iterable: given, it lists only those COB
partitions (a bounded read whose cost does not grow with total Transport
history); omitted (the default), it still lists the whole `received/`
prefix, v1 markers included, exactly as before.

`transport_reconcile` (the Airflow DAG) uses the bounded form by default —
the last `TRANSPORT_RECONCILE_WINDOW_DAYS` calendar days (default 7) — and
falls back to the unbounded form only when triggered with
`-c '{"full_sweep": true}'`, an occasional/manual operation rather than the
periodic schedule. See `docs/AIRFLOW-ORCHESTRATION.md`, "Reconciliation scale
(v2)", for the full design and why this is a genuine narrowing of the earlier
"always list everything" behaviour, not an addition to it — a Transport whose
COB date falls outside the window, or a v1 marker, is no longer discovered by
the default scheduled run and needs a full sweep to be found.

## Deferred: DeliveryManifest does not yet snapshot Transport's cob_date/source_system

`Delivery.create_delivery` still resolves its own `business_date` purely from
declared control/filename evidence, exactly as before Contract v2 — Transport
describes what DCM delivered, Delivery describes what the reporting platform
understood/accepted it to be, and this contract deliberately does not collapse
the two (see `docs/DELIVERY-CONTRACT.md`). Transport's `cob_date` is not
cross-checked against Delivery's resolved `business_date`, and
`DeliveryManifest`'s `transport: {...}` block does not (yet) carry
`cob_date`/`source_system` through for reference. Both would be small,
additive follow-ups; neither was needed to make v2 work end to end, so
neither is implemented here.

## Reusable package layout

```text
reporting_transport/
    contract.py    the versioned wire contract: parse/validate/path rules,
                   pure Python, no boto3, no environment reads
    storage.py     StorageConfig, the boto3 client builder (standard
                   credential chain only), and generic S3 evidence I/O
    publisher.py   publish_transport() -- the one publication algorithm,
                   used by both the local simulator and the CLI
    cli.py         argument parsing, JSON output, exit-code mapping
    __main__.py    `python -m reporting_transport`
```

`reporting_platform/ingest/transport.py` is RPL's **consumer** side: it
imports `reporting_transport.contract` for parsing (so there is exactly one
implementation of what a marker means) and adds only what belongs to the
platform specifically — its own S3 client/bucket wiring
(`reporting_platform/ingest/arrival.py`'s conventions), evidence
re-verification, and discovery. Neither module knows about Airflow DAGs,
Iceberg tables, DeliveryIDs, or raw/prepared/reporting layers — see
`docs/DELIVERY-CONTRACT.md` for where that starts.

The accepted-Transport consumer is specified in
[`DELIVERY-CONTRACT.md`](DELIVERY-CONTRACT.md). Orchestration —
`transport_watch`, `transport_reconcile`, `transport_ingest` — is in
`docs/AIRFLOW-ORCHESTRATION.md`.
