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
for each declared file: storage.publish_object()
        |
  already exists remotely?  --yes--> hash locally once, confirm against remote
        |no
  stat it, choose single PUT (small) or multipart (>= threshold)
        |
  upload, hashing exactly what is streamed (one read, see below)
        |
  confirm durable storage matches that hash -- cheaply, via a
  server-computed SHA-256 checksum, when the backend supports one
  usable for THIS object; via a full GET + rehash otherwise
        |
  guard against source mutation (stat before vs. stat + bytes-read after)
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
file, and it exists to fix a real correctness/efficiency gap — see next, and
"Large-file upload and verification strategy" below for the full hardening
this version adds for multi-GB files.

### Hashing and mutation safety

v1's simulator computed a file's SHA-256 with one full local read, then
opened the file again as the upload body — two reads, and a window between
them in which the file could change without the second read noticing before
upload. v2's publisher uploads a file it doesn't yet have evidence for through
a small wrapper (`_HashingReader`, now in `storage.py`) that computes SHA-256
and byte count *from what is actually streamed to object storage*, in the
same read pass as the upload (extended for multipart to hash across part
boundaries in one continuous pass — see below) — one disk read instead of two
for the common case (first publish of a large file), and the declared
evidence can never describe bytes the upload itself did not send.

That still leaves a source file re-written *after* the read completes but
before the object is durable. This is closed by an unconditional confirmation
step, run once per file inside `storage.publish_object()`: every declared
object's actual size and SHA-256 are re-derived from what object storage
holds *right now* — via a server-computed checksum where that is trustworthy
and usable without a download, via a full GET + rehash otherwise (see "Large-
file upload and verification strategy") — for every file, every time, before
`_COMPLETE.json` is ever written. A marker is therefore never published whose
SHA-256 describes different bytes than what is durably stored — the
guarantee holds even if the hash-while-streaming property above were never
true at all; it is an efficiency and a belt-and-braces improvement, not the
sole mechanism enforcing correctness. (Earlier versions of this publisher ran
this confirmation TWICE per file — once inline for a pre-existing object,
once again unconditionally after the loop; that duplication is gone, not the
guarantee it provided.)

`storage.publish_object()` additionally guards against the source file
itself changing during publication — see "Source mutation protection" below.

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

## Large-file upload and verification strategy

The v1/early-v2 publisher uploaded a file while hashing it (good), then
called `verify_object_evidence()`, which performed a full `GetObject` and
re-hashed the whole response body — for every file, every publish, including
an idempotent retry of an object already durably stored. For a large feed
this means the object crosses the network three times for one publish
(source → publisher, publisher → S3, S3 → publisher) where two would do, and
it does not scale to multi-GB feed files. `storage.py` now avoids the third
leg whenever the backend can prove it unnecessary, and adds real multipart
upload so the second leg itself is streamed, bounded-memory, and can use
concurrent part uploads.

### Trust boundary: four kinds of evidence

The hardening turns on keeping these distinct, never conflating one for
another:

1. **Evidence calculated from the local source.** Only ever produced by
   hashing what is actually read from disk — reused for a retry against a
   pre-existing object (`_hash_path`), and inherent in (2) for a fresh
   upload. Trusted for what the *producer* observed, never alone sufficient
   to publish `_COMPLETE.json`.
2. **Evidence calculated from exactly the bytes streamed during upload.**
   `_HashingReader` (single PUT) and the per-part + whole-object hashing loop
   in `_upload_multipart` compute SHA-256 and byte count from what is
   actually handed to the HTTP layer, in the same pass as the upload — never
   from a separate stat()/read() beforehand. This is what makes the declared
   evidence in the marker structurally unable to describe bytes the upload
   did not send.
3. **Storage acknowledgement/checksum.** A server-computed value, returned by
   the backend itself, describing what it durably received — never trusted
   merely because the client claims it matches; always independently
   compared against (2) (or, for a pre-existing object, against (1)) before
   being trusted. This is the leg that used to require re-downloading the
   object; see below for exactly when it can stand in for that download and
   when it cannot.
4. **Independent consumer-side verification.** `reporting_platform.ingest.
   transport.validate_transport` (the RPL Delivery consumer) always performs
   a full `GetObject` + rehash via `storage.verify_object_evidence` —
   deliberately UNCHANGED by this work. It is a genuinely independent check,
   run once per accepted Transport rather than once per retry, and its cost
   is not the problem this hardening addresses (see "Producer vs consumer
   verification" below).

`storage.verify_object_evidence()` — the original, unconditional GET +
rehash — is kept byte-for-byte as it was, and is what (4) uses, and what (3)
falls back to whenever a cheap confirmation is not available. Nothing that
used to be strongly verified is now weakly verified; the change is that a
strong-and-cheap confirmation is used instead of a strong-and-expensive one
whenever the backend actually supports it.

### Checksum strategy, confirmed against real MinIO, not assumed

Investigated directly against this platform's own MinIO
(`RELEASE.2024-09-22T00-33-43Z`), not read off the S3 API reference, because
"S3-compatible" does not mean "every optional feature implemented the same
way" (see "S3-compatible storage considerations" below):

- `put_object(..., ChecksumAlgorithm="SHA256")` returns a `ChecksumSHA256`
  that is **exactly** the plain full-object SHA-256 for a single-PUT object —
  confirmed byte-for-byte, not assumed. The same value is retrievable later,
  with no request body, via `head_object(..., ChecksumMode="ENABLED")`. For a
  single-PUT object, this checksum can always stand in for a full download.
- A **multipart** object's checksum is a **composite** hash — confirmed as
  `base64(sha256(concat(raw part SHA-256 digests)))` + `-<part count>` — and
  is *never* the plain full-object SHA-256, no matter how it is requested.
  `ChecksumType="FULL_OBJECT"` (AWS's newer full-object multipart checksum)
  is silently ignored by this MinIO version; the composite form is returned
  regardless. A composite checksum can stand in for a download **only**
  within the same upload that produced it (the part boundaries are known);
  it can **never** stand in for re-verifying a *pre-existing* multipart
  object later, because the part boundaries used to create it are not
  recorded anywhere and a later verifier has no way to reconstruct the
  composite formula.
- **Completing a checksummed multipart upload requires repeating each part's
  own `ChecksumSHA256` in its `Parts` entry of `CompleteMultipartUpload`** —
  `ETag` alone, sufficient when no checksum was requested at all, is *not*
  sufficient once checksums are in play: MinIO rejects the completion with
  `InvalidPart` otherwise. This was found by running the multipart path
  against real MinIO, not by reading documentation — the FakeS3 test double
  did not (and, per its own stated design, deliberately does not try to)
  model this, so it was invisible to the mocked test suite alone; it is now
  both fixed in `storage.py` and enforced by `FakeS3` itself, so a
  regression fails a mocked test too.
- `IfNoneMatch: "*"` is honoured on `CompleteMultipartUpload` exactly as on
  `PutObject` — confirmed with a racing-writer test against real MinIO (see
  "Multipart create-only race semantics" below).

This yields the capability/fallback model in `storage._confirm_durable()`
and `_try_cheap_verify()`:

```text
confirm durable storage matches declared evidence
        |
checksum_mode == "full_download"?  --yes--> GET + rehash (original path)
        |no
HEAD with ChecksumMode=ENABLED: does the object have a USABLE checksum?
  (present, and not a composite/multipart checksum for a PRE-EXISTING
  object whose part boundaries are unknown here)
        |                                    |
       yes                                   no
        |                                    |
compare against declared SHA-256        GET + rehash (fallback --
  (base64<->hex conversion only;          correct for ANY backend,
  no network transfer of the object)      including one that has never
        |                                  been tested against this code)
mismatch --> TransportEvidenceError (either path, identical guarantee)
```

`UploadTuning.checksum_mode` (`"auto"`, the default, or `"full_download"`)
is the explicit control this exposes — see "Configuration" below. There is
no third "require checksums" mode: a backend that cannot prove itself
cheaply always still gets the original, always-correct verification, never a
refusal to publish. That is the deliberate choice this hardening makes —
efficiency is opportunistic, correctness is not conditional on it.

### Multipart upload

`storage._upload_multipart()` replaces a single `put_object()` for any file
at or above `UploadTuning.multipart_threshold` (default 8 MiB, boto3's own
`s3transfer` default — not a number chosen for local MinIO):

- **Streaming, bounded memory.** The source file is read sequentially in
  `multipart_part_size`-byte chunks (default 8 MiB); a whole-object SHA-256
  is updated across every chunk in read order, giving the same "hash exactly
  what was streamed" guarantee as the single-PUT path, just spanning part
  boundaries. Up to `multipart_max_concurrency` parts (default 4) are
  in-flight at once, via a sliding window of submitted `ThreadPoolExecutor`
  futures — reading blocks once that many parts are outstanding, so memory
  is bounded by `part_size × concurrency`, never by file size. The whole
  file is never held in memory at once.
- **Verification without a download.** Each `upload_part` call requests a
  server-computed per-part SHA-256 (when `checksum_mode` allows it) and
  compares it to this process's own hash of exactly the bytes it sent for
  that part — proof the network transit of every part was intact. After
  `CompleteMultipartUpload`, the server's returned COMPOSITE checksum is
  compared against the same formula computed locally from those same part
  digests — proof S3 assembled exactly, and only, the parts this process
  sent, in order, with nothing substituted or dropped. Either check failing
  (or checksums being unavailable) falls back to the unconditional full GET
  + rehash rather than trusting a partial result.
- **Failure and abort.** Any exception during the read/upload loop (a part
  upload failing, a network error) aborts the multipart upload
  (`abort_multipart_upload`, best-effort — its own failure is logged, never
  raised, since the caller is already unwinding a real error) before
  re-raising. A part-count that would exceed S3's 10,000-part limit for the
  configured `multipart_part_size` is rejected up front, before any network
  call, naming the fix (raise `multipart_part_size`).
- **Zero/small files never take this path** — `multipart_threshold` gates it,
  so a control file (routinely near-zero bytes) always uses a single PUT.

### Multipart create-only race semantics

The critical invariant — *a TransportID must never silently replace
different evidence already stored under the same object key* — is preserved
for multipart exactly as for single PUT, via the same mechanism plus one
independent, always-on backstop:

1. **Primary mechanism: `IfNoneMatch: "*"` on `CompleteMultipartUpload`.**
   Confirmed against real MinIO with a racing-writer test: two multipart
   uploads racing to complete the same key, the second gets
   `PreconditionFailed` and the first writer's bytes are left untouched, byte
   for byte. This is the same conditional-write mechanism this codebase
   already relies on for the marker itself and for single-PUT source
   objects, extended to the multipart completion call, and it is what a
   modern S3-compatible backend is expected to support — AWS S3 added
   conditional writes for both `PutObject` and `CompleteMultipartUpload`
   together in August 2024; this MinIO release (September 2024) already
   implements it.

   For the interleaving in the hardening brief —

   ```text
   Publisher A                    Publisher B
   HEAD -> missing
                                  HEAD -> missing
   start upload
                                  start upload
   complete upload
                                  complete upload
   ```

   — both publishers upload every part independently (wasted work for the
   loser, not a correctness problem), then race on `CompleteMultipartUpload`.
   The winner's write proceeds; the loser gets `PreconditionFailed` and takes
   the same "lost the race" branch a single-PUT loser takes: confirm the
   object that is now there against exactly the bytes THIS process streamed,
   with no second local read. Identical bytes → the loser also succeeds,
   describing the SAME evidence (an idempotent-shaped outcome even though
   this was technically a race, not a retry). Different bytes → the loser's
   confirmation fails and `publish_transport_result` raises
   `TransportConflictError` — the object is left holding the winner's bytes,
   and the loser never gets to claim otherwise.

2. **Backstop, independent of whether (1) is honoured: the confirmation
   step never writes `_COMPLETE.json` for evidence it has not itself just
   reconfirmed against durable storage.** A backend that silently ignores an
   unrecognised `IfNoneMatch` header (rather than rejecting the request, the
   riskier failure mode for a conditional header — see "Do not assume a
   pre-upload HEAD alone provides atomic protection" below) would let two
   racing completions both "succeed", with whichever completes last
   physically holding the key. Each publisher still runs the SAME
   post-upload confirmation as the happy path, against the object as it
   ACTUALLY now stands: the process whose bytes lost — even though its own
   `CompleteMultipartUpload` call returned success — observes a durable
   object that does not match what it itself sent, and its confirmation
   raises `TransportConflictError` rather than proceeding to a false marker.
   The process whose bytes won observes a match and proceeds normally.

   This does **not** prevent the loser's storage cost/wasted upload, and it
   does **not** guarantee which of two genuinely-different concurrent
   publishes wins when the backend cannot enforce (1) — that is a real,
   documented limitation of any backend without atomic conditional writes,
   not something client-side code can close. What it does guarantee, on
   *any* backend, honouring conditional writes or not, is the actual
   invariant this contract makes: `_COMPLETE.json` is never published
   describing evidence that does not match what is durably stored. A
   pre-upload `HEAD` (used only to decide "reuse vs. upload", never treated
   as proof of anything) could never provide this on its own — only a check
   performed AFTER the write, against what actually landed, can.

### Source mutation protection

DCM asserts a source file is complete before invoking the publisher; the
publisher still defends against it changing during a long-running upload,
via `storage._guard_mutation()`, called after every upload (single or
multipart) and after every existing-object reuse:

- **Primary mechanism, unchanged in spirit from before this hardening:** the
  declared SHA-256/byte count are always derived from bytes actually
  streamed (see (2) in the trust boundary above), never from a separate
  read — so even an undetected mutation cannot make the marker describe
  bytes that were never sent.
- **Fail-safe guard, new:** the file is `stat()`-ed before publication
  starts and again after it finishes. `TransportSourceMutatedError` (a
  `TransportContractError`, mapped to CLI exit code 6) is raised if either
  disagrees:
  - the number of bytes actually read differs from the size observed by the
    initial `stat()` (the file grew or shrank while being read), or
  - a `stat()` taken after publication disagrees with the initial one in
    size or modification time (the file was touched during upload, even if
    the read itself happened to observe a consistent byte count).

  Metadata is explicitly **not** treated as an integrity proof on its own — a
  rewrite that preserves size and (an unusual, but possible) mtime would not
  be caught by this guard, which is exactly why the hash-what-you-stream
  property remains the thing that actually protects the declared SHA-256.
  This guard's job is narrower: fail loudly, before `_COMPLETE.json` is ever
  reached, on the ordinary ways a "complete" DFS file turns out not to have
  been.
- Either failure prevents `_COMPLETE.json` from being written for this
  attempt. Source objects already uploaded are left in place (unmarked
  evidence, exactly as any other failed-before-the-marker attempt) — a retry
  with the now-stable file re-hashes and either matches (marker gets
  written) or is treated as a conflicting retry.

### Producer vs consumer verification

The producer (this package) needs enough verification to safely publish
`_COMPLETE.json`: durable storage matches the declared evidence, established
via (2)+(3), falling back to (2)+GET+rehash. The consumer
(`reporting_platform.ingest.transport.validate_transport`) needs enough
verification to safely accept the Transport as immutable evidence going
forward, and performs its own full GET + rehash unconditionally — genuinely
independent of whatever the producer did, run once per accepted Transport
rather than on every producer-side retry. This division is deliberate, not
an oversight: the producer does not need to pay for the same expensive
verification twice when a cheaper, equally strong (for a single-PUT object;
scoped correctly for multipart, above) proof is available, but the consumer
gains nothing from trusting the producer's own checksum-based confirmation
and continues to prove the evidence itself from first principles. Neither
side's guarantee depends on the other.

### Very large object limits

Validated against the boto3/S3 APIs this package actually calls, not
assumed from local MinIO having no practical size limit of its own:

| Limit | Value | Enforced |
|---|---|---|
| Maximum single-PUT object size | 5 GiB | `UploadTuning.multipart_threshold` cannot exceed this (config-time) |
| Minimum multipart part size (all but the last part) | 5 MiB | by the backend, at `CompleteMultipartUpload` — deliberately NOT duplicated as a config-time floor, so a low `multipart_part_size` can exercise the multipart path against small test fixtures; a real too-small part size fails loudly against a real backend, naming the operation that rejected it |
| Maximum part size | 5 GiB | `UploadTuning.multipart_part_size` cannot exceed this (config-time) |
| Maximum part count | 10,000 | checked against the configured `multipart_part_size` and the actual file size before any network call, naming the fix (raise `multipart_part_size`) |
| Maximum object size (any method) | 5 TiB | checked before any network call |

### Abandoned multipart uploads

A process that dies mid-upload (crash, kill, lost connectivity) leaves an
**incomplete** multipart upload on the backend: parts already durably
stored, consuming storage, but no object visible at the target key (an
incomplete upload is invisible to `HeadObject`/`GetObject`/`ListObjectsV2` on
the key) and no `_COMPLETE.json`. This is never mistaken for a completed
source object — `storage.object_exists()` (a plain `HeadObject`) correctly
reports the key as absent, so a retry takes the normal "publish from
scratch" path, starting a fresh multipart upload under a new `UploadId`. The
abandoned one is not referenced by anything this package writes and cannot
be discovered from the Transport evidence alone.

This package aborts what it can, when it can (see "Failure and abort"
above), but a process that is killed outright has no opportunity to run that
cleanup. The durable backstop is a bucket **lifecycle rule** — every major
S3-compatible backend, MinIO included, supports expiring incomplete
multipart uploads after N days of inactivity (AWS: `AbortIncompleteMultipartUpload`;
MinIO: `mc ilm add`/`mc ilm rule add` with the equivalent expiry action). This
platform's local MinIO does not currently configure one; a production
deployment should, sized comfortably above the slowest expected upload
duration — for example:

```bash
mc ilm add local/lakehouse --expiry-days 3
```

(applied to whichever bucket `REPORTING_TRANSPORT_BUCKET`/`REPORTING_WAREHOUSE`
names; consult the specific backend's lifecycle documentation for the exact
incomplete-multipart-only action, since "expire objects after N days" and
"abort incomplete multipart uploads after N days" are configured
differently on some S3-compatible implementations).

### S3-compatible storage considerations

Nothing in this hardening assumes AWS S3 specifically, and nothing in it
assumes every S3-compatible backend behaves identically to this platform's
local MinIO:

- **Checksums are entirely opportunistic.** `checksum_mode="auto"` requests a
  server-side SHA-256 checksum on every relevant call, but never *requires*
  one: a backend that returns no `ChecksumSHA256` (does not implement the
  checksum extension, or silently drops the parameter) is treated exactly as
  "no usable checksum available", and the original, always-correct GET +
  rehash confirmation runs instead. There is no error path for "backend
  doesn't support checksums" — only a slower, but equally correct, one. This
  is distinct from — and does not by itself protect against — botocore's OWN
  independent default checksum behaviour; see "Dell ECS" immediately below.
- **`IfNoneMatch` conditional writes are not assumed universal.** Confirmed
  supported by real MinIO for both `PutObject` and `CompleteMultipartUpload`,
  and documented as AWS S3 behaviour since August 2024, but a backend that
  ignores the header rather than honouring it does not silently weaken the
  contract — see "Multipart create-only race semantics" above for exactly
  what still holds regardless.
- **Do not assume a pre-upload HEAD alone provides atomic protection**, on
  any backend. A `HeadObject` reporting "missing" is a point-in-time
  observation, not a lock; every atomicity guarantee this contract makes
  comes from a conditional WRITE (`IfNoneMatch` on the actual `PutObject`/
  `CompleteMultipartUpload`) and/or the post-write confirmation step, never
  from the pre-upload existence check by itself — which exists only to
  decide "reuse vs. upload", a performance/idempotency choice, not a safety
  one.
- **Multipart minimum/maximum part sizes and part counts are the numbers S3
  itself documents**, not a local-MinIO assumption — see "Very large object
  limits" above.

#### Dell ECS

Researched against ECS's own Data Access Guide (3.8.x, April 2024) and
corroborating third-party reports, not tested against a live ECS cluster —
**verify directly against the actual on-prem ECS before relying on this in
production**, the same "verify against the live stack" habit this repo
opens with. Three findings, most to least severe:

1. **botocore's own default checksum behaviour (independent of anything
   this package does) is reported to make Dell ECS reject writes outright.**
   Since botocore 1.36 (January 2025), `PutObject`/`UploadPart` carry an
   automatic CRC32 (or CRC64NVME) checksum trailer by default — regardless
   of this package's `checksum_mode`, which only controls whether *this
   package* additionally asks for a SHA-256 checksum. Apache Iceberg's own
   S3FileIO hardening (`apache/iceberg#17177`) names Dell ECS specifically:
   *"Some S3-compatible object stores (e.g. Dell ECS, GCS S3-compatible API)
   reject these headers, which breaks writes."* This is not a graceful
   degradation like the SHA-256 fallback above — it is a request failure.
   **Fixed here**: `storage.build_client()` now pins
   `request_checksum_calculation`/`response_checksum_validation` to
   `"when_required"` on any botocore new enough to support those `Config`
   keys, silently left alone on one too old to recognise them (which
   predates the default-on behaviour this suppresses, so there is nothing to
   protect against there). This stops botocore's *unrequested* default
   trailer without disabling this package's own explicit opt-in — `auto`
   mode still asks for a SHA-256 checksum on the calls it chooses to, and
   `when_required` still honours that explicit request.
2. **No evidence ECS implements the AWS "additional checksums" API
   (`x-amz-checksum-sha256`/`ChecksumAlgorithm`/`ChecksumMode`) at all.**
   The ECS 3.8.x S3 API reference documents `Content-MD5`-based integrity
   (`BadDigest`, `ContentMD5Missing`, `ContentMD5Empty`) throughout, and does
   not mention SHA-256/CRC32/`ChecksumAlgorithm` anywhere in its S3 section
   — the only "checksum" header it documents (`x-emc-wschecksum`) belongs to
   the unrelated legacy Atmos protocol. Consequence: `checksum_mode="auto"`
   is very likely a no-op on ECS (every confirmation transparently falls
   back to the full GET + rehash this package has always supported) rather
   than an error — the capability/fallback design absorbs this correctly —
   but the large-file re-download this whole hardening exists to avoid
   should be expected to still happen on ECS until confirmed otherwise.
   Given (1), **do not treat that as reason enough to leave `checksum_mode`
   at `auto`** without first confirming ECS does not error on the
   `ChecksumAlgorithm`/`ChecksumMode` parameters themselves (distinct from
   botocore's own automatic default, already handled) — `full_download` is
   the safer starting point for a first production rollout against ECS,
   moved to `auto` only after confirming (empirically, e.g. with
   `scripts/transport_integration_check.py` pointed at ECS) that it behaves.
3. **`IfNoneMatch` create-only semantics on `PutObject`/`CompleteMultipartUpload`
   are undocumented for ECS's S3 API**, one way or the other. ECS's S3 error
   table does define the generic `PreconditionFailed` (412) response, and
   `If-Match`/`If-None-Match` precondition handling is documented — but only
   in the guide's Atmos protocol section, not its S3 section, and ECS's
   documentation predates AWS's own August 2024 rollout of `If-None-Match: *`
   create-only semantics specifically for `PutObject`/`CompleteMultipartUpload`
   (as opposed to the older, narrower use of those headers for cache
   validation on GET). This is the single most important thing to verify
   directly before depending on this contract's create-only guarantee in
   production: run two processes racing a conditional write against the
   same key on the actual ECS cluster and confirm the loser gets
   `PreconditionFailed` rather than a silent overwrite. If ECS does not
   honour it, the "Multipart create-only race semantics" backstop above
   still prevents `_COMPLETE.json` from ever describing the wrong evidence,
   but it does **not** prevent one publisher's bytes from silently
   overwriting another's under that condition — a real risk, not merely a
   missed optimisation, and worth an explicit test before go-live.

Also noted, lower risk: `CompleteMultipartUpload` on ECS returns a literal
`ETag` of `"00"` rather than a composite hash (documented ECS behaviour,
differing from both AWS and MinIO). This package never treats ETag as a
digest of anything — see "why ETag is not used as the canonical digest"
throughout this document — so it does not by itself break anything here,
but it is a further signal that ECS's multipart completion response should
not be assumed to carry a usable checksum.

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

### Upload tuning

`reporting_transport.storage.UploadTuning` — multipart thresholds/
concurrency and checksum verification strategy — is deliberately a SEPARATE
config object from `StorageConfig`, resolved independently: a caller that
constructs its own `client`/`bucket` directly (every test in this repo does)
still gets these from the environment rather than silently always using
hard-coded defaults, since it never builds a `StorageConfig` at all.
`StorageConfig.upload` carries one when a full `StorageConfig` IS built.

| `UploadTuning` field | `REPORTING_TRANSPORT_*` | Default |
|---|---|---|
| `multipart_threshold` | `MULTIPART_THRESHOLD_BYTES` | 8 MiB (boto3's own `s3transfer` default) |
| `multipart_part_size` | `MULTIPART_PART_SIZE_BYTES` | 8 MiB |
| `multipart_max_concurrency` | `MULTIPART_MAX_CONCURRENCY` | 4 |
| `checksum_mode` | `CHECKSUM_MODE` | `auto` (`auto` \| `full_download`) |

`REPORTING_TRANSPORT_LOG_LEVEL` (default `INFO`) controls the CLI's stderr
operational logging — see "Progress/logging" below.

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
`--bucket`/`--s3-endpoint`/`--region`/`--received-prefix`. Upload tuning is
resolved from the environment (see "Upload tuning" above) or overridden with
`--multipart-threshold-bytes`, `--multipart-part-size-bytes`,
`--multipart-max-concurrency`, `--checksum-mode`. A normal DCM invocation
needs none of these — the defaults are production-sized and DCM never has to
understand multipart mechanics; they exist for tuning against a specific
backend or exercising the multipart path deliberately (as the tests and the
benchmark script both do, against small fixtures — see "Tests" and
"Benchmark / demonstration" below).

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
| 6 | Source file changed during publication | `source_mutated` |

"Newly published" and "already published" share exit code 0 deliberately: a
DCM scheduler checking `$? -eq 0` should treat an idempotent retry as success,
not a failure requiring alerting. The finer distinction is carried in
`status` for anything that wants it.

## Progress/logging

A large upload can run for several minutes; the CLI logs operational
progress to **stderr only** — stdout stays exactly the one final JSON
result, always, so DCM's parsing is unaffected by log verbosity. Controlled
by `--log-level`/`REPORTING_TRANSPORT_LOG_LEVEL` (default `INFO`):
transport ID, object key, upload method (single PUT vs. multipart), part
size/count, elapsed time and whether the confirmation used a storage
checksum or a full download are all logged at `INFO`; nothing per-chunk is
logged at any level below that. Never logged, at any level: source-file
contents, credentials, or anything else this package does not already print
in its final JSON result.

## Benchmark / demonstration

```powershell
docker compose exec -T airflow python -m scripts.benchmark_transport_upload
docker compose exec -T airflow python -m scripts.benchmark_transport_upload `
  --size-mb 200 --checksum-mode full_download   # contrast with the pre-hardening behaviour
```

Generates a synthetic file, publishes it through the same
`publish_transport_result` a real invocation uses, and reports file size,
upload method, bytes read locally, bytes uploaded, bytes **downloaded for
producer-side verification**, elapsed time, and the SHA-256 (local vs.
declared, confirmed equal). The number that demonstrates the fix is bytes
downloaded: ~0 in the default (`auto`) mode against this platform's MinIO,
equal to the full file size in `--checksum-mode full_download` — the
pre-hardening behaviour, reproducible on demand for comparison. Cleans up
everything it publishes, success or failure.

`scripts/transport_integration_check.py` is the companion MinIO integration
check (not part of `python -m tests.run`, which deliberately needs no stack
— see `tests/README.md` — and covers this package's logic against `FakeS3`
instead): small-file and multipart publish, idempotent retry, conflicting
retry, and a real two-publisher multipart create-only race, all against real
MinIO. It is how the `InvalidPart`/composite-checksum behaviour above was
actually found, and it exists so that class of bug — real only against a
real backend, invisible to a mock — has a repeatable check going forward:

```bash
docker compose exec -T airflow python -m scripts.transport_integration_check
```

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
    storage.py     StorageConfig/UploadTuning, the boto3 client builder
                   (standard credential chain only), and ALL S3 mechanics --
                   single PUT vs. multipart, checksum capability/fallback,
                   create-only races, source mutation guard -- behind
                   publish_object() and verify_object_evidence(). publisher.py
                   expresses publication semantics; storage.py is where every
                   boto3-specific detail lives, per "Storage abstraction" in
                   the hardening brief this section implements.
    publisher.py   publish_transport() -- the one publication algorithm,
                   used by both the local simulator and the CLI
    cli.py         argument parsing, JSON output, exit-code mapping,
                   stderr operational logging
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
