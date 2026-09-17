# Sample-driven feed onboarding: review and implementation handoff

Reviewed 2026-09-17 at commit `b191feaae68e909d5324708c1a537cfc7a671e24`.
Scope: unclaimed deliveries, sample inspection, feed configuration, inbox,
control checks, normalization, ingest, and generated dbt datasets.
Phases 0 and 1 were implemented in this checkout on 2026-09-17. The remaining
phases are a bounded plan; the implementation status and validation evidence are
recorded below.

## Recommendation

Keep the ingestion, registry, provenance and write-audit-publish foundations.
Replace the onboarding portion of the Feeds UI with a sample-driven workbench,
backed by a reusable service that can also run from a command line. Keep the
existing Feeds and Arrivals screens for operating established feeds and make
the existing configuration form an advanced editor.

The intended result is a **tested feed package**, rather than a filled-in form:
feed YAML, raw source definition, prepared dataset model, quality tests,
documentation and a validation report tied to the exact samples and definition.
Users should supply data and control files together, answer business questions,
see the interpreted values, and obtain a reproducible configuration.

Do not replace Spark, dbt, Iceberg, Nessie or the inbox as part of this project.
Do not automatically invent reporting joins, financial rules or SCD2 semantics.
A documented, typed prepared dataset is the initial reusable output.

## Evidence and limits

The other machine's original supplier samples, exception text, installed images
and checkout revision were not available. The fixtures below reproduce the
demonstrated parser failures, but do not prove the cause of that incident.

Attempted the existing targeted test modules for sniffing, controls, form
round-tripping, normalization, conformance and inbox routing. The host lacks
`ruamel.yaml` and `duckdb`; the run produced dependency failures and aborted
when importing the sniffer. A separate control-format run confirmed the missing
`ruamel` dependency. These are not evidence of application regressions.
Initially Docker was unavailable. After the user started Docker Desktop, its
running containers proved to be mounted from a different checkout,
`C:\Users\richa\source\repos\juno-mod-local`. To avoid testing that code or
altering that deployment, the current checkout was mounted read-only into a
disposable, network-disabled container using the available
`reporting-platform-feed-ui` image.

**Executed baseline: 230 passed, 2 failed.** Both failures are in `test_sniff`:
`test_cp1252_specific_bytes_fall_through_as_low_confidence` and
`test_bytes_invalid_in_every_fallback_raise_cleanly`. Both raise:

```text
Invalid Input Error: The CSV Reader does not support the encoding: "cp1252"
```

The error lists UTF-8, UTF-16 and Latin-1 as supported and suggests installing
the encodings extension. A separate inspection confirmed DuckDB **1.5.5** (the
repository's pinned version), with `encodings` neither installed nor loaded.
This is a reproduced deployment dependency gap, not just version speculation.
The source sniffer contained no extension initialization. A fresh image was then
built from this checkout and reproduced the same CP1252 dependency failure plus
the BOM, invalid UTF-8 replacement and unterminated-quote behavior captured by
the new fixtures.
Registry-unavailable warnings occurred in the isolated tests, which intentionally
had no database connection. No dbt build, production ingest or browser walkthrough
was performed, and the other machine's specific failure remains unconfirmed.

Reproduce the baseline from this checkout on Windows (substitute its absolute
path if needed):

```powershell
docker run --rm --network none --entrypoint python --mount "type=bind,source=C:\Users\richa\reporting-platform-local,target=/review,readonly" --workdir /review reporting-platform-feed-ui -m tests.run test_sniff test_control_format test_control test_delivery_form test_registry_roundtrip test_normalize test_conform test_inbox
```

Review ratings: correctness needs substantial work at onboarding boundaries;
maintainability has good shared primitives but a tightly coupled form;
performance needs bounded sample handling; security was reviewed only within
this scope and is not certified. The UI is intentionally unauthenticated
(`docs/FEED-UI.md`); shared deployment needs a separate security review.

## Current path

1. `ingest/inbox.py` routes known filenames and moves unknown ones to `.rejected`.
2. `/api/unclaimed` lists these files. Sniffing passes one filename and its bytes
   to `ingest/sniff.py`; ordinary upload sniffing also accepts one file.
3. DuckDB proposes dialect, encoding, columns, broad types and single-column
   key candidates. ZIP handling inspects one data member and can pair controls
   inside the ZIP. The UI translates the proposal into a large configuration form.
4. Create writes the feed registry first, then raw source, prepared SQL and tests.
   dbt parse runs after writing. Edit changes the registry only.
5. The inbox's conformance stage derives delivery identity and preserves/promotes
   control bytes. Normalization finds controls and produces a manifest. Plain
   files are passed through, not transcoded into a canonical text format.
6. Spark reads strings into raw; dbt types and deduplicates them into prepared.
   A separate feed-test action builds existing raw data on a disposable Nessie
   branch. It does not validate an unsaved draft from original sample bytes.

Useful foundations to retain: one control parser shared between gates; regex
and delimited controls; configuration validation and quoted YAML serialization;
manifest-based read options; immutable landing evidence; provenance macros;
branch-isolated dbt tests; extensive existing pure/config tests.

## Prioritized findings

### P1: one encoding setting crosses readers and file roles without an end-to-end check

Evidence: `ingest/sniff.py:158,228`, `common/context.py:934`,
`ingest/conform.py:247,789,843`, `ingest/normalize.py:183`,
`ingest/ingest_feed.py:279`, `ui/feeddata.py:145`.

The sniffer tries DuckDB encodings; configuration validation only establishes
that Python knows the codec; Spark receives that name as a CSV option. Those
checks do not prove equivalent decoding on every engine. Control files inherit
the data file's encoding even when the sender used a different encoding.
Several Python paths use `errors="replace"`, silently replacing undecodable
bytes before counting rows, previewing or matching controls.

**Reproduced:** CP1252 fallback fails on DuckDB 1.5.5 in the available image,
because the optional encodings extension is absent. The message says `encoding`,
not `encoded`, so `_is_encoding_failure` does not recognize it and it escapes
instead of producing the intended diagnostic. Prioritize this in phase 1.
Prefer strict Python decoding to a temporary UTF-8 inspection stream, retaining
the original encoding in the proposal; alternatively bundle and explicitly load
a tested extension at image build/runtime. Do not depend on an implicit network
download on the user's first upload. Verify any chosen extension's accepted
encoding names rather than assuming `cp1252` is an accepted alias.

Concrete failure path visible in code: UTF-8 BOM bytes decoded with `utf-8`
remain U+FEFF. A delimited control's first header is then not the configured
column name; an anchored regex can fail at its first line too. DuckDB's BOM
handling does not establish that the control parser behaves identically.
The sniffer also labels Latin-1 as high confidence despite the ambiguity of
single-byte encodings. Its retry decision depends on the exception containing
the word `encoded`; other encoding-related failures bypass fallback.

Fix: a shared strict byte-decoding contract, BOM handling, independently
specified data/control encoding, explicit uncertain detection and manual
override followed by revalidation. Never replace bad bytes silently. Test
support against the actual pinned Python/DuckDB/Spark environment.

### P1: control rules can be saved without proving they read the supplied files

Evidence: `ui/app.py:173,301,387,407`; `common/context.py:626`;
`ingest/control.py:77`; `ui/static/index.html:628,1231,1338`.

Configuration validation checks regex syntax and required named groups, not
whether the user's real control file matches. Runtime uses `re.search` without
implicit multiline mode. A syntactically valid `^ROWS=...$` need not match a
line inside a multi-line file. The error attributes mismatch to upstream format
change even when the initial configuration was never correct.

ZIP member control suggestions do read candidates back through the shared
parser, which is good. That protection does not cover arbitrary manually edited
rules or the normal separate CSV-plus-control onboarding case: the API accepts
one sample, not a delivery bundle. Field candidates for ZIPs are presented as
notes requiring manual transfer/interpretation.

Fix: pair samples explicitly; show extracted date/count/checksum next to observed
values; run the real shared parser on every selected pair before activation.
Prefer column selection for delimited controls and add structured key/value
mapping. Keep regex as an advanced option with visible flags, captures, match
counts and errors. Distinguish decoding, extraction, invalid value, mismatch
and missing control instead of blaming every failure on upstream drift.

### P1: inferred CSV behavior is not fully carried into production reads

Evidence: `ingest/sniff.py:228,485`; `ingest/ingest_feed.py:279`.

The proposal retains delimiter, quote, header and encoding, but not the full
reader contract inferred by DuckDB. Spark uses permissive reading and does not
enable multiline CSV. Python's CSV counting can treat a quoted embedded newline
as one row, while Spark's configured reader does not promise that interpretation.
Consequences include control count failures or wrongly shaped data after a
successful sniff. Date-format information is also discarded before scaffolding.

Fix: define the supported dialect explicitly, including escape behavior,
multiline records, preambles and null handling. Carry supported options through
the manifest, reject unsupported formats clearly, and compare production-reader
results with preview/counts. A permissive parse must not silently qualify a draft.

### P1: saving and editing do not deliver a consistent, validated package

Evidence: `ui/app.py:173-216,247-276`; `ui/scaffold.py:297,440`.

Create activates registry configuration before scaffolding and parsing finish.
Scaffolding intentionally reports partial writes. Edit updates only the registry;
re-scaffold skips existing models/tests. Changing a column type or schema can
therefore leave old SQL in place even though the form shows the new choice.
Existence-based scaffold status cannot establish consistency.

Fix: draft/validate/render before activation; stage all generated artifacts and
show a diff. Record hashes and generator version. Apply with concurrency checks,
recoverable journaling and an activation boundary that readers respect. Preserve
hand-edited SQL and surface an explicit reconciliation diff for existing feeds.
Do not promise filesystem atomicity across several files without implementing it.

### P1: the generated dataset has insufficient type fidelity and quality gates

Evidence: `ui/scaffold.py:182,306`; `ingest/sniff.py:42,228`;
`dbt/macros/engine.sql:65-75`.

Every decimal becomes DECIMAL(18,2), every integer INT; dates support only two
formats. DuckDB integer inference can represent a wider range, decimal scale
can be lost, and numeric-looking identifiers can lose leading zeroes. The
sniffer's date classification does not carry its format into the model.
Safe casts yield NULL, but generated tests cover keys/date/provenance/uniqueness,
not conversion failures for every typed column. Thus a non-key field can become
NULL without failing the generated tests; rounding need not produce NULL at all.

Fix: an explicit column contract with semantic role, target type, precision/scale,
date/timestamp format, null tokens and conversion policy. Profile candidate
conversions and show changed/rejected examples. Require confirmation for keys,
dates and identifiers. Generate conversion-failure tests independently of source
nullability; reject unsupported precision instead of silently rounding it.

### P2: archive and unclaimed flows need explicit selection and completion

Evidence: `ingest/sniff.py:676`; `ui/static/index.html:1338-1392`;
`docs/todo/19-sniffer-can-propose-a-marker-file.md`.

ZIP schema inference uses the first candidate member. Unpaired marker files can
win selection, and heterogeneous members are not all profiled. Unclaimed entries
operate individually; after registering a matching feed the user is told to drop
the file into the inbox again. Registration is disconnected from proving and
replaying the original delivery bundle.

Fix: classify/select members, report schema differences, never silently combine
different schemas, and offer deliberate replay of the original bundle after
activation with duplicate/version checks and visible results.

### P2: sample work needs resource limits and cleanup

Evidence: `ingest/sniff.py:144,205,267,676,756`; `ui/app.py:388`.

Uploads and ZIP members are read into memory; key discovery scans the full file;
`read_blob` retrieves content before slicing a small prefix. Temporary sample
files use `delete=False` without cleanup and proposal connections are not
explicitly closed. Large samples and repeated attempts can exhaust resources.
Source headers interpolated into key-profiling SQL are not identifier-escaped,
so a valid header containing a double quote can break inspection.

Fix: bounded profiling, streamed uploads, ZIP expanded-size/member limits,
temporary-directory/connection context managers and correct identifier quoting.
Label sampled results; a sample is not proof of global uniqueness.

## Proposed workflow

1. **Select examples:** upload multiple files or select an unclaimed bundle;
   retain original names and hashes; classify data, controls, containers and
   excluded members. Support several dates/redeliveries where available.
2. **Confirm interpretation:** preview decoded text and rows; override encoding
   independently per role; confirm delimiter/header/quoting and pairing.
3. **Map delivery facts:** choose business date, version, row count and checksum
   fields from previews. Show every extraction and comparison. Missing mappings
   are explicit unresolved decisions, never silent skipped checks.
4. **Define the dataset:** name, description, column meanings, types, business key,
   snapshot/append intent, null policy and quality rules. Start with the existing
   supported snapshot template; do not silently infer behavior from key count.
5. **Validate and preview:** run original bytes through the production parsing
   contract, then isolated raw ingest and dbt build; show counts, conversions,
   duplicate handling, schema and provenance. Invalidate results after any edit.
6. **Review and activate:** show the complete artifact diff and validation report;
   save a consistent package, then explicitly replay selected examples through
   the normal delivery path. Show waiting/failed/loaded/prepared outcomes.

Use one onboarding service beneath UI and CLI. The first read-only operation is
now implemented as `/api/samples/diagnose`: it accepts paired data/control
samples, hashes them, runs the shared production parser, and reports extracted
and observed values without writing configuration or activating a feed. The
remaining operations are proposed: create draft, attach samples, update mappings,
validate, render, activate and replay.
Persist drafts outside the active feed registry; keep sample bytes out of Git
unless deliberately sanitized into test fixtures. Reports should identify the
definition hash, sample hashes, engine versions and validation scope.

## Implementation sequence and acceptance gates

### 0. Reproduce and establish a baseline

Obtain sanitized original data/control pairs, their raw bytes, exact failing
regex/YAML, error logs and the other machine's revision/image versions. Establish
the supported test environment and run the existing targeted modules. Add
regressions for observed failures before changing behavior. No need to wait for
those examples to implement independently demonstrated defects above.

Gate: a recorded baseline and failures classified by stage; no claims that an
environment import failure proves an ingest defect.
**Completed 2026-09-17.** The network-disabled baseline was 230 passed and 2
failed, both from DuckDB 1.5.5 rejecting CP1252 without its optional encodings
extension. A fresh image built from this checkout reproduced those failures and
the new byte-level fixtures reproduced BOM leakage, replacement of invalid UTF-8
and acceptance of an unterminated quoted record. The host Python environment was
not treated as evidence because it lacks the repository's DuckDB and ruamel
dependencies. The unavailable supplier samples and second-machine runtime remain
an explicit evidence gap.

### 1. Fix parsing contracts before redesigning the UI

Implement strict shared decoding with BOM and separate control encoding; preserve
legacy defaults when new fields are absent. Extend validated dialect/manifest
options and align counting, preview and Spark reads. Add structured control
extraction and sample-backed diagnostics; retain old regex configuration.

Gate: fixtures cover UTF-8/BOM, UTF-16 LE/BE, Latin-1, CP1252, mixed data/control
encodings, invalid bytes, CRLF/LF, quoted delimiters/newlines, headerless files,
multiline regexes, delimited controls, zero/mismatched counts, missing controls
and wrong checksums. Every supported case produces the same interpretation at
inspection and ingest; unsupported cases fail before publication.

**Completed 2026-09-17.** A shared parser now performs strict decoding, removes
only a matching Unicode BOM, validates the supported Python-to-Spark encoding
mapping, and carries `escape_char`, `multiline`, encoding and headerless columns
in parser-contract-v2 manifests. Data and control encodings are independent.
Preview, row counting, conformance, normalization and Spark ingestion use the
same validated contract; v1 manifests retain their prior defaults. Spark uses
FAILFAST for v2, and malformed bytes or records are refused before publication.
The existing write-audit-publish branch/merge sequence is unchanged.

Controls retain regex and delimited formats and add structured `key_value`
reading through the existing shared parser. Diagnostics report decode errors,
match counts, regex flags/captures, extracted values, observed row count/checksum
and comparison status. The service is read-only and bounded to 8 MiB per sample.

Validation results:

* Targeted pure suite: **248 passed, 0 failed**.
* Full pure suite: **597 passed, 0 failed, 14 skipped** in the container. The
  initial run exposed a stale assertion that expected exactly two SCD2 models
  although `main` already contains three. The check now discovers all SCD2
  models and refuses an empty set, so every current and future model is checked.
  The 14 repository-text checks skipped because the runtime image does not
  contain the repository root; their host run was **15 passed, 0 failed**.
* Real Spark 3.5.3 validation passed 14 isolated cases: UTF-8, UTF-8 BOM,
  UTF-16/LE/BE, Latin-1, CP1252 including punctuation, headerless input,
  backslash escaping, pipe delimiters, zero rows, null/empty fields and ASCII.
  Invalid UTF-8 was refused before Spark. The run used a unique object prefix
  and throwaway Nessie branch, exercised schema reconciliation, left `main` at
  the same hash, and removed its branch and objects.
* JavaScript syntax, Python compilation and whitespace checks passed.

The gate is met for the supported parser contract. A full dbt build and normal
production replay were deliberately not run because phase 1 validates parsing
without publishing or modifying the live stack.

### 2. Build draft validation and consistent artifact generation

Introduce the draft model/service and sample bundles. Refactor scaffold writers
into pure renderers plus an apply step. Add explicit column contracts and
conversion tests. Stage dbt files and validate without making the feed active.
Implement stale-draft/concurrent-edit detection and recovery after interrupted
apply. Design the activation marker/reader contract before wiring save buttons.

Gate: injected write/parse failures cannot expose an active half-configured feed;
editing types produces the appropriate model/test diff; user SQL is preserved;
reruns are idempotent; existing feeds still load with unchanged semantics.

**Next bounded implementation step:** persist the existing sample diagnostic as
a draft/sample-bundle record keyed by definition and sample hashes, then split
artifact generation into a pure render operation and an explicit apply boundary.
Do not activate registry configuration in this step. Acceptance is a stable
rendered diff that can be revalidated after restart and is invalidated whenever
the definition or either sample hash changes.

### 3. Prove ingestion and the prepared dataset together

Reuse production functions with explicit isolated destinations and Nessie refs.
The current feed-test creates a branch from main and reads existing raw; extend
the harness to ingest the draft's sample bytes on that branch too. Isolate object
prefixes, registry records, dbt artifacts and scheduler effects as well as table
state. Do not trigger production publication while validating a draft.

Gate: a successful isolated ingest plus dbt build/test, result preview and
validation report; no main merge or production asset event. Test redelivery,
duplicates, removed keys and malformed rows. Verify a later normal replay matches
the validated interpretation, with expected delivery/provenance differences.

### 4. Add the guided UI and CLI, then migrate documentation

Implement the six steps above over the service. Route Unclaimed and New Feed
into it; keep advanced editing and operating views. Add accessible progress,
field-local errors, resumable drafts and a replay action. CLI exports the same
package/report so onboarding can be reproduced on another clone.

Gate: a clean-clone walkthrough on a second machine takes a real paired sample
to a tested prepared dataset without editing regexes, manually converting bytes
or writing dbt SQL for the standard case. Advanced regex remains available.
Update FEED-UI, ADDING-A-FEED, QUICKSTART and DELIVERY-SHAPES; reconcile existing
todo items 13, 18 and 19 against actual current behavior rather than assuming
all historical tickets remain open.

## Storage decision

Preserve original bytes and checksums as evidence. Raw remains strings with
delivery provenance; prepared Iceberg tables are the typed reusable dataset.
Canonical UTF-8 staging is an implementation option, not a reason to rewrite
landing objects. If adopted, generate versioned derived ready objects and record
source/output hashes, original encoding, transform version and parser contract.
Validate supplier checksums against original bytes, not transcoded output.
Do not change retention policies as a side effect of this onboarding work.

## New-session prompt

> Read CLAUDE.md, docs/QUICKSTART.md, docs/ARCHITECTURE.md and
> docs/FEED-ONBOARDING-REMEDIATION.md. Implement phases 0 and 1 first, preserving
> existing feed behavior and write-audit-publish. Check the current checkout and
> runtime before relying on the review's line numbers. Establish the targeted
> test baseline, reproduce encoding/control failures with fixtures, then align
> strict decoding, BOM handling, data/control encodings and CSV reader options.
> Add sample-backed control diagnostics using the existing shared parser.
> Do not start with a frontend rewrite or bypass control checks. Run the pure
> tests and verify supported cases on the real Spark stack using isolated data
> and a throwaway branch; report unavailable checks honestly. Update this plan
> with results and the next bounded implementation step.
