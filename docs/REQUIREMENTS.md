# Requirements index

`REQ-nnn` identifiers appear in 28 files across this repository — module
docstrings, DDL comments, DAG task docs, `feeds.yml`, `retention.yml`, the
tests, `CLAUDE.md` and `docs/DECISIONS.md`. Until this file existed, none of
them said what the requirement *was*. A citation you cannot resolve is a
citation nobody checks.

**This index is reconstructed from the implementation, not transcribed from an
upstream requirements document.** Each statement below is derived from the code
and comments that cite the ID. Where the citations pin the requirement
precisely the statement is exact; where they only constrain it, the row says
so in *Confidence*. If the original wording exists somewhere, this file should
be corrected against it — the IDs and groupings are certainly right, the
phrasing may not be.

## How to read the tables

| Column | Means |
|---|---|
| **Requirement** | What the platform must do. |
| **Where it lives** | The code that implements it. A requirement with no implementation column is not built — the table says so explicitly rather than omitting the row. |
| **Proved by** | The test that fails if it breaks. `—` means it is verified by running the stack, not by `python -m tests.run`. |
| **Confidence** | `stated` — the citation gives the requirement in words. `derived` — reconstructed from what the code does and why. |

The numbering blocks are:

| Block | Subject |
|---|---|
| **100** | What arrived: the delivery record |
| **200** | What was promised: lateness and supersession |
| **300** | What we believed: knowledge time and provenance |
| **400** | What was published: the run record |
| **500** | The as-at date's lifecycle |
| **600** | Evidence retention |
| **700** | Reclamation and reproducibility |

---

## 100 — What arrived: the delivery record

> The platform must be able to answer "did they send it, and what was in it?"
> without listing object storage by hand.

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-100** | Every delivery the platform accepts is recorded: one row carrying its COB date, arrival time, size, checksum, the name the upstream used, and the column contract it was read against. | `registry/deliveries.py` (`observations()`, `register()`) | `test_registry.py`, `test_registry_roundtrip.py` | stated |
| **REQ-101** | The delivery record is an **index, not a ledger**: it holds observations and no verdicts, and it is rebuildable from object storage by the same code that writes it. | `registry/deliveries.py` (`reconcile()` is the authority), `registry/db.py` `registry.delivery` | `test_registry.py::…observations only` | stated, *as amended* |
| **REQ-104** | A delivery's checksum is measured once and never recomputed — from the `.meta.json` sidecar, else the object's ETag, else by reading it (multipart uploads only). A row already registered is never re-hashed. | `registry/deliveries.py`, "Where the md5 comes from" | `test_registry_roundtrip.py` | derived |
| **REQ-106** | A **refused** delivery is evidence too: its bytes go to `quarantine/`, a row goes to `registry.rejection`, and the rejection date is carried in the object key because a quarantined file usually has no parsable name. | `ingest/inbox.py::_quarantine()`, `registry/rejections.py::quarantine_quietly()`, `retention/quarantine.py` | `test_registry.py`, `test_inbox.py` | stated |

**Not cited anywhere:** REQ-102, REQ-103, REQ-105. Either they do not exist or
nothing in this repository implements them. Do not assume the block is
contiguous.

Reasoning: [`DECISIONS.md#the-registry-records-observations-not-verdicts`](DECISIONS.md#the-registry-records-observations-not-verdicts),
[`#quarantine-is-where-a-refused-delivery-goes`](DECISIONS.md#quarantine-is-where-a-refused-delivery-goes).

---

## 200 — What was promised: lateness and supersession

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-201** | A feed may declare the **wall-clock time** by which its upstream has committed to deliver. Deliveries that arrived after it are reported. The deadline is that time on the day *after* the COB date. A feed that has made no promise is skipped, not defaulted to midnight. | `common/context.py::Feed.expected_by`, `monitoring/lateness.py` | `test_retention_classes.py` | stated |
| **REQ-202** | How a later delivery relates to an earlier one is **declared, not assumed**. `full_snapshot` is the only built mode and the default; `delta_append` and `correction` are named in the requirement and refused at load with the reason. | `common/context.py` (`supersession:`, `NOT_BUILT` set), `dedupe_rank` | `test_supersession.py` | stated |

The refusal *is* the deliverable for the unbuilt modes: a delta feed deduped as
a snapshot silently loses every key its newest file omits, and the row counts
still look plausible.

Reasoning: [`#lateness-is-a-wall-clock-time-not-a-duration`](DECISIONS.md#lateness-is-a-wall-clock-time-not-a-duration),
[`#supersession-is-declared-not-assumed`](DECISIONS.md#supersession-is-declared-not-assumed),
[`#delivery-expected-not-completeness`](DECISIONS.md#delivery-expected-not-completeness).

---

## 300 — What we believed: knowledge time and provenance

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-300** | "What did we believe on date X" must be answerable — a build restricted to the knowledge available at a stated time. | `known_as_of()` in `dbt/macros/engine.sql`, driven by the `knowledge_time` dbt var | — (run it) | stated |
| **REQ-301** | That answer comes from **the models that already exist**, not a parallel set. As-of is a var; it compiles to `1 = 1` when unset, and it refuses an incremental run. | the same models, `--vars '{knowledge_time: …}'` | — (run it) | stated |
| **REQ-303** | Raw must distinguish "the upstream did not supply this column" from "the upstream supplied it empty". | `common/context.py` (the declared column contract), `_extra_columns`, NULL-fill on absence | `test_raw_schema.py` | stated |
| **REQ-304** | Every raw row carries the provenance of the delivery it came from: `_delivery_id`, `_received_at`, `_schema_version`, `_source_system`. | `ingest/ingest_feed.py` (provenance columns), `source_provenance()` in `dbt/macros/engine.sql`, `ui/scaffold.py` | `test_provenance.py` | stated |

**REQ-302 is not cited anywhere.**

Two operational consequences that are easy to miss and are not in the
requirement text:

- The provenance columns are **added, never backfilled**. History reads NULL,
  so an as-of query falls back to `_source_file` via `delivery_ref()`.
- The migration is **lazy**. A feed that has not delivered keeps the old
  schema, and every prepared model then fails — not just that feed's. Run
  `ingest.migrate_raw` before the next ingest when deploying a new provenance
  column.

Reasoning: [`#as-of-is-a-var-not-a-second-model`](DECISIONS.md#as-of-is-a-var-not-a-second-model),
[`#provenance-is-added-not-backfilled`](DECISIONS.md#provenance-is-added-not-backfilled),
[`#delivery-ref-is-the-fallback-with-the-prefix-stripped`](DECISIONS.md#delivery-ref-is-the-fallback-with-the-prefix-stripped).

---

## 400 — What was published: the run record

> A delivery is an observation about stored bytes and can be rebuilt from them.
> A **run** is an event that happened once. This is the first thing in the
> registry that cannot be reconstructed, and it is why `run` has a mutable
> status where `delivery` may not.

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-400** | Every build run that reached the point of writing is recorded, together with **which deliveries it actually read** — derived from the prepared models on the branch before the merge, never declared by the caller. | `registry/db.py` `registry.run` / `registry.run_input`, `registry/runs.py::open_run()`/`record_inputs()`, `registry/inputs.py`, `scripts/_spark_task.py` | `test_runs.py` | stated |
| **REQ-401** | A published report carries a **version number**, allocated per `(report, as-at date)` — not per run and not per report family. | `registry/runs.py::allocate_version()`, `registry.report_version` | `test_runs.py` | stated |
| **REQ-402** | That a version was **sent somewhere** is a separate event from publishing it, and reports submitted together are grouped on the submission. | `registry/runs.py::record_submission()`, `registry.submission` | `test_runs.py` | stated |
| **REQ-403** | A published version is **addressable**: the version row carries the Nessie tag that pins the commit it was published from. | `registry.report_version.tag`, allocated by `allocate_version()` and unique across the table | `test_reproducibility.py` | stated |
| **REQ-404** | A run must be able to say what **code** produced it, and must not conflate a deployed identity with a content digest of a mounted tree. | `common/context.py::code_ref()` (returns value **and** kind), `dbt_manifest_ref` | `test_runs.py` | stated |
| **REQ-405** | A publication records the **change it was made under** — supplied by whoever triggered it, and also written onto the Nessie merge commit. | `dbt_builds.py` (`change_ref`), written as Nessie **commit properties** by `common/nessie.py::merge()` so it is queryable, not only greppable | — | stated |
| **REQ-406** | **Deployment** provenance is recorded separately from per-run change refs: `dbt_project_ref`, `deployment_change_ref`, `deployment_pipeline_ref`, constant across every run of a deployed version. | `registry.run` columns, populated from the environment | `test_runs.py` | stated |

The 405/406 split is the load-bearing part: **a change is a deployment event,
not a run event.** One ticket authorises a version and hundreds of runs inherit
it. Merging the two would make a scheduled run either record no change at all
or carry a re-typed identifier, and a re-typed identifier is an unverified one.

Reasoning: [`#a-run-is-the-first-thing-the-registry-cannot-rebuild`](DECISIONS.md#a-run-is-the-first-thing-the-registry-cannot-rebuild),
[`#version-is-per-report-and-as-at-date`](DECISIONS.md#version-is-per-report-and-as-at-date),
[`#a-change-is-a-deployment-event-not-a-run-event`](DECISIONS.md#a-change-is-a-deployment-event-not-a-run-event),
[`#code-identity-is-a-digest-when-it-cannot-be-a-tag`](DECISIONS.md#code-identity-is-a-digest-when-it-cannot-be-a-tag),
[`#a-version-diff-is-inputs-and-code-not-data`](DECISIONS.md#a-version-diff-is-inputs-and-code-not-data).

---

## 500 — The as-at date's lifecycle

> A `(report, as-at date)` pair moves `open → locked → submitted`, and back to
> `reopened` only deliberately. **`open` is the absence of a row** —
> `registry.as_at_transition` is append-only and records departures from the
> default, not the default itself.

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-500** | An as-at date has a lifecycle, and closing it is what submission does. | `registry/lifecycle.py::transition()`, `registry/runs.py::record_submission()` | `test_lifecycle.py` | stated |
| **REQ-501** | Reopening a **submitted** date requires the approval of the report's **named owner** — and that owner is the dbt exposure's `owner.name`, not a new config field. | `common/context.py::report_owner()`, `registry/lifecycle.py::reopen()` | `test_lifecycle.py` | stated |
| **REQ-502** | A published as-at date whose **inputs later change** must be detectable: the question is not "has anything happened since" but "has what we published stopped being what the inputs now say". | `registry/lifecycle.py::inputs_changed()` and `check_publishable()` | `test_lifecycle.py` | stated |
| **REQ-503** | **Nothing branches on what kind of report it is.** A daily internal dashboard and a quarterly regulatory return travel the same path. | `registry/lifecycle.py` — by the absence of any `type`/`maturity` branch | `test_lifecycle.py` greps for it | stated |

Two implementation facts that the requirement text does not carry, and that
have both already caused a defect:

- **The publish gate runs BEFORE the merge.** `publish()` merges first and
  versions second, so a gate placed alongside the versioning fires after `main`
  has already moved.
- `LifecycleRefused` is deliberately **not caught** in `dbt_builds.py`. A
  refusal must fail the task, not degrade to a warning.

Reasoning: [`#the-as-at-date-has-a-lifecycle`](DECISIONS.md#the-as-at-date-has-a-lifecycle).

---

## 600 — Evidence retention

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-600** | A feed's evidence window is a named **retention class**, declared on the feed. Naming a class the policy file does not define is refused at load. | `feeds.yml` `retention_class:`, `common/context.py` | `test_retention_classes.py` | stated |
| **REQ-601** | The **window** for a class is per-environment policy and lives in `retention.yml`, separately from the feed that names the class. | `retention.yml` `retention_classes:`, `retention/landing.py`, `retention/quarantine.py` | `test_retention_classes.py` | derived — always cited as "REQ-600/601", never alone |
| **REQ-602** | Retention must not delete the evidence a published report is reproduced from. | `retention.check_reproducibility_window()` (the configuration half), `monitoring/evidence.py` (the per-delivery half), `feeds_behind_report()` | `test_tag_retention.py`, `test_reproducibility.py` | stated |

REQ-602 is deliberately **two halves**, and neither is sufficient:

- `check_reproducibility_window()` compares two numbers — the landing window
  against the longest published-tag window — and refuses the whole sweep if
  landing is shorter. Cheap enough to run before every delete; catches the
  *configuration* that guarantees evidence loss.
- `monitoring/evidence.py` walks each published pin's actual deliveries.
  What the window check structurally cannot catch is one landing object going
  missing inside an otherwise coherent window — deleted by hand, swept under a
  shorter window last month, or never uploaded. The numbers still agree and the
  evidence is still gone.

Two files for classes and windows because they are **two decisions with two
owners**: the class is a property of the obligation a feed carries, set by
whoever onboards it; the window is retention policy, set per environment.
Classes govern `landing/` and `quarantine/` **only** — table keep-sets stay per
layer, because a per-feed raw window would fight `find_pending`.

Reasoning: [`#retention-classes-name-the-obligation`](DECISIONS.md#retention-classes-name-the-obligation),
[`#published-tags-are-the-reproducibility-window`](DECISIONS.md#published-tags-are-the-reproducibility-window),
[`#the-evidence-interlock-is-two-halves`](DECISIONS.md#the-evidence-interlock-is-two-halves).
See also `RETENTION.md`'s own note that the window check is *not* the full
REQ-602 interlock.

---

## 700 — Reclamation and reproducibility

| ID | Requirement | Where it lives | Proved by | Confidence |
|---|---|---|---|---|
| **REQ-700** | Storage that nothing references is reclaimed — expired snapshots, orphaned files, unreferenced Nessie objects. | `maintenance/maintain.py`, `retention/retention.py`, `retention/orphan_storage.py`, `nessie-gc` | `test_orphan_storage.py`, `test_maintenance_decisions.py` | derived |
| **REQ-702** | A published run must still be **readable at its own pin** — and this is verified by exercising the pin against the real catalog, not asserted. | `monitoring/reproducibility.py`, run as the last step of `platform_housekeeping` | `test_reproducibility.py` | stated |

**REQ-701 is not cited anywhere.**

REQ-702 exists because nothing about a broken pin announces itself. A tag
deleted too early, a GC cutoff that collected a file the tag referenced, a
maintenance step that expired the snapshot it pointed at: each leaves a catalog
that looks healthy, and the first to find out is whoever was asked to reproduce
a figure from three years ago.

The check reports `not_yet_meaningful` when no scanned pin holds a file `main`
has dropped — that is an honest "this has not been exercised yet", not a pass.
`--tag <t>` forces one.

Reasoning: [`#gc-lag-and-assertions`](DECISIONS.md#gc-lag-and-assertions),
[`#an-incomplete-keep-set-refuses`](DECISIONS.md#an-incomplete-keep-set-refuses).

---

## Keeping this file true

The IDs are a vocabulary shared between code, tests and prose, so the ways it
rots are the ordinary ones:

- **A new `REQ-nnn` in code with no row here.** Add the row when you add the
  citation; a citation that resolves to nothing is what this file exists to
  end.
- **A row whose *Where it lives* no longer exists.** Renaming a module is when
  to grep for its ID.
- **A `derived` row that someone can turn into `stated`.** If the original
  wording surfaces, correct the statement and change the column.

To see every citation:

```bash
grep -rn 'REQ-[0-9]\{3\}' --include='*.py' --include='*.md' --include='*.yml' --include='*.sql' .
```

## Related

- [`DECISIONS.md`](DECISIONS.md) — why each of these is implemented the way it is
- [`RETENTION.md`](RETENTION.md) — the 600 and 700 blocks in operational detail
- [`../tests/README.md`](../tests/README.md) — what `python -m tests.run` covers, and what only running the stack can
