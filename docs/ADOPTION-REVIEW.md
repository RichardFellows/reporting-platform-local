# Adoption Review — this repo as a base for an AutoSys / SSIS / SQL Server migration

**Question asked:** is this repository a suitable base for a team migrating its
processes off a legacy AutoSys + SSIS + SQL Server stack onto a lakehouse?

**Short answer:** yes as a *reference architecture and decision record*, and it
is an unusually good one. No as a *migration platform* you can start pointing
existing SSIS packages at — the source-side, consumption-side and
engineering-hygiene halves of that job are not in here, and one of them (how
your reports get consumed) is explicitly unimplemented.

Read this as: **adopt the design, adopt roughly 60% of the code, and budget for
the three subsystems that are missing rather than assuming they are small.**

---

## 0. How this review was produced, and what it does not cover

This was a **static review**: every file in the repo was read, the Python
compiled, the YAML parsed, and the pure business logic exercised directly
(`calendar_rules.keep_set`, `month_end_dates`, `Feed.parse_filename`,
`managed_tables`) — all behaved as documented.

The stack was **not** run. No Docker daemon was available in the review
environment, so nothing below is a claim about live behaviour that the repo's
own docs do not already claim. `CLAUDE.md` is right that a claim is worth what
its last execution proved; where this review asserts a defect, it is one
provable by reading (a config that contradicts another config, a Spark
construct with known semantics), not one inferred from a docstring.

Not covered: performance at any volume, the OpenShift target (no cluster), and
whether the numbers the reports produce are the numbers your business wants.

---

## 1. What this repo actually is

| | |
|---|---|
| Size | 63 files, ~9,200 lines, one squashed commit |
| Feeds | 3, all daily CSV drops (`trade`, `counterparty`, `rating`) |
| Data volume | 400 trades/day × 60 counterparties — ~25k rows in the whole estate |
| Models | 3 `prepared` + 3 `reporting`, incremental Iceberg, dbt tests on all |
| Runtime | MinIO, Nessie 0.99, Postgres 16, Spark 3.5.3, Airflow 2.10.5, dbt 1.8.7 |
| Tests / CI | none of either |

It is a **working, end-to-end, laptop-scale proof of a specific set of design
decisions** — not a partially-built production platform. That distinction
drives everything below.

---

## 2. What transfers directly, and is worth more than the code

The strongest thing here is not any module; it is that nearly every non-obvious
decision carries a written record of *what broke*, at the point in the code
where you would otherwise reverse it. That is exactly the asset a team
migrating a decade-old estate needs, and it is the part you cannot buy.

Things worth keeping as-is:

1. **The layer model** (`landing` → `raw` → `prepared` → `reporting`), and
   specifically the rule that **a load must never fail because a value was
   unparseable — it must land, and then fail a test.** This is the single most
   valuable inversion relative to SSIS. Today a bad `notional` aborts a package
   at 03:00 and someone gets paged; here it lands, `TRY_CAST`s to NULL, fails a
   `not_null` test, and the build simply does not publish. Nobody sees a wrong
   number and nobody is woken up.

2. **Write-audit-publish on Nessie branches.** Build on a branch, test on the
   branch, merge only if green. This gives you three things SQL Server made
   expensive: atomic multi-table publication, rollback by resetting a ref, and
   "what exactly did we publish on the 5th?" as an answerable question. For a
   regulated reporting estate this is the headline capability.

3. **`landing` separate from `raw`.** The byte-exact evidence copy, retained
   8 years on the cheapest tier, kept on a *flat age* policy rather than the
   keep-set the tables use — with the reasoning (`retention.yml`) that
   sampling your evidence by keep-set destroys precisely the thing evidence is
   for. Your `stg` schema conflates these today.

4. **The retention subsystem.** `docs/RETENTION.md` is the best document in the
   repo. The two-stage delete model, and the Nessie-specific third condition —
   **a tag pins every file its commit referenced, so tag retention *is* data
   retention** — is the kind of thing teams discover eighteen months in when
   the storage bill does not fall. It reproduces your "10 working days + 80
   month-ends" partition-switch rule faithfully, computed from *observed*
   business dates so a skipped upstream day does not silently shorten the
   window.

5. **The maintenance subsystem.** `docs/MAINTENANCE.md` states plainly what
   SQL Server did for you for free (index maintenance, statistics, autogrow)
   and Iceberg does not. Metric-driven compaction rather than a blind nightly
   rewrite.

6. **The external watchdog.** A monitor that deliberately shares none of
   Airflow's runtime, reads the metadata DB directly, and treats *silence as a
   failure*. Its docstring explains that the previous tripwire lived inside the
   DAG it was meant to watch. Most teams build this after the incident.

7. **The completeness check.** The observation that `dbt source freshness`
   catches a feed that *stopped*, but never a *hole in the middle* that later
   resumed, is subtle and correct — and the gap is the more dangerous shape,
   because the report runs and returns numbers. The business calendar is
   inferred from what other feeds delivered, so no holiday file is needed and a
   holiday can never be a false alarm. Its blind spots are documented.

8. **`docs/DEV-PROCESS.md`.** Ticket → CAB → prod, with the digest-pinning
   control (compare the artifact digest at deploy time against the digest
   recorded in the CR, and fail closed) that is the difference between an audit
   trail and a plausible-looking one. If your governance shape is anything like
   the one described, this document is directly reusable — as a specification.
   None of it is implemented.

9. **Config-driven feed registry.** Adding a feed edits `feeds.yml`; the DAG is
   generated. `managed_tables()` derives maintenance and retention scope from
   the same registry, after a documented incident where hand-maintained lists
   drifted and four tables silently grew forever.

10. **`dbt/macros/engine.sql`** — engine-specific constructs in one place, with
    the honest note that the value was never portability (that promise was
    withdrawn) but preventing copy-paste drift across models.

---

## 3. The gaps that matter for *your* migration

Grouped by how much work they are, worst first.

### 3.1 Consumption — the biggest hole

The `serving` layer is **not implemented**. The README diagram marks it so; the
Postgres `serving` database is created by `init-postgres.sql` and nothing ever
writes to it; there is no `serving_export` DAG despite the architecture diagram
showing one.

This matters more than it looks, because today *everything the business sees
comes out of SQL Server*. Right now this repo can build a correct
`reporting.rpt_counterparty_exposure` on Iceberg and has no story for getting it
in front of a person except `scripts/duckdb_console.py`, which is a developer
tool. Specifically unanswered:

- **How does the BI tool read this?** Qlik/an equivalent against Iceberg +
  Nessie is not a solved thing. Export to an RDBMS is the pragmatic bridge and
  it is the piece that is missing.
- **What is the access-control model?** This is the one I would raise loudest.
  SQL Server gave you `GRANT`, schema-level permissions, views as a security
  boundary, and row-level security. The lakehouse here has **none** of that:
  Nessie runs `authentication.type: NONE`, readers hold S3 credentials, and
  there is no row/column-level story anywhere in the repo. `OPENSHIFT-MAPPING`
  lists secrets as "secret source only" — that understates it. For a
  counterparty-exposure dataset in a regulated estate, "who can see which rows"
  is a design workstream, not a config change.
- **Masking for non-prod** is named as a future need
  (`OPENSHIFT-MAPPING.md` §3) with the sound advice to keep environment-varying
  values in config now. Nothing implements it.

### 3.2 Source ingestion — the repo assumes away the SSIS surface area

Every feed here is **a CSV file that upstream drops into a landing prefix**.
That is a legitimate and deliberately chosen target shape — and
`OPENSHIFT-MAPPING.md` §1 argues well for a push agent over in-cluster Kerberos
to DFS. But it means the repo has no pattern for most of what an SSIS estate
actually does:

- **Database sources.** SSIS packages typically read OLE DB / ODBC sources
  directly. There is no JDBC extract pattern, no connection registry, no
  chunking/watermark strategy, no `raw` shape for a table-sourced feed.
- **CDC / incremental extract.** Everything assumes a full daily snapshot per
  business date. If any of your sources are CDC or delta-based, the `raw`
  contract (`_business_date` + `_file_version`, dedupe by latest version)
  does not model them.
- **Slowly-changing dimensions.** `prep_counterparty` is a per-business-date
  full snapshot. There are no dbt snapshots and no Type-2 history anywhere. If
  any legacy package uses the SSIS SCD wizard or a merge-based history table,
  that pattern needs designing.
- **In-flight lookups, script tasks, conditional splits** — the SSIS constructs
  that do not map to "land it and test it" — have no worked example.
- **Non-CSV formats** (fixed-width, XML, Excel, multi-record-type files) are
  unrepresented.

None of this is a criticism of the design; it is a scope statement. The repo
proves the file-drop path very well and says nothing about the others.

### 3.3 Scheduler migration — AutoSys is barely addressed

The word "AutoSys" does not appear in the repo (the legacy stack is referred to
generically throughout). What is missing for the scheduler half of the job:

- **No inventory or mapping artifact.** There is no JIL-to-Airflow translation
  pattern, no box-job semantics, no calendar/exclusion-date handling (Airflow's
  file-arrival model sidesteps calendars — which is fine until a job genuinely
  is calendar-driven), no on-call/alerting runbook, and no story for AutoSys
  jobs that are not data jobs at all but which your data jobs depend on.
- **The behavioural change needs explicit sign-off, and the repo says so.**
  Moving from AutoSys's gated model to per-feed asset triggering means *a late
  feed no longer blocks the feeds that arrived* — the reporting layer carries
  forward the last good dimension and a test flags it.
  `docs/ARCHITECTURE.md` flags this as needing report-owner sign-off. Treat
  that as a real governance item: it changes what a published report *means* on
  a bad day.
- **No parallel-run / reconciliation harness.** For a migration of this kind
  this is arguably the number-one deliverable: run old and new side by side,
  reconcile control totals per report per day, and use the divergence log as
  the cutover evidence. `DEV-PROCESS.md` assumes "reconciliation totals" exist
  as CI evidence; nothing produces them. Building this early also gives you the
  data diff the MR review process depends on.

### 3.4 Engineering hygiene — the fastest gap to close, and the one that blocks a team

For one person driving a proof, the current state is fine. For a team, it is
the first thing to fix:

- **Zero tests.** No `tests/`, no `conftest.py`, no pytest anywhere — even
  though `DEV-PROCESS.md` §2 specifies exactly which unit tests CI should run
  ("keep-set, filename parsing"). The good news is that the code is *shaped*
  for testing: I exercised `keep_set`, `month_end_dates` and
  `Feed.parse_filename` directly with no stack running, in seconds. The
  month-end edge case (an in-progress month is not a month-end) and the
  keep-set union are exactly the logic that must not silently regress —
  retention *deletes*.
- **No CI.** No `.github/`, `.gitlab-ci.yml`, `Jenkinsfile` or equivalent, and
  no packaging (`pyproject.toml`/`requirements.txt` absent — deps live only in
  `Dockerfile.airflow`). `DEV-PROCESS.md` describes a validate/verify pipeline
  in detail, including the genuinely clever part — CI builds `state:modified+`
  on an ephemeral Nessie branch and posts a *data diff* to the MR. That is the
  best idea in the repo and it exists only as a diagram.
- **No lint config** despite `ruff` and `sqlfluff` being named as CI stages.
- **One squashed commit, and comments that cite a history you do not have.**
  The code refers to "Session 5", "D7", "Bug #8", "Phase 0" — none of which is
  in the git log. A new joiner reading `dbt_builds.py` cannot look any of them
  up. Either land the underlying history or normalise those references.
- **Redaction damage in shipped text.** Legacy product names were scrubbed to
  generic phrases and the pass left broken sentences behind:
  `dbt/macros/engine.sql:10` ends `...override.1".`; `.env.example:14` the
  same; `reporting_platform/config/feeds.yml:29` has an unclosed
  `Both branches have been exercised (`; `retention.yml:3` reads "the legacy
  the legacy RDBMS"; `docs/DEV-PROCESS.md:109` has "Registry Registry"; and
  `docs/RETENTION.md` and `MAINTENANCE.md` open with the doubled phrase. One of
  these is inside a **runtime error message an operator will actually read**
  (`airflow/dags/dbt_builds.py:105`, `...without failing.1'.`). Cosmetic, but
  it is the first impression the repo makes, and it undermines documentation
  that is otherwise its strongest asset. (Note also `Country Exposure (Qlik)`
  survives in `dbt/models/reporting/_reporting.yml:99` — the scrub was partial,
  so check whether anything else was meant to be generic.)

### 3.5 Platform and technology risk

- **Nessie coupling is deep and deliberate — decide it consciously.** The whole
  safety model is branch-based WAP, and only Spark can address a Nessie branch.
  That single fact is what forces: Spark as the *only* build engine, DuckDB
  demoted to read-only, no dbt-duckdb, and `dbt_builds.py` refusing a non-Spark
  target. The reasoning is sound and well evidenced. But recognise the trade:
  **any future tool that cannot set the Nessie ref cannot participate in a
  build.** The alternative worth pricing before you commit is Iceberg's own
  branch/WAP support (`spark.wap.branch`) on a plain Iceberg REST catalog,
  which is engine-native, more portable, and better supported by the wider
  ecosystem — at the cost of the one thing Nessie genuinely gives you that it
  cannot: **atomic multi-table publication**. If a nine-table report refresh
  appearing atomically is a hard requirement, Nessie earns its coupling. If it
  is a nice-to-have, it may not.
- **Airflow 2.10.5 is pinned for a local-stack reason, not a target-architecture
  reason.** The header in `Dockerfile.airflow` is specific and credible: under
  Airflow 3 no DAG run on this stack could complete. But the 2.x line is the
  superseded one, your cluster target is KubernetesExecutor (a different
  execution path entirely), and the DAG code already carries an
  `airflow.sdk` / `airflow.datasets` shim. Do not inherit this pin into the
  cluster design; schedule the Airflow 3 validation as its own piece of work.
- **Pins are aging.** Iceberg 1.6.1, Nessie 0.99.0, dbt-core 1.8.7. Deliberate
  and coherent (Iceberg/Spark/Nessie minors move together), but they are a
  snapshot and the upgrade path is untested by anything here.
- **`spark_ocp` is explicitly unproven** — `dbt/profiles.yml` says so: "STILL
  UNPROVEN: no cluster has ever run this." Resource sizings in
  `OPENSHIFT-MAPPING.md` are labelled as starting points, not measurements.
- **Nothing has been exercised at volume.** ~25k rows total. Every conclusion
  about compaction thresholds, partition strategy, incremental windows and job
  sizing is provisional.

---

## 4. Concrete defects found

These are findings from reading, not speculation. Each is small.

### 4.1 Spark extension ordering is inconsistent across the three session configs

`reporting_platform/common/context.py:232-247` documents this as load-bearing
and gets it right:

> ORDER MATTERS. Each extension injects a parser that wraps the previous one,
> so the LAST listed ends up outermost. … `rewrite_data_files(strategy =>
> 'sort', …)` checks `parser instanceof ExtendedParser` … with Nessie last …
> `java.lang.IllegalStateException: Cannot parse order: parser is not an
> Iceberg ExtendedParser` … **Nessie first, Iceberg last.**

The other two declarations use the opposite order:

| File | Order | |
|---|---|---|
| `reporting_platform/common/context.py:243` | Nessie, Iceberg | correct |
| `conf/spark-defaults.conf:4` | Iceberg, Nessie | **inverted** |
| `dbt/profiles.yml:42` | Iceberg, Nessie | **inverted** |

Impact today is limited — maintenance runs through `context.spark_session()`,
which is the correct one. But `conf/spark-defaults.conf` is mounted into
`spark-master`/`spark-worker` (so the `spark-sql` commands the README tells you
to run carry the broken order), and `dbt/profiles.yml` says the **`spark_ocp`
cluster target deliberately relies on that same file for everything except the
ref** — so the in-cluster path inherits it. Any sort-ordered Iceberg procedure
invoked from either would fail with the exact exception `context.py` warns
about, and `maintenance.yml` gives `prepared`/`reporting` `strategy: sort`.

This is precisely the failure class the repo elsewhere calls out — a forked
copy that stops matching the original. Two-line fix.

### 4.2 `_row_number` will not scale, and weakens the dedupe tiebreak

`reporting_platform/ingest/ingest_feed.py:217`:

```python
row_win = Window.orderBy(F.monotonically_increasing_id())
```

A window with no `partitionBy` forces **all rows of the file into a single
partition** (Spark logs "No Partition Defined for Window operation! Moving all
data to a single partition, this can cause serious performance degradation").
At 400 rows/day it is invisible; on a real front-office extract it is a
bottleneck and a memory risk on the driver-only local session
(`spark.driver.memory: 3g`).

It also matters semantically: `_row_number` is the tiebreak in `dedupe_rank`
("we take the last occurrence in file order, which matches the legacy tool's
behaviour"), and `monotonically_increasing_id()` is a non-deterministic
expression whose values depend on input split layout. For a single small CSV it
tracks file order; for a large multi-split read the guarantee is weaker than
the comment claims. If "last occurrence in file order" is a business rule you
are relying on for parity with the legacy behaviour, it needs a stronger basis
(e.g. `input_file_name()` + a per-file monotonic ordering, or an explicit
upstream sequence column).

### 4.3 `make build` / `make prepared` / `make reporting` build on `main`

The Makefile flags this itself in a comment (`Makefile:56`) — they bypass
write-audit-publish because `nessie_ref` defaults to `main`. It is documented
and the reasoning (convenience on a throwaway stack) is fine, but for a team
base I would make the branch the default and require an explicit
`--on-main`-style opt-out, so the safe path is the lazy path.

### 4.4 Publish-merge races are unhandled but self-healing

`Nessie.merge()` pins the target's expected hash, so two merges racing into
`main` produce a conflict. Ingest merges happen inside the `lakehouse_write`
pool and are serialised; the dbt `publish` task is **not** pooled, so it can
race an ingest merge. Airflow's `retries: 1` re-reads the head hash and would
succeed on retry, so this self-heals — but it will look like an intermittent
failure to whoever is on call, and the pool assignment is worth making
deliberate.

---

## 5. Recommendation

**Adopt it as the architecture and the decision record. Do not treat it as a
platform starter kit.** Concretely:

**Keep and build on (little to no change):** the layer model and the
land-then-test rule; write-audit-publish; the retention and maintenance
subsystems and their documents; the watchdog and completeness monitors;
`feeds.yml` as the single feed registry; the dbt project layout, macros and
naming override; `DEV-PROCESS.md` as the target process spec.

**Build before your fourth feed lands** (in rough priority order):

1. **A reconciliation / parallel-run harness.** Old vs new, per report, per
   day, control totals plus row-level divergence. It is your cutover evidence,
   it is the data diff the MR process wants, and it is the thing that makes
   every other argument with report owners tractable.
2. **The serving path**, and with it the access-control model. Pick the BI
   bridge (RDBMS export is the obvious one) and decide who can see which rows
   *before* the first real dataset lands.
3. **Tests and CI.** Start with the pure logic (`calendar_rules`, filename
   parsing, `find_gaps` calendar inference, `managed_tables` consistency) —
   it needs no stack and it guards the code that *deletes data*. Then DAG-import
   tests, `dbt parse`, and the ephemeral-branch build. Add `pyproject.toml` and
   the lint configs `DEV-PROCESS.md` already assumes.
4. **A source pattern for database-sourced feeds**, if any of your SSIS packages
   read from SQL Server directly — including the SCD/CDC question.
5. **The AutoSys inventory and mapping**, plus the report-owner sign-off on the
   "a late feed no longer blocks" behavioural change.

**Decide deliberately, and write it down:**

- Nessie branch-WAP vs Iceberg-native WAP on a REST catalog — i.e. how much you
  are willing to pay in engine lock-in for atomic multi-table publication.
- Airflow 3 on the cluster (validate it; do not inherit the 2.10.5 pin).
- Whether the `prepared` layer models snapshots only, or needs Type-2 history.

**Housekeeping before anyone else clones it:** fix the extension ordering
(§4.1), repair the redaction damage (§3.4) including the one in a runtime error
message, and either land the real history or normalise the "Session 5 / D7 /
Bug #8" references.

---

## 6. One-line verdict

A genuinely high-quality reference implementation with an exceptional written
rationale — worth more than most consultancy target-state decks — that covers
the middle of your migration (land → conform → publish → retain) thoroughly and
the two ends (getting data out of SQL Server, and getting reports back to
people) not at all. Start from it; budget for the ends.
