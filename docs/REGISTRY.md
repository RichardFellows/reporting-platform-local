# The registry

What arrived, what was published, out of which inputs, under whose authority.

The registry lives in the Postgres database `platform`, schema `registry`. It
is reached through one CLI:

```bash
docker compose exec -T airflow python -m reporting_platform.registry <command>
```

**There is no Spark in this CLI.** Everything it touches is boto3, json and
psycopg2, so it runs in the task process rather than through
`scripts/_spark_task.py`. The one Spark-using module in the package,
`registry/inputs.py`, is deliberately *not* wired in here — it is invoked as
`scripts._spark_task run-inputs <branch>`, like every other Spark caller in
this repo.

Every command prints JSON.

## The one thing to understand first

The registry has two halves, and confusing them is how people end up asking it
questions it will not answer.

| | Observations | Events |
|---|---|---|
| **Tables** | `delivery`, `delivery_part`, `rejection` | `run`, `run_input`, `report_version`, `submission`, `submission_item`, `as_at_transition` |
| **About** | bytes that still exist in object storage | things that happened once |
| **Rebuildable?** | **yes** — `reconcile()` is the authority, not a repair tool | **no** |
| **Mutable status?** | no | `run` has one |
| **Records verdicts?** | **never** | yes, that is what they are |

The delivery half holds no `ingested`, no `superseded`, no `status`. Whether a
delivery reached raw stays derived from `_source_file` in the raw table;
whether it supersedes another stays `dedupe_rank`'s answer. That is the single
difference from the legacy `stg` load-control tables this platform replaces —
those held a status that could disagree with the data, and eventually did.

So: **do not add a status column to `delivery` to answer a question.** The
question already has a derived answer somewhere, and two answers that can
disagree is the failure mode being avoided.

Reasoning: [`DECISIONS.md#the-registry-records-observations-not-verdicts`](DECISIONS.md#the-registry-records-observations-not-verdicts),
[`#a-run-is-the-first-thing-the-registry-cannot-rebuild`](DECISIONS.md#a-run-is-the-first-thing-the-registry-cannot-rebuild).

---

## Command reference

### Setup and repair

| Command | Does |
|---|---|
| `schema` | Create the registry schema if absent. Idempotent. |
| `reconcile [--feed F] [--no-normalize]` | Walk object storage and register everything not yet known. **This is the rebuild path and it is idempotent.** |
| `coverage [--feed F]` | Manifests in `ready/` versus rows in the registry. |

**`reconcile` normalizes first, by default.** It creates any missing manifests
before registering, which is what gives a file pushed straight into the bucket
a manifest at all. `--no-normalize` suppresses that.

This matters for dry runs. `deliveries.reconcile()` writing manifests is the
reason `registry_reconcile` in `platform_housekeeping` had to be *narrowed*
rather than skipped under `{"dry_run": true}`: skipping it is not the fix,
because it is the rebuild path, and *events are an optimisation, the poll is
the correctness guarantee*. Under a dry run it now writes rows but not
manifests and reports the count as `would_normalize`.
([`#a-dry-run-may-write-to-the-index-not-to-object-storage`](DECISIONS.md#a-dry-run-may-write-to-the-index-not-to-object-storage))

**Reading `coverage`:**

```json
{ "feed": "fo_trade", "manifests": 157, "registered": 157,
  "missing": [], "manifest_expired": 0 }
```

- `missing` non-empty means **reconcile has not run since a write failed** —
  not that anything is wrong with the deliveries. Registry writes during
  normalize are best-effort and never fatal: `landing/` is the evidence and the
  raw table is the ledger, and taking ingestion down to protect an index would
  be the wrong way round. `coverage` is what makes that silent fallback
  visible.
- `manifest_expired` non-zero is **normal and never a warning.** `ready/` is a
  days-long cache and `landing/` keeps deliveries for years, so the registry
  outliving the manifest is the designed steady state.

### What arrived

| Command | Does |
|---|---|
| `deliveries --cob-date DATE [--feed F]` | Registered deliveries for one COB date. |
| `rejections [--feed F] [--limit N]` | Most recent refused deliveries. |

A rejection carries **the rejection date in its object key**, because a
quarantined file usually has no parsable name — that is precisely why it was
refused. The bytes are in `quarantine/`; `.rejected/` is the console's working
copy of the same thing.

### What was published

| Command | Does |
|---|---|
| `runs [--purpose prepared\|reporting] [--limit N]` | Build runs, newest first. |
| `versions [--report R] [--limit N]` | Published report versions and the tag pinning each. |
| `inputs --run-id RUN` | The deliveries that run actually read. |
| `submissions` | Recorded submissions. |
| `submit --destination D --by WHO --version R:DATE:N [--version …] [--family F] [--note …]` | Record that published versions were **sent somewhere**. |
| `diff --report R --as-at DATE [--from N] [--to N]` | What changed between two versions — **inputs and code, not data**. |
| `provenance` | The code and deployment identity a run *would* record right now. |

**`inputs` is derived, not declared.** The publish step selects the distinct
delivery ids out of the prepared models on the branch before the merge, using
the same `delivery_ref()` the rows themselves carry. Nobody hands the platform
a list.

**`diff` defaults to the last two versions**, which is the comparison anybody
actually wants and the one that is tedious to type. It reports the set
difference of two `run_input` sets plus the `change_ref` on each run. It does
**not** diff the data — the published tables are pinned by their tags and can
be compared directly with `nessie_ref`; what was missing was the ability to say
which *inputs* differ, which no query over the tables can answer.

The delivery join in `diff` is a **left join**, because `run_input` has no
foreign key to `delivery`. A delivery the registry cannot describe is reported
with nulls rather than dropped — dropping it would remove it from both sides
equally and make a real difference look like agreement.

**`provenance` exists for the deployment pipeline as much as for you.** The
pipeline bakes `dbt_project_digest` in as `DBT_PROJECT_DIGEST`, and every run
recomputes and compares it. Printing the value from the same code that computes
it is what stops those being two implementations that agree until one changes.
`check_project_drift()` compares **digest to digest**, and refuses only in
`uat`/`prod` — the project is writable at run time in dev, which is what the
feed console does.

---

## The as-at lifecycle

A `(report, as-at date)` pair moves `open → locked → submitted`, and back to
`reopened` only deliberately. The state machine, and why the gate sits where it
does, is in [`ARCHITECTURE.md`](ARCHITECTURE.md#the-as-at-date-has-a-lifecycle).

| Command | Does |
|---|---|
| `state --report R --as-at DATE` | The state of one pair. |
| `lifecycle [--report R] [--history]` | Every date that has a state; `--history` gives every transition. |
| `lock --report R --as-at DATE --actor WHO --reason WHY` | Close a date to routine republication. |
| `reopen --report R --as-at DATE --actor WHO --reason WHY [--approved-by OWNER]` | Reopen a locked or submitted date. |

**There is no `open` command.** `open` is the absence of a transition, so a
date returns to it by being *reopened*, not by being set back.

`--approved-by` is required to reopen a **submitted** date and must match the
owner declared on the report's dbt exposure. That is a real check — the owner
comes from the exposure and cannot be supplied by the person doing the
reopening. Locking needs only an actor and a reason: a lock is an internal
control, and requiring an approver for one would make the one place approval
matters indistinguishable from routine.

**A refusal exits 2, with a message and no traceback.** A refusal is the tool
working; the message names what to do next, and a stack trace would bury it. A
mistyped report name is the most likely refusal of all and is handled the same
way — it comes from `context.report()` rather than the state machine, and it
already lists the valid names.

```bash
# a worked example
R=counterparty_exposure; D=2026-08-01
docker compose exec -T airflow python -m reporting_platform.registry \
  state --report $R --as-at $D
docker compose exec -T airflow python -m reporting_platform.registry \
  lock --report $R --as-at $D --actor rfellows --reason "signed off for August"
# ... and to undo it after submission, with the exposure owner's approval
docker compose exec -T airflow python -m reporting_platform.registry \
  reopen --report $R --as-at $D --actor rfellows \
  --reason "restated on RPT-1421" --approved-by "Risk Reporting"
```

---

## Adding a column to a registry table

**`SCHEMA` is not enough — you must also add it to `MIGRATIONS`.**

`CREATE TABLE IF NOT EXISTS` is a no-op against an existing table, so a column
added only to `SCHEMA` never appears on any database that already exists. The
`INSERT` then fails later, in a task, at publish — a long way from the edit
that caused it. Both constants are in `registry/db.py`.

---

## What is deliberately not here

- **An Iceberg replica of the registry for analytical joins.** It needs a
  namespace outside the dbt project, which `managed_tables()` derives from, so
  maintenance and retention would not cover it without explicit registration.
- **A transport for `submit`.** This platform submits nothing; there is no
  transport here and the table does not pretend otherwise. It is the record
  that a submission was made, written by whoever made it, and it exists because
  the alternative is reconstructing it later from email.
- **An arrivals table.** The console's Arrivals page joins `registry.delivery`,
  `registry.rejection`, `inbox.route()` and Airflow **per request** and writes
  nothing. An arrivals table would hold verdicts the platform derives and could
  not be rebuilt. ([`#the-arrivals-view-is-a-join-not-a-record`](DECISIONS.md#the-arrivals-view-is-a-join-not-a-record))

## Related

- [`REQUIREMENTS.md`](REQUIREMENTS.md) — the 100 and 400/500 blocks
- [`ARCHITECTURE.md`](ARCHITECTURE.md#the-registry-what-is-recorded-and-what-stays-derived) — where the registry sits
- [`MONITORING.md`](MONITORING.md) — the checks that read it
- [`DECISIONS.md`](DECISIONS.md) — the reasoning behind every rule above
