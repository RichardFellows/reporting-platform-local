# Monitoring

Six checks. Each one exists because something failed quietly, and each is
written against the mechanism that *exists* rather than the one that is
documented — a guard written against a documented mechanism rather than the
working one can only ever produce false alarms.

## The six, at a glance

| Check | Asks | Needs Spark | Runs as |
|---|---|---|---|
| [Completeness](#completeness) | Which COB dates are **missing** from a feed's history? | yes | `completeness_check` in `platform_housekeeping` |
| [Lateness](#lateness) | Which deliveries that **did** arrive were late? | **no** | `lateness_check` |
| [Evidence](#evidence) | Does every published pin still have the deliveries behind it? | no | `evidence_check` |
| [Reproducibility](#reproducibility) | Can a published run still be **read at its own pin**? | yes | `reproducibility_check`, last step |
| [The watchdog](#the-watchdog) | Is housekeeping running at all? | no | **its own container** |
| [The orphan sweep's refusal](#the-orphan-sweeps-refusal) | Is the keep-set complete enough to delete against? | no | inside `enforce_retention` |

Five of the six run inside `platform_housekeeping`. The sixth cannot, and that
is the point of it.

---

## Completeness

```bash
docker compose exec -T airflow python -m scripts._spark_task completeness
```

**Why `dbt source freshness` is not this check.** Freshness measures the age of
the newest `_ingest_ts`, so it catches a feed that has *stopped* arriving. It
cannot catch a hole in the middle of a history that later resumed, because a
newer delivery resets the clock. The seed's deliberately absent counterparty
day raises no freshness warning at all.

A gap is worse than a late feed. A late feed is visibly missing and the report
visibly incomplete. **A gap is a report that runs, returns numbers, and is
quietly wrong for one date, forever.**

### It has three answers, not two

| Status | Means | Fails `--fail-on-gap`? |
|---|---|---|
| `no data` | The table exists and is empty. | no |
| `no table` | `TABLE_OR_VIEW_NOT_FOUND` — a feed that has never delivered. | no |
| `unreadable` | Anything else. The table could not be read. | **yes** |

**A subject it could not READ is not a subject that is EMPTY**, and reporting
the first as the second is how a monitor goes green on a table nobody opened.
Collapsing `unreadable` into `no data` would mean nothing on screen and a
passing check.

Which dates *should* exist is inferred from the other feeds' calendar, unless
the feed declares `cadence:` (it does not deliver every COB date) or
`delivery_expected: false` (monthly or ad-hoc — opts out entirely).

---

## Lateness

```bash
docker compose exec -T airflow python -m reporting_platform.monitoring.lateness
```

No Spark: it reads the registry and object storage.

**One lateness concept, and it is a wall-clock time.** `expected_by: "07:00"`
is the time an upstream committed to, judged on the day *after* the COB date —
fixed at +1 and forgiving on purpose, because a date must have ended before its
extract can be taken. Two earlier config keys that pretended to be this,
`arrival_timeout_hours` and `arrival_poke_seconds`, were deleted for being
settings nothing read, and one had already been mistaken for a mechanism.

A duration would need an origin event, and a delivery arriving by `PutObject`
does not have one.

**Quote the value.** YAML 1.1 reads `7:00` as the integer 420, and a leading
zero hides that until the first unpadded hour.

**No `expected_by` means the feed is skipped**, not judged against midnight.
A feed that has made no promise cannot be late.

**This is not the completeness check and must not become it.** Completeness
asks which COB dates are *missing*; this asks which of the deliveries that
*did* arrive were late. A date with no delivery is a gap, not an infinitely
late arrival.

A backfill — many late COB dates, one arrival day — is **one event**. It is
described differently from a routine late delivery, but never suppressed.

---

## Evidence

```bash
docker compose exec -T airflow python -m reporting_platform.monitoring.evidence
docker compose exec -T airflow python -m reporting_platform.monitoring.evidence --fail-on-missing
```

Walks each published pin's deliveries and checks the landing object is still
there. This is **half** of REQ-602, and the other half is a configuration
check that runs before every sweep:

| Half | What it catches | Cost |
|---|---|---|
| `retention.check_reproducibility_window()` | The **configuration** that guarantees evidence loss: landing's window shorter than the longest published-tag window. Refuses the whole sweep. | two numbers |
| `monitoring.evidence` | **One delivery** going missing inside an otherwise coherent window — deleted by hand, swept under a shorter window last month, never actually uploaded. | per delivery |

Neither is sufficient. The window check compares two numbers, and when one
object vanishes the numbers still agree and the evidence is still gone.

---

## Reproducibility

```bash
docker compose exec -T airflow python -m reporting_platform.monitoring.reproducibility
docker compose exec -T airflow python -m reporting_platform.monitoring.reproducibility --tag <t>
```

Runs as the last step of `platform_housekeeping`, after everything that
deletes.

**Why a test and not a comment.** Everything that makes a published run
reproducible is a *pin*, and nothing about a pin announces its own failure. A
tag deleted too early, a GC cutoff that collected a file the tag referenced, a
maintenance step that expired the snapshot it pointed at — each leaves a
catalog that looks healthy and a pin that no longer resolves. The first to find
out is whoever was asked to reproduce a figure from three years ago.

So the pin is **exercised** against the real catalog: the reference is
resolved and read.

### `not_yet_meaningful` is not a pass

| Status | Means |
|---|---|
| `reproduced` | A pin holding data files `main` has since dropped was read successfully. This is a real pass. |
| `not_yet_meaningful` | No scanned pin holds a file `main` has dropped, so nothing has actually been tested yet. **Honest, not green.** |
| `BROKEN` | A pin did not resolve or did not read. |

A capped `not_yet_meaningful` does not mean no pin diverges — only that none of
the ones scanned did. `--tag <t>` forces a specific pin.

The check that used to sit here was **an age**, and the age was wrong in both
directions: a pin ages *out* of the compaction window rather than into it, and
expiring a COB date is a metadata-level partition delete, so `main` stops
referencing a date's files while every pin goes on referencing them. On a
catalog whose newest COB date is older than the compaction window — which is
every catalog between deliveries — the age gate opened on a fixed date and
reported green on a pin still byte-identical to `main`. **Worse than an
unverified check: a check that turns green on a calendar.**

---

## The watchdog

```bash
docker compose logs --tail 20 watchdog
```

**It runs in its own container, and that is the whole design.**
`storage_report` is the tripwire for reclamation not happening — and it is a
task *inside* `platform_housekeeping`, the DAG whose absence it would need to
detect. A monitor inside the thing it monitors cannot report that thing being
down. Pause the DAG, break its import, or leave a stale run wedging
`max_active_runs=1`, and the tripwire never fires.

Not hypothetical: a storage leak that stranded table directories went unnoticed
for exactly this reason, and the stale working branches found on first
inspection existed because housekeeping had never run at all.

It makes five checks:

| Check | Looks for |
|---|---|
| `check_orchestrator` | Has the watched DAG succeeded recently; is a run stuck |
| `check_nessie` | Is the catalog reachable, and how many working branches are outstanding |
| `check_orphan_tables` | Warehouse prefixes no reference points at |
| `check_warehouse` | Is total warehouse size **flat** across a window — i.e. is reclamation happening at all |
| `check_deferred_backlog` | Have GC-identified files gone undeleted past their deferred-delete window |

### Two tuning rules that have both already caused a false alarm

**Match the window to the cadence of whatever clears it.** A check whose window
does not contain the thing it describes will either never fire or never stop.
`check_warehouse` is a **wall-clock** window, not a sample count: a
sample-counted window does not match the nightly cadence of the thing that
clears it, and quick samples would declare the warehouse flat for 36 hours.
The window is only genuinely covered once history reaches back across it.

**Eligible is not overdue.** `check_deferred_backlog` fires only when a
housekeeping run *completed after* the files became eligible and they are still
there — not merely when files are older than the deferred-delete window.

All thresholds are environment variables, so they can be tuned without a
rebuild:

| Variable | Default |
|---|---|
| `REPORTING_WATCHDOG_DAG` | `platform_housekeeping` |
| `REPORTING_WATCHDOG_MAX_AGE_HOURS` | `48` |
| `REPORTING_WATCHDOG_STALE_RUN_HOURS` | `6` |
| `REPORTING_WATCHDOG_MAX_BRANCHES` | `12` |
| `REPORTING_WATCHDOG_FLAT_HOURS` | `36` |
| `REPORTING_WATCHDOG_HISTORY` | `/var/lib/reporting-watchdog/history.jsonl` |

Reasoning: [`#watchdog-independent`](DECISIONS.md#watchdog-independent),
[`#watchdog-wall-clock-window`](DECISIONS.md#watchdog-wall-clock-window),
[`#watchdog-eligible-vs-overdue`](DECISIONS.md#watchdog-eligible-vs-overdue).

---

## The orphan sweep's refusal

```bash
docker compose exec -T airflow python -m reporting_platform.retention.orphan_storage --dry-run
```

Not a monitor in the usual sense — it is a **delete** that has learned to
refuse. It is here because its refusal is a signal you must act on, and a
refusal is not a success.

`orphan_storage` deletes every warehouse prefix not live on some reference, so
**its input is a keep-set and a short answer is a deletion order.** A Nessie
that was down therefore used to read as "nothing is live".

Three safety properties, all of which exit **non-zero**:

- **An unreadable reference refuses the sweep** — anything but a 404. A 404 is
  the ordinary "this branch is gone" the sweep exists to reclaim; a connection
  refused, a 401 or a 500 is not.
- **An empty live set against a non-empty warehouse refuses.** A warehouse
  holding objects and a catalog claiming nothing is live means the catalog has
  lost its tables, not that everything is garbage.
- **The prefix depth is derived from `REPORTING_WAREHOUSE`, never assumed.** A
  nested warehouse root would otherwise make every namespace an orphan.

**Nothing deleted is not the same as nothing to delete.** The exit code is what
distinguishes them, so do not treat a refusal as a quiet night.
([`#an-incomplete-keep-set-refuses`](DECISIONS.md#an-incomplete-keep-set-refuses))

---

## What is deliberately not monitored here

**Lineage is not a monitor.** A skipped Airflow task shows as `RUNNING` in
Marquez forever — Airflow 2.10's listener spec has no skipped hook, and
skipping is the ingest DAGs' idle state. `RUNNING` there means "started, did
not succeed or fail". **Airflow is the authority on what is running.**

## Related

- [`REQUIREMENTS.md`](REQUIREMENTS.md) — the 200, 600 and 700 blocks
- [`RETENTION.md`](RETENTION.md) — what the sweeps these checks guard actually do
- [`MAINTENANCE.md`](MAINTENANCE.md) — the metric-driven triggering the watchdog watches for
- [`REGISTRY.md`](REGISTRY.md) — `coverage`, the check that catches a registry falling behind
