# Development and Deployment Process

Covers the path from a Tracker ticket to production for the two change types
that dominate day-to-day work: **dbt model changes** and **Airflow DAG changes**.

Governance context this is designed around:

- feature branches per Tracker ticket, enforced
- merge review into `main`
- named release branches cut off `main`, protected — changes only via approved MR
- **UAT is gated by that approved MR alone**
- **CAB sign-off, automated through ServiceNow, gates UAT → prod**

---

## 1. Why dbt and Airflow changes are not the same risk

Treating them identically is the mistake to avoid. They fail differently, they
are verifiable to different depths in CI, and — crucially — they roll back
differently.

| | dbt model change | Airflow DAG change |
|---|---|---|
| Primary risk | wrong numbers | job doesn't run, or runs wrong |
| CI can prove | model compiles, tests pass **against real data** on an ephemeral Nessie branch | DAG imports, graph is acyclic, assets resolve, callables unit-test |
| CI cannot prove | performance at prod volume | that the trigger condition actually fires |
| Detection | dbt test, at build time | absence of a run — silent until someone asks where the report is |
| Rollback | revert code **and** reset Nessie ref | `helm rollback` |

**The asymmetry that matters most:** a dbt change is verifiable to a much
greater depth before merge than an Airflow change is, because Nessie lets CI
build the real models over real data at zero blast radius. An Airflow trigger
condition, by contrast, is only truly proven by an arrival happening. That is
what the UAT soak period is for, and why a DAG change should not be cut into a
release the same day it merges.

---

## 2. Feature branch and merge request

```mermaid
sequenceDiagram
    autonumber
    actor Dev
    participant Tracker
    participant Forge as Git host
    participant CI as CI runner
    participant Nessie
    participant S3 as Object Store (dev)
    participant Rev as Reviewer

    Dev->>Tracker: pick ticket PROJ-1234
    Dev->>Forge: branch feature/PROJ-1234-add-column
    Dev->>Dev: local: docker compose up, dbt build on local Nessie
    Dev->>Forge: push + open MR to main

    Forge->>CI: pipeline (rules: changes)

    rect rgb(238, 244, 250)
    note over CI: stage: validate — runs for every MR
    CI->>CI: ruff, sqlfluff, yaml schema check
    CI->>CI: unit tests (keep-set, filename parsing)
    CI->>CI: dbt parse  → manifest.json artifact
    CI->>CI: DAG import test (no DB, no scheduler)
    CI->>CI: consistency: every feeds.yml entry has source + model
    end

    rect rgb(240, 249, 240)
    note over CI,S3: stage: verify — the part Nessie makes cheap
    CI->>Nessie: create branch ci/mr-1234 from main
    CI->>CI: dbt build --vars nessie_ref=ci/mr-1234<br/>--defer --state $PROD_MANIFEST --select state:modified+
    CI->>S3: writes land on the CI branch only
    CI->>Nessie: query row counts, compare vs main
    CI-->>Forge: post diff summary as MR comment
    CI->>Nessie: delete branch ci/mr-1234
    end

    CI-->>Forge: pipeline green
    Rev->>Forge: review — reads the data diff, not just the SQL
    Rev->>Forge: approve
    Dev->>Forge: merge to main (squash, Tracker key in message)
    Forge->>Tracker: transitions PROJ-1234 via smart commit
```

Two things worth calling out in that flow.

**`--defer --state $PROD_MANIFEST --select state:modified+`** means CI builds
only the changed models and their descendants; everything upstream resolves to
the existing production tables. A one-model change does not rebuild the estate.
This requires publishing the prod `manifest.json` as a CI package artifact
after each production deploy — do that from day one, because retrofitting it
means a period with no baseline.

**The data diff in the MR comment** is the review artifact that matters. A
reviewer approving a dbt change from SQL alone is guessing. Row counts, null
counts and control-total deltas against `main` turn the review into something
evidential. This is a capability the legacy ETL/stored-procedure estate never had and
is worth building properly rather than as an afterthought.

---

## 3. Merge to main, and the dev deploy

```mermaid
sequenceDiagram
    autonumber
    participant Forge as Git host
    participant CI as CI runner
    participant Registry as Registry Registry
    participant Helm
    participant OCP as OpenShift (dev)
    participant AF as Airflow (dev)

    Forge->>CI: pipeline on main

    rect rgb(238, 244, 250)
    note over CI,Registry: build — path rules skip what didn't change
    alt reporting_platform/** changed
        CI->>Registry: publish reporting-platform wheel (version bump)
    end
    alt Dockerfile.spark or jars changed
        CI->>Registry: push spark-base:$SHA
    end
    CI->>CI: dbt parse → manifest.json
    CI->>Registry: push airflow-app:$SHA (DAGs + dbt + manifest)
    end

    CI->>Helm: upgrade dev, image tag = $SHA
    Helm->>OCP: rolling update, pre-upgrade hook:<br/>airflow db migrate, pools, Nessie namespaces
    OCP->>AF: scheduler reparses DAG bag

    note over AF: NEW DAGs land PAUSED<br/>(is_paused_upon_creation)

    CI->>AF: post-deploy smoke: DAG bag import errors == 0
    CI->>AF: trigger dbt build on a dev Nessie branch, assert tests pass
    CI-->>Forge: dev green
```

**New DAGs must deploy paused.** With DAGs generated from `feeds.yml`, adding a
feed creates a DAG that would otherwise start polling the landing prefix the
moment it appears — and in an environment holding production data, that means
an unreviewed ingest. `is_paused_upon_creation=True` plus a deliberate unpause
step is the safe default. This is a specific hazard of dynamic DAG generation
and easy to miss.

---

## 4. Release branch → UAT

**Gate: an approved MR into the release branch. No CAB.**

`release/2026.09` is a protected branch. It cannot be pushed to directly — the
initial cut and every subsequent fix arrive as an MR from `main`, and merging
that MR is what deploys UAT. The approval on the MR *is* the control.

```mermaid
sequenceDiagram
    autonumber
    actor RM as Release Manager
    actor App as Approver
    participant Forge as Git host
    participant CI as CI runner
    participant Registry
    participant Nessie
    participant Helm
    participant OCP as OpenShift (UAT)

    RM->>Forge: MR: main@$SHA → release/2026.09 (protected)
    Forge->>CI: MR pipeline (same validate + verify as any MR)
    CI-->>Forge: green, plus Tracker keys in range
    App->>Forge: approve
    RM->>Forge: merge → release branch created/updated

    Forge->>CI: release pipeline

    rect rgb(250, 245, 235)
    note over CI,Registry: NO REBUILD — promote the tested artifact
    CI->>Registry: verify airflow-app:$SHA digest exists
    CI->>Registry: retag digest as release-2026.09
    end

    rect rgb(240, 249, 240)
    note over CI,Nessie: data rollback point BEFORE any build
    CI->>Nessie: tag release/2026.09/uat-pre-deploy on main
    end

    CI->>Helm: upgrade uat, image = release-2026.09 digest
    Helm->>OCP: pre-upgrade hook, rolling update
    CI->>OCP: smoke: DAG import errors == 0, Nessie reachable,<br/>catalog namespaces present
    CI->>OCP: unpause new DAGs (explicit, listed in RELEASE.md)
    CI-->>RM: UAT deployed — no manual gate, no CR

    note over OCP: soak begins — and this soak is<br/>the ONLY evidence CAB will see
```

Because there is no CAB here, UAT deployment is cheap and can happen often.
That is the right shape: iterate in UAT, gate hard at prod.

It does mean the release branch is **long-lived and mutable** for the duration
of the release. Each fix MR redeploys UAT and produces a new artifact digest —
which matters a great deal once a prod CR exists (see §5).

---

## 5. UAT → Prod: the CAB gate

```mermaid
sequenceDiagram
    autonumber
    actor RM as Release Manager
    participant Forge as Git host
    participant CI as CI runner
    participant SNOW as ServiceNow
    participant CAB
    participant Registry
    participant Nessie
    participant Helm
    participant OCP as OpenShift (prod)

    rect rgb(238, 244, 250)
    note over CI,SNOW: evidence is harvested, not written by hand
    CI->>OCP: collect UAT soak results — DAG run success rates,<br/>dbt test results, reconciliation totals, soak duration
    CI->>Forge: collect MR data diffs across the release range
    CI->>CI: assemble evidence pack + release notes from Tracker keys
    end

    CI->>SNOW: create CR, attach evidence pack, affected CIs,<br/>artifact digest, rollback plan
    SNOW-->>CI: CR number, state = Assess
    CI->>Forge: write CR number + pinned digest to RELEASE.md

    CAB->>SNOW: CAB call — review evidence, approve
    SNOW-->>SNOW: CR state = Scheduled

    RM->>Forge: click manual deploy:prod

    rect rgb(245, 235, 235)
    note over CI,SNOW: fail closed, twice
    CI->>SNOW: GET CR state
    alt state not in (Scheduled, Implement) or outside window
        CI-->>RM: FAIL — no approved CR
    end
    CI->>Registry: compare current release-2026.09 digest<br/>against digest pinned in the CR
    alt digest differs
        CI-->>RM: FAIL — release changed after CAB approved it
    end
    end

    CI->>SNOW: set CR state = Implement
    CI->>Nessie: tag release/2026.09/prod-pre-deploy on main
    CI->>Helm: upgrade prod, image = the CAB-approved digest
    Helm->>OCP: pre-upgrade hook, rolling update
    CI->>OCP: smoke + unpause listed DAGs
    CI->>SNOW: attach smoke results, set CR = Review
```

### The digest check is the control that actually matters

With UAT ungated, fix MRs land on the release branch freely — including after
the CR has been raised. Nothing stops someone merging a fix on Thursday for a
CR that CAB approved on Wednesday, and then prod deploys an artifact no one
reviewed. **The governance is only real if the pipeline compares the digest at
deploy time against the digest recorded in the CR and refuses on mismatch.**

Handle a legitimate late fix explicitly: the pipeline should push the CR back
to `Assess` with a note whenever the release branch moves after CR creation, so
the drift is visible in ServiceNow rather than discovered at the deploy job.
Cheap to build, and it is the difference between an audit trail and a
plausible-looking one.

### Soak duration is now a real gate

UAT is the only place a change is observed before CAB sees it. So the soak
requirement needs a stated minimum — at least one full arrival cycle for every
feed touched, and for a DAG change, at least one cycle where the trigger fired
naturally rather than by manual trigger. Manually triggering a DAG proves the
tasks work; it does not prove the asset condition fires, which is the thing
most likely to be wrong.

Put the minimum in `RELEASE.md` and have the evidence job assert it, otherwise
it becomes whatever the calendar allows.

### Rollback is two commands, in both environments

`helm rollback` restores the **image**. It does not un-write the tables.

```bash
helm rollback platform-airflow -n platform-prod
# and
nessie ref main --assign --to release/2026.09/prod-pre-deploy
```

Miss the second and you have a rolled-back deployment still serving the numbers
you rolled back to avoid. Rehearse it once in dev — 2am is a bad time to
discover the second command.

---

## 6. Where each change type spends its time

```mermaid
sequenceDiagram
    autonumber
    participant dbt as dbt-only change
    participant dag as DAG-only change
    participant feed as New feed

    note over dbt: MR CI builds the model on a Nessie<br/>branch — reviewer sees the data diff
    note over dbt: build: airflow-app only<br/>spark-base skipped by path rules
    note over dbt: UAT soak: one clean build cycle usually enough

    note over dag: MR CI proves it PARSES, not that it FIRES
    note over dag: build: airflow-app only
    note over dag: UAT soak: needs a NATURALLY TRIGGERED run —<br/>a manual trigger proves nothing about the asset condition

    note over feed: touches feeds.yml + source + model + tests
    note over feed: new DAG deploys PAUSED
    note over feed: needs upstream to deliver a real file<br/>into UAT landing before it means anything
    note over feed: unpause is an explicit release step
```

## 7. Practical rules

1. **Never rebuild for a release.** Retag the digest that passed CI on `main`.
2. **Pin the digest in the CR, and check it at prod deploy time.** This is the
   only thing preventing an unreviewed artifact reaching prod under an approved
   CR.
3. **Fail closed on the ServiceNow check.** An unreachable governance system is
   not an approval.
4. **The CR number lives in `RELEASE.md`** on the release branch, so the audit
   trail is reconstructable from git alone.
5. **Tag Nessie before every deploy**, UAT and prod, even for DAG-only changes.
6. **New DAGs deploy paused; unpausing is a listed release step.**
7. **A DAG change needs a naturally triggered UAT run before CAB.**

## 8. Open questions worth deciding deliberately

- **Hotfix path.** Straight to the release branch by MR gets to UAT quickly,
  but the merge-back to `main` is what people forget, and the next release then
  silently regresses the fix. A CI check that every release-branch commit has a
  corresponding `main` ancestor closes it — worth building before the first
  hotfix rather than after.
- **Release granularity.** One release bundling dbt and DAG changes means one
  CAB, but a prod rollback reverts both. Given how differently they fail, there
  is a case for separate release trains. Not obviously right — it doubles the
  CAB load, which is the expensive part — but it should be a decision rather
  than a default.
- **How long UAT holds a release.** Longer soak means better evidence for CAB
  but a longer-lived mutable release branch and more digest churn. Somewhere
  around one full week of arrivals is the usual balance point; worth setting
  explicitly since nothing else forces it.
