"""Write-audit-publish for one dbt build: open a branch, publish it, or fail it.

THE BUILD'S BRANCH, RUN RECORD AND PUBLICATION, WITHOUT AN ORCHESTRATOR. This
was the body of three tasks in `airflow/dags/dbt_builds.py`; it is here so
the DAG and `python -m reporting_platform.transform` run ONE implementation.
A second copy of the publish sequence is how a hand-run build ends up merging
without the lifecycle gate, or tagging without a version.

The shape of a build, whoever drives it:

    branch = open_build(purpose, label)          # branch off main + run row
    ...dbt builds on `branch`...                 # transform.dbt, or Cosmos
    publish(branch, purpose, dbt_attempts=...)   # audited -> main, tags
      or fail_build(branch, reason)              # branch KEPT for diagnosis

Nothing here knows about Airflow. What the DAG supplies from its own context
(its dag id, its run id, the task attempts it saw succeed) arrives as
arguments.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone

PURPOSES = ("prepared", "reporting")

log = logging.getLogger(__name__)


def branch_name(purpose: str, label: str, *, now: datetime | None = None) -> str:
    """`build/<purpose>/<utc date>/<slug of label>`.

    Slugified rather than sliced. `run_id[-24:]` cut mid-token and produced
    branch names like `-08-21T101555.6748970000` -- unreadable, and it
    discarded the part identifying the run.
    """
    if purpose not in PURPOSES:
        raise ValueError(f"purpose must be one of {PURPOSES}, got {purpose!r}")
    slug = re.sub(r"[^A-Za-z0-9]+", "-", label).strip("-")[:40]
    if not slug:
        raise ValueError(f"label {label!r} has nothing left to name a branch with")
    return f"build/{purpose}/{(now or datetime.now(timezone.utc)):%Y-%m-%d}/{slug}"


def run_key(branch: str) -> str:
    """The registry run id for a build branch.

    Derived from the branch rather than recomputed from the caller's run id,
    so opening and publishing cannot slugify differently and end up writing
    two rows for one run. Prefixed with the purpose because the prepared and
    reporting builds of one dataset-triggered Airflow run slugify the SAME
    run id, and their two runs would otherwise collide on the primary key.
    """
    return f"{branch.split('/')[1]}-{branch.rsplit('/', 1)[-1]}"


def purpose_of(branch: str) -> str:
    purpose = branch.split("/")[1] if branch.startswith("build/") else ""
    if purpose not in PURPOSES:
        raise ValueError(f"{branch!r} is not a build branch (build/<purpose>/...)")
    return purpose


def open_build(purpose: str, label: str, *, change_ref: str | None = None,
               dag_id: str | None = None,
               airflow_run_id: str | None = None) -> str:
    """Branch off `main` and open the run record. Returns the branch.

    exist_ok: a retry must not be blocked by the branch its own previous
    attempt left behind. `fail_build` deliberately retains it, so without
    this every retry 409s and a retry is a trap rather than a safety net.
    """
    from reporting_platform.common.context import Nessie

    branch = branch_name(purpose, label)
    Nessie().create_branch(branch, from_ref="main", exist_ok=True)
    _open_run(branch, purpose, change_ref=change_ref, dag_id=dag_id,
              airflow_run_id=airflow_run_id)
    return branch


def _open_run(branch: str, purpose: str, *, change_ref, dag_id,
              airflow_run_id) -> None:
    """REQ-400/REQ-404. Record the run BEFORE it builds anything.

    Opened here rather than at publish because a run that fails is the one
    most worth having a record of -- and because `code_ref` and
    `dbt_manifest_ref` describe the code that is about to run, which is only
    knowable now. Best-effort in the same sense `normalize`'s registry write
    is: the registry is a record over work that happens whether or not the
    row lands. `publish` re-opens the run if this failed, so a lost row here
    costs the start time and nothing else.
    """
    from reporting_platform.common.context import (
        ENV, check_project_drift, code_ref, dbt_manifest_ref,
        deployment_provenance,
    )
    from reporting_platform.registry import artifacts, runs

    # BEFORE the run row and before any model builds. In a controlled
    # environment this RAISES, and failing here is the point: the project on
    # disk is not the one the pipeline deployed, so anything published from
    # it would be attributed to a commit that did not produce it. Elsewhere
    # it returns a description and the build proceeds -- `dev` diverges by
    # design. NOT inside the try below: this refusal must not be swallowed.
    drift = check_project_drift()
    if drift:
        log.warning("transformation project drift: %s", drift)

    run_id = run_key(branch)
    try:
        ref, kind = code_ref()
        runs.open_run(
            run_id, purpose, branch,
            environment=ENV, code_ref=ref, code_ref_kind=kind,
            dbt_manifest_ref=dbt_manifest_ref(),
            dbt_artifacts_ref=artifacts.reference(run_id),
            dag_id=dag_id, airflow_run_id=airflow_run_id,
            change_ref=change_ref or None,
            provenance=deployment_provenance())
    except Exception as exc:                                    # noqa: BLE001
        log.warning("could not open the run record for %s: %s", run_id,
                    f"{type(exc).__name__}: {exc}")


def publish(branch: str, *, dbt_attempts: list[tuple[str, int]],
            change_ref: str | None = None) -> dict:
    """Merge the audited branch into main, and RECORD THE PUBLICATION.

    Call it only when every dbt model AND test on `branch` succeeded -- that
    is the "publish" in write-audit-publish, and the reason a failed test can
    never move `main`. `dbt_attempts` is every (task id, try number) whose
    artifacts `transform.dbt.archive_invocation` retained; publication
    refuses without them.

    The order matters:

      0. THE EVIDENCE IS VERIFIED: every dbt invocation's artifacts retained.
      1. THE INPUT SET IS READ OFF THE BRANCH, before the merge, while it is
         still exactly what was audited. REQ-400.
      1b. THE AS-AT LIFECYCLE GATE runs between them, because it is the only
         check here that must be able to stop the publication -- and after
         the merge it could not. REQ-500/502.
      2. The merge carries a COMMIT MESSAGE naming the purpose, the COB date
         and the change reference. REQ-405.
      3. A REPORTING build cuts one tag PER REPORT --
         `published/<report>/<bd>/<run_id>` -- and allocates that report's
         next version for the as-at date. REQ-401/REQ-403.
      4. The run is closed with the merged hash.

    A prepared build publishes no report and cuts no tag: it is a run, it is
    recorded as one, and calling it a publication is the mistake the ingest
    DAG used to make.
    """
    from reporting_platform.common.context import Nessie, published_tag, reports
    from reporting_platform.common.spark_task import run as spark_run
    from reporting_platform.registry import artifacts, runs

    purpose = purpose_of(branch)
    run_id = run_key(branch)
    change_ref = (change_ref or "").strip() or None

    # 0. Every successful dbt invocation must have copied its manifest and
    # run_results before anything is merged.
    if not dbt_attempts:
        raise RuntimeError("no successful dbt invocations to publish")
    artifacts.require_complete(run_id, dbt_attempts)

    # 1. What this build read, from the branch it built on. In its own driver
    # process (spark_task.run), because the caller may be an Airflow task and
    # a JVM in it would keep it alive. See docs/DECISIONS.md#spark-in-a-subprocess
    inputs, cob_date = {}, None
    try:
        inputs = spark_run("run-inputs", branch)
        cob_date = inputs.get("max_cob_date")
        if inputs.get("unreadable"):
            # NOT fatal, deliberately: the tables are audited and correct, and
            # refusing to publish them because their provenance could not be
            # enumerated would take the platform down to protect a record of
            # it. Loud, though.
            log.error("run %s: input set INCOMPLETE -- %s", run_id,
                      inputs["unreadable"])
        runs.record_inputs(
            run_id, [(feed, delivery) for feed, delivery in
                     inputs.get("inputs", [])])
    except Exception as exc:                                    # noqa: BLE001
        log.error("run %s: could not record the input set: %s", run_id,
                  f"{type(exc).__name__}: {exc}")

    # 1b. THE AS-AT LIFECYCLE GATE, and it must be HERE -- after the input
    # set (which makes the as-at date knowable) and BEFORE the merge. A
    # LifecycleRefused is deliberately NOT caught: it is the one error here
    # that must stop the publication, with main untouched and the branch
    # retained.
    carried_forward: list[dict] = []
    if purpose == "reporting" and cob_date:
        from reporting_platform.registry import lifecycle

        candidate = {(feed, delivery) for feed, delivery
                     in inputs.get("inputs", [])}
        for report in sorted(reports()):
            verdict = lifecycle.check_publishable(
                report, date.fromisoformat(cob_date), candidate)
            if verdict["carried_forward"]:
                # The policy said carry forward, so the publication proceeds
                # -- but "we knowingly did not restate this" belongs on the
                # run record, not in a log that is swept with the rest.
                carried_forward.append(verdict)

    # 2. The merge, with something written on it.
    n = Nessie()
    message = (f"publish({purpose}): {cob_date or 'no COB date'}"
               f" run {run_id}"
               + (f" [{change_ref}]" if change_ref else ""))
    n.merge(branch, into="main", message=message,
            properties={"purpose": purpose, "run_id": run_id,
                        "change_ref": change_ref or "",
                        "cob_date": cob_date or ""})
    merged = n.get_reference("main")["reference"]["hash"]
    n.delete_reference(branch)

    # 3. The pins, one per report, and the version each one is.
    published: list[dict] = []
    version_errors: list[str] = []
    if purpose == "reporting" and cob_date:
        as_at = date.fromisoformat(cob_date)
        for report in sorted(reports()):
            tag = published_tag(report, as_at, run_id)
            try:
                n.create_tag(tag, from_ref="main")
            except Exception as exc:                            # noqa: BLE001
                # A rerun finds its own tag already there. allocate_version
                # is idempotent on the tag, so the version is not minted
                # twice either.
                log.warning("tag %s: %s", tag, str(exc)[:200])
            try:
                version = runs.allocate_version(report, as_at, run_id, tag)
            except Exception as exc:                            # noqa: BLE001
                # NOT fatal -- the merge has happened and the tag is cut, so
                # the publication is real whether or not it was numbered. But
                # a tag with no version row is an inconsistency somebody has
                # to explain later, so it is carried onto the run record.
                detail = f"{type(exc).__name__}: {exc}"
                log.error("could not allocate a version for %s %s: %s",
                          report, as_at, detail)
                version_errors.append(f"{report}: {detail}")
                version = None
            published.append({"report": report, "tag": tag,
                              "version": version})
    elif purpose == "reporting":
        log.error(
            "reporting build on %s published no report: the input set "
            "yielded no COB date, so there is nothing to pin an as-at date "
            "to. See the run-inputs error above.", branch)

    # 4. Close the run.
    try:
        notes = list(version_errors)
        notes += [
            f"carried forward: {v['report']} {v['as_at_date']} is "
            f"{v['state']} with {len(v.get('added') or [])} added and "
            f"{len(v.get('removed') or [])} removed delivery(ies); "
            f"v{v.get('published_version')} stands"
            for v in carried_forward]
        runs.finish_run(
            run_id, runs.PUBLISHED, merged_hash=merged,
            cob_date=date.fromisoformat(cob_date) if cob_date else None,
            error="; ".join(notes) or None)
    except Exception as exc:                                    # noqa: BLE001
        log.warning("could not close run %s: %s", run_id,
                    f"{type(exc).__name__}: {exc}")

    return {"merged": branch, "run_id": run_id, "hash": merged,
            "cob_date": cob_date,
            "inputs": len(inputs.get("inputs", [])),
            "carried_forward": carried_forward,
            "published": published}


def fail_build(branch: str, reason: str) -> str:
    """The build failed: KEEP the branch, and say so on the run record.

    The branch is deliberately NOT deleted -- it holds the exact bad data for
    diagnosis. Retention sweeps abandoned `build/*` branches after 120h
    (retention.yml -> references.working_branches), deliberately longer than
    `ingest/*`, so a build that fails at 22:00 on a Friday is still there on
    Monday morning.

    THE RUN RECORD HAS TO SAY IT FAILED, or the only rows in it are the
    successes and "how often does this build fail" is a question about a
    table that only records wins.
    """
    from reporting_platform.registry import runs

    log.error(
        "build failed (%s); branch %s retained for inspection. Query it with "
        "the nessie_ref var, or delete it by hand once you are done: it is "
        "swept automatically after 120h.", reason, branch)
    try:
        runs.finish_run(run_key(branch), runs.FAILED, error=reason)
    except Exception as exc:                                    # noqa: BLE001
        log.warning("could not close the failed run: %s",
                    f"{type(exc).__name__}: {exc}")
    return branch
