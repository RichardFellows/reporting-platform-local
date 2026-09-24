"""One dbt invocation on a build branch, and the evidence it leaves.

The Airflow build renders one Cosmos task per model; run by hand it is ONE
`dbt build`. Both end the same way: each invocation's `manifest.json` and
`run_results.json` are archived under the run before the next invocation
overwrites `target/`, and `wap.publish` refuses to merge a build whose
archive is incomplete. `archive_invocation` is that step for both drivers.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys

from reporting_platform.transform.wap import PURPOSES, purpose_of, run_key

log = logging.getLogger(__name__)

SELECT = {purpose: f"path:models/{purpose}" for purpose in PURPOSES}

# The task id a hand-run build archives its single invocation under. The
# Cosmos tasks are `dbt.<model>_run` / `dbt.dbt_test`; this cannot collide.
CLI_TASK_ID = "dbt.build"


def target() -> str:
    """DBT_TARGET, refused unless it is a Spark target.

    The failure it prevents is silent: a non-Spark engine ignores the
    `nessie_ref` var, so the build SUCCEEDS having written to main with no
    branch and no audit. See docs/DECISIONS.md#dbt-target-guard
    """
    value = os.environ.get("DBT_TARGET", "spark_local")
    if not value.startswith("spark"):
        raise RuntimeError(
            f"DBT_TARGET is {value!r}, which is not a Spark target. Builds "
            f"must run on Spark: the branch each build opens is passed to dbt "
            f"as the `nessie_ref` var, and a non-Spark engine ignores it and "
            f"writes to the default branch, bypassing write-audit-publish "
            f"without failing.")
    return value


def archive_invocation(run_id: str, task_id: str, try_number: int) -> dict:
    """Retain one dbt invocation's artifacts, then project its test results
    into `registry.validation_result`.

    Runs for a FAILED invocation too, so a failing test's evidence is captured
    exactly like a passing one, before publication is even considered.
    Raises if dbt left no artifacts: publication would refuse later anyway,
    and here the reason is still nameable.
    """
    from reporting_platform.registry import artifacts, validation

    target_path = os.environ.get("DBT_TARGET_PATH")
    result = artifacts.archive(run_id, task_id, try_number,
                               target_path=target_path)
    if result["missing"]:
        raise RuntimeError(
            f"{task_id}: dbt did not produce required artifacts "
            f"{result['missing']}")
    log.info("retained dbt artifacts for %s at %s", task_id, result["reference"])
    try:
        captured = validation.capture_dbt_task(
            run_id, task_id, try_number, target_path=target_path,
            evidence_ref=result["reference"])
        log.info("captured %d validation result(s) for %s", len(captured), task_id)
    except Exception as exc:                                    # noqa: BLE001
        # Best-effort, like every other registry write on this path: the
        # artifacts just archived remain the authoritative evidence.
        log.warning("could not capture validation results for %s: %s",
                    task_id, f"{type(exc).__name__}: {exc}")
    return result


def build(branch: str, *, select: str | None = None,
          full_refresh: bool = False, vars_: dict | None = None) -> dict:
    """`dbt build` the branch's layer ON the branch, then archive the evidence.

    `select` defaults to the branch's purpose -- `path:models/prepared` for a
    prepared build. Returns `{"ok", "returncode", "attempt"}`; `attempt` is
    what `wap.publish(dbt_attempts=[...])` needs. The caller decides between
    publish and fail: this never merges.

    dbt's own output goes straight to this process's STDERR, so a person
    watches the build as it happens and stdout stays the caller's -- the
    CLIs print one JSON result there.
    """
    project = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")
    profiles = os.environ.get("DBT_PROFILES_DIR", project)
    dbt = os.environ.get("DBT_EXECUTABLE_PATH") or shutil.which("dbt") or "dbt"
    select = select or SELECT[purpose_of(branch)]
    # THE WHOLE OF WRITE-AUDIT-PUBLISH IS THIS VAR: profiles.yml threads
    # `var('nessie_ref')` into spark.sql.catalog.lakehouse.ref, so the entire
    # build lands on `branch`.
    all_vars = {**(vars_ or {}), "nessie_ref": branch}
    cmd = [dbt, "build", "--project-dir", project, "--profiles-dir", profiles,
           "--target", target(), "--select", select,
           # Exposures are documentation and build nothing.
           "--exclude", "resource_type:exposure",
           "--vars", json.dumps(all_vars)]
    if full_refresh:
        cmd.append("--full-refresh")
    from reporting_platform.registry import artifacts

    run_id = run_key(branch)
    # Numbered BEFORE dbt runs: a rebuild of the same branch is a new
    # attempt, and `archive` refuses to overwrite an earlier one.
    attempt = (CLI_TASK_ID,
               max(artifacts.attempts(run_id, CLI_TASK_ID), default=0) + 1)
    log.info("running %s", " ".join(cmd))
    code = subprocess.run(cmd, check=False, stdout=sys.stderr).returncode
    archive_invocation(run_id, *attempt)
    return {"ok": code == 0, "returncode": code, "attempt": attempt}


# What a node in run_results.json may say and still be publishable. `skipped`
# is NOT here: dbt skips what sits downstream of a failure, so a skip is a
# model that was never built.
PUBLISHABLE_STATUSES = frozenset({"success", "pass", "warn"})


def verified_attempt(branch: str) -> tuple[str, int]:
    """The newest archived hand-run attempt on `branch`, if dbt's OWN results
    say every node in it passed. Raises naming the nodes that did not.

    This is what lets `publish` run as a separate command from `build`: it
    publishes on the evidence in the archive, not on the word of whoever
    invoked it. Airflow's equivalent is the task states Cosmos reports.
    """
    from reporting_platform.registry import artifacts

    run_id = run_key(branch)
    tries = artifacts.attempts(run_id, CLI_TASK_ID)
    if not tries:
        raise RuntimeError(
            f"no archived dbt build for {branch} (run {run_id}); run "
            f"`transform dbt {branch}` first")
    results = artifacts.read_run_results(run_id, CLI_TASK_ID, tries[-1])
    bad = [f"{r.get('unique_id')}={r.get('status')}"
           for r in results.get("results", [])
           if r.get("status") not in PUBLISHABLE_STATUSES]
    if bad or not results.get("results"):
        raise RuntimeError(
            f"dbt attempt {tries[-1]} on {branch} did not pass, refusing to "
            f"publish: {', '.join(bad[:20]) or 'it ran no nodes'}"
            + (f" (+{len(bad) - 20} more)" if len(bad) > 20 else ""))
    return (CLI_TASK_ID, tries[-1])


def build_layer(purpose: str, label: str, *, change_ref: str | None = None,
                select: str | None = None, full_refresh: bool = False,
                publish: bool = True) -> dict:
    """One whole build: open a branch, dbt build on it, then publish -- or,
    if dbt failed, close the run as failed and KEEP the branch.

    `{"ok": bool, "published": bool, "branch", ...}` -- `ok` is whether dbt
    passed; with `published` true the rest is `wap.publish`'s result. `publish=False` stops after the audit and leaves
    the run open, for `transform publish` or `transform fail` later.
    """
    from reporting_platform.transform import wap

    branch = wap.open_build(purpose, label, change_ref=change_ref)
    log.info("%s build on %s", purpose, branch)
    result = build(branch, select=select, full_refresh=full_refresh)
    if not result["ok"]:
        wap.fail_build(branch, f"dbt build exited {result['returncode']}")
        return {"ok": False, "published": False, "branch": branch,
                "reason": "dbt build failed; branch kept"}
    if not publish:
        return {"ok": True, "published": False, "branch": branch,
                "reason": "audit only; branch kept, run left open"}
    return {"ok": True, "published": True, **wap.publish(
        branch, dbt_attempts=[verified_attempt(branch)], change_ref=change_ref)}
