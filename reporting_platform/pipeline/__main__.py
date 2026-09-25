"""The whole pipeline, one step at a time, with no Airflow and no Spark cluster.

    python -m reporting_platform.pipeline check        # what it will talk to
    python -m reporting_platform.pipeline setup        # registry schema, raw tables, dbt deps
    python -m reporting_platform.pipeline run FILE...  # land -> raw -> prepared -> reporting
    python -m reporting_platform.pipeline run FILE... --through raw
    python -m reporting_platform.pipeline run --transport [MARKER...]  # Transports, not files
    python -m reporting_platform.pipeline ingest|transform ...  # one stage

GLUE, NOT A THIRD IMPLEMENTATION. Each stage is its component's own entry
point, runnable on its own and with the same code the Airflow DAGs call:

    land       python -m reporting_platform.ingest land FILE...     (ingest)
    ingest     python -m reporting_platform.ingest ingest FEED...   (ingest)
      or       python -m reporting_platform.ingest transport ...    (ingest)
    prepared   python -m reporting_platform.transform build prepared  (dbt)
    reporting  python -m reporting_platform.transform build reporting (dbt)

This module only decides the order and stops at the first stage that fails.
A failed build keeps its branch and leaves `main` where it was.

Endpoints are the platform's ordinary settings (S3_ENDPOINT, NESSIE_URI,
REPORTING_WAREHOUSE, REPORTING_LANDING, REGISTRY_DSN), so local MinIO/Nessie
and a deployed S3/Nessie differ only in environment. Spark runs wherever
PLATFORM_EXECUTION says -- `embedded` is in this process, no cluster.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

STAGES = ("raw", "prepared", "reporting")

log = logging.getLogger("pipeline")


def check() -> dict:
    """Resolve every endpoint and reach each one. Reports what it could not
    reach as `unreachable`, never as empty -- and exits 1 on any."""
    from reporting_platform.common import settings

    out: dict = {"environment": settings.env(),
                 "execution": settings.execution(),
                 "spark_master": os.environ.get("SPARK_MASTER", "(unset)"),
                 "dbt_target": os.environ.get("DBT_TARGET", "spark_local")}
    missing = settings.missing()
    if missing:
        out["missing_settings"] = missing
        return out

    def probe(name, fn):
        try:
            out[name] = {"ok": True, **fn()}
        except Exception as exc:                                # noqa: BLE001
            out[name] = {"ok": False,
                         "unreachable": f"{type(exc).__name__}: {exc}"[:300]}

    def s3():
        from reporting_platform.ingest.arrival import _client  # noqa: PLC2701

        bucket = settings.bucket_of(settings.warehouse())
        _client().head_bucket(Bucket=bucket)
        return {"endpoint": settings.s3_endpoint(), "bucket": bucket,
                "landing": settings.landing()}

    def nessie():
        from reporting_platform.common.context import Nessie

        main = Nessie().get_reference("main")["reference"]
        return {"uri": settings.nessie_uri(), "main": main["hash"][:12]}

    def registry():
        from reporting_platform.registry import db

        with db.connect(ensure=False) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"dsn_host": settings.registry_dsn().rsplit("@", 1)[-1]}

    def jars():
        paths = [p for p in os.environ.get("PLATFORM_DRIVER_JARS", "").split(",") if p]
        absent = [p for p in paths if not os.path.isfile(p)]
        if not paths or absent:
            raise FileNotFoundError(f"PLATFORM_DRIVER_JARS: {absent or 'unset'}")
        return {"count": len(paths)}

    probe("s3", s3)
    probe("nessie", nessie)
    probe("registry", registry)
    probe("driver_jars", jars)
    return out


def setup() -> dict:
    """What `airflow-init` does that this path needs: the registry schema,
    a raw table for every declared feed -- or the prepared build fails on
    the first feed that has not delivered yet, and publishes nothing
    (docs/DECISIONS.md#a-declared-feed-has-a-raw-table-before-it-delivers) --
    and dbt's packages, without which `dbt build` cannot compile a
    `dbt_utils` test. Idempotent."""
    from reporting_platform.ingest.migrate_raw import migrate
    from reporting_platform.registry import db

    db.ensure_schema()
    raw = migrate()
    project = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")
    subprocess.run(["dbt", "deps", "--project-dir", project, "--profiles-dir",
                    os.environ.get("DBT_PROFILES_DIR", project)],
                   check=True, stdout=sys.stderr)
    return {"registry_schema": "ensured",
            "raw_tables": {k: raw[k] for k in ("created", "migrated",
                                               "already_current")},
            "dbt_deps": "installed"}


def run(files: list[Path], *, through: str, change_ref: str | None,
        label: str | None) -> tuple[int, dict]:
    from reporting_platform.common.context import new_run_id
    from reporting_platform.ingest import steps

    report: dict = {}
    label = label or f"pipeline-{new_run_id()}"

    log.info("== land: %d file(s)", len(files))
    landed = steps.land(files)
    report["land"] = landed
    feeds = sorted({r["feed"] for r in landed
                    if r["status"] in ("landed", "conformed", "duplicate")
                    and r.get("feed")})
    refused = [r for r in landed
               if r["status"] not in ("landed", "conformed", "duplicate")]
    if refused:
        log.error("land: %d file(s) not landed -- see report", len(refused))
    if not feeds:
        return 1, report

    log.info("== ingest: %s", ", ".join(feeds))
    report["ingest"] = {f: steps.ingest(f) for f in feeds}
    if any("error" in r for rs in report["ingest"].values() for r in rs):
        log.error("ingest: a delivery failed -- not building on it")
        return 1, report
    return _build(report, through=through, change_ref=change_ref,
                  label=label, refused=bool(refused))


def run_transports(markers: list[str], *, cob_dates: list[str] | None,
                   through: str, change_ref: str | None,
                   label: str | None) -> tuple[int, dict]:
    """Transports -> raw -> prepared -> reporting. The Transport twin of `run`.

    `markers` names the Transports; empty means every one not yet in raw
    over `cob_dates` (None: all of `received/`). Nothing to land: DCM, or
    `runner transport publish`, already completed them under `received/`.

    THE SAME RULE `run` APPLIES TO A QUARANTINED FILE. A REFUSED Transport --
    wrong checksum, missing control, an identity conflict -- never reached
    raw, so building on the ones that did is safe: it is reported, the
    builds go ahead, and the exit is 1. Any OTHER failure stops before the
    builds, as a failed file ingest does, because raw may be missing
    something that was meant to be there.
    """
    from reporting_platform.common.context import new_run_id
    from reporting_platform.ingest import transport_steps

    report: dict = {}
    label = label or f"pipeline-{new_run_id()}"
    unreadable: list = []
    if not markers:
        log.info("== pending Transports: %s",
                 "all of received/" if cob_dates is None
                 else f"{len(cob_dates)} COB date(s)")
        found = transport_steps.pending(cob_dates)
        markers, unreadable = found["marker_keys"], found["unreadable"]
        report["unreadable"] = unreadable
        if unreadable:
            log.error("%d Transport marker(s) could not be read -- see report",
                      len(unreadable))
    if not markers:
        log.info("nothing to ingest")
        return (1 if unreadable else 0), report

    log.info("== transport ingest: %d Transport(s)", len(markers))
    results = [transport_steps.ingest_transport(m) for m in markers]
    report["transport"] = results
    failed = [r for r in results if "error" in r and not r["refused"]]
    refused = [r for r in results if "error" in r and r["refused"]]
    if failed:
        log.error("transport ingest: %d failed -- not building on it",
                  len(failed))
        return 1, report
    if refused:
        log.error("transport ingest: %d refused (not in raw) -- building on "
                  "the rest", len(refused))
    if len(refused) == len(results):
        return 1, report
    return _build(report, through=through, change_ref=change_ref,
                  label=label, refused=bool(refused or unreadable))


def _build(report: dict, *, through: str, change_ref: str | None,
           label: str, refused: bool) -> tuple[int, dict]:
    """prepared then reporting, stopping at `through` or the first failure.
    `refused` only sets the exit code: something upstream needs a look."""
    from reporting_platform.transform.dbt import build_layer

    if through == "raw":
        return (1 if refused else 0), report
    for purpose in ("prepared", "reporting"):
        log.info("== %s build", purpose)
        result = build_layer(purpose, label, change_ref=change_ref)
        report[purpose] = result
        if not result["ok"]:
            log.error("%s build failed; branch %s kept, main unchanged",
                      purpose, result["branch"])
            return 1, report
        if through == purpose:
            break
    return (1 if refused else 0), report


# Each stage's own CLI, reachable through this one. So a container whose
# entrypoint is this module can run any single step without overriding the
# entrypoint -- which, for the Airflow image, skips the step that makes an
# arbitrary uid a real user, and Spark then dies on `?/.ivy2`.
COMPONENT_CLIS = {
    "ingest": "reporting_platform.ingest.__main__",
    "transform": "reporting_platform.transform.__main__",
    # The PRODUCER's CLI, what DCM runs: `transport publish` completes a
    # Transport under received/, so the runner can make its own to take in.
    # Its storage settings fall back to S3_ENDPOINT / REPORTING_WAREHOUSE /
    # REPORTING_RECEIVED_PREFIX, which the runner already sets.
    "transport": "reporting_transport.cli",
}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] in COMPONENT_CLIS:
        import importlib

        return importlib.import_module(COMPONENT_CLIS[argv[0]]).main(argv[1:])

    p = argparse.ArgumentParser(prog="python -m reporting_platform.pipeline",
                                description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="resolve and reach S3, Nessie, the registry")
    sub.add_parser("setup", help="registry schema + raw tables + dbt deps "
                                 "(idempotent)")
    sub.add_parser("ingest", help="-> python -m reporting_platform.ingest ...")
    sub.add_parser("transform", help="-> python -m reporting_platform.transform ...")
    sub.add_parser("transport", help="-> python -m reporting_transport ... (publish)")
    r = sub.add_parser("run", help="land -> ingest -> prepared -> reporting")
    r.add_argument("files", nargs="*",
                   help="files to land; with --transport, _COMPLETE.json keys")
    r.add_argument("--transport", action="store_true",
                   help="take Transports from received/ instead of landing "
                        "files; with no keys, every one not yet in raw")
    scope = r.add_mutually_exclusive_group()
    scope.add_argument("--window", type=int, metavar="DAYS",
                       help="--transport, no keys: the last DAYS COB dates "
                            "(default 7)")
    scope.add_argument("--cob-date", action="append", metavar="YYYY-MM-DD",
                       help="--transport, no keys: these COB dates")
    scope.add_argument("--all", action="store_true", dest="full_sweep",
                       help="--transport, no keys: all of received/")
    r.add_argument("--through", choices=STAGES, default="reporting",
                   help="stop after this layer (default: reporting)")
    r.add_argument("--change-ref", help="the ticket authorising the builds")
    r.add_argument("--label", help="names the build branches")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if a.cmd == "check":
        out = check()
        print(json.dumps(out, indent=2))
        bad = out.get("missing_settings") or [
            k for k, v in out.items() if isinstance(v, dict) and not v["ok"]]
        return 1 if bad else 0
    if a.cmd == "setup":
        print(json.dumps(setup(), indent=2))
        return 0
    if a.transport:
        if a.files and (a.window or a.cob_date or a.full_sweep):
            p.error("name the markers, or give a window -- not both")
        from reporting_platform.ingest.transport_steps import window_cob_dates

        cob_dates = (None if a.full_sweep
                     else a.cob_date or window_cob_dates(a.window or 7))
        code, report = run_transports(
            [str(f) for f in a.files], cob_dates=cob_dates, through=a.through,
            change_ref=a.change_ref, label=a.label)
    else:
        if not a.files:
            p.error("name the files to land, or --transport")
        if a.window or a.cob_date or a.full_sweep:
            p.error("--window/--cob-date/--all go with --transport")
        code, report = run([Path(f) for f in a.files], through=a.through,
                           change_ref=a.change_ref, label=a.label)
    print(json.dumps(report, indent=2, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
