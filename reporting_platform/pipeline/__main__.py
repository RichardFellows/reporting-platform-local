"""The whole pipeline, one step at a time, with no Airflow and no Spark cluster.

    python -m reporting_platform.pipeline check        # what it will talk to
    python -m reporting_platform.pipeline setup        # registry schema, dbt deps
    python -m reporting_platform.pipeline run FILE...  # land -> raw -> prepared -> reporting
    python -m reporting_platform.pipeline run FILE... --through raw
    python -m reporting_platform.pipeline ingest|transform ...  # one stage

GLUE, NOT A THIRD IMPLEMENTATION. Each stage is its component's own entry
point, runnable on its own and with the same code the Airflow DAGs call:

    land       python -m reporting_platform.ingest land FILE...     (ingest)
    ingest     python -m reporting_platform.ingest ingest FEED...   (ingest)
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
    and dbt's packages -- without them `dbt build` cannot compile a
    `dbt_utils` test. Idempotent."""
    from reporting_platform.registry import db

    db.ensure_schema()
    project = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")
    subprocess.run(["dbt", "deps", "--project-dir", project, "--profiles-dir",
                    os.environ.get("DBT_PROFILES_DIR", project)],
                   check=True, stdout=sys.stderr)
    return {"registry_schema": "ensured", "dbt_deps": "installed"}


def run(files: list[Path], *, through: str, change_ref: str | None,
        label: str | None) -> tuple[int, dict]:
    from reporting_platform.common.context import new_run_id
    from reporting_platform.ingest import steps
    from reporting_platform.transform.dbt import build_layer

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
    sub.add_parser("setup", help="registry schema + dbt deps (idempotent)")
    sub.add_parser("ingest", help="-> python -m reporting_platform.ingest ...")
    sub.add_parser("transform", help="-> python -m reporting_platform.transform ...")
    r = sub.add_parser("run", help="land -> ingest -> prepared -> reporting")
    r.add_argument("files", nargs="+", type=Path)
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
    code, report = run(a.files, through=a.through, change_ref=a.change_ref,
                       label=a.label)
    print(json.dumps(report, indent=2, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
