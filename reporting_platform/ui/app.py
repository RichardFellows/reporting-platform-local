"""HTTP surface of the feed console.

Every route is a thin wrapper over `registry`, `scaffold`, `feeddata` or
`orchestration`. Nothing decides anything here, on purpose: the console must
not become a place where platform behaviour is defined, because then there
would be two definitions of it and the file-based one would be the one people
read.

There is no authentication. This is the local reference stack -- Airflow's own
UI on 8081 is admin/admin and MinIO's on 19001 is minioadmin. Deploying this
anywhere shared means putting a real identity layer in front of it; it writes
source files and triggers builds.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from reporting_platform.common.context import CATALOG, feeds
from . import (dbt_check, feeddata, feedtest, jobs, orchestration,
               registry, sampledata, scaffold)
from .registry import FeedSpec, FeedValidationError

STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Feed console", docs_url="/api/docs", redoc_url=None)


@app.exception_handler(FeedValidationError)
async def _validation_handler(_request, exc: FeedValidationError):
    # 422 with the per-field errors intact, so the form can put each message
    # next to the field it belongs to instead of showing one joined string.
    return JSONResponse(status_code=422, content={"errors": exc.errors})


@app.exception_handler(feeddata.DataError)
async def _data_handler(_request, exc: feeddata.DataError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(sampledata.GenerationError)
async def _generation_handler(_request, exc: sampledata.GenerationError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(jobs.JobBusy)
async def _busy_handler(_request, exc: jobs.JobBusy):
    # 409, with the running job's id, so the UI can attach to it rather than
    # telling the user to try again later.
    return JSONResponse(status_code=409,
                        content={"detail": str(exc), "job_id": exc.job_id})


@app.exception_handler(orchestration.AirflowError)
async def _airflow_handler(_request, exc: orchestration.AirflowError):
    return JSONResponse(status_code=502, content={"detail": str(exc)})


def _feed_or_404(name: str):
    try:
        return feeds()[name]
    except KeyError:
        raise HTTPException(404, f"no such feed: {name}") from None


def _summary(fd) -> dict[str, Any]:
    """One feed, as the console lists it.

    Includes the SCAFFOLD STATUS rather than assuming a registered feed is a
    complete one. A feed can be in feeds.yml with no prepared model -- that is
    what a half-finished onboarding looks like, and it reaches raw and stops
    there with nothing red. The console names the missing pieces.
    """
    dag_id = f"ingest_{fd.name}"
    # Airflow being down must not take the feed list down with it. Registering
    # a feed, scaffolding it and landing sample data all work with the
    # scheduler stopped, and the console is the natural place to be while
    # waiting for it to come back -- so an unreachable Airflow degrades this to
    # "unknown" and is reported once, in the header, rather than turning every
    # feed read into a 502.
    try:
        dag = orchestration.get_dag(dag_id)
        dag_known = dag is not None
    except orchestration.AirflowError:
        dag, dag_known = None, None
    return {
        "name": fd.name,
        "description": fd.description,
        "source_system": fd.source_system,
        "filename_pattern": fd.filename_pattern,
        "business_key": list(fd.business_key),
        "columns": list(fd.columns),
        # Stored overrides first, inference only as the fallback. This USED to
        # be a bare `infer_types(fd.columns)`, which meant the endpoint the
        # edit form is populated from re-guessed every type on every read --
        # so a type the user chose was shown back to them as whatever the
        # column name suggested, and `readTypes()` then posted that guess
        # back. The choice survived exactly one scaffold call. See
        # Feed.column_types in common/context.py for what that cost.
        "column_types": scaffold.resolve_types(fd),
        "expected_min_rows": fd.expected_min_rows,
        "cadence": fd.cadence,
        "completeness": fd.completeness,
        "schema_drift": fd.schema_drift,
        "raw_table": fd.raw_table,
        "asset_uri": fd.asset_uri,
        "landing_prefix": f"{fd.landing_prefix}/{fd.name}/",
        "dag_id": dag_id,
        # False means Airflow is up and has not parsed the new feed yet -- a
        # normal transient state for the first ~30s after a feed is added, not
        # an error. None means Airflow could not be asked at all.
        "dag_known": dag_known,
        "dag_paused": dag.get("is_paused") if dag else None,
        "scaffold": scaffold.status(fd.name),
    }


# --------------------------------------------------------------------- feeds
@app.get("/api/feeds")
def api_feeds():
    return {"catalog": CATALOG,
            "feeds": [_summary(fd) for fd in feeds().values()]}


@app.get("/api/feeds/{name}")
def api_feed(name: str):
    return _summary(_feed_or_404(name))


@app.post("/api/feeds")
def api_create_feed(payload: dict):
    """Register a feed and scaffold its dbt files in one call.

    Order is registry first, scaffold second, and it matters: the scaffold
    reads nothing from the registry file, but if it did run first and the
    registry write then failed, the dbt project would reference a source that
    does not exist and every dbt invocation -- including ones for unrelated
    feeds -- would fail to parse.
    """
    spec = FeedSpec.from_payload(payload)
    # Reduce the form's full type map to just the genuine disagreements, and
    # PERSIST them: that is what makes the choice outlive this request. The
    # scaffold below then builds from the same resolved map the generator will
    # read later, rather than from a value only this call can see.
    spec.column_types = scaffold.overrides_only(spec.columns, spec.column_types)
    registry.validate(spec, existing=set(feeds()))
    registry.add(spec)

    types = scaffold.resolve_types(feeds()[spec.name])
    steps = scaffold.scaffold(spec, {c: types.get(c, "string") for c in spec.columns})
    return {"feed": _summary(feeds()[spec.name]),
            "steps": [s.__dict__ for s in steps],
            "validation": _validate_after(steps, payload)}


@app.put("/api/feeds/{name}")
def api_update_feed(name: str, payload: dict):
    """Edit a registered feed.

    Renaming is not offered. The name is the raw table, the DAG id, the S3
    prefix, the dbt source, the model and the PREPARED_TABLES entry all at
    once, and changing it in the registry alone would leave a raw table with
    data in it that nothing refers to any more. Delete and re-add, or rename
    all six by hand.
    """
    payload = {**payload, "name": name}
    spec = FeedSpec.from_payload(payload)
    # Same reduction as create, or an edit would drop the overrides: the form
    # posts a full type map, and anything not persisted here reverts to the
    # name-based guess on the next read.
    spec.column_types = scaffold.overrides_only(spec.columns, spec.column_types)
    registry.validate(spec, existing=set(feeds()), updating=True)
    registry.update(spec)
    return _summary(feeds()[name])


@app.delete("/api/feeds/{name}")
def api_delete_feed(name: str, confirm: str = "", scaffold_too: bool = False):
    """Remove a feed from the registry.

    DATA IS NOT TOUCHED. `lakehouse.raw.<name>` and anything built from it
    stay exactly where they are -- but they stop being in `managed_tables()`,
    so retention and maintenance stop covering them and they grow untended.
    That is worth doing on purpose and not by accident, hence `confirm`.
    """
    fd = _feed_or_404(name)
    if confirm != name:
        raise HTTPException(400, f"pass confirm={name} to delete this feed")

    removed = []
    if scaffold_too:
        model = scaffold.PREPARED_DIR / f"{name}.sql"
        if model.exists():
            model.unlink()
            removed.append(str(model))
    registry.remove(name)
    return {"deleted": name, "raw_table_left_in_place": fd.raw_table,
            "files_removed": removed,
            "note": ("The dbt source entry, the test block and the "
                     "PREPARED_TABLES entry are left for you to remove -- each "
                     "sits inside a file with other feeds' content in it.")}


@app.post("/api/scaffold/{name}")
def api_rescaffold(name: str, payload: dict | None = None):
    """Re-run steps 2-5 for an existing feed. Fills gaps; overwrites nothing."""
    fd = _feed_or_404(name)
    spec = registry.spec_from_feed(fd)
    types = (payload or {}).get("column_types") or scaffold.resolve_types(fd)
    steps = scaffold.scaffold(spec, {c: types.get(c, "string") for c in spec.columns})
    return {"steps": [s.__dict__ for s in steps], "scaffold": scaffold.status(name),
            "validation": _validate_after(steps, payload or {})}


def _validate_after(steps, payload: dict) -> dict | None:
    """Parse the dbt project after scaffolding, unless asked not to.

    Runs even when every step reported `skipped`: "the files were already
    there" says nothing about whether dbt can read them, and a project broken
    by a previous hand-edit is exactly what someone pressing this is trying to
    find out about.

    Skipped only when a step FAILED -- the scaffold is then known to be
    incomplete, and a parse error would just be a second, less specific report
    of the same problem.
    """
    if payload.get("validate") is False:
        return None
    if any(not s.ok for s in steps):
        return {"ok": False, "ran": False,
                "summary": "not run — a scaffolding step failed first",
                "detail": "", "seconds": 0.0}
    return dbt_check.parse()


@app.post("/api/validate")
def api_validate():
    """Ask dbt to parse the project. ~10s, no Spark and no warehouse.

    Project-wide, not per-feed: dbt builds one manifest. A failure here may
    well be someone else's model, and the response says so rather than
    implying the feed you are looking at is the broken one.
    """
    return dbt_check.parse()


# ------------------------------------------------------------------ helpers
@app.post("/api/derive-pattern")
def api_derive_pattern(payload: dict):
    example = str(payload.get("example", "")).strip()
    pattern = registry.derive_pattern(example)
    if pattern is None:
        raise HTTPException(
            400, f"{example!r} has no 8-digit business date in it, so there is "
                 f"nothing to anchor a (?P<business_date>...) group to")
    return {"pattern": pattern, "example": example}


@app.post("/api/test-pattern")
def api_test_pattern(payload: dict):
    """Try a pattern against a filename, the way arrival matching will.

    `re.fullmatch`, not `search` -- which is the difference that makes a
    plausible-looking pattern match nothing at all.
    """
    import re as _re
    from datetime import datetime

    pattern = str(payload.get("pattern", ""))
    filename = str(payload.get("filename", ""))
    try:
        m = _re.fullmatch(pattern, filename)
    except _re.error as exc:
        return {"ok": False, "reason": f"not a valid regex: {exc}"}
    if not m:
        partial = _re.search(pattern, filename) if pattern else None
        return {"ok": False,
                "reason": ("matches only part of the filename -- arrival uses "
                           "fullmatch, so this would match nothing"
                           if partial else "does not match")}
    if "business_date" not in m.groupdict():
        return {"ok": False, "reason": "no business_date group"}
    try:
        bd = datetime.strptime(m.group("business_date"), "%Y%m%d").date()
    except ValueError as exc:
        return {"ok": False, "reason": f"business_date is not yyyyMMdd: {exc}"}
    return {"ok": True, "business_date": bd.isoformat(),
            "version": int(m.groupdict().get("version") or 1)}


@app.post("/api/infer-columns")
async def api_infer_columns(file: UploadFile = File(...)):
    """Read a CSV's header to pre-fill the column list and types."""
    content = await file.read()
    columns = feeddata.columns_from_csv(content)
    return {"columns": columns, "column_types": scaffold.infer_types(columns),
            "types_available": scaffold.COLUMN_TYPES}


@app.get("/api/links")
def api_links():
    """Host-side URLs for the header links.

    Every host port is overridable (*_HOST_PORT in .env) because the defaults
    collide with things people actually run -- 8080 with almost anything,
    5432 with a local Postgres, 9000/9001 with ZScaler. A page that hardcodes
    them sends people to whatever else is listening, which looks like a
    working link. Served from the API so the one place that knows the host
    mapping -- the compose file -- is what tells the browser.
    """
    return {
        "minio": os.environ.get("MINIO_CONSOLE_URL", "http://localhost:19001"),
        "airflow": os.environ.get("AIRFLOW_UI_URL", "http://localhost:8081"),
        "spark": os.environ.get("SPARK_UI_URL", "http://localhost:8080"),
        "nessie": os.environ.get("NESSIE_UI_URL", "http://localhost:19120"),
    }


@app.get("/api/column-types")
def api_column_types():
    return {"types": scaffold.COLUMN_TYPES}


@app.post("/api/infer-types")
def api_infer_types(payload: dict):
    """Guess types for column names typed by hand, not read from a CSV.

    `/api/infer-columns` has always done this, but only for an uploaded file,
    so a column added with "+ column" defaulted to `string` while the same
    column read from a header got `decimal`. That gap mattered little when the
    type was used once to scaffold and thrown away; now that it is persisted
    and drives the sample-data generator too, a silently wrong default is the
    exact divergence `Feed.column_types` exists to stop.
    """
    columns = [str(c).strip() for c in (payload.get("columns") or []) if str(c).strip()]
    return {"column_types": scaffold.infer_types(columns)}


# --------------------------------------------------------------------- data
@app.get("/api/feeds/{name}/files")
def api_files(name: str):
    fd = _feed_or_404(name)
    return {"seed": feeddata.list_seed(fd), "landed": feeddata.list_landed(fd)}


@app.get("/api/feeds/{name}/preview")
def api_preview(name: str, filename: str):
    return feeddata.preview(_feed_or_404(name), filename)


@app.post("/api/feeds/{name}/upload")
async def api_upload(name: str, file: UploadFile = File(...),
                     filename: str = Form("")):
    fd = _feed_or_404(name)
    content = await file.read()
    return feeddata.save_to_seed(fd, filename or file.filename or "", content)


@app.post("/api/feeds/{name}/land")
def api_land(name: str, payload: dict | None = None):
    fd = _feed_or_404(name)
    filenames = (payload or {}).get("filenames")
    keys = feeddata.land(fd, filenames)
    return {"landed": keys, "count": len(keys)}


@app.get("/api/feeds/{name}/pending")
def api_pending(name: str):
    """The true pending set. Runs a Spark job -- see feeddata's header."""
    fd = _feed_or_404(name)
    return {"pending": feeddata.pending(fd)}


# ------------------------------------------------- sample data + feed test
@app.post("/api/feeds/{name}/generate")
def api_generate(name: str, payload: dict | None = None):
    """Generate deliveries for this feed into seed/<feed>/.

    Dates come from what the OTHER feeds have in seed/, not from today: rows
    dated where no reference data exists fail relationships tests for reasons
    that say nothing about the feed being tested.
    """
    fd = _feed_or_404(name)
    p = payload or {}
    from datetime import date as _date
    # types= is NOT optional here, and leaving it off was the bug. Without it
    # `sampledata.generate` falls back to `infer_types(feed.columns)` and
    # re-guesses from the column name -- so a column the prepared model
    # safe_casts to DECIMAL got a non-numeric sample value, landed 100% NULL,
    # and the build passed because safe_cast is meant to null and nothing
    # tested it. Same resolved map the scaffold used.
    return sampledata.generate(
        fd,
        days=int(p.get("days") or 3),
        rows=int(p.get("rows") or 0),
        end=_date.fromisoformat(p["end"]) if p.get("end") else None,
        version=int(p["version"]) if p.get("version") else None,
        types=scaffold.resolve_types(fd),
    )


@app.post("/api/feeds/{name}/test")
def api_test_feed(name: str, payload: dict | None = None):
    """Build and test just this feed, on a throwaway branch. Never merges."""
    fd = _feed_or_404(name)
    job = feedtest.start(fd, downstream=bool((payload or {}).get("downstream")))
    return {"job_id": job.id, "label": job.label,
            "branch": job.result.get("branch"),
            "note": feedtest.environment_note()}


@app.get("/api/jobs/{job_id}")
def api_job(job_id: str, since: int = 0):
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, f"no such job: {job_id}")
    return job.snapshot(since=since)


@app.get("/api/jobs")
def api_jobs():
    running = jobs.running(feedtest.KIND)
    return {"running": running.snapshot(since=running.produced) if running else None}


# ------------------------------------------------------------ orchestration
@app.get("/api/airflow/health")
def api_airflow_health():
    return orchestration.health()


@app.post("/api/feeds/{name}/ingest")
def api_ingest(name: str, payload: dict | None = None):
    """Unpause the feed's DAG if needed, then trigger a run.

    THE UNPAUSE IS NOT A CONVENIENCE. A generated DAG is created PAUSED, and a
    paused DAG accepts a trigger and then never runs it -- `airflow dags list`
    shows it identically to a live one except for one boolean. It is the step
    docs/ADDING-A-FEED.md says gets forgotten, so the console does it and
    reports that it did.
    """
    fd = _feed_or_404(name)
    dag_id = f"ingest_{fd.name}"
    dag = orchestration.get_dag(dag_id)
    if dag is None:
        raise HTTPException(
            409,
            f"Airflow has not parsed {dag_id} yet. The DAG is generated from "
            f"feeds.yml on the scheduler's next pass over feed_ingest.py, "
            f"which is within about 30 seconds of the feed being added.")

    unpaused = False
    if dag.get("is_paused"):
        orchestration.set_paused(dag_id, False)
        unpaused = True

    stale = orchestration.stale_non_terminal(dag_id)
    payload = payload or {}

    # ONE RUN INGESTS ONE ARRIVAL. That is the platform's design -- a feed is
    # processed as soon as it is received, so the unit of work is a delivery,
    # not a batch -- and it surprises anyone who has just landed a week of
    # backfill and pressed a button once. `all_pending` triggers one run per
    # outstanding object instead, which `max_active_runs=1` then drains in
    # order.
    #
    # It has to resolve the pending set itself (a Spark call, see
    # feeddata.pending) rather than triggering a run per LANDED object: with
    # an explicit object_key the DAG ingests what it is given without checking
    # whether that file is already in raw, so a run per landed object would
    # re-ingest the history as new _file_versions.
    if payload.get("all_pending"):
        keys = feeddata.pending(fd)
        runs = [orchestration.trigger(dag_id, conf={"object_key": k},
                                      note="feed console: drain pending")
                for k in keys]
        return {"dag_id": dag_id, "unpaused": unpaused,
                "run_ids": [r["dag_run_id"] for r in runs],
                "run_id": runs[0]["dag_run_id"] if runs else None,
                "state": runs[0].get("state") if runs else None,
                "pending": keys, "runs_already_in_flight": stale}

    conf = {k: v for k, v in payload.items()
            if k in ("object_key", "business_date") and v}
    run = orchestration.trigger(dag_id, conf=conf, note="triggered from feed console")
    return {"dag_id": dag_id, "run_id": run["dag_run_id"], "state": run.get("state"),
            "run_ids": [run["dag_run_id"]], "unpaused": unpaused,
            # Not an error: a run may legitimately be in flight. But under
            # max_active_runs=1 it is also the reason a new run sits queued
            # forever, so the console shows it instead of leaving it to be
            # discovered.
            "runs_already_in_flight": stale}


@app.post("/api/builds/{layer}")
def api_build(layer: str):
    if layer not in ("prepared", "reporting"):
        raise HTTPException(404, "layer must be prepared or reporting")
    dag_id = (orchestration.PREPARED_BUILD_DAG if layer == "prepared"
              else orchestration.REPORTING_BUILD_DAG)
    dag = orchestration.get_dag(dag_id)
    if dag is None:
        raise HTTPException(409, f"Airflow does not know a DAG {dag_id}")
    if dag.get("is_paused"):
        orchestration.set_paused(dag_id, False)
    run = orchestration.trigger(dag_id, note="triggered from feed console")
    return {"dag_id": dag_id, "run_id": run["dag_run_id"], "state": run.get("state")}


@app.get("/api/runs/{dag_id}/{run_id}")
def api_run(dag_id: str, run_id: str):
    return orchestration.run_state(dag_id, run_id)


@app.get("/api/runs/{dag_id}")
def api_runs(dag_id: str, limit: int = 5):
    return {"dag_id": dag_id, "runs": orchestration.recent_runs(dag_id, limit)}


@app.get("/api/feeds/{name}/state")
def api_feed_state(name: str):
    """Cheap per-feed pipeline state for the stage strip. NO Spark.

    Deliberately does not call `feeddata.pending()`, which starts a Spark job:
    this is rendered every time the Run tab opens and after every run, and a
    strip that cost 30s of cluster time to draw would simply be turned off.
    Counts of seed and landed objects come from the filesystem and S3 listing;
    the three DAG stages come from the metadata DB.
    """
    fd = _feed_or_404(name)
    out: dict = {"seed": len(feeddata.list_seed(fd)),
                 "landed": len(feeddata.list_landed(fd))}
    for stage, dag_id in (("raw", f"ingest_{fd.name}"),
                          ("prepared", orchestration.PREPARED_BUILD_DAG),
                          ("reporting", orchestration.REPORTING_BUILD_DAG)):
        try:
            runs = orchestration.recent_runs(dag_id, 1)
            out[stage] = {"state": runs[0]["state"] if runs else None,
                          "run_id": runs[0]["run_id"] if runs else None}
        except orchestration.AirflowError:
            # Airflow down must not blank the strip -- the seed/landing halves
            # are still true and still worth showing.
            out[stage] = {"state": "unknown", "run_id": None}
    return out


@app.get("/api/pipeline")
def api_pipeline():
    """Recent runs of the two build DAGs, for the pipeline strip in the UI."""
    return {
        "prepared": orchestration.recent_runs(orchestration.PREPARED_BUILD_DAG, 3),
        "reporting": orchestration.recent_runs(orchestration.REPORTING_BUILD_DAG, 3),
    }


# --------------------------------------------------------------------- pages
@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")
