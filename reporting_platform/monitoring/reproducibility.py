"""Can a published run still be reproduced? REQ-702.

WHY A TEST AND NOT A COMMENT. Everything that makes a published run
reproducible is a *pin* -- `record_publication` cuts
`published/<report>/<cob_date>/<run_id>`, and retention keeps that tag for
`references.published_tags`. Nothing about that announces its own failure. A
tag deleted too early, a GC cutoff that collected a file the tag referenced, a
maintenance step that expired the snapshot it pointed at: each leaves a catalog
that looks healthy and a pin that no longer resolves, and the first to find out
is whoever was asked to reproduce a figure from three years ago.

So the pin is EXERCISED against the real catalog: the reference is resolved,
the metadata opened, and every data file it names confirmed to still be an
object. Both halves are needed -- a `SELECT COUNT(*)` at the tag is answered
out of Iceberg's own manifests and returns the published number happily from a
table whose parquet files have been deleted. `check_tag` measures that.

WHICH PIN IS WORTH READING, AND WHY IT IS NOT AN AGE. A tag whose files are the
ones `main` still uses would resolve even if pinning did not work at all. What
makes the read mean something is the pin holding at least one data file `main`
no longer references -- a file a too-eager collector would take.

This used to be approximated by AGE, on the grounds that compaction rewrites
old partitions. That is backwards twice over. `compact()` scopes its rewrite to
the RECENT partitions, so a pin ages OUT of the window rather than into it. And
compaction is not the mechanism that produces the divergence: expiring a COB
date is a metadata-level partition delete, so `main` stops referencing that
date's files while every pin goes on referencing them. On a catalog whose
newest COB date is older than the compaction window -- every catalog between
deliveries -- the age gate opened on a fixed date and reported GREEN on a pin
still byte-identical to `main`.

So the question is asked directly: `divergence_scan()` compares each pin's
data-file set against `main`'s and selects the oldest pin holding a file `main`
has dropped. Below that the report is `not_yet_meaningful` rather than a pass,
because a green there is worse than no result.

WHAT IT COSTS, MEASURED. One Spark session addresses every reference --
Nessie's catalog accepts `catalog.namespace.`table@ref`.files`, so this is NOT
a session per pin. Against the live stack, 11 managed tables: 6.9s for `main`'s
baseline, then ~1.0s per pin. The scan stops at the first divergent pin and is
capped at `MAX_PINS_SCANNED`. `check_tag` still opens its own session at the
tag, which buys it exercising the same resolution path a human reproduction
uses.

WHAT THIS CANNOT TELL YOU: that the numbers match those published. It asserts
the tables are READABLE at the pin and reports row counts. Comparing against
what was published needs the run record.

Nor does a capped `not_yet_meaningful` mean NO pin diverges -- only that none
of the `pins_scanned` oldest ones does. A newer pin can diverge while an older
one does not, so the scan is bounded in the direction of missing a young pin,
never of passing an old one.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from reporting_platform.common.context import (
    Nessie, managed_tables, spark_session,
)

log = logging.getLogger("reproducibility")

# How many DISTINCT pin commits the divergence scan looks at before giving up
# and reporting `not_yet_meaningful`. Bounded because the tag count grows
# without limit -- one per report per published COB date -- while the scan is
# oldest-first and stops at its first hit, so wherever retention has run it
# stops at 1. The cap only bites on a catalog that has never removed a file,
# which is precisely where there is nothing meaningful to read anyway.
MAX_PINS_SCANNED = 25


def published_tags(nessie: Nessie) -> list[dict]:
    """Every published tag with its commit time and hash, oldest first.

    Oldest first because among the pins that hold a file `main` has dropped,
    the oldest one's files have been unreferenced by anything else for the
    longest -- the most GC identify passes have had a chance at them, and the
    most deferred-delete windows have expired over them. That is the pin a
    too-eager collector takes first, so it is the one worth reading.
    """
    from reporting_platform.retention.retention import TAG_RE

    out = []
    for ref in nessie.list_references("published/", fetch_all=True):
        if ref.get("type") != "TAG":
            continue
        m = TAG_RE.match(ref["name"])
        if not m:
            continue
        committed = ((ref.get("metadata") or {})
                     .get("commitMetaOfHEAD") or {}).get("commitTime")
        when = None
        if committed:
            try:
                when = datetime.fromisoformat(committed.replace("Z", "+00:00"))
            except ValueError:
                when = None
        out.append({"tag": ref["name"], "report": m.group("report"),
                    "cob_date": m.group("bd"), "committed": when,
                    # Two exposures publishing one build cut two tags at the
                    # SAME commit, so they name the same file set. The scan
                    # dedupes on this rather than paying for it twice.
                    "hash": ref.get("hash")})
    # A tag with no readable commit time sorts last rather than first: its age
    # is unknown, and treating unknown as ancient would pick it as the probe
    # and report a misleading "oldest".
    return sorted(out, key=lambda t: (t["committed"] is None,
                                      t["committed"] or datetime.max.replace(
                                          tzinfo=timezone.utc)))


ABSENT = ("NoSuchTable", "TABLE_OR_VIEW_NOT_FOUND")


def _files_at(spark, table: str, ref: str | None = None) -> set[str]:
    """The data files the table's CURRENT snapshot references at `ref`.

    `<table>.files` is Iceberg's own metadata table, so this is the file list
    the catalog itself would hand a reader -- not an object-storage listing,
    which would also show files no snapshot references any more.

    Nessie's Iceberg catalog resolves `namespace.`table@ref`` , which is the
    whole reason this is affordable: one session reaches every reference, so
    comparing nine pins costs nine queries rather than nine JVM starts.
    Verified against the running stack; the alternatives
    (`table.`files@ref`` and `VERSION AS OF '<tag>'`) both fail.
    """
    if ref is None:
        ident = f"{table}.files"
    else:
        catalog, namespace, name = table.split(".")
        ident = f"{catalog}.{namespace}.`{name}@{ref}`.files"
    return {r["file_path"]
            for r in spark.sql(f"SELECT file_path FROM {ident}").collect()}


def _exclusive(spark, tables: list[str], ref: str,
               main: dict[str, set[str]]) -> dict:
    """Files this pin references that `main` does not, per table.

    A table absent at the pin contributes nothing: the platform gains tables
    over time and a pin predating one is history, not damage -- the same rule
    `check_tag` applies.

    A table that errors for any OTHER reason is recorded and treated as
    DIVERGENT. Failing toward doing the check is the only safe direction: an
    unreadable `.files` at a pin is itself a symptom, and `check_tag` is what
    produces the verdict. Failing the other way would silently skip the one
    pin most likely to be broken.
    """
    out: dict = {"exclusive": {}, "errors": []}
    for table in tables:
        try:
            out["exclusive"][table] = sorted(_files_at(spark, table, ref)
                                             - main.get(table, set()))
        except Exception as exc:                            # noqa: BLE001
            text = f"{type(exc).__name__}: {str(exc)[:200]}"
            if any(a in text for a in ABSENT):
                continue
            out["errors"].append({"table": table, "error": text})
    out["exclusive"] = {t: f for t, f in out["exclusive"].items() if f}
    out["exclusive_files"] = sum(len(f) for f in out["exclusive"].values())
    out["diverges"] = bool(out["exclusive"]) or bool(out["errors"])
    return out


def divergence_scan(tags: list[dict], tables: list[str],
                    limit: int = MAX_PINS_SCANNED) -> dict:
    """The oldest pin holding a data file `main` no longer references.

    THE REPLACEMENT FOR THE AGE GATE, and the module header says why age was
    wrong. This asks the question age was standing in for -- has this pin's
    data been rewritten or dropped underneath it -- of the Iceberg metadata,
    which knows.

    ONE SESSION FOR EVERY REFERENCE. `main`'s baseline is read once and
    reused; each pin is one query per table against the same session. Stops
    at the first pin that diverges, because that is the pin it would select,
    and dedupes on commit hash because two tags at one commit name one file
    set.
    """
    out: dict = {"tables": len(tables), "pins_scanned": 0, "chosen": None,
                 "cap": limit, "capped": False, "pins": []}
    if not tags:
        return out
    spark = spark_session("reproducibility-divergence", ref="main")
    try:
        main = {}
        for table in tables:
            try:
                main[table] = _files_at(spark, table)
            except Exception as exc:                        # noqa: BLE001
                text = f"{type(exc).__name__}: {str(exc)[:200]}"
                if not any(a in text for a in ABSENT):
                    # A table `main` cannot list leaves every pin looking
                    # divergent on it. Record it and treat main as holding
                    # nothing, which is the fail-toward-checking direction.
                    out.setdefault("main_errors", []).append(
                        {"table": table, "error": text})
                main[table] = set()
        by_hash: dict = {}
        for entry in tags:
            key = entry.get("hash")
            if key is not None and key in by_hash:
                verdict = by_hash[key]
            else:
                if out["pins_scanned"] >= limit:
                    out["capped"] = True
                    break
                verdict = _exclusive(spark, tables, entry["tag"], main)
                out["pins_scanned"] += 1
                if key is not None:
                    by_hash[key] = verdict
            out["pins"].append({
                "tag": entry["tag"],
                "exclusive_files": verdict["exclusive_files"],
                "diverges": verdict["diverges"],
                "errors": verdict["errors"],
            })
            if verdict["diverges"]:
                out["chosen"] = entry
                out["exclusive"] = {t: len(f)
                                    for t, f in verdict["exclusive"].items()}
                out["exclusive_files"] = verdict["exclusive_files"]
                out["scan_errors"] = verdict["errors"]
                break
    finally:
        spark.stop()
    return out


def _object_of(path: str) -> tuple[str, str]:
    """`s3a://bucket/key` -> (bucket, key). Also accepts s3:// and s3n://."""
    rest = path.split("://", 1)[1]
    bucket, _, key = rest.partition("/")
    return bucket, key


def absent_objects(paths: set[str]) -> list[str]:
    """Which of these data files are no longer in object storage.

    ONE LIST PER TABLE DATA PREFIX, not a HEAD per file. The prefix is derived
    from the paths themselves rather than from the catalog, so this needs no
    second opinion about where a table lives, and the cost is one paginated
    listing per table however many files it holds.
    """
    from reporting_platform.ingest.arrival import _client

    groups: dict[tuple[str, str], set[str]] = {}
    for path in paths:
        bucket, key = _object_of(path)
        head = key.rsplit("/data/", 1)[0] + "/data/" if "/data/" in key \
            else key.rsplit("/", 1)[0] + "/"
        groups.setdefault((bucket, head), set()).add(key)

    s3 = _client()
    gone: list[str] = []
    for (bucket, prefix), keys in groups.items():
        present: set[str] = set()
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix):
            present.update(o["Key"] for o in page.get("Contents", []))
        gone.extend(sorted(keys - present))
    return gone


def check_tag(tag: str, tables: list[str]) -> dict:
    """Read every managed table at `tag`. One Spark session, one session stop.

    A table missing AT THE PIN is not a failure: the platform gains tables over
    time, and a report published before `reporting.exposure_by_country` existed
    cannot be expected to contain it. A table that exists and cannot be READ is
    the failure this looks for.

    TWO ASSERTIONS, AND THE SECOND IS NOT REDUNDANT. This used to be one
    `SELECT COUNT(*)` per table, on the stated grounds that the read "resolves
    the reference, opens the metadata the commit pointed at, and reads the data
    files that metadata names". It does not read them. Iceberg answers
    `COUNT(*)` -- and `COUNT(<column>)` -- from the record counts in its own
    manifests, without opening a single parquet file. MEASURED: one data file
    the pin referenced and `main` did not was deleted from MinIO, and

        COUNT(*)                                    -> 16400
        COUNT(trade_id)                             -> 16400
        COUNT(*) WHERE trade_id IS NOT NULL         -> raised
        COUNT(*) WHERE _cob_date = '2026-08-06'-> raised

    so the whole check reported `reproduced` on a pin whose data was gone. The
    count proves the METADATA chain resolves, which is worth asserting and is
    all it ever asserted. What proves the DATA survives is that the files the
    pin's snapshot names are still objects: `<table>.files` at the tag is the
    list, and object storage is asked whether they are there. That is the exact
    shape of the failure a too-eager collector produces, it costs no Spark, and
    it names the file that went rather than handing over a Py4J stack.

    Adding a predicate to force a real scan would also have worked and is not
    what this does: a full scan of every published table every night is a cost
    with no extra signal, since a file that is present is not corrupt in any
    failure mode this platform has -- GC deletes, it does not rewrite.
    """
    result: dict = {"tag": tag, "tables": [], "unreadable": [], "absent": [],
                    "missing_files": []}
    spark = spark_session("reproducibility", ref=tag)
    referenced: dict[str, set[str]] = {}
    try:
        for table in tables:
            try:
                rows = spark.sql(f"SELECT COUNT(*) c FROM {table}").collect()
                files = _files_at(spark, table)
                referenced[table] = files
                result["tables"].append({"table": table,
                                         "rows": rows[0]["c"],
                                         "data_files": len(files)})
            except Exception as exc:                     # noqa: BLE001
                text = f"{type(exc).__name__}: {str(exc)[:300]}"
                # "table not found" at an old pin is history, not damage.
                if any(a in text for a in ABSENT):
                    result["absent"].append({"table": table, "error": text})
                else:
                    log.error("cannot read %s at %s: %s", table, tag, text)
                    result["unreadable"].append({"table": table,
                                                 "error": text})
    finally:
        spark.stop()

    for table, files in referenced.items():
        gone = absent_objects(files)
        if not gone:
            continue
        log.error("%d of %d data file(s) %s references at %s are gone from "
                  "object storage: %s", len(gone), len(files), table, tag,
                  gone[:3])
        result["missing_files"].append({"table": table, "missing": len(gone),
                                        "of": len(files),
                                        "examples": gone[:5]})
        result["unreadable"].append({
            "table": table,
            "error": (f"{len(gone)} of {len(files)} data file(s) the pin "
                      f"references are no longer in object storage, e.g. "
                      f"{gone[0]}")})

    result["ok"] = not result["unreadable"]
    return result


def run(tag: str | None = None, scan: bool = True) -> dict:
    """Exercise the oldest pin whose files `main` no longer keeps alive.

    `scan=False` skips the divergence scan entirely. It exists for a manual
    `--tag` invocation where the caller already knows why the pin is
    interesting and does not want to pay for `main`'s baseline; the report
    then carries no `divergence` key rather than an empty one, so a reader
    cannot mistake "not measured" for "measured, none".
    """
    tables = [t for t, _layer in managed_tables()]
    report: dict = {"checked_at": datetime.now(timezone.utc).isoformat()}

    if tag is not None:
        # An explicit tag is checked whatever the scan says -- the caller
        # named it. The scan still runs by default, so a `--tag` run states
        # how much of that pin `main` no longer holds instead of leaving the
        # reader to establish it by hand, which is what phase 1 had to do.
        if scan:
            one = divergence_scan([{"tag": tag, "hash": None}], tables)
            report["divergence"] = {
                "exclusive_files": one.get("exclusive_files", 0),
                "exclusive": one.get("exclusive", {}),
                "diverges": one.get("chosen") is not None,
                "errors": one.get("scan_errors", []),
            }
        report.update(check_tag(tag, tables))
        report["selected"] = "explicit"
        report["status"] = "reproduced" if report["ok"] else "BROKEN"
        if not report["ok"]:
            log.error("REPRODUCIBILITY BROKEN at %s: %d table(s) unreadable.",
                      tag, len(report["unreadable"]))
        return report

    tags = published_tags(Nessie())
    report["published_tags"] = len(tags)
    if not tags:
        report.update({"ok": True, "status": "no_published_tags"})
        return report

    scanned = divergence_scan(tags, tables)
    report["divergence"] = {k: scanned[k] for k in
                            ("pins_scanned", "capped", "cap", "tables")}
    if scanned.get("main_errors"):
        report["divergence"]["main_errors"] = scanned["main_errors"]
    chosen = scanned["chosen"]

    if chosen is None:
        # Deliberately NOT a pass. Every pin scanned names only files `main`
        # still references, so each of them would resolve whether pinning
        # worked or not. See the module header: this is the state the old age
        # gate would have reported green on, on a date rather than on evidence.
        detail = (
            f"none of the {scanned['pins_scanned']} oldest published pin(s) "
            f"holds a data file main no longer references, so a successful "
            f"read would not prove the pin holds. Nothing has been rewritten "
            f"or expired under them yet.")
        if scanned["capped"]:
            detail += (f" The scan stopped at its cap of {scanned['cap']} "
                       f"pins; a newer pin may diverge.")
        report.update({"ok": True, "status": "not_yet_meaningful",
                       "detail": detail})
        return report

    report.update(check_tag(chosen["tag"], tables))
    report["selected"] = "oldest_diverging"
    report["cob_date"] = chosen["cob_date"]
    report["committed"] = (chosen["committed"].isoformat()
                           if chosen["committed"] else None)
    report["divergence"].update({
        "exclusive_files": scanned["exclusive_files"],
        "exclusive": scanned["exclusive"],
        "errors": scanned.get("scan_errors", []),
    })
    report["status"] = "reproduced" if report["ok"] else "BROKEN"
    if not report["ok"]:
        log.error("REPRODUCIBILITY BROKEN at %s: %d table(s) unreadable. A "
                  "published run can no longer be read at its own pin.",
                  chosen["tag"], len(report["unreadable"]))
    else:
        log.info("reproducibility: %s reproduced from %d data file(s) main no "
                 "longer references", chosen["tag"],
                 scanned["exclusive_files"])
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--tag", default=None,
                   help="check this published tag instead of the oldest "
                        "eligible one")
    p.add_argument("--fail-on-break", action="store_true",
                   help="exit non-zero if a pin no longer resolves")
    p.add_argument("--no-scan", dest="scan", action="store_false",
                   help="skip the divergence scan (with --tag, when the "
                        "caller already knows the pin diverges)")
    a = p.parse_args(argv)

    report = run(a.tag, scan=a.scan)
    print(json.dumps(report, indent=2, default=str))
    if a.fail_on_break and not report.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
