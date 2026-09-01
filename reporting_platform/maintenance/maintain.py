"""Iceberg table maintenance — metric-driven, not schedule-driven.

Reads the Iceberg metadata tables first and only acts where a threshold is
breached. Metrics are emitted every run whether or not we act, so degradation
is visible as a time series before it becomes a support call. Ship these to
the existing Elastic/ECS observability stack rather than building a new
dashboard.

All of these are Iceberg stored procedures and require SPARK. DuckDB cannot
perform any of them — which is the standing justification for keeping Spark in
the platform even if every transformation eventually runs on DuckDB.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from reporting_platform.common.context import CATALOG, maintenance_config, spark_session

log = logging.getLogger("maintenance")


def _short(fqn: str) -> str:
    parts = fqn.split(".")
    return ".".join(parts[1:]) if parts[0] == CATALOG else fqn


# ------------------------------------------------------------------- metrics
def collect_metrics(spark, table: str, recent_days: int) -> dict:
    """Read Iceberg metadata tables. Cheap: no data files are touched."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=recent_days)).date()
    m: dict = {"table": table, "collected_at": datetime.now(timezone.utc).isoformat()}

    files = spark.sql(
        f"SELECT partition, COUNT(*) AS file_count, "
        f"       AVG(file_size_in_bytes) AS avg_size, "
        f"       SUM(file_size_in_bytes) AS total_size, SUM(record_count) AS records "
        f"FROM {table}.files GROUP BY partition"
    ).collect()
    if files:
        m["partition_count"] = len(files)
        m["total_files"] = sum(r["file_count"] for r in files)
        m["max_files_per_partition"] = max(r["file_count"] for r in files)
        sizes = [r["avg_size"] for r in files if r["avg_size"]]
        m["avg_file_size_mb"] = round(sum(sizes) / len(sizes) / 1_048_576, 2) if sizes else 0
        m["total_size_mb"] = round(sum(r["total_size"] or 0 for r in files) / 1_048_576, 2)
        m["records"] = sum(r["records"] or 0 for r in files)
    else:
        m.update(partition_count=0, total_files=0, max_files_per_partition=0,
                 avg_file_size_mb=0, total_size_mb=0, records=0)

    m["snapshot_count"] = spark.sql(f"SELECT COUNT(*) c FROM {table}.snapshots").collect()[0]["c"]
    m["manifest_count"] = spark.sql(f"SELECT COUNT(*) c FROM {table}.manifests").collect()[0]["c"]
    try:
        m["delete_file_count"] = spark.sql(
            f"SELECT COUNT(*) c FROM {table}.delete_files"
        ).collect()[0]["c"]
    except Exception:
        m["delete_file_count"] = 0
    m["recent_partition_cutoff"] = cutoff.isoformat()
    return m


# ------------------------------------------------------------------- actions
def compact(spark, table: str, layer_cfg: dict, defaults: dict,
            date_column: str) -> dict:
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=defaults["recent_partition_days"])).date()
    strategy = layer_cfg.get("strategy", "binpack")

    opts = [
        f"'target-file-size-bytes','{defaults['target_file_size_bytes']}'",
        "'min-input-files','5'",
    ]
    if defaults.get("partial_progress"):
        # Commits in batches, so a failure halfway does not waste the whole run.
        opts += ["'partial-progress.enabled','true'", "'partial-progress.max-commits','10'"]

    sort_clause = ""
    if strategy == "sort" and layer_cfg.get("sort_order"):
        # `sort_order` is configured PER LAYER, but the tables in a layer do
        # not share a schema: `reporting` holds both `counterparty_exposure`
        # (per counterparty) and `exposure_by_country` (aggregated, so
        # `counterparty_id` does not exist on it). Iceberg binds every sort
        # field against the table schema up front and throws
        #   ValidationException: Cannot find field 'counterparty_id' in struct
        # which aborted the whole maintenance run at the first such table
        #. Intersect the configured order with the columns
        # that actually exist, and fall back to binpack if none survive --
        # a layer-wide sort key is a preference, not a schema guarantee.
        present = {f.name.lower() for f in spark.table(table).schema.fields}
        wanted = [c.strip() for c in layer_cfg["sort_order"].split(",") if c.strip()]
        usable = [c for c in wanted if c.split()[0].strip('"`').lower() in present]
        dropped = [c for c in wanted if c not in usable]
        if dropped:
            log.warning("%s: sort columns %s not in schema, sorting on %s",
                        table, dropped, usable or "nothing (binpack)")
        if usable:
            order = ", ".join(usable)
            sort_clause = f", sort_order => '{order}'"
        else:
            strategy = "binpack"

    # ALWAYS scoped. An unscoped compaction rewrites every retained month-end.
    sql = (
        f"CALL {CATALOG}.system.rewrite_data_files("
        f"  table => '{_short(table)}',"
        f"  strategy => '{strategy}'{sort_clause},"
        f"  where => '{date_column} >= date \"{cutoff}\"',"
        f"  options => map({', '.join(opts)}))"
    )
    row = spark.sql(sql).collect()
    out = row[0].asDict() if row else {}
    log.info("compacted %s (%s): %s", table, strategy, out)
    return {"action": "rewrite_data_files", "strategy": strategy, **out}


def rewrite_manifests(spark, table: str) -> dict:
    row = spark.sql(f"CALL {CATALOG}.system.rewrite_manifests('{_short(table)}')").collect()
    return {"action": "rewrite_manifests", **(row[0].asDict() if row else {})}


def rewrite_deletes(spark, table: str) -> dict:
    row = spark.sql(
        f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{_short(table)}')"
    ).collect()
    return {"action": "rewrite_position_delete_files", **(row[0].asDict() if row else {})}


# ----------------------------------------------------------------- decisioning
def decide(metrics: dict, thresholds: dict) -> list[str]:
    actions = []
    # Gate on total_files, NOT on the truthiness of avg_file_size_mb. That
    # value is rounded to 2dp, so any table averaging under ~5 KB per file
    # reports 0.0 -- which is falsy, and which silently skipped compaction on
    # exactly the most fragmented tables. An empty table reports 0 too, and
    # only that case should opt out.
    has_files = metrics["total_files"] > 0
    if has_files and (
            metrics["max_files_per_partition"] > thresholds["compact_when_files_per_partition_gt"]
            or metrics["avg_file_size_mb"] < thresholds["compact_when_avg_file_size_mb_lt"]):
        actions.append("compact")
    if metrics["manifest_count"] > thresholds["rewrite_manifests_when_manifest_count_gt"]:
        actions.append("rewrite_manifests")
    if metrics["delete_file_count"] > thresholds["rewrite_deletes_when_delete_files_gt"]:
        actions.append("rewrite_deletes")
    if metrics["snapshot_count"] > thresholds["expire_snapshots_when_snapshot_count_gt"]:
        # Flagged, not performed here — snapshot expiry is owned by the
        # retention job so that tag expiry provably runs first.
        actions.append("flag_expire_snapshots")
    return actions


def run(tables: list[tuple[str, str]], force: bool = False,
        dry_run: bool = False) -> dict:
    cfg = maintenance_config()
    defaults, thresholds, layers = cfg["defaults"], cfg["thresholds"], cfg["layers"]

    spark = spark_session("maintenance", ref="main")
    report = {"started": datetime.now(timezone.utc).isoformat(), "tables": []}
    failures: list[str] = []
    try:
        for table, layer in tables:
            date_column = "_business_date" if layer == "raw" else "business_date"
            entry: dict = {"layer": layer}
            try:
                metrics = collect_metrics(spark, table, defaults["recent_partition_days"])
            except Exception as exc:                       # table may not exist yet
                report["tables"].append({"table": table, "error": str(exc)})
                continue

            entry["metrics"] = metrics
            actions = ["compact", "rewrite_manifests"] if force else decide(metrics, thresholds)
            entry["decided_actions"] = actions
            entry["performed"] = []

            if not dry_run:
                layer_cfg = layers.get(layer, {})
                for action in actions:
                    # Isolate per table: an action failing on one table must
                    # not throw away the metrics collected for the others,
                    # which are what diagnose the failure. Recorded here and
                    # re-raised after the loop, so the run still fails loudly.
                    try:
                        if action == "compact":
                            entry["performed"].append(
                                compact(spark, table, layer_cfg, defaults, date_column))
                        elif action == "rewrite_manifests":
                            entry["performed"].append(rewrite_manifests(spark, table))
                        elif action == "rewrite_deletes":
                            entry["performed"].append(rewrite_deletes(spark, table))
                        elif action == "flag_expire_snapshots":
                            log.warning(
                                "%s has %d snapshots — retention job should expire",
                                table, metrics["snapshot_count"])
                    except Exception as exc:
                        log.exception("%s: %s failed", table, action)
                        entry.setdefault("errors", []).append(
                            {"action": action, "error": str(exc)})
                        failures.append(f"{table}/{action}: {exc}")
            report["tables"].append({"table": table, **entry})
    finally:
        spark.stop()

    report["finished"] = datetime.now(timezone.utc).isoformat()
    if failures:
        raise RuntimeError("maintenance actions failed: " + "; ".join(failures))
    return report


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--table", action="append", default=[], help="fqn:layer (repeatable)")
    p.add_argument("--force", action="store_true", help="ignore thresholds and compact anyway")
    p.add_argument("--dry-run", action="store_true", help="collect metrics only")
    p.add_argument("--all-managed", action="store_true",
                   help="every table the platform manages, from "
                        "managed_tables() -- the same list "
                        "platform_housekeeping uses")
    a = p.parse_args(argv)
    # Derived, never hand-listed -- see the note in retention.main().
    tables = [tuple(t.split(":", 1)) for t in a.table]
    if a.all_managed:
        from reporting_platform.common.context import managed_tables
        tables = managed_tables() + tables
    if not tables:
        p.error("give --table, or --all-managed for everything the platform "
                "manages")
    print(json.dumps(run(tables, a.force, a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
