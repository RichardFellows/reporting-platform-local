"""Shared platform context: config loading, Spark session, Nessie helpers.

Everything environment-varying is read from config + env here, so no DAG or
job module contains an endpoint, a credential or a policy value.
"""
from __future__ import annotations

import os
import re
import uuid
from urllib.parse import quote as _urlquote
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(os.environ.get("REPORTING_CONFIG_DIR", "/opt/platform/reporting_platform/config"))
CATALOG = os.environ.get("REPORTING_CATALOG", "lakehouse")
ENV = os.environ.get("REPORTING_ENV", "local")


# --------------------------------------------------------------------- config
@lru_cache(maxsize=None)
def _load(name: str) -> dict[str, Any]:
    with open(CONFIG_DIR / name) as fh:
        return yaml.safe_load(fh)


@dataclass(frozen=True)
class Feed:
    name: str
    description: str
    source_system: str
    filename_pattern: str
    business_key: list[str]
    columns: list[str]
    expected_min_rows: int = 0
    landing_prefix: str = "landing"
    raw_namespace: str = "raw"
    arrival_poke_seconds: int = 60
    arrival_timeout_hours: int = 26
    delimiter: str = ","
    quote_char: str = '"'
    header: bool = True
    file_encoding: str = "utf-8"
    schema_drift: str = "warn"
    # Whether this feed is expected to deliver on every business date. False
    # opts it out of the completeness check, which infers the
    # business calendar from what other feeds delivered -- a feed that does
    # not deliver daily would otherwise show every non-delivery day as a gap.
    completeness: bool = True
    # How often the feed is expected to deliver: "daily" (a business date is
    # expected whenever another feed delivered on it) or "weekly" (only that
    # each week containing business dates saw at least one delivery).
    cadence: str = "daily"

    @property
    def raw_table(self) -> str:
        return f"{CATALOG}.{self.raw_namespace}.{self.name}"

    @property
    def asset_uri(self) -> str:
        """Airflow asset URI emitted when this feed's raw table is updated."""
        return f"iceberg://{CATALOG}/{self.raw_namespace}/{self.name}"

    def parse_filename(self, filename: str) -> tuple[date, int] | None:
        """Return (business_date, version) or None if the name does not match."""
        m = re.fullmatch(self.filename_pattern, filename)
        if not m:
            return None
        bd = datetime.strptime(m.group("business_date"), "%Y%m%d").date()
        raw_version = m.groupdict().get("version")
        return bd, int(raw_version) if raw_version else 1


@lru_cache(maxsize=1)
def feeds() -> dict[str, Feed]:
    cfg = _load("feeds.yml")
    defaults = cfg.get("defaults", {})
    out: dict[str, Feed] = {}
    for block in cfg["feeds"]:
        merged = {**defaults, **block}
        allowed = {f.name for f in Feed.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        out[block["name"]] = Feed(**{k: v for k, v in merged.items() if k in allowed})
    return out


def feed(name: str) -> Feed:
    return feeds()[name]


# These are dbt MODEL names, and because no `alias` is configured on any of
# them, each is ALSO the table name in the catalog: model `trade` materialises
# as `prepared.trade`. Keep it that way -- model and table must be renamed
# TOGETHER. A mismatch in either direction points every maintenance and
# retention task at a table that does not exist, and does so silently, because
# managed_tables() never checks that its entries resolve.
#
# NO LAYER PREFIX ON ANY OF THESE. The names used to be `prep_*` and `rpt_*`;
# the namespace already says which layer a table is in, so the prefix repeated
# it inside the name -- `prepared.prep_trade`, `reporting.rpt_exposure_change`.
# The layer is now the ONLY thing distinguishing a table from its upstream:
# `raw.trade` is the landed 1:1 copy and `prepared.trade` the conformed one,
# same name, different namespace. That is legal because dbt keeps models and
# sources in separate namespaces -- a model named `trade` and a source
# `raw.trade` coexist without collision (verified, not assumed).
PREPARED_TABLES = ["trade", "counterparty", "rating",
                   "primary_limits"]
REPORTING_TABLES = ["counterparty_exposure", "exposure_by_country",
                    "exposure_change"]


def managed_tables() -> list[tuple[str, str]]:
    """(fully qualified table, layer) for everything the platform maintains.

    ONE definition, imported by both the DAG and the CLIs. It used to live in
    `platform_housekeeping.py`, which meant the `--table` examples in the
    Makefile and README were a hand-maintained subset -- and they had already
    drifted to five tables against the DAG's nine, so `make retention` quietly
    left four tables growing. That is a forked copy
    that stops matching the original, where the copy looks authoritative.

    The raw half is derived from `feeds()` rather than listed, so adding a feed
    extends maintenance and retention automatically.
    """
    tables = [(f.raw_table, "raw") for f in feeds().values()]
    tables += [(f"{CATALOG}.prepared.{t}", "prepared") for t in PREPARED_TABLES]
    tables += [(f"{CATALOG}.reporting.{t}", "reporting") for t in REPORTING_TABLES]
    return tables


def retention_policy(layer: str) -> dict[str, Any]:
    return _load("retention.yml")["environments"][ENV][layer]


def reference_policy(kind: str) -> dict[str, Any]:
    return _load("retention.yml")["references"][kind]


def nessie_gc_config() -> dict[str, Any]:
    return _load("retention.yml").get("nessie_gc", {})


def gc_window_hours(key: str, default: int) -> int:
    """An hours-valued nessie_gc policy, scalar or per-environment map.

    `deferred_delete_after_hours` is per-environment because it expresses how
    long a human needs to notice, and a laptop where a day of pipeline runs in
    ten minutes is not prod. A bare number is accepted too, for a deployment
    that does not care. Lives here rather than in retention.py so the watchdog
    can read the same value without importing the thing it monitors.
    """
    value = nessie_gc_config().get(key)
    if value is None:
        return default
    if isinstance(value, dict):
        if ENV not in value:
            raise KeyError(
                f"retention.yml: nessie_gc.{key} has no entry for env {ENV!r} "
                f"(has {sorted(value)}). Add one, or use a bare number."
            )
        return int(value[ENV])
    return int(value)


def all_snapshot_retention_days() -> list[int]:
    """snapshot_retention_days across every layer in the current environment.

    Used by the Nessie GC cutoff interlock: a GC cutoff shorter than the
    longest snapshot retention would collect files those snapshots still
    reference. See retention.nessie_gc().
    """
    env = _load("retention.yml")["environments"][ENV]
    return [v["snapshot_retention_days"] for v in env.values()
            if isinstance(v, dict) and "snapshot_retention_days" in v]


def maintenance_config() -> dict[str, Any]:
    return _load("maintenance.yml")


# ------------------------------------------------------------------- identity
def new_run_id() -> str:
    """Short, sortable run identifier used in branch names and _batch_id."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def branch_name(purpose: str, scope: str, business_date: date, run_id: str) -> str:
    """<purpose>/<scope>/<business_date>/<run_id> — see docs/ARCHITECTURE.md."""
    return f"{purpose}/{scope}/{business_date:%Y-%m-%d}/{run_id}"


def published_tag(business_date: date, run_id: str) -> str:
    return f"published/{business_date:%Y-%m-%d}/{run_id}"


# ---------------------------------------------------------------------- spark
def spark_session(app_name: str, ref: str = "main"):
    """Build a Spark session bound to the Nessie catalog at a given ref.

    `ref` is the Nessie branch. Ingest and dbt builds run on a working branch;
    maintenance and snapshot expiry run on main.

    THE SESSION IS A CLIENT OF THE STANDALONE CLUSTER, never local[*]. The
    caller's process is the driver; every task runs in an executor on
    `spark-worker`. See the `master` handling below for why there is no
    local fallback.
    """
    from pyspark.sql import SparkSession

    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("REPORTING_WAREHOUSE", "s3a://lakehouse/warehouse")
    nessie_uri = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")

    # No local[*] fallback, deliberately. A missing/blank SPARK_MASTER used to
    # mean "run the whole job inside this container", which is a configuration
    # error that LOOKS like success: the job completes, the cluster sits idle,
    # and nothing anywhere is red. Fail loudly instead. The value is set on
    # the airflow services in docker-compose.yml; the default below is the
    # same address so a bare `python -m ...` in the container still works.
    master = os.environ.get("SPARK_MASTER") or "spark://spark-master:7077"
    if master.startswith("local"):
        raise RuntimeError(
            f"SPARK_MASTER is {master!r}. This platform runs every Spark job on "
            f"the spark-master/spark-worker cluster; an in-process local session "
            f"silently bypasses it. Point SPARK_MASTER at the cluster "
            f"(spark://spark-master:7077)."
        )

    # `pyspark` here is the pip-installed runtime baked into
    # Dockerfile.airflow, and it is the DRIVER. It has NONE of the
    # Iceberg/Nessie/S3A jars that Dockerfile.spark curls into
    # spark-master/spark-worker's /opt/spark/jars, so the driver still has to
    # resolve every one of them via Ivy or the first Iceberg SQL statement
    # fails with ClassNotFoundException before it even runs.
    #
    # Keep this list even though the executors already have most of it baked
    # in. spark.jars.packages jars are shipped from the driver's file server
    # to every executor, so what the executors actually load is what is
    # resolved here -- which is why the versions must stay equal to
    # Dockerfile.spark's, and why hadoop-aws (which the Spark image does NOT
    # bake) reaches the executors at all.
    packages = ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.99.0",
        # Needed separately from iceberg-aws-bundle: reading landing CSVs via
        # spark.read.csv("s3a://...") goes through Hadoop's S3A connector,
        # not Iceberg's own S3FileIO, and Spark's official binaries don't
        # bundle hadoop-aws by default.
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", packages)
        # The driver runs in the calling container and does no task work, so
        # it needs far less heap than the old local[*] session did -- but not
        # the 1g default, which is tight once Iceberg/Nessie/aws-sdk-bundle
        # classes are loaded and exercised across repeated catalog operations.
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        # spark.driver.host is left at its default: Spark advertises this
        # container's hostname, and Docker's embedded DNS resolves it from
        # spark-worker, so executors can call back. Verified live -- a task
        # scheduled on the worker returned its result to a driver advertising
        # the raw container id.
        #
        # CAP THE APP so one job cannot take the whole cluster. Standalone
        # mode gives an application every free core by default and holds them
        # until it stops; two overlapping jobs would leave the second waiting
        # forever with "Initial job has not accepted any resources" rather
        # than failing. The `lakehouse_write` pool already serialises the
        # WRITERS -- this is what keeps the read-only jobs outside that pool
        # (arrival checks, completeness, maintenance metrics) from colliding.
        # Sized against SPARK_WORKER_CORES/SPARK_WORKER_MEMORY in
        # docker-compose.yml: three concurrent applications fit.
        .config("spark.cores.max", os.environ.get("SPARK_APP_CORES", "2"))
        .config("spark.executor.cores", os.environ.get("SPARK_APP_CORES", "2"))
        .config("spark.executor.memory", os.environ.get("SPARK_APP_MEMORY", "2g"))
        .config(
            # ORDER MATTERS. Each extension injects a parser that wraps the
            # previous one, so the LAST listed ends up outermost. Iceberg's
            # `rewrite_data_files(strategy => 'sort', sort_order => ...)` checks
            # `parser instanceof ExtendedParser` against the session's active
            # parser -- with Nessie last, Nessie's parser is outermost and the
            # check fails:
            #   java.lang.IllegalStateException: Cannot parse order: parser is
            #   not an Iceberg ExtendedParser
            # which broke maintenance on every `prepared`/`reporting` table,
            # since maintenance.yml gives those layers strategy: sort.
            # Nessie first, Iceberg last.
            "spark.sql.extensions",
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", nessie_uri)
        .config(f"spark.sql.catalog.{CATALOG}.ref", ref)
        .config(f"spark.sql.catalog.{CATALOG}.authentication.type", "NONE")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()


# --------------------------------------------------------------------- nessie
class Nessie:
    """Thin wrapper over Nessie's REST API.

    Deliberately REST rather than the Spark SQL extensions for branch
    management, so branch lifecycle can be driven from Airflow tasks that do
    not need a Spark session (cheaper pods, faster failure).
    """

    def __init__(self, uri: str | None = None):
        self.uri = (uri or os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")).rstrip("/")

    def _req(self, method: str, path: str, **kwargs):
        import requests

        r = requests.request(method, f"{self.uri}{path}", timeout=30, **kwargs)
        if not r.ok:
            # requests' default raise_for_status() drops the response body,
            # which is exactly where Nessie puts the useful part (status/
            # reason/message/errorCode) -- surface it instead of a bare
            # "404 Client Error" with no context.
            raise requests.exceptions.HTTPError(
                f"{r.status_code} {r.reason} for url {r.url}: {r.text}", response=r
            )
        return r.json() if r.content else {}

    def get_reference(self, name: str) -> dict[str, Any]:
        return self._req("GET", f"/trees/{_urlquote(name, safe='')}")

    def create_branch(self, name: str, from_ref: str = "main",
                      exist_ok: bool = False) -> dict[str, Any]:
        """POST /v2/trees?name=<new>&type=BRANCH

        Per Nessie's v2 REST spec (confirmed against the live server's
        /nessie-openapi/openapi.yaml), the new reference's name/type are
        QUERY params, and the JSON body is the SOURCE reference being
        branched from (not the new branch) -- i.e. {type, name, hash} of
        `from_ref`. Getting this backwards produces a 404
        "Named reference '<new-name>' not found", since the server tries to
        resolve the body's name as the existing source ref.
        """
        if exist_ok:
            # Retries must not be poisoned by their own previous attempt. A
            # failed build deliberately leaves its branch behind for
            # inspection (see dbt_builds.keep_failed_branch), so re-running the
            # task hits 409 Conflict "already exists" and can NEVER succeed --
            # which made `retries` actively harmful rather than useless.
            # Reusing the branch is the right behaviour: it is the same run_id,
            # so it is the same logical build.
            try:
                existing = self.get_reference(name)
                log_msg = f"branch {name} already exists; reusing it"
                import logging as _logging
                _logging.getLogger("nessie").info(log_msg)
                return existing
            except Exception:
                pass
        src = self.get_reference(from_ref)["reference"]
        return self._req(
            "POST",
            "/trees",
            params={"name": name, "type": "BRANCH"},
            json={"type": src["type"], "name": src["name"], "hash": src["hash"]},
        )

    def merge(self, from_branch: str, into: str = "main") -> dict[str, Any]:
        """POST /v2/trees/{branch}@{expectedHash}/history/merge

        v2 has no separate `expectedHash` body field -- the target's expected
        HEAD is pinned via `name@hash` in the path (mandatory: the server
        rejects an unpinned merge with "Expected hash must be provided").
        This only works once `into` has at least one real commit -- pinning
        at Nessie's sentinel "no ancestor" hash (a boundary marker, not an
        actual graph node) fails with "No common ancestor in parents of
        <sentinel> and <source-hash>". Callers are expected to have run a
        one-time bootstrap commit against `into` before the first merge (see
        `_bootstrap_main_if_empty` in ingest_feed.py) so this path is only
        ever hit once `into` is off the sentinel.
        """
        src = self.get_reference(from_branch)["reference"]
        tgt = self.get_reference(into)["reference"]
        return self._req(
            "POST",
            f"/trees/{_urlquote(into, safe='')}@{tgt['hash']}/history/merge",
            json={"fromRefName": from_branch, "fromHash": src["hash"]},
        )

    def create_tag(self, name: str, from_ref: str = "main") -> dict[str, Any]:
        src = self.get_reference(from_ref)["reference"]
        return self._req(
            "POST",
            "/trees",
            params={"name": name, "type": "TAG"},
            json={"type": src["type"], "name": src["name"], "hash": src["hash"]},
        )

    def delete_reference(self, name: str) -> None:
        """DELETE /v2/trees/{name}@{hash}?type=...

        Like merge, the expected hash rides in the path (`name@hash`), not
        as a query param -- an `expectedHash` query param is silently
        ignored by the server.
        """
        ref = self.get_reference(name)["reference"]
        self._req("DELETE", f"/trees/{_urlquote(name, safe='')}@{ref['hash']}",
                  params={"type": ref["type"]})

    def list_entries(self, ref: str) -> list[dict[str, Any]]:
        """Every content entry on `ref`, with the content payload inlined.

        `content=true` is what makes `metadataLocation` available, which is the
        only way to learn where a table's files actually live. Without it you
        get names and ids and no way to map a table to object storage.
        """
        out, token = [], None
        enc = _urlquote(ref, safe="")
        while True:
            params: dict[str, Any] = {"content": "true"}
            if token:
                params["page-token"] = token
            page = self._req("GET", f"/trees/{enc}/entries", params=params)
            out.extend(page.get("entries", []))
            token = page.get("token")
            if not token:
                break
        return out

    def list_references(self, prefix: str = "",
                        fetch_all: bool = False) -> list[dict[str, Any]]:
        """List references, optionally with their commit metadata.

        `fetch_all=True` adds `fetch=ALL`, which is what makes each reference
        carry a `metadata.commitMetaOfHEAD.commitTime`. WITHOUT it the server
        returns only type/name/hash -- no metadata key at all. Any caller that
        wants to reason about a branch's age must pass it, or every age check
        silently sees `None` and treats every branch as arbitrarily old.
        Costs an extra lookup per reference server-side, so it is opt-in.
        """
        out, token = [], None
        while True:
            params: dict[str, Any] = {"fetch": "ALL"} if fetch_all else {}
            if token:
                params["page-token"] = token
            page = self._req("GET", "/trees", params=params)
            out.extend(page.get("references", []))
            token = page.get("token")
            if not token:
                break
        return [r for r in out if r["name"].startswith(prefix)]
