"""The one place a SparkSession is built.

SPLIT OUT OF `context.py` FOR THE IMPORT, not for tidiness. Everything in this
repo reads `feeds.yml` through `context`, and while this function lived there
so did `from pyspark.sql import SparkSession` -- inside the function, which
kept it lazy, but the module still could not be reasoned about, tested or read
without the engine half in view. Config and engine are now separable: the feed
console, the registry CLI and every config-level test import one and not the
other.

`SPARK_MASTER` IS STILL READ IN TWO PLACES AND THEY MUST NOT DIVERGE -- here
and `spark.master` in `dbt/profiles.yml`. Moving this file does not change
that; see docs/DECISIONS.md#spark-master-single-source.
"""
from __future__ import annotations

import os
from urllib.parse import urlparse

from reporting_platform.common import settings
from reporting_platform.common.settings import CATALOG

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

    endpoint = settings.s3_endpoint()
    warehouse = settings.warehouse()
    nessie_uri = settings.nessie_uri()
    # Hadoop's S3A connector does not infer TLS from the endpoint URL's own
    # scheme -- it has a separate switch, and it used to be hardcoded off
    # (fine for MinIO in http-only compose). A second env var for this would
    # just be one more thing to keep in sync with S3_ENDPOINT, so it derives
    # from the same URL instead: https:// turns it on. See
    # docs/DECISIONS.md#s3-ssl-follows-the-endpoint-scheme.
    s3_ssl_enabled = "true" if urlparse(endpoint).scheme == "https" else "false"

    # No local[*] fallback, deliberately: running in-container is a config
    # error that LOOKS like success. The default below matches
    # docker-compose.yml so a bare `python -m ...` still works.
    # See docs/DECISIONS.md#spark-master-no-local-fallback
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
    # Iceberg/Nessie/S3A jars Dockerfile.spark bakes into the workers'
    # /opt/spark/jars, so without them the first Iceberg SQL statement fails
    # with ClassNotFoundException.
    #
    # They are BAKED INTO THE IMAGE, and PLATFORM_DRIVER_JARS lists them --
    # the same variable dbt/profiles.yml reads, so the two drivers cannot
    # diverge, and the versions live once, in the Dockerfile's ARGs. They are
    # set as spark.jars, which ships them to every executor as well: that is
    # how hadoop-aws (not baked into the Spark image) reaches them at all.
    # This used to be spark.jars.packages, resolved from Maven Central by Ivy
    # on every cold start -- a runtime dependency on egress.
    # See docs/DECISIONS.md#driver-jars-are-baked
    jars = os.environ.get("PLATFORM_DRIVER_JARS", "").strip()
    if not jars:
        raise RuntimeError(
            "PLATFORM_DRIVER_JARS is not set, so this driver has no Iceberg, "
            "Nessie or S3A jars. Dockerfile.airflow sets it in the image; a "
            "process started outside that image has to set it itself.")
    missing = [p for p in jars.split(",") if not os.path.isfile(p)]
    if missing:
        raise RuntimeError(
            f"PLATFORM_DRIVER_JARS names jars that are not here: {missing}. "
            f"Rebuild the image (Dockerfile.airflow bakes them) rather than "
            f"pointing this at another copy -- their versions must equal "
            f"Dockerfile.spark's.")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars", jars)
        # The driver does no task work, so it needs far less heap than a
        # local[*] session would -- but not the 1g default, which is tight once
        # Iceberg/Nessie/aws-sdk-bundle classes are loaded and exercised.
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        # spark.driver.host is left at its default: Spark advertises this
        # container's hostname and Docker's embedded DNS resolves it from
        # spark-worker, so executors can call back. Verified live.
        #
        # CAP THE APP so one job cannot take the whole cluster. Standalone mode
        # grants an application every free core by default and holds them until
        # it stops; two overlapping jobs would leave the second waiting forever
        # rather than failing. The `lakehouse_write` pool already serialises the
        # WRITERS -- this is what keeps read-only jobs outside that pool from
        # colliding. Sized against SPARK_WORKER_CORES/SPARK_WORKER_MEMORY:
        # three concurrent applications fit.
        .config("spark.cores.max", os.environ.get("SPARK_APP_CORES", "2"))
        .config("spark.executor.cores", os.environ.get("SPARK_APP_CORES", "2"))
        .config("spark.executor.memory", os.environ.get("SPARK_APP_MEMORY", "2g"))
        .config(
            # ORDER MATTERS. Each extension injects a parser wrapping the
            # previous one, so the LAST listed ends up outermost. Iceberg's
            # `rewrite_data_files(strategy => 'sort', sort_order => ...)`
            # checks `parser instanceof ExtendedParser`; with Nessie last it
            # fails with "Cannot parse order: parser is not an Iceberg
            # ExtendedParser", which broke maintenance on every
            # prepared/reporting table. Nessie first, Iceberg last.
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
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl_enabled)
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()
