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

def session_conf(app_name: str, ref: str = "main") -> tuple[str, dict[str, str]]:
    """(master, every .config pair) for a session bound to the Nessie catalog
    at `ref`. Pure -- no JVM -- so what a session WOULD be is testable, in
    both execution modes, without starting one.
    """
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
    #
    # EXCEPT WHEN THAT IS THE DECLARED MODE. `embedded` runs every task in
    # this process on purpose, and there it is a cluster master that would be
    # the mistake -- so the two refusals mirror each other and neither mode
    # can be reached by a typo in the other's master.
    master = os.environ.get("SPARK_MASTER") or "spark://spark-master:7077"
    mode = settings.execution()
    if mode == "embedded":
        if not master.startswith("local"):
            raise RuntimeError(
                f"PLATFORM_EXECUTION is 'embedded' but SPARK_MASTER is "
                f"{master!r}. Embedded runs every task in this process: set "
                f"SPARK_MASTER=local[2] (or local[N]), which dbt/profiles.yml "
                f"reads too.")
    elif master.startswith("local"):
        raise RuntimeError(
            f"SPARK_MASTER is {master!r}. This platform runs every Spark job on "
            f"the spark-master/spark-worker cluster; an in-process local session "
            f"silently bypasses it. Point SPARK_MASTER at the cluster "
            f"(spark://spark-master:7077), or set PLATFORM_EXECUTION=embedded "
            f"if running without one is the intent.")
    kubernetes = mode == "kubernetes"
    if kubernetes != master.startswith("k8s://"):
        # The two must agree, or a cluster runs its executors somewhere its
        # execution mode does not describe -- or a laptop tries to reach an
        # API server. See docs/DECISIONS.md#execution-mode-is-configuration
        raise RuntimeError(
            f"PLATFORM_EXECUTION is {mode!r} but SPARK_MASTER "
            f"is {master!r}. `kubernetes` needs a k8s:// master and `{mode}` "
            f"must not have one.")

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

    cores = os.environ.get("SPARK_APP_CORES", "2")
    conf = {
        "spark.app.name": app_name,
        "spark.jars": jars,
        # The driver does no task work, so it needs far less heap than a
        # local[*] session would -- but not the 1g default, which is tight once
        # Iceberg/Nessie/aws-sdk-bundle classes are loaded and exercised.
        "spark.driver.memory": "2g",
        "spark.driver.maxResultSize": "1g",
        # CAP THE APP so one job cannot take the whole cluster. Standalone mode
        # grants an application every free core by default and holds them until
        # it stops; two overlapping jobs would leave the second waiting forever
        # rather than failing. The `lakehouse_write` pool already serialises the
        # WRITERS -- this is what keeps read-only jobs outside that pool from
        # colliding. Sized against SPARK_WORKER_CORES/SPARK_WORKER_MEMORY:
        # three concurrent applications fit.
        "spark.cores.max": cores,
        "spark.executor.cores": cores,
        "spark.executor.memory": os.environ.get("SPARK_APP_MEMORY", "2g"),
        # ORDER MATTERS. Each extension injects a parser wrapping the
        # previous one, so the LAST listed ends up outermost. Iceberg's
        # `rewrite_data_files(strategy => 'sort', sort_order => ...)`
        # checks `parser instanceof ExtendedParser`; with Nessie last it
        # fails with "Cannot parse order: parser is not an Iceberg
        # ExtendedParser", which broke maintenance on every
        # prepared/reporting table. Nessie first, Iceberg last.
        "spark.sql.extensions":
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.catalog-impl":
            "org.apache.iceberg.nessie.NessieCatalog",
        f"spark.sql.catalog.{CATALOG}.uri": nessie_uri,
        f"spark.sql.catalog.{CATALOG}.ref": ref,
        # See docs/DECISIONS.md#nessie-auth-is-a-setting
        f"spark.sql.catalog.{CATALOG}.authentication.type":
            settings.nessie_auth_type(),
        f"spark.sql.catalog.{CATALOG}.warehouse": warehouse,
        f"spark.sql.catalog.{CATALOG}.io-impl":
            "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG}.s3.endpoint": endpoint,
        f"spark.sql.catalog.{CATALOG}.s3.path-style-access": "true",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": s3_ssl_enabled,
        "spark.sql.session.timeZone": "UTC",
    }
    token = settings.nessie_auth_token()
    if token:
        conf[f"spark.sql.catalog.{CATALOG}.authentication.token"] = token

    if kubernetes:
        # EXECUTORS AS PODS, the driver where this process is: in its own pod
        # when `spark_task.run` launched it, in the Airflow task's pod for a
        # dbt build. Client mode, so the executors call back to THIS pod,
        # which is why its IP -- from the downward API, set by the pod spec
        # `spark_task` builds and by the chart -- is required rather than
        # guessed. spark.cores.max means nothing here; instances x cores is
        # the same 2-core cap. See docs/DECISIONS.md#execution-mode-is-configuration
        pod_ip = os.environ.get("POD_IP", "").strip()
        if not pod_ip:
            raise RuntimeError(
                "PLATFORM_EXECUTION is 'kubernetes' and POD_IP is not set, so "
                "executors cannot call this driver back. The pod spec sets it "
                "from the downward API (status.podIP).")
        conf.pop("spark.cores.max")
        conf.update({
            "spark.kubernetes.namespace": settings.kubernetes("SPARK_K8S_NAMESPACE"),
            "spark.kubernetes.container.image":
                settings.kubernetes("SPARK_EXECUTOR_IMAGE"),
            "spark.kubernetes.authenticate.driver.serviceAccountName":
                settings.kubernetes("SPARK_SERVICE_ACCOUNT"),
            "spark.executor.instances": os.environ.get("SPARK_APP_EXECUTORS", "1"),
            "spark.driver.host": pod_ip,
            "spark.driver.bindAddress": "0.0.0.0",
        })
        # EXECUTOR PODS GET NONE OF THE PLATFORM'S ENVIRONMENT. Locally the
        # spark-worker container carries AWS_*, and its executors inherit it;
        # a pod Spark creates has only what these keys give it. Without them
        # the first S3 read on an executor fails with "Unable to load region
        # from any of the providers in the chain" (found by the local-k8s
        # smoke). The credentials come from the platform Secret by
        # reference, so they never appear in the Spark conf or its UI.
        secret = settings.kubernetes("PLATFORM_ENV_SECRET")
        conf["spark.executorEnv.AWS_REGION"] = os.environ.get("AWS_REGION", "us-east-1")
        for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            conf[f"spark.kubernetes.executor.secretKeyRef.{var}"] = f"{secret}:{var}"
    return master, conf


def spark_session(app_name: str, ref: str = "main"):
    """Build a Spark session bound to the Nessie catalog at a given ref.

    `ref` is the Nessie branch. Ingest and dbt builds run on a working branch;
    maintenance and snapshot expiry run on main.

    THE SESSION IS A CLIENT OF A CLUSTER, never local[*]. The caller's
    process is the driver; every task runs in an executor -- on
    `spark-worker` locally, in executor pods on Kubernetes. What it is
    configured with is `session_conf()`.
    """
    from pyspark.sql import SparkSession

    master, conf = session_conf(app_name, ref)
    builder = SparkSession.builder.master(master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
