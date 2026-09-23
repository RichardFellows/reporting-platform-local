"""The environment-derived settings, in one place.

SEPARATE FROM `context.py` SO THE CLIENTS CAN BE. `spark.py` needs the catalog
name and `context.py` needs all three; putting them in either would make the
other import it, and `context` importing `spark` is what forces every reader
of `feeds.yml` to have pyspark installed. A module with no imports of its own
can be imported by both. It still imports nothing but `os` and `pathlib`.

The three constants are read at import, so a process that changes one of them
in its own environment must be started with it set -- which is how every
container here works, and why `docker-compose.yml` edits need the container
RECREATED, not restarted. The endpoint accessors below read on every call.
"""
from __future__ import annotations

import os
from pathlib import Path

CONFIG_DIR = Path(os.environ.get("REPORTING_CONFIG_DIR",
                                 "/opt/platform/reporting_platform/config"))
CATALOG = os.environ.get("REPORTING_CATALOG", "lakehouse")
ENV = os.environ.get("REPORTING_ENV", "local")


# ------------------------------------------------------------------ endpoints
# WHERE THE PLATFORM'S STORES ARE, and the only place their local defaults are
# written. Each accessor returns the compose default ONLY when REPORTING_ENV is
# `local`; anywhere else an unset variable is a refusal that names it. A
# default here is a host that exists only in docker-compose.yml, and a cluster
# process falling back to `http://minio:9000` fails later, somewhere else,
# with an error about a connection rather than about configuration -- the
# same reason spark_session() refuses a `local` master rather than falling
# back to one (docs/DECISIONS.md#spark-master-no-local-fallback).
#
# Read on every call, not at import like the constants above: tests and
# `registry` commands switch REPORTING_ENV inside one process.
# tests/test_settings.py fails if a direct os.environ read of one of these
# names comes back anywhere else. See docs/DECISIONS.md#settings-refuse-outside-local

LOCAL_DEFAULTS = {
    "S3_ENDPOINT": "http://minio:9000",
    "NESSIE_URI": "http://nessie:19120/api/v2",
    "REPORTING_WAREHOUSE": "s3a://lakehouse/warehouse",
    "REPORTING_LANDING": "s3a://lakehouse/landing",
}

# Required outside `local`. REGISTRY_DSN is on this list but has no local
# default either -- see registry_dsn().
REQUIRED = (*LOCAL_DEFAULTS, "REGISTRY_DSN")


class MissingSetting(RuntimeError):
    """A variable this environment requires is unset."""


def env() -> str:
    """REPORTING_ENV now, not at import."""
    return os.environ.get("REPORTING_ENV", "local")


def _required(name: str) -> str:
    # An empty value is unset: compose's `${X:-}` produces one.
    value = os.environ.get(name, "").strip()
    if value:
        return value
    current = env()
    if current == "local" and name in LOCAL_DEFAULTS:
        return LOCAL_DEFAULTS[name]
    raise MissingSetting(
        f"{name} is not set and REPORTING_ENV is {current!r}. Only `local` "
        f"falls back to the compose default ({LOCAL_DEFAULTS.get(name, 'none')}); "
        f"every other environment must set it -- in the deployment's "
        f"ConfigMap or Secret, or docker-compose.yml locally, where a "
        f"container started before it was added needs RECREATING.")


def s3_endpoint() -> str:
    """The S3-compatible endpoint URL. Its scheme also decides S3A TLS
    (docs/DECISIONS.md#s3-ssl-follows-the-endpoint-scheme)."""
    return _required("S3_ENDPOINT")


def nessie_uri() -> str:
    """Nessie's REST API v2 base URL."""
    return _required("NESSIE_URI")


def warehouse() -> str:
    """The Iceberg warehouse root, `s3a://<bucket>/<prefix>`."""
    return _required("REPORTING_WAREHOUSE")


def landing() -> str:
    """The landing root, `s3a://<bucket>/landing`."""
    return _required("REPORTING_LANDING")


def bucket_of(uri: str) -> str:
    """`s3a://lakehouse/warehouse` -> `lakehouse`."""
    return uri.split("//", 1)[-1].split("/", 1)[0]


def registry_dsn() -> str:
    """The registry connection string, or a refusal -- IN EVERY ENVIRONMENT.

    NO DEFAULT, deliberately, not even locally. Every other connection string
    in this stack is written down in `docker-compose.yml` where it can be
    seen and changed; a default here would let a container come up pointing
    at a database nobody configured and report a healthy, empty registry.
    """
    value = os.environ.get("REGISTRY_DSN", "").strip()
    if not value:
        raise MissingSetting(
            "REGISTRY_DSN is not set, so the delivery registry has no store. "
            "It is set on the shared `x-airflow-common` environment block in "
            "docker-compose.yml; a container started before that was added "
            "needs recreating, not restarting.")
    return value


# ------------------------------------------------------------ execution mode
# WHERE A SPARK DRIVER RUNS, AND WHERE ITS EXECUTORS DO. `local` is compose:
# `spark_task.run` starts the driver as a child process and the executors
# run on the standalone spark-worker. `kubernetes` is a cluster:
# `spark_task.run` starts the driver as its own POD (same image, same module,
# same arguments) and the executors are pods too, through a `k8s://` master.
# dbt is the exception, and deliberately: Cosmos stays LOCAL + SUBPROCESS in
# both modes, because its artifact archive, validation capture and the
# publish gate read dbt's target/ from the task's own filesystem. In
# `kubernetes` its child process is the driver and only the executors move.
# See docs/DECISIONS.md#execution-mode-is-configuration

EXECUTION_MODES = ("local", "kubernetes")

# Required when PLATFORM_EXECUTION=kubernetes, in every environment -- there
# is no compose default for a namespace or an image.
KUBERNETES_REQUIRED = (
    "SPARK_K8S_NAMESPACE",      # where driver and executor pods run
    "SPARK_DRIVER_IMAGE",       # the platform RELEASE image (Dockerfile.airflow)
    "SPARK_EXECUTOR_IMAGE",     # the Spark image (Dockerfile.spark)
    "SPARK_SERVICE_ACCOUNT",    # may create executor pods (the chart's RBAC)
    "PLATFORM_ENV_CONFIGMAP",   # every setting a driver pod needs, by envFrom
    "PLATFORM_ENV_SECRET",      # the credentials it needs, by envFrom
)


def execution() -> str:
    """PLATFORM_EXECUTION, validated. An unknown value is refused rather than
    read as `local`: a typo on a cluster would otherwise run every driver
    inside the Airflow task and look like success."""
    value = os.environ.get("PLATFORM_EXECUTION", "local").strip() or "local"
    if value not in EXECUTION_MODES:
        raise MissingSetting(
            f"PLATFORM_EXECUTION is {value!r}; it must be one of "
            f"{', '.join(EXECUTION_MODES)}.")
    return value


def kubernetes(name: str) -> str:
    """One of KUBERNETES_REQUIRED, or a refusal naming it."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise MissingSetting(
            f"{name} is not set and PLATFORM_EXECUTION is 'kubernetes'. The "
            f"chart's ConfigMap sets it; see docs/DECISIONS.md"
            f"#execution-mode-is-configuration.")
    return value


# ----------------------------------------------------------------- Nessie auth
# How the Nessie catalog authenticates, read by all four of its clients:
# spark_session(), both dbt targets and the Python `Nessie` REST client. The
# shared Nessie has NO auth today, so NONE is the default in every
# environment, not only `local` -- it is a real value, not a fallback.
# BEARER takes a token from a Secret.
#
# The token's name carries dbt's DBT_ENV_SECRET_ prefix, and the Python side
# reads the SAME name: dbt scrubs a variable with that prefix from its logs
# and refuses it outside profiles.yml, and one secret with two names is two
# things to keep in step. See docs/DECISIONS.md#nessie-auth-is-a-setting
NESSIE_AUTH_TYPES = ("NONE", "BEARER")
NESSIE_TOKEN_VAR = "DBT_ENV_SECRET_NESSIE_AUTH_TOKEN"


def nessie_auth_type() -> str:
    value = (os.environ.get("NESSIE_AUTH_TYPE", "NONE").strip() or "NONE").upper()
    if value not in NESSIE_AUTH_TYPES:
        raise MissingSetting(
            f"NESSIE_AUTH_TYPE is {value!r}; it must be one of "
            f"{', '.join(NESSIE_AUTH_TYPES)}.")
    return value


def nessie_auth_token() -> str | None:
    """The bearer token when NESSIE_AUTH_TYPE is BEARER, else None."""
    if nessie_auth_type() != "BEARER":
        return None
    value = os.environ.get(NESSIE_TOKEN_VAR, "").strip()
    if not value:
        raise MissingSetting(
            f"NESSIE_AUTH_TYPE is BEARER and {NESSIE_TOKEN_VAR} is not set. "
            f"It comes from the platform Secret.")
    return value


def missing() -> list[str]:
    """Every variable this environment requires that is unset, or is set to
    a value it refuses.

    The endpoints are empty in `local`, where they have defaults, and
    REGISTRY_DSN is left to registry_dsn()'s own refusal: `config check`
    runs in the cheap CI tier with no registry, and must not need one. The
    execution-mode and Nessie-auth requirements apply in EVERY environment,
    because neither has a compose default to fall back to.
    """
    out = []
    if env() != "local":
        out += [n for n in REQUIRED if not os.environ.get(n, "").strip()]
    try:
        if execution() == "kubernetes":
            out += [n for n in KUBERNETES_REQUIRED
                    if not os.environ.get(n, "").strip()]
    except MissingSetting:
        out.append("PLATFORM_EXECUTION (unknown value)")
    try:
        if (nessie_auth_type() == "BEARER"
                and not os.environ.get(NESSIE_TOKEN_VAR, "").strip()):
            out.append(NESSIE_TOKEN_VAR)
    except MissingSetting:
        out.append("NESSIE_AUTH_TYPE (unknown value)")
    return out
