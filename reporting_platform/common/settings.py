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


def missing() -> list[str]:
    """Every variable this environment requires that is unset.

    Empty in `local`, where the endpoints have defaults and REGISTRY_DSN is
    left to registry_dsn()'s own refusal: `config check` runs in the cheap CI
    tier with no registry, and must not need one.
    """
    if env() == "local":
        return []
    return [n for n in REQUIRED if not os.environ.get(n, "").strip()]
