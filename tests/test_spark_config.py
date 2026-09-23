"""Where Spark's catalog and S3 configuration comes from, per environment.

THREE SOURCES configure a Spark session here: `conf/spark-defaults.conf`
(shipped in the cluster's Spark image), `spark_session()` in
`common/spark.py`, and the two dbt targets in `dbt/profiles.yml`. The cluster
target, `spark_ocp`, used to take everything but the ref from the conf file --
which named `http://minio:9000`, `http://nessie:19120` and
`ssl.enabled false`. A cluster built from that image talks to hosts that do
not exist there, and nothing about the failure says "config".

So the conf file holds only what is the same everywhere, and every
per-environment value is read from the environment. What this checks:

- no source names a local host, and none writes `ssl.enabled` as a literal
  -- it derives from `S3_ENDPOINT`'s scheme;
- `spark_ocp` RENDERS with TLS on for an `https://` endpoint and names no
  local host, which is the check the plan's "Done when" asks for;
- every key `spark_local` sets reaches `spark_ocp` (a YAML anchor merges
  them: its driver runs in the platform image, which has no conf file), and
  `spark_ocp` puts the executors in pods;
- the extensions are in the same order everywhere they are written.

`common/spark.py` reads its endpoints through `common/settings.py`, whose
local defaults are the only place compose's hosts are written
(docs/DECISIONS.md#settings-refuse-outside-local). No stack, no network.
See docs/DECISIONS.md#spark-defaults-hold-only-invariants
"""
from __future__ import annotations

import re

import jinja2
import yaml

from tests.support import repo_file

CONF = "conf/spark-defaults.conf"
PROFILES = "dbt/profiles.yml"
SPARK_PY = "reporting_platform/common/spark.py"

LOCAL_HOSTS = re.compile(r"minio|nessie:19120|localhost:9000|spark-master")

# `ssl.enabled` followed by a hardcoded true/false, in any of the three
# syntaxes: conf whitespace, YAML `key: "false"`, Python `.config("...", "false")`.
LITERAL_SSL = re.compile(
    r"""ssl\.enabled["']?\s*[:,\s]\s*["']?(true|false)\b""", re.IGNORECASE)

EXTENSIONS_KEY = "spark.sql.extensions"


def _text(path: str) -> str:
    return repo_file(path).read_text()


def _conf() -> dict[str, str]:
    out = {}
    for line in _text(CONF).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition(" ")
        out[key] = value.strip()
    return out


def _targets() -> dict:
    return yaml.safe_load(_text(PROFILES))["reporting_platform"]["outputs"]


def _render(target: str, env: dict[str, str]) -> dict[str, str]:
    """Render one target's server_side_parameters the way dbt would: env_var()
    with no default raises on a missing variable, var() takes its default."""
    def env_var(name, default=None):
        if name in env:
            return env[name]
        if default is None:
            raise KeyError(f"env_var({name!r}) is required and unset")
        return default

    def var(name, default=None):
        return default

    params = _targets()[target]["server_side_parameters"]
    return {
        k: jinja2.Template(str(v)).render(env_var=env_var, var=var)
        for k, v in params.items()
    }


CLUSTER_ENV = {
    "S3_ENDPOINT": "https://s3.example",
    "NESSIE_URI": "https://nessie.example/api/v2",
    "REPORTING_WAREHOUSE": "s3a://lakehouse/warehouse",
    "PLATFORM_DRIVER_JARS": "/opt/platform/jars/a.jar",
    "SPARK_MASTER": "k8s://https://kubernetes.default.svc:443",
    "SPARK_K8S_NAMESPACE": "reporting",
    "SPARK_EXECUTOR_IMAGE": "registry.example/spark@sha256:abc",
    "SPARK_SERVICE_ACCOUNT": "spark",
    "POD_IP": "10.0.0.7",
}


def test_the_conf_file_names_no_host_and_no_per_environment_key():
    conf = _conf()
    hosts = {k: v for k, v in conf.items() if LOCAL_HOSTS.search(v)}
    assert not hosts, f"{CONF} names a local host: {hosts}"
    per_env = [k for k in conf if re.search(
        r"\.(uri|warehouse|endpoint|ref|ssl\.enabled|authentication\.type"
        r"|credentials\.provider|access\.key|secret\.key)$", k)]
    assert not per_env, (
        f"{CONF} sets per-environment keys {per_env}; they belong in "
        f"spark_session() and dbt/profiles.yml, read from the environment")


def test_no_source_writes_ssl_enabled_as_a_literal():
    for path in (CONF, PROFILES, SPARK_PY):
        hit = LITERAL_SSL.search(_text(path))
        assert not hit, (
            f"{path} hardcodes {hit.group(0)!r}; TLS derives from "
            f"S3_ENDPOINT's scheme (DECISIONS.md#s3-ssl-follows-the-endpoint-scheme)")


# Two profile lines may name a local host by design: `host:` is inert in
# session mode but dbt-spark demands it, and spark.master's default IS the
# compose cluster -- spark_local is the local target, and spark_ocp sets no
# master at all.
EXEMPT_PROFILE_LINE = re.compile(r"^\s*(#|host:|spark\.master:)")


def test_profiles_name_no_local_host():
    hits = [line.strip() for line in _text(PROFILES).splitlines()
            if LOCAL_HOSTS.search(line) and not EXEMPT_PROFILE_LINE.match(line)]
    assert not hits, f"{PROFILES} names a local host: {hits}"
    rendered = _render("spark_ocp", CLUSTER_ENV)
    assert not any(LOCAL_HOSTS.search(v) for v in rendered.values()), rendered


# spark.py's own master default IS the compose cluster, as in profiles.yml,
# so only the store hosts are checked there.
STORE_HOSTS = re.compile(r"minio|nessie:19120")


def test_spark_session_names_no_store_host():
    code = [line.strip() for line in _text(SPARK_PY).splitlines()
            if STORE_HOSTS.search(line) and not line.strip().startswith("#")]
    assert not code, (
        f"{SPARK_PY} names a store host {code}; read it through "
        f"common/settings.py, which refuses outside REPORTING_ENV=local")


def test_spark_ocp_turns_tls_on_for_an_https_endpoint():
    rendered = _render("spark_ocp", CLUSTER_ENV)
    assert rendered["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"
    assert rendered["spark.hadoop.fs.s3a.endpoint"] == "https://s3.example"
    assert rendered["spark.sql.catalog.lakehouse.s3.endpoint"] == "https://s3.example"
    assert rendered["spark.sql.catalog.lakehouse.uri"] == CLUSTER_ENV["NESSIE_URI"]
    plain = _render("spark_ocp", {**CLUSTER_ENV, "S3_ENDPOINT": "http://s3.example"})
    assert plain["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"


def test_spark_ocp_refuses_to_render_without_the_environment():
    # env_var() with no default: a missing variable is an error at dbt
    # startup, not a silent fallback to a local host.
    for missing in CLUSTER_ENV:
        env = {k: v for k, v in CLUSTER_ENV.items() if k != missing}
        try:
            _render("spark_ocp", env)
        except KeyError as exc:
            assert missing in str(exc)
        else:
            raise AssertionError(f"spark_ocp rendered with {missing} unset")


def test_every_spark_local_key_reaches_spark_ocp():
    # spark_ocp's driver is the Airflow task's dbt child process, in the
    # platform image, which has no spark-defaults.conf -- so it must carry
    # EVERY key spark_local does. It merges them through a YAML anchor.
    # See docs/DECISIONS.md#execution-mode-is-configuration
    local = set(_targets()["spark_local"]["server_side_parameters"])
    ocp = set(_targets()["spark_ocp"]["server_side_parameters"])
    assert not local - ocp, (
        f"spark_local sets {sorted(local - ocp)} and spark_ocp does not")


def test_spark_ocp_puts_the_executors_in_pods():
    rendered = _render("spark_ocp", CLUSTER_ENV)
    assert rendered["spark.master"].startswith("k8s://"), rendered["spark.master"]
    assert rendered["spark.kubernetes.namespace"] == "reporting"
    assert rendered["spark.kubernetes.container.image"] == CLUSTER_ENV["SPARK_EXECUTOR_IMAGE"]
    assert rendered["spark.driver.host"] == "10.0.0.7"
    # and spark_local stays on the standalone cluster
    local = _render("spark_local", {k: v for k, v in CLUSTER_ENV.items()
                                    if k != "SPARK_MASTER"})
    assert local["spark.master"] == "spark://spark-master:7077"


def _extensions(value: str) -> list[str]:
    return [e.strip() for e in value.split(",") if e.strip()]


def test_extensions_are_in_the_same_order_everywhere():
    conf = _extensions(_conf()[EXTENSIONS_KEY])
    local = _extensions(_targets()["spark_local"]["server_side_parameters"][EXTENSIONS_KEY])
    # spark.py writes the value as adjacent string literals after the key
    m = re.search(r'"spark\.sql\.extensions"[,:]\s*((?:"[^"]*"\s*)+)', _text(SPARK_PY))
    assert m, f"{SPARK_PY}: spark.sql.extensions not found"
    py = _extensions("".join(re.findall(r'"([^"]*)"', m.group(1))))
    assert conf == local == py, (conf, local, py)
    # Nessie first, Iceberg last, or rewrite_data_files cannot parse a sort
    # order (see the comment in spark_session()).
    assert "Iceberg" in py[-1], py
