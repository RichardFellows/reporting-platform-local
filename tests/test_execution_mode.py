"""PLATFORM_EXECUTION: where a Spark driver runs, and where its executors do.

`local` is compose: `_spark_task.run` starts the driver as a child process
against the standalone master. `kubernetes` is a cluster: `run` starts the
driver as its own POD of the platform image -- same module, same arguments,
same JSON -- and the executors are pods through a `k8s://` master. dbt keeps
its driver in the task (Cosmos LOCAL) and only its executors move.

NO CLUSTER HERE. What is checked is everything short of one: the settings
refuse what they should, `session_conf()` produces the configuration each mode
needs (it is pure, so no JVM), the driver pod manifest carries what a driver
needs, and `run()` goes to the pod launcher rather than a subprocess. That the
pod actually runs is the local-k8s smoke test's job (plan 2d).
See docs/DECISIONS.md#execution-mode-is-configuration
"""
from __future__ import annotations

import contextlib
import os
import tempfile

from reporting_platform.common import settings

K8S = {
    "PLATFORM_EXECUTION": "kubernetes",
    "SPARK_MASTER": "k8s://https://kubernetes.default.svc:443",
    "SPARK_K8S_NAMESPACE": "reporting",
    "SPARK_DRIVER_IMAGE": "registry.example/platform@sha256:aaa",
    "SPARK_EXECUTOR_IMAGE": "registry.example/spark@sha256:bbb",
    "SPARK_SERVICE_ACCOUNT": "spark",
    "PLATFORM_ENV_CONFIGMAP": "platform-env",
    "PLATFORM_ENV_SECRET": "platform-secrets",
    "POD_IP": "10.1.2.3",
}
CLEAR = {k: None for k in (*K8S, "NESSIE_AUTH_TYPE", settings.NESSIE_TOKEN_VAR,
                           "REPORTING_ENV")}


@contextlib.contextmanager
def _env(**values):
    saved = {k: os.environ.get(k) for k in values}
    try:
        for k, v in values.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextlib.contextmanager
def _jars():
    """PLATFORM_DRIVER_JARS naming files that exist, as the image's do."""
    with tempfile.TemporaryDirectory() as d:
        paths = [os.path.join(d, n) for n in ("a.jar", "b.jar")]
        for p in paths:
            open(p, "w").close()
        with _env(PLATFORM_DRIVER_JARS=",".join(paths)):
            yield


def _raises(fn, *needles):
    try:
        fn()
    except (RuntimeError, settings.MissingSetting) as exc:
        for n in needles:
            assert n in str(exc), (n, str(exc))
        return
    raise AssertionError(f"{fn} did not refuse")


# ------------------------------------------------------------------ settings
def test_execution_defaults_to_local_and_refuses_an_unknown_mode():
    with _env(**CLEAR):
        assert settings.execution() == "local"
        with _env(PLATFORM_EXECUTION="kubernetes"):
            assert settings.execution() == "kubernetes"
        with _env(PLATFORM_EXECUTION="k8s"):
            _raises(settings.execution, "PLATFORM_EXECUTION", "k8s")


def test_kubernetes_mode_lists_every_setting_it_lacks():
    with _env(**CLEAR):
        assert settings.missing() == []
        with _env(PLATFORM_EXECUTION="kubernetes"):
            assert settings.missing() == list(settings.KUBERNETES_REQUIRED)
        with _env(**K8S):
            assert settings.missing() == []


def test_nessie_auth_is_none_by_default_and_bearer_needs_its_token():
    with _env(**CLEAR):
        assert settings.nessie_auth_type() == "NONE"
        assert settings.nessie_auth_token() is None
        with _env(NESSIE_AUTH_TYPE="bearer"):
            assert settings.missing() == [settings.NESSIE_TOKEN_VAR]
            _raises(settings.nessie_auth_token, settings.NESSIE_TOKEN_VAR)
            with _env(**{settings.NESSIE_TOKEN_VAR: "t0k"}):
                assert settings.nessie_auth_token() == "t0k"
                assert settings.missing() == []
        with _env(NESSIE_AUTH_TYPE="OAUTH2"):
            _raises(settings.nessie_auth_type, "OAUTH2")


# -------------------------------------------------------------- session_conf
def _conf(**env):
    from reporting_platform.common.spark import session_conf

    with _env(**CLEAR), _env(**env), _jars():
        return session_conf("t", ref="b1")


def test_local_mode_is_the_standalone_cluster_with_no_kubernetes_keys():
    master, conf = _conf()
    assert master == "spark://spark-master:7077"
    assert conf["spark.cores.max"] == "2"
    assert not [k for k in conf if k.startswith("spark.kubernetes.")]
    assert conf["spark.sql.catalog.lakehouse.authentication.type"] == "NONE"
    assert "spark.sql.catalog.lakehouse.authentication.token" not in conf


def test_kubernetes_mode_puts_the_executors_in_pods():
    master, conf = _conf(**K8S)
    assert master.startswith("k8s://")
    assert conf["spark.kubernetes.namespace"] == "reporting"
    assert conf["spark.kubernetes.container.image"] == K8S["SPARK_EXECUTOR_IMAGE"]
    assert conf["spark.kubernetes.authenticate.driver.serviceAccountName"] == "spark"
    assert conf["spark.driver.host"] == "10.1.2.3"
    assert conf["spark.executor.instances"] == "1"
    # cores.max is a standalone-mode cap; instances x cores is the cap here
    assert "spark.cores.max" not in conf
    # executor pods inherit nothing: region by value, credentials by
    # reference to the platform Secret, never as values in the conf
    assert conf["spark.executorEnv.AWS_REGION"]
    assert conf["spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID"] == \
        "platform-secrets:AWS_ACCESS_KEY_ID"
    assert not [v for v in conf.values() if "minioadmin" in str(v)]


def test_the_mode_and_the_master_must_agree():
    from reporting_platform.common.spark import session_conf

    with _env(**CLEAR), _jars():
        with _env(SPARK_MASTER=K8S["SPARK_MASTER"]):
            _raises(lambda: session_conf("t"), "PLATFORM_EXECUTION", "k8s://")
        with _env(PLATFORM_EXECUTION="kubernetes"):
            _raises(lambda: session_conf("t"), "PLATFORM_EXECUTION")


def test_embedded_mode_runs_in_process_and_only_there():
    """`embedded` is the one mode a `local[N]` master is right in, and the
    one mode a cluster master is wrong in. Each refusal names the way out."""
    from reporting_platform.common.spark import session_conf

    master, conf = _conf(PLATFORM_EXECUTION="embedded", SPARK_MASTER="local[2]")
    assert master == "local[2]"
    assert not [k for k in conf if k.startswith("spark.kubernetes.")]
    assert conf["spark.sql.catalog.lakehouse.ref"] == "b1"
    with _env(**CLEAR), _jars():
        with _env(PLATFORM_EXECUTION="embedded"):
            # unset falls back to the compose cluster, which embedded refuses
            _raises(lambda: session_conf("t"), "embedded", "local[")
            with _env(SPARK_MASTER=K8S["SPARK_MASTER"]):
                _raises(lambda: session_conf("t"), "embedded")
        # and the other modes still refuse an in-process session
        with _env(SPARK_MASTER="local[*]"):
            _raises(lambda: session_conf("t"), "local[*]",
                    "PLATFORM_EXECUTION=embedded")


def test_kubernetes_mode_refuses_without_the_pod_ip():
    from reporting_platform.common.spark import session_conf

    with _env(**CLEAR), _env(**K8S), _env(POD_IP=None), _jars():
        _raises(lambda: session_conf("t"), "POD_IP")


def test_bearer_auth_reaches_the_catalog():
    _, conf = _conf(NESSIE_AUTH_TYPE="BEARER",
                    **{settings.NESSIE_TOKEN_VAR: "t0k"})
    assert conf["spark.sql.catalog.lakehouse.authentication.type"] == "BEARER"
    assert conf["spark.sql.catalog.lakehouse.authentication.token"] == "t0k"


# ------------------------------------------------------------ the driver pod
def test_the_driver_pod_runs_the_same_module_with_the_same_arguments():
    from reporting_platform.common.spark_task import driver_pod

    with _env(**CLEAR), _env(**K8S):
        pod = driver_pod(("ingest", "fo_trade", "ready/x.json", "r1", ""), "p1")
    spec = pod["spec"]
    container = spec["containers"][0]
    assert container["command"] == ["python", "-m", "reporting_platform.common.spark_task",
                                    "ingest", "fo_trade", "ready/x.json", "r1", ""]
    assert container["image"] == K8S["SPARK_DRIVER_IMAGE"]
    assert spec["restartPolicy"] == "Never"
    assert spec["serviceAccountName"] == "spark"
    sources = [next(iter(e.values()))["name"] for e in container["envFrom"]]
    assert sources == ["platform-env", "platform-secrets"]
    env = {e["name"]: e for e in container["env"]}
    assert env["PLATFORM_EXECUTION"]["value"] == "kubernetes"
    assert env["POD_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP"


def test_run_goes_to_a_pod_in_kubernetes_mode_and_parses_its_log():
    import reporting_platform.common.spark_task as task

    calls = []

    def fake_pod(args):
        calls.append(args)
        return 0, 'spark says hello\n{"pending": ["ready/a.json"]}\n', ""

    real, task._run_in_pod = task._run_in_pod, fake_pod
    try:
        with _env(**CLEAR), _env(**K8S):
            assert task.run("pending", "fo_trade") == {"pending": ["ready/a.json"]}
    finally:
        task._run_in_pod = real
    assert calls == [("pending", "fo_trade")]


def test_a_failed_driver_reports_the_head_of_its_traceback():
    from reporting_platform.common.spark_task import parse_result

    log = ("x" * 5000 + "\nTraceback (most recent call last)\n  ...\n"
           "ValueError: the actual message\n" + "java frame\n" * 400)
    try:
        parse_result(("ingest",), 1, log, "")
    except RuntimeError as exc:
        assert "ValueError: the actual message" in str(exc), str(exc)[:500]
        assert "exit 1" in str(exc)
    else:
        raise AssertionError("a failed driver parsed as success")
