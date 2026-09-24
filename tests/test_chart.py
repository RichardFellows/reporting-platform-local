"""The Helm chart's ConfigMap and Secret carry the variables
`reporting_platform/common/settings.py` actually requires, and the official
Airflow subchart's `extraEnvFrom` names the SAME two objects this chart's own
helpers do.

NO HELM NEEDED -- pure text over the templates, the same reasoning
`tests/test_versions.py` uses for the jar triple: what matters is that the
SAME name (or the same requirement) appears everywhere it must, and that is
checkable by reading the files, not by rendering them. `helm template`
actually rendering every combination is the Done-when transcript in the PR,
not a unit test.

Each check is PROBED here too: run against a deliberately doctored copy of
the text, it must fail, or the check itself is a gate that cannot fail
(docs/DECISIONS.md#a-gate-that-cannot-fail) -- worth it precisely because
these are regexes over text, not a real render, so nothing else proves they
can trip at all.

No stack. See docs/DECISIONS.md#the-chart-is-the-only-place-settings-are-written
"""
from __future__ import annotations

import re

from tests.support import repo_file

CHART = "deploy/helm/reporting-platform"


def _text(relative: str) -> str:
    return repo_file(f"{CHART}/{relative}").read_text()


# ------------------------------------------------------------------- ConfigMap
def _configmap_keys(text: str) -> set[str]:
    """Every literal `KEY:` at the `data:` block's own indent."""
    return set(re.findall(r"^  ([A-Z][A-Z0-9_]*):", text, re.M))


def _required_configmap_keys() -> set[str]:
    from reporting_platform.common import settings

    # REGISTRY_DSN is on settings.REQUIRED too, but it is a CREDENTIAL --
    # secret-platform.yaml carries it, not this ConfigMap.
    keys = (set(settings.REQUIRED) - {"REGISTRY_DSN"}) | set(settings.KUBERNETES_REQUIRED)
    keys |= {"PLATFORM_EXECUTION", "DBT_PROJECT_REF", "DBT_PROJECT_DIGEST",
             "DEPLOYMENT_CHANGE_REF", "DEPLOYMENT_PIPELINE_REF"}
    return keys


def _assert_configmap_complete(text: str) -> None:
    present = _configmap_keys(text)
    missing = _required_configmap_keys() - present
    if missing:
        raise AssertionError(
            f"configmap-platform-env.yaml is missing: {sorted(missing)}")


def test_configmap_names_every_required_key():
    _assert_configmap_complete(_text("templates/configmap-platform-env.yaml"))


def test_configmap_check_fails_on_doctored_copy():
    doctored = _text("templates/configmap-platform-env.yaml").replace(
        "SPARK_K8S_NAMESPACE", "SPARK_K8S_NAMESPACE_TYPO")
    try:
        _assert_configmap_complete(doctored)
    except AssertionError:
        return
    raise AssertionError("removing SPARK_K8S_NAMESPACE did not trip the check")


# ---------------------------------------------------------------------- Secret
def _assert_secret_complete(text: str) -> None:
    from reporting_platform.common import settings

    needed = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "REGISTRY_DSN",
              settings.NESSIE_TOKEN_VAR}
    present = set(re.findall(r"^  ([A-Z][A-Z0-9_]*):", text, re.M))
    missing = needed - present
    if missing:
        raise AssertionError(f"secret-platform.yaml is missing: {sorted(missing)}")


def test_secret_names_every_required_key():
    _assert_secret_complete(_text("templates/secret-platform.yaml"))


def test_secret_check_fails_on_doctored_copy():
    doctored = _text("templates/secret-platform.yaml").replace(
        "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY_TYPO")
    try:
        _assert_secret_complete(doctored)
    except AssertionError:
        return
    raise AssertionError("removing AWS_SECRET_ACCESS_KEY did not trip the check")


# --------------------------------------------------------- extraEnvFrom agrees
# The one thing tests/test_chart.py CANNOT check without `helm template`: that
# a per-environment override (values-dev/uat/prod.yaml) names whatever custom
# `secrets.existingSecret` IT sets. That is exercised by actually rendering
# each one -- see the PR's Done-when transcript and
# deploy/helm/reporting-platform/README.md, "Secrets".
def _extra_env_from_block(values_text: str) -> str:
    m = re.search(r"extraEnvFrom:\s*\|\n((?:^ {4,}\S.*\n?)+)", values_text, re.M)
    if not m:
        raise AssertionError("values.yaml has no `extraEnvFrom: |` block")
    return m.group(1)


def _assert_extra_env_from_agrees(values_text: str, helpers_text: str) -> None:
    block = _extra_env_from_block(values_text)
    if "{{ .Release.Name }}-platform-env" not in block:
        raise AssertionError(
            "airflow.extraEnvFrom does not name {{ .Release.Name }}-platform-env")
    if "{{ .Release.Name }}-platform-secrets" not in block:
        raise AssertionError(
            "airflow.extraEnvFrom does not name {{ .Release.Name }}-platform-secrets")
    # And the helpers must produce the SAME literals, not merely similar ones.
    if '{{ .Release.Name }}-platform-env' not in helpers_text:
        raise AssertionError(
            "reporting-platform.envConfigMap does not match airflow.extraEnvFrom")
    if 'printf "%s-platform-secrets" .Release.Name' not in helpers_text:
        raise AssertionError(
            "reporting-platform.envSecret's fallback does not match airflow.extraEnvFrom")


def test_extra_env_from_agrees_with_helpers():
    _assert_extra_env_from_agrees(_text("values.yaml"), _text("templates/_helpers.tpl"))


def test_extra_env_from_check_fails_on_doctored_copy():
    doctored = _text("values.yaml").replace(
        "{{ .Release.Name }}-platform-secrets", "{{ .Release.Name }}-something-else")
    try:
        _assert_extra_env_from_agrees(doctored, _text("templates/_helpers.tpl"))
    except AssertionError:
        return
    raise AssertionError("renaming the secret in extraEnvFrom did not trip the check")
