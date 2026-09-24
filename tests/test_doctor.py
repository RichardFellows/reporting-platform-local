"""`scripts/doctor.py`'s checks, each a pure function of injected inputs.

WHY NO REAL DOCKER HERE. `doctor.py` runs on the host before any container --
or even `.env` -- exists, so it has to work with nothing installed but the
stdlib. Testing it for real would mean actually breaking Docker, occupying a
port and running under Git Bash; instead every check takes its inputs as
arguments (an env dict, a platform name, a uid, `docker-compose.yml`'s text, a
set of busy ports, a memory figure), so this module drives all five failure
paths from Plan #19's "Done when" by construction, and `main()`/`run_checks()`
-- the only things that touch the real environment -- are left untested here.

No stack. See docs/DECISIONS.md#containers-run-as-the-host-uid,
docs/DECISIONS.md#spark-worker-sizing
"""
from __future__ import annotations

import atexit
import pathlib
import re
import shutil
import tempfile

from tests.support import repo_file
from scripts import doctor

_MADE: list[pathlib.Path] = []


def _tmp_dir() -> pathlib.Path:
    """A throwaway directory. `tests/run.py` calls only `test_*` functions
    (see its own docstring), not a `teardown_module`, so cleanup is `atexit`
    -- the same pattern `tests/support.py` uses for its own temp dirs.

    Deliberately NOT `support.config_dir()` -- these tests are about
    `scripts/doctor.py`, which knows nothing of `REPORTING_CONFIG_DIR` or the
    feed registry, so a plain empty directory is the honest fixture.
    """
    d = pathlib.Path(tempfile.mkdtemp(prefix="doctor-test-"))
    _MADE.append(d)
    return d


@atexit.register
def _cleanup() -> None:
    for d in _MADE:
        shutil.rmtree(d, ignore_errors=True)
    _MADE.clear()


# ------------------------------------------------------------- .env ----


def test_env_file_present_is_ok():
    d = _tmp_dir()
    (d / ".env").write_text("", encoding="utf-8")
    ok, msg = doctor.check_env_file(d)
    assert ok
    assert ".env exists" in msg


def test_env_file_missing_names_the_fix():
    ok, msg = doctor.check_env_file(_tmp_dir())
    assert not ok
    assert "cp .env.example .env" in msg
    assert "make env" in msg


# ------------------------------------------------------- AIRFLOW_UID ----


def test_airflow_uid_matches_is_ok():
    ok, msg = doctor.check_airflow_uid({"AIRFLOW_UID": "1000"}, "Linux", 1000)
    assert ok
    assert "1000" in msg


def test_airflow_uid_mismatch_names_the_fix():
    ok, msg = doctor.check_airflow_uid({"AIRFLOW_UID": "12345"}, "Linux", 1000)
    assert not ok
    assert "does not match your uid" in msg
    assert "AIRFLOW_UID=1000 in .env" in msg


def test_airflow_uid_unset_on_linux_names_the_fix():
    ok, msg = doctor.check_airflow_uid({}, "Linux", 1000)
    assert not ok
    assert "AIRFLOW_UID=1000 in .env" in msg


def test_airflow_uid_not_a_number_on_linux_names_the_fix():
    ok, msg = doctor.check_airflow_uid({"AIRFLOW_UID": "banana"}, "Linux", 1000)
    assert not ok
    assert "not a number" in msg


def test_airflow_uid_unset_on_macos_is_ok():
    ok, msg = doctor.check_airflow_uid({}, "Darwin", None)
    assert ok
    assert "unset" in msg


def test_airflow_uid_set_on_macos_names_the_fix():
    ok, msg = doctor.check_airflow_uid({"AIRFLOW_UID": "1000"}, "Darwin", None)
    assert not ok
    assert "remove AIRFLOW_UID from .env" in msg


def test_airflow_uid_set_on_windows_names_the_fix():
    ok, msg = doctor.check_airflow_uid({"AIRFLOW_UID": "1000"}, "Windows", None)
    assert not ok
    assert "remove AIRFLOW_UID from .env" in msg


# ------------------------------------------------------------ memory ----


def test_docker_memory_enough_is_ok():
    ok, msg = doctor.check_docker_memory(9 * 1024 ** 3, 8)
    assert ok
    assert "9.0 GiB" in msg


def test_docker_memory_too_low_names_the_fix():
    ok, msg = doctor.check_docker_memory(4 * 1024 ** 3, 8)
    assert not ok
    assert "4.0 GiB" in msg
    assert "raise Docker Desktop's memory limit" in msg


def test_docker_not_running_is_its_own_failure():
    ok, msg = doctor.check_docker_memory(None, 8)
    assert not ok
    assert "does not appear to be running" in msg
    assert "start Docker Desktop" in msg


def test_min_docker_memory_is_the_documented_sum():
    # ONE NAMED CONSTANT, and this pins what it is made of: spark-worker's
    # own literal plus the documented allowance for everything else.
    assert doctor.MIN_DOCKER_MEMORY_GIB == (
        doctor.SPARK_WORKER_MEMORY_GIB + doctor.OTHER_SERVICES_ALLOWANCE_GIB)


def test_spark_worker_memory_gib_matches_docker_compose():
    # THE NUMBER IS A LITERAL IN TWO PLACES NOW: docker-compose.yml's own
    # `SPARK_WORKER_MEMORY: "6g"` and doctor.py's copy of it. Nothing makes
    # doctor.py read the file at runtime (see its own comment on why), so
    # this is the guard against the two drifting apart silently.
    text = repo_file("docker-compose.yml").read_text(encoding="utf-8")
    match = re.search(r'SPARK_WORKER_MEMORY:\s*"(\d+)g"', text)
    assert match, "SPARK_WORKER_MEMORY not found in docker-compose.yml"
    assert doctor.SPARK_WORKER_MEMORY_GIB == int(match.group(1))


# -------------------------------------------------------------- ports ----


def test_parse_host_ports_reads_the_real_compose_file():
    text = repo_file("docker-compose.yml").read_text(encoding="utf-8")
    ports = doctor.parse_host_ports(text)
    # Spot-check a handful rather than the whole set, so this does not
    # become a second hand-written list to keep in sync with compose.
    assert ports["NESSIE_HOST_PORT"] == 19120
    assert ports["POSTGRES_HOST_PORT"] == 5432
    assert ports["FEED_UI_HOST_PORT"] == 8082


def test_resolve_ports_honours_env_override():
    text = "ports: ['${FOO_HOST_PORT:-1234}:1234']"
    assert doctor.resolve_ports(text, {}) == {"FOO_HOST_PORT": 1234}
    assert doctor.resolve_ports(text, {"FOO_HOST_PORT": "9999"}) == {
        "FOO_HOST_PORT": 9999}


def test_resolve_ports_ignores_a_non_numeric_override():
    text = "ports: ['${FOO_HOST_PORT:-1234}:1234']"
    assert doctor.resolve_ports(text, {"FOO_HOST_PORT": "not-a-port"}) == {
        "FOO_HOST_PORT": 1234}


def test_port_free_is_ok():
    ok, msg = doctor.check_port("FOO_HOST_PORT", 1234, busy=set(), stack_owned=set())
    assert ok
    assert "is free" in msg


def test_port_busy_by_someone_else_names_the_fix():
    ok, msg = doctor.check_port("FOO_HOST_PORT", 1234, busy={1234}, stack_owned=set())
    assert not ok
    assert "already in use by something else" in msg
    assert "FOO_HOST_PORT=<a free port> in .env" in msg


def test_port_busy_by_this_stack_is_ok():
    ok, msg = doctor.check_port("FOO_HOST_PORT", 1234, busy={1234}, stack_owned={1234})
    assert ok
    assert "this stack's own containers" in msg


def test_parse_stack_owned_ports_reads_compose_ps_output():
    sample = (
        "NAME                          PORTS\n"
        "reporting-platform-minio-1    0.0.0.0:19000->9000/tcp, [::]:19000->9000/tcp\n"
        "reporting-platform-postgres-1 0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp\n"
        "reporting-platform-airflow-1  8080/tcp\n"
    )
    assert doctor.parse_stack_owned_ports(sample) == {19000, 5432}


def test_parse_stack_owned_ports_on_empty_output():
    # `docker compose ps` failed, or nothing is running -- an empty keep-set,
    # not a crash, so every busy port is correctly reported as a conflict.
    assert doctor.parse_stack_owned_ports("") == set()


# ---------------------------------------------------------------- MSYS ----


def test_msys_not_set_is_not_applicable():
    ok, msg = doctor.check_msys({})
    assert ok
    assert "not applicable" in msg


def test_msys_set_without_no_pathconv_names_the_fix():
    ok, msg = doctor.check_msys({"MSYSTEM": "MINGW64"})
    assert not ok
    assert "export MSYS_NO_PATHCONV=1" in msg


def test_msys_set_with_no_pathconv_is_ok():
    ok, msg = doctor.check_msys({"MSYSTEM": "MINGW64", "MSYS_NO_PATHCONV": "1"})
    assert ok
    assert "MSYS_NO_PATHCONV=1 is set" in msg


# --------------------------------------------------------- env parsing ----


def test_parse_env_file_skips_comments_and_blanks():
    d = _tmp_dir()
    (d / ".env").write_text("# a comment\n\nFOO=bar\nBAZ= qux \n", encoding="utf-8")
    assert doctor.parse_env_file(d / ".env") == {"FOO": "bar", "BAZ": "qux"}


def test_parse_env_file_missing_is_empty():
    assert doctor.parse_env_file(pathlib.Path("/no/such/file/.env")) == {}


def test_effective_env_environment_overrides_dot_env():
    # COMPOSE'S OWN PRECEDENCE: a shell-exported var wins over the file.
    file_env = {"AIRFLOW_UID": "1000"}
    merged = doctor.effective_env(file_env, {"AIRFLOW_UID": "12345"})
    assert merged["AIRFLOW_UID"] == "12345"


def test_effective_env_falls_back_to_dot_env():
    file_env = {"AIRFLOW_UID": "1000"}
    merged = doctor.effective_env(file_env, {})
    assert merged["AIRFLOW_UID"] == "1000"
