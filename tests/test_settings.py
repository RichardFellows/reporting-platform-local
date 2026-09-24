"""settings.py owns five variables. Nothing else may read them directly.

WHY AN AST WALK AND NOT A GREP. `os.environ.get("S3_ENDPOINT", "http://minio:9000")`
sitting anywhere outside `common/settings.py` is exactly the bug this whole
accessor layer was written to remove: it bypasses `REPORTING_ENV`'s refusal
and reintroduces a local host name that a cluster process will resolve to
nothing. A grep over the five names would also flag the DEFINITIONS inside
`settings.py` and the WRITE in `scripts/verify_parsing_contract.py` (which
seeds the env for a subprocess it is about to launch, and is fine) -- so the
check has to know the difference between a read and a write, which means
parsing, not matching text. See docs/DECISIONS.md#settings-refuse-outside-local.

`reporting_transport/` IS NOT WALKED, deliberately, not by an exclusion list
but by never being one of the scanned roots: it is independent of
`reporting_platform` by design (CLAUDE.md), ships no local defaults of its
own, and already refuses a missing value on its own terms. Folding it into
this scanner would be asserting a coupling that does not exist.

No stack: this is pure `ast` over files already on disk.
"""
from __future__ import annotations

import ast
import contextlib
import os

from reporting_platform.common import settings
from tests.support import DAGS, REPO

_SETTINGS_PY = (REPO / "reporting_platform" / "common" / "settings.py").resolve()


# --------------------------------------------------------------- the scanner
def _is_os_environ(node: ast.AST) -> bool:
    """`os.environ`, or a bare `environ` from `from os import environ`."""
    if isinstance(node, ast.Attribute) and node.attr == "environ":
        return isinstance(node.value, ast.Name) and node.value.id == "os"
    return isinstance(node, ast.Name) and node.id == "environ"


def _is_os(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == "os"


def _const_str(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Index):                      # py<3.9 subscripts
        node = node.value
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def hits(source: str, filename: str = "<source>") -> list[str]:
    """`"filename:line: what"` for every READ of a name in `settings.REQUIRED`
    via `os.environ["X"]`, `os.environ.get("X"...)`, `os.getenv("X"...)` or
    `environ.get("X"...)`. A WRITE (`os.environ["X"] = ...`, Store context)
    is not flagged -- setting the environment for a subprocess is not reading
    it back through the mechanism this module exists to replace.

    Takes SOURCE TEXT, not a path, so the same function scans a real file and
    a one-line probe string -- see the tests proving this can fail, below.
    """
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        return []

    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load) \
                and _is_os_environ(node.value):
            key = _const_str(node.slice)
            if key in settings.REQUIRED:
                found.append(f"{filename}:{node.lineno}: os.environ[{key!r}]")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            is_environ_get = attr == "get" and _is_os_environ(node.func.value)
            is_getenv = attr == "getenv" and _is_os(node.func.value)
            if (is_environ_get or is_getenv) and node.args:
                key = _const_str(node.args[0])
                if key in settings.REQUIRED:
                    found.append(f"{filename}:{node.lineno}: "
                                 f"{'environ.get' if is_environ_get else 'os.getenv'}"
                                 f"({key!r}, ...)")
    return found


def _py_files():
    """Every `.py` under `reporting_platform/`, `scripts/` and the Airflow
    DAGs, except `settings.py` itself.

    `DAGS` (from `tests.support`) rather than `REPO / "airflow"`: the repo's
    `airflow/` directory holds nothing but `dags/` in Python, and
    `docker-compose.yml` mounts only that subdirectory into the container at
    a different path (`/opt/airflow/dags`) -- the same resolution
    `test_versions`-adjacent tests already need, done once in `support.py`.
    """
    roots = [REPO / "reporting_platform", REPO / "scripts", DAGS]
    for root in roots:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.py")):
            if path.resolve() == _SETTINGS_PY:
                continue
            yield path


def _scan_repo() -> list[str]:
    found: list[str] = []
    for path in _py_files():
        # In the container the DAGs are mounted OUTSIDE the repo root
        # (/opt/airflow/dags), so a repo-relative label cannot exist for them.
        label = path.relative_to(REPO) if path.is_relative_to(REPO) else path
        found.extend(hits(path.read_text(encoding="utf-8"), str(label)))
    return found


def test_no_direct_read_of_a_setting_outside_settings_py():
    found = _scan_repo()
    assert not found, (
        "direct read of a variable settings.py now owns, found outside it -- "
        "use the accessor instead:\n" + "\n".join(found))


# ------------------------------------------------------- the scanner, proven
def test_the_scanner_flags_a_read_and_ignores_a_write():
    """Test 1 above is only worth anything if this scanner can actually fail.
    A read of a REQUIRED name is one hit; assigning to it (seeding a
    subprocess's environment, `scripts/verify_parsing_contract.py`'s own
    move) is none."""
    read = hits('os.environ.get("S3_ENDPOINT", "x")\n')
    assert len(read) == 1, read
    write = hits('os.environ["S3_ENDPOINT"] = "x"\n')
    assert write == [], write


# ------------------------------------------------------------ the accessors
@contextlib.contextmanager
def _env(**values):
    """Set/unset `os.environ` entries for one test, restored after -- always,
    including on failure. `None` unsets rather than setting the string
    "None"."""
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


ENDPOINT_ACCESSORS = {
    "S3_ENDPOINT": settings.s3_endpoint,
    "NESSIE_URI": settings.nessie_uri,
    "REPORTING_WAREHOUSE": settings.warehouse,
    "REPORTING_LANDING": settings.landing,
}


def test_local_with_the_var_unset_returns_the_compose_default():
    for name, accessor in ENDPOINT_ACCESSORS.items():
        with _env(REPORTING_ENV="local", **{name: None}):
            assert accessor() == settings.LOCAL_DEFAULTS[name], name


def test_dev_with_the_var_unset_raises_naming_it():
    for name, accessor in ENDPOINT_ACCESSORS.items():
        with _env(REPORTING_ENV="dev", **{name: None}):
            try:
                accessor()
            except settings.MissingSetting as exc:
                assert name in str(exc), (name, str(exc))
            else:
                raise AssertionError(f"{name} did not raise outside local")


def test_an_empty_string_counts_as_unset_in_both_envs():
    for name, accessor in ENDPOINT_ACCESSORS.items():
        with _env(REPORTING_ENV="local", **{name: ""}):
            assert accessor() == settings.LOCAL_DEFAULTS[name], name
        with _env(REPORTING_ENV="dev", **{name: ""}):
            try:
                accessor()
            except settings.MissingSetting:
                pass
            else:
                raise AssertionError(f"{name}='' was accepted outside local")


def test_a_set_value_wins_in_every_environment():
    for name, accessor in ENDPOINT_ACCESSORS.items():
        for current in ("local", "dev", "uat", "prod"):
            with _env(REPORTING_ENV=current, **{name: "http://set/value"}):
                assert accessor() == "http://set/value", (name, current)


def test_registry_dsn_refuses_even_in_local():
    """No default anywhere -- `local` is not an exception for this one."""
    with _env(REPORTING_ENV="local", REGISTRY_DSN=None):
        try:
            settings.registry_dsn()
        except settings.MissingSetting as exc:
            assert "REGISTRY_DSN" in str(exc)
        else:
            raise AssertionError("registry_dsn() did not refuse in local")
    with _env(REPORTING_ENV="local", REGISTRY_DSN="postgresql://x"):
        assert settings.registry_dsn() == "postgresql://x"


def test_missing_is_empty_in_local_and_lists_the_unset_required_names():
    with _env(REPORTING_ENV="local", S3_ENDPOINT=None, NESSIE_URI=None,
              REPORTING_WAREHOUSE=None, REPORTING_LANDING=None,
              REGISTRY_DSN=None):
        assert settings.missing() == []

    with _env(REPORTING_ENV="dev", S3_ENDPOINT=None, NESSIE_URI="http://x",
              REPORTING_WAREHOUSE=None, REPORTING_LANDING="s3a://b/landing",
              REGISTRY_DSN=None):
        assert sorted(settings.missing()) == sorted(
            ["S3_ENDPOINT", "REPORTING_WAREHOUSE", "REGISTRY_DSN"])


def test_config_check_refuses_an_environment_missing_a_setting():
    # The plan's "Done when" for #5, as a test: `config check` outside local
    # with no S3 variables exits 1 and names S3_ENDPOINT.
    import io
    import sys

    from reporting_platform.config.__main__ import main

    unset = {n: None for n in settings.REQUIRED}
    err = io.StringIO()
    saved, sys.stderr = sys.stderr, err
    try:
        with _env(REPORTING_ENV="dev", **unset):
            code = main(["check"])
    finally:
        sys.stderr = saved
    assert code == 1, code
    assert "S3_ENDPOINT" in err.getvalue(), err.getvalue()
