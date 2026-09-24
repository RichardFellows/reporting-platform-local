"""`make doctor` -- catch a new user's environment before the stack does.

    python3 scripts/doctor.py            # human-readable, exits 1 on any ✗
    python3 scripts/doctor.py --json

A new user's first failures here are almost never in the platform's own code:
they are ".env is missing", "AIRFLOW_UID does not match your uid", "Docker
Desktop has 4 GiB", "something else is already on 5432". None of those produce
a message that names the problem -- the console returns a bare HTTP 500
(docs/DECISIONS.md#containers-run-as-the-host-uid), a Spark job just never
gets scheduled, `docker compose up` says "port is already allocated" with no
hint that it might be this stack's OWN, already-running containers.

STDLIB ONLY, DELIBERATELY. This runs on the HOST before any container (or even
`.env`) exists, so it cannot assume the cheap CI tier's dependencies
(pyyaml/ruamel/requests/duckdb/jinja2), let alone anything installed inside an
image it hasn't built yet.

Every check is a pure function of INJECTED inputs (an env dict, a platform
name, a uid, `docker-compose.yml`'s text, a set of busy ports, a memory
figure) precisely so `tests/test_doctor.py` can drive all five failure paths
without Docker, without root, and without touching this developer's real
`.env`. `main()` is the only thing that gathers real values.
"""
from __future__ import annotations

import argparse
import json
import os
import platform as platform_module
import re
import socket
import subprocess
import sys
from pathlib import Path

DEFAULT_REPO = Path(__file__).resolve().parent.parent

# --------------------------------------------------------------- .env ----


def parse_env_file(path: Path) -> dict[str, str]:
    """A dumb `KEY=value` reader -- good enough for what `.env` actually is.

    No quoting, no `${...}` expansion: compose's own `.env` handling doesn't
    do much more than this for the keys doctor cares about (uids and bare
    port numbers), and pulling in a real dotenv parser would be the first
    non-stdlib dependency this script has.
    """
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def effective_env(file_env: dict[str, str], os_environ: dict[str, str]) -> dict[str, str]:
    """`.env` merged under the real environment.

    ENVIRONMENT OVERRIDES `.env`, because that is what compose itself does --
    a shell-exported var wins over the file for variable substitution. Doctor
    matches that precedence so `AIRFLOW_UID=12345 python3 scripts/doctor.py`
    checks the same value compose would actually use.
    """
    merged = dict(file_env)
    merged.update(os_environ)
    return merged


def check_env_file(repo: Path) -> tuple[bool, str]:
    if (repo / ".env").is_file():
        return True, ".env exists"
    return False, ".env is missing. Fix: cp .env.example .env (or `make env`)"


# --------------------------------------------------------- AIRFLOW_UID ----


def check_airflow_uid(env: dict[str, str], system: str, uid: int | None) -> tuple[bool, str]:
    """Linux: must equal the host uid. macOS/Windows: must be UNSET.

    `docker-compose.yml` runs every Airflow container as
    `${AIRFLOW_UID:-50000}:0`, and half the bind mounts here are read-write --
    the feed console writes `feeds.yml`, the inbox watcher creates
    `.processed/`. A bind mount keeps the HOST's ownership, which no `chown`
    in the image can reach, so a mismatch on Linux fails SILENTLY: a bare
    HTTP 500 with the traceback only in the container logs, or a delivery
    landed correctly and then stuck because moving the local file failed.
    Docker Desktop's VM remaps ownership for macOS/Windows itself, so setting
    it there is WRONG, not merely redundant -- the default 50000 is already
    correct. See docs/DECISIONS.md#containers-run-as-the-host-uid and
    .env.example's own AIRFLOW_UID comment.
    """
    value = env.get("AIRFLOW_UID")
    if system == "Linux":
        if uid is None:
            return False, "could not determine this host's uid (os.getuid unavailable)"
        if value is None:
            return False, (
                "AIRFLOW_UID is not set; Linux containers default to 50000:0 and "
                f"cannot write your checkout. Fix: set AIRFLOW_UID={uid} in .env")
        try:
            declared = int(value)
        except ValueError:
            return False, (
                f"AIRFLOW_UID={value!r} is not a number. Fix: set AIRFLOW_UID={uid} "
                f"in .env")
        if declared != uid:
            return False, (
                f"AIRFLOW_UID={declared} does not match your uid ({uid}). Fix: set "
                f"AIRFLOW_UID={uid} in .env")
        return True, f"AIRFLOW_UID={declared} matches your uid"
    # macOS / Windows: Docker Desktop maps ownership itself.
    if value is not None:
        return False, (
            f"AIRFLOW_UID={value} is set, but this is {system}, where Docker "
            f"Desktop maps container ownership itself and the default (50000) is "
            f"already correct. Fix: remove AIRFLOW_UID from .env (leave it unset)")
    return True, f"AIRFLOW_UID unset, correct for {system}"


# ------------------------------------------------------------- memory ----

# SPARK_WORKER_MEMORY is a LITERAL in docker-compose.yml's spark-worker
# service ("6g" -- not an `${...}` override, see
# docs/DECISIONS.md#spark-worker-sizing: it is sized so three 2-core/2g-capped
# applications can hold cores at once), so it is a literal here too rather
# than something parsed out at runtime.
SPARK_WORKER_MEMORY_GIB = 6

# No other service in docker-compose.yml declares a mem_limit (marquez's
# 900m + 256m sit behind --profile lineage and are OFF by default, so they
# are not counted). This is a documented GUESS at the footprint of postgres,
# nessie (a JVM), minio, four airflow processes, feed-ui, notebook, inbox and
# watchdog together -- not a measurement. If it doesn't hold up in practice,
# treat it as exactly that: a guess, not a derived fact.
OTHER_SERVICES_ALLOWANCE_GIB = 2

# This lines up with docs/QUICKSTART.md's and README.md's own "~8 GB
# available" claim -- reassuring, not circular: both were written
# independently of this arithmetic.
MIN_DOCKER_MEMORY_GIB = SPARK_WORKER_MEMORY_GIB + OTHER_SERVICES_ALLOWANCE_GIB


def check_docker_memory(mem_bytes: int | None, min_gib: float) -> tuple[bool, str]:
    """`mem_bytes` is `docker info`'s `MemTotal`, or `None` if docker is down.

    Docker not running is its OWN failure with its own fix, distinct from "it
    is running but too small" -- conflating them would tell someone to raise
    a memory limit when the actual fix is to start Docker Desktop at all.
    """
    if mem_bytes is None:
        return False, (
            "Docker does not appear to be running (`docker info` failed). "
            "Fix: start Docker Desktop (or the docker daemon)")
    gib = mem_bytes / (1024 ** 3)
    if gib < min_gib:
        return False, (
            f"Docker has {gib:.1f} GiB available; the stack needs at least "
            f"{min_gib:g} GiB ({SPARK_WORKER_MEMORY_GIB:g} GiB for spark-worker + "
            f"{OTHER_SERVICES_ALLOWANCE_GIB:g} GiB for everything else). "
            f"Fix: raise Docker Desktop's memory limit (Settings -> Resources -> "
            f"Memory)")
    return True, f"Docker has {gib:.1f} GiB available (>= {min_gib:g} GiB needed)"


# --------------------------------------------------------------- ports ----


def parse_host_ports(compose_text: str) -> dict[str, int]:
    """`{VAR: default_port}` for every `${VAR_HOST_PORT:-default}` in compose.

    Read off the file rather than hand-listed, so a newly published port is
    covered by construction -- the same reasoning as `managed_tables()` for
    dbt models: a list somebody has to remember to update goes stale.
    """
    pattern = re.compile(r"\$\{(\w*_HOST_PORT):-(\d+)\}")
    return {var: int(default) for var, default in pattern.findall(compose_text)}


def resolve_ports(compose_text: str, env: dict[str, str]) -> dict[str, int]:
    """Each `*_HOST_PORT`'s value after `.env`/environment overrides."""
    resolved: dict[str, int] = {}
    for var, default in parse_host_ports(compose_text).items():
        raw = env.get(var)
        if raw:
            try:
                resolved[var] = int(raw)
                continue
            except ValueError:
                pass
        resolved[var] = default
    return resolved


def parse_stack_owned_ports(compose_ps_text: str) -> set[int]:
    """Host ports `docker compose ps` shows published, for THIS stack.

    `docker-compose.yml` pins `name: reporting-platform` precisely so
    container/volume names don't depend on the clone directory -- which also
    means `docker compose ps`, run from any checkout of this same file, only
    ever lists this one project's containers. No name-prefix matching needed:
    anything it prints IS this stack.
    """
    return {int(p) for p in re.findall(r":(\d+)->\d+/tcp", compose_ps_text)}


def check_port(var: str, port: int, busy: set[int], stack_owned: set[int]) -> tuple[bool, str]:
    """A port held by THIS stack's own running containers is not a conflict."""
    if port not in busy:
        return True, f"{var} ({port}) is free"
    if port in stack_owned:
        return True, f"{var} ({port}) is in use by this stack's own containers -- ok"
    return False, (
        f"{var} ({port}) is already in use by something else. Fix: stop it, or "
        f"set {var}=<a free port> in .env")


def _port_is_busy(port: int) -> bool:
    """Best-effort: can we bind it? Docker's port publish holds the bind too."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", port))
        except OSError:
            return True
        return False


# ---------------------------------------------------------------- MSYS ----


def check_msys(env: dict[str, str]) -> tuple[bool, str]:
    """Git Bash rewrites `/opt/platform/...` path args unless told not to.

    See CLAUDE.md: "Windows: use PowerShell, or MSYS_NO_PATHCONV=1 with
    bash". `MSYSTEM` is set by Git Bash's own shell profile (to e.g.
    MINGW64), never by PowerShell or cmd.exe, so its presence in the
    environment IS the signal that this is Git Bash.
    """
    if "MSYSTEM" not in env:
        return True, "not running under Git Bash (MSYSTEM unset) -- not applicable"
    if not env.get("MSYS_NO_PATHCONV"):
        return False, (
            "Git Bash detected (MSYSTEM set) without MSYS_NO_PATHCONV; "
            "`docker compose exec` path arguments (e.g. /opt/platform/...) will be "
            "rewritten and the exec will fail. Fix: export MSYS_NO_PATHCONV=1")
    return True, "MSYS_NO_PATHCONV=1 is set"


# ---------------------------------------------------------------- main ----


def _read_uid() -> int | None:
    return os.getuid() if hasattr(os, "getuid") else None


def _docker_mem_bytes() -> int | None:
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.MemTotal}}"],
            capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    try:
        return int(proc.stdout.strip())
    except ValueError:
        return None


def _compose_ps_text(repo: Path) -> str:
    try:
        proc = subprocess.run(
            ["docker", "compose", "ps"], cwd=repo,
            capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return proc.stdout if proc.returncode == 0 else ""


def run_checks(repo: Path, min_memory_gib: float | None = None) -> list[tuple[str, bool, str]]:
    """Gather the real values and run every check against them.

    The only function here that is NOT a pure function of injected inputs --
    everything it calls is, and `tests/test_doctor.py` drives those directly.
    """
    file_env = parse_env_file(repo / ".env")
    env = effective_env(file_env, dict(os.environ))
    system = platform_module.system()
    uid = _read_uid()

    results: list[tuple[str, bool, str]] = [
        (".env", *check_env_file(repo)),
        ("AIRFLOW_UID", *check_airflow_uid(env, system, uid)),
        ("docker memory", *check_docker_memory(
            _docker_mem_bytes(),
            min_memory_gib if min_memory_gib is not None else MIN_DOCKER_MEMORY_GIB)),
    ]

    compose_path = repo / "docker-compose.yml"
    if compose_path.is_file():
        compose_text = compose_path.read_text(encoding="utf-8")
        ports = resolve_ports(compose_text, env)
        busy = {port for port in ports.values() if _port_is_busy(port)}
        stack_owned = parse_stack_owned_ports(_compose_ps_text(repo))
        for var, port in sorted(ports.items()):
            results.append((var, *check_port(var, port, busy, stack_owned)))
    else:
        results.append(("ports", False, f"docker-compose.yml not found at {compose_path}"))

    results.append(("MSYS_NO_PATHCONV", *check_msys(env)))
    return results


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", type=Path, default=DEFAULT_REPO,
                        help="repo root to check (default: this script's checkout)")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--min-memory-gib", type=float, default=None,
                        help="override the derived Docker memory threshold")
    args = parser.parse_args(argv)

    results = run_checks(args.repo.resolve(), args.min_memory_gib)

    if args.json:
        print(json.dumps(
            [{"check": name, "ok": ok, "message": msg} for name, ok, msg in results],
            indent=2))
    else:
        for name, ok, msg in results:
            mark = "✓" if ok else "✗"
            print(f"{mark} {name}: {msg}")

    return 1 if any(not ok for _, ok, _ in results) else 0


if __name__ == "__main__":
    sys.exit(main())
