"""Build one wheel per component in `components.yml`, and prove each stands alone.

    python -m scripts.build_components                    # every packaged component
    python -m scripts.build_components core ingest        # just these
    python -m scripts.build_components --version 1.4.0    # stamp a version
    python -m scripts.build_components --check            # + install-and-import each

WHY A STAGED BUILD AND NOT A pyproject.toml PER DIRECTORY. Ownership is by
module, not by directory (docs/DECISIONS.md#components-are-declared-and-enforced):
`registry.deliveries` ships with ingest while `registry/db.py` ships with
core. So a component's files are copied into a staging directory, exactly
the ones `components.yml` assigns it, and built from there. The pyproject
this writes is generated from components.yml, so the module list, the
dependencies and the entry points have one source.

The config YAML under `reporting_platform/config/` is NOT in any wheel. It is
deployment content, read from REPORTING_CONFIG_DIR, and releases on its own
cadence -- a feed onboarded is not a new core version.

`--check` is the proof the boundaries are real: for each component, a fresh
virtualenv OUTSIDE the repo (so nothing resolves from the checkout), the
wheel and its sibling wheels installed BY FILE (never by name, which an
index could answer with somebody else's package), third-party `requires`
from the index -- and NOT the extras -- then every module the component owns
imported. A module failing only on a `host` package (Airflow itself, for DAG
support and the lineage extractor) is reported as skipped, not failed.
Needs `uv` and an index to install from; the corporate mirror is set the
usual way (UV_INDEX_URL).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
SOURCE_ROOTS = ("reporting_platform", "reporting_transport")
PYTHON = "3.11"          # the image's interpreter


def components() -> dict:
    return yaml.safe_load((REPO / "components.yml").read_text())["components"]


def _owner_table(comps: dict) -> list[tuple[str, str]]:
    table = [(m, n) for n, c in comps.items() for m in c.get("modules", [])]
    return sorted(table, key=lambda t: -len(t[0]))


def _owner(module: str, table) -> str | None:
    for prefix, name in table:
        if module == prefix or module.startswith(prefix + "."):
            return name
    return None


def owned_files(name: str, comps: dict) -> dict[str, pathlib.Path]:
    """`module -> source path` for every .py file `name` owns."""
    table = _owner_table(comps)
    out = {}
    for root in SOURCE_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            parts = list(path.relative_to(REPO).with_suffix("").parts)
            if parts[-1] == "__init__":
                parts.pop()
            module = ".".join(parts)
            if _owner(module, table) == name:
                out[module] = path
    return out


def pyproject(name: str, comps: dict, version: str) -> str:
    c = comps[name]
    deps = [comps[d]["distribution"] for d in c.get("depends_on", [])]
    deps += c.get("requires", [])
    lines = [
        "[build-system]",
        'requires = ["hatchling>=1.21"]',
        'build-backend = "hatchling.build"',
        "",
        "[project]",
        f'name = "{c["distribution"]}"',
        f'version = "{version}"',
        f"description = {json.dumps(c.get('description', ''))}",
        f'requires-python = ">={PYTHON}"',
        f"dependencies = {json.dumps(deps)}",
    ]
    if c.get("extras"):
        lines += ["", "[project.optional-dependencies]"]
        lines += [f"{k} = {json.dumps(v)}" for k, v in c["extras"].items()]
    if c.get("scripts"):
        lines += ["", "[project.scripts]"]
        lines += [f'{k} = "{v}"' for k, v in c["scripts"].items()]
    tops = sorted({p.split(".")[0] for p in c["modules"]})
    lines += ["", "[tool.hatch.build.targets.wheel]",
              f"only-include = {json.dumps(tops)}", ""]
    return "\n".join(lines)


def build(name: str, comps: dict, version: str, out: pathlib.Path) -> pathlib.Path:
    files = owned_files(name, comps)
    with tempfile.TemporaryDirectory(prefix=f"rp-build-{name}-") as tmp:
        stage = pathlib.Path(tmp)
        for path in files.values():
            dest = stage / path.relative_to(REPO)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
        (stage / "pyproject.toml").write_text(pyproject(name, comps, version))
        before = set(out.glob("*.whl"))
        subprocess.run(["uv", "build", "--wheel", "--out-dir", str(out), str(stage)],
                       check=True)
    made = sorted(set(out.glob("*.whl")) - before) or sorted(
        out.glob(f"{comps[name]['distribution'].replace('-', '_')}-{version}-*.whl"))
    print(f"built {name}: {len(files)} modules -> {made[-1].name}")
    return made[-1]


# What runs inside the throwaway venv: import each module, classify failures.
_PROBE = r"""
import importlib, json, sys
modules, host = json.loads(sys.argv[1]), set(json.loads(sys.argv[2]))
ok, skipped, failed = [], [], []
for m in modules:
    try:
        importlib.import_module(m)
        ok.append(m)
    except ModuleNotFoundError as e:
        top = (e.name or "").split(".")[0]
        (skipped if top in host else failed).append([m, f"{type(e).__name__}: {e}"])
    except Exception as e:
        failed.append([m, f"{type(e).__name__}: {e}"])
print(json.dumps({"ok": ok, "skipped": skipped, "failed": failed}))
"""

# The import name each `host` distribution provides, for the probe above.
_HOST_IMPORTS = {
    "apache-airflow": ["airflow", "pendulum", "attr"],
    "astronomer-cosmos": ["cosmos"],
    "apache-airflow-providers-openlineage": ["openlineage"],
}


def _closure(name: str, comps: dict) -> list[str]:
    out, stack = [], [name]
    while stack:
        n = stack.pop()
        if n not in out:
            out.append(n)
            stack.extend(comps[n].get("depends_on", []))
    return out


def check(name: str, comps: dict, wheels: dict[str, pathlib.Path]) -> bool:
    c = comps[name]
    modules = sorted(owned_files(name, comps))
    host = sorted({i for h in c.get("host", []) for i in _HOST_IMPORTS.get(h, [h])})
    with tempfile.TemporaryDirectory(prefix=f"rp-check-{name}-") as tmp:
        venv = pathlib.Path(tmp) / "venv"
        subprocess.run(["uv", "venv", "--quiet", "--python", PYTHON, str(venv)],
                       check=True)
        py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        # The sibling WHEEL FILES, never their names: a name is resolved
        # against the index too, and `reporting-core` on a public index is
        # somebody else's package (dependency confusion). Third-party
        # `requires` come from the index; extras are deliberately not asked for.
        local = [str(wheels[n]) for n in _closure(name, comps)]
        subprocess.run(["uv", "pip", "install", "--quiet", "--python", str(py), *local],
                       check=True)
        # cwd is the temp dir, so the checkout is NOT on sys.path.
        proc = subprocess.run([str(py), "-c", _PROBE, json.dumps(modules), json.dumps(host)],
                              cwd=tmp, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"FAIL  {name}: probe crashed\n{proc.stderr[-2000:]}")
        return False
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    for m, why in result["skipped"]:
        print(f"skip  {name}: {m} needs the host Airflow ({why})")
    for m, why in result["failed"]:
        print(f"FAIL  {name}: {m}: {why}")
    status = "ok   " if not result["failed"] else "FAIL "
    print(f"{status} {name}: {len(result['ok'])} imported, "
          f"{len(result['skipped'])} need the host, {len(result['failed'])} failed")
    return not result["failed"]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="scripts.build_components")
    p.add_argument("names", nargs="*", help="components to build (default: all packaged)")
    p.add_argument("--version", default="0.0.0.dev0")
    p.add_argument("--out", default=str(REPO / "dist"))
    p.add_argument("--check", action="store_true",
                   help="install each wheel in a clean venv and import every module")
    a = p.parse_args(argv)

    comps = components()
    packaged = [n for n, c in comps.items()
                if c.get("packaged", True) and c.get("modules")]
    names = a.names or packaged
    unknown = [n for n in names if n not in packaged]
    if unknown:
        p.error(f"not a packaged component with modules: {unknown} "
                f"(packaged: {packaged})")

    out = pathlib.Path(a.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    # A check installs the sibling wheels, so build every dependency too.
    wanted, stack = [], list(names)
    while stack:
        n = stack.pop()
        if n not in wanted:
            wanted.append(n)
            stack.extend(d for d in comps[n].get("depends_on", []) if d in packaged)
    wheels = {n: build(n, comps, a.version, out) for n in packaged if n in wanted}
    if not a.check:
        return 0
    results = [check(n, comps, wheels) for n in names]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
