"""The component boundaries in `components.yml` hold, import by import.

Each component is built, versioned and deployed on its own, so an import
from one into another it does not declare is a ModuleNotFoundError waiting
in whichever environment installed the first without the second. Nothing
fails locally, where every module is on one PYTHONPATH -- which is why this
reads the source rather than trying the imports.

EVERY IMPORT COUNTS, including one inside a function. Most cross-package
imports in this codebase are lazy on purpose (to keep pyspark and boto3 off
the import path of things that only read config), and a lazy import of a
package that is not installed is still a failure: later, in a task, on the
first run that reaches that branch. A string passed to `importlib` is not
seen; `spark_task.OPS` is the one such table and has its own test below.

No stack. See docs/PACKAGING.md.
"""
from __future__ import annotations

import ast
import pathlib

import yaml

from tests.support import repo_file

SOURCE_ROOTS = ("reporting_platform", "reporting_transport")
FIRST_PARTY = ("reporting_platform", "reporting_transport", "scripts")


def _components() -> dict:
    return yaml.safe_load(repo_file("components.yml").read_text())["components"]


def _owner_table(components: dict) -> list[tuple[str, str]]:
    """`(module prefix, component)`, longest prefix first."""
    table = [(m, name) for name, c in components.items() for m in c.get("modules", [])]
    return sorted(table, key=lambda t: -len(t[0]))


def _owner(module: str, table) -> str | None:
    for prefix, name in table:
        if module == prefix or module.startswith(prefix + "."):
            return name
    return None


def _module_name(path: pathlib.Path, repo: pathlib.Path) -> str:
    parts = list(path.relative_to(repo).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _source_files(repo: pathlib.Path):
    for root in SOURCE_ROOTS:
        for path in sorted((repo / root).rglob("*.py")):
            yield _module_name(path, repo), path


def _imports(path: pathlib.Path, module: str) -> list[tuple[int, str]]:
    """Every first-party module this file imports, lazy ones included."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.extend((node.lineno, a.name) for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")
                base = base[:len(base) - (node.level - 1)]
                target = ".".join(base + ([node.module] if node.module else []))
            else:
                target = node.module or ""
            # `from pkg import mod` imports a MODULE when `mod` is one, and
            # the module is what decides the owner. When `mod` is a name in
            # `pkg`, `_resolve` walks back to `pkg`.
            out.extend((node.lineno, f"{target}.{a.name}") for a in node.names)
    return [(line, name) for line, name in out if name.split(".")[0] in FIRST_PARTY]


def _resolve(name: str, repo: pathlib.Path) -> str:
    """The longest prefix of `name` that is a real module or package."""
    parts = name.split(".")
    while parts:
        base = repo.joinpath(*parts)
        if base.with_suffix(".py").exists() or (base / "__init__.py").exists():
            return ".".join(parts)
        parts.pop()
    return name


def _allowed(components: dict, owner: str) -> set[str]:
    return {owner, *components[owner].get("depends_on", [])}


def _violations() -> list[str]:
    repo = repo_file("components.yml").parent
    components = _components()
    table = _owner_table(components)
    subjects = [(m, p) for m, p in _source_files(repo)]
    for name, c in components.items():
        for dag in c.get("dags", []):
            subjects.append((f"dag:{dag}", repo_file(dag)))

    dag_owner = {f"dag:{d}": n for n, c in components.items() for d in c.get("dags", [])}
    bad = []
    for module, path in subjects:
        owner = dag_owner.get(module) or _owner(module, table)
        packaged = components[owner].get("packaged", True)
        allowed = _allowed(components, owner)
        seen = set()
        for line, name in _imports(path, module):
            target = _resolve(name, repo)
            if (line, target) in seen:
                continue
            seen.add((line, target))
            if target.split(".")[0] == "scripts":
                if packaged:
                    bad.append(f"{path.relative_to(repo)}:{line} ({owner}) imports "
                               f"{target}: scripts/ is not shipped in any component")
                continue
            dep = _owner(target, table)
            if dep is None or dep in allowed or not packaged:
                continue
            bad.append(f"{path.relative_to(repo)}:{line} ({owner}) imports "
                       f"{target} ({dep}), which `{owner}` does not depend on")
    return bad


def test_every_module_belongs_to_exactly_one_component():
    repo = repo_file("components.yml").parent
    table = _owner_table(_components())
    orphans = [m for m, _ in _source_files(repo) if _owner(m, table) is None]
    assert not orphans, (
        "modules no component in components.yml claims -- add each to the "
        "component that ships it:\n  " + "\n  ".join(orphans))
    prefixes = [p for p, _ in table]
    dupes = sorted({p for p in prefixes if prefixes.count(p) > 1})
    assert not dupes, f"claimed by more than one component: {dupes}"


def test_every_dag_file_belongs_to_exactly_one_component():
    repo = repo_file("components.yml").parent
    claimed = [d for c in _components().values() for d in c.get("dags", [])]
    on_disk = sorted(str(p.relative_to(repo)).replace("\\", "/")
                     for p in (repo / "airflow" / "dags").glob("*.py"))
    assert sorted(claimed) == on_disk, (
        f"unclaimed: {sorted(set(on_disk) - set(claimed))}; "
        f"claimed but absent: {sorted(set(claimed) - set(on_disk))}; "
        f"claimed twice: {sorted({d for d in claimed if claimed.count(d) > 1})}")


def test_dependencies_name_components_and_do_not_cycle():
    components = _components()
    for name, c in components.items():
        for dep in c.get("depends_on", []):
            assert dep in components, f"{name} depends on unknown component {dep!r}"
            assert components[dep].get("packaged", True), (
                f"{name} depends on {dep}, which is never packaged")

    def visit(name, path):
        assert name not in path, f"dependency cycle: {' -> '.join(path + [name])}"
        for dep in components[name].get("depends_on", []):
            visit(dep, path + [name])

    for name in components:
        visit(name, [])


def test_no_import_crosses_a_boundary_the_wrong_way():
    bad = _violations()
    assert not bad, (
        f"{len(bad)} import(s) cross a component boundary components.yml does "
        f"not allow:\n  " + "\n  ".join(bad))


# ------------------------------------------------------------ spark_task.OPS
# The one dispatch table this file cannot see by reading imports: the
# launcher is core, and names each operation's implementation as a STRING so
# it never imports the component that owns it.

def _ops() -> dict[str, str]:
    from reporting_platform.common.spark_task import OPS
    return OPS


def test_every_spark_op_names_a_function_that_exists():
    repo = repo_file("components.yml").parent
    bad = []
    for op, target in _ops().items():
        module, _, function = target.partition(":")
        path = repo.joinpath(*module.split(".")).with_suffix(".py")
        if not path.exists():
            bad.append(f"{op}: no module {module}")
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if function not in {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}:
            bad.append(f"{op}: {module} defines no {function}")
    assert not bad, "\n".join(bad)


def _dag_spark_calls(path: pathlib.Path) -> list[tuple[int, str]]:
    """`(line, op)` for every Spark-task launch in a DAG file.

    Every DAG launches through `spark_task.run`, directly or through a local
    one-line wrapper around it; the op is always a string literal first
    argument, so a call to one of those names with one is a launch.
    """
    launchers = {"run", "_spark_run", "_spark_subprocess"}
    out = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in launchers and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)):
            out.append((node.lineno, node.args[0].value))
    return out


def test_every_dag_launches_only_ops_its_component_may_import():
    components = _components()
    table = _owner_table(components)
    ops = _ops()
    bad, seen = [], 0
    for owner, c in components.items():
        for dag in c.get("dags", []):
            for line, op in _dag_spark_calls(repo_file(dag)):
                seen += 1
                if op not in ops:
                    bad.append(f"{dag}:{line} launches unknown op {op!r}")
                    continue
                dep = _owner(ops[op].partition(":")[0], table)
                if dep not in _allowed(components, owner) and components[owner].get("packaged", True):
                    bad.append(f"{dag}:{line} ({owner}) launches {op!r}, "
                               f"implemented in {dep}, which `{owner}` does not depend on")
    # A scan that finds nothing is not a pass: the wrapper names above are
    # what it keys on, and renaming one would silently turn this off.
    assert seen >= 10, f"found only {seen} Spark-task launches in the DAGs"
    assert not bad, "\n".join(bad)


# ------------------------------------------------------ third-party packaging
# The import name is not always the distribution name, and a few imports come
# in transitively through a declared distribution rather than being one.
IMPORT_TO_DIST = {
    "yaml": "pyyaml",
    "ruamel": "ruamel.yaml",
    "psycopg2": "psycopg2-binary",
    "botocore": "boto3",
    "smbclient": "smbprotocol",
    "gssapi": "smbprotocol",          # through smbprotocol[kerberos]
    "airflow": "apache-airflow",
    "pendulum": "apache-airflow",
    "attr": "apache-airflow",
    "cosmos": "astronomer-cosmos",
    "openlineage": "apache-airflow-providers-openlineage",
}


def _dist_name(requirement: str) -> str:
    import re
    return re.split(r"[\[<>=!~ ;]", requirement, 1)[0].strip().lower()


def _declared(components: dict, name: str) -> set[str]:
    """Every distribution `name` or anything it depends on declares."""
    out, stack, seen = set(), [name], set()
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        c = components[n]
        out |= {_dist_name(r) for r in c.get("requires", [])}
        out |= {_dist_name(r) for rs in c.get("extras", {}).values() for r in rs}
        out |= {_dist_name(r) for r in c.get("host", [])}
        stack.extend(c.get("depends_on", []))
    return out


def test_every_third_party_import_is_declared():
    import sys
    repo = repo_file("components.yml").parent
    components = _components()
    table = _owner_table(components)
    dag_files = {p.stem for p in (repo / "airflow" / "dags").glob("*.py")}
    subjects = [(m, p, _owner(m, table)) for m, p in _source_files(repo)]
    subjects += [(d, repo_file(d), n) for n, c in components.items()
                 for d in c.get("dags", [])]
    bad = set()
    for module, path, owner in subjects:
        if not components[owner].get("packaged", True):
            continue
        declared = _declared(components, owner)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and not node.level:
                names = [node.module or ""]
            else:
                continue
            for name in names:
                top = name.split(".")[0]
                if (not top or top in sys.stdlib_module_names or top == "__future__"
                        or top in FIRST_PARTY or top in dag_files):
                    continue
                dist = IMPORT_TO_DIST.get(top, top).lower()
                if dist not in declared:
                    bad.add(f"{path.relative_to(repo)} ({owner}) imports {top}: "
                            f"declare {dist} in requires, extras or host")
    assert not bad, "\n".join(sorted(bad))


def test_a_modules_parent_packages_ship_with_it_or_a_dependency():
    """`registry.deliveries` ships in ingest's wheel, but `registry/__init__.py`
    in core's -- which is only sound because ingest depends on core. An owner
    that did not would install a module into a package that does not exist."""
    repo = repo_file("components.yml").parent
    components = _components()
    table = _owner_table(components)
    modules = {m for m, _ in _source_files(repo)}
    bad = []
    for module in sorted(modules):
        owner = _owner(module, table)
        if not components[owner].get("packaged", True):
            continue
        parts = module.split(".")
        for i in range(1, len(parts)):
            parent = ".".join(parts[:i])
            if parent in modules and _owner(parent, table) not in _allowed(components, owner):
                bad.append(f"{module} ({owner}) sits in {parent} "
                           f"({_owner(parent, table)}), which {owner} does not depend on")
    assert not bad, "\n".join(bad)


def test_the_packaging_workflow_builds_every_packaged_component():
    """A second list, so it is checked: a component missing from the matrix
    is one no CI job ever builds on its own."""
    workflow = yaml.safe_load(
        repo_file(".github/workflows/components.yml").read_text())
    matrix = workflow["jobs"]["wheel"]["strategy"]["matrix"]["component"]
    packaged = [n for n, c in _components().items()
                if c.get("packaged", True) and c.get("modules")]
    assert sorted(matrix) == sorted(packaged), (
        f"components.yml packages {sorted(packaged)}; the workflow matrix "
        f"builds {sorted(matrix)}")


def test_an_op_whose_component_is_not_installed_is_refused_by_name():
    """A driver image built without the owning component says so, naming the
    module -- not a bare ModuleNotFoundError. A dependency missing from INSIDE
    an installed op is a different failure and must not be reworded as this."""
    from reporting_platform.common import spark_task

    saved = dict(spark_task.OPS)
    spark_task.OPS["absent"] = "reporting_platform.not_installed.spark_ops:op_x"
    try:
        try:
            spark_task.main(["absent"])
        except SystemExit as exc:
            message = str(exc)
        else:
            raise AssertionError("an op with no module ran")
    finally:
        spark_task.OPS.clear()
        spark_task.OPS.update(saved)
    assert "reporting_platform.not_installed.spark_ops" in message, message
    assert "not installed" in message, message
