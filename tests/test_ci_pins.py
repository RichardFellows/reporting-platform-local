"""The parse tier installs what the image installs, or it gates nothing.

`.github/workflows/parse.yml` reproduces `Dockerfile.airflow`'s dependency
set with pip on a bare runner, because building the image on every PR costs
the minutes the cheap tiers exist to avoid. That reproduction is A SECOND COPY
OF EVERY PIN, and a second copy that drifts is worse than no tier at all: CI
would install a combination that resolves, go green, and say nothing about the
one the stack actually runs. The trap it most obviously re-opens is the cosmos
`--no-deps` one -- installed WITH its dependencies cosmos resolves perfectly
well and breaks dbt, so a workflow that dropped the flag would be green
precisely when the image is broken.

WHAT IS CHECKED IS THE DIRECTION THAT MATTERS: every version the workflow pins
must be the version the Dockerfile pins. Not the reverse -- the image installs
pyspark, duckdb, pandas, marimo and the console's web stack, and the workflow
deliberately installs none of them (nothing it runs opens a Spark session, and
300 MB of wheels would double the job). A package the image has and CI does
not is a documented omission; a package where the two disagree is drift.

EVERY CHECK HERE READS COMMANDS, NEVER PROSE, and that is not fussiness -- it
is a bug this module already had twice. Matching `"install"` against whole
lines hit the workflow's own STEP NAME ("--no-deps exactly as the image
installs it") and stayed green with the flag deleted from the command below
it; searching the file text for `dbt --version` hit the header paragraph
explaining why that smoke test matters. A guard that reads a comment guards a
comment. So the workflow is parsed as YAML and only `run:` blocks are
searched, and the Dockerfile has its comment lines stripped first.

Pure string work over two files. No stack, no network.
See docs/DECISIONS.md#cosmos-no-deps
"""
from __future__ import annotations

import pathlib
import re

import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
DOCKERFILE = REPO / "Dockerfile.airflow"
WORKFLOW = REPO / ".github" / "workflows" / "parse.yml"

# A pip requirement as either file writes one: always double-quoted, extras
# optional, `==` only. Neither uses a range anywhere, and a range would be the
# drift this module exists to catch.
PIN = re.compile(r'"([A-Za-z0-9_.-]+)(?:\[[^\]]*\])?==([^"]+)"')
PROVIDER = re.compile(r'"(apache-airflow-providers-[A-Za-z0-9_.-]+)"')
CONSTRAINTS = re.compile(r"constraints-([0-9.]+)/constraints-([0-9.]+)\.txt")

# THE PINS THE WORKFLOW MUST STILL CARRY. Without this the version test goes
# vacuous the moment somebody deletes a line: every remaining pin agrees, and
# the thing that stopped being installed is what nobody notices.
REQUIRED = {"apache-airflow", "dbt-core", "dbt-spark", "astronomer-cosmos"}


def _dockerfile() -> str:
    """The Dockerfile's INSTRUCTIONS, with `ARG` defaults substituted in.

    Comment lines go first, so a version named in prose is never read as a
    pin. `astronomer-cosmos` is pinned through an ARG so `docker build
    --build-arg` can move it; the default is still a literal in the file and
    comparable with the workflow's.
    """
    text = "\n".join(ln for ln in DOCKERFILE.read_text(encoding="utf-8").splitlines()
                     if not ln.lstrip().startswith("#"))
    for name, value in re.findall(r"^ARG\s+([A-Za-z0-9_]+)=(\S+)", text,
                                  re.MULTILINE):
        text = text.replace(f"${{{name}}}", value)
    return text


def _steps() -> list[dict]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    assert len(jobs) == 1, (
        f"{WORKFLOW.name} has grown a second job ({', '.join(sorted(jobs))}); "
        f"this module assumes the parse tier is one job")
    return list(jobs["parse"]["steps"])


def _commands() -> list[str]:
    """Every `run:` block, in order. The only thing this module trusts."""
    return [str(s["run"]) for s in _steps() if "run" in s]


def _pins(text: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for name, version in PIN.findall(text):
        if found.get(name, version) != version:
            raise AssertionError(
                f"{name} is pinned to both {found[name]} and {version} in the "
                f"same file")
        found[name] = version
    return found


def test_every_workflow_pin_is_the_image_s_pin():
    image = _pins(_dockerfile())
    ci = _pins("\n".join(_commands()))

    assert ci, f"{WORKFLOW.name} runs no pinned install at all"

    for name, version in sorted(ci.items()):
        if name == "apache-airflow":
            # Not pip-installed in the image -- it IS the base image, and
            # test_airflow_is_the_version_the_base_image_is compares this pin
            # against the FROM tag.
            continue
        assert name in image, (
            f"{WORKFLOW.name} installs {name}=={version}, which "
            f"Dockerfile.airflow does not install at all -- CI would be "
            f"testing a dependency set the stack has never had")
        assert image[name] == version, (
            f"{name}: Dockerfile.airflow pins {image[name]}, "
            f"{WORKFLOW.name} pins {version}")


def test_the_pins_that_must_not_go_missing():
    ci = _pins("\n".join(_commands()))
    missing = sorted(REQUIRED - set(ci))
    assert not missing, (
        f"{WORKFLOW.name} no longer installs: {', '.join(missing)}. Every "
        f"remaining pin agreeing with the image is not evidence of anything "
        f"once the important ones are gone.")


def test_the_same_providers_are_installed():
    """Unpinned in both, because Airflow's constraint file pins them.

    So the version test above cannot see them at all. A provider the image has
    and the runner does not is an ImportError in CI that is not real; the
    other way round is a DAG importing a provider the image lacks, green here
    and broken at the scheduler.
    """
    image = set(PROVIDER.findall(_dockerfile()))
    ci = set(PROVIDER.findall("\n".join(_commands())))
    assert image, "Dockerfile.airflow installs no providers -- has it moved?"
    assert image == ci, (
        f"the image and the parse tier install different providers:\n"
        f"  only in Dockerfile.airflow: {sorted(image - ci) or 'none'}\n"
        f"  only in {WORKFLOW.name}: {sorted(ci - image) or 'none'}")


def test_airflow_is_the_version_the_base_image_is():
    """2.10.5 is deliberate. See docs/DECISIONS.md#airflow-2-not-3.

    Four strings have to say it: the base image tag, the version pip installs,
    the constraint file each resolves against, and the interpreter. The
    constraint file is the load-bearing one -- it is what stops a provider
    dragging in a different Airflow -- and it is written as a URL, so nothing
    else could ever have checked it.
    """
    docker = _dockerfile()
    commands = _commands()

    base = re.search(r"^FROM apache/airflow:(\S+)-python(\S+)", docker,
                     re.MULTILINE)
    assert base, "Dockerfile.airflow has no recognisable FROM line"
    version, python = base.group(1), base.group(2)

    installed = _pins("\n".join(commands)).get("apache-airflow")
    assert installed == version, (
        f"the base image is apache/airflow:{version}, the workflow installs "
        f"apache-airflow=={installed}")

    for text, where in ((docker, "Dockerfile.airflow"),
                        ("\n".join(commands), WORKFLOW.name)):
        url = CONSTRAINTS.search(text)
        assert url, f"{where} resolves no Airflow constraint file"
        assert url.group(1) == version, (
            f"{where} constrains against Airflow {url.group(1)}, but the base "
            f"image is {version}")
        assert url.group(2) == python, (
            f"{where} constrains against Python {url.group(2)}, but the base "
            f"image is python{python}")

    setup = [s for s in _steps()
             if str(s.get("uses", "")).startswith("actions/setup-python")]
    assert len(setup) == 1, "the workflow does not set up exactly one Python"
    assert str(setup[0]["with"]["python-version"]) == python, (
        f"the workflow runs on Python {setup[0]['with']['python-version']}, "
        f"the image on {python}")


def _cosmos_command(commands: list[str]) -> tuple[int, str]:
    for i, command in enumerate(commands):
        for line in command.splitlines():
            if "astronomer-cosmos" in line and "pip install" in line:
                return i, line
    raise AssertionError(f"{WORKFLOW.name} never pip installs astronomer-cosmos")


def test_cosmos_is_installed_no_deps_in_both():
    """The flag, not just the version. See docs/DECISIONS.md#cosmos-no-deps.

    Installed WITH its dependencies, cosmos pins typing_extensions back to
    4.12.2 and every dbt invocation dies at import. A workflow that dropped
    the flag installs cleanly and passes every other check here.
    """
    _, ci_line = _cosmos_command(_commands())
    docker_line = next(
        (ln for ln in _dockerfile().splitlines()
         if "astronomer-cosmos" in ln and "pip install" in ln), None)
    assert docker_line, "Dockerfile.airflow no longer pip installs cosmos"

    for line, where in ((docker_line, "Dockerfile.airflow"),
                        (ci_line, WORKFLOW.name)):
        assert "--no-deps" in line, (
            f"{where} installs astronomer-cosmos WITHOUT --no-deps:\n"
            f"  {line.strip()}")


def test_the_dbt_smoke_test_runs_in_ci():
    """`dbt --version` AFTER the cosmos install is the trap's only guard.

    It is in `Dockerfile.airflow`, where it runs at image build -- which CI
    never did. The whole point of this tier is that it now runs on a PR, so
    its absence from the workflow is the regression to catch. After, not
    anywhere: run before cosmos it proves the opposite of what it claims.
    """
    commands = _commands()
    cosmos_step, _ = _cosmos_command(commands)
    later = [c for c in commands[cosmos_step:] if "dbt --version" in c]
    assert later, (
        "no step after the cosmos install runs `dbt --version`, so the "
        "--no-deps trap is unguarded again")
