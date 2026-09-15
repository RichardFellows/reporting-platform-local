"""The three jar versions, and the many places each one is written down.

THE FAILURE THIS PREVENTS NAMES NO VERSION. Diverge them and the first write
raises `NoSuchMethodError` -- after the image builds, after the stack comes up,
inside a Spark task, with nothing anywhere saying "version". The rule has been
documented in CLAUDE.md, `.env.example` and `docs/DECISIONS.md#jar-versions`
for as long as it has existed and was checked by nothing: `common/spark.py`
reads two of the three and interpolates them straight into
`spark.jars.packages`.

WHAT IS CHECKABLE HERE AND WHAT IS NOT. That the same version agrees across
every file that declares it is pure string work over the repo, and it is the
drift that actually happens -- five files hold `ICEBERG_VERSION`'s default and
a bump touches one. Whether a given extensions build is compatible with a
given Iceberg is NOT derivable from anything in this repo: it is a property of
what upstream compiled against. So the validated combinations are a hand-kept
list, sourced from `.env.example`'s own header, and the test's job is to
notice when the configured triple leaves it -- not to compute compatibility it
cannot know.

No stack, no network. See docs/DECISIONS.md#jar-versions
"""
from __future__ import annotations

import re

import yaml

from tests import test_ci_pins as ci_pins
from tests.support import repo_file

# EVERY DECLARATION SITE, with the pattern that finds the version in it. Each
# is a real second copy, not a reference: `common/spark.py` and
# `dbt/profiles.yml` are the TWO DRIVERS CLAUDE.md warns must not diverge from
# the image, because every submitting process runs a pip pyspark with no jars
# of its own and resolves these coordinates itself.
ICEBERG_SITES = {
    ".env.example": r"^ICEBERG_VERSION=(\S+)",
    "docker-compose.yml": r"ICEBERG_VERSION:\s*\$\{ICEBERG_VERSION:-([^}]+)\}",
    "Dockerfile.spark": r"^ARG ICEBERG_VERSION=(\S+)",
    "reporting_platform/common/spark.py":
        r'os\.environ\.get\("ICEBERG_VERSION",\s*"([^"]+)"\)',
    "dbt/profiles.yml":
        r"env_var\('ICEBERG_VERSION',\s*'([^']+)'\)",
}
EXT_SITES = {
    ".env.example": r"^NESSIE_SPARK_EXT_VERSION=(\S+)",
    "docker-compose.yml":
        r"NESSIE_SPARK_EXT_VERSION:\s*\$\{NESSIE_SPARK_EXT_VERSION:-([^}]+)\}",
    "Dockerfile.spark": r"^ARG NESSIE_SPARK_EXT_VERSION=(\S+)",
    "reporting_platform/common/spark.py":
        r'os\.environ\.get\("NESSIE_SPARK_EXT_VERSION",\s*"([^"]+)"\)',
    "dbt/profiles.yml":
        r"env_var\('NESSIE_SPARK_EXT_VERSION',\s*'([^']+)'\)",
}
SERVER_SITES = {
    ".env.example": r"^NESSIE_SERVER_VERSION=(\S+)",
    "docker-compose.yml":
        r"NESSIE_SERVER_VERSION:\s*\$\{NESSIE_SERVER_VERSION:-([^}]+)\}",
    "Dockerfile.airflow": r"^ARG NESSIE_SERVER_VERSION=(\S+)",
}

# The executors' Python minor is NOT a `.env`/`docker-compose.yml` knob like
# the jar triple, because nothing there could keep the promise a shared
# variable implies: `Dockerfile.airflow`'s Airflow constraint file URL
# (`constraints-2.10.5/constraints-3.11.txt`, in itself AND in
# `.github/workflows/parse.yml`) and `python-version: '3.11'` in both
# `.github/workflows/config.yml` and `parse.yml` are LITERAL strings a
# `PYTHON_MINOR=3.12` in `.env` would never reach -- it would build a 3.12
# driver image against 3.11 constraints, which resolves, installs and fails
# nowhere near here. So the driver's minor stays five literal sites, checked
# against each other and against the one place a version genuinely IS an
# ARG: `Dockerfile.spark`'s `PYTHON_VERSION`, which installs its own copy of
# CPython (deadsnakes does not publish 3.11 for Focal -- measured at build).
# See docs/DECISIONS.md#executor-python-matches-the-driver
DRIVER_MINOR_SITES = (
    "Dockerfile.airflow's FROM tag",
    "Dockerfile.airflow's Airflow constraint file URL",
    "parse.yml's Airflow constraint file URL",
    "parse.yml's actions/setup-python",
    "config.yml's actions/setup-python",
)

# (ICEBERG_VERSION, NESSIE_SPARK_EXT_VERSION) combinations this stack has been
# run against. BOTH COME FROM `.env.example`'s OWN HEADER -- the shipped pair
# and the "known-good fully-upgraded alternative" it documents. Adding a
# combination here is a claim that somebody ran it; the point of the list is
# that the claim is made once, in one place, rather than implied by whatever
# happens to be in `.env` today.
VALIDATED_PAIRS = {
    ("1.6.1", "0.99.0"),
    ("1.9.1", "0.103.3"),
}


def _version(relative: str, pattern: str) -> str:
    # `repo_file`, not `REPO / relative`: four of these sites -- `.env.example`,
    # `docker-compose.yml` and both Dockerfiles -- describe the DEPLOYMENT and
    # are not mounted into it, so in a container this skips rather than
    # reporting the drift it exists to catch as absent.
    text = repo_file(relative).read_text(encoding="utf-8")
    found = re.findall(pattern, text, re.M)
    assert found, f"{relative}: nothing matched {pattern!r} -- the version " \
                  f"moved, or this site no longer declares one"
    unique = set(found)
    assert len(unique) == 1, \
        f"{relative} declares {sorted(unique)} for one version"
    return found[0]


def _agree(sites: dict[str, str], label: str) -> str:
    found = {name: _version(name, pattern) for name, pattern in sites.items()}
    unique = set(found.values())
    assert len(unique) == 1, (
        f"{label} disagrees across the files that declare it: "
        + ", ".join(f"{n}={v}" for n, v in sorted(found.items()))
        + ". Diverging them gives NoSuchMethodError on the first write, "
          "never anything naming a version.")
    return unique.pop()


def _parts(version: str) -> tuple[int, ...]:
    return tuple(int(p) for p in re.findall(r"\d+", version))


def test_iceberg_version_agrees_everywhere_it_is_declared():
    """FIVE COPIES. The image bakes the jars in; `spark_session()` and
    `dbt/profiles.yml` each resolve the coordinates again, because every
    submitting process runs a pip pyspark with no jars of its own. A bump that
    touches four of the five is the realistic mistake."""
    assert _agree(ICEBERG_SITES, "ICEBERG_VERSION")


def test_nessie_spark_extension_version_agrees_everywhere():
    assert _agree(EXT_SITES, "NESSIE_SPARK_EXT_VERSION")


def test_nessie_server_version_agrees_with_the_gc_jar():
    """The server image and the `nessie-gc` jar are ONE version, and
    `docker-compose.yml`'s comment says so. The jar is ~128MB of
    `Dockerfile.airflow` and is fetched by release tag, so a mismatch is a
    404 at build -- loud, but only once somebody rebuilds that image."""
    assert _agree(SERVER_SITES, "NESSIE_SERVER_VERSION")


def test_the_configured_pair_is_one_that_has_been_run():
    """The extensions track ICEBERG, not the server, and the compatibility is
    upstream's build-time fact -- not anything this repo can compute. So the
    assertion is membership of a list somebody maintained deliberately."""
    pair = (_agree(ICEBERG_SITES, "ICEBERG_VERSION"),
            _agree(EXT_SITES, "NESSIE_SPARK_EXT_VERSION"))
    assert pair in VALIDATED_PAIRS, (
        f"ICEBERG_VERSION={pair[0]} with NESSIE_SPARK_EXT_VERSION={pair[1]} "
        f"is not a combination this stack has been run against "
        f"({sorted(VALIDATED_PAIRS)}). The extensions track Iceberg and "
        f"newer-than-yours is the failing direction. If you have run it, add "
        f"it to VALIDATED_PAIRS in this file and to `.env.example`'s header.")


def test_the_server_may_be_newer_than_the_extensions_but_not_older():
    """DELIBERATELY AHEAD, and `.env.example` says why: ECS-shaped JSON logging
    needs a Quarkus build-time extension absent from the 0.99.0 image, so the
    keys in the `nessie:` block are inert below ~0.104. The server is allowed
    to lead; it is not allowed to lag, because the extensions speak to it."""
    server = _parts(_agree(SERVER_SITES, "NESSIE_SERVER_VERSION"))
    ext = _parts(_agree(EXT_SITES, "NESSIE_SPARK_EXT_VERSION"))
    assert server >= ext, (
        f"NESSIE_SERVER_VERSION {server} is older than the Spark extensions "
        f"{ext}. The server may be newer than the extensions; the reverse is "
        f"the failing direction.")


def test_the_quarkus_json_logging_keys_match_the_server_they_need():
    """`.env.example`: pin the server below 0.104 and the two
    `quarkus.log.console.json.*` lines must go with it -- left behind they are
    inert config that reads as working, which is this repo's most-repeated
    failure shape. Nothing enforced the pairing."""
    compose = repo_file("docker-compose.yml").read_text(encoding="utf-8")
    has_keys = "quarkus.log.console.json" in compose
    server = _parts(_agree(SERVER_SITES, "NESSIE_SERVER_VERSION"))
    if has_keys:
        assert server >= (0, 104), (
            f"docker-compose.yml sets quarkus.log.console.json.* but the "
            f"server is {server}, below 0.104 where the build-time extension "
            f"first appears. Those keys change nothing and say nothing there.")


def _agree_values(values: dict[str, str], label: str) -> str:
    """Like `_agree`, but starting from already-extracted values rather than
    (relative path, pattern) sites -- two of the driver sites are a workflow
    field, not a whole-file regex, so they cannot go through `_version`."""
    unique = set(values.values())
    assert len(unique) == 1, (
        f"{label} disagrees across the places that declare it: "
        + ", ".join(f"{n}={v}" for n, v in sorted(values.items())))
    return unique.pop()


def _workflow_python_version(relative: str, job: str) -> str:
    """`python-version:` off the `actions/setup-python` step, read as YAML --
    never the raw file text, which is how a stale copy sitting in a comment
    could still match. Mirrors `test_ci_pins._steps()`, generalised to
    whichever workflow file is asked for (that module hardcodes parse.yml)."""
    workflow = yaml.safe_load(repo_file(relative).read_text(encoding="utf-8"))
    steps = workflow["jobs"][job]["steps"]
    setup = [s for s in steps if str(s.get("uses", "")).startswith("actions/setup-python")]
    assert len(setup) == 1, f"{relative} does not set up exactly one Python"
    return str(setup[0]["with"]["python-version"])


def _airflow_from_tag_minor() -> str:
    text = repo_file("Dockerfile.airflow").read_text(encoding="utf-8")
    m = re.search(r"^FROM apache/airflow:2\.10\.5-python(\d+\.\d+)$", text, re.M)
    assert m, "Dockerfile.airflow has no recognisable FROM apache/airflow:2.10.5-pythonX.Y line"
    return m.group(1)


def _spark_python_minor() -> str:
    """`Dockerfile.spark`'s `PYTHON_VERSION` ARG, reduced to its minor --
    `3.11.11` -> `3.11` -- for comparison against the drivers' sites, which
    only ever name a minor."""
    text = repo_file("Dockerfile.spark").read_text(encoding="utf-8")
    m = re.search(r"^ARG PYTHON_VERSION=(\d+\.\d+)\.\d+$", text, re.M)
    assert m, "Dockerfile.spark: nothing matched ARG PYTHON_VERSION=X.Y.Z -- " \
              "the pin moved, or this site no longer declares one"
    return m.group(1)


def _driver_minor_values() -> dict[str, str]:
    airflow_constraints = ci_pins.CONSTRAINTS.search(ci_pins._dockerfile())
    assert airflow_constraints, "Dockerfile.airflow resolves no Airflow constraint file"
    parse_constraints = ci_pins.CONSTRAINTS.search("\n".join(ci_pins._commands()))
    assert parse_constraints, "parse.yml resolves no Airflow constraint file"

    return dict(zip(DRIVER_MINOR_SITES, [
        _airflow_from_tag_minor(),
        airflow_constraints.group(2),
        parse_constraints.group(2),
        _workflow_python_version(".github/workflows/parse.yml", "parse"),
        _workflow_python_version(".github/workflows/config.yml", "config"),
    ]))


def test_the_drivers_python_minor_agrees_across_every_site_that_names_it():
    """FIVE SITES, none of them `.env`. `Dockerfile.airflow`'s own FROM tag
    and constraint URL have to agree with each other (already asserted by
    `test_ci_pins.test_airflow_is_the_version_the_base_image_is`, reused here
    rather than re-implemented) and with the same two facts in
    `.github/workflows/parse.yml`, plus `config.yml`'s own Python setup --
    the one site nothing else in this repo checks, because `test_ci_pins.py`
    is scoped to `parse.yml` only."""
    assert _agree_values(_driver_minor_values(), "the drivers' Python minor")


def test_the_executors_python_minor_matches_the_drivers():
    """The one comparison that is actually this item: `Dockerfile.spark`'s
    installed CPython against the drivers' agreed minor above. Diverge them
    and the failure is `PYTHON_VERSION_MISMATCH` inside a Spark task, naming
    neither the image nor this file -- exactly like the jar triple's
    `NoSuchMethodError`. See docs/DECISIONS.md#executor-python-matches-the-driver
    """
    driver_minor = _agree_values(_driver_minor_values(), "the drivers' Python minor")
    spark_minor = _spark_python_minor()
    assert spark_minor == driver_minor, (
        f"Dockerfile.spark installs Python {spark_minor} for the executors, "
        f"but the drivers all run {driver_minor}.")


def test_dockerfile_spark_actually_uses_its_python_version_arg():
    """The ARG's value agreeing with the drivers is not the same claim as the
    build actually spending it -- parse the download URL and the symlink
    commands, not just the ARG declaration beside them."""
    text = repo_file("Dockerfile.spark").read_text(encoding="utf-8")
    assert re.search(r"cpython-\$\{PYTHON_VERSION\}", text), (
        "Dockerfile.spark declares ARG PYTHON_VERSION but the download URL "
        "does not reference ${PYTHON_VERSION} -- the pin is declared and "
        "nothing installs it")
    assert re.search(r"PYVER_MINOR=.*PYTHON_VERSION", text), (
        "Dockerfile.spark does not derive a minor from ${PYTHON_VERSION} -- "
        "a hardcoded python3.11 in the symlink commands would silently stop "
        "tracking the ARG on the next bump")
    assert re.search(r"ln -sf .*PYVER_MINOR.* /usr/local/bin/python3\b", text), (
        "Dockerfile.spark does not symlink /usr/local/bin/python3 to the "
        "interpreter it just installed")
