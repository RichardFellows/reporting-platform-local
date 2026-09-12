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

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent

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
    text = (REPO / relative).read_text(encoding="utf-8")
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
    compose = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
    has_keys = "quarkus.log.console.json" in compose
    server = _parts(_agree(SERVER_SITES, "NESSIE_SERVER_VERSION"))
    if has_keys:
        assert server >= (0, 104), (
            f"docker-compose.yml sets quarkus.log.console.json.* but the "
            f"server is {server}, below 0.104 where the build-time extension "
            f"first appears. Those keys change nothing and say nothing there.")
