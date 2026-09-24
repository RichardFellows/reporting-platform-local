"""The release image carries every piece of code compose bind-mounts.

Compose runs the `dev` stage of Dockerfile.airflow with the code MOUNTED, so
a new package added to the airflow anchor's volumes works locally at once.
The `release` stage has no mounts, so the same package must be COPYed there
too, or the first cluster run fails with ModuleNotFoundError. That is
exactly how `reporting_transport/` was missed once in compose itself.
Nothing else connects the two lists, so this does.

Also that compose pins `target: dev`: a multi-stage build defaults to the
LAST stage, which would put a stale copy of the code under the mounts.
No stack. See docs/DECISIONS.md#the-release-image-carries-the-code
"""
from __future__ import annotations

import re

import yaml

from tests.support import repo_file

# Mounted into the dev containers but deliberately NOT runtime code: tests
# must never ship in a runtime image, and seeds are sample data.
NOT_SHIPPED = {"./tests", "./seed", "./seed_clean"}


def _anchor() -> dict:
    compose = yaml.safe_load(repo_file("docker-compose.yml").read_text())
    return compose["x-airflow-common"]


def _release_copies() -> dict[str, str]:
    text = repo_file("Dockerfile.airflow").read_text()
    release = text.split("FROM dev AS release", 1)
    assert len(release) == 2, "Dockerfile.airflow has no `FROM dev AS release` stage"
    return {dest: src for src, dest in
            re.findall(r"^COPY\s+(?:--\S+\s+)*(\S+)\s+(\S+)\s*$", release[1], re.M)}


def test_compose_builds_the_dev_stage():
    assert _anchor()["build"].get("target") == "dev", (
        "the airflow anchor must pin `target: dev`, or compose builds `release`")


def test_every_mounted_code_directory_is_copied_into_release():
    copies = _release_copies()
    missing = []
    for volume in _anchor()["volumes"]:
        source, dest = volume.split(":")[:2]
        if not source.startswith("./") or source in NOT_SHIPPED:
            continue                                   # named volumes, fixtures
        if copies.get(dest) != source[2:]:
            missing.append(f"{source} -> {dest}")
    assert not missing, (
        f"mounted in compose but not COPYed into the release stage: {missing}")
