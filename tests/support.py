"""Helpers for the config-level tests.

These tests exercise the parts of the platform that are pure Python and pure
config: what `feeds.yml` resolves to, and what the feed console writes back.
They deliberately need NOTHING from the stack -- no Spark, no Airflow, no
MinIO -- so they run on a laptop in under a second and inside the existing
image with no rebuild.

Everything here works by pointing REPORTING_CONFIG_DIR at a throwaway copy of
the config and re-importing. `common.context` caches on the config file's
mtime rather than its name, so a second copy in a second directory is a clean
slate; what is NOT clean is the already-imported module object, hence
`_purge()`.

THAT IS PROCESS-WIDE STATE, SO IT IS RESTORED. `config_dir()` sets two
environment variables and empties `sys.modules` of the package under test;
without an undo, every test module inherits whatever its predecessor happened
to leave set, and the suite passes in the order it is run rather than in any
order. `reset()` is that undo, `tests/run.py` calls it between modules, and
`atexit` catches the last one -- which is also what stops a full run leaving a
temp directory behind per call.
"""
from __future__ import annotations

import atexit
import os
import pathlib
import shutil
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parent.parent
CONFIG = REPO / "reporting_platform" / "config"

# THE DAG FILES ARE NOT UNDER `REPO` IN THE CONTAINER. Several tests read a
# DAG's source to pin something it must keep doing -- the publish gate's
# position, the four load-bearing cosmos settings -- and `docker-compose.yml`
# mounts `./airflow/dags` at `/opt/airflow/dags`, beside `/opt/platform`
# rather than inside it. Resolved rather than assumed, because tests/README.md
# says this suite runs in the container and four of them did not.
DAGS = next((d for d in (REPO / "airflow" / "dags",
                         pathlib.Path("/opt/airflow/dags")) if d.is_dir()),
            REPO / "airflow" / "dags")

# What these variables were before any test touched them, captured once at
# import. `None` means "was not set", which is a different thing to restore to
# than any value.
_ENV_KEYS = ("REPORTING_CONFIG_DIR", "DBT_PROJECT_DIR")
_ORIGINAL_ENV = {k: os.environ.get(k) for k in _ENV_KEYS}
_MADE: list[pathlib.Path] = []


def _purge() -> None:
    for name in [m for m in sys.modules if m.startswith("reporting_platform")]:
        del sys.modules[name]


def reset() -> None:
    """Put the process back as `config_dir()` found it.

    Idempotent, and safe to call when nothing has been created. The import
    purge is part of it: leaving `reporting_platform` imported against a
    config directory that has just been deleted is the state that makes a
    failure depend on which test ran first.
    """
    for d in _MADE:
        shutil.rmtree(d, ignore_errors=True)
    _MADE.clear()
    for key, value in _ORIGINAL_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    _purge()


atexit.register(reset)


def config_dir(feeds_yml: str | None = None) -> pathlib.Path:
    """A throwaway config directory, defaulting to the REAL feeds.yml.

    Pass `feeds_yml` to test a shape the shipped config does not have. Passing
    nothing is the more valuable case: it pins what the config this repo
    actually ships resolves to.
    """
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-test-"))
    _MADE.append(d)
    (d / "feeds.yml").write_text(
        feeds_yml if feeds_yml is not None
        else (CONFIG / "feeds.yml").read_text(encoding="utf-8"), encoding="utf-8")
    shutil.copy(CONFIG / "retention.yml", d / "retention.yml")
    # The third shipped config file. `maintenance_config()` reads it, and a
    # test directory without it sends the reader to the image's path, which
    # does not exist on a laptop.
    shutil.copy(CONFIG / "maintenance.yml", d / "maintenance.yml")
    os.environ["REPORTING_CONFIG_DIR"] = str(d)
    # The REAL dbt project, not a copy. `reports()`, `managed_tables()` and
    # `feeds_behind_report()` are derivations from the project directory, and
    # since phase 7 the retention interlock walks that lineage -- so a config
    # test that does not set this reads /opt/platform/dbt, which exists in the
    # image and not on a laptop. It is deliberately not copied: the project is
    # what these tests are pinning, and a copy would let it drift.
    os.environ.setdefault("DBT_PROJECT_DIR", str(REPO / "dbt"))
    _purge()
    return d


def feeds_from(feeds_yml: str | None = None):
    """(feeds dict, config dir) for the given feeds.yml text."""
    d = config_dir(feeds_yml)
    from reporting_platform.common.context import feeds
    return feeds(), d


def registry_on(d: pathlib.Path):
    """The console's registry module, pointed at this config dir's feeds.yml.

    FEEDS_YML is module-level, so it has to be redirected after the import
    that `config_dir()` invalidated.
    """
    import reporting_platform.ui.registry as registry
    registry.FEEDS_YML = d / "feeds.yml"
    return registry


# A minimal feeds.yml for the shapes the shipped config does not contain.
def synthetic(conventions: str = "", feed_extra: str = "") -> str:
    return f"""
defaults:
  landing_prefix: landing
  delimiter: ","
{conventions}
feeds:
  - name: t_one
    description: d
    source_system: SRC
    filename_pattern: 'A_(?P<cob_date>\\d{{8}})\\.csv'
    business_key: [k]
    columns: [k, v]
{feed_extra}"""
