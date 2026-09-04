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
"""
from __future__ import annotations

import os
import pathlib
import shutil
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parent.parent
CONFIG = REPO / "reporting_platform" / "config"


def _purge() -> None:
    for name in [m for m in sys.modules if m.startswith("reporting_platform")]:
        del sys.modules[name]


def config_dir(feeds_yml: str | None = None) -> pathlib.Path:
    """A throwaway config directory, defaulting to the REAL feeds.yml.

    Pass `feeds_yml` to test a shape the shipped config does not have. Passing
    nothing is the more valuable case: it pins what the config this repo
    actually ships resolves to.
    """
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-test-"))
    (d / "feeds.yml").write_text(
        feeds_yml if feeds_yml is not None
        else (CONFIG / "feeds.yml").read_text(encoding="utf-8"), encoding="utf-8")
    shutil.copy(CONFIG / "retention.yml", d / "retention.yml")
    os.environ["REPORTING_CONFIG_DIR"] = str(d)
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
    filename_pattern: 'A_(?P<business_date>\\d{{8}})\\.csv'
    business_key: [k]
    columns: [k, v]
{feed_extra}"""
