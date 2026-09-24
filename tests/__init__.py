"""The config-level test suite. See tests/README.md and tests/run.py.

THE SUITE DEFAULTS THE CONFIG DIRECTORY TO THIS CHECKOUT'S; THE PLATFORM DOES
NOT. Some tests read the REAL registry through `context.feed()`/`feeds()`
without a `support.config_dir()` copy (test_dedupe_rank, test_migration_config,
test_doc_claims), and `common/settings.py` defaults `CONFIG_DIR` to the
container's `/opt/platform/reporting_platform/config`. On a fresh clone with
nothing set, those twelve failed with "no feed registry at /opt/platform/..."
-- red on tests with nothing wrong with them (todo 26). In the container this
path IS that one, so the default changes nothing there.

HERE, IN THE PACKAGE, because it has to run before the first
`reporting_platform` import (`CONFIG_DIR` is read at import time) and this is
the one module every `tests.*` import runs first -- `tests/run.py`, a direct
`python -c 'import tests.test_x'` and `pytest tests/` alike. In
`tests/support.py` it was too late for any module that imports the platform
before (or without) importing `support`.

NOT IN `settings.py`: a service without its config mounted must keep REFUSING
(`layout.feed_paths`) rather than go looking for some other registry. An
explicit value -- a developer pointing at another tree -- still wins; an EMPTY
one is treated as unset, because `Path("")` is the current directory.
`.github/workflows/config.yml` runs the suite with the variable unset, and
`tests/test_ci_pins.py` fails if it stops doing so.

Because it points at the working tree, `tests/run.py` fails the run if the
directory's registry files (`.yml`/`.yaml`) changed during it: a test that
forgets `config_dir()` and saves through the console would otherwise rewrite
the checkout silently.

`CONFIG_DEFAULT` is the one definition of the path; `tests/support.py`
imports it as `CONFIG`.
"""
from __future__ import annotations

import os
import pathlib

CONFIG_DEFAULT = (pathlib.Path(__file__).resolve().parent.parent
                  / "reporting_platform" / "config")

if not os.environ.get("REPORTING_CONFIG_DIR"):
    os.environ["REPORTING_CONFIG_DIR"] = str(CONFIG_DEFAULT)
