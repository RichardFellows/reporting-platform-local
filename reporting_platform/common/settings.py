"""The three environment-derived constants, in one place.

SEPARATE FROM `context.py` SO THE CLIENTS CAN BE. `spark.py` needs the catalog
name and `context.py` needs all three; putting them in either would make the
other import it, and `context` importing `spark` is what forces every reader
of `feeds.yml` to have pyspark installed. A module with no imports of its own
can be imported by both.

Read at import, so a process that changes one of these in its own environment
must be started with it set -- which is how every container here works, and
why `docker-compose.yml` edits need the container RECREATED, not restarted.
"""
from __future__ import annotations

import os
from pathlib import Path

CONFIG_DIR = Path(os.environ.get("REPORTING_CONFIG_DIR",
                                 "/opt/platform/reporting_platform/config"))
CATALOG = os.environ.get("REPORTING_CATALOG", "lakehouse")
ENV = os.environ.get("REPORTING_ENV", "local")
