"""`python -m reporting_platform.ui` -- run the feed console.

Single worker on purpose. The console writes feeds.yml, _sources.yml,
_prepared.yml and context.py, and two workers racing on the same
read-modify-write of a YAML document would interleave into a file that has
lost one of the edits. One process is also entirely enough for a console.
"""
from __future__ import annotations

import os


def main() -> int:
    import uvicorn

    uvicorn.run(
        "reporting_platform.ui.app:app",
        host=os.environ.get("FEED_UI_HOST", "0.0.0.0"),
        port=int(os.environ.get("FEED_UI_PORT", "8082")),
        workers=1,
        log_level=os.environ.get("FEED_UI_LOG_LEVEL", "info"),
        reload=os.environ.get("FEED_UI_RELOAD", "").lower() in ("1", "true"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
