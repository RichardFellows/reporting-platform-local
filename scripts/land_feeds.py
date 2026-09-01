"""Upload generated CSVs into the S3 landing prefix, oldest first.

Stands in for the upstream delivery. In OpenShift this is the Windows-side
push agent doing PutObject — see docs/OPENSHIFT-MAPPING.md.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from reporting_platform.common.context import feeds
from reporting_platform.ingest.arrival import put_landing


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source", type=Path, default=Path("/opt/platform/seed"))
    p.add_argument("--feed", help="land only this feed")
    p.add_argument("--limit", type=int, help="land only the N oldest files per feed")
    a = p.parse_args()

    for name, fd in feeds().items():
        if a.feed and name != a.feed:
            continue
        d = a.source / name
        if not d.exists():
            print(f"skip {name}: {d} not found")
            continue
        # Matched against the FEED'S OWN pattern, not a *.csv glob. A feed
        # may deliver .txt, .dat or anything else -- feeds.yml carries the
        # filename pattern precisely so the extension is not the platform's
        # business -- and a glob here silently landed nothing for those,
        # leaving the feed reporting "0 pending" with the file sitting in
        # seed/. It also skips files that do not belong to the feed, which the
        # glob would have landed for the ingest to ignore later.
        files = sorted(p for p in d.iterdir()
                       if p.is_file() and fd.parse_filename(p.name))
        if a.limit:
            files = files[: a.limit]
        for f in files:
            key = put_landing(fd, str(f))
            print(f"landed {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
