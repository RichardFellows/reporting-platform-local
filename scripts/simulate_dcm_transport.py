"""Publish a completed local DCM transport to the configured object store."""
from __future__ import annotations

import argparse
import json

from reporting_platform.ingest.dcm_simulator import simulate_transport


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Simulate one DCM follow-on upload; no directory watching")
    parser.add_argument("--transport-id", required=True)
    parser.add_argument("--legacy-feed-id", required=True)
    parser.add_argument("--source-observed-at", required=True,
                        help="ISO-8601 timestamp observed by DCM")
    parser.add_argument("--uploaded-at",
                        help="fixed ISO-8601 timestamp; defaults to now")
    parser.add_argument("--producer-run-id")
    parser.add_argument("--source", default="DCM")
    parser.add_argument("--data", action="append", default=[], metavar="PATH",
                        help="data source path; repeat for multiple objects")
    parser.add_argument("--control", action="append", default=[], metavar="PATH",
                        help="control source path; repeat when present")
    args = parser.parse_args(argv)
    if not args.data:
        parser.error("at least one --data path is required")
    files = [("data", path) for path in args.data]
    files.extend(("control", path) for path in args.control)
    result = simulate_transport(
        transport_id=args.transport_id,
        legacy_feed_id=args.legacy_feed_id,
        source_observed_at=args.source_observed_at,
        uploaded_at=args.uploaded_at,
        producer_run_id=args.producer_run_id,
        source=args.source,
        files=files,
    )
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
