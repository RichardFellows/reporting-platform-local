"""Local RPL wrapper for the reference DCM transport publisher.

This is a thin caller, not a second implementation: it builds a
``StorageConfig`` from the same environment variables ``docker-compose.yml``
already sets for the rest of this platform (``S3_ENDPOINT``,
``REPORTING_WAREHOUSE``, ``REPORTING_RECEIVED_PREFIX``) and calls
:func:`reporting_transport.publisher.publish_transport` -- the identical
function a real DCM invocation of ``python -m reporting_transport publish``
calls. There is exactly one algorithm for publishing a Transport; this script
exists only to save re-typing storage flags in local development.
"""
from __future__ import annotations

import argparse
import json

from reporting_transport.publisher import publish_transport
from reporting_transport.storage import StorageConfig


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish one local DCM transfer to MinIO via the "
                    "reference reporting_transport publisher")
    parser.add_argument("--legacy-feed-id", required=True)
    parser.add_argument("--producer-run-id", required=True,
                        help="DCM's own execution/run identity; required so "
                             "the TransportID is derived deterministically")
    parser.add_argument("--cob-date", required=True,
                        help="business date this delivery represents, "
                             "YYYY-MM-DD")
    parser.add_argument("--source-system", required=True,
                        help="DCM's classification of the producing system")
    parser.add_argument("--source-observed-at", required=True,
                        help="ISO-8601 timestamp observed by DCM")
    parser.add_argument("--uploaded-at",
                        help="fixed ISO-8601 timestamp; defaults to now")
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
    result = publish_transport(
        legacy_feed_id=args.legacy_feed_id,
        producer_run_id=args.producer_run_id,
        cob_date=args.cob_date,
        source_system=args.source_system,
        source_observed_at=args.source_observed_at,
        uploaded_at=args.uploaded_at,
        source=args.source,
        files=files,
        config=StorageConfig.from_env(),
    )
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
