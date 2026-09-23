"""Publish one DCM transfer WITHOUT the storage-side checksum checks.

A stripped-down stand-in for ``python -m reporting_transport publish`` for
S3-compatible stores that reject or mis-report checksums. Same arguments,
same object keys, same ``_COMPLETE.json``. What it leaves out:

- no ``ChecksumAlgorithm="SHA256"`` on the upload, and botocore's own
  automatic CRC trailers and response validation are switched off;
- no post-upload confirmation (neither the HEAD checksum nor the GET +
  rehash) -- the object is trusted to hold what was sent;
- single PUT only (no multipart), so each file must be under 5 GiB.

The manifest still declares each file's size and SHA-256, computed from the
LOCAL file, because the contract requires them. The platform's consumer
(``reporting_platform/ingest/transport.py``) re-downloads and re-hashes every
object at ingest regardless, so if the stored bytes really differ from the
local file this moves the failure to ingest; it does not hide it.

    python scripts/publish_transport_unverified.py \\
      --legacy-feed-id 1234 --producer-run-id 849217 \\
      --cob-date 2026-09-21 --source-system RISK_ENGINE_X \\
      --source-observed-at 2026-09-22T01:13:00Z \\
      --data positions.csv --control positions.ctl \\
      --bucket lakehouse --s3-endpoint https://s3.example.internal

Credentials come from the standard boto3 chain, as for the real CLI.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402

from reporting_transport import contract  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--legacy-feed-id", required=True)
    p.add_argument("--producer-run-id", required=True)
    p.add_argument("--cob-date", required=True)
    p.add_argument("--source-system", required=True)
    p.add_argument("--source-observed-at", required=True)
    p.add_argument("--source", default="DCM")
    p.add_argument("--uploaded-at")
    p.add_argument("--data", action="append", default=[], required=True)
    p.add_argument("--control", action="append", default=[])
    p.add_argument("--bucket", default=os.environ.get("REPORTING_TRANSPORT_BUCKET"))
    p.add_argument("--s3-endpoint",
                   default=os.environ.get("REPORTING_TRANSPORT_S3_ENDPOINT"))
    p.add_argument("--region", default=os.environ.get("REPORTING_TRANSPORT_REGION"))
    p.add_argument("--received-prefix",
                   default=os.environ.get("REPORTING_TRANSPORT_RECEIVED_PREFIX",
                                          contract.DEFAULT_RECEIVED_PREFIX))
    args = p.parse_args(argv)
    if not args.bucket:
        p.error("--bucket (or REPORTING_TRANSPORT_BUCKET) is required")

    transport_id = f"{args.source.lower()}-{args.legacy_feed_id}-{args.producer_run_id}"
    contract.validate_transport_id(transport_id)
    cob_date = contract.validate_cob_date(args.cob_date)
    source_system = contract.validate_source_system(args.source_system)
    prefix = contract.transport_prefix(cob_date, source_system, transport_id,
                                       args.received_prefix)
    marker_key = contract.complete_key(cob_date, source_system, transport_id,
                                       args.received_prefix)

    s3 = boto3.client(
        "s3", endpoint_url=args.s3_endpoint, region_name=args.region,
        config=Config(request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"))

    files = [("data", Path(x)) for x in args.data]
    files += [("control", Path(x)) for x in args.control]
    declared = []
    for role, path in files:
        body = path.read_bytes()
        key = f"{prefix}{path.name}"
        s3.put_object(Bucket=args.bucket, Key=key, Body=body)
        print(f"uploaded {path} -> s3://{args.bucket}/{key}", file=sys.stderr)
        declared.append({"role": role, "original_filename": path.name,
                         "object_key": key, "bytes": len(body),
                         "sha256": hashlib.sha256(body).hexdigest()})

    uploaded_at = args.uploaded_at or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    raw = {
        "transport_contract_version": contract.CONTRACT_VERSION,
        "transport_id": transport_id,
        "source": args.source,
        "legacy_feed_id": args.legacy_feed_id,
        "producer_run_id": args.producer_run_id,
        "cob_date": cob_date,
        "source_system": source_system,
        "source_observed_at": args.source_observed_at,
        "uploaded_at": uploaded_at,
        "files": declared,
    }
    # Round-trip through the contract parser so a manifest the platform
    # would refuse is never written.
    manifest = contract.parse_transport(json.dumps(raw), marker_key,
                                        args.received_prefix)
    s3.put_object(Bucket=args.bucket, Key=marker_key,
                  Body=contract.serialize_transport(manifest),
                  ContentType="application/json")
    print(json.dumps({**manifest.as_dict(), "status": "published_unverified"},
                     indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
