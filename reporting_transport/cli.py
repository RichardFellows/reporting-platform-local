"""Production-capable CLI a real DCM environment can invoke as a subprocess.

    python -m reporting_transport publish \\
      --legacy-feed-id 1234 --producer-run-id 849217 \\
      --cob-date 2026-09-21 --source-system RISK_ENGINE_X \\
      --source-observed-at 2026-09-22T01:13:00Z \\
      --data \\dfs\\...\\positions.csv --control \\dfs\\...\\positions.ctl

Always prints exactly one JSON object to stdout and never prints source-file
contents or credentials. Exit codes are stable and documented -- see
``docs/TRANSPORT-CONTRACT.md``, "CLI invocation" -- so DCM automation can
branch on them without parsing the message text.
"""
from __future__ import annotations

import argparse
import json
import sys

from reporting_transport.contract import (
    TransportConflictError, TransportContractError, TransportEvidenceError,
    TransportStorageError,
)
from reporting_transport.publisher import publish_transport_result
from reporting_transport.storage import StorageConfig

EXIT_OK = 0
EXIT_UNEXPECTED = 1
EXIT_INVALID_INPUT = 2
EXIT_CONFLICT = 3
EXIT_EVIDENCE = 4
EXIT_STORAGE = 5


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m reporting_transport",
        description="Publish one DCM transfer to the S3 Transport contract")
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish = subparsers.add_parser(
        "publish", description="Publish source objects, then _COMPLETE.json")
    publish.add_argument("--legacy-feed-id", required=True)
    publish.add_argument("--producer-run-id", required=True)
    publish.add_argument("--cob-date", required=True,
                         help="business date this delivery represents, "
                              "YYYY-MM-DD")
    publish.add_argument("--source-system", required=True,
                         help="DCM's classification of the producing system, "
                              "e.g. RISK_ENGINE_X")
    publish.add_argument("--source-observed-at", required=True,
                         help="ISO-8601 timestamp DCM observed completion")
    publish.add_argument("--source", default="DCM")
    publish.add_argument("--uploaded-at",
                         help="fixed ISO-8601 timestamp; defaults to now "
                              "(testing/replay only)")
    publish.add_argument("--data", action="append", default=[], metavar="PATH",
                         help="data source path; repeat for multiple objects")
    publish.add_argument("--control", action="append", default=[],
                         metavar="PATH",
                         help="control source path; repeat when present")
    publish.add_argument("--s3-endpoint",
                         help="override REPORTING_TRANSPORT_S3_ENDPOINT/"
                              "S3_ENDPOINT")
    publish.add_argument("--bucket",
                         help="override REPORTING_TRANSPORT_BUCKET/"
                              "REPORTING_WAREHOUSE")
    publish.add_argument("--region",
                         help="override REPORTING_TRANSPORT_REGION/AWS_REGION")
    publish.add_argument("--received-prefix",
                         help="override REPORTING_TRANSPORT_RECEIVED_PREFIX/"
                              "REPORTING_RECEIVED_PREFIX")

    args = parser.parse_args(argv)
    if not args.data:
        parser.error("at least one --data path is required")

    files = [("data", path) for path in args.data]
    files.extend(("control", path) for path in args.control)

    try:
        config = _config_from_args(args)
        result = publish_transport_result(
            legacy_feed_id=args.legacy_feed_id,
            producer_run_id=args.producer_run_id,
            cob_date=args.cob_date,
            source_system=args.source_system,
            source_observed_at=args.source_observed_at,
            source=args.source,
            uploaded_at=args.uploaded_at,
            files=files,
            config=config,
        )
    except TransportConflictError as exc:
        return _fail(EXIT_CONFLICT, "transport_conflict", exc)
    except TransportEvidenceError as exc:
        return _fail(EXIT_EVIDENCE, "evidence_mismatch", exc)
    except TransportContractError as exc:
        return _fail(EXIT_INVALID_INPUT, "invalid_input", exc)
    except TransportStorageError as exc:
        return _fail(EXIT_STORAGE, "storage_error", exc)
    except Exception as exc:  # noqa: BLE001 - last-resort classification
        if _looks_like_storage_error(exc):
            return _fail(EXIT_STORAGE, "storage_error", exc)
        return _fail(EXIT_UNEXPECTED, "unexpected_error", exc)

    payload = result.transport.as_dict()
    payload["status"] = "published" if result.newly_published else \
        "already_published"
    print(json.dumps(payload, indent=2, sort_keys=True))
    return EXIT_OK


def _config_from_args(args: argparse.Namespace) -> StorageConfig:
    """CLI flags win over the environment -- resolved BEFORE the bucket
    presence check, so ``--bucket`` alone is enough with no env var set at
    all. ``StorageConfig.from_env()`` alone cannot do this: it raises the
    moment a bucket is absent from the environment, before a CLI override
    ever gets a chance to supply one.
    """
    values = StorageConfig._env_values()  # noqa: SLF001 - same package
    bucket = args.bucket or values["bucket"]
    if not bucket:
        raise TransportContractError(
            "no bucket configured: pass --bucket, or set "
            "REPORTING_TRANSPORT_BUCKET / REPORTING_WAREHOUSE")
    return StorageConfig(
        bucket=bucket,
        endpoint_url=args.s3_endpoint or values["endpoint_url"],
        region=args.region or values["region"],
        received_prefix=args.received_prefix or values["received_prefix"])


def _looks_like_storage_error(exc: Exception) -> bool:
    module = type(exc).__module__
    return module.startswith("botocore") or module.startswith("boto3") or \
        module.startswith("urllib3")


def _fail(code: int, error_type: str, exc: Exception) -> int:
    payload = {
        "status": "error",
        "error_type": error_type,
        "message": str(exc),
        "exit_code": code,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"reporting_transport publish failed ({error_type}): {exc}",
         file=sys.stderr)
    return code
