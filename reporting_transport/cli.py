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
import logging
import os
import sys

from reporting_transport.contract import (
    TransportConflictError, TransportContractError, TransportEvidenceError,
    TransportSourceMutatedError, TransportStorageError,
)
from reporting_transport.publisher import publish_transport_result
from reporting_transport.storage import CHECKSUM_MODES, StorageConfig, UploadTuning

EXIT_OK = 0
EXIT_UNEXPECTED = 1
EXIT_INVALID_INPUT = 2
EXIT_CONFLICT = 3
EXIT_EVIDENCE = 4
EXIT_STORAGE = 5
EXIT_SOURCE_MUTATED = 6

logger = logging.getLogger(__name__)


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
    publish.add_argument("--multipart-threshold-bytes", type=int,
                         help="files at/above this size use multipart upload; "
                              "override REPORTING_TRANSPORT_MULTIPART_"
                              "THRESHOLD_BYTES (default: "
                              f"{UploadTuning.multipart_threshold})")
    publish.add_argument("--multipart-part-size-bytes", type=int,
                         help="bytes per multipart part; override REPORTING_"
                              "TRANSPORT_MULTIPART_PART_SIZE_BYTES (default: "
                              f"{UploadTuning.multipart_part_size})")
    publish.add_argument("--multipart-max-concurrency", type=int,
                         help="parts uploaded in parallel; override "
                              "REPORTING_TRANSPORT_MULTIPART_MAX_CONCURRENCY "
                              f"(default: {UploadTuning.multipart_max_concurrency})")
    publish.add_argument("--checksum-mode", choices=sorted(CHECKSUM_MODES),
                         help="'auto' (default) uses a server-side SHA-256 "
                              "checksum to confirm a newly-stored object "
                              "instead of downloading it back, when the "
                              "backend supports one; 'full_download' always "
                              "re-downloads and re-hashes. Override "
                              "REPORTING_TRANSPORT_CHECKSUM_MODE")
    publish.add_argument("--log-level", default=None,
                         help="stderr logging level (default: INFO, or "
                              "REPORTING_TRANSPORT_LOG_LEVEL)")

    args = parser.parse_args(argv)
    if not args.data:
        parser.error("at least one --data path is required")

    _configure_logging(args.log_level)

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
    except TransportSourceMutatedError as exc:
        return _fail(EXIT_SOURCE_MUTATED, "source_mutated", exc)
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


def _configure_logging(level: str | None) -> None:
    """Operational logging to STDERR only -- stdout stays exactly the final
    JSON result DCM parses. Idempotent-ish: repeated CLI invocations in the
    same process (tests) just reconfigure the root handler rather than
    stacking handlers.
    """
    resolved = level or os.environ.get("REPORTING_TRANSPORT_LOG_LEVEL", "INFO")
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(handler)
    root.setLevel(resolved.upper())


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
    env_upload = UploadTuning.from_env()
    upload = UploadTuning(
        multipart_threshold=(args.multipart_threshold_bytes
                             if args.multipart_threshold_bytes is not None
                             else env_upload.multipart_threshold),
        multipart_part_size=(args.multipart_part_size_bytes
                             if args.multipart_part_size_bytes is not None
                             else env_upload.multipart_part_size),
        multipart_max_concurrency=(args.multipart_max_concurrency
                                   if args.multipart_max_concurrency is not None
                                   else env_upload.multipart_max_concurrency),
        checksum_mode=args.checksum_mode or env_upload.checksum_mode)
    return StorageConfig(
        bucket=bucket,
        endpoint_url=args.s3_endpoint or values["endpoint_url"],
        region=args.region or values["region"],
        received_prefix=args.received_prefix or values["received_prefix"],
        upload=upload)


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
