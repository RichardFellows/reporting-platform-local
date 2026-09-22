"""Durable dbt execution artifacts attached to a registry run.

Cosmos invokes dbt once per rendered task.  Each invocation overwrites the
shared ``target/`` directory, so copying artifacts only in ``publish`` would
retain whichever task happened to finish last.  The DAG therefore calls
``archive`` from each dbt task callback, while that task still owns the files.

Artifacts are immutable objects below one run prefix.  The registry stores the
prefix, not a local container path, and ``require_complete`` is the pre-merge
guard which proves every successful dbt task retained both its manifest and
run results.  ``catalog.json`` is retained when an invocation generated one;
ordinary ``dbt run``/``dbt test`` invocations do not.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Iterable

from reporting_platform.common import settings

REQUIRED = ("manifest.json", "run_results.json")
OPTIONAL = ("catalog.json",)
_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def _client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint(),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _bucket() -> str:
    return settings.bucket_of(settings.warehouse())


def _segment(value: str) -> str:
    segment = _SAFE.sub("-", value).strip("-")
    if not segment:
        raise ValueError(f"dbt artifact path segment is empty: {value!r}")
    return segment


def prefix(run_id: str) -> str:
    """The immutable object prefix associated with one registry run."""
    return f"dbt-artifacts/{_segment(run_id)}/"


def reference(run_id: str, *, bucket: str | None = None) -> str:
    """Durable reference stored on ``registry.run``."""
    return f"s3://{bucket or _bucket()}/{prefix(run_id)}"


def attempt_prefix(run_id: str, task_id: str, try_number: int) -> str:
    if try_number < 1:
        raise ValueError(f"try_number must be positive, got {try_number}")
    return f"{prefix(run_id)}{_segment(task_id)}/attempt-{try_number}/"


def archive(run_id: str, task_id: str, try_number: int, *,
            target_path: str | Path | None = None, client=None,
            bucket: str | None = None) -> dict[str, Any]:
    """Copy one dbt invocation's available artifacts to immutable storage."""
    target = Path(target_path or os.environ.get(
        "DBT_TARGET_PATH", "/opt/platform/run/dbt/target"))
    client = client or _client()
    bucket = bucket or _bucket()
    base = attempt_prefix(run_id, task_id, try_number)
    written: list[str] = []
    missing: list[str] = []
    for name in REQUIRED + OPTIONAL:
        path = target / name
        if not path.is_file():
            if name in REQUIRED:
                missing.append(name)
            continue
        key = base + name
        body = path.read_bytes()
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body,
                              ContentType="application/json", IfNoneMatch="*")
        except Exception:
            # A callback may be retried after it uploaded the object.  Accept
            # only byte-identical existing evidence; never overwrite an
            # earlier attempt's artifact with today's target directory.
            existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            if existing != body:
                raise RuntimeError(f"dbt artifact already exists with different bytes: {key}")
        written.append(key)
    return {"run_id": run_id, "task_id": task_id, "try_number": try_number,
            "reference": reference(run_id, bucket=bucket),
            "written": written, "missing": missing}


def require_complete(run_id: str, attempts: Iterable[tuple[str, int]], *,
                     client=None, bucket: str | None = None) -> dict[str, Any]:
    """Refuse publication when a successful dbt task lacks required evidence."""
    client = client or _client()
    bucket = bucket or _bucket()
    missing: list[str] = []
    checked = 0
    for task_id, try_number in attempts:
        base = attempt_prefix(run_id, task_id, try_number)
        for name in REQUIRED:
            key = base + name
            checked += 1
            try:
                client.head_object(Bucket=bucket, Key=key)
            except Exception:
                missing.append(key)
    if missing:
        raise RuntimeError(
            "dbt artifact retention is incomplete; refusing to publish: "
            + ", ".join(missing))
    return {"run_id": run_id, "reference": reference(run_id, bucket=bucket),
            "artifacts_checked": checked}
