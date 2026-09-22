"""Difference artifacts: object storage, never Postgres (Phase 8, section 30).

Written ONLY when a comparison finds something to investigate -- an ordinary
PASS leaves `diff_ref` NULL on its evidence row. Mirrors
`registry/artifacts.py`'s client/bucket construction exactly, so this module
needs no new environment variable.
"""
from __future__ import annotations

import json
import os
import re
from datetime import date
from typing import Any

from reporting_platform.common import settings

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
        raise ValueError(f"migration diff path segment is empty: {value!r}")
    return segment


def prefix(feed: str, comparison_id: str) -> str:
    return f"migration-diffs/{_segment(feed)}/{_segment(comparison_id)}/"


def write(feed: str, comparison_id: str, *, summary: dict[str, Any],
         legacy_only: list | None = None, new_only: list | None = None,
         changed: list | None = None, client=None,
         bucket: str | None = None) -> str:
    """Write a diagnostic bundle for one non-PASS comparison. Returns the
    `s3://` reference stored as `diff_ref`.

    Each list is capped defensively -- diagnostic SAMPLES, never a full
    difference dump (section 30: "Support an optional difference artifact",
    section 39/49's guard against millions of rows landing anywhere this
    platform indexes). A caller needing the full set already has it in the
    engine that computed it (Spark, or the legacy adapter's own store).
    """
    cli = client or _client()
    buck = bucket or _bucket()
    key_prefix = prefix(feed, comparison_id)
    cap = 10_000

    def _put(name: str, payload: Any) -> None:
        cli.put_object(Bucket=buck, Key=f"{key_prefix}{name}",
                       Body=json.dumps(payload, default=str, indent=2).encode("utf-8"),
                       ContentType="application/json")

    _put("summary.json", summary)
    if legacy_only:
        _put("legacy-only.json", legacy_only[:cap])
    if new_only:
        _put("new-only.json", new_only[:cap])
    if changed:
        _put("changed.json", changed[:cap])
    return f"s3://{buck}/{key_prefix}"
