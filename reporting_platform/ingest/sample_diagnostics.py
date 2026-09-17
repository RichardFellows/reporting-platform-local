"""Read-only validation of an explicitly paired data/control sample.

This does not activate a feed, land bytes or certify a prepared dataset.
"""
from __future__ import annotations

import hashlib

from reporting_platform.common.parsing import csv_rows, feed_format, raw_values
from reporting_platform.ingest import control

MAX_SAMPLE_BYTES = 8 * 1024 * 1024


def diagnose_samples(feed, data: bytes, control_data: bytes | None, *,
                     data_filename: str = "sample.csv",
                     control_filename: str = "sample.ctl") -> dict:
    if len(data) > MAX_SAMPLE_BYTES or (control_data is not None and len(control_data) > MAX_SAMPLE_BYTES):
        raise ValueError("Sample diagnostics accepts at most 8 MiB per file")
    fmt = feed_format(feed)
    report = {"scope": "sample parsing and control checks; no Spark or dbt build",
              "data_sha256": hashlib.sha256(data).hexdigest(),
              "control_sha256": (hashlib.sha256(control_data).hexdigest()
                                 if control_data is not None else None),
              "format": fmt, "controls": {}}
    observed = {"md5": hashlib.md5(data).hexdigest()}
    try:
        rows = csv_rows(data, fmt, source=data_filename)
        header = next(rows, []) if feed.header else []
        preview, count = [], 0
        for row in rows:
            count += 1
            if len(preview) < 5:
                preview.append(raw_values(row))
        observed["row_count"] = count
        report["data"] = {"ok": True, "header": header, "rows": preview, **observed}
    except ValueError as exc:
        report["data"] = {"ok": False, "stage": "data_parsing", "error": str(exc)}
    encoding = feed.control_encoding or feed.file_encoding
    for block, fields in (("arrival", ("cob_date", "version")),
                          ("delivery", ("row_count", "md5"))):
        cfg = getattr(feed, block, {}).get("control")
        if cfg:
            report["controls"][block] = control.diagnose(
                cfg, control_data, encoding=encoding, fields=fields,
                feed_name=feed.name, filename=control_filename,
                block=f"{block}.control", observed=observed)
    report["ok"] = report["data"]["ok"] and all(c["ok"] for c in report["controls"].values())
    return report


def from_definition(definition: dict, data: bytes, control_data: bytes | None,
                    **filenames) -> dict:
    """Validate an unsaved definition with the normal config validators."""
    from dataclasses import asdict
    from reporting_platform.common.context import (
        Feed, effective_defaults, resolve_arrival_config, resolve_delivery_config,
    )
    from reporting_platform.ui.registry import FeedSpec, validate
    payload = {**effective_defaults(definition.get("convention") or ""), **definition}
    # Validate raw blocks before the form adapter can discard blank/unknown
    # values. An unsupported reader must never become the default regex reader.
    resolve_arrival_config(payload.get("name", "draft"), payload.get("arrival"),
                           payload.get("filename_pattern"))
    resolve_delivery_config(payload.get("name", "draft"), payload.get("delivery"))
    spec = FeedSpec.from_payload(payload)
    validate(spec, existing=set())
    values = asdict(spec)
    values["arrival"] = resolve_arrival_config(spec.name, spec.arrival, spec.filename_pattern)
    values["delivery"] = resolve_delivery_config(spec.name, spec.delivery)
    feed = Feed(**{k: v for k, v in values.items() if k in Feed.__dataclass_fields__})
    if feed.delivery.get("kind") == "archive" or feed.arrival.get("archive"):
        raise ValueError("Pair a selected CSV member with its control; archive-bundle validation is not available here")
    import json
    report = diagnose_samples(feed, data, control_data, **filenames)
    report["definition_sha256"] = hashlib.sha256(
        json.dumps(values, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return report
