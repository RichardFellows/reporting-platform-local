"""Reference implementation of the reporting-platform S3 Transport producer.

This package has no dependency on the rest of ``reporting_platform``: it only
needs ``boto3`` and the standard library. It is meant to be usable two ways:

- as the local RPL development producer (``scripts/simulate_dcm_transport.py``
  calls :func:`reporting_transport.publisher.publish_transport` directly); and
- as the reference producer a real DCM (.NET) environment invokes as a
  subprocess through :mod:`reporting_transport.cli` /
  ``python -m reporting_transport``.

``reporting_platform.ingest.transport`` is the CONSUMER side: it re-uses
:mod:`reporting_transport.contract` for parsing/validation so there is exactly
one implementation of the wire contract, and adds the RPL-specific S3 client
wiring, evidence re-validation and discovery helpers that only the platform
needs. See ``docs/TRANSPORT-CONTRACT.md``.
"""
from __future__ import annotations
