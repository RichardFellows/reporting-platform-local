"""Control-file validation: is this the file the sender sent, complete?

Some feeds ship a small sidecar alongside the delivery:

    ReportingDate|20260831
    CreatedDate|20260901Z0003.14
    Rows|143256
    MD5|<digest>

This module locates it, parses it, and answers whether the delivery matches.

WHY THIS ABORTS THE INGEST RATHER THAN FAILING A TEST. The platform's rule is
that a bad VALUE must land and fail a test, not a load -- casting happens in
prepared so that a broken value is caught by write-audit-publish rather than
stopping a delivery. A control file asks a different question: not "is this
value right" but "is this the file the sender sent, complete and uncorrupted".
Modelling bytes you already know are wrong is not a test, it is a waste, and
`expected_min_rows` already sets the precedent that delivery-level completeness
aborts. An abort is cheap here: the branch is abandoned and `main` never moved.

THE PARSER IS PER-FEED, because every source system writes these differently.
Two shapes are supported and the first covers the common case:

  * `format: keyvalue` -- one `KEY<delimiter>VALUE` per line. Which keys mean
    what is declared per feed, so `Rows`/`RECORD_COUNT`/`NumRecords` are all
    the same thing to this module without any of them being hard-coded.
  * `content_pattern` -- a regex with named groups, for anything that is not
    key/value at all. Same escape hatch as `filename_pattern`, and the same
    reason: the platform should not need a release to accept a new dialect.

WHAT IS NOT ASSUMED. The digest is compared as an opaque string against the
computed one rendered in the feed's declared encoding. A hex MD5 is the common
case and the default; base64 exists in the wild, and a checker that insists on
32 hex characters rejects a perfectly good control file with a message about
the wrong thing entirely.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import re
from datetime import date
from typing import Any

log = logging.getLogger(__name__)


class ControlError(Exception):
    """The delivery does not match its control file, or the file is unusable."""


def spec(feed) -> dict[str, Any]:
    """The feed's `control:` block, or {} when it ships no control file."""
    return getattr(feed, "control", None) or {}


def required(feed) -> bool:
    """Is a control file mandatory for this feed?

    Defaults TRUE when a control block exists at all: declaring one and then
    treating its absence as fine is the configuration equivalent of a test that
    never runs. A feed whose sender is inconsistent can set it false
    explicitly, which at least records that the inconsistency is known.
    """
    s = spec(feed)
    return bool(s) and s.get("required", True)


def control_name(feed, business_date: date, version: int = 1) -> str | None:
    """The control filename this delivery should be accompanied by.

    Built by substituting the business date into the feed's control
    `filename_pattern` -- the same pattern used to RECOGNISE one, read in the
    other direction. Returns None when the feed has no control block.
    """
    s = spec(feed)
    template = s.get("filename_template")
    if template:
        return template.format(business_date=f"{business_date:%Y%m%d}",
                               version=version, name=feed.name)
    return None


def is_control(feed, filename: str) -> tuple[date, int] | None:
    """Does this filename look like one of THIS feed's control files?

    Returns (business_date, version) so a control file can be paired with the
    delivery it describes, or None. Deliberately mirrors Feed.parse_filename:
    a control file is routed exactly like a delivery, it just is not one.
    """
    pattern = spec(feed).get("filename_pattern")
    if not pattern:
        return None
    m = re.fullmatch(pattern, filename)
    if not m:
        return None
    groups = m.groupdict()
    if "business_date" not in groups:
        raise ControlError(
            f"{feed.name}: control filename_pattern has no (?P<business_date>) "
            f"group, so a control file cannot be paired with its delivery")
    from datetime import datetime
    bd = datetime.strptime(groups["business_date"], "%Y%m%d").date()
    return bd, int(groups.get("version") or 1)


# ------------------------------------------------------------------ parsing
def parse(feed, text: str) -> dict[str, str]:
    """Control-file text -> {logical name: value}.

    Logical names are this module's (`rows`, `md5`, `business_date`); the feed
    says which of ITS keys map onto them.
    """
    s = spec(feed)
    if s.get("content_pattern"):
        m = re.search(s["content_pattern"], text, re.S)
        if not m:
            raise ControlError(
                f"{feed.name}: control file did not match content_pattern")
        return {k: v for k, v in m.groupdict().items() if v is not None}

    delim = s.get("delimiter", "|")
    keys = {str(v).strip().lower(): k for k, v in (s.get("keys") or {}).items()}
    if not keys:
        raise ControlError(
            f"{feed.name}: control block declares neither `keys` nor "
            f"`content_pattern`, so nothing can be read out of the file")

    found: dict[str, str] = {}
    for line in text.splitlines():
        if delim not in line:
            continue
        raw_key, _, value = line.partition(delim)
        logical = keys.get(raw_key.strip().lower())
        if logical:
            found[logical] = value.strip()
    return found


# ------------------------------------------------------------------ digests
def digest(content: bytes, algorithm: str = "md5", encoding: str = "hex") -> str:
    h = hashlib.new(algorithm, content)
    if encoding == "base64":
        return base64.b64encode(h.digest()).decode()
    return h.hexdigest()


def _same_digest(declared: str, computed: str) -> bool:
    # Case-insensitive: hex digests are written both ways and neither is wrong.
    return declared.strip().lower() == computed.strip().lower()


# ------------------------------------------------------------------ checking
def check_bytes(feed, content: bytes, control_text: str,
                business_date: date | None = None) -> dict[str, Any]:
    """Everything checkable WITHOUT reading the delivery as a table.

    Run before the ingest branch is created, so a failure leaves nothing to
    clean up.
    """
    s = spec(feed)
    values = parse(feed, control_text)
    report: dict[str, Any] = {"declared": values, "checks": {}}

    if "md5" in values:
        computed = digest(content, s.get("algorithm", "md5"),
                          s.get("digest_encoding", "hex"))
        ok = _same_digest(values["md5"], computed)
        report["checks"]["digest"] = {"declared": values["md5"],
                                      "computed": computed, "ok": ok}
        if not ok:
            raise ControlError(
                f"{feed.name}: {s.get('algorithm', 'md5')} mismatch -- control "
                f"file declares {values['md5']!r}, delivery computes "
                f"{computed!r}. The file is corrupt, truncated, or was "
                f"re-encoded in transit (line-ending translation on a Windows "
                f"hop does exactly this to an otherwise good file).")

    if business_date is not None and "business_date" in values:
        declared = values["business_date"].strip()
        expected = f"{business_date:%Y%m%d}"
        ok = declared == expected
        report["checks"]["business_date"] = {"declared": declared,
                                             "expected": expected, "ok": ok}
        if not ok:
            raise ControlError(
                f"{feed.name}: control file is for business date {declared}, "
                f"the delivery filename says {expected}. One of them is "
                f"mislabelled, or the wrong control file was paired.")
    return report


def check_rows(feed, control_text: str, row_count: int) -> dict[str, Any]:
    """The row count, which needs the delivery parsed and so runs later."""
    s = spec(feed)
    values = parse(feed, control_text)
    if "rows" not in values:
        return {}
    declared = int(re.sub(r"[^0-9]", "", values["rows"]) or 0)
    # Some senders count the header line, some do not. Neither is wrong, and
    # guessing produces an off-by-one that looks like data loss.
    if s.get("rows_include_header") and feed.header:
        declared -= 1
    ok = declared == row_count
    if not ok:
        raise ControlError(
            f"{feed.name}: control file declares {declared} data rows, "
            f"delivery has {row_count}. A short file is a truncated transfer; "
            f"a long one usually means the header or a trailer is being "
            f"counted -- see `rows_include_header`.")
    return {"declared": declared, "actual": row_count, "ok": ok}
