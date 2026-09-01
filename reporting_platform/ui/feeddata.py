"""Sample data: what is in seed/, what is in landing, and moving between them.

`seed/` stands in for the upstream. `scripts/land_feeds.py` is the CLI form of
the same move; this module is what the console calls, and both end at
`arrival.put_landing` so there is one implementation of "a file arrives".

WHAT THIS MODULE WILL NOT DO IS START A SPARK SESSION. `find_pending` reads
the raw table's own `_source_file` values to decide what is outstanding, which
means a Spark application on the shared cluster holding two of the worker's
six cores for the length of the call. That is the right cost for an ingest and
entirely the wrong one for rendering a page. So the listing here reports what
is LANDED and whether each object matches the feed's pattern -- both answered
from S3 alone -- and the true pending check is a button the user presses,
labelled with what it costs.
"""
from __future__ import annotations

import csv
import io
import os
import re
from pathlib import Path
from typing import Any

from reporting_platform.common.context import Feed
from reporting_platform.ingest.arrival import find_pending, list_landing, put_landing

SEED_DIR = Path(os.environ.get("REPORTING_SEED_DIR", "/opt/platform/seed"))

# Refuse anything that is not a plain filename. The name goes into a path
# under seed/<feed>/ and then into an S3 key, and "../" in either is a way out
# of both.
SAFE_FILENAME = re.compile(r"^[A-Za-z0-9._-]+$")

MAX_UPLOAD_BYTES = 64 * 1024 * 1024


class DataError(ValueError):
    pass


def seed_dir(feed: Feed) -> Path:
    return SEED_DIR / feed.name


def list_seed(feed: Feed) -> list[dict[str, Any]]:
    """Every CSV under seed/<feed>/, newest business date first.

    `matches` is the load-bearing column. A file whose name does not match the
    feed's `filename_pattern` can be landed perfectly happily and will then
    never be ingested, because `find_pending` filters on exactly that match --
    the failure is a feed that reports nothing pending, forever, with no error
    anywhere. Showing it here is the cheapest place to catch it.
    """
    d = seed_dir(feed)
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("*.csv")):
        parsed = feed.parse_filename(p.name)
        out.append({
            "filename": p.name,
            "size": p.stat().st_size,
            "matches": parsed is not None,
            "business_date": parsed[0].isoformat() if parsed else None,
            "version": parsed[1] if parsed else None,
        })
    return sorted(out, key=lambda r: (r["business_date"] or "", r["filename"]),
                  reverse=True)


def list_landed(feed: Feed) -> list[dict[str, Any]]:
    """Objects under the feed's landing prefix. S3 only, no Spark."""
    out = []
    for key in list_landing(feed):
        name = key.rsplit("/", 1)[-1]
        parsed = feed.parse_filename(name)
        out.append({
            "key": key,
            "filename": name,
            "matches": parsed is not None,
            "business_date": parsed[0].isoformat() if parsed else None,
            "version": parsed[1] if parsed else None,
        })
    return sorted(out, key=lambda r: (r["business_date"] or "", r["filename"]),
                  reverse=True)


def pending(feed: Feed) -> list[str]:
    """The real outstanding set. Starts a Spark application -- see the header.

    Two filters, both in `arrival.find_pending`: not already in the raw table,
    and inside the retention keep-set. The second is why a landed file can be
    correct, unmatched by anything, and still not pending: a delivery for a
    date retention has already expired is not new.
    """
    return find_pending(feed)


def preview(feed: Feed, filename: str, rows: int = 5) -> dict[str, Any]:
    """First few rows of a seed file, plus how its header compares to the feed.

    The comparison is the point. `columns` in feeds.yml is a DECLARED
    contract, not something discovered from the file: a column in the file but
    not declared lands in `_extra_columns`, and a column declared but absent
    lands as NULL. Both are drift, both are reported at ingest, and both are
    much cheaper to see here.
    """
    path = _seed_path(feed, filename)
    with path.open(encoding=feed.file_encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=feed.delimiter, quotechar=feed.quote_char)
        header = next(reader, [])
        sample = [row for _, row in zip(range(rows), reader)]
    return {"header": header, "rows": sample, **compare_header(feed, header)}


def compare_header(feed: Feed, header: list[str]) -> dict[str, list[str]]:
    got = [h.strip() for h in header]
    return {
        # Compared in SOURCE names: this is a statement about the delivered
        # file, and the platform name a column would become is not something
        # the upstream can act on.
        # See docs/DECISIONS.md#source-column-names
        "missing_columns": [c for c in feed.file_header if c not in got],
        "extra_columns": [c for c in got if c not in feed.file_header],
    }


def header_of(content: bytes, feed: Feed) -> list[str]:
    text = content.decode(feed.file_encoding, errors="replace")
    reader = csv.reader(io.StringIO(text), delimiter=feed.delimiter,
                        quotechar=feed.quote_char)
    return [h.strip() for h in next(reader, [])]


def columns_from_csv(content: bytes, encoding: str = "utf-8",
                     delimiter: str = ",") -> list[str]:
    """Header of an uploaded CSV, for pre-filling the column list on a new feed.

    Used before the feed exists, so it cannot go through a Feed object.
    """
    text = content.decode(encoding, errors="replace")
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    return [h.strip() for h in next(reader, [])]


def _seed_path(feed: Feed, filename: str) -> Path:
    if not SAFE_FILENAME.match(filename):
        raise DataError(f"unusable filename: {filename!r}")
    path = seed_dir(feed) / filename
    if not path.exists():
        raise DataError(f"no such file under seed/{feed.name}: {filename}")
    return path


def save_to_seed(feed: Feed, filename: str, content: bytes) -> dict[str, Any]:
    """Write an uploaded CSV into seed/<feed>/.

    The filename is checked against the feed's pattern and REFUSED if it does
    not match, rather than saved with a warning. A non-matching file in seed/
    is invisible work: it lands, it is never ingested, and nothing reports it.
    The caller gets told which part did not match so the name -- or the
    pattern -- can be fixed.
    """
    if not SAFE_FILENAME.match(filename):
        raise DataError(f"unusable filename: {filename!r} -- letters, digits, "
                        f"dot, dash and underscore only")
    if len(content) > MAX_UPLOAD_BYTES:
        raise DataError(f"file is larger than the {MAX_UPLOAD_BYTES // 1024 // 1024} MB limit")
    if feed.parse_filename(filename) is None:
        raise DataError(
            f"{filename!r} does not match this feed's filename_pattern "
            f"({feed.filename_pattern}). It would land and then never be "
            f"ingested, because arrival matching uses that pattern. Rename the "
            f"file, or change the pattern on the feed.")

    d = seed_dir(feed)
    d.mkdir(parents=True, exist_ok=True)
    path = d / filename
    existed = path.exists()
    path.write_bytes(content)

    header = header_of(content, feed)
    return {"filename": filename, "path": str(path), "replaced": existed,
            "header": header, **compare_header(feed, header)}


def land(feed: Feed, filenames: list[str] | None = None) -> list[str]:
    """Copy seed files into the landing prefix. Oldest business date first.

    Order matters for a first load: the prepared layer's `relationships` tests
    compare against whatever reference data has arrived, so landing a trade
    file before the counterparty file for the same date makes the first build
    fail on a reference that simply has not been delivered yet.
    """
    files = list_seed(feed)
    if filenames is not None:
        wanted = set(filenames)
        files = [f for f in files if f["filename"] in wanted]
        unknown = wanted - {f["filename"] for f in files}
        if unknown:
            raise DataError(f"not in seed/{feed.name}: {', '.join(sorted(unknown))}")
    ordered = sorted(files, key=lambda f: (f["business_date"] or "", f["filename"]))

    keys = []
    for f in ordered:
        if not f["matches"]:
            continue
        keys.append(put_landing(feed, str(seed_dir(feed) / f["filename"])))
    return keys
