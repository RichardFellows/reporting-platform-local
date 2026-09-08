"""Make a legacy delivery conformant, or refuse it. The inbox's gate.

WHY THIS EXISTS. `landing/` has a CONTRACT: every object in it is correctly
named and classified, so `Feed.parse_filename` answers for all of them and
landing retention can date all of them. That contract is what keeps the rest
of the platform simple -- one shape, one code path, no feed needing the whole
pipeline taught a second way to find a COB date.

Real upstreams do not honour it. A legacy feed sends `positions.csv` every
day with the date on a line inside `positions.ctl`. There are two ways to
absorb that: teach `landing/` and everything downstream to cope with a
dateless constant name, or make the delivery conformant AT THE DOOR. This
module is the second, and the second is much cheaper -- it confines the
irregularity to one stage instead of spreading it across seven modules.

So: the inbox classifies, waits for the control file, verifies what the
control file declares, derives the COB date, and promotes the delivery
into `landing/` under the name the feed's own `filename_pattern` describes,
with a metadata sibling recording what actually arrived.

**The rename cannot produce a name landing will not accept.**
`common/filenames.render_filename` builds it FROM `filename_pattern` and feeds
the result back through `parse_filename` before returning it. That makes the
silent failure -- a renamed file that lands and is never ingested, with the
feed reporting nothing pending forever -- structurally impossible rather than
something a test has to remember.

**THIS MODULE ESTABLISHES IDENTITY, NOT INTEGRITY.** Which source system,
which feed, which COB date, which version -- everything needed to give
the file its correct name and to know its prerequisites are present. It does
NOT check the row count or the checksum. Those are `delivery.control`'s job,
they run at ingest, and they run identically for a legacy delivery and for one
an approved sender wrote straight into `landing/`. A second implementation on
one of the two paths is exactly what would drift.

That split is what makes the two failure modes different, and both are right:

  * an IDENTITY failure -- no feed claims the name, or no COB date can be
    found -- means the file CANNOT BE NAMED, so it cannot land at all. It goes
    to `.rejected/`.
  * an INTEGRITY failure -- wrong row count, wrong checksum -- has nothing to
    do with naming. The delivery LANDS, because `landing/` is the evidence copy
    and "the upstream sent us a truncated file on the 3rd" is precisely what it
    exists to prove, and then the ingest refuses, abandons its Nessie branch
    and leaves `main` untouched, exactly as `expected_min_rows` already does.

There is a third outcome, and it is neither: **an unchanged RESEND is a
no-op.** A name already taken for a COB date used to be versioned to
`_v2` on sight, which turned a retried transfer into a restatement the
upstream never made -- see `DuplicateDelivery`. Sameness is decided on the
bytes (md5), not on the name, and only against deliveries already landed for
that same date.

**THE CONTROL FILE IS PROMOTED, NOT CONSUMED.** The delivery is the data file
and its control file together, so both are renamed and written to `landing/`.
That is what makes the process identical from landing onwards: whichever way a
delivery arrived, `landing/` holds the same pair and `delivery.control` reads
it the same way.

WHAT THIS DOES NOT DO: decide when a file is complete. That is the inbox
watcher's stability check (`inbox.STABLE_POLLS`), which is about a file still
being written. This module assumes the bytes it is handed are final.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Callable

from reporting_platform.common.context import Feed
from reporting_platform.common.filenames import (
    FilenameError, literal_from_regex, render_filename,
)

METADATA_VERSION = 1

# The suffix that makes a metadata object recognisable to landing retention
# without a lookup. See `retention/landing.py` -- an object named
# `<delivery>.meta.json` carries its own COB date in the `<delivery>`
# part, so it expires on its own name exactly when its delivery does.
METADATA_SUFFIX = ".meta.json"


class NotReady(Exception):
    """The delivery is here but incomplete -- typically its control file has
    not arrived. Not a failure: nothing is wrong, the sender is not done.

    Distinct from `ConformanceError` for the same reason
    `normalize.NotReady` is distinct from a normalization failure: one clears
    on its own and one never will, and collapsing them into one code path
    means either warning forever about an ordinary wait or staying silent
    about a real break.
    """


class ConformanceError(Exception):
    """The delivery arrived and is wrong -- a declared count that does not
    match, a checksum that does not match, a control file that does not parse.

    Will not clear on its own. The delivery is rejected rather than held.
    """


class DuplicateDelivery(Exception):
    """These exact bytes are already landed for this COB date.

    NOT a `ConformanceError`, deliberately: nothing is wrong with the file and
    it must not go to `.rejected/`. An upstream resending an unchanged file is
    ordinary -- a retried transfer, a re-run of the sender's own job -- and the
    right answer is to do nothing at all.

    Doing nothing is not what used to happen. `_free_name` versions a name
    that is taken, so a byte-identical resend landed as `_v2`, and `_v2` is a
    `_source_file` the raw table has never seen: `already_ingested` does not
    match it, `next_file_version` gives it MAX+1, and `dedupe_rank` -- which
    ranks `_file_version DESC` -- lets it SUPERSEDE the delivery it is a copy
    of. Identical rows, so nothing visibly changes; a restatement in the
    history that never happened, which is the part that matters.

    Carries `landing_filename`: the delivery already holding these bytes.
    """

    def __init__(self, message: str, landing_filename: str) -> None:
        super().__init__(message)
        self.landing_filename = landing_filename


def is_source_control_file(feed: Feed, filename: str) -> bool:
    """Whether `filename` is SHAPED like this feed's inbound control file.

    A control file matches no `filename_pattern` and no `source_pattern` -- it
    names no delivery, it says something about one -- so without this the
    inbox would reject it to `.rejected/` and the delivery it belongs to would
    wait forever on a file that can never arrive.
    """
    pattern = feed.source_control_pattern()
    if not pattern:
        return False
    return re.fullmatch(pattern.replace("{stem}", "(?P<stem>.+)"),
                        filename) is not None


def control_filename_for(feed: Feed, data_filename: str) -> str | None:
    """The control filename this delivery needs, as a REGEX to match siblings.

    `pattern` is a regex template, not a literal one: `'{stem}\\.ctl'` with
    `{stem}` filled in is `'positions\\.ctl'`, where the backslash escapes the
    dot for the regex and is not a character in any real filename. Treating it
    as a plain string template -- looking for a file literally containing a
    backslash -- is the mistake this docstring exists to prevent.
    """
    pattern = feed.source_control_pattern()
    if not pattern:
        return None
    stem = _stem(data_filename)
    return pattern.format(stem=re.escape(stem))


def _stem(filename: str) -> str:
    return filename[:filename.rindex(".")] if "." in filename else filename


def find_control(feed: Feed, data_filename: str,
                 available: list[str]) -> str | None:
    """This delivery's control file among `available`, or None if not yet here."""
    regex = control_filename_for(feed, data_filename)
    if regex is None:
        return None
    compiled = re.compile(regex)
    return next((n for n in sorted(available)
                 if n != data_filename and compiled.fullmatch(n)), None)


# ----------------------------------------------------------- what it declares
def read_control(feed: Feed, text: str, control_filename: str) -> dict[str, Any]:
    """Everything the control file declares, as configured. Nothing inferred.

    IDENTITY ONLY -- COB date and version. The row count and checksum
    the same file may also declare are read on the LANDING side, by
    `delivery.control`, so that they are checked once for every delivery
    rather than twice for one of the two arrival paths.

    A key absent from `arrival.control` is absent from the result: "the sender
    did not say" and "the sender said zero" are different facts.
    """
    control = (feed.arrival or {}).get("control") or {}
    out: dict[str, Any] = {}

    for key, group in (("cob_date", "cob_date"),
                       ("version", "version")):
        pattern = control.get(key)
        if pattern is None:
            continue
        m = re.search(pattern, text)
        if not m:
            raise ConformanceError(
                f"{feed.name}: control file {control_filename} does not match "
                f"`arrival.control.{key}` {pattern!r}. The control file "
                f"arrived and does not say what it was configured to say -- a "
                f"format change upstream, not a timing problem, so it will "
                f"not clear on its own.")
        out[key] = m.group(group)

    if "cob_date" in out:
        raw = out["cob_date"]
        try:
            out["cob_date"] = datetime.strptime(raw, "%Y%m%d").date()
        except ValueError as exc:
            raise ConformanceError(
                f"{feed.name}: control file {control_filename} declares "
                f"COB date {raw!r}, which is not yyyyMMdd: {exc}. The "
                f"regex matched, so `arrival.control.cob_date` is "
                f"capturing the wrong part of the line.") from exc
    if "version" in out:
        out["version"] = int(out["version"])
    return out


def count_rows(feed: Feed, content: bytes) -> int:
    """Data rows in the delivery, excluding the header.

    Parsed with the feed's own dialect rather than counting newlines. A quoted
    field containing a newline is one row and two lines, and a naive count
    would report a correct file as short -- rejecting a good delivery at the
    door, which is worse than the truncation the check exists to catch.
    """
    text = content.decode(feed.file_encoding, errors="replace")
    reader = csv.reader(io.StringIO(text), delimiter=feed.delimiter,
                        quotechar=feed.quote_char)
    rows = sum(1 for row in reader if row)
    return max(rows - 1, 0) if feed.header else rows


def observe(feed: Feed, content: bytes) -> dict[str, Any]:
    """What arrived, measured. RECORDED, never checked.

    Deliberately not `verify`. An earlier draft compared these against what
    the control file declared and rejected a mismatch at the door; that put a
    second implementation of the row-count check on one of the two arrival
    paths, and left an approved sender writing straight to landing with WEAKER
    checking than a legacy one -- the trusted path being the less verified one,
    which is backwards. The comparison now happens once, at ingest, for
    everybody (`delivery.control`, `ingest_feed.py`).

    Measured anyway, because it is provenance: the size, hash and row count of
    the object as the inbox received it, written into the metadata sibling. If
    the ingest later disputes the row count, this says whether the file
    changed after arrival or arrived wrong.
    """
    return {"bytes": len(content),
            "md5": hashlib.md5(content).hexdigest(),
            "row_count": count_rows(feed, content)}


# ------------------------------------------------------------- the promotion
def landing_control_filename(feed: Feed, landing_filename: str) -> str | None:
    """What the promoted control file must be called in `landing/`.

    Derived from `delivery.control.pattern` with the LANDING stem -- not from
    `arrival.control.pattern`, and not by keeping the name it had in the
    inbox. `delivery.control` is what looks for it once it is there, so its
    pattern is the one that has to be satisfied; a control file promoted under
    any other name leaves the delivery waiting in landing forever on a file
    sitting right beside it.

    Checked against that same regex before being returned, the way
    `render_filename` checks the data file's name, so the two cannot disagree.
    """
    pattern = ((feed.delivery or {}).get("control") or {}).get("pattern")
    if not pattern:
        return None
    stem = _stem(landing_filename)
    regex = pattern.format(stem=re.escape(stem))
    name = literal_from_regex(regex, {}, what="the landing control filename")
    if not re.fullmatch(regex, name):
        raise ConformanceError(
            f"{feed.name}: promoted control filename {name!r} does not match "
            f"`delivery.control.pattern` ({pattern}) with stem {stem!r}. It "
            f"would land beside the delivery and never be found.")
    return name


def conform(feed: Feed, data_filename: str, content: bytes, *,
            control_filename: str | None = None,
            control_text: str | None = None,
            received_at: datetime | None = None,
            taken: set[str] | None = None,
            landed_md5: Callable[[str], str | None] | None = None
            ) -> dict[str, Any]:
    """One inbound delivery -> the landing names and metadata it should become.

    Pure: computes and returns, writing nothing. The caller does the uploads,
    which is what lets this be tested without S3 or a watcher.

    `taken` is the set of filenames already in this feed's landing prefix, and
    it is how a RE-DELIVERY gets its version. A corrected file for a COB
    date already landed must not overwrite the first one -- `landing/` is the
    evidence copy, and the original is the evidence of what was originally
    ingested. So the name is rendered with no version, and if that is taken,
    with `_v2`, `_v3` and so on until it is not. Pass None to skip that (the
    caller has no listing), which renders the unversioned name.

    `landed_md5` answers "what is the md5 of the delivery already landed under
    this name", or None if that is not known. It is what tells a CORRECTED
    file apart from an unchanged RESEND, which `taken` alone cannot: see
    `_free_name` and `DuplicateDelivery`.

    Raises `NotReady` if the control file is needed and absent,
    `DuplicateDelivery` if these exact bytes are already landed for this date,
    and `ConformanceError` for an IDENTITY failure -- a delivery that cannot
    be named. Content is not checked here at all; see the module docstring.
    """
    if not feed.needs_conforming:
        raise ValueError(
            f"{feed.name} has no `arrival:` block, so its upstream writes "
            f"conformant names to landing/ directly and nothing here applies.")

    control = (feed.arrival or {}).get("control") or {}
    declared: dict[str, Any] = {}
    if control:
        if control_filename is None or control_text is None:
            raise NotReady(
                f"{feed.name}: {data_filename} is waiting on a control file "
                f"matching {control['pattern']!r} (stem {_stem(data_filename)!r}) "
                f"in the inbox. Not a failure -- a late feed, not a failed one.")
        declared = read_control(feed, control_text, control_filename)

    cob_date = declared.get("cob_date") \
        or feed.source_cob_date(data_filename)
    if cob_date is None:
        # resolve_arrival_config guarantees one source exists, so reaching
        # here means the configured source produced nothing -- which the
        # readers above would already have raised on. Kept as a guard rather
        # than an assert because it is the one value nothing downstream can
        # do without, and without it the file cannot be named at all.
        raise ConformanceError(
            f"{feed.name}: no COB date for {data_filename} -- neither "
            f"`arrival.source_pattern` nor the control file yielded one.")

    # Measured BEFORE the name is chosen, because the name now depends on it:
    # a candidate already landed with this md5 is the same delivery arriving
    # twice, not the next version of it.
    observed = observe(feed, content)
    try:
        landing_filename = _free_name(feed, cob_date,
                                      declared.get("version"), taken,
                                      md5=observed["md5"], landed_md5=landed_md5)
        control_landing_filename = landing_control_filename(feed, landing_filename)
    except FilenameError as exc:
        raise ConformanceError(
            f"{feed.name}: cannot build the landing name for "
            f"{data_filename}: {exc}") from exc

    now = datetime.now(timezone.utc)
    metadata = {
        "metadata_version": METADATA_VERSION,
        "feed": feed.name,
        "source_system": feed.source_system,
        "cob_date": cob_date.isoformat(),
        "landing_filename": landing_filename,
        "landing_control_filename": control_landing_filename,
        # WHAT ACTUALLY ARRIVED. The objects in landing are byte-identical to
        # what the upstream sent, but their NAMES are the platform's, so the
        # originals survive only if they are written down here.
        "source_filename": data_filename,
        "source_control_filename": control_filename,
        "received_at": (received_at or now).astimezone(timezone.utc).isoformat(),
        "promoted_at": now.isoformat(),
        "promoted_by": "inbox",
        # Measured at the door, not checked here -- see `observe`. If the
        # ingest later disputes the row count, this says whether the file
        # changed after arrival or arrived wrong.
        **observed,
        "declared": {k: (v.isoformat() if isinstance(v, date) else v)
                     for k, v in declared.items()},
        # NO verbatim copy of the control file. An earlier draft embedded one,
        # correctly, because the gate CONSUMED the control file and it reached
        # landing no other way. It is promoted now -- byte-identical, under
        # `landing_control_filename` -- so a copy here would be a second
        # version of the same bytes with nothing keeping them in step.
    }
    return {
        "landing_filename": landing_filename,
        "control_landing_filename": control_landing_filename,
        "metadata_filename": landing_filename + METADATA_SUFFIX,
        "cob_date": cob_date,
        "metadata": metadata,
    }


def _free_name(feed: Feed, cob_date: date, version: int | None,
               taken: set[str] | None, *, md5: str | None = None,
               landed_md5: Callable[[str], str | None] | None = None) -> str:
    """The first landing name for this date that is not already used.

    A version the CONTROL FILE declared wins outright -- the sender said which
    restatement this is, and second-guessing that would be worse than
    obeying it. Otherwise the unversioned name is tried first, so an ordinary
    single delivery is `FEED_20260801.csv` and not `FEED_20260801_v1.csv`.

    A NAME BEING TAKEN IS NOT ENOUGH TO VERSION IT. `taken` says a delivery
    for this date landed; it does not say whether it is a DIFFERENT delivery.
    Every candidate already in `taken` is compared against `md5` first, and an
    equal one raises `DuplicateDelivery` rather than stepping past it -- see
    that exception for what an undetected copy does to the raw history.

    `landed_md5` maps a landing filename to the md5 recorded for it, or None
    when there is no record. **None means unknown, and unknown versions.**
    That is the fail-open direction on purpose: `landing/` is the evidence
    copy, so an unnecessary `_v2` costs an object, while suppressing a real
    restatement loses the evidence that it was ever sent. Both arguments
    absent (a caller with no listing) skips the comparison entirely, which is
    the behaviour every caller had before this existed.
    """
    def _refuse_if_identical(name: str) -> None:
        if md5 is None or landed_md5 is None:
            return
        if landed_md5(name) == md5:
            raise DuplicateDelivery(
                f"{feed.name}: {name} already holds these exact bytes "
                f"(md5 {md5}) for {cob_date.isoformat()}. An unchanged "
                f"resend is not a restatement -- nothing is landed and "
                f"nothing is ingested.", name)

    if version is not None:
        name = render_filename(feed, cob_date, version)
        if taken and name in taken:
            _refuse_if_identical(name)
        return name
    name = render_filename(feed, cob_date)
    if not taken or name not in taken:
        return name
    _refuse_if_identical(name)
    for n in range(2, 100):
        candidate = render_filename(feed, cob_date, n)
        if candidate not in taken:
            return candidate
        _refuse_if_identical(candidate)
    raise ConformanceError(
        f"{feed.name}: 99 versions already landed for "
        f"{cob_date.isoformat()}. Something is re-delivering in a loop.")


def is_archive(feed: Feed) -> bool:
    return bool((feed.arrival or {}).get("archive"))


def _safe_member_name(name: str) -> str:
    """The member's own name, with any directory structure refused.

    A zip member may name `../../etc/passwd` or an absolute path; extracting
    one by joining it onto a prefix is the standard archive traversal bug.
    Here it would write outside the feed's landing prefix -- into another
    feed's evidence. Only a plain filename is accepted, so the check cannot be
    defeated by encoding: anything containing a separator is rejected outright
    rather than normalised into something that looks safe.
    """
    if name.endswith("/"):
        raise ConformanceError(f"archive member {name!r} is a directory")
    if "/" in name or "\\" in name or name in ("", ".", ".."):
        raise ConformanceError(
            f"archive member {name!r} contains a path. Only flat members are "
            f"extracted -- a member naming a directory could be written "
            f"outside this feed's landing prefix.")
    return name


def unpack(feed: Feed, container_filename: str,
           content: bytes) -> list[tuple[str, bytes]]:
    """A container -> the (name, bytes) of every member this feed claims.

    A ZIP IS NOT A DELIVERY, it is a transport wrapper. Unpacking it turns one
    inbox file into N inbox files, each of which then follows the ordinary
    single-file path -- so `landing/` never holds an archive and nothing
    downstream needs a reader for one.

    Members not matching `member_pattern` are SKIPPED, not an error: a zip
    routinely carries a checksum or a manifest alongside the data. A zip
    matching NONE is an error, because an archive that unpacks to nothing is a
    delivery problem, not an empty day, and landing zero rows would pass
    `expected_min_rows` only by accident.
    """
    import io
    import zipfile

    pattern = re.compile(feed.arrival["archive"]["member_pattern"])
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as exc:
        raise ConformanceError(
            f"{feed.name}: {container_filename} is not a readable zip: {exc}"
        ) from exc

    out: list[tuple[str, bytes]] = []
    with zf:
        # Sorted so the landing order does not depend on how the sender
        # happened to build the archive.
        for info in sorted(zf.infolist(), key=lambda i: i.filename):
            if info.is_dir() or not pattern.fullmatch(info.filename):
                continue
            out.append((_safe_member_name(info.filename),
                        zf.read(info.filename)))
    if not out:
        raise ConformanceError(
            f"{feed.name}: {container_filename} holds no member matching "
            f"{feed.arrival['archive']['member_pattern']!r}. An archive that "
            f"unpacks to nothing is a delivery problem, not an empty day.")
    return out


def member_cob_date(feed: Feed, member_filename: str) -> date:
    """The COB date a member carries in its own name."""
    pattern = feed.arrival["archive"]["member_pattern"]
    m = re.fullmatch(pattern, member_filename)
    if not m:
        raise ConformanceError(
            f"{feed.name}: member {member_filename!r} does not match "
            f"{pattern!r}")
    raw = m.group("cob_date")
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:
        raise ConformanceError(
            f"{feed.name}: member {member_filename!r} declares COB date "
            f"{raw!r}, which is not yyyyMMdd: {exc}") from exc


def conform_member(feed: Feed, container_filename: str, container_content: bytes,
                   member_filename: str, member_content: bytes, *,
                   received_at: datetime | None = None,
                   taken: set[str] | None = None,
                   landed_md5: Callable[[str], str | None] | None = None
                   ) -> dict[str, Any]:
    """One archive member -> the landing name and metadata it should become.

    The same result shape `conform()` returns, so the caller uploads both the
    same way. The CONTAINER is recorded but never landed: the members hold the
    rows the upstream sent, byte for byte, and the metadata carries the
    container's name and checksum so what arrived is still provable without
    keeping an object nothing reads.

    Raises `DuplicateDelivery` on the same terms `conform()` does. A container
    resent whole is the commonest way this happens -- the members inside it
    are byte-identical, and versioning them would restate every COB date
    the archive covers at once.
    """
    cob_date = member_cob_date(feed, member_filename)
    observed = observe(feed, member_content)
    try:
        landing_filename = _free_name(feed, cob_date, None, taken,
                                      md5=observed["md5"], landed_md5=landed_md5)
    except FilenameError as exc:
        raise ConformanceError(
            f"{feed.name}: cannot build the landing name for member "
            f"{member_filename}: {exc}") from exc

    now = datetime.now(timezone.utc)
    return {
        "landing_filename": landing_filename,
        "control_landing_filename": None,
        "metadata_filename": landing_filename + METADATA_SUFFIX,
        "cob_date": cob_date,
        "metadata": {
            "metadata_version": METADATA_VERSION,
            "feed": feed.name,
            "source_system": feed.source_system,
            "cob_date": cob_date.isoformat(),
            "landing_filename": landing_filename,
            "source_filename": member_filename,
            # The container is NOT landed, so this is the only record that it
            # existed, what it was called and that these bytes came out of it.
            "source_container": container_filename,
            "source_container_md5": hashlib.md5(container_content).hexdigest(),
            "source_container_bytes": len(container_content),
            "received_at": (received_at or now).astimezone(timezone.utc).isoformat(),
            "promoted_at": now.isoformat(),
            "promoted_by": "inbox",
            **observed,
            "declared": {},
        },
    }


def metadata_bytes(metadata: dict[str, Any]) -> bytes:
    return json.dumps(metadata, indent=2, sort_keys=True).encode("utf-8")


def is_metadata_key(key: str) -> bool:
    return key.endswith(METADATA_SUFFIX)


def delivery_of_metadata(filename: str) -> str:
    """`X.meta.json` -> `X`. What lets landing retention date a metadata
    object from its own name, with no lookup of the delivery beside it."""
    return filename[:-len(METADATA_SUFFIX)]
