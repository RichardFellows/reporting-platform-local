"""Make a legacy delivery conformant, or refuse it. The inbox's gate.

WHY THIS EXISTS. `landing/` has a CONTRACT: every object in it is correctly
named and classified, so `Feed.parse_filename` answers for all of them and
landing retention can date all of them. That contract is what keeps the rest of
the platform simple -- one shape, one code path.

Real upstreams do not honour it: a legacy feed sends `positions.csv` every day
with the date on a line inside `positions.ctl`. Either everything downstream
learns to cope with a dateless constant name, or the delivery is made
conformant AT THE DOOR. This module is the second, and it is much cheaper --
the irregularity stays in one stage instead of spreading across seven modules.

So: the inbox classifies, waits for the control file, verifies what it
declares, derives the COB date, and promotes the delivery into `landing/`
under the name the feed's own `filename_pattern` describes.

**The rename cannot produce a name landing will not accept.**
`common/filenames.render_filename` builds it FROM `filename_pattern` and feeds
the result back through `parse_filename`, so the silent failure -- a renamed
file that lands and is never ingested -- is structurally impossible.

**THIS MODULE ESTABLISHES IDENTITY, NOT INTEGRITY.** Which source system,
feed, COB date and version -- everything needed to name the file. It does NOT
check the row count or checksum: those are `delivery.control`'s job, run at
ingest, identically for a legacy delivery and for one an approved sender wrote
straight into `landing/`.

That split makes the two failure modes different, and both are right:

  * an IDENTITY failure -- no feed claims the name, or no COB date can be
    found -- means the file CANNOT BE NAMED, so it cannot land. It goes to
    `.rejected/`.
  * an INTEGRITY failure -- wrong row count or checksum -- has nothing to do
    with naming. The delivery LANDS, because `landing/` is the evidence copy
    and a truncated file is precisely what it exists to prove, and then the
    ingest refuses and leaves `main` untouched.

There is a third outcome: **an unchanged RESEND is a no-op**
(docs/DECISIONS.md#an-unchanged-resend-is-a-no-op). A name already
taken used to be versioned to `_v2` on sight, turning a retried transfer into
a restatement the upstream never made. Sameness is decided on the bytes (md5),
only against deliveries already landed for that date.

**THE CONTROL FILE IS PROMOTED, NOT CONSUMED.** The delivery is the data file
and its control file together, so both are renamed into `landing/`. That is
what makes the process identical from landing onwards.

WHAT THIS DOES NOT DO: decide when a file is complete. That is the inbox
watcher's stability check (`inbox.STABLE_POLLS`).
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable

from reporting_platform.common.context import Feed
from reporting_platform.common.filenames import (
    FilenameError, claims_control_file, literal_from_regex, render_filename,
)
from reporting_platform.ingest import control as control_mod

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
    """Whether `filename` is this feed's inbound control file, AT THE DOOR.

    A control file matches no `filename_pattern` and no `source_pattern` -- it
    names no delivery, it says something about one -- so without this the
    inbox would reject it to `.rejected/` and the delivery it belongs to would
    wait forever on a file that can never arrive.

    THE STEM IS PART OF THE QUESTION. `claims_control_file` requires the stem
    to be one this feed's own source names can have, so two feeds that both
    send `.ctl` no longer each claim every control file at the door. An
    ARCHIVE feed claims none here: its control files are packed inside the
    container -- see `is_member_control_file`, which is the container's own
    namespace and stays permissive because inside a container there is only
    one feed to be.
    """
    return claims_control_file(feed, filename, "arrival")


def is_member_control_file(feed: Feed, member_filename: str) -> bool:
    """Whether an archive MEMBER is a control file rather than a delivery.

    Deliberately the loose test -- the stem shape is not required, only the
    control pattern's own shape. Inside a container there is no other feed to
    confuse this with: the container has already been attributed, and the
    member names are the sender's, not the platform's. What this must catch is
    a `member_pattern` permissive enough to claim the control files as data,
    which would ingest a control file's own text as rows.
    """
    pattern = feed.source_control_pattern()
    if not pattern:
        return False
    return re.fullmatch(pattern.replace("{stem}", "(?P<stem>.+)"),
                        member_filename) is not None


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

    HOW the file is read is `arrival.control.format`'s answer and
    `ingest/control.py` is the only implementation of it -- a regex over the
    text, a column of a delimited table, and one day something else. What is
    read out of it, and what a value has to be for this gate to name a file
    after it, stays here.
    """
    control_cfg = (feed.arrival or {}).get("control") or {}
    try:
        out: dict[str, Any] = control_mod.read(
            control_cfg, text, fields=("cob_date", "version"),
            feed_name=feed.name, filename=control_filename,
            block="arrival.control")
    except control_mod.ControlParseError as exc:
        # An IDENTITY failure: the delivery cannot be NAMED, so it cannot
        # land. Re-raised as the exception the inbox routes to `.rejected/`.
        raise ConformanceError(str(exc)) from exc

    if "cob_date" in out:
        raw = out["cob_date"]
        try:
            out["cob_date"] = datetime.strptime(raw, "%Y%m%d").date()
        except ValueError as exc:
            raise ConformanceError(
                f"{feed.name}: control file {control_filename} declares "
                f"COB date {raw!r}, which is not yyyyMMdd: {exc}. The file "
                f"was read, so `arrival.control.cob_date` is pointing at the "
                f"wrong part of it.") from exc
    if "version" in out:
        try:
            out["version"] = int(out["version"])
        except ValueError as exc:
            raise ConformanceError(
                f"{feed.name}: control file {control_filename} declares "
                f"version {out['version']!r}, which is not a number -- and "
                f"the landing name is built out of it.") from exc
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
    is how a RE-DELIVERY gets its version: a corrected file for a date already
    landed must not overwrite the original, so the name is rendered
    unversioned, then `_v2`, `_v3` until free. Pass None to skip that.

    `landed_md5` answers "what is the md5 of the delivery already landed under
    this name", or None if unknown. It is what tells a CORRECTED file apart
    from an unchanged RESEND, which `taken` alone cannot.

    Raises `NotReady` if the control file is needed and absent,
    `DuplicateDelivery` if these exact bytes are already landed for this date,
    and `ConformanceError` for an IDENTITY failure. Content is not checked
    here at all.
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
        # resolve_arrival_config guarantees one source exists, so reaching here
        # means the configured source produced nothing -- which the readers
        # above would already have raised on. A guard rather than an assert
        # because without it the file cannot be named at all.
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
        # correctly, because the gate CONSUMED it and it reached landing no
        # other way. It is promoted now -- byte-identical, under
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
    restatement this is. Otherwise the unversioned name is tried first, so an
    ordinary single delivery is `FEED_20260801.csv`.

    A NAME BEING TAKEN IS NOT ENOUGH TO VERSION IT. `taken` says a delivery
    for this date landed; not whether it is a DIFFERENT delivery. Every
    candidate in `taken` is compared against `md5` first, and an equal one
    raises `DuplicateDelivery` rather than stepping past it.

    `landed_md5` maps a landing filename to its recorded md5, or None when
    there is no record. **None means unknown, and unknown versions** -- the
    fail-open direction on purpose: an unnecessary `_v2` costs an object,
    while suppressing a real restatement loses the evidence it was sent. Both
    arguments absent skips the comparison entirely.
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
    """Whether the gate unpacks this feed's deliveries. One rule, in
    `arrival_shape`, so a predicate and a dispatch cannot disagree."""
    return arrival_shape(feed) == "archive"


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


def archive_members(feed: Feed, container_filename: str,
                    content: bytes) -> list[tuple[str, bytes]]:
    """Every member of the container, claimed or not, sorted by name.

    Separate from `unpack` because the two questions are different: this is
    what the container HOLDS, and `unpack` is which of those are DELIVERIES.
    A member that is neither -- a checksum file, a manifest, and now a
    member's control file -- has to be readable without being landed.
    """
    import io
    import zipfile

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
            if info.is_dir():
                continue
            out.append((_safe_member_name(info.filename),
                        zf.read(info.filename)))
    return out


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

    A MEMBER SHAPED LIKE THIS FEED'S CONTROL FILE IS NEVER A DELIVERY, even
    when `member_pattern` would claim it. A permissive pattern (`.*\\.csv`) and
    a control file the sender also writes as CSV is an ordinary combination,
    and landing `POS_A.ctl.csv` as a delivery would ingest the control file's
    own text as rows. The pattern says which members are data; the control
    block says which are not, and the second wins.
    """
    pattern = re.compile(feed.arrival["archive"]["member_pattern"])
    out = [(name, body)
           for name, body in archive_members(feed, container_filename, content)
           if pattern.fullmatch(name) and not is_member_control_file(feed, name)]
    if not out:
        raise ConformanceError(
            f"{feed.name}: {container_filename} holds no member matching "
            f"{feed.arrival['archive']['member_pattern']!r}. An archive that "
            f"unpacks to nothing is a delivery problem, not an empty day.")
    return out


def member_cob_date(feed: Feed, member_filename: str) -> date | None:
    """The COB date a member carries in its own name, or None if it carries
    none -- which is legal when `arrival.control` reads one out of the
    member's control file instead. `resolve_arrival_config` guarantees exactly
    one of the two exists."""
    pattern = feed.arrival["archive"]["member_pattern"]
    m = re.fullmatch(pattern, member_filename)
    if not m:
        raise ConformanceError(
            f"{feed.name}: member {member_filename!r} does not match "
            f"{pattern!r}")
    if "cob_date" not in m.groupdict():
        return None
    raw = m.group("cob_date")
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:
        raise ConformanceError(
            f"{feed.name}: member {member_filename!r} declares COB date "
            f"{raw!r}, which is not yyyyMMdd: {exc}") from exc


def conform_member(feed: Feed, container_filename: str, container_content: bytes,
                   member_filename: str, member_content: bytes, *,
                   control_filename: str | None = None,
                   control_text: str | None = None,
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

    A MEMBER MAY HAVE ITS OWN CONTROL FILE, packed in the same container, and
    then it is a delivery in every sense the plain path means: the control
    file names the COB date, both are promoted into `landing/` renamed, and
    `delivery.control` verifies the pair there. That is what makes a zip of
    statically named members workable -- the members say nothing, so something
    else has to, and the sender already ships it.

    `NotReady` here means the control file is CONFIGURED AND ABSENT FROM THE
    CONTAINER, which is a delivery error rather than a wait: a container
    arrives complete, so a file missing from it will never turn up beside it.
    The caller reports it as a refusal of that member; see `_plan_archive`.

    Raises `DuplicateDelivery` on the same terms `conform()` does. A container
    resent whole is the commonest way this happens -- the members inside it
    are byte-identical, and versioning them would restate every COB date
    the archive covers at once.
    """
    control = (feed.arrival or {}).get("control") or {}
    declared: dict[str, Any] = {}
    if control:
        if control_filename is None or control_text is None:
            raise NotReady(
                f"{feed.name}: member {member_filename} of "
                f"{container_filename} has no control file matching "
                f"{control['pattern']!r} (stem {_stem(member_filename)!r}) "
                f"inside the container. The member's COB date is read out of "
                f"that file, so without it the member cannot be named.")
        declared = read_control(feed, control_text, control_filename)

    cob_date = declared.get("cob_date") or member_cob_date(feed, member_filename)
    if cob_date is None:
        # resolve_arrival_config guarantees one source exists, so reaching here
        # means the configured one produced nothing -- which the readers above
        # would already have raised on. A guard, because without a date the
        # member cannot be named at all.
        raise ConformanceError(
            f"{feed.name}: no COB date for member {member_filename} of "
            f"{container_filename} -- neither `arrival.archive.member_pattern` "
            f"nor its control file yielded one.")

    observed = observe(feed, member_content)
    try:
        landing_filename = _free_name(feed, cob_date, declared.get("version"),
                                      taken, md5=observed["md5"],
                                      landed_md5=landed_md5)
        control_landing_filename = landing_control_filename(feed, landing_filename)
    except FilenameError as exc:
        raise ConformanceError(
            f"{feed.name}: cannot build the landing name for member "
            f"{member_filename}: {exc}") from exc

    now = datetime.now(timezone.utc)
    metadata = {
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
        "declared": {k: (v.isoformat() if isinstance(v, date) else v)
                     for k, v in declared.items()},
    }
    if control_filename is not None:
        # Present only for a member that HAD one, so the two keys mean what
        # they mean on the plain path: absent, not null, when there was none.
        metadata["landing_control_filename"] = control_landing_filename
        metadata["source_control_filename"] = control_filename
    return {
        "landing_filename": landing_filename,
        "control_landing_filename": (control_landing_filename
                                     if control_filename is not None else None),
        "metadata_filename": landing_filename + METADATA_SUFFIX,
        "cob_date": cob_date,
        "metadata": metadata,
    }


# ============================================================ arrival shapes
# HOW A FILE AT THE DOOR BECOMES DELIVERIES. One entry per shape, and adding a
# shape is a planner plus a line in `ARRIVAL_SHAPES` -- not a branch in the
# watcher, which is what this seam exists to prevent. Before it there were two
# `_promote*` functions in `inbox.py`, each with its own copy of the write
# order, the move rules and the trigger, already disagreeing about which
# failures were fatal.
#
# THE CONTRACT, and it is deliberately small:
#
#   planner(feed, filename, content, siblings, ...) -> list[Outcome]
#
# PURE -- it reads bytes it is handed and returns what SHOULD be written,
# never writing anything, which is what lets every shape be tested without S3
# or a watcher. One inbox file may become many outcomes (an archive) or one (a
# plain delivery), and the four outcome types below are the whole vocabulary
# the caller has to understand.
#
# A NEW SHAPE MUST NOT NEED A NEW OUTCOME TYPE. If it does, the thing it wants
# to say is something `inbox.py` does not yet know how to act on, and that is
# the conversation to have before writing the planner.


@dataclass(frozen=True)
class Siblings:
    """The files a delivery's control file could be among, and how to read one.

    THE ONE ABSTRACTION THE SHAPES SHARE. A plain delivery's control file sits
    beside it in the inbox directory; an archive member's sits beside it
    INSIDE the container. Same question -- "which of these names is this
    delivery's control file, and what does it say" -- asked of two different
    namespaces, so `find_control` and `read_control` serve both and a future
    shape (a tar, a sidecar directory, an object-store prefix) supplies its
    own namespace rather than its own copy of the pairing rule.

    `read` is a callable rather than a dict of bytes so a shape whose members
    are expensive to materialise reads only the one file it turns out to need.
    """
    names: list[str] = field(default_factory=list)
    read: Callable[[str], bytes] = lambda name: b""


@dataclass(frozen=True)
class Planned:
    """One delivery the gate could name, and every object it should become.

    `consumed` names the INBOX files that travelled with this delivery and
    should be moved with it -- a plain delivery's control file, and nothing
    for an archive member, whose control file was inside the container and
    never existed as a file of its own.
    """
    source_name: str
    data: bytes
    landing_filename: str
    metadata_filename: str
    metadata: dict[str, Any]
    cob_date: date
    control: bytes | None = None
    control_landing_filename: str | None = None
    consumed: tuple[str, ...] = ()


@dataclass(frozen=True)
class Refused:
    """An IDENTITY failure: these bytes cannot be given a landing name.

    `source_name` equal to the inbox file's own name means the file itself is
    refused; anything else names something found inside it, and the caller
    quarantines those bytes rather than the container's.
    """
    source_name: str
    data: bytes
    reason: str
    consumed: tuple[str, ...] = ()


@dataclass(frozen=True)
class Duplicate:
    """These exact bytes are already landed for this COB date. Not a failure,
    and not a restatement -- there is nothing to write and nothing to trigger."""
    source_name: str
    landing_filename: str
    reason: str
    consumed: tuple[str, ...] = ()


@dataclass(frozen=True)
class Waiting:
    """The delivery is here and incomplete -- its control file has not arrived.
    Nothing is wrong; the caller leaves everything where it is and tries again."""
    source_name: str
    reason: str


Outcome = Planned | Refused | Duplicate | Waiting


def arrival_shape(feed: Feed) -> str:
    """Which planner this feed's deliveries go through. See `ARRIVAL_SHAPES`."""
    return "archive" if (feed.arrival or {}).get("archive") else "file"


def plan_arrival(feed: Feed, filename: str, content: bytes, *,
                 siblings: Siblings | None = None,
                 received_at: datetime | None = None,
                 taken: set[str] | None = None,
                 landed_md5: Callable[[str], str | None] | None = None
                 ) -> list[Outcome]:
    """One file at the door -> what should be written for it. Writes nothing."""
    return ARRIVAL_SHAPES[arrival_shape(feed)](
        feed, filename, content, siblings or Siblings(),
        received_at, taken, landed_md5)


def _plan_file(feed: Feed, filename: str, content: bytes, siblings: Siblings,
               received_at, taken, landed_md5) -> list[Outcome]:
    """A plain delivery, with its control file beside it in the inbox."""
    control_name = find_control(feed, filename, siblings.names)
    control_bytes = siblings.read(control_name) if control_name else None
    control_text = (control_bytes.decode(feed.file_encoding, errors="replace")
                    if control_bytes is not None else None)
    consumed = (control_name,) if control_name else ()
    try:
        plan = conform(feed, filename, content,
                       control_filename=control_name, control_text=control_text,
                       received_at=received_at, taken=taken,
                       landed_md5=landed_md5)
    except NotReady as exc:
        return [Waiting(filename, str(exc))]
    except DuplicateDelivery as exc:
        return [Duplicate(filename, exc.landing_filename, str(exc), consumed)]
    except ConformanceError as exc:
        # The control file is refused WITH the delivery it gates: on its own it
        # is unreadable evidence, and the commonest identity failure is that
        # the two disagree.
        return [Refused(filename, content, str(exc), consumed)]
    return [Planned(
        source_name=filename, data=content,
        landing_filename=plan["landing_filename"],
        metadata_filename=plan["metadata_filename"],
        metadata=plan["metadata"], cob_date=plan["cob_date"],
        control=control_bytes,
        control_landing_filename=plan["control_landing_filename"],
        consumed=consumed)]


def _plan_archive(feed: Feed, filename: str, content: bytes,
                  siblings: Siblings, received_at, taken,
                  landed_md5) -> list[Outcome]:
    """A container: N deliveries out, each following the single-file path.

    `taken` ADVANCES AS MEMBERS ARE PLANNED, not read once: two members for
    the same COB date inside one container would otherwise both render the
    unversioned name and the second would overwrite the first.

    ONE BAD MEMBER DOES NOT DISCARD THE REST. The others are real deliveries
    that arrived and can be ingested, so a member's refusal is one `Refused`
    among the batch rather than an exception that abandons it. A failure of
    the CONTAINER -- unreadable zip, no member claimed -- refuses the inbox
    file itself, which the caller tells apart by `source_name`.
    """
    try:
        members = dict(archive_members(feed, filename, content))
        claimed = unpack(feed, filename, content)
    except ConformanceError as exc:
        return [Refused(filename, content, str(exc))]

    inside = Siblings(names=list(members), read=lambda name: members[name])
    seen = set(taken or ())
    out: list[Outcome] = []
    for member_name, member_bytes in claimed:
        control_name = find_control(feed, member_name, inside.names)
        control_bytes = inside.read(control_name) if control_name else None
        control_text = (control_bytes.decode(feed.file_encoding,
                                             errors="replace")
                        if control_bytes is not None else None)
        try:
            plan = conform_member(
                feed, filename, content, member_name, member_bytes,
                control_filename=control_name, control_text=control_text,
                received_at=received_at, taken=seen, landed_md5=landed_md5)
        except DuplicateDelivery as exc:
            out.append(Duplicate(f"{filename}!{member_name}",
                                 exc.landing_filename, str(exc)))
            continue
        except (ConformanceError, NotReady) as exc:
            # NotReady CANNOT CLEAR ON ITS OWN HERE, so it is a refusal rather
            # than a wait: a member's control file is packed in the same
            # container, and a container is complete the moment it arrives.
            # Waiting for a file that is already known to be absent would hold
            # the delivery for ever and report nothing.
            out.append(Refused(f"{filename}!{member_name}", member_bytes,
                               str(exc)))
            continue
        seen.add(plan["landing_filename"])
        out.append(Planned(
            source_name=f"{filename}!{member_name}", data=member_bytes,
            landing_filename=plan["landing_filename"],
            metadata_filename=plan["metadata_filename"],
            metadata=plan["metadata"], cob_date=plan["cob_date"],
            control=control_bytes,
            control_landing_filename=plan["control_landing_filename"]))
    return out


# Every shape this platform can accept at the door. See the contract above.
ARRIVAL_SHAPES: dict[str, Callable[..., list[Outcome]]] = {
    "file": _plan_file,
    "archive": _plan_archive,
}


def metadata_bytes(metadata: dict[str, Any]) -> bytes:
    return json.dumps(metadata, indent=2, sort_keys=True).encode("utf-8")


def is_metadata_key(key: str) -> bool:
    return key.endswith(METADATA_SUFFIX)


def delivery_of_metadata(filename: str) -> str:
    """`X.meta.json` -> `X`. What lets landing retention date a metadata
    object from its own name, with no lookup of the delivery beside it."""
    return filename[:-len(METADATA_SUFFIX)]
