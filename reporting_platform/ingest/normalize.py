"""Landing object -> a manifest in `ready/`, the one shape ingest understands.

WHY THIS EXISTS. `landing/` was doing two jobs with opposite requirements: it
is the immutable evidence copy, kept for years and never deleted on a guess,
AND it was the work queue. The tension is visible in `retention/landing.py`,
which refuses to delete an object whose name it cannot parse -- correct for
evidence, and the reason anything the platform does not recognise accumulates
in the queue forever.

So the two are split:

    landing/<feed>/   what the upstream sent, byte for byte, kept for years
    ready/<feed>/     a manifest per delivery, plus any derived parts

and this module is the stage between them.
See docs/DECISIONS.md#ready-is-a-derived-index and docs/DELIVERY-SHAPES.md.

THE MANIFEST IS THE POINT, not the directory. It records, once, the three
things every downstream reader was previously re-deriving from the filename
with its own copy of the same regex: which business date this delivery is
for, which objects hold its rows, and how to read them. `Feed.parse_filename`
had fourteen call sites across seven modules, each free to disagree.

FOR A PLAIN CSV NOTHING IS COPIED. The manifest's single part points straight
back at the landing object, so the common case costs one small JSON object
rather than a second copy of every delivery -- and ingest still has exactly
one code path, because it reads `parts` and neither knows nor cares whether
they point into `landing/` or `ready/`. A normalizer copies bytes only when it
genuinely transforms them, which the archive normalizer will and this one does
not.

WHAT IS NOT IN THE MANIFEST: whether the delivery has been ingested.
`arrival.already_ingested` derives that from `_source_file` in the raw table
precisely so it cannot drift from reality; the legacy `stg` load-control
tables are what that avoids. A manifest carrying `"ingested": true` would be
that table under a new name. The manifest records OBSERVATIONS about an event
-- what arrived, how big it was, how to read it -- never derived state.

A MANIFEST IS A PURE FUNCTION OF (feed config, landing object). `received_at`
is the landing object's LastModified, not the time this ran, so re-normalizing
an unchanged delivery rewrites byte-identical content. That is what makes
`ready/` a cache that can be deleted and rebuilt rather than a third copy of
the data, and it is asserted in the tests.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from reporting_platform.common.context import Feed
# One more copy of these would be the fourth in the repo. They are private to
# arrival only in the sense that nothing outside ingest should need them.
from reporting_platform.ingest.arrival import (
    _bucket, _client, list_landing, matching,
)

log = logging.getLogger("normalize")

# Bumped when the shape below changes incompatibly. Readers check it rather
# than discovering a missing key three layers down.
MANIFEST_VERSION = 1

# One entry per delivery kind, mapping to the string recorded in the
# manifest's `normalizer` field. Adding a kind is an entry here plus a
# function, not a new branch in five places. The control-file gate (step 4 of
# docs/DELIVERY-SHAPES.md) is orthogonal to this -- it modifies `kind: file`
# rather than adding a kind, so it has no entry of its own.
NORMALIZERS = {"file": "file/v1", "archive": "archive/v1"}


class NotReady(Exception):
    """The delivery has landed but is not yet safe to normalize.

    Distinct from a normalization failure: nothing is wrong, the sender just
    is not done. `reconcile()` catches this separately from every other
    exception so a delivery waiting on its control file is reported as
    waiting, not as failed -- see docs/DECISIONS.md#control-file-gate.
    """


def manifest_prefix(feed: Feed) -> str:
    return f"{feed.ready_prefix}/{feed.name}/"


def manifest_key(feed: Feed, object_key: str) -> str:
    """Where the manifest for this landing object lives.

    Derived from the landing FILENAME, including its extension, so it is
    stable across re-normalization and reversible by eye. Including the
    extension matters: `X.csv` and `X.csv.gz` are different deliveries and
    would otherwise share a manifest.
    """
    return f"{manifest_prefix(feed)}{object_key.rsplit('/', 1)[-1]}.json"


def is_manifest_key(feed: Feed, key: str) -> bool:
    return key.startswith(manifest_prefix(feed)) and key.endswith(".json")


# ------------------------------------------------------------------ the file
def _head(bucket: str, key: str) -> dict[str, Any]:
    return _client().head_object(Bucket=bucket, Key=key)


def _stem(filename: str) -> str:
    return filename[:filename.rindex(".")] if "." in filename else filename


def is_control_file(feed: Feed, filename: str) -> bool:
    """Whether `filename` is SHAPED like this feed's control file.

    `pattern` is a regex template, not a literal one -- `delivery.control`
    validates `pattern.format(stem="X")` as a regex, and
    `'{stem}\\.ctl'.format(stem="TRADE_20260903")` gives
    `'TRADE_20260903\\.ctl'`, where the backslash is the regex escape for the
    dot, not a literal character in the filename. Treating it as a plain
    string template -- looking for an object literally containing a backslash
    -- was tried first and is wrong.

    A control file itself never matches `filename_pattern`: it names no
    business date, only says something about a delivery that does. Without
    this, `inbox.route()` would reject it as unroutable and it would never
    reach `landing/`, and the delivery it belongs to would wait on a control
    file that can never arrive.
    """
    control = feed.delivery.get("control")
    if not control:
        return False
    regex = control["pattern"].replace("{stem}", "(?P<stem>.+)")
    return re.fullmatch(regex, filename) is not None


def _find_control_key(feed: Feed, object_key: str) -> str | None:
    """This delivery's control file, if it has landed alongside the data file.

    Searches the SAME landing folder for a sibling matching `pattern` with
    `{stem}` substituted for the data object's own stem -- never a fixed name,
    or every delivery would race to look for the same control file. Returns
    None rather than a not-yet-existing key: unlike a plain-CSV part, which
    exists as soon as normalize runs, a control file may simply not be there
    yet, and that is the ordinary case this whole mechanism exists for.
    """
    from reporting_platform.ingest.arrival import list_landing

    dirname, filename = object_key.rsplit("/", 1)
    stem = _stem(filename)
    regex = re.compile(feed.delivery["control"]["pattern"].format(stem=re.escape(stem)))
    for key in list_landing(feed):
        if key == object_key or key.rsplit("/", 1)[0] != dirname:
            continue
        if regex.fullmatch(key.rsplit("/", 1)[-1]):
            return key
    return None


def _declared_row_count(feed: Feed, control_key: str) -> int | None:
    """The exact row count a control file declares, or None if it declares none.

    `row_count` is optional on `delivery.control` -- some control files are
    pure readiness gates with nothing to parse out of them.
    """
    row_count = feed.delivery["control"].get("row_count")
    if row_count is None:
        return None
    body = _client().get_object(Bucket=_bucket(), Key=control_key)["Body"].read()
    text = body.decode(feed.file_encoding, errors="replace")
    m = re.search(row_count, text)
    if not m:
        raise ValueError(
            f"{feed.name}: control file {control_key} does not match "
            f"`delivery.control.row_count` {row_count!r}. The control file "
            f"arrived but does not say what it was validated to say -- a "
            f"format change upstream, not a timing problem.")
    return int(m.group("rows"))


def _normalize_file(feed: Feed, object_key: str) -> dict[str, Any]:
    """The pass-through normalizer: one landing object, one delivery, no copy.

    Exactly what `parse_filename` did, written down once instead of being
    re-derived at every call site.
    """
    filename = object_key.rsplit("/", 1)[-1]
    parsed = feed.parse_filename(filename)
    if parsed is None:
        raise ValueError(
            f"{feed.name}: {filename!r} does not match filename_pattern "
            f"{feed.filename_pattern!r}, so it has no business date and "
            f"cannot be normalized. Fix the pattern, or the file is not this "
            f"feed's.")
    business_date, _version = parsed

    control_key = None
    declared_row_count = None
    if "control" in feed.delivery:
        control_key = _find_control_key(feed, object_key)
        if control_key is None:
            raise NotReady(
                f"{feed.name}: {filename} is waiting on a control file "
                f"matching {feed.delivery['control']['pattern']!r} (stem "
                f"{_stem(filename)!r}) in the same landing folder. Not a "
                f"failure -- a late feed, not a failed one.")
        declared_row_count = _declared_row_count(feed, control_key)

    head = _head(_bucket(), object_key)
    return {
        "manifest_version": MANIFEST_VERSION,
        "feed": feed.name,
        "business_date": business_date.isoformat(),
        # Deterministic, so re-normalizing is a no-op. Anything time-based
        # here would make an idempotent operation produce a new manifest.
        "delivery_id": filename,
        # The DELIVERY's arrival time, not this run's. See the module header.
        "received_at": head["LastModified"].astimezone(timezone.utc).isoformat(),
        "source_object": object_key,
        "parts": [{"object_key": object_key, "bytes": int(head["ContentLength"])}],
        # Resolved from feeds.yml at normalize time and READ BACK from here at
        # ingest, so an ingest is reproducible: you can say what delimiter was
        # actually used for a delivery six months ago. Correcting a wrong one
        # means fixing feeds.yml and re-normalizing, which is cheap because
        # `ready/` is a cache.
        "format": {
            "delimiter": feed.delimiter,
            "quote_char": feed.quote_char,
            "header": feed.header,
            "encoding": feed.file_encoding,
        },
        # What the control file declared, an OBSERVATION recorded once rather
        # than re-read at ingest -- see the module header on why the manifest
        # never holds derived state, only what arrived. None for a feed with
        # no `delivery.control`, or one whose control file sets no row_count.
        "control_object": control_key,
        "declared_row_count": declared_row_count,
        "normalizer": NORMALIZERS["file"],
    }


def _member_dir(feed: Feed, object_key: str) -> str:
    """Where this container's extracted members live.

    Derived from the container FILENAME, so it is stable across
    re-normalization. That is not cosmetic: `already_ingested` matches on
    `_source_file`, so a key containing a timestamp or a uuid would make every
    re-normalized delivery re-ingest as a new `_file_version`.
    """
    stem = object_key.rsplit("/", 1)[-1]
    stem = stem[:stem.rindex(".")] if "." in stem else stem
    return f"{manifest_prefix(feed)}{stem}/"


def _safe_member_name(name: str) -> str:
    """The member's own name, with any directory structure refused.

    A zip member may name `../../etc/passwd` or an absolute path; extracting
    one by joining it onto a prefix is the standard archive traversal bug. Here
    it would write outside the feed's `ready/` prefix -- into another feed's,
    or over a manifest. Only a plain filename is accepted, so the check cannot
    be defeated by encoding: anything containing a separator is rejected
    outright rather than normalised into something that looks safe.
    """
    if name.endswith("/"):
        raise ValueError(f"archive member {name!r} is a directory")
    if "/" in name or "\\" in name or name in ("", ".", ".."):
        raise ValueError(
            f"archive member {name!r} contains a path. Only flat members are "
            f"extracted -- a member naming a directory could be written "
            f"outside this feed's ready/ prefix.")
    return name


def _normalize_archive(feed: Feed, object_key: str) -> dict[str, Any]:
    """A container of members -> one delivery whose parts are those members.

    THE DATE IS ON THE CONTAINER, NOT THE MEMBERS. That is the whole shape
    this exists for: `custodyPositions_20260903.zip` holding `positions_1.csv`
    and `positions_2.csv`, neither of which says which day it is for. The
    container name is matched by the feed's ordinary `filename_pattern`, so
    routing, `matching()` and landing retention need to know nothing about
    archives.

    THIS ONE COPIES BYTES, and it is the first normalizer that does. The
    extracted members land under `ready/`, where short retention and
    rebuildability apply -- never under `landing/`, which is the evidence copy
    and holds the container as delivered.
    """
    import io
    import zipfile

    filename = object_key.rsplit("/", 1)[-1]
    parsed = feed.parse_filename(filename)
    if parsed is None:
        raise ValueError(
            f"{feed.name}: container {filename!r} does not match "
            f"filename_pattern {feed.filename_pattern!r}, so it has no "
            f"business date. With `business_date_from: container` the date "
            f"lives in the container name and nowhere else.")
    business_date, _version = parsed

    head = _head(_bucket(), object_key)
    body = _client().get_object(Bucket=_bucket(), Key=object_key)["Body"].read()
    member_re = re.compile(feed.delivery["member_pattern"])
    dest = _member_dir(feed, object_key)

    parts = []
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        # Sorted so `parts` order -- and therefore the union order at ingest --
        # does not depend on how the sender happened to build the archive.
        for info in sorted(zf.infolist(), key=lambda i: i.filename):
            if info.is_dir() or not member_re.fullmatch(info.filename):
                continue
            name = _safe_member_name(info.filename)
            key = f"{dest}{name}"
            _client().put_object(Bucket=_bucket(), Key=key,
                                 Body=zf.read(info.filename))
            parts.append({"object_key": key, "bytes": info.file_size,
                          "member": name})

    if not parts:
        raise ValueError(
            f"{feed.name}: {filename} holds no member matching "
            f"{feed.delivery['member_pattern']!r}. An archive that unpacks to "
            f"nothing is a delivery problem, not an empty day -- landing it as "
            f"zero rows would pass `expected_min_rows` only by accident.")

    return {
        "manifest_version": MANIFEST_VERSION,
        "feed": feed.name,
        "business_date": business_date.isoformat(),
        "delivery_id": filename,
        "received_at": head["LastModified"].astimezone(timezone.utc).isoformat(),
        "source_object": object_key,
        "parts": parts,
        "format": {
            "delimiter": feed.delimiter,
            "quote_char": feed.quote_char,
            "header": feed.header,
            "encoding": feed.file_encoding,
        },
        # `delivery.control` is rejected for `kind: archive` at load
        # (context.resolve_delivery_config), so both are always None here.
        "control_object": None,
        "declared_row_count": None,
        "normalizer": NORMALIZERS["archive"],
    }


def _kind(feed: Feed) -> str:
    """Which normalizer this feed needs, from its validated `delivery:` block."""
    return (feed.delivery or {}).get("kind", "file")


# ------------------------------------------------------------------- storage
def write_manifest(feed: Feed, manifest: dict[str, Any]) -> str:
    key = manifest_key(feed, manifest["source_object"])
    _client().put_object(
        Bucket=_bucket(), Key=key,
        Body=json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json")
    return key


def read_manifest(key: str) -> dict[str, Any]:
    body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    manifest = json.loads(body)
    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        raise ValueError(
            f"{key}: manifest_version {version!r}, this platform writes "
            f"{MANIFEST_VERSION}. Delete the ready/ prefix and re-normalize -- "
            f"it is a cache, rebuildable from landing/.")
    return manifest


def list_manifests(feed: Feed) -> list[str]:
    paginator = _client().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=_bucket(), Prefix=manifest_prefix(feed)):
        keys += [o["Key"] for o in page.get("Contents", [])
                 if o["Key"].endswith(".json")]
    return sorted(keys)


def manifests_for(feed: Feed) -> list[tuple[str, dict[str, Any]]]:
    return [(k, read_manifest(k)) for k in list_manifests(feed)]


# ----------------------------------------------------------------- the stage
def normalize(feed: Feed, object_key: str, *, write: bool = True,
              force: bool = False) -> dict[str, Any]:
    """One landing object -> its manifest. Idempotent.

    `write=False` builds the manifest without storing it, which is what the
    single-file CLI path uses so that `--object landing/...` keeps working
    without leaving a queue entry behind.
    """
    kind = _kind(feed)
    if kind not in NORMALIZERS:
        raise ValueError(
            f"{feed.name}: unknown delivery kind {kind!r}. Known: "
            f"{', '.join(sorted(NORMALIZERS))}")

    if kind == "archive":
        # WRITE IS NOT OPTIONAL FOR AN ARCHIVE. `write=False` exists so a
        # manual `--object landing/...` ingest does not leave a queue entry
        # behind, which is free when the part IS the landing object. An
        # archive's parts have to be materialised before anything can read
        # them, so skipping the manifest would leave extracted members that
        # the `ready/` sweep -- which iterates manifests -- could never
        # collect. Cheaper to enqueue and let it be marked ingested.
        manifest = _normalize_archive(feed, object_key)
        write_manifest(feed, manifest)
        return manifest

    manifest = _normalize_file(feed, object_key)
    if write:
        key = manifest_key(feed, object_key)
        if force or not _exists(key):
            write_manifest(feed, manifest)
    return manifest


def _exists(key: str) -> bool:
    try:
        _head(_bucket(), key)
        return True
    except Exception:                                          # noqa: BLE001
        return False


def reconcile(feed: Feed) -> dict[str, Any]:
    """Give every landed delivery a manifest. Cheap, idempotent, no Spark.

    `ready/` is a DERIVED INDEX of `landing/`, not a queue somebody has to
    remember to fill. Reconciling on demand is what removes the ordering bug
    where a file lands -- pushed straight into the bucket by the upstream
    agent, which is the production path and goes through no code of ours --
    and is then never normalized, so it never becomes pending and nothing
    anywhere reports it.

    An object that cannot be normalized is COUNTED AND SKIPPED, not raised on.
    One unroutable file must not stop the other nineteen from being ingested;
    it is reported instead, and the unclaimed queue in step 5 is what turns
    that count into something actionable.

    A `NotReady` delivery is counted separately from a failure, in its own
    `awaiting_control` list, and logged at INFO rather than WARNING -- nothing
    is wrong, the control file just has not landed yet, and this function
    runs on every poll, so it would otherwise warn about the same ordinary
    wait on every single pass.
    """
    landed = matching(feed, list_landing(feed))
    have = set(list_manifests(feed))
    created, failed, awaiting = [], [], []
    for key in landed:
        if manifest_key(feed, key) in have:
            continue
        try:
            created.append(write_manifest(feed, normalize(feed, key, write=False)))
        except NotReady as exc:
            awaiting.append({"object": key, "waiting_for": str(exc)})
        except Exception as exc:                               # noqa: BLE001
            failed.append({"object": key, "error": f"{type(exc).__name__}: {exc}"})
    if failed:
        log.warning("%s: %d landed object(s) could not be normalized: %s",
                    feed.name, len(failed), failed[:3])
    if awaiting:
        log.info("%s: %d delivery(ies) awaiting their control file: %s",
                 feed.name, len(awaiting),
                 [a["object"] for a in awaiting][:3])
    return {"feed": feed.name, "landed": len(landed),
            "created": created, "failed": failed, "awaiting_control": awaiting}


def business_date_of(manifest: dict[str, Any]) -> date:
    return date.fromisoformat(manifest["business_date"])
