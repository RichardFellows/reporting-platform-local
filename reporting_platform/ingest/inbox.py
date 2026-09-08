"""Watch a host folder and ingest whatever is dropped into it.

Drop `TRADE_20260901.csv` into `inbox/` and it lands in
`landing/fo_trade/TRADE_20260901.csv`, the ingest DAG runs, and the file moves to
`inbox/.processed/trade/`. No console, no MinIO UI, no CLI.

That is what the upstream will actually do: write a file to a directory. The
console's upload button (docs/FEED-UI.md) is for a person with a file in their
hand; this is for the twenty files that arrive overnight.

POLLED, NOT inotify, AND THAT IS DELIBERATE. Filesystem events do not cross a
Docker Desktop bind mount on Windows or macOS -- the host writes the file, the
container is never told, and a watcher built on `watchdog`/inotify sits there
reporting itself healthy while nothing happens.
See docs/DECISIONS.md#inbox-is-polled. Polling costs a directory
listing every few seconds and works the same on every host, which for a folder
that receives a handful of files a day is the right trade.

FOUR THINGS IT DOES THAT A NAIVE LOOP WOULD NOT, each of which is the
difference between a watcher you can leave running and one you cannot:

**It waits for the file to stop changing.** A file appears in a directory the
moment it is created, not when it is finished. Uploading a half-written CSV
gives you a short file that ingests cleanly -- `expected_min_rows` is the only
thing between that and a silently truncated delivery. A file is considered
ready when its size and mtime are unchanged across two consecutive polls.

**It routes by the feeds' own filename patterns**, so no configuration here
repeats what `feeds.yml` already says. A file matching no feed is moved to
`.rejected/` rather than left in place, because a file that stays put is one
the watcher retries forever, logging on every pass.

**A file matching MORE than one feed is rejected, not guessed.** Two feeds with
overlapping patterns is a configuration error, and picking one arbitrarily
would put a delivery in the wrong raw table -- which looks like data, not like
an error.

**It moves the file before triggering.** If the trigger fails, the file is
already out of the way and recorded as landed, so the next pass does not
re-upload it as a new `_file_version`. The DAG can be retriggered by hand; a
duplicate ingest is much harder to undo.

AND IT IS THE CONFORMANCE GATE. `landing/` has a contract -- every object in
it is correctly named and classified -- and a legacy upstream that sends
`positions.csv` with the date inside `positions.ctl` does not satisfy it. For
a feed with an `arrival:` block this watcher waits for the control file,
verifies what it declares (row count, md5), derives the COB date, and
promotes the delivery under the name `filename_pattern` describes, with a
`.meta.json` sibling recording what actually arrived. A feed with no
`arrival:` block is a conformant upstream: its file is uploaded under its own
name, exactly as before.

**An unchanged file sent twice lands nothing.** The gate compares the bytes
against what is already landed for that COB date and reports `duplicate`
-- nothing uploaded, nothing triggered, the inbox copy moved to
`.processed/`. A conformant upstream gets this free by writing the same key
twice; the gate had to be taught it, because its rename would otherwise turn
the second copy into `_v2` and the raw table into a restatement that never
happened. See `conform.DuplicateDelivery`.

That gate is why the two names in play are different strings and must stay
so. `positions.csv` is what the upstream sends; `trs_position_20260801.csv`
is what landing holds. `Feed.claims_source` answers the first,
`Feed.parse_filename` the second, and `ingest/conform.py` is the only thing
that crosses between them.
See docs/DECISIONS.md#the-inbox-is-the-conformance-gate.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from reporting_platform.common.context import Feed, feeds
from reporting_platform.ingest import conform
from reporting_platform.ingest import normalize as norm
from reporting_platform.ingest.arrival import (
    landed_md5_lookup, list_landing, put_landing, put_landing_bytes,
)

log = logging.getLogger("inbox")

INBOX = Path(os.environ.get("REPORTING_INBOX", "/opt/platform/inbox"))
PROCESSED = ".processed"
REJECTED = ".rejected"
# Two consecutive identical observations, so a file being written is not
# uploaded half-finished. At the default interval that is a few seconds of
# quiet, which every real delivery has and no partial write does.
STABLE_POLLS = 2


def _skip(path: Path) -> bool:
    """Directories, dotfiles and our own bookkeeping folders."""
    return (path.is_dir() or path.name.startswith(".")
            or path.name.endswith((".tmp", ".part", ".crdownload", ".filepart")))


def route(filename: str) -> tuple[Feed | None, str | None, bool]:
    """Which feed claims this filename, by the feeds' own patterns.

    Returns (feed, reason-it-was-rejected, is_control). Exactly one of the
    first two is set.

    FOUR THINGS CAN CLAIM A NAME, checked in this order:

    1. `filename_pattern` -- an ALREADY-CONFORMANT delivery, dropped into the
       inbox rather than PUT straight to landing. Uploaded under its own name.
    2. `arrival.source_pattern` -- a legacy delivery under the name its
       upstream sends. Goes through the conformance gate and is renamed.
    3. `arrival.control.pattern` -- a legacy delivery's control file, consumed
       at the door.
    4. `delivery.control.pattern` -- a conformant upstream's control file,
       which passes through to landing and gates the delivery there.

    Conformant first, on purpose: a feed can have both patterns, and a file
    that already satisfies `filename_pattern` needs no renaming, no control
    file and no verification. Making it take the legacy path would demand a
    control file that a conformant sender has no reason to include.

    A CONTROL FILE MATCHES NO DATA PATTERN -- it names no COB date, it
    says something about a delivery that does -- so it is checked only once no
    data pattern claims the name. Without that, the inbox would reject it to
    `.rejected/` and the delivery it belongs to would wait forever on a file
    that can never arrive.
    """
    registry = list(feeds().values())

    for claimant, what, why in (
        (lambda fd: fd.parse_filename(filename) is not None,
         "filename_pattern",
         "overlapping filename_patterns are a configuration error, and "
         "guessing would put the delivery in the wrong raw table"),
        (lambda fd: fd.claims_source(filename),
         "arrival.source_pattern",
         "overlapping arrival.source_patterns are a configuration error, and "
         "guessing would put the delivery in the wrong raw table"),
    ):
        matched = [fd for fd in registry if claimant(fd)]
        if len(matched) > 1:
            return None, (f"matches more than one feed's {what} ("
                          + ", ".join(sorted(f.name for f in matched))
                          + f") -- {why}"), False
        if matched:
            return matched[0], None, False

    control_matched = [fd for fd in registry
                       if conform.is_source_control_file(fd, filename)
                       or norm.is_control_file(fd, filename)]
    if len(control_matched) > 1:
        return None, ("matches more than one feed's control pattern ("
                      + ", ".join(sorted(f.name for f in control_matched))
                      + ") -- overlapping control patterns are a "
                        "configuration error, and guessing would gate the "
                        "wrong feed's delivery"), False
    if control_matched:
        return control_matched[0], None, True

    return None, ("matches no feed's filename_pattern, arrival.source_pattern "
                  "or control pattern -- check the name, or the patterns in "
                  "feeds.yml"), False


def _quarantine(feed, path, reason_class: str, reason: str,
                dry_run: bool = False) -> None:
    """Copy a refused file to `quarantine/` and record why (REQ-106).

    BEFORE `_move`, always. `_move` is what makes the file stop being
    reprocessed, and once it has happened the bytes are on this container's
    bind mount and nowhere else; doing the durable copy first means a crash
    between the two leaves the file to be rejected again rather than leaves no
    record of it at all.

    Never raises -- see `registry.rejections.quarantine_quietly`. Keeping a
    bad file out of `landing/` is this function's caller's actual job, and it
    must still happen when Postgres or MinIO is unreachable.
    """
    if dry_run:
        return
    from reporting_platform.registry.rejections import quarantine_quietly

    try:
        content = path.read_bytes()
        received = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError as exc:
        log.warning("cannot read %s to quarantine it: %s", path.name, exc)
        return
    quarantine_quietly(feed, path.name, content, reason_class=reason_class,
                       reason=reason, received_at=received)


def list_rejected() -> list[dict]:
    """Files sitting in `.rejected/` -- landed nowhere, claimed by no feed.

    This is docs/DELIVERY-SHAPES.md#5-onboard-from-a-real-file's "unclaimed
    deliveries" backlog: the console's entry point for sniffing a file
    nobody has a feed for yet. `route()` is re-run rather than reading a
    stored reason, because feeds.yml may have changed since rejection --
    the reason (or a feed claiming it now) should reflect the CURRENT
    config, not the moment it was rejected.

    Only `INBOX/.rejected/` -- landing's own "unrecognised object" count
    (`retention/landing.py`) is a narrower, per-feed case (a file for an
    ALREADY-onboarded feed with the wrong name) and is not surfaced here.
    """
    d = INBOX / REJECTED
    if not d.is_dir():
        return []
    out = []
    for path in sorted(d.iterdir()):
        if _skip(path):
            continue
        feed, reason, is_control = route(path.name)
        stat = path.stat()
        out.append({
            "filename": path.name,
            "bytes": stat.st_size,
            "rejected_at": stat.st_mtime,
            "reason": reason,
            # feeds.yml may have moved on since this was rejected -- either
            # of these means a re-drop into inbox is now the right move,
            # not a sniff.
            "now_claimed_by": feed.name if feed else None,
            "now_routes_as_control": is_control,
        })
    return out


def read_rejected(filename: str) -> bytes:
    """Bytes of one file in `.rejected/`, for the console to sniff.

    `filename` reaches this from an HTTP request, so it is validated as a
    bare filename before being joined onto `INBOX` -- the same
    directory-traversal concern `ingest/normalize.py`'s `_safe_member_name`
    guards against for an archive member, here for a name coming over the
    API instead of out of a zip.
    """
    if "/" in filename or "\\" in filename or filename in ("", ".", ".."):
        raise ValueError(f"{filename!r} is not a bare filename")
    path = INBOX / REJECTED / filename
    if not path.is_file():
        raise FileNotFoundError(f"no {filename!r} in .rejected/")
    return path.read_bytes()


def _move(path: Path, folder: str, feed_name: str | None = None) -> Path:
    dest_dir = INBOX / folder / (feed_name or "")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / path.name
    if dest.exists():
        # Same filename delivered twice is ordinary -- a corrected file keeps
        # its name. Keep both rather than overwriting the evidence.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = dest_dir / f"{path.stem}.{stamp}{path.suffix}"
    shutil.move(str(path), str(dest))
    return dest


def _trigger(feed: Feed, key: str | None) -> dict:
    """Unpause if needed, then trigger one run.

    `key` is the DATA object to ingest, and is what `resolve_arrival` acts on
    directly. Pass None for a CONTROL file: it names no delivery of its own,
    so the run falls back to `resolve_arrival`'s `find_pending` path instead,
    which reconciles `ready/` and picks up whichever waiting delivery this
    control file has just unblocked -- including one from an earlier, already
    -triggered run that hit `normalize.NotReady` and was skipped rather than
    retried into it.

    Imports the console's orchestration module rather than opening a second
    HTTP client: there is one definition of how this platform talks to
    Airflow's API, and a copy here would drift from it.
    """
    from reporting_platform.ui import orchestration

    dag_id = f"ingest_{feed.name}"
    dag = orchestration.get_dag(dag_id)
    if dag is None:
        return {"triggered": False,
                "reason": f"Airflow has not parsed {dag_id} yet"}
    unpaused = False
    if dag.get("is_paused"):
        orchestration.set_paused(dag_id, False)
        unpaused = True
    conf = {"object_key": key} if key else {}
    run = orchestration.trigger(dag_id, conf=conf,
                                note="dropped into the inbox")
    return {"triggered": True, "dag_id": dag_id, "unpaused": unpaused,
            "run_id": run.get("dag_run_id")}


def sweep(seen: dict[str, tuple[int, float, int]], *, dry_run: bool = False) -> list[dict]:
    """One pass. `seen` carries stability state between passes."""
    results: list[dict] = []
    if not INBOX.is_dir():
        raise RuntimeError(
            f"no inbox directory at {INBOX}. Create it and mount it, or set "
            f"REPORTING_INBOX.")

    for path in sorted(INBOX.iterdir()):
        if _skip(path):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:          # moved or removed mid-pass
            seen.pop(path.name, None)
            continue

        size, mtime, count = seen.get(path.name, (-1, -1.0, 0))
        if (size, mtime) != (stat.st_size, stat.st_mtime):
            seen[path.name] = (stat.st_size, stat.st_mtime, 1)
            continue
        if count < STABLE_POLLS:
            seen[path.name] = (stat.st_size, stat.st_mtime, count + 1)
            continue

        feed, reason, is_control = route(path.name)
        if feed is None:
            log.warning("rejecting %s: %s", path.name, reason)
            if not dry_run:
                # `route` returns one string for two different situations, and
                # they are worth counting apart: nothing claimed the name
                # (usually a name nobody has onboarded) versus two feeds
                # claiming it (always a configuration error in feeds.yml).
                _quarantine(None, path,
                            "ambiguous" if "more than one" in (reason or "")
                            else "unroutable", reason or "", dry_run)
                _move(path, REJECTED)
            seen.pop(path.name, None)
            results.append({"file": path.name, "status": "rejected",
                            "reason": reason})
            continue

        # A LEGACY CONTROL FILE IS NOT PROMOTED ON ITS OWN. It is consumed at
        # the door as part of its data file's delivery, so it is left in place
        # here and picked up when that data file is processed -- which may be
        # this same pass (the loop is sorted, and `.csv` sorts before `.ctl`
        # for the usual naming, so it is usually the NEXT pass) or a later one
        # if the data file has not arrived yet. Its own arrival is what
        # unblocks a data file that was waiting.
        #
        # A CONFORMANT feed's control file is different: it belongs in landing,
        # where `delivery.control` gates the delivery, so it falls through to
        # the ordinary upload below.
        if is_control and conform.is_source_control_file(feed, path.name):
            results.append({"file": path.name, "status": "held (control file)",
                            "feed": feed.name})
            continue

        if dry_run:
            results.append({"file": path.name,
                            "status": "would conform" if feed.needs_conforming
                                      and not feed.parse_filename(path.name)
                                      else "would ingest",
                            "feed": feed.name})
            continue

        # Already-conformant names take the plain path even for a feed that
        # HAS an arrival block -- see route()'s docstring.
        if feed.needs_conforming and feed.parse_filename(path.name) is None:
            if conform.is_archive(feed):
                results.extend(_promote_archive(feed, path, seen))
                continue
            outcome = _promote(feed, path, seen)
            if outcome is not None:
                results.append(outcome)
            continue

        try:
            key = put_landing(feed, str(path), path.name)
        except Exception as exc:                        # noqa: BLE001
            # Left in place on purpose: an upload failure is usually MinIO
            # being unreachable, which the next pass may well survive.
            log.error("upload failed for %s: %s", path.name, str(exc)[:300])
            results.append({"file": path.name, "status": "upload failed",
                            "error": str(exc)[:300]})
            continue

        # Moved BEFORE the trigger -- see the module docstring.
        moved = _move(path, PROCESSED, feed.name)
        seen.pop(path.name, None)
        outcome = {"file": path.name, "status": "landed", "feed": feed.name,
                   "key": key, "moved_to": str(moved.relative_to(INBOX)),
                   "is_control": is_control}
        # A control file names no delivery of its own -- see _trigger's
        # docstring for why the run gets no object_key.
        outcome.update(_trigger(feed, None if is_control else key))
        log.info("landed %s -> %s%s%s", path.name, key,
                 " (control file)" if is_control else "",
                 "" if outcome.get("triggered") else
                 f" (NOT triggered: {outcome.get('reason')})")
        results.append(outcome)

    return results


def _promote(feed: Feed, path: Path,
             seen: dict[str, tuple[int, float, int]]) -> dict | None:
    """Run one legacy delivery through the conformance gate.

    Returns the outcome, or None when the delivery is simply not ready yet and
    nothing should be reported -- `NotReady` is the ordinary case this whole
    mechanism exists for, and reporting it every few seconds would bury the
    outcomes that matter.

    THREE OBJECTS GO TO LANDING, not one: the renamed data file, the renamed
    control file, and the metadata sibling. The delivery is the data file and
    its control file together, so landing holds the pair -- which is what lets
    `delivery.control` verify it there exactly as it verifies a delivery an
    approved sender wrote straight into the bucket. Nothing is consumed here.

    ORDER OF WRITES: control file, then data file, then metadata. The data
    file is what a triggered run acts on, so writing it last means a run can
    never find a delivery whose control file has not arrived yet. If the
    metadata write then fails, the delivery is complete and will ingest with
    its provenance missing -- the failure to prefer over the reverse.
    Originals stay in the inbox on any failure, so the next pass retries;
    every write is a PutObject to a derived key, so a retry overwrites rather
    than duplicating.
    """
    control_name = conform.find_control(
        feed, path.name, [p.name for p in INBOX.iterdir() if not _skip(p)])
    control_path = INBOX / control_name if control_name else None
    control_text = None
    if control_path is not None:
        try:
            control_text = control_path.read_text(
                encoding=feed.file_encoding, errors="replace")
        except OSError as exc:
            log.error("cannot read control file %s: %s", control_name, exc)
            return {"file": path.name, "status": "control unreadable",
                    "feed": feed.name, "error": str(exc)[:300]}

    content = path.read_bytes()
    landed = list_landing(feed)
    try:
        plan = conform.conform(
            feed, path.name, content,
            control_filename=control_name,
            control_text=control_text,
            received_at=datetime.fromtimestamp(path.stat().st_mtime,
                                               tz=timezone.utc),
            # A re-delivery for a date already landed must not overwrite the
            # original -- landing is the evidence copy. The listing is what
            # lets the gate pick the next free `_vN`.
            taken={k.rsplit("/", 1)[-1] for k in landed},
            # ...and this is what stops it picking one for a delivery that is
            # not a re-delivery at all, only the same file sent twice.
            landed_md5=landed_md5_lookup(feed, landed))
    except conform.NotReady as exc:
        log.debug("%s", exc)
        return None
    except conform.DuplicateDelivery as exc:
        # NOT a rejection. The delivery is already landed and already
        # ingested; there is nothing to write and nothing to trigger. The
        # inbox copy still moves to `.processed/`, where `_move` timestamps a
        # colliding name, so the resend itself remains on disk as evidence it
        # happened -- it is simply not evidence of a new delivery.
        log.info("%s", exc)
        _move(path, PROCESSED, feed.name)
        if control_path is not None and control_path.exists():
            _move(control_path, PROCESSED, feed.name)
            seen.pop(control_name, None)
        seen.pop(path.name, None)
        return {"file": path.name, "status": "duplicate", "feed": feed.name,
                "landed_as": exc.landing_filename, "reason": str(exc)}
    except conform.ConformanceError as exc:
        # AN IDENTITY FAILURE, and the only kind that can happen here. The
        # delivery cannot be NAMED -- no COB date, or a pattern no
        # concrete filename can be built from -- so there is no landing key to
        # write it to and no amount of waiting fixes it. Content failures are
        # not checked here at all: they land and the ingest refuses.
        log.warning("rejecting %s: %s", path.name, exc)
        _quarantine(feed, path, "identity", str(exc))
        _move(path, REJECTED)
        if control_path is not None and control_path.exists():
            # The control file is quarantined with the delivery it gates: on
            # its own it is unreadable evidence, and the commonest identity
            # failure is that the two disagree.
            _quarantine(feed, control_path, "identity",
                        f"control file for {path.name}: {exc}")
            _move(control_path, REJECTED)
        seen.pop(path.name, None)
        return {"file": path.name, "status": "rejected", "feed": feed.name,
                "reason": str(exc)}

    try:
        if plan["control_landing_filename"] and control_text is not None:
            put_landing_bytes(feed, plan["control_landing_filename"],
                              control_path.read_bytes())
        key = put_landing_bytes(feed, plan["landing_filename"], content)
        put_landing_bytes(feed, plan["metadata_filename"],
                          conform.metadata_bytes(plan["metadata"]),
                          content_type="application/json")
    except Exception as exc:                            # noqa: BLE001
        log.error("upload failed for %s: %s", path.name, str(exc)[:300])
        return {"file": path.name, "status": "upload failed",
                "feed": feed.name, "error": str(exc)[:300]}

    moved = _move(path, PROCESSED, feed.name)
    if control_path is not None and control_path.exists():
        _move(control_path, PROCESSED, feed.name)
        seen.pop(control_name, None)
    seen.pop(path.name, None)

    outcome = {"file": path.name, "status": "conformed", "feed": feed.name,
               "key": key, "landed_as": plan["landing_filename"],
               "control_landed_as": plan["control_landing_filename"],
               "cob_date": plan["cob_date"].isoformat(),
               "moved_to": str(moved.relative_to(INBOX))}
    outcome.update(_trigger(feed, key))
    log.info("conformed %s -> %s (%s)%s", path.name, key,
             plan["cob_date"].isoformat(),
             "" if outcome.get("triggered") else
             f" (NOT triggered: {outcome.get('reason')})")
    return outcome


def _promote_archive(feed: Feed, path: Path,
                     seen: dict[str, tuple[int, float, int]]) -> list[dict]:
    """Unpack one container and land every member as its own delivery.

    ONE INBOX FILE BECOMES N LANDED DELIVERIES. That is the whole idea: a zip
    is a transport wrapper, the gate removes it, and every member then follows
    the ordinary single-file path -- so `landing/` holds only objects Spark can
    read, and no stage downstream needs to know an archive was ever involved.

    The container itself is NOT landed. The members are byte-for-byte what the
    upstream sent; the metadata records the container's name, size and md5, so
    what arrived stays provable without keeping an object nothing reads.

    `taken` is advanced as members land, not read once: two members for the
    same COB date inside one zip would otherwise both render the
    unversioned name and the second would overwrite the first.

    A CONTAINER RESENT WHOLE is the commonest duplicate here, and it is the
    expensive one: without the md5 check every member restates its own
    COB date at once. Duplicates count as HANDLED -- if they did not, a
    zip whose members are all already landed would find nothing to land, stay
    in the inbox, and be unpacked again on every pass forever.
    """
    content = path.read_bytes()
    received = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    try:
        members = conform.unpack(feed, path.name, content)
    except conform.ConformanceError as exc:
        log.warning("rejecting %s: %s", path.name, exc)
        _quarantine(feed, path, "identity", str(exc))
        _move(path, REJECTED)
        seen.pop(path.name, None)
        return [{"file": path.name, "status": "rejected", "feed": feed.name,
                 "reason": str(exc)}]

    landing_keys = list_landing(feed)
    taken = {k.rsplit("/", 1)[-1] for k in landing_keys}
    landed_md5 = landed_md5_lookup(feed, landing_keys)
    results, landed, duplicates = [], [], 0
    for member_name, member_bytes in members:
        try:
            plan = conform.conform_member(
                feed, path.name, content, member_name, member_bytes,
                received_at=received, taken=taken, landed_md5=landed_md5)
            key = put_landing_bytes(feed, plan["landing_filename"], member_bytes)
            put_landing_bytes(feed, plan["metadata_filename"],
                              conform.metadata_bytes(plan["metadata"]),
                              content_type="application/json")
        except conform.DuplicateDelivery as exc:
            log.info("%s member %s: %s", path.name, member_name, exc)
            duplicates += 1
            results.append({"file": f"{path.name}!{member_name}",
                            "status": "duplicate", "feed": feed.name,
                            "landed_as": exc.landing_filename,
                            "reason": str(exc)})
            continue
        except conform.ConformanceError as exc:
            # ONE BAD MEMBER DOES NOT DISCARD THE REST. The others are real
            # deliveries that arrived and can be ingested; reporting the
            # failure and continuing beats rejecting a whole week of files
            # because one of them is misnamed.
            log.warning("%s member %s rejected: %s", path.name, member_name, exc)
            # THE MEMBER'S OWN BYTES, not the container's. The container is
            # never landed -- the members are the deliveries -- so quarantining
            # the zip would keep the nineteen good members alongside the one
            # bad one and make the evidence harder to read, not easier. The
            # container is named in the reason instead.
            from reporting_platform.registry.rejections import quarantine_quietly
            quarantine_quietly(
                feed, f"{path.name}!{member_name}", member_bytes,
                reason_class="member",
                reason=f"member of {path.name}: {exc}", received_at=received)
            results.append({"file": f"{path.name}!{member_name}",
                            "status": "rejected", "feed": feed.name,
                            "reason": str(exc)})
            continue
        except Exception as exc:                        # noqa: BLE001
            log.error("upload failed for %s!%s: %s", path.name, member_name,
                      str(exc)[:300])
            results.append({"file": f"{path.name}!{member_name}",
                            "status": "upload failed", "feed": feed.name,
                            "error": str(exc)[:300]})
            continue
        taken.add(plan["landing_filename"])
        landed.append((member_name, plan, key))

    if not landed and not duplicates:
        # Nothing reached landing and nothing was already there, so the
        # container is still worth another pass or an operator's attention --
        # leave it where it is.
        return results

    moved = _move(path, PROCESSED, feed.name)
    seen.pop(path.name, None)
    for member_name, plan, key in landed:
        outcome = {"file": f"{path.name}!{member_name}", "status": "conformed",
                   "feed": feed.name, "key": key,
                   "landed_as": plan["landing_filename"],
                   "cob_date": plan["cob_date"].isoformat(),
                   "moved_to": str(moved.relative_to(INBOX))}
        outcome.update(_trigger(feed, key))
        results.append(outcome)
    log.info("unpacked %s -> %d delivery(ies) for %s%s", path.name, len(landed),
             feed.name,
             f" ({duplicates} already landed, skipped)" if duplicates else "")
    return results


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--loop", type=int, metavar="SECONDS",
                   help="poll forever at this interval instead of once")
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be ingested; move and upload nothing")
    p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")

    seen: dict[str, tuple[int, float, int]] = {}
    if not a.loop:
        results = sweep(seen, dry_run=a.dry_run)
        # Once-off cannot observe stability across passes, so give it the two
        # observations it needs rather than reporting an empty inbox.
        if not results:
            time.sleep(1)
            results = sweep(seen, dry_run=a.dry_run)
        print(json.dumps(results, indent=2) if a.json else
              "\n".join(f"{r['status']:14} {r['file']}" for r in results)
              or "inbox empty")
        return 0

    log.info("watching %s every %ss", INBOX, a.loop)
    while True:
        try:
            sweep(seen, dry_run=a.dry_run)
        except Exception:                               # noqa: BLE001
            # A watcher that dies on one bad pass stops watching, which is the
            # failure it exists to prevent.
            log.exception("sweep failed")
        time.sleep(a.loop)


if __name__ == "__main__":
    raise SystemExit(main())
