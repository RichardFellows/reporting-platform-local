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
from reporting_platform.ingest import normalize as norm
from reporting_platform.ingest.arrival import put_landing

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
    first two is set. A CONTROL file never matches `filename_pattern` -- it
    names no business date, only says something about a delivery that does --
    so it is checked separately, after data files, once no feed's
    filename_pattern claims it. See ingest/normalize.py:is_control_file.
    """
    matched = [fd for fd in feeds().values() if fd.parse_filename(filename)]
    if len(matched) > 1:
        return None, ("matches more than one feed ("
                      + ", ".join(sorted(f.name for f in matched))
                      + ") -- overlapping filename_patterns are a "
                        "configuration error, and guessing would put the "
                        "delivery in the wrong raw table"), False
    if matched:
        return matched[0], None, False

    control_matched = [fd for fd in feeds().values()
                       if norm.is_control_file(fd, filename)]
    if len(control_matched) > 1:
        return None, ("matches more than one feed's control pattern ("
                      + ", ".join(sorted(f.name for f in control_matched))
                      + ") -- overlapping delivery.control.pattern is a "
                        "configuration error, and guessing would gate the "
                        "wrong feed's delivery"), False
    if control_matched:
        return control_matched[0], None, True

    return None, ("matches no feed's filename_pattern or control pattern -- "
                  "check the name, or the pattern in feeds.yml"), False


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
                _move(path, REJECTED)
            seen.pop(path.name, None)
            results.append({"file": path.name, "status": "rejected",
                            "reason": reason})
            continue

        if dry_run:
            results.append({"file": path.name, "status": "would ingest",
                            "feed": feed.name})
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
