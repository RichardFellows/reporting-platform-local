"""Watch a host folder and ingest whatever is dropped into it.

Drop `TRADE_20260901.csv` into `inbox/` and it lands in
`landing/trade/TRADE_20260901.csv`, the ingest DAG runs, and the file moves to
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


def route(filename: str) -> tuple[Feed | None, str | None]:
    """Which feed claims this filename, by the feeds' own patterns.

    Returns (feed, reason-it-was-rejected). Exactly one of the two is set.
    """
    matched = [fd for fd in feeds().values() if fd.parse_filename(filename)]
    if not matched:
        return None, ("matches no feed's filename_pattern -- check the name, "
                      "or the pattern in feeds.yml")
    if len(matched) > 1:
        return None, ("matches more than one feed ("
                      + ", ".join(sorted(f.name for f in matched))
                      + ") -- overlapping filename_patterns are a "
                        "configuration error, and guessing would put the "
                        "delivery in the wrong raw table")
    return matched[0], None


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


def _trigger(feed: Feed, key: str) -> dict:
    """Unpause if needed, then trigger one run for this object.

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
    run = orchestration.trigger(dag_id, conf={"object_key": key},
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

        feed, reason = route(path.name)
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
                   "key": key, "moved_to": str(moved.relative_to(INBOX))}
        outcome.update(_trigger(feed, key))
        log.info("landed %s -> %s%s", path.name, key,
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
