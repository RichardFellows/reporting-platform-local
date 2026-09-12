"""What the configuration resolves to, and where each value came from.

    python -m reporting_platform.config show <feed> [--origin]
    python -m reporting_platform.config list
    python -m reporting_platform.config check

NO STACK. This reads the registry off disk through `common.context` -- the
same loader Airflow, ingest and the console use -- so `check` is also the
cheapest possible answer to "would the platform start with this config".

WHY `--origin` IS THE POINT. A feed's settings resolve
`_defaults.yml -> convention -> feed`, and a middle tier only earns its keep
if values are NOT written where they are used. That makes "why is this feed
reading a pipe delimiter" a question about three files, and a fourth once a
convention names a `parent:`. Layering is survivable when there is a render
command; this is that command. See docs/DECISIONS.md#the-registry-is-a-directory
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import sys


def _fmt(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str) if value else "-"
    return "-" if value == "" else str(value)


def _show(name: str, with_origin: bool) -> int:
    from reporting_platform.common.context import feeds, origins
    registry = feeds()
    if name not in registry:
        print(f"no such feed: {name!r}. Known: "
              f"{', '.join(sorted(registry)) or '(none)'}", file=sys.stderr)
        return 2
    resolved = dataclasses.asdict(registry[name])
    where = origins(name) if with_origin else {}
    width = max(len(k) for k in resolved)
    for key in sorted(resolved):
        line = f"{key:<{width}}  {_fmt(resolved[key])}"
        if with_origin:
            print(f"{line:<72}  {where[key]}")
        else:
            print(line)
    return 0


def _list() -> int:
    from reporting_platform.common.context import conventions, feeds
    known = conventions()
    for name, fd in sorted(feeds().items()):
        tail = f"  <- {fd.convention}" if fd.convention else ""
        print(f"{name:<24} {fd.source_system:<12} "
              f"{len(fd.columns):>3} columns{tail}")
    print(f"\n{len(feeds())} feed(s), {len(known)} convention(s): "
          f"{', '.join(sorted(known)) or '(none)'}")
    return 0


def _check() -> int:
    """Load everything and say so. The exit code is the answer.

    Every way the registry can be wrong is already a load-time error that
    names its file -- this just provides the seam to run them in CI without
    starting a container, and reports the counts so a config that loads but
    resolves to nothing cannot pass quietly.
    """
    from reporting_platform.common.context import conventions, feeds
    try:
        registry, known = feeds(), conventions()
    except Exception as exc:                                 # noqa: BLE001
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if not registry:
        print("the registry resolved to no feeds at all", file=sys.stderr)
        return 1
    print(f"ok: {len(registry)} feed(s), {len(known)} convention(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m reporting_platform.config",
                                     description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    show = sub.add_parser("show", help="one feed, fully resolved")
    show.add_argument("feed")
    show.add_argument("--origin", action="store_true",
                      help="name the tier each value came from")
    sub.add_parser("list", help="every feed, with its convention")
    sub.add_parser("check", help="load the registry; exit 1 if it will not")
    args = parser.parse_args(argv)
    if args.cmd == "show":
        return _show(args.feed, args.origin)
    return _list() if args.cmd == "list" else _check()


if __name__ == "__main__":
    raise SystemExit(main())
