"""Run the config-level tests. No pytest, and that is deliberate.

    python -m tests.run                 # on the host, or in any container
    python -m tests.run test_conventions

These tests need only what the platform already installs (`pyyaml`,
`ruamel.yaml`), so they run inside the existing image with no rebuild. Adding
pytest to `Dockerfile.airflow` would ship a test framework into the runtime
image of six services -- compose builds one image per service from that file,
and `airflow`/`airflow-init` have to be rebuilt together -- for no runtime
benefit at all.

The tests are written as plain `test_*` functions taking no arguments, so
`pytest tests/` also works if you happen to have it. Nothing here depends on
that.
"""
from __future__ import annotations

import importlib
import pathlib
import sys
import traceback

HERE = pathlib.Path(__file__).resolve().parent


def modules(only: list[str]) -> list[str]:
    names = sorted(p.stem for p in HERE.glob("test_*.py"))
    if not only:
        return names
    chosen = [n for n in names if n in only or f"test_{n}" in only]
    missing = set(only) - set(chosen) - {f"test_{n}" for n in chosen}
    if missing:
        raise SystemExit(f"no such test module: {', '.join(sorted(missing))}\n"
                         f"available: {', '.join(names)}")
    return chosen


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    sys.path.insert(0, str(HERE.parent))
    passed, failed = 0, []

    for name in modules(argv):
        mod = importlib.import_module(f"tests.{name}")
        for attr in sorted(vars(mod)):
            if not attr.startswith("test_"):
                continue
            fn = getattr(mod, attr)
            if not callable(fn):
                continue
            try:
                fn()
            except Exception:                                  # noqa: BLE001
                failed.append((f"{name}.{attr}", traceback.format_exc()))
                print(f"FAIL  {name}.{attr}")
            else:
                passed += 1
                print(f"ok    {name}.{attr}")

    print(f"\n{passed} passed, {len(failed)} failed")
    for label, tb in failed:
        print(f"\n--- {label} ---\n{tb}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
