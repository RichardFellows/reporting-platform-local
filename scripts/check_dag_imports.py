"""Prove every DAG file in `airflow/dags/` imports, and say what it produced.

    python -m scripts.check_dag_imports            # the configured dags folder
    python -m scripts.check_dag_imports <folder>

WHY THIS IS NOT `python -c "import dbt_builds"`. A DAG file is not an ordinary
module: `dbt_builds.py` renders its task graph AT IMPORT by shelling out to
`dbt ls` (LoadMode.DBT_LS), and Cosmos caches that render in an Airflow
Variable -- so the import needs an Airflow metadata database, a dbt executable,
installed dbt packages and a resolvable profile. Airflow's own `DagBag` is the
loader the scheduler uses, so it is the only loader whose silence means
anything. See docs/DECISIONS.md#cosmos-load-bearing-settings

AN EMPTY DAGBAG HAS NO IMPORT ERRORS EITHER, and that is this repo's own
recurring trap -- a subject it could not READ is not a subject that is EMPTY.
Point `DagBag` at the wrong folder, or at a checkout where the dags never
mounted, and `import_errors == {}` is exactly what a healthy run looks like.
So three things are asserted rather than one:

  1. no import error, for any file;
  2. every `*.py` in the folder contributed at least one DAG -- a file Airflow
     silently skipped (no `dag`/`airflow` token in it, an early `return`) is a
     DAG that does not exist, and DagBag reports that as nothing at all;
  3. one `ingest_<feed>` per feed in `feeds.yml` -- `feed_ingest.py` GENERATES
     its DAGs from the registry, so an empty registry imports cleanly and
     yields no ingest DAGs whatsoever. That is the same failure as 2, one
     layer down, and neither shows up as an error.

No stack: it needs no scheduler, no Spark and no catalog. It DOES need a
metadata db (`airflow db migrate` against sqlite is enough) and `dbt deps` to
have run, because Cosmos needs both.
"""
from __future__ import annotations

import pathlib
import sys


def _dags_folder(argv: list[str]) -> pathlib.Path:
    if argv:
        return pathlib.Path(argv[0]).resolve()
    from airflow.configuration import conf

    return pathlib.Path(conf.get("core", "dags_folder")).resolve()


def _dag_files(folder: pathlib.Path) -> list[pathlib.Path]:
    # `_`-prefixed and `__init__.py` are helpers by convention, not DAG files.
    return sorted(p for p in folder.glob("*.py")
                  if not p.name.startswith("_"))


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    folder = _dags_folder(argv)

    from airflow.models import DagBag

    # include_examples=False, or Airflow's shipped example DAGs pad the count
    # and any of them failing reads as a failure here.
    bag = DagBag(str(folder), include_examples=False)

    problems: list[str] = []

    for path, error in sorted(bag.import_errors.items()):
        problems.append(f"{path} did not import:\n{error}")

    files = _dag_files(folder)
    if not files:
        problems.append(f"{folder} holds no DAG files at all -- wrong folder, "
                        f"or nothing is mounted there")

    produced: dict[str, list[str]] = {}
    for dag_id, dag in bag.dags.items():
        produced.setdefault(str(pathlib.Path(dag.fileloc).resolve()),
                            []).append(dag_id)

    for path in files:
        if str(path) not in produced and str(path) not in bag.import_errors:
            problems.append(f"{path.name} imported and defined no DAG -- "
                            f"Airflow skipped it, or every DAG in it is "
                            f"conditional on something absent here")

    # The generated ingest DAGs, against the registry they are generated from.
    from reporting_platform.common.context import feeds

    expected = {f"ingest_{name}" for name in feeds()}
    if not expected:
        problems.append("feeds.yml resolved to no feeds -- feed_ingest.py then "
                        "generates nothing and imports cleanly")
    missing = sorted(expected - set(bag.dags))
    if missing:
        problems.append("no DAG was generated for: " + ", ".join(missing))

    for path in files:
        ids = produced.get(str(path), [])
        print(f"{path.name}: {len(ids)} dag(s) -- {', '.join(sorted(ids)) or '-'}")
    print(f"\n{len(bag.dags)} DAGs from {len(files)} files in {folder}")

    if problems:
        print("\n" + "\n\n".join(problems), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
