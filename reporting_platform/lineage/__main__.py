"""Print the lineage this platform exports, without Airflow and without Spark.

`python -m reporting_platform.lineage` answers "what will Marquez be told",
which is the question you actually have when the graph looks wrong -- and it
answers it from the same derivation the extractor uses, so a missing edge here
is a missing edge there. Nothing in this module talks to Marquez: the export
happens per task run, through the OpenLineage listener.

`--columns` answers the other half, and it is THE SEAM WHERE `unresolved` IS
ENFORCED. The lineage package may not raise and may not fail a build -- it
runs inside an extractor, and an export is not an authority -- so a column the
parser could not read has to be caught somewhere that is allowed to say no.
This is that somewhere: it exits 1 when any column of any managed table is
`unresolved`, which makes it usable as a CI check without giving a
DESCRIPTION of the pipeline the power to stop it. It needs the catalog and the
compiled SQL, which the edge listing does not, so it is a flag rather than the
default. See `columns.py` for why that division is deliberate.
"""
from __future__ import annotations

import argparse
import json
import sys

from reporting_platform.lineage import graph


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true",
                        help="machine-readable, for a test or a diff")
    parser.add_argument("--columns", action="store_true",
                        help="classify every column of every managed table; "
                             "exits 1 if any is unresolved")
    args = parser.parse_args()

    if args.columns:
        return _columns(args.json)

    edges = graph.edges()
    if args.json:
        print(json.dumps(edges, indent=2))
        return 0

    for edge in edges:
        print(edge["job"])
        for namespace, name in edge["inputs"]:            # type: ignore[misc]
            print(f"    in   {namespace}  {name}")
        for namespace, name in edge["outputs"]:           # type: ignore[misc]
            print(f"    out  {namespace}  {name}")
    return 0


def _columns(as_json: bool) -> int:
    """Every managed table's columns, classified. Exit 1 on any defect."""
    from reporting_platform.common.context import feeds, models_in
    from reporting_platform.lineage import columns

    tables = [(graph.raw_table(f), columns.ingest_columns(f))
              for f in sorted(feeds())]
    for layer in ("prepared", "reporting"):
        for model in models_in(layer):
            tables.append((graph.table(layer, model),
                           columns.model_columns(layer, model)))

    defects = {f"{name}.{column}": traced[column].detail
               for (_, name), traced in tables
               for column in columns.unresolved_columns(traced)}

    if as_json:
        print(json.dumps({
            "tables": {name: {c: {"classification": l.classification,
                                  "sources": [[list(d), f] for d, f in l.sources],
                                  "transformation": l.transformation,
                                  "detail": l.detail}
                              for c, l in traced.items()}
                       for (_, name), traced in tables},
            "unresolved": defects,
        }, indent=2))
    else:
        for (_, name), traced in tables:
            if not traced:
                # Not "no columns" -- nothing known. A model that has never
                # been built has no compiled SQL, and a table that has never
                # been published has no column list to classify.
                print(f"{name}: not derivable (unbuilt or unpublished)")
                continue
            tally: dict[str, int] = {}
            for lineage in traced.values():
                tally[lineage.classification] = tally.get(
                    lineage.classification, 0) + 1
            summary = "  ".join(f"{k}={v}" for k, v in sorted(tally.items()))
            print(f"{name}: {len(traced)} columns  {summary}")
            for column, lineage in sorted(traced.items()):
                if lineage.classification == columns.SOURCED:
                    continue
                detail = f"  -- {lineage.detail}" if lineage.detail else ""
                print(f"    {lineage.classification:<16} {column}"
                      f"{detail}")
        print()
        if defects:
            print(f"UNRESOLVED: {len(defects)}")
            for name, detail in sorted(defects.items()):
                print(f"    {name}  -- {detail}")
        else:
            print("unresolved: none")

    return 1 if defects else 0


if __name__ == "__main__":
    sys.exit(main())
