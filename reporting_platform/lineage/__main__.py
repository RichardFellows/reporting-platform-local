"""Print the lineage this platform exports, without Airflow and without Spark.

`python -m reporting_platform.lineage` answers "what will Marquez be told",
which is the question you actually have when the graph looks wrong -- and it
answers it from the same derivation the extractor uses, so a missing edge here
is a missing edge there. Nothing in this module talks to Marquez: the export
happens per task run, through the OpenLineage listener.
"""
from __future__ import annotations

import argparse
import json

from reporting_platform.lineage import graph


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true",
                        help="machine-readable, for a test or a diff")
    args = parser.parse_args()

    edges = graph.edges()
    if args.json:
        print(json.dumps(edges, indent=2))
        return

    for edge in edges:
        print(edge["job"])
        for namespace, name in edge["inputs"]:            # type: ignore[misc]
            print(f"    in   {namespace}  {name}")
        for namespace, name in edge["outputs"]:           # type: ignore[misc]
            print(f"    out  {namespace}  {name}")


if __name__ == "__main__":
    main()
