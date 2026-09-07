"""What each task reads and writes, derived from the dbt project and feeds.yml.

DERIVED, NOT DECLARED. There is no list of edges anywhere in this repo and
there must not be one: a prepared model's inputs are the `source()`s in its
SQL, a reporting model's are its `ref()`s, and an ingest's output is the raw
table its feed names. Adding a model or a feed extends this graph on its own,
which is the same property `managed_tables()` and the DAG set already have.

IT SHARES ITS WALKER WITH RETENTION. `context.model_refs()` is what
`feeds_behind_report()` walks to decide whether a feed's landing evidence
outlives the pins of the reports built from it. Drawing the picture from a
second walker would produce exactly the failure docs/DECISIONS.md keeps
rejecting -- two answers to one question, of which the wrong one is invisible
until an under-retained feed has already lost its evidence. So the picture in
Marquez is the retention rule's own arithmetic, rendered.

NO OPENLINEAGE IMPORT HERE, on purpose. This module is pure derivation --
(namespace, name) pairs and nothing else -- so it can be read by the CLI in
any container, and so `extractor.py` is the only file that has to know what an
OpenLineage `Dataset` looks like.

DATASET NAMING. An Iceberg table is `iceberg://<catalog>` + `<layer>.<table>`,
which is `Feed.asset_uri` split at the point OpenLineage splits a dataset:
the namespace is the thing that holds tables, the name identifies one inside
it. A landing prefix is `s3://<bucket>` + `<prefix>/<feed>` -- the PREFIX, not
the object, because the graph describes the shape of the flow and a node per
delivery would redraw it 157 times on a cold load.
"""
from __future__ import annotations

from reporting_platform.common.context import (
    CATALOG, feeds, layer_of, model_refs, model_sources, models_in,
)

# A dataset is (namespace, name). Kept as a plain tuple so this module owes
# nothing to the OpenLineage client -- see the module docstring.
Dataset = tuple[str, str]

CATALOG_NAMESPACE = f"iceberg://{CATALOG}"


def table(layer: str, name: str) -> Dataset:
    """The dataset for one Iceberg table in one layer."""
    return (CATALOG_NAMESPACE, f"{layer}.{name}")


def landing(feed_name: str) -> Dataset:
    """The dataset for a feed's landing prefix -- the evidence copy.

    The bucket comes from `ingest.arrival._bucket()` rather than being parsed
    again here, for the reason this whole module exists: REPORTING_WAREHOUSE
    is already read in one place and a second reading of it is a second
    answer. `normalize.py` imports it the same way.
    """
    from reporting_platform.ingest.arrival import _bucket

    fd = feeds()[feed_name]
    return (f"s3://{_bucket()}", f"{fd.landing_prefix}/{fd.name}")


def raw_table(feed_name: str) -> Dataset:
    fd = feeds()[feed_name]
    return table(fd.raw_namespace, fd.name)


def ingest_io(feed_name: str) -> tuple[list[Dataset], list[Dataset]]:
    """(inputs, outputs) for `ingest_<feed>.ingest`.

    Landing in, raw out. This is the one edge in the graph that is NOT a dbt
    edge -- dbt's world starts at the raw source -- and it is what makes the
    picture start where the data actually arrives rather than at a table that
    appears from nowhere.
    """
    return [landing(feed_name)], [raw_table(feed_name)]


def model_io(model: str) -> tuple[list[Dataset], list[Dataset]]:
    """(inputs, outputs) for one dbt model.

    A `source('raw', 'x')` is the raw table; a `ref('y')` is whichever layer
    holds `y`. An unresolvable ref is DROPPED rather than guessed at, because
    a wrong edge in a lineage graph is worse than a missing one -- but it
    cannot happen silently for anything that matters: `feeds_behind_report()`
    raises on the same unresolvable ref, and it is the one deciding retention.

    DEDUPLICATED, because these are reads of the SQL and a model that joins a
    table twice or CTEs off it six times names it that many times.
    `exposure_change` refs `counterparty_exposure` six times; six identical
    input datasets is one edge reported as six.
    """
    layer = layer_of(model)
    if not layer:
        return [], []
    inputs = [table(schema, name) for schema, name in model_sources(layer, model)]
    inputs += [table(layer_of(ref), ref) for ref in model_refs(layer, model)
               if layer_of(ref)]
    return _unique(inputs), [table(layer, model)]


def _unique(datasets: list[Dataset]) -> list[Dataset]:
    """Order-preserving dedupe -- a stable graph is a diffable one."""
    seen: set[Dataset] = set()
    out = []
    for dataset in datasets:
        if dataset not in seen:
            seen.add(dataset)
            out.append(dataset)
    return out


def edges() -> list[dict[str, object]]:
    """The whole graph, for the CLI. Every producer the platform has."""
    out: list[dict[str, object]] = []
    for feed_name in sorted(feeds()):
        ins, outs = ingest_io(feed_name)
        out.append({"job": f"ingest_{feed_name}.ingest",
                    "inputs": ins, "outputs": outs})
    for layer, dag in (("prepared", "prepared_build"),
                       ("reporting", "reporting_build")):
        for model in models_in(layer):
            ins, outs = model_io(model)
            out.append({"job": f"{dag}.dbt.{model}_run",
                        "inputs": ins, "outputs": outs})
    return out
