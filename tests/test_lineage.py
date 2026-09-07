"""The exported graph is the platform's own lineage, not a second drawing of it.

WHY THIS IS A TEST AND NOT A DOC. Marquez is a consumer, and a consumer that
disagrees with the thing it consumes is worse than no consumer at all -- the
picture is what people reason from, while the rule it contradicts is the one
that quietly decides whether evidence still exists. So the property pinned
here is agreement: the edges Marquez is told about are walked by
`context.model_refs()`, which is what `feeds_behind_report()` walks to size a
retention window.

The other half is CONTINUITY. The graph is only a graph if the dataset an
ingest writes is character-for-character the one the prepared model reads --
`iceberg://lakehouse` + `raw.fo_trade` from two different derivations, one
through `feeds.yml` and one through a `source()` in SQL. Drift between them
does not fail anything: it draws two nodes and no edge, and the lineage simply
looks thinner than it is. That is exactly the failure a test has to catch,
because nothing else will.

No stack. This reads the dbt project and feeds.yml off disk, like the rest of
tests/.
"""
from __future__ import annotations

from tests.support import config_dir


def _setup():
    """(context, graph) pointed at this checkout.

    IMPORTED INSIDE THE TESTS, not at module scope. `context.DBT_MODELS_DIR`
    is bound at import time from DBT_PROJECT_DIR, so a module-level import
    would bind the container path before `config_dir()` could set the real
    one -- which is why `config_dir()` purges `reporting_platform` from
    sys.modules and test_runs.py imports the same way.
    """
    config_dir()
    from reporting_platform.common import context
    from reporting_platform.lineage import graph
    return context, graph


def test_every_ingest_writes_the_table_its_prepared_model_reads():
    """The landing -> raw -> prepared chain is connected, per feed.

    The two sides are derived independently -- the ingest output from
    `Feed.raw_namespace`/`Feed.name`, the prepared input from the `source()`
    call in the model's SQL -- and the graph only joins up if they produce the
    identical (namespace, name).
    """
    context, graph = _setup()
    for feed_name in context.feeds():
        _, outputs = graph.ingest_io(feed_name)
        prepared_inputs, _ = graph.model_io(feed_name)
        assert outputs == [graph.raw_table(feed_name)]
        assert outputs[0] in prepared_inputs, (
            f"{feed_name}: ingest writes {outputs[0]} but its prepared model "
            f"reads {prepared_inputs} -- the graph would show two unconnected "
            f"nodes")


def test_every_model_produces_exactly_one_table():
    context, graph = _setup()
    for layer in ("prepared", "reporting"):
        for model in context.models_in(layer):
            _, outputs = graph.model_io(model)
            assert outputs == [graph.table(layer, model)]


def test_every_input_is_a_table_the_platform_manages():
    """No edge points at something that does not exist.

    A ref that resolves to no layer is dropped by `model_io` rather than
    guessed at, so the way that failure would SHOW is a model with an input
    missing -- which this catches by requiring every input to be a raw,
    prepared or reporting table the platform knows about.
    """
    context, graph = _setup()
    known = {graph.raw_table(f) for f in context.feeds()}
    for layer in ("prepared", "reporting"):
        known |= {graph.table(layer, m) for m in context.models_in(layer)}
    for layer in ("prepared", "reporting"):
        for model in context.models_in(layer):
            inputs, _ = graph.model_io(model)
            unknown = [d for d in inputs if d not in known]
            assert not unknown, f"{model} reads unmanaged {unknown}"


def test_the_graph_reaches_the_same_feeds_retention_does():
    """THE AGREEMENT TEST. Walking the exported edges backwards from a
    report's outputs must find exactly the feeds `feeds_behind_report()`
    names -- the function that decides whether a feed's landing evidence
    outlives the pins of the reports built from it.

    Two derivations of one answer is the failure this repo keeps rejecting.
    They share a walker so that they cannot diverge; this asserts they have
    not.
    """
    context, graph = _setup()
    producer = {}
    for layer in ("prepared", "reporting"):
        for model in context.models_in(layer):
            inputs, outputs = graph.model_io(model)
            producer[outputs[0]] = inputs

    raw_of = {graph.raw_table(f): f for f in context.feeds()}
    for name in context.reports():
        queue = [graph.table("reporting", m) for m in context.reports()[name]["models"]]
        seen, found = set(), set()
        while queue:
            dataset = queue.pop()
            if dataset in seen:
                continue
            seen.add(dataset)
            if dataset in raw_of:
                found.add(raw_of[dataset])
                continue
            queue.extend(producer.get(dataset, ()))
        assert found == set(context.feeds_behind_report(name)), (
            f"{name}: the graph reaches {sorted(found)}, retention sizes "
            f"windows for {context.feeds_behind_report(name)}")


def test_a_dataset_name_carries_its_layer():
    """`raw.fo_trade` and `prepared.fo_trade` are different nodes.

    Every layer holds a table per feed with the SAME name, so a dataset named
    by the table alone would collapse the three layers into one node and the
    graph would show a table that feeds itself.
    """
    context, graph = _setup()
    assert graph.raw_table("fo_trade") != graph.table("prepared", "fo_trade")
    assert graph.table("prepared", "fo_trade") == (
        graph.CATALOG_NAMESPACE, "prepared.fo_trade")


def test_a_landing_dataset_carries_the_files_own_column_names():
    """Landing shows what the UPSTREAM sent; raw shows what the platform calls
    it. A feed may name a column differently in the file than in the platform
    (`- trade_id: "Trade Id"`), and the rename happens at ingest -- so the two
    ends of that edge legitimately differ, and the graph is where somebody can
    see that they do. `Feed.file_header` is the same list schema drift is
    measured against; this asserts the export reads it rather than the
    platform-side names.

    Uses a synthetic feed because no shipped feed renames a column, which is
    exactly why this could regress unnoticed.
    """
    from tests.support import feeds_from, synthetic

    feeds_from(synthetic(feed_extra='    source_columns: {v: "V Column"}\n'))
    from reporting_platform.lineage import schemas

    assert schemas.landing_columns("t_one") == [("k", "string"),
                                                ("V Column", "string")]


def test_an_unreadable_catalog_costs_the_schema_and_not_the_edge():
    """THE TOTALITY PROPERTY. Schemas are read from the published tables
    through DuckDB, so they depend on a catalog being reachable -- and this
    runs inside an OpenLineage extractor, where an exception costs the
    DATASETS of whatever was being extracted. No catalog here (these tests
    take no stack), so this is the failing case in its natural state: it must
    return no columns and raise nothing, leaving the edge intact.
    """
    context, graph = _setup()
    from reporting_platform.lineage import schemas

    assert schemas.table_columns("prepared", "fo_trade") == []
    assert schemas.columns_for((graph.CATALOG_NAMESPACE, "raw.fo_trade")) == []
    inputs, outputs = graph.model_io("fo_trade")
    assert outputs == [graph.table("prepared", "fo_trade")]


def test_ingest_column_lineage_is_the_declared_rename():
    """Landing -> raw is a RENAME, not a query, so it is not parsed.

    Ingest performs the mapping `feeds.yml` declares, so the column lineage
    for it IS `Feed.source_column()` -- and the platform's own added columns
    (`_business_date`, the provenance four) come from no file column and must
    be absent rather than invented.
    """
    from tests.support import feeds_from, synthetic

    feeds_from(synthetic(feed_extra='    source_columns: {v: "V Column"}\n'))
    from reporting_platform.lineage import columns, graph

    traced = columns.ingest_columns("t_one")
    assert set(traced) == {"k", "v"}, "only the feed's own columns"
    sources, transformation = traced["v"]
    assert sources == [(graph.landing("t_one"), "V Column")]
    assert transformation == "", "a rename has no expression to show"


def test_column_lineage_is_absent_rather_than_wrong_without_compiled_sql():
    """THE TOTALITY PROPERTY, for the parsed half.

    Column lineage is read from `target/compiled/**`, which does not exist
    until dbt has built the model -- and never exists in a test run. It must
    come back empty and raise nothing, exactly as it does for a model that has
    never been built.
    """
    _setup()
    import os

    previous = os.environ.get("DBT_TARGET_PATH")
    os.environ["DBT_TARGET_PATH"] = "/nonexistent/target"
    try:
        from reporting_platform.lineage import columns

        assert columns.model_columns("prepared", "fo_trade") == {}
    finally:
        # Restored: the tests share one process, and a module that reads this
        # at call time would see the bogus path for the rest of the run.
        if previous is None:
            del os.environ["DBT_TARGET_PATH"]
        else:
            os.environ["DBT_TARGET_PATH"] = previous
