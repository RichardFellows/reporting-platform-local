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
    DATASETS of whatever was being extracted. So a catalog that cannot be
    reached must cost the columns and never the edge.

    ASSERTED AS TOTALITY, NOT AS EMPTINESS. This used to assert `== []`, which
    was true only because the host has no catalog -- and it therefore FAILED
    the moment the same suite was run in the airflow container, where the
    catalog answers. That is CLAUDE.md's rule about a check whose window does
    not contain the thing it describes: the property is that these return a
    list and raise, whichever environment they are run in, and the edge is
    intact either way.
    """
    context, graph = _setup()
    from reporting_platform.lineage import schemas

    assert isinstance(schemas.table_columns("prepared", "fo_trade"), list)
    assert isinstance(
        schemas.columns_for((graph.CATALOG_NAMESPACE, "raw.fo_trade")), list)
    # A table the platform does not manage has no columns anywhere.
    assert schemas.table_columns("prepared", "no_such_model") == []
    inputs, outputs = graph.model_io("fo_trade")
    assert outputs == [graph.table("prepared", "fo_trade")]
    assert inputs, "the edge survives whatever the catalog did"


def test_ingest_column_lineage_is_the_declared_rename():
    """Landing -> raw is a RENAME, not a query, so it is not parsed.

    Ingest performs the mapping `feeds.yml` declares, so the column lineage
    for it IS `Feed.source_column()`. The feed's own columns are `sourced`,
    and they carry no transformation: a rename has no expression to show.
    """
    from tests.support import feeds_from, synthetic

    feeds_from(synthetic(feed_extra='    source_columns: {v: "V Column"}\n'))
    from reporting_platform.lineage import columns, graph

    traced = columns.ingest_columns("t_one")
    assert set(traced) == {"k", "v"}, "the feed's columns, with no raw table"
    assert traced["v"].classification == columns.SOURCED
    assert traced["v"].sources == [(graph.landing("t_one"), "V Column")]
    assert traced["v"].transformation == "", "a rename has no expression"


def test_ingest_classifies_the_platforms_own_columns_rather_than_omitting_them():
    """R-LIN-8, the ingest half.

    `_business_date`, `_ingest_ts` and the provenance four come from no file
    column. They used to be dropped, which made them indistinguishable from a
    declared column whose mapping had gone missing -- one is by construction,
    the other is a defect, and silence reported them identically. They are
    `ingest_added` now, and they are PRESENT.

    The raw table is injected rather than read: these tests take no catalog,
    and the property being pinned is what the classifier does with a raw
    column list, not whether DuckDB can fetch one.
    """
    from tests.support import feeds_from, synthetic

    feeds_from(synthetic(feed_extra='    source_columns: {v: "V Column"}\n'))
    from reporting_platform.lineage import columns, schemas

    schemas._TABLES = {"raw.t_one": [("k", "string"), ("v", "string"),
                                     ("_business_date", "date"),
                                     ("_delivery_id", "string")]}
    try:
        traced = columns.ingest_columns("t_one")
        assert set(traced) == {"k", "v", "_business_date", "_delivery_id"}
        assert traced["_business_date"].classification == columns.INGEST_ADDED
        assert traced["_delivery_id"].classification == columns.INGEST_ADDED
        assert traced["_business_date"].sources == [], "sourceless by construction"
        assert traced["k"].classification == columns.SOURCED
        assert not columns.unresolved_columns(traced), (
            "a platform column is not a defect")
    finally:
        # Per-process and deliberately not invalidated, so a test that sets it
        # owns restoring it -- see schemas._TABLES.
        schemas._TABLES = None


def test_a_raw_column_neither_declared_nor_platform_added_is_a_defect():
    """The drift case, and the reason the `_` prefix is only a TIEBREAK.

    A raw column the feed does not declare is the platform's own -- unless it
    does not look like one, in which case the table and `feeds.yml` disagree
    and that is exactly what `unresolved` exists to surface. Classifying it as
    a platform column would bury a real divergence under a reassuring name.
    """
    from tests.support import feeds_from, synthetic

    feeds_from(synthetic())
    from reporting_platform.lineage import columns, schemas

    schemas._TABLES = {"raw.t_one": [("k", "string"), ("v", "string"),
                                     ("dropped_from_feeds_yml", "string")]}
    try:
        traced = columns.ingest_columns("t_one")
        assert traced["dropped_from_feeds_yml"].classification == columns.UNRESOLVED
        assert columns.unresolved_columns(traced) == ["dropped_from_feeds_yml"]
        assert traced["dropped_from_feeds_yml"].detail, "a defect says why"
    finally:
        schemas._TABLES = None


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


# ------------------------------------------------------------------ R-LIN-8
# THE CLASSIFIER NEEDS sqlglot, AND sqlglot IS IN THE IMAGE, NOT ON THE HOST.
# It went into the UNCONSTRAINED pip block with dbt and duckdb; the host runs
# `python -m tests.run` against a checkout with only pyyaml and ruamel, which
# is the property tests/run.py exists to keep. So these tests assert the FULL
# battery wherever the parser is -- `docker compose exec -T airflow python -m
# tests.run test_lineage`, which is the run that has teeth -- and assert the
# TOTALITY property where it is not: a missing parser must cost the column
# lineage and raise nothing, because this package runs inside an OpenLineage
# extractor. Neither branch passes vacuously.
def _sqlglot() -> bool:
    try:
        import sqlglot                                       # noqa: F401
    except Exception:                                        # noqa: BLE001
        return False
    return True


# One query with every sourceless shape the shipped project produces, plus a
# real column. Hand-written rather than read from `target/compiled/`, so the
# classes are pinned by a fixture that cannot silently change under a rebuild.
_SQL = """
select
    t.trade_id                          as trade_id,
    try_cast(trim(t.notional) as decimal(28,4)) as notional,
    count(*)                            as trade_count,
    current_timestamp()                 as dbt_updated_at,
    cast('abc-123' as string)           as dbt_invocation_id
from raw.fo_trade as t
group by t.trade_id, t.notional
"""
_SCHEMA = {"raw": {"fo_trade": {"trade_id": "string", "notional": "string"}}}
_CLASSIFIED = ["trade_id", "notional", "trade_count", "dbt_updated_at",
               "dbt_invocation_id"]


def test_every_column_asked_for_comes_back_classified():
    """R-LIN-8's whole point: no column is silently dropped.

    The old shape returned only the columns that traced, so a column's absence
    meant a literal, an aggregate, or a parser failure nobody noticed -- three
    different facts rendered identically as nothing. Every requested column
    must now appear, whatever it turned out to be.
    """
    _setup()
    from reporting_platform.lineage import columns

    if not _sqlglot():
        assert columns.model_columns("prepared", "fo_trade") == {}, (
            "no parser must cost the lineage and raise nothing")
        return
    traced = columns.classify_sql(_SQL, _CLASSIFIED, _SCHEMA)
    assert set(traced) == set(_CLASSIFIED)
    assert all(l.classification for l in traced.values()), (
        "a column with no classification is the thing this replaced")


def test_each_kind_of_sourceless_column_is_told_apart():
    """The classes are distinct, and each is read off the expression node.

    `count(*)` reads no column, `current_timestamp()` is a property of the
    build, and `cast('...' as string)` is a constant dbt injected. All three
    have an empty `sources`; the class is what says which emptiness it is.
    """
    _setup()
    from reporting_platform.lineage import columns

    if not _sqlglot():
        assert columns.ingest_columns("nonexistent_feed") == {}
        return
    traced = columns.classify_sql(_SQL, _CLASSIFIED, _SCHEMA)

    assert traced["trade_id"].classification == columns.SOURCED
    assert traced["notional"].classification == columns.SOURCED
    assert traced["notional"].sources, "a sourced column names its input"

    assert traced["trade_count"].classification == columns.ROW_AGGREGATE
    assert traced["dbt_updated_at"].classification == columns.BUILD_METADATA
    assert traced["dbt_invocation_id"].classification == columns.LITERAL

    for column in ("trade_count", "dbt_updated_at", "dbt_invocation_id"):
        assert traced[column].sources == [], (
            f"{column} has no input column, and inventing one to make the "
            f"facet entry look well-formed is a fabricated edge")
        assert not traced[column].is_defect

    assert traced["trade_count"].transformation == "COUNT(*)", (
        "the classification and the SQL shown come from the SAME node")


def test_the_unresolved_class_is_reachable_and_says_why():
    """PROVEN REACHABLE, NOT ASSUMED EMPTY.

    `unresolved` is empty against the shipped project, and a class that is
    only ever asserted to be empty is a class nobody has shown can be entered
    -- so the assertion below it would be worthless. This drives it
    deliberately: sqlglot raises "Cannot find column 'x' in query" whenever
    the TABLE has a column the current SQL does not produce, which is the real
    condition (a column dropped from a model, or added by a later version) and
    is what silence used to hide.
    """
    _setup()
    from reporting_platform.lineage import columns

    if not _sqlglot():
        import os

        previous = os.environ.get("DBT_TARGET_PATH")
        os.environ["DBT_TARGET_PATH"] = "/nonexistent/target"
        try:
            assert columns.model_columns("prepared", "fo_trade") == {}
        finally:
            if previous is None:
                del os.environ["DBT_TARGET_PATH"]
            else:
                os.environ["DBT_TARGET_PATH"] = previous
        return

    traced = columns.classify_sql(_SQL, _CLASSIFIED + ["dropped_column"], _SCHEMA)
    assert traced["dropped_column"].classification == columns.UNRESOLVED
    assert traced["dropped_column"].is_defect
    assert "dropped_column" in traced["dropped_column"].detail, (
        "a defect that does not say what it was is not actionable")
    assert columns.unresolved_columns(traced) == ["dropped_column"]
    assert not columns.unresolved_columns(
        columns.classify_sql(_SQL, _CLASSIFIED, _SCHEMA)), (
        "and it does not fire on a query that resolves")


def test_the_shipped_project_has_no_unresolved_columns():
    """Zero defects against what this repo actually ships.

    STRONG ONLY WHERE THE ARTEFACTS ARE. This needs the compiled SQL under
    `DBT_TARGET_PATH` and the catalog behind `schemas.py`, which a config-level
    run on the host has neither of -- there it asserts the derivation is
    correctly UNAVAILABLE (empty, not "everything is unresolved"), which is a
    different and equally real property. Run it where the artefacts are:

        docker compose exec -T airflow python -m tests.run test_lineage

    The enforcing seam is the CLI, not this test: `python -m
    reporting_platform.lineage --columns` exits 1 on any unresolved column, so
    CI can gate on it without giving an EXPORT the power to fail a build. See
    `columns.py` on why nothing in that package refuses.
    """
    context, graph = _setup()
    from reporting_platform.lineage import columns

    defects, derivable = {}, 0
    for feed_name in context.feeds():
        traced = columns.ingest_columns(feed_name)
        derivable += bool(traced)
        defects.update({f"raw.{feed_name}.{c}": traced[c].detail
                        for c in columns.unresolved_columns(traced)})
    for layer in ("prepared", "reporting"):
        for model in context.models_in(layer):
            traced = columns.model_columns(layer, model)
            derivable += bool(traced)
            defects.update({f"{layer}.{model}.{c}": traced[c].detail
                            for c in columns.unresolved_columns(traced)})

    assert not defects, f"unresolved columns in the shipped project: {defects}"
    if not _sqlglot():
        # The host case, asserted rather than skipped: `ingest_columns` still
        # answers from `feeds.yml` alone, so what must be absent here is the
        # PARSED half.
        assert all(not columns.model_columns(layer, model)
                   for layer in ("prepared", "reporting")
                   for model in context.models_in(layer)), (
            "without the parser the parsed half must be empty, not guessed")


def test_no_classification_name_is_eaten_by_the_secrets_masker():
    """THE VALUES THIS PACKAGE EMITS PASS THROUGH AIRFLOW'S SecretsMasker.

    The OpenLineage provider redacts facet values on the way out, and this
    deployment's Postgres password is the word `platform` -- so the class
    originally called `platform_column` arrived in Marquez as `***_column`.
    The facet was well-formed and the value was corrupted, which no amount of
    reading the emitting code would have shown; it was found by reading the
    facet back off the running Marquez.

    Pinned as a NAMING RULE rather than as a stack test, because the masker is
    not importable here: no class name may contain any word that a credential
    in this estate is likely to be. `platform` is the one that bit, and it is
    in `REPORTING_DSN`/`REGISTRY_DSN` as both user and password, so it is the
    one asserted.
    """
    _setup()
    from reporting_platform.lineage import columns

    classes = [columns.SOURCED, columns.ROW_AGGREGATE, columns.BUILD_METADATA,
               columns.LITERAL, columns.INGEST_ADDED, columns.UNRESOLVED]
    for name in classes:
        assert "platform" not in name, (
            f"{name!r} contains a value the secrets masker redacts; it would "
            f"reach Marquez mangled. See columns.INGEST_ADDED.")
    assert len(set(classes)) == len(classes), "the classes are distinct"
