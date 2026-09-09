"""The OpenLineage extractor that puts `graph.py`'s datasets on the real runs.

WHY AN EXTRACTOR AND NOT A SECOND EMITTER. A task posting its own OpenLineage
events would create a second job node per model in Marquez, next to the one
Airflow already emits, and the graph would show every transformation twice. An
extractor attaches the datasets to the run Airflow is ALREADY reporting, so
there is one node per task and the operational record and the data flow are the
same object.

HOW IT IS SELECTED. `ExtractorManager.get_extractor_class` checks
`task.task_type in self.extractors` BEFORE looking for
`get_openlineage_facets_on_*` on the operator, so a custom extractor wins over
Cosmos's own -- which matters, because Cosmos HAS one and it returns nothing
here: `openlineage-integration-common` raises `NotImplementedError` for dbt's
`method: session`, which is load-bearing on this platform. Registered through
AIRFLOW__OPENLINEAGE__EXTRACTORS, read at process start -- the airflow
containers must be RECREATED, not restarted.

IT CLAIMS `_PythonDecoratedOperator`, WHICH IS EVERY `@task` IN THE ESTATE.
There is no narrower hook. Two consequences this file must honour: it runs on
`open_branch`, `publish`, every housekeeping task and everything added later,
so it must be CHEAP -- no I/O until it has recognised the task -- and it must
be TOTAL, because an extractor that raises would take the DATASETS of whatever
it was extracting with it. Every path here returns an empty lineage instead.

An unrecognised task returns `OperatorLineage()` with nothing in it, and the
manager falls back to the task's own inlets and outlets.

WHAT IS DELIBERATELY NOT HERE. `dbt_test` produces no dataset: a test reads the
models and writes nothing, and drawing it as a transformation would put a node
in the graph that never produced a table.
See docs/DECISIONS.md#lineage-is-derived-from-the-dbt-project.
"""
from __future__ import annotations

import attr
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import (
    DatasetFacet, column_lineage_dataset, schema_dataset,
)

from airflow.providers.openlineage.extractors.base import (
    BaseExtractor, OperatorLineage,
)

from reporting_platform.lineage import columns, graph, schemas

INGEST_DAG_PREFIX = "ingest_"
INGEST_TASK_ID = "ingest"

# A CUSTOM facet, and it is documented in this repo rather than in the spec,
# which is what `_schemaURL` is for. Marquez stores any facet it is given and
# returns it verbatim on the dataset -- verified here before this was built on
# -- so a producer-defined facet is a supported shape rather than a smuggled
# one.
CLASSIFICATION_SCHEMA_URL = (
    "https://github.com/RichardFellows/reporting-platform-local/blob/main/"
    "docs/DECISIONS.md#a-column-with-no-source-says-so")


@attr.define
class ColumnClassificationDatasetFacet(DatasetFacet):
    """Per-column classification, and the defect list hoisted out of it.

    `columns` is {column: {"classification": ..., "detail": ...}} for every
    column of the dataset; `unresolved` names the ones the export could not
    read, so a consumer does not have to scan the map to find out whether
    there were any.
    """

    columns: dict = attr.field(factory=dict)
    unresolved: list = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return CLASSIFICATION_SCHEMA_URL


class PlatformLineageExtractor(BaseExtractor):
    """Datasets for this platform's two kinds of producing task."""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["DbtRunLocalOperator", "_PythonDecoratedOperator"]

    def _execute_extraction(self) -> OperatorLineage:
        """The one hook. `BaseExtractor.extract()` calls this, and its
        `extract_on_complete()` calls that, so START and COMPLETE both land
        here -- which is right: the graph is a property of the project, not of
        how the run turned out, so a finished task knows nothing about its
        lineage that it did not know when it started.
        """
        try:
            model = self._dbt_model()
            if model:
                return _lineage(*graph.model_io(model),
                                columns.model_columns(graph.layer_of(model),
                                                      model))
            feed = self._ingest_feed()
            if feed:
                return _lineage(*graph.ingest_io(feed),
                                columns.ingest_columns(feed))
        except Exception:                      # noqa: BLE001 -- see the header
            self.log.warning("lineage derivation failed for %s.%s; emitting "
                             "none", self.operator.dag_id,
                             self.operator.task_id, exc_info=True)
        return OperatorLineage()

    def _dbt_model(self) -> str:
        """The model this Cosmos task builds, from Cosmos's own node config.

        IDENTITY ONLY -- the dependencies come from the project files, via
        `graph.model_io`. Cosmos resolves `unique_id` by running `dbt ls`
        (LoadMode.DBT_LS, which dbt_builds.py already calls load-bearing), so
        asking it WHICH model this task is costs nothing and is exact, where
        slicing `dbt.<model>_run` out of the task id would be a second guess
        at a Cosmos naming convention.
        """
        config = getattr(self.operator, "extra_context", None) or {}
        unique_id = (config.get("dbt_node_config") or {}).get("unique_id", "")
        # `model.<project>.<name>`; a seed, snapshot or test is not a model
        # and has no table to name.
        if not unique_id.startswith("model."):
            return ""
        return unique_id.rsplit(".", 1)[-1]

    def _ingest_feed(self) -> str:
        """The feed this task ingests, or "" if this is not an ingest task.

        DERIVED FROM THE DAG ID, which is derived from the feed name -- the
        same one string that is already the raw table, the landing prefix and
        the dbt source table (see CLAUDE.md on feed naming). So a new feed is
        recognised here with no edit, and a task that merely happens to be
        called `ingest` in some other DAG is not, because its feed would not
        be in feeds.yml.
        """
        dag_id = getattr(self.operator, "dag_id", "") or ""
        if self.operator.task_id != INGEST_TASK_ID:
            return ""
        if not dag_id.startswith(INGEST_DAG_PREFIX):
            return ""
        from reporting_platform.common.context import feeds

        feed = dag_id[len(INGEST_DAG_PREFIX):]
        return feed if feed in feeds() else ""


def _lineage(inputs, outputs, column_lineage=None) -> OperatorLineage:
    """Column lineage rides on the OUTPUT, which is where the spec puts it:
    the facet maps a dataset's OWN columns to the upstream columns each was
    computed from, so it belongs to the thing being produced.
    """
    return OperatorLineage(
        inputs=[_dataset(d) for d in inputs],
        outputs=[_dataset(d, column_lineage) for d in outputs],
    )


def _dataset(dataset: tuple[str, str], column_lineage=None) -> Dataset:
    """One dataset, with whatever facets can be derived for it.

    Each facet is OMITTED rather than sent empty when it cannot be derived --
    the table is not published yet, the catalog is unreachable, the model has
    never been compiled. An empty `fields` list is a claim that the table has
    no columns, and Marquez renders it as one; no facet leaves what it already
    knows in place.
    """
    namespace, name = dataset
    facets = {}
    schema = schemas.columns_for(dataset)
    if schema:
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name=column,
                                                            type=data_type)
                    for column, data_type in schema])
    if column_lineage:
        facets["columnLineage"] = _column_lineage(column_lineage)
        facets["columnClassification"] = _column_classification(column_lineage)
    return Dataset(namespace=namespace, name=name, facets=facets or None)


def _column_lineage(traced) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    """EVERY column, including the ones with no input field (R-LIN-8).

    A sourceless column is emitted with an EMPTY `inputFields` rather than
    omitted, and rather than given a fabricated input to make the entry look
    well-formed. Checked against the running Marquez before being relied on,
    because the spec does not settle it: an entry with `inputFields: []` is
    accepted (201) and read back verbatim out of the dataset's facets. What it
    does NOT do is appear in Marquez's `/api/v1/column-lineage` graph, which is
    built from edges and has none to build from -- so the classification rides
    in `columnClassification` alongside, where it is queryable. See
    docs/DECISIONS.md#a-column-with-no-source-says-so.
    """
    fields = {}
    for column, lineage in traced.items():
        fields[column] = column_lineage_dataset.Fields(
            inputFields=[column_lineage_dataset.InputField(
                namespace=namespace, name=name, field=field)
                for (namespace, name), field in lineage.sources],
            # Absent for a pure rename, where the input field's own name is
            # already the whole story.
            transformationDescription=lineage.transformation or None,
            transformationType=("TRANSFORMATION" if lineage.transformation
                                else "IDENTITY"),
        )
    return column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields)


def _column_classification(traced) -> ColumnClassificationDatasetFacet:
    """WHY A SECOND FACET EXISTS AT ALL.

    `ColumnLineageDatasetFacet` can carry a sourceless column but it cannot
    SAY anything about one: its per-field vocabulary is `inputFields` and a
    transformation description, so `count(*)`, `current_timestamp()`, an
    injected literal and a column the parser could not read are all reduced to
    the same empty list. Those are the four different facts this requirement
    exists to distinguish, and the distinction is the answer to the auditor's
    question. So the class goes in a facet of its own rather than being
    encoded as a fake input field -- a fabricated edge is worse than an absent
    one -- and `unresolved` is hoisted to the top of it, because a defect
    nobody has to go looking for is a defect somebody will find.
    """
    return ColumnClassificationDatasetFacet(
        columns={column: {"classification": lineage.classification,
                          **({"detail": lineage.detail} if lineage.detail else {})}
                 for column, lineage in traced.items()},
        unresolved=columns.unresolved_columns(traced),
    )
