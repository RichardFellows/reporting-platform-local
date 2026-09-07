"""The dataset half of the OpenLineage export, derived from the dbt project.

Airflow already emits an event per task run, so Marquez already knows every
JOB. What it did not know was what any of them READ or WROTE, so it drew 41
disconnected boxes instead of a graph. This package supplies the datasets, and
it derives them from the same read of the same project files that
`feeds_behind_report()` uses to decide retention windows -- see
`graph.py` for why that sharing is the whole point.

MARQUEZ IS STILL A CONSUMER. Nothing here is a source of truth: the graph is
computed from the dbt project on every emit, so it cannot drift from it, and
nothing in the platform reads it back. See
docs/DECISIONS.md#openlineage-is-an-export-not-a-record.
"""
