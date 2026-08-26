"""Feed management UI.

A small FastAPI service that puts a form in front of the six-file feed
onboarding in `docs/ADDING-A-FEED.md`, and a set of buttons in front of the
run sequence at the end of it: seed -> landing -> raw -> prepared ->
reporting.

It is deliberately a SEPARATE SERVICE from Airflow rather than an Airflow
plugin. Two reasons, and the second is the one that matters:

  * It edits files that Airflow reads while Airflow is running. A plugin
    editing the DAG-generating config from inside the scheduler's own process
    is a loop nobody wants to debug.
  * Airflow 2's plugin UI is FAB/Flask-App-Builder, and pinning this to it
    would tie the feed console to the Airflow major version the platform is
    mid-migration away from. This talks to Airflow over its stable REST API,
    which Airflow 3 also serves.

Nothing here is a second source of truth. Every read goes through
`common.context.feeds()` and every write goes back into
`config/feeds.yml` and the dbt project, so a feed added through the UI is
byte-for-byte the same kind of change as one added by hand -- and shows up in
`git diff` for review.
"""
