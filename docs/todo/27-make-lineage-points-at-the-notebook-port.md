# `make lineage` says to serve dbt docs on port 8083, which is the notebook's port

**Value** low · **Effort** 15–30 min · **Branch** `fix/dbt-docs-port`

## What is wrong (verified 2026-09-24 against `f1493ca`)

```bash
sed -n '/^lineage:/,/^$/p' Makefile
#  @echo "run: docker compose exec airflow dbt docs serve --port 8083"
#  @echo "(NOT 8082 -- that is the feed console)"
grep -n 'NOTEBOOK_HOST_PORT' docker-compose.yml
#  ports: ["${NOTEBOOK_HOST_PORT:-8083}:8083"]
grep -n 'AIRFLOW_HOST_PORT' docker-compose.yml
#  ports: ["${AIRFLOW_HOST_PORT:-8081}:8080"]   (airflow-webserver; `airflow` publishes none)
```

It fails in two ways. 8083 is the marimo notebook's host port, and the
`airflow` container publishes no port for `dbt docs serve` at all. So the
printed command starts a server that nothing on the host can reach. Until
2026-09-24 the README gave port 8082, the feed console's. That line has
been removed from the README; the Makefile still has its version.

## What done looks like

- [ ] Either publish a port for dbt docs (a free one, e.g. `8084`, behind
      a `DBT_DOCS_HOST_PORT` variable in line with the other ports) and
      print a command that works, or drop the `serve` hint and say that
      `docs generate` writes to `/opt/platform/run/dbt/target`.
- [ ] Verified by opening the page from the host, if a port is published.

## Prompt for a new session

```text
Read docs/todo/27-make-lineage-points-at-the-notebook-port.md. Make
`make lineage` print something that works from the host, or stop printing
a serve command. Verify it.
```
