# Tests

```bash
python -m tests.run                    # everything
python -m tests.run test_conventions   # one module
```

Runs on the host (needs `pyyaml`, `ruamel.yaml` and `duckdb`) or inside the
stack with no rebuild:

```powershell
docker compose exec -T airflow python -m tests.run
```

## What is in scope here

The parts of the platform that are **pure Python and pure config**: what
`feeds.yml` resolves to, and what the feed console writes back into it. These
need nothing from the stack — no Spark, no Airflow, no MinIO — so they run in
under a second and are cheap enough to run on every change.

The normalize stage and `find_pending` run against `tests/fakes3.py`, a
40-line in-memory stand-in implementing only the four S3 calls this platform
makes. It cannot tell you whether Spark reads a part correctly or whether the
Nessie branch merges — those were verified by running them.

`test_sniff.py` is the one exception to "no MinIO, no Spark, no Airflow" and
deliberately not to the spirit of it: `reporting_platform/ingest/sniff.py`
(step 5 of `docs/DELIVERY-SHAPES.md`) calls a REAL `duckdb.connect()` against
LOCAL temp files rather than S3 -- DuckDB is an embedded library, not a
service to fake, and sniffing is the one thing here worth testing against the
genuine engine rather than a stand-in of it. What it cannot tell you is
whether `s3://lakehouse/...` reads work the same way over `httpfs` against
real MinIO -- that is verified by running it, same as everything else.

`test_raw_schema.py` is the same split applied to a Spark migration:
`plan_raw_schema` decides what an existing raw table is missing and what it
carries that `feeds.yml` no longer declares, out of the config alone, so the
decision is pinned here in a second. Whether Iceberg's `ALTER TABLE ADD
COLUMNS` actually commits on a Nessie branch is not something a test in this
directory can honestly claim, and it was verified by running it.

`test_registry.py` covers the registry's PURE parts only -- the projection
from (feed, manifest, sidecar) to a row, the `schema_version` digest, the
quarantine key shape and the date retention reads back out of it. Nothing that
talks to Postgres is covered here, deliberately: a fake database would agree
with whatever the code asked it, which is the one thing a registry test must
not do. The insert, the conflict clause, the sequence and the reconcile
gap-fill were exercised against the live stack instead.

`test_calendar_rules.py`, `test_maintenance_decisions.py` and
`test_completeness.py` cover three pure functions that decide what gets
DELETED, what gets COMPACTED and whether a monitor may report green --
`keep_set`/`expire_set`/`month_end_dates`, `maintain.decide` and
`completeness.find_gaps`. Each of their docstrings already named a bug it was
written to fix ("quietly shortened the retention window by one month, every
month"; "silently skipping compaction on exactly the most fragmented tables"),
and `find_gaps` was explicitly split out from its Spark reads so it could be
exercised without a cluster. None of them had a test. All three are the shape
this directory exists for: no stack, no fakes, and a wrong answer that nothing
else in the repo would notice.

`test_orphan_storage.py` is the same idea one step out. The sweep deletes
warehouse prefixes, so what is pinned is its REFUSALS -- an unreadable Nessie
reference, and an empty live set against a non-empty warehouse -- out of a
Nessie stand-in and an S3 stand-in, with a guard client that raises if a
delete is attempted.

**Where the DAG files are.** Four tests read a DAG's source to pin something
it must keep doing. `docker-compose.yml` mounts `./airflow/dags` at
`/opt/airflow/dags`, BESIDE `/opt/platform` rather than inside it, so
`REPO / "airflow" / "dags"` resolves on the host and not in the container --
which is where this README says the suite runs. `support.DAGS` resolves it
either way.

Everything else in this repo is verified by running it against the live stack,
which is the habit `CLAUDE.md` opens with. These tests do not replace that and
should not grow to try: a test that mocks Spark would prove the mock works.

## Why there is no pytest

`Dockerfile.airflow` builds the image for **six** services, `airflow` and
`airflow-init` have to be rebuilt together, and the result is 2.7GB. Adding a
test framework to it means that rebuild for every test-only dependency, and
ships the framework into the runtime image. These tests need only what the
platform already installs.

They are nevertheless written as plain `test_*` functions taking no arguments,
so `pytest tests/` works if you have it. Nothing depends on that.

## Clear `__pycache__` when a test result surprises you

A stale `.pyc` produced a genuinely confusing half-hour here: a guard was
disabled on purpose to check the suite noticed, and the interpreter kept
running the previous bytecode, so the evidence pointed at the wrong
conclusion. `PYTHONDONTWRITEBYTECODE=1` while iterating, or:

```bash
find . -name __pycache__ -type d -not -path "./.git/*" | xargs rm -rf
```

## A test must fail when the thing it names breaks

Worth doing deliberately, because it is easy to write one that cannot. The
undefined-convention check passed with its guard removed, because a *second*
layer raised a similar message — it now asserts on the feed name, which only
the guard it is testing knows. Break the code on purpose and confirm the
failure before trusting a green run.
