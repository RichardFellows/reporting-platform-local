# Tests

```bash
python -m tests.run                    # everything
python -m tests.run test_conventions   # one module
```

Runs on the host (needs `pyyaml`, `ruamel.yaml` and `duckdb`) or inside the
stack with no rebuild -- where the three modules that read the repo rather
than the package skip, and say so ("And where the rest of the repo is not",
below):

```powershell
docker compose exec -T airflow python -m tests.run
```

## What is in scope here

The parts of the platform that are **pure Python and pure config**: what
the feed registry resolves to, and what the feed console writes back into it. These
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

**And where the rest of the repo is not.** Three modules read files that are
not under `/opt/platform` at all and cannot be resolved to anywhere, because
the container holds the PACKAGE and they read the REPO:

| Module | Reads |
|---|---|
| `test_versions.py` | `.env.example`, `docker-compose.yml`, `Dockerfile.spark`, `Dockerfile.airflow` |
| `test_ci_pins.py` | `Dockerfile.airflow`, `.github/workflows/parse.yml` |
| `test_doc_claims.py` | `CLAUDE.md`, `docs/*.md`, and `git ls-files` for the source |

Mounting them is the other answer and it was rejected: it means bind-mounting
this repo's docs, CI config and git history into the runtime image of six
services so that a test can open them.

So they **skip**, through `support.repo_file()`, naming the path that is not
there -- `skip  test_versions.test_...: .env.example is not here`. A subject
that could not be READ is not a subject that is EMPTY, and a repo-text test
with no repo has to say which one it was rather than pass vacuously. In the
container the run is `512 passed, 0 failed, 14 skipped`; on the host and in
CI it is `526 passed, 0 failed` and nothing skips. (`config.yml` is the tier
that runs this suite. `parse.yml` runs `dbt parse` and `check_dag_imports`
and never invokes it.)

**That last clause is the guard, and it is not decoration.** `repo_file()`
raises `Skipped` only when there is no checkout to read; in one, a missing
file is an ordinary `AssertionError`. A skip that could fire on the host
would be a gate that cannot fail
([DECISIONS.md#a-gate-that-cannot-fail](../docs/DECISIONS.md#a-gate-that-cannot-fail)),
and every one of these tests exists to catch drift only a checkout can see.
Skips never affect the exit code. Confirm it the way this README asks below --
move `.env.example` aside and watch `test_versions` FAIL rather than skip.

**What decides "is this a checkout" is `support._CHECKOUT_MARKERS`, and none
of them is a file any test reads.** That is the requirement, not an accident:
the first version used `docker-compose.yml`, which `test_versions` reads and
one of its cases is entirely about, so renaming it to `compose.yaml` would
have turned all fourteen into skips and left CI green on a rename that should
have failed five assertions loudly. A sentinel that is also a subject cannot
notice its own subject going missing.

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
