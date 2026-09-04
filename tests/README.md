# Tests

```bash
python -m tests.run                    # everything
python -m tests.run test_conventions   # one module
```

Runs on the host (needs `pyyaml` and `ruamel.yaml`) or inside the stack with
no rebuild:

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
