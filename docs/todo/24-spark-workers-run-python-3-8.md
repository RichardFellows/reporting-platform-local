# The Spark workers run Python 3.8; every driver runs 3.11

**Value** medium · **Effort** 1–2 hours · **Branch** `fix/spark-worker-python`

## What is wrong (verified 2026-09-14)

```bash
docker compose exec -T spark-worker python3 --version     # Python 3.8.10
docker compose exec -T airflow python --version           # Python 3.11.11
docker compose exec -T airflow python -c "
from reporting_platform.common.spark import spark_session
s = spark_session('pyver-probe')
try: print(s.createDataFrame([(1,)], ['x']).collect())
except Exception as e: print('FAILED:', str(e).splitlines()[0][:120])
s.stop()" 2>&1 | grep -E 'MISMATCH|FAILED'
#  [PYTHON_VERSION_MISMATCH] Python in worker has different version (3, 8) than that in driver 3.11 ...
```

Anything that makes Spark run Python on an executor — `createDataFrame` from
Python objects, a Python UDF, `rdd.map`, pandas UDFs — fails on the cluster.
The platform's own ingest and dbt never do (they read files and run SQL in the
JVM), which is why nothing has shown it. Found while verifying item 09: the
first live seeding helper used `createDataFrame` and every write failed; it
was rewritten as SQL `INSERT ... SELECT`.

## Why it matters

`CLAUDE.md` requires every Spark job to run on the cluster, never
`local[*]`, so there is no local fallback that would hide it — the first
Python UDF anyone writes, or a notebook doing the obvious thing, fails with an
error about versions that names neither the image nor the compose file.

## What done looks like

- [ ] Find where the worker's Python comes from (the Spark image's base, or
      `PYSPARK_PYTHON` unset) and make the executors run the drivers' minor
      version — pin it beside the jar versions if it is an image choice.
- [ ] A check that fails when they diverge (the way `tests/test_versions.py`
      pins the jar triple), or a startup assertion.
- [ ] The probe above prints `[Row(x=1)]`.

## Prompt for a new session

```text
Read CLAUDE.md and docs/todo/24-spark-workers-run-python-3-8.md. Run its
probe. Make the Spark executors run the same Python minor version as the
drivers, and add a check that fails when they diverge.
```
