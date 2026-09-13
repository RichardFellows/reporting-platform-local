# CI: a second tier that runs `dbt parse` and imports the DAGs

**Value** high · **Effort** half a day · **Branch** `ci/parse-and-dag-import`

## What is missing

`.github/workflows/config.yml` is the only workflow, and its own header says
what it does not cover:

> the image builds (the cosmos `--no-deps` trap, caught by
> `Dockerfile.airflow`'s own `dbt --version` smoke test), `dbt parse` (a bad
> `ref()` or schema YAML dbt rejects), DAG import, and column-level lineage.

```bash
ls .github/workflows/          # config.yml, and nothing else
grep -rn 'dbt parse\|DagBag\|import' .github/workflows/   # no hits
```

`CLAUDE.md` repeats it under **Still ungated**. So a bad `ref()`, a DAG that
does not import, or a dependency resolution that breaks dbt reaches you at the
next Airflow parse rather than on the PR.

## Why it matters

The two failures this catches are both silent-until-late:

* **The cosmos `--no-deps` trap.** Under Airflow's constraints,
  `astronomer-cosmos` pins `typing_extensions==4.12.2`; dbt's `mashumaro`
  needs 4.13+, and every dbt invocation then dies at import — in dbt, not
  cosmos, and not until something runs dbt. The only guard today is a
  `dbt --version` smoke test inside `Dockerfile.airflow`, which CI never runs.
  See `DECISIONS.md#cosmos-no-deps`.
* **DAG import.** `airflow/dags/dbt_builds.py` refuses a non-Spark
  `DBT_TARGET` at import time, and the two build DAGs do not import at all
  without `dbt deps` having run (`DECISIONS.md#airflow-init-four-things`). A
  DAG that does not import is invisible until the scheduler parses it.

## What done looks like

- [ ] A second job (same workflow file or a new one) that installs the image's
      real requirements rather than the config tier's four packages.
- [ ] `dbt deps` then `dbt parse` against `dbt/`, with `DBT_TARGET=spark_local`.
- [ ] An import check over `airflow/dags/*.py` that fails on ImportError —
      `python -c "import airflow.dags..."` will not work directly; use
      Airflow's own `DagBag` with `include_examples=False`, or import each
      module with a stub Airflow context.
- [ ] The cheap config tier stays as it is: ~10s, four packages. Its speed is
      the reason it runs on every push, and this new tier must not be merged
      into it.
- [ ] `CLAUDE.md`'s **CI** section updated — move `dbt parse` and DAG import
      out of "Still ungated".

## Watch out for

* Airflow 2.10.5 is deliberate (`DECISIONS.md#airflow-2-not-3`); pin the same
  version CI installs as the image does, or the import check proves nothing
  about what runs.
* `astronomer-cosmos` must be installed `--no-deps`, exactly as
  `Dockerfile.airflow` does it, or CI will install a working combination the
  image does not have.
* A post-build tier could also run
  `python -m reporting_platform.lineage --columns --require-derivable`, which
  needs a built catalog — out of scope here, but it is the natural third tier.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/01-ci-dbt-parse-and-dag-import.md.

Add a second CI tier that runs `dbt parse` and proves every DAG in
airflow/dags/ imports. The existing config job in .github/workflows/config.yml
must keep its four-package dependency set and its ~10s runtime -- read its
header comment for why before touching it.

Verify by breaking something on purpose: a bad ref() in a dbt model, and a
syntax error in a DAG, should each fail the new job. Put them back afterwards.
Update CLAUDE.md's CI section so "Still ungated" is accurate again.
```
