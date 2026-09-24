# `python -m tests.run` on a host fails 12 tests unless `REPORTING_CONFIG_DIR` is set

**Value** medium · **Effort** 1 hour · **Branch** `fix/tests-run-config-dir`

## What is wrong (verified 2026-09-24 against `f1493ca`)

```bash
env -i HOME=$HOME PATH=$PATH python3 -m tests.run 2>&1 | tail -1
#  929 passed, 12 failed, 1 skipped
env -i HOME=$HOME PATH=$PATH REPORTING_CONFIG_DIR=$PWD/reporting_platform/config \
  python3 -m tests.run 2>&1 | grep -E 'passed'
#  941 passed, 0 failed, 1 skipped
```

Each of the 12 fails with the same error:

```text
RuntimeError: no feed registry at /opt/platform/reporting_platform/config/feeds. ...
set REPORTING_CONFIG_DIR, or mount the config directory into this service.
```

The failing tests are 6 in `test_dedupe_rank`, 5 in `test_migration_config`
and 1 in `test_doc_claims`. They read the real feed registry through
`context.feed()`/`feeds()`. `common/settings.py` defaults `CONFIG_DIR` to
the container path `/opt/platform/reporting_platform/config`. Most tests
point `REPORTING_CONFIG_DIR` at a throwaway copy (`tests/support.py`);
these 12 do not. `.github/workflows/config.yml` sets the variable
explicitly, so CI never runs the unset case.

## Why it matters

`CLAUDE.md`, `tests/README.md` and `make test` all present
`python -m tests.run` as the no-stack check that "just runs". On a fresh
clone it goes red on tests that have nothing wrong with them. That teaches
people to ignore a red suite.

## What done looks like

- [ ] `python -m tests.run` passes on a host with no `REPORTING_*`
      variables set.
- [ ] The container default in `settings.py` is unchanged. The refusal on
      an absent registry is deliberate (`layout.feed_paths`) and stays.
- [ ] CI runs the unset case at least once, or `config.yml` stops setting
      the variable, so this cannot return silently.

## Watch out for

The likely fix is for `tests/run.py` (or the 12 tests, through
`tests/support.py`) to default `REPORTING_CONFIG_DIR` to the repo's config
when it is unset. Do **not** make `settings.py` fall back to the repo path
by itself. A container without the config mounted must keep refusing
rather than find some other registry. `context.CONFIG_DIR` is read at
import time, so the variable has to be set before the first
`reporting_platform` import.

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/26-tests-run-on-a-host-fails-without-reporting-config-dir.md.
Reproduce with `env -i`, make the suite pass with no REPORTING_* variables
set without changing settings.py's container default, and make CI exercise
the unset case.
```
