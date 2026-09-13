# `python -m tests.run` fails inside the container

**Value** medium · **Effort** 20 minutes · **Branch** `docs/keep-years-drift` (same PR)

## What is wrong (verified 2026-09-13)

```bash
docker compose exec -T airflow python -m tests.run test_versions
#  FileNotFoundError: [Errno 2] No such file or directory: '/opt/platform/.env.example'
```

`tests/README.md` and `CLAUDE.md` both say the suite runs in the container —
that is the point of it needing only packages the image already has. But
`tests/test_versions.py` reads `.env.example` to pin `ICEBERG_VERSION` across
the five files that declare it, and `.env.example` is not in the compose
mounts, so a module-level failure aborts that module every time.

On the host it passes, which is why it has not been noticed: 515 tests pass
there and the container run is the one that fails.

## What done looks like

Either, and both are defensible:

- [ ] **Mount it**: add `./.env.example:/opt/platform/.env.example:ro` to the
      airflow service's volumes, so the container run matches the host's.
- [ ] **Or skip cleanly**: have the test report "skipped: .env.example is not
      mounted" rather than raising. Note that `tests/run.py` has no skip
      concept — adding one is a bigger change than mounting the file.
- [ ] Whichever: `docker compose exec -T airflow python -m tests.run` passes
      in full, and `tests/README.md` stays true.

## Watch out for

Editing `docker-compose.yml` needs the container **recreated**, not restarted
(`CLAUDE.md`, Environment).

## Prompt for a new session

```text
Read CLAUDE.md, then docs/todo/05-test-versions-fails-in-the-container.md.

`docker compose exec -T airflow python -m tests.run` fails: test_versions reads
/opt/platform/.env.example, which is not mounted. tests/README.md says the
suite runs in the container, so either mount the file read-only or make the
test skip with a message that says why. Recreate the container afterwards and
show the full suite passing inside it.
```
