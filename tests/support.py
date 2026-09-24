"""Helpers for the config-level tests.

These tests exercise the parts of the platform that are pure Python and pure
config: what `feeds.yml` resolves to, and what the feed console writes back.
They deliberately need NOTHING from the stack -- no Spark, no Airflow, no
MinIO -- so they run on a laptop in under a second and inside the existing
image with no rebuild.

Everything here works by pointing REPORTING_CONFIG_DIR at a throwaway copy of
the config and re-importing. `common.context` caches on the mtimes of the
registry FILES rather than on their names, so a second copy in a second
directory is a clean slate; what is NOT clean is the already-imported module
object, hence `_purge()`.

THAT IS PROCESS-WIDE STATE, SO IT IS RESTORED. `config_dir()` sets two
environment variables and empties `sys.modules` of the package under test;
without an undo, every test module inherits whatever its predecessor happened
to leave set, and the suite passes in the order it is run rather than in any
order. `reset()` is that undo, `tests/run.py` calls it between modules, and
`atexit` catches the last one -- which is also what stops a full run leaving a
temp directory behind per call.
"""
from __future__ import annotations

import atexit
import os
import pathlib
import shutil
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parent.parent
CONFIG = REPO / "reporting_platform" / "config"

# THE DAG FILES ARE NOT UNDER `REPO` IN THE CONTAINER. Several tests read a
# DAG's source to pin something it must keep doing -- the publish gate's
# position, the four load-bearing cosmos settings -- and `docker-compose.yml`
# mounts `./airflow/dags` at `/opt/airflow/dags`, beside `/opt/platform`
# rather than inside it. Resolved rather than assumed, because tests/README.md
# says this suite runs in the container and four of them did not.
DAGS = next((d for d in (REPO / "airflow" / "dags",
                         pathlib.Path("/opt/airflow/dags")) if d.is_dir()),
            REPO / "airflow" / "dags")

# ...AND NEITHER IS THE REST OF THE REPO. `DAGS` above solved this one prefix
# at a time; three modules read files that are not under `/opt/platform` at
# all and cannot be resolved to anywhere, because the container holds the
# PACKAGE and they read the REPO: `.env.example`, `docker-compose.yml`, both
# Dockerfiles, `CLAUDE.md`, `docs/` and `.github/` are not mounted, and
# `test_doc_claims` shells out to `git ls-files` with no `.git` to read.
#
# Mounting them is the other answer and it was rejected: it means bind-mounting
# this repo's docs, CI config and git history into the runtime image of six
# services so that a test can open them.
#
# So they report SKIPPED, naming the path. A subject that could not be READ is
# not a subject that is EMPTY -- see CLAUDE.md, "The one habit that matters" --
# and a repo-text test with no repo must say which one it was rather than pass
# vacuously.
# NONE OF THESE IS READ BY ANY TEST, AND THAT IS THE WHOLE REQUIREMENT. The
# first version of this used `docker-compose.yml`, which `test_versions` reads
# and one of its cases is entirely about -- so renaming it to `compose.yaml`,
# the modern default name, would have turned all fourteen of these into skips
# and left CI green on a rename that should have failed five assertions. A
# sentinel that is also a subject cannot detect its own subject going missing.
# `any`, not `all`, for the same reason: one of them being renamed must not
# decide this on its own.
_CHECKOUT_MARKERS = (".gitignore", "README.md", ".git")
IN_CHECKOUT = any((REPO / m).exists() for m in _CHECKOUT_MARKERS)


class Skipped(Exception):
    """A test that cannot run HERE. Not a pass, and not a failure.

    `tests/run.py` counts these separately and they never affect its exit
    code, so nothing downstream can read a skip as a success.
    """


def repo_file(relative: str | pathlib.Path) -> pathlib.Path:
    """`REPO / relative`, or `Skipped` when there is no repo to read.

    MISSING FROM A CHECKOUT IS A FAILURE AND MISSING FROM THE CONTAINER IS A
    SKIP, and collapsing the two is the whole trap: a skip that can fire on
    the host is a gate that cannot fail
    (`docs/DECISIONS.md#a-gate-that-cannot-fail`), and every one of these
    tests exists to catch drift that only a checkout can see. `config.yml` is
    the tier that runs this suite -- `parse.yml` runs `dbt parse` and
    `check_dag_imports` and never invokes it -- and it checks the repo out in
    full, so `IN_CHECKOUT` is true there and nothing can skip. If one ever
    does, the checkout is broken and that is a failure worth having.

    Call it INSIDE the test, never at module scope: `run.py` imports a module
    before it can attribute anything to it.
    """
    path = REPO / relative
    if path.exists():
        return path
    if IN_CHECKOUT:
        raise AssertionError(
            f"{relative} is missing from the checkout at {REPO}. This is a "
            f"checkout, so the file is meant to be here -- it was moved or "
            f"deleted and this test reads it.")
    raise Skipped(f"{relative} is not here: /opt/platform holds the package, "
                  f"not the repo")

# `REPORTING_CONFIG_DIR` is defaulted to `CONFIG` by `tests/__init__.py`,
# which runs before this module, so `_ORIGINAL_ENV` below captures the
# default and `reset()` restores to it. See that docstring for why there.

# What these variables were before any test touched them, captured once at
# import. `None` means "was not set", which is a different thing to restore to
# than any value.
_ENV_KEYS = ("REPORTING_CONFIG_DIR", "DBT_PROJECT_DIR")
_ORIGINAL_ENV = {k: os.environ.get(k) for k in _ENV_KEYS}
_MADE: list[pathlib.Path] = []


def _purge() -> None:
    for name in [m for m in sys.modules if m.startswith("reporting_platform")]:
        del sys.modules[name]


def reset() -> None:
    """Put the process back as `config_dir()` found it.

    Idempotent, and safe to call when nothing has been created. The import
    purge is part of it: leaving `reporting_platform` imported against a
    config directory that has just been deleted is the state that makes a
    failure depend on which test ran first.
    """
    for d in _MADE:
        shutil.rmtree(d, ignore_errors=True)
    _MADE.clear()
    for key, value in _ORIGINAL_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    _purge()


atexit.register(reset)


def config_dir(feeds_yml: str | None = None) -> pathlib.Path:
    """A throwaway config directory, defaulting to the REAL feeds.yml.

    Pass `feeds_yml` to test a shape the shipped config does not have. Passing
    nothing is the more valuable case: it pins what the config this repo
    actually ships resolves to.
    """
    d = pathlib.Path(tempfile.mkdtemp(prefix="rp-test-"))
    _MADE.append(d)
    if feeds_yml is None:
        # The REAL registry tree, copied. Pinning what this repo actually
        # ships is the more valuable case, and a copy rather than a symlink
        # so a test that writes (the console round-trip ones do) cannot
        # touch the working tree.
        shutil.copytree(CONFIG / "feeds", d / "feeds")
    else:
        # A fixture is still written as ONE document and split on the way in.
        # Fifty call sites build one, and a synthetic registry is far easier
        # to read whole than as four files in a string. `split_document` is
        # the same function the migration off `feeds.yml` used, so a fixture
        # cannot be assembled by different rules than the real config was.
        from reporting_platform.common import layout
        layout.split_document(feeds_yml, d)
    shutil.copy(CONFIG / "retention.yml", d / "retention.yml")
    # The third shipped config file. `maintenance_config()` reads it, and a
    # test directory without it sends the reader to the image's path, which
    # does not exist on a laptop.
    shutil.copy(CONFIG / "maintenance.yml", d / "maintenance.yml")
    os.environ["REPORTING_CONFIG_DIR"] = str(d)
    # The REAL dbt project, not a copy. `reports()`, `managed_tables()` and
    # `feeds_behind_report()` are derivations from the project directory, and
    # since phase 7 the retention interlock walks that lineage -- so a config
    # test that does not set this reads /opt/platform/dbt, which exists in the
    # image and not on a laptop. It is deliberately not copied: the project is
    # what these tests are pinning, and a copy would let it drift.
    os.environ.setdefault("DBT_PROJECT_DIR", str(REPO / "dbt"))
    _purge()
    return d


def feeds_from(feeds_yml: str | None = None):
    """(feeds dict, config dir) for the given feeds.yml text."""
    d = config_dir(feeds_yml)
    from reporting_platform.common.context import feeds
    return feeds(), d


def registry_on(d: pathlib.Path):
    """The console's registry module, pointed at this config dir's tree.

    CONFIG_ROOT is module-level, so it has to be redirected after the import
    that `config_dir()` invalidated.
    """
    import reporting_platform.ui.registry as registry
    registry.CONFIG_ROOT = d
    return registry


# A minimal feeds.yml for the shapes the shipped config does not contain.
def synthetic(conventions: str = "", feed_extra: str = "") -> str:
    return f"""
defaults:
  landing_prefix: landing
  delimiter: ","
{conventions}
feeds:
  - name: t_one
    description: d
    source_system: SRC
    filename_pattern: 'A_(?P<cob_date>\\d{{8}})\\.csv'
    business_key: [k]
    columns: [k, v]
{feed_extra}"""


def feed_text(d: pathlib.Path, name: str) -> str:
    """One feed's file as written, for asserting on what the console emitted.

    The successor to reading `feeds.yml` and slicing the block out of it by
    string search: the block IS the file now, so there is nothing to slice
    and nothing to get wrong at the boundary between two feeds.
    """
    return (d / "feeds" / f"{name}.yml").read_text(encoding="utf-8")


def registry_text(d: pathlib.Path) -> str:
    """The WHOLE registry tree concatenated -- the successor to reading the
    single `feeds.yml`.

    All three tiers, because that is what the assertions using this are
    about: "this value is declared once in the registry" is only true if the
    count covers `_defaults.yml` and `conventions/` as well as the feeds.
    Counting the feed files alone would let a value be pinned into a feed
    AND left on its convention without the count noticing.

    Sorted, so a before/after diff is stable.
    """
    root = d / "feeds"
    return "".join(f"--- {p.relative_to(root)}\n{p.read_text(encoding='utf-8')}"
                   for p in sorted(root.rglob("*.yml")))
