"""Generate a delivery for every feed in feeds.yml, from its own definition.

Local-stack only: sample data so a newly onboarded feed has something to run
against before the upstream starts sending.

This used to carry hand-written generators for four synthetic feeds -- a
30-month history with deliberate pathologies in it: two injected data-quality
failures, schema drift, an absent delivery, a re-delivered business date. Those
feeds are gone and so is that code. What remains is the definition-driven
generator the feed console uses (`reporting_platform/ui/sampledata.py`), driven
across every registered feed at once.

WHAT THAT COSTS, and it is worth knowing before relying on this. The old seed
could produce a build that FAILED its tests on purpose, which is the only way
to watch write-audit-publish refuse to publish. A definition-driven generator
cannot: it produces data satisfying the feed's own contract. To prove the
safety net still works, break something deliberately -- land a file with a bad
value in it -- rather than expecting the generator to.

IT CANNOT BOOTSTRAP THE FIRST FEED, and that is by design rather than a gap.
`sampledata` takes its business dates and its foreign keys from what the OTHER
feeds have already delivered, because rows dated where no reference data exists
fail a `relationships` test for reasons that say nothing about the feed under
test. With an empty `seed/` it says so and stops. For the first feed, upload a
real delivery -- the better artefact anyway, being the actual file shape rather
than a plausible one.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", type=Path, default=Path("seed"),
                   help="seed root to write into (default: seed)")
    p.add_argument("--days", type=int, default=0,
                   help="business dates per feed; 0 (the default) means every "
                        "date the other feeds have delivered on")
    p.add_argument("--rows", type=int, default=0,
                   help="rows per file; 0 means comfortably above the feed's "
                        "expected_min_rows")
    p.add_argument("--version", type=int, default=1,
                   help="emit files with a _vN suffix, i.e. as a redelivery of "
                        "the same business dates")
    p.add_argument("--feed", help="generate for this feed only")
    a = p.parse_args()

    # Imported HERE, after REPORTING_SEED_DIR is set: sampledata resolves the
    # seed root through feeddata.SEED_DIR, which is bound from that env var at
    # IMPORT time, so a module-scope import makes --out silently ineffective.
    os.environ["REPORTING_SEED_DIR"] = str(a.out)
    from reporting_platform.common.context import feeds           # noqa: E402
    from reporting_platform.ui import sampledata, scaffold         # noqa: E402

    registered = feeds()
    if not registered:
        print("no feeds in feeds.yml yet -- nothing to generate. "
              "See docs/ADDING-A-FEED.md.")
        return 0

    wanted = [f for f in registered.values() if not a.feed or f.name == a.feed]
    if a.feed and not wanted:
        print(f"no such feed: {a.feed}")
        return 1

    for feed in wanted:
        try:
            # types= is NOT optional. Without it sampledata re-guesses from the
            # column NAME, so a column declared `decimal` gets a non-numeric
            # value, the prepared model's safe_cast nulls the whole column, and
            # nothing fails -- because nulling is what safe_cast is for.
            # See docs/DECISIONS.md#resolve-types-is-authoritative
            res = sampledata.generate(feed, days=a.days, rows=a.rows,
                                      types=scaffold.resolve_types(feed),
                                      version=a.version if a.version > 1 else None)
            print(f"  {feed.name}: {len(res['written'])} file(s) "
                  f"(from feed definition)")
        except Exception as exc:                                  # noqa: BLE001
            # Not fatal. One feed the generator cannot satisfy should not cost
            # you the others, and the message has to be legible: the usual
            # cause is a feed whose columns reference data no other feed
            # provides.
            print(f"  {feed.name}: NOT generated -- {exc}")

    print(f"wrote into {a.out}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
