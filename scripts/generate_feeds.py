"""Generate sample upstream CSVs.

Produces a history deep enough to exercise the retention rules — you cannot
meaningfully test "10 business days plus 80 month-ends" against three days of
data, and a retention bug that only appears at month boundaries is exactly the
kind that reaches production.

The generated data deliberately includes the awkward cases:

  * a re-delivered business date (_v2), to exercise version handling
  * a day where the counterparty feed is late/absent, to exercise carry-forward
  * a trade referencing a counterparty missing from the reference feed
  * an unparseable notional, to prove the load lands and the TEST fails
  * a new upstream column appearing partway through, to exercise schema drift
  * mixed is_active representations (Y/N/1/0/true)
  * mixed date formats (yyyyMMdd and yyyy-MM-dd)

`--clean` suppresses the two injected *failures* (the orphan counterparty
reference and the unparseable notional) while keeping everything else --
redelivery, the absent feed, schema drift, mixed representations. It exists so
a build can be produced that genuinely passes its tests, which is what a
publish-to-main demonstration needs. Combine with `--version 2` to emit the
clean history as a redelivery of the same business dates: the prepared layer
takes the latest `_file_version`, so the restatement supersedes the bad data
exactly as a real upstream correction would.

Usage:
    python scripts/generate_feeds.py --months 30 --out seed/
    python scripts/generate_feeds.py --months 30 --out seed_clean/ --clean --version 2
"""
from __future__ import annotations

import argparse
import csv
import random
import zlib
from datetime import date, timedelta
from pathlib import Path

from reporting_platform.common.volatility import (
    epoch as _epoch, epoch_start as _epoch_start, stable_rng as _stable_rng)

random.seed(20260811)

COUNTRIES = ["GB", "US", "DE", "FR", "JP", "SG", "CH", "NL", "IE", "AU"]
SECTORS = ["BANKS", "INSURANCE", "SOVEREIGN", "CORPORATE", "FUNDS", "CENTRAL_CPTY"]
AGENCIES = ["MOODYS", "SP", "FITCH"]
RATINGS = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-",
           "BB+", "BB", "BB-", "B+", "B"]
PRODUCTS = ["IRS", "CDS", "FXFWD", "REPO", "EQ_SWAP", "OPTION"]
BOOKS = ["LDN_RATES", "LDN_CREDIT", "NY_RATES", "SG_FX", "LDN_EQD"]
CURRENCIES = ["GBP", "USD", "EUR", "JPY", "CHF"]

N_COUNTERPARTIES = 60
N_TRADES = 400

LIMIT_TYPES = ["PRE_SETTLEMENT", "SETTLEMENT", "ISSUER"]

# Slowly-changing values: see reporting_platform/common/volatility.py for what
# was wrong (every attribute redrawn on every delivery) and why the fix is
# shaped the way it is. The helpers live there because the feed console's
# generator needs exactly the same behaviour and two copies of this would
# drift.


# How long each attribute holds still, in days. Named rather than inlined
# because these ARE the realism model for the built-in feeds, and someone
# tuning the seed should be able to see the whole set at once.
HOLD = {
    "cpty_name": 900,        # a rebrand, once or twice in the seed's span
    "cpty_active": 500,      # a name going inactive
    "rating_grade": 260,     # an upgrade or downgrade, roughly yearly
    "rating_outlook": 130,   # outlook moves more often than the grade
    "limit_amount": 180,     # a limit review, roughly twice a year
    "limit_status": 420,     # suspension is rarer still
}


def business_days(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def counterparty_ids() -> list[str]:
    return [f"CP{i:05d}" for i in range(1, N_COUNTERPARTIES + 1)]


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)


def gen_counterparty(bd: date, out: Path, drift: bool, version: int = 1) -> None:
    """Reference data: legal name, domicile, sector, parent, active flag.

    Almost all of this is constant for the life of the counterparty, which is
    what makes it reference data. Two things move, both on long epochs: a
    handful of names rebrand, and a handful of counterparties go inactive.

    The MIXED is_active REPRESENTATIONS ARE KEPT -- Y/N/1/0/true/false across
    the file is what exercises the boolean CASE in the prepared model -- but
    each counterparty now keeps its own representation instead of drawing a
    new one every day. Varying it per name still covers every branch; varying
    it per day only manufactured churn.
    """
    header = ["counterparty_id", "legal_name", "country_code", "sector",
              "parent_counterparty_id", "is_active"]
    if drift:
        # New upstream column appears partway through the history. The pipeline
        # must land it in _extra_columns and warn, not fail.
        header.append("lei_code")

    rows = []
    for i, cid in enumerate(counterparty_ids()):
        # A rebrand: the same legal entity, restated under a new name. Only a
        # few names ever do it, and never on the same day as each other.
        name = f"Counterparty {i + 1} Holdings Ltd"
        if i % 17 == 0 and _epoch(cid, bd, HOLD["cpty_name"], "name") % 2:
            name = f"Counterparty {i + 1} Group PLC"

        # Only the last five can ever go inactive, as before -- but now it is a
        # state that persists for an epoch rather than a coin flip per file.
        active = True
        if i >= N_COUNTERPARTIES - 5:
            active = _stable_rng(cid, "active",
                                 _epoch(cid, bd, HOLD["cpty_active"], "active")
                                 ).random() > 0.4

        truthy = ["Y", "1", "true", "TRUE", "Yes"]
        falsy = ["N", "0", "false"]
        pool = truthy if active else falsy
        active_repr = pool[zlib.crc32(cid.encode()) % len(pool)]

        row = [
            cid,
            name,
            COUNTRIES[i % len(COUNTRIES)],
            SECTORS[i % len(SECTORS)],
            f"CP{((i // 10) * 10 + 1):05d}" if i % 10 else "",
            active_repr,
        ]
        if drift:
            row.append(f"LEI{i:015d}")
        rows.append(row)

    suffix = f"_v{version}" if version > 1 else ""
    write_csv(out / f"CPTY_{bd:%Y%m%d}{suffix}.csv", header, rows)


def gen_rating(bd: date, out: Path, version: int = 1) -> None:
    """Agency ratings: a grade, when it was assigned, and an outlook.

    Three things used to churn daily and no longer do. WHICH agencies rate a
    name was redrawn per file, so coverage flickered on and off; it is now
    decided once per (counterparty, agency). The grade moves on a roughly
    yearly epoch and the outlook about twice as often, which is the real
    relationship between the two.

    `rating_date` is now the date the CURRENT GRADE came into force, not the
    business date. A rating_date equal to the delivery date on every row is
    the delivery date under another name, and it guaranteed that every row
    differed from yesterday's even when the rating had not moved.
    """
    header = ["counterparty_id", "agency", "rating", "rating_date", "outlook"]
    rows = []
    for i, cid in enumerate(counterparty_ids()):
        for agency in AGENCIES:
            # Standing coverage, not a per-file coin flip.
            if _stable_rng(cid, agency, "covers").random() < 0.15:
                continue

            # Drift up or down from the name's baseline grade as the epoch
            # advances, staying inside the scale.
            base = (i + AGENCIES.index(agency)) % len(RATINGS)
            move = _stable_rng(cid, agency, "grade",
                               _epoch(cid + agency, bd, HOLD["rating_grade"],
                                      "grade")).randint(-1, 1)
            grade = RATINGS[min(max(base + move, 0), len(RATINGS) - 1)]
            assigned = _epoch_start(cid + agency, bd, HOLD["rating_grade"], "grade")

            outlook = _stable_rng(cid, agency, "outlook",
                                  _epoch(cid + agency, bd, HOLD["rating_outlook"],
                                         "outlook")
                                  ).choice(["STABLE", "STABLE", "POSITIVE",
                                            "NEGATIVE", ""])
            rows.append([
                cid, agency, grade,
                # Mixed date formats on purpose, stable per counterparty.
                assigned.strftime("%Y-%m-%d") if i % 2 else assigned.strftime("%Y%m%d"),
                outlook,
            ])
    suffix = f"_v{version}" if version > 1 else ""
    write_csv(out / f"RATING_{bd:%Y%m%d}{suffix}.csv", header, rows)


def gen_primary_limits(bd: date, out: Path, version: int = 1) -> None:
    """Primary credit limits from gcis2: one row per counterparty per type.

    `limit_id` is stable across business dates, because that is what the
    upstream system does -- a limit is a standing object that gets restated,
    not a new record each day. That is also what makes
    (business_date, limit_id) a meaningful unique key rather than a tautology.

    EVERYTHING ELSE ABOUT IT USED TO CHANGE DAILY, which made that comment
    describe an intent the data contradicted. `limit_amount` was redrawn per
    file, so LIM00001IS carried 35 different amounts over its 35 delivered
    dates; `effective_date` and `expiry_date` were computed as offsets from the
    business date, so a limit's own start date moved every time it was
    delivered. A limit's effective date is a property of the limit.

    Now: the amount holds for a review cycle, the status for longer, and the
    two dates are fixed for the life of the limit -- anchored to `bd.year` only
    through the limit's own hash, never through the delivery date.
    """
    header = ["limit_id", "counterparty_id", "limit_type", "limit_amount",
              "currency", "effective_date", "expiry_date", "status"]
    rows = []
    for i, cid in enumerate(counterparty_ids()):
        for k, ltype in enumerate(LIMIT_TYPES):
            lid = f"LIM{cid[2:]}{ltype[:2]}"
            # Standing: whether this counterparty holds this limit type at all
            # is a fact about the pair, not about today.
            if _stable_rng(lid, "held").random() < 0.2:
                continue

            fixed = _stable_rng(lid, "terms")
            # Immutable for the life of the limit. Anchored well before any
            # business date the seed generates, so effective_date is always in
            # the past whichever dates are emitted.
            effective = date(2019, 1, 1) + timedelta(days=fixed.randint(0, 1500))
            # An open-ended limit arrives with an EMPTY expiry, not a
            # far-future date. The prepared model treats that as "no expiry"
            # rather than as a missing value.
            expiry = ""
            if fixed.random() < 0.7:
                expiry = (effective + timedelta(days=fixed.randint(400, 4000))
                          ).strftime("%Y-%m-%d")

            amount = round(_stable_rng(lid, "amount",
                                       _epoch(lid, bd, HOLD["limit_amount"],
                                              "amount")
                                       ).uniform(1_000_000, 250_000_000), 2)
            if version > 1:
                amount = round(amount * 1.05, 2)

            status = _stable_rng(lid, "status",
                                 _epoch(lid, bd, HOLD["limit_status"], "status")
                                 ).choice(["ACTIVE"] * 8 + ["SUSPENDED", "EXPIRED"])

            rows.append([
                lid,
                cid,
                # Mixed case on purpose: prepared upper()s it.
                ltype if k % 2 else ltype.lower(),
                f"{amount}",
                CURRENCIES[i % len(CURRENCIES)],
                # Mixed date formats, same as the other feeds.
                effective.strftime("%Y-%m-%d") if i % 2 else effective.strftime("%Y%m%d"),
                expiry,
                status,
            ])

    suffix = f"_v{version}" if version > 1 else ""
    write_csv(out / f"primaryLimits_{bd:%Y%m%d}{suffix}.csv", header, rows)


# How often a trade is revalued, by how much of its life is left. A trade
# near maturity is marked every day; a ten-year swap is not repriced
# meaningfully between month-ends. This is the whole "short-dated trades move
# day on day, long-dated ones sit still for weeks" behaviour, in one table.
MTM_HOLD = ((90, 1), (365, 5), (10_000, 20))    # (residual days <=, hold days)

# The portfolio: 400 slots, each holding one live trade at a time. When a
# trade matures the slot is refilled with a new one. Tenor buckets are chosen
# so that roughly a quarter of the book is short-dated and turning over often
# while the long end persists for years.
TENORS = ((25, 20, 90), (45, 180, 540), (30, 1000, 3000))   # (weight, min, max)


def _slot_tenor(slot: int) -> int:
    """Days this slot's trades run for. Fixed per slot, so the book has a
    stable maturity profile rather than a new one every morning."""
    rng = _stable_rng("slot", slot, "tenor")
    roll, acc = rng.randint(1, 100), 0
    for weight, lo, hi in TENORS:
        acc += weight
        if roll <= acc:
            return rng.randint(lo, hi)
    return rng.randint(180, 540)


def gen_trade(bd: date, out: Path, version: int = 1, inject_bad: bool = False,
              clean: bool = False) -> None:
    """A PERSISTING BOOK, not 400 brand-new trades every morning.

    `trade_id` used to embed the business date -- TRD{bd}{n} -- so every
    delivery invented an entirely new portfolio and no trade ever appeared
    twice. `prepared.trade` held 16,400 rows with 16,400 distinct trade_ids
    across 41 dates: a book with no continuity, in which nothing could be
    compared to itself and `exposure_change` never saw an UNCHANGED row.

    Now each of the 400 slots holds one trade for its tenor and is refilled at
    maturity. Within a trade's life EVERYTHING IS IMMUTABLE -- counterparty,
    book, product, currency, notional, trade_date, maturity_date -- and only
    `mtm_value` moves, at a frequency set by how much life the trade has left
    (see MTM_HOLD). So a 30-day trade is remarked daily while a ten-year swap
    is byte-identical for a month at a time, which is what the prepared and
    reporting layers should be being asked to cope with.
    """
    header = ["trade_id", "counterparty_id", "book", "product_type", "notional",
              "currency", "trade_date", "maturity_date", "mtm_value"]
    cps = counterparty_ids()
    rows = []
    for t in range(N_TRADES):
        tenor = _slot_tenor(t)
        # The generation of trade currently occupying this slot, and the day it
        # was struck. bd always falls inside [trade_date, maturity].
        gen = _epoch(f"slot{t}", bd, tenor, "book")
        trade_dt = _epoch_start(f"slot{t}", bd, tenor, "book")
        maturity = trade_dt + timedelta(days=tenor - 1)
        tid = f"TRD{t:04d}G{gen:04d}"

        # Everything below is a property of the TRADE, so it is drawn from the
        # trade's identity and cannot move while the trade is alive.
        fixed = _stable_rng(tid, "terms")
        cid = cps[t % len(cps)]
        # One trade per file references a counterparty absent from the
        # reference feed, so the relationships test has something to catch.
        if t == 7 and not clean:
            cid = "CP99999"

        notional = round(fixed.uniform(50_000, 25_000_000), 2)
        notional_str = f"{notional}"
        if inject_bad and t == 13 and not clean:
            notional_str = "N/A"          # must land as NULL and fail a test

        residual = (maturity - bd).days
        hold = next(h for limit, h in MTM_HOLD if residual <= limit)
        mtm = round(_stable_rng(tid, "mtm", _epoch(tid, bd, hold, "mtm")
                                ).uniform(-2_000_000, 5_000_000), 2)
        # Version 2 of a date is a genuine restatement, not a byte-identical
        # resend -- the marks move, the terms do not.
        if version > 1:
            mtm = round(mtm * 1.05, 2)

        rows.append([
            tid,
            cid,
            BOOKS[t % len(BOOKS)],
            PRODUCTS[t % len(PRODUCTS)],
            notional_str,
            CURRENCIES[t % len(CURRENCIES)],
            trade_dt.strftime("%Y-%m-%d") if t % 2 else trade_dt.strftime("%Y%m%d"),
            maturity.strftime("%Y-%m-%d"),
            f"{mtm}",
        ])

    suffix = f"_v{version}" if version > 1 else ""
    write_csv(out / f"TRADE_{bd:%Y%m%d}{suffix}.csv", header, rows)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--months", type=int, default=30,
                   help="how much history to generate (default 30, enough to "
                        "exercise month-end retention meaningfully)")
    p.add_argument("--end", type=lambda s: date.fromisoformat(s), default=date.today())
    p.add_argument("--out", type=Path, default=Path("seed"))
    p.add_argument("--dense-days", type=int, default=25,
                   help="generate every business day for this many days at the "
                        "end; earlier history is month-ends only, to keep the "
                        "seed directory a sane size")
    p.add_argument("--clean", action="store_true",
                   help="omit the two injected FAILURES (orphan counterparty "
                        "reference, unparseable notional) so the build passes "
                        "its tests. Redelivery, the absent feed, schema drift "
                        "and mixed representations are all still generated.")
    p.add_argument("--version", type=int, default=1,
                   help="emit every file with a _vN suffix, i.e. as a "
                        "redelivery of the same business dates. Use with "
                        "--clean to restate a bad history.")
    a = p.parse_args()

    start = a.end - timedelta(days=int(a.months * 30.44))
    all_days = business_days(start, a.end)
    dense = set(all_days[-a.dense_days:])

    # Month-ends for the sparse portion: last business day of each month.
    by_month: dict[tuple[int, int], date] = {}
    for d in all_days:
        by_month[(d.year, d.month)] = d
    sparse = set(by_month.values())

    days = sorted(dense | sparse)

    # A late/absent counterparty feed on one recent day.
    skip_cpty = sorted(dense)[-3] if len(dense) >= 3 else None
    # Schema drift begins here.
    drift_from = sorted(dense)[-8] if len(dense) >= 8 else None
    # This date gets re-delivered as _v2.
    redeliver = sorted(dense)[-5] if len(dense) >= 5 else None
    # The date carrying the injected unparseable notional. Guarded like the
    # three above -- it was the one `sorted(dense)[-N]` written inline in the
    # loop below instead, so `--dense-days` under 6 died with
    # `IndexError: list index out of range` from inside the generator rather
    # than being told it had asked for too few days. `--dense-days` is a
    # documented, user-facing flag, so any value it accepts has to work.
    # (Hoisting it also stops re-sorting `dense` once per generated day.)
    inject_bad_on = sorted(dense)[-6] if len(dense) >= 6 else None

    for d in days:
        gen_trade(d, a.out / "trade", version=a.version, clean=a.clean,
                  inject_bad=(inject_bad_on is not None and d == inject_bad_on))
        if d != skip_cpty:
            gen_counterparty(d, a.out / "counterparty", version=a.version,
                             drift=bool(drift_from and d >= drift_from))
        if d.weekday() == 0 or d in sparse:      # ratings arrive weekly-ish
            gen_rating(d, a.out / "rating", version=a.version)
        gen_primary_limits(d, a.out / "primary_limits", version=a.version)

    # The deliberate _v2 restatement of one date. Skipped when the whole run is
    # already a versioned redelivery, which would otherwise collide.
    if redeliver and a.version == 1:
        gen_trade(redeliver, a.out / "trade", version=2, clean=a.clean)

    print(f"generated {len(days)} business dates into {a.out}/")
    print(f"  dense (every business day): {min(dense)}.. {max(dense)}")
    print(f"  sparse (month-ends only):   {min(sparse)}.. {max(sparse)}")
    print(f"  counterparty feed absent:   {skip_cpty}")
    print(f"  schema drift (lei_code) from: {drift_from}")
    print(f"  re-delivered as _v2:        {redeliver if a.version == 1 else 'n/a'}")
    print(f"  injected failures:          {'OMITTED (--clean)' if a.clean else 'present'}")
    print(f"  file version suffix:        {'_v%d' % a.version if a.version > 1 else 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
