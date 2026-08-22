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
from datetime import date, timedelta
from pathlib import Path

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
    header = ["counterparty_id", "legal_name", "country_code", "sector",
              "parent_counterparty_id", "is_active"]
    if drift:
        # New upstream column appears partway through the history. The pipeline
        # must land it in _extra_columns and warn, not fail.
        header.append("lei_code")

    rows = []
    for i, cid in enumerate(counterparty_ids()):
        active_repr = random.choice(["Y", "N", "1", "0", "true", "TRUE", "Yes"])
        if i < N_COUNTERPARTIES - 5:
            active_repr = random.choice(["Y", "1", "true"])
        row = [
            cid,
            f"Counterparty {i + 1} Holdings Ltd",
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
    header = ["counterparty_id", "agency", "rating", "rating_date", "outlook"]
    rows = []
    for i, cid in enumerate(counterparty_ids()):
        for agency in AGENCIES:
            if random.random() < 0.15:
                continue  # not every agency rates every name
            rows.append([
                cid, agency,
                RATINGS[(i + AGENCIES.index(agency)) % len(RATINGS)],
                # Mixed date formats on purpose.
                bd.strftime("%Y-%m-%d") if i % 2 else bd.strftime("%Y%m%d"),
                random.choice(["STABLE", "POSITIVE", "NEGATIVE", ""]),
            ])
    suffix = f"_v{version}" if version > 1 else ""
    write_csv(out / f"RATING_{bd:%Y%m%d}{suffix}.csv", header, rows)


def gen_trade(bd: date, out: Path, version: int = 1, inject_bad: bool = False,
              clean: bool = False) -> None:
    header = ["trade_id", "counterparty_id", "book", "product_type", "notional",
              "currency", "trade_date", "maturity_date", "mtm_value"]
    cps = counterparty_ids()
    rows = []
    for t in range(N_TRADES):
        cid = cps[t % len(cps)]
        # One trade per file references a counterparty absent from the
        # reference feed, so the relationships test has something to catch.
        if t == 7 and not clean:
            cid = "CP99999"

        notional = round(random.uniform(50_000, 25_000_000), 2)
        notional_str = f"{notional}"
        if inject_bad and t == 13 and not clean:
            notional_str = "N/A"          # must land as NULL and fail a test

        trade_dt = bd - timedelta(days=random.randint(0, 900))
        maturity = trade_dt + timedelta(days=random.randint(30, 3650))
        mtm = round(random.uniform(-2_000_000, 5_000_000), 2)
        # Version 2 of a date is a genuine restatement, not a byte-identical
        # resend — the values move.
        if version > 1:
            mtm = round(mtm * random.uniform(0.9, 1.1), 2)

        rows.append([
            f"TRD{bd:%Y%m%d}{t:06d}",
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

    for d in days:
        gen_trade(d, a.out / "trade", version=a.version, clean=a.clean,
                  inject_bad=(d in dense and d == sorted(dense)[-6]))
        if d != skip_cpty:
            gen_counterparty(d, a.out / "counterparty", version=a.version,
                             drift=bool(drift_from and d >= drift_from))
        if d.weekday() == 0 or d in sparse:      # ratings arrive weekly-ish
            gen_rating(d, a.out / "rating", version=a.version)

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
