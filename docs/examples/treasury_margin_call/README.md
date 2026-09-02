# Worked example: `treasury_margin_call`

Four business dates of a pipe-delimited delivery with a control file each, for
replaying [BUILDING-A-PIPELINE.md](../../BUILDING-A-PIPELINE.md).

The headers are deliberately awkward — `Margin Call Id`, `Call Amount (USD)` —
because real ones are, and that is what the feed's column mapping exists for.

Two entities change, so the SCD2 table has versions to close:

| business date | what changes |
|---|---|
| 20260828 | first delivery, 12 calls |
| 20260831 | nothing changes — no new SCD2 versions |
| 20260901 | MC0003 → 9999.00 |
| 20260902 | MC0006 → 5555.00 |

`Call Date` is a property of the call and is held steady, so the SCD2 hash only
moves when something really does. Put a per-delivery date in that hash and every
entity versions every day.

To run: copy them into `./inbox` and watch. The delivery lands and waits; the
control file triggers the ingest.

```bash
cp docs/examples/treasury_margin_call/* inbox/     # drop the README, it matches nothing
docker compose logs -f inbox
```
