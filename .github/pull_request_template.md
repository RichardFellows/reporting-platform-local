<!--
plan #24: a model review checklist, automated where possible.
tests/test_model_rules.py enforces what a text scan can enforce; each dbt
item below says so. A rule it enforces will already have failed CI if it is
broken -- ticking the box is a statement that you looked, not the only thing
standing between the regression and `main`.
-->

## What changed

<!-- One or two sentences: what, and why. -->

## How verified

<!-- The exact command you ran, and its last line of output. For anything
     touching `dbt/models/` or `common/`, that is at minimum:
       python -m tests.run
     (no REPORTING_CONFIG_DIR needed: tests/__init__.py defaults it to
     this checkout's config.)
     Paste the "N passed, N failed" line itself, not a description of it. -->

## dbt checklist

Only relevant if this PR touches `dbt/models/` or `dbt/macros/`.

- [ ] Every raw→prepared model calls `{{ known_as_of() }}` in its `where` and
      `{{ source_provenance() }}` in its select.
      **Enforced** — `tests/test_model_rules.py`.
- [ ] `insert_overwrite` is used only when the model's SELECT returns each COB
      date it touches WHOLE. A filter that keeps some rows of a date and not
      others truncates that date to the part kept, and nothing restores the
      rest later — a MERGE never deletes, and `insert_overwrite` replaces
      exactly the partitions the select returns.
      **THIS ONE IS A HUMAN REVIEW ITEM AND NO TEST CAN CHECK IT** — "whole"
      is a property of what the model's WHERE clause admits, not of anything
      visible in its shape. See
      docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date.
- [ ] No business thresholds in `prepared` — a numeric literal belongs in
      config (a seed, `var()`), not hard-coded in a conforming model.
      `prepared` restates what the feed said; a threshold is somebody's
      opinion about it, and that opinion belongs in `reporting` at most.
      **Partly enforced** — `tests/test_model_rules.py` refuses any bare
      numeric literal of 5+ digits anywhere under `dbt/models/` (not only
      `prepared`), with a narrow, named allowlist for what is already known
      and tracked (see the test file); it cannot tell a threshold from an
      innocuous constant, so a short one can still hide an opinion.
- [ ] An SCD2 model goes through `scd2_prepared` once it exists (plan #15);
      until then, rank after cleaning the way `ref_counterparty.sql` does.
- [ ] The dedupe rank (`dedupe_rank(...)`) is on the CLEANED key — it runs in
      a CTE built from `cleaned` (or whatever CTE holds the
      `clean_string(<key>)` projection), never in the CTE that reads
      `source('raw', ...)` directly. Ranked on the raw key, `' T1'` and `'T1'`
      in one file both survive as distinct partitions.
      **Enforced** — `tests/test_model_rules.py`.
