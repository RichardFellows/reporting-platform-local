"""Supersession: how a later delivery relates to an earlier one (REQ-202).

THE BEHAVIOUR IS OLD AND THE DECLARATION IS NEW. `dedupe_rank` has always
implemented `full_snapshot` -- newest `_file_version` wins within a COB
date, last row in file order wins within a version -- and no feed said so.
These tests hold the two halves together: that the default is what the macro
already does, and that a mode the macro cannot serve is refused at LOAD rather
than ingested and silently reduced to whatever the newest file happened to
contain.

The macro half is verified by grepping the models rather than by compiling
them: `dbt parse` needs the dbt project and its packages, and CLAUDE.md's rule
is to grep for the CONSTRUCT rather than trust that fixing a macro reached
everything. A model that stops calling `known_as_of()` is exactly the drift
that would otherwise go unnoticed until an as-of query quietly returned
everything.
"""
from __future__ import annotations

import pathlib

from tests.support import REPO, config_dir, feeds_from, synthetic

MODELS = REPO / "dbt" / "models" / "prepared"
ENGINE = REPO / "dbt" / "macros" / "engine.sql"


# ------------------------------------------------------------------- config
def test_the_default_is_full_snapshot_which_is_what_the_macro_does():
    got, _ = feeds_from()
    assert {f.supersession_mode for f in got.values()} == {"full_snapshot"}


def test_an_explicit_full_snapshot_is_accepted():
    got, _ = feeds_from(synthetic(feed_extra="    supersession:\n      mode: full_snapshot\n"))
    assert got["t_one"].supersession_mode == "full_snapshot"


def test_a_not_built_mode_is_refused_at_load_and_says_so():
    for mode in ("delta_append", "correction"):
        try:
            feeds_from(synthetic(feed_extra=f"    supersession:\n      mode: {mode}\n"))
        except ValueError as exc:
            assert "NOT BUILT" in str(exc), str(exc)
            assert mode in str(exc)
        else:
            raise AssertionError(f"{mode} was accepted")


def test_an_unknown_mode_is_a_different_error_from_a_not_built_one():
    """A typo and a missing feature need different fixes, so they must not
    produce the same message."""
    try:
        feeds_from(synthetic(feed_extra="    supersession:\n      mode: snapshot\n"))
    except ValueError as exc:
        assert "not recognised" in str(exc)
        assert "not built" in str(exc)          # names the other two as well
    else:
        raise AssertionError("an unknown mode was accepted")


def test_an_unknown_key_inside_the_block_is_refused():
    try:
        feeds_from(synthetic(feed_extra="    supersession:\n      strategy: x\n"))
    except ValueError as exc:
        assert "unknown key" in str(exc)
    else:
        raise AssertionError("an unknown key was accepted")


def test_a_convention_can_carry_it_and_a_feed_inherits():
    """Supersession is a property of the SOURCE SYSTEM far more often than of
    one feed, which is what the conventions tier is for."""
    got, _ = feeds_from(synthetic(
        conventions=("conventions:\n"
                     "  src:\n"
                     "    supersession:\n"
                     "      mode: full_snapshot\n"),
        feed_extra="    convention: src\n"))
    assert got["t_one"].supersession_mode == "full_snapshot"


# -------------------------------------------------------------- the models
def _prepared_models() -> list[pathlib.Path]:
    return sorted(MODELS.glob("*.sql"))


def test_every_prepared_model_filters_on_knowledge_time():
    """The as-of predicate is a WHERE clause, so it has to be AT each call
    site -- there is no macro every model already calls that could carry it
    (two of the four do not call `incremental_window` on their incremental
    path at all). A model that omits it silently returns everything, whatever
    knowledge_time says."""
    missing = [p.name for p in _prepared_models()
               if "known_as_of()" not in p.read_text(encoding="utf-8")]
    assert not missing, f"prepared models with no as-of filter: {missing}"


def test_every_prepared_model_resolves_supersession_through_the_macro():
    """A hand-rolled ROW_NUMBER() is how the rule drifts between models --
    the copy-pasted CAST in the reporting layer is the precedent."""
    for path in _prepared_models():
        text = path.read_text(encoding="utf-8")
        assert "dedupe_rank(" in text, path.name
        assert "row_number() over" not in text.lower().replace(
            "{{ dedupe_rank", ""), path.name


def test_the_scaffold_writes_both_of_those():
    """A new feed must not be the one model without them. The console
    scaffolds the model, so this is where a gap would be introduced."""
    text = (REPO / "reporting_platform" / "ui" / "scaffold.py").read_text(
        encoding="utf-8")
    assert "known_as_of()" in text
    assert "dedupe_rank([" in text
    assert '"delivery_id"' in text          # the migration guard test


def test_the_delivery_ref_fallback_strips_the_prefix():
    """`_delivery_id` is a BASENAME and `_source_file` is a full key, so a
    coalesce of the two without stripping would mix two namespaces in one
    column -- every join and group-by over it silently wrong for exactly the
    rows that predate provenance."""
    macro = ENGINE.read_text(encoding="utf-8")
    body = macro[macro.index("{% macro delivery_ref()"):]
    body = body[:body.index("{% endmacro %}")]
    assert "coalesce(_delivery_id" in body
    assert "split(_source_file" in body


def test_knowledge_time_falls_back_to_ingest_ts():
    """`_received_at` is NULL for every row ingested before provenance
    existed, so an as-of filter on it alone would exclude the whole of
    history rather than include it."""
    macro = ENGINE.read_text(encoding="utf-8")
    body = macro[macro.index("{% macro known_as_of()"):]
    body = body[:body.index("{% endmacro %}")]
    assert "coalesce(_received_at, _ingest_ts)" in body
    # And it must refuse to write an as-of result into the published table.
    assert "is_incremental()" in body and "raise_compiler_error" in body
