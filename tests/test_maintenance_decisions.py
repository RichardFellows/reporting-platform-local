"""What maintenance decides to do, out of the metrics alone.

`maintenance/maintain.py` is metric-driven: `collect_metrics` reads Iceberg's
metadata tables and `decide` turns that into a list of actions. Everything
either side of `decide` needs a cluster; `decide` needs nothing, and it is the
part that can be wrong without anything failing.

Its own comment records the bug: the compaction gate once tested the
truthiness of `avg_file_size_mb`, which is rounded to 2dp, so a table
averaging under ~5 KB per file reported `0.0` -- falsy -- and was skipped.
That is silently skipping compaction on exactly the most fragmented tables,
and no run turns red for it.

The thresholds used here are the ones the shipped `maintenance.yml` declares,
restated rather than loaded: this pins the decision, and `test_conventions`
pins the config. What this cannot tell you is whether `rewrite_data_files`
actually compacts anything -- that was verified by running it.
"""
from __future__ import annotations

from reporting_platform.maintenance.maintain import decide

# As shipped in reporting_platform/config/maintenance.yml.
THRESHOLDS = {
    "compact_when_files_per_partition_gt": 50,
    "compact_when_avg_file_size_mb_lt": 32,
    "rewrite_manifests_when_manifest_count_gt": 20,
    "expire_snapshots_when_snapshot_count_gt": 100,
    "rewrite_deletes_when_delete_files_gt": 10,
}


def _metrics(**over) -> dict:
    """A healthy table: every metric comfortably inside its threshold."""
    m = {"total_files": 40, "max_files_per_partition": 4,
         "avg_file_size_mb": 210.0, "manifest_count": 3,
         "delete_file_count": 0, "snapshot_count": 12}
    m.update(over)
    return m


def test_a_healthy_table_is_left_alone():
    assert decide(_metrics(), THRESHOLDS) == []


# ------------------------------------------------------------- compaction
def test_a_tiny_average_file_size_compacts_even_when_it_rounds_to_zero():
    """THE BUG THE GATE'S COMMENT RECORDS.

    `avg_file_size_mb` is rounded to 2dp, so anything under ~5 KB per file
    reports 0.0. Gating on truthiness skipped compaction on the most
    fragmented table there is.
    """
    assert "compact" in decide(_metrics(avg_file_size_mb=0.0), THRESHOLDS)


def test_an_empty_table_is_the_one_zero_that_opts_out():
    """No files is not fragmentation, and compacting nothing is a Spark job
    per night per empty table. `total_files` is what separates the two, which
    is why the gate reads it rather than the size.
    """
    assert decide(_metrics(total_files=0, avg_file_size_mb=0.0,
                           max_files_per_partition=0), THRESHOLDS) == []


def test_too_many_files_in_one_partition_compacts():
    assert "compact" in decide(_metrics(max_files_per_partition=51), THRESHOLDS)


def test_the_file_count_threshold_is_exclusive():
    """`_gt`, as the key says. Exactly at the threshold is not over it."""
    assert decide(_metrics(max_files_per_partition=50), THRESHOLDS) == []
    assert "compact" in decide(_metrics(max_files_per_partition=51), THRESHOLDS)


def test_the_average_size_threshold_is_exclusive_the_other_way():
    """`_lt`: exactly at the target size is not under it."""
    assert decide(_metrics(avg_file_size_mb=32), THRESHOLDS) == []
    assert "compact" in decide(_metrics(avg_file_size_mb=31.99), THRESHOLDS)


def test_either_compaction_reason_is_enough_and_it_is_not_listed_twice():
    """A table can breach both. The action list drives a loop of Spark calls,
    so a duplicate is a second full rewrite of the same table.
    """
    actions = decide(_metrics(max_files_per_partition=99, avg_file_size_mb=0.1),
                     THRESHOLDS)
    assert actions.count("compact") == 1


# ------------------------------------------------------- the other actions
def test_manifest_count_over_the_threshold_rewrites_manifests():
    assert decide(_metrics(manifest_count=21), THRESHOLDS) == \
        ["rewrite_manifests"]


def test_delete_files_over_the_threshold_rewrites_deletes():
    assert decide(_metrics(delete_file_count=11), THRESHOLDS) == \
        ["rewrite_deletes"]


def test_snapshot_count_is_FLAGGED_not_performed():
    """Snapshot expiry belongs to the retention job, so that tag expiry
    provably runs first. Maintenance may only report it.
    """
    actions = decide(_metrics(snapshot_count=101), THRESHOLDS)
    assert actions == ["flag_expire_snapshots"]
    assert not any(a == "expire_snapshots" for a in actions)


def test_every_breach_is_reported_together():
    """One pass over the metrics, not the first thing that matches."""
    actions = decide(_metrics(max_files_per_partition=99, manifest_count=99,
                              delete_file_count=99, snapshot_count=999),
                     THRESHOLDS)
    assert actions == ["compact", "rewrite_manifests", "rewrite_deletes",
                       "flag_expire_snapshots"]
