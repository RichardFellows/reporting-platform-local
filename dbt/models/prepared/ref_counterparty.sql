{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    scd2_retractions=true,
    unique_key=['counterparty_id', 'effective_from'],
    partition_by=['effective_from_month'],
    tags=['prepared', 'reference', 'scd2']
  )
}}

{#
  Counterparty reference, conformed — SCD2.

  Shared by every downstream report — this is the single conformed
  representation the legacy estate never had. Each of the legacy apps built
  its own counterparty view and they diverged; the whole point of putting this
  in prepared and referencing it with ref() is that divergence becomes visible
  in the lineage graph.

  ONE ROW PER VERSION, NOT PER COB DATE. This table held 2,400 rows to
  express 70 versions: 60 counterparties that changed 10 times between them
  across 40 retained COB dates, restated in full every day. Consumers
  join point-in-time with `as_of()` instead of on equality.

  (70, not the 68 distinct attribute tuples the table contains: two names
  reverted to an earlier value, which is a third version rather than a
  duplicate of the first. `count(distinct ...)` is a lower bound on an SCD2
  row count, never the answer.)

  Two things this deliberately does NOT change:

    * `raw` and `landing` still hold every delivery, 1:1 with what arrived.
      This is where the provenance of a RESTATEMENT now lives -- see the
      lineage note in docs/ARCHITECTURE.md. The audit chain moved, it did not
      shorten.
    * The table keeps its NAME. `managed_tables()` in common/context.py,
      managed_tables(), retention and maintenance are all keyed by name, and
      renaming this to `counterparty_history` would have meant touching every
      one of them to express nothing.

  The unique key is (counterparty_id, effective_from) and NOT effective_to,
  which is what lets the incremental merge UPDATE a previously-open row to
  close it rather than inserting a second one alongside.

  MERGE, NOT insert_overwrite, unlike every date-partitioned model. The
  partition is `effective_from_month` and an incremental run re-derives only
  the touched keys, so overwriting the months it returns would truncate
  them to those keys.

  A KEY ABSENT FROM A DATE'S NEWEST DELIVERY IS TWO DIFFERENT THINGS, and
  they are treated differently:

    * It does not CLOSE the version in force. An absent counterparty is a
      gap to show, not a retirement to infer: the version carries forward
      and `counterparty_exposure` flags it (`reference_carried_forward`).
    * It does RETRACT a version that the replaced delivery itself began. If
      the 09-02 delivery changed a name and its re-delivery omits the
      counterparty, the change never happened: the 09-02 version goes and
      the one before it is open again. `scd2_retractions` below emits the
      marker rows and `scd2_retractions=true` gives this model the merge in
      macros/merge.sql that deletes on them. Without both, the retracted
      version stays current, and the key's next change opens a second one.
      `scd2_replay` compares keys only after this model's cleaning, and
      starts the replay with the version before its start, so that one can
      be reopened or extended when the start version itself is retracted.
  See docs/DECISIONS.md#a-snapshot-re-delivery-restates-the-whole-date
#}

{#
  `source_file` is the delivery on which a value FIRST appeared, not the most
  recent one to repeat it -- the more useful question, and the restatements
  are still in raw. is_active is normalised once, here (yes_no_flag), rather
  than in every consuming report: upstream sends Y/N/1/0/true/false depending
  on the release.
#}

{{ scd2_prepared(
    source_name='ref_counterparty',
    keys=['counterparty_id'],
    cleaning={
        'counterparty_id':        clean_string('counterparty_id'),
        'legal_name':             clean_string('legal_name'),
        'country_code':           'upper(' ~ clean_string('country_code') ~ ')',
        'sector':                 clean_string('sector'),
        'parent_counterparty_id': clean_string('parent_counterparty_id'),
        'is_active':              yes_no_flag(clean_string('is_active')),
    },
    hashed=['legal_name', 'country_code', 'sector', 'parent_counterparty_id', 'is_active'],
) }}
