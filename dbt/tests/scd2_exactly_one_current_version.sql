{#
  Every entity in every SCD2 table must have exactly ONE open version.

  One test across every SCD2 table rather than one per model: the invariant is
  a property of the SCD2 shape, not of counterparties, and a per-model copy is
  the thing that gets forgotten when the next table is converted.

  It catches two different broken states, and neither is expressible as a
  column test:

    * MORE than one open version -- the incremental replay failed to close the
      previous one. as_of() matches both and doubles the row downstream.
    * ZERO open versions -- every version has been closed, so the entity simply
      vanishes from any point-in-time join after its last effective_to, with
      no error anywhere. `where is_current` alone cannot see this, because the
      entity has no row left to group.

  Returns the offending entities; empty means healthy.
#}

with all_entities as (

    select 'counterparty' as model,
           counterparty_id as entity, is_current
    from {{ ref('counterparty') }}

    union all
    select 'rating',
           counterparty_id || '|' || agency, is_current
    from {{ ref('rating') }}

)

select
    model,
    entity,
    sum(case when is_current then 1 else 0 end) as open_versions
from all_entities
group by model, entity
having sum(case when is_current then 1 else 0 end) <> 1
