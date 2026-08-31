{#
  Every counterparty must have exactly ONE open version.

  A singular test rather than a generic one because it catches two different
  broken states with one query, and neither is expressible as a column test:

    * MORE than one open version -- the incremental replay failed to close the
      previous one. `as_of()` matches both and doubles the row downstream.
    * ZERO open versions -- every version has been closed, so the counterparty
      simply vanishes from any point-in-time join after its last effective_to,
      with no error anywhere. `where is_current` alone would not see this,
      because the entity has no row to group.

  Returns the offending counterparties; empty means healthy.
#}
select
    counterparty_id,
    sum(case when is_current then 1 else 0 end) as open_versions
from {{ ref('counterparty') }}
group by counterparty_id
having sum(case when is_current then 1 else 0 end) <> 1
