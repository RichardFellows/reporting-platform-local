{#
  `REMOVED` in exposure_change means exactly this, in both directions:

    * every counterparty with a row on a date's PREVIOUS retained date in
      counterparty_exposure, and none on that date, has a REMOVED row for
      that date -- the direction the model used to miss entirely, because it
      was driven from the current date's rows (todo 16, plan #21);
    * every REMOVED row is such a counterparty -- the direction it used to get
      WRONG, labelling a present counterparty with an unknown total REMOVED.

  An invariant over whatever data the build has, not a fixture: it runs in
  every branch build and in the CI build tier. The constructed date pair is
  tests/test_exposure_change.py. Only dates exposure_change actually holds
  are checked, since an incremental build rewrites a window, not history.

  Returns the offending rows; empty means healthy.
#}

with dates as (
    select cob_date,
           lag(cob_date) over (order by cob_date) as prior_cob_date
    from (select distinct cob_date from {{ ref('counterparty_exposure') }}) d
),

disappeared as (
    select d.cob_date, prev.counterparty_id
    from dates d
    join {{ ref('counterparty_exposure') }} prev
      on prev.cob_date = d.prior_cob_date
    where d.cob_date in (select distinct cob_date from {{ ref('exposure_change') }})
      and not exists (
          select 1 from {{ ref('counterparty_exposure') }} cur
          where cur.cob_date = d.cob_date
            and cur.counterparty_id = prev.counterparty_id
      )
),

removed as (
    select cob_date, counterparty_id
    from {{ ref('exposure_change') }}
    where change_category = 'REMOVED'
)

select 'disappeared but not REMOVED' as problem, x.cob_date, x.counterparty_id
from disappeared x
where not exists (select 1 from removed r
                  where r.cob_date = x.cob_date and r.counterparty_id = x.counterparty_id)

union all

select 'REMOVED but still present' as problem, r.cob_date, r.counterparty_id
from removed r
where not exists (select 1 from disappeared x
                  where x.cob_date = r.cob_date and x.counterparty_id = r.counterparty_id)
