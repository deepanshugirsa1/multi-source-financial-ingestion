with fact as (
    select
        canonical_account_id,
        effective_date,
        currency,
        sum(case when direction = 'inflow' then amount else 0 end) as inflow_amount,
        sum(case when direction = 'outflow' then amount else 0 end) as outflow_amount
    from {{ ref('fct_financial_events') }}
    where status in ('approved', 'posted', 'filled', 'settled')
    group by canonical_account_id, effective_date, currency
)
select fact.canonical_account_id
from fact
inner join {{ ref('fct_account_daily') }} as daily
    on fact.canonical_account_id = daily.canonical_account_id
    and fact.effective_date = daily.effective_date
    and fact.currency = daily.currency
where fact.inflow_amount <> daily.inflow_amount
    or fact.outflow_amount <> daily.outflow_amount
