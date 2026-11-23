select
    canonical_account_id,
    effective_date,
    currency,
    sum(case when direction = 'inflow' then amount else 0 end) as inflow_amount,
    sum(case when direction = 'outflow' then amount else 0 end) as outflow_amount,
    sum(case when direction = 'inflow' then amount else -amount end) as net_amount,
    count(*) as event_count
from {{ ref('fct_financial_events') }}
where status in ('approved', 'posted', 'filled', 'settled')
group by canonical_account_id, effective_date, currency
