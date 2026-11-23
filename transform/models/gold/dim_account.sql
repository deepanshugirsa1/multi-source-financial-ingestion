select
    canonical_account_id,
    min(legal_name) as legal_name,
    count(*) as source_key_count
from {{ ref('identity_crosswalk') }}
group by canonical_account_id
