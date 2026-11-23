select
    canonical_event_id,
    source_system,
    source_event_id,
    schema_version,
    event_type,
    canonical_account_id,
    source_account_key,
    counterparty,
    instrument,
    amount,
    currency,
    direction,
    event_ts,
    effective_date,
    status,
    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ ref('stg_canonical_events') }}
where identity_status = 'resolved'
