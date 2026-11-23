with origins as (
    select source as source_system, event_id as source_event_id
    from {{ ref('stg_bronze_events') }}
    union
    select source, event_id
    from {{ ref('stg_replay_events') }}
)
select canonical_event_id
from {{ ref('stg_canonical_events') }} as canonical
left join origins
    on canonical.source_system = origins.source_system
    and canonical.source_event_id = origins.source_event_id
where origins.source_event_id is null
