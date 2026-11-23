select
    replay_id,
    source,
    event_id,
    topic,
    "partition" as source_partition,
    "offset" as source_offset,
    outcome,
    cast(replace(replace(replayed_at, 'T', ' '), 'Z', '') as timestamp) as replayed_at,
    batch_id
from read_parquet('{{ env_var("FININGEST_REPLAY_GLOB") }}', union_by_name=true)
where replay_id is not null
