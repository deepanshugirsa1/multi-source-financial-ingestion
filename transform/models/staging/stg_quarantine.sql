select
    source,
    schema_version,
    event_id,
    topic,
    "partition" as source_partition,
    "offset" as source_offset,
    reason,
    payload_json,
    batch_id,
    cast(replace(replace(ingested_at, 'T', ' '), 'Z', '') as timestamp) as ingested_at
from read_parquet('{{ env_var("FININGEST_QUARANTINE_GLOB") }}', union_by_name=true)
where event_id is not null
