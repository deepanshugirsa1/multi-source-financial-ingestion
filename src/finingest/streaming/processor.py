from __future__ import annotations

import json

from finingest.contracts import ContractBundle, isoformat_z, normalize, utc_now
from finingest.models import ContractError, KafkaRecord, ProcessResult


def process_records(
    records: list[KafkaRecord],
    *,
    bundle: ContractBundle,
    seen_ids: set[str],
    batch_id: int,
    ingested_at: str | None = None,
    replay_id: str | None = None,
) -> ProcessResult:
    """Schema-enforce, deduplicate, and map identities for one micro-batch.

    Bronze raw keeps every live record, including duplicates and quarantined
    payloads. Silver canonical keeps the first valid copy of each source event id.
    Replays append to replay history and do not rewrite raw.
    """
    ingested = ingested_at or isoformat_z(utc_now())
    seen = set(seen_ids)
    batch_seen: set[str] = set()
    result = ProcessResult(seen_ids=seen)
    ordered = sorted(records, key=lambda record: (record.topic, record.partition, record.offset))
    for record in ordered:
        envelope = record.envelope
        topic_offsets = result.offsets.setdefault(record.topic, {})
        topic_offsets[str(record.partition)] = max(
            int(topic_offsets.get(str(record.partition), -1)), record.offset
        )
        try:
            canonical = normalize(envelope, bundle, ingested_at=ingested)
        except ContractError as exc:
            if replay_id:
                result.replay_rows.append(_replay_row(record, replay_id, "quarantined", ingested, batch_id))
            else:
                result.raw_rows.append(_raw_row(record, valid=False, duplicate=False, reason=exc.reason, batch_id=batch_id, ingested_at=ingested))
                result.quarantine_rows.append(_quarantine_row(record, exc.reason, batch_id, ingested))
            continue
        identity = f"{canonical['source_system']}|{canonical['source_event_id']}"
        if identity in seen or identity in batch_seen:
            if replay_id:
                result.replay_rows.append(_replay_row(record, replay_id, "duplicate", ingested, batch_id))
            else:
                result.raw_rows.append(
                    _raw_row(record, valid=True, duplicate=True, reason="", batch_id=batch_id, ingested_at=ingested)
                )
            continue
        batch_seen.add(identity)
        canonical.update(
            {
                "kafka_topic": record.topic,
                "kafka_partition": record.partition,
                "kafka_offset": record.offset,
                "batch_id": batch_id,
            }
        )
        result.canonical_rows.append(canonical)
        if replay_id:
            result.replay_rows.append(_replay_row(record, replay_id, "inserted", ingested, batch_id))
        else:
            result.raw_rows.append(
                _raw_row(record, valid=True, duplicate=False, reason="", batch_id=batch_id, ingested_at=ingested)
            )
    result.seen_ids = seen | batch_seen
    return result


def _raw_row(record: KafkaRecord, *, valid: bool, duplicate: bool, reason: str, batch_id: int, ingested_at: str) -> dict:
    return {
        "source": record.envelope.source,
        "schema_version": str(record.envelope.schema_version),
        "event_id": record.envelope.event_id,
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "event_ts": record.envelope.event_ts,
        "payload_json": json.dumps(record.envelope.payload, sort_keys=True, separators=(",", ":"), default=str),
        "is_valid": valid,
        "is_duplicate": duplicate,
        "quarantine_reason": reason,
        "batch_id": batch_id,
        "ingested_at": ingested_at,
    }


def _quarantine_row(record: KafkaRecord, reason: str, batch_id: int, ingested_at: str) -> dict:
    return {
        "source": record.envelope.source,
        "schema_version": str(record.envelope.schema_version),
        "event_id": record.envelope.event_id,
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "reason": reason,
        "payload_json": json.dumps(record.envelope.payload, sort_keys=True, separators=(",", ":"), default=str),
        "batch_id": batch_id,
        "ingested_at": ingested_at,
    }


def _replay_row(record: KafkaRecord, replay_id: str, outcome: str, replayed_at: str, batch_id: int) -> dict:
    return {
        "replay_id": replay_id,
        "source": record.envelope.source,
        "event_id": record.envelope.event_id,
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "outcome": outcome,
        "replayed_at": replayed_at,
        "batch_id": batch_id,
    }
