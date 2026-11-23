from __future__ import annotations

import json
import zlib
from pathlib import Path

from finingest.contracts import ContractBundle, isoformat_z, utc_now
from finingest.models import Envelope, KafkaRecord, ProducedEvent


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode("utf-8")) % partitions


class LocalKafkaLog:
    """Partitioned, offset-ordered log with the same read/append contract as the broker client.

    Records are the source of truth for replay. Consumer progress lives in the checkpoint,
    not in this file, so a reader can resume or re-read from any offset.
    """

    def __init__(self, path: Path, partitions: int):
        self.path = path
        self.partitions = partitions
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("", encoding="utf-8")

    def append(self, events: list[ProducedEvent], bundle: ContractBundle) -> list[KafkaRecord]:
        existing = self._read_all()
        next_offset: dict[tuple[str, int], int] = {}
        for record in existing:
            key = (record.topic, record.partition)
            next_offset[key] = max(next_offset.get(key, -1), record.offset) + 1
        written: list[KafkaRecord] = []
        now_ms = int(utc_now().timestamp() * 1000)
        with self.path.open("a", encoding="utf-8") as handle:
            for event in events:
                topic = bundle.topic_for(event.envelope.source)
                partition = partition_for(event.key, self.partitions)
                slot = (topic, partition)
                offset = next_offset.get(slot, 0)
                next_offset[slot] = offset + 1
                record = KafkaRecord(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    key=event.key,
                    envelope=event.envelope,
                    timestamp_ms=now_ms,
                )
                handle.write(json.dumps(_dump(record), separators=(",", ":")) + "\n")
                written.append(record)
        return written

    def read_after(self, committed: dict[str, dict[str, int]]) -> list[KafkaRecord]:
        records = []
        for record in self._read_all():
            last = committed.get(record.topic, {}).get(str(record.partition), -1)
            if record.offset > last:
                records.append(record)
        return records

    def read_all(self) -> list[KafkaRecord]:
        return self._read_all()

    def end_offsets(self) -> dict[str, dict[str, int]]:
        offsets: dict[str, dict[str, int]] = {}
        for record in self._read_all():
            topic_offsets = offsets.setdefault(record.topic, {})
            current = topic_offsets.get(str(record.partition), -1)
            topic_offsets[str(record.partition)] = max(current, record.offset)
        return offsets

    def _read_all(self) -> list[KafkaRecord]:
        if not self.path.exists():
            return []
        records: list[KafkaRecord] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            raw = json.loads(line)
            payload = raw["envelope"]
            records.append(
                KafkaRecord(
                    topic=raw["topic"],
                    partition=int(raw["partition"]),
                    offset=int(raw["offset"]),
                    key=raw["key"],
                    envelope=Envelope(
                        source=payload["source"],
                        schema_version=str(payload["schema_version"]),
                        event_id=payload["event_id"],
                        event_ts=payload["event_ts"],
                        payload=payload["payload"],
                    ),
                    timestamp_ms=int(raw["timestamp_ms"]),
                )
            )
        return records


def _dump(record: KafkaRecord) -> dict:
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "key": record.key,
        "timestamp_ms": record.timestamp_ms,
        "envelope": record.envelope.to_dict(),
        "appended_at": isoformat_z(utc_now()),
    }
