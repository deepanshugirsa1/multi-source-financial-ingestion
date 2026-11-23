from __future__ import annotations

import json
from pathlib import Path

from finingest.models import ProcessResult


class Checkpoint:
    """Consumer offsets plus the exact-dedupe set.

    Offsets are the last successfully processed Kafka offset per topic partition.
    The seen-id set is committed in the same write, after the lake files exist,
    so a crash either retries the batch or skips it.
    """

    def __init__(self, path: Path, consumer_group: str = "spark-bronze"):
        self.path = path
        self.consumer_group = consumer_group
        self.last_batch_id = -1
        self.replay_seq = 0
        self.offsets: dict[str, dict[str, int]] = {}
        self.seen_ids: set[str] = set()
        if path.exists():
            self._load()

    def commit(self, result: ProcessResult, batch_id: int, *, replay: bool = False) -> None:
        self.seen_ids = set(result.seen_ids)
        if replay:
            self.replay_seq = batch_id
        else:
            self.last_batch_id = batch_id
            for topic, partitions in result.offsets.items():
                current = self.offsets.setdefault(topic, {})
                for partition, offset in partitions.items():
                    current[str(partition)] = max(int(current.get(str(partition), -1)), int(offset))
        self._save()

    def _load(self) -> None:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        self.consumer_group = payload.get("consumer_group", self.consumer_group)
        self.last_batch_id = int(payload.get("last_batch_id", -1))
        self.replay_seq = int(payload.get("replay_seq", 0))
        self.offsets = {
            topic: {str(partition): int(offset) for partition, offset in partitions.items()}
            for topic, partitions in payload.get("offsets", {}).items()
        }
        self.seen_ids = set(payload.get("seen_ids", []))

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "consumer_group": self.consumer_group,
            "last_batch_id": self.last_batch_id,
            "replay_seq": self.replay_seq,
            "offsets": self.offsets,
            "seen_ids": sorted(self.seen_ids),
        }
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temporary.replace(self.path)
