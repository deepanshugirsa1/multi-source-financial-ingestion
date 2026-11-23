from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class Envelope:
    source: str
    schema_version: str
    event_id: str
    event_ts: str
    payload: dict

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ProducedEvent:
    envelope: Envelope
    key: str


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: str
    envelope: Envelope
    timestamp_ms: int


class ContractError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass
class ProcessResult:
    raw_rows: list[dict] = field(default_factory=list)
    quarantine_rows: list[dict] = field(default_factory=list)
    canonical_rows: list[dict] = field(default_factory=list)
    replay_rows: list[dict] = field(default_factory=list)
    seen_ids: set[str] = field(default_factory=set)
    offsets: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def inserted(self) -> int:
        return len(self.canonical_rows)

    @property
    def quarantined(self) -> int:
        return len(self.quarantine_rows)

    @property
    def duplicates(self) -> int:
        return sum(1 for row in self.raw_rows if row["is_duplicate"]) + sum(
            1 for row in self.replay_rows if row["outcome"] == "duplicate"
        )
