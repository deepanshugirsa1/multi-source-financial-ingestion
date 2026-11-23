from __future__ import annotations

from pathlib import Path

from finingest.contracts import ContractBundle
from finingest.kafka.log import LocalKafkaLog
from finingest.storage.lake import Lake
from finingest.storage.s3mirror import mirror_file
from finingest.streaming.checkpoint import Checkpoint
from finingest.streaming.processor import process_records


class InjectedCrash(RuntimeError):
    """Raised only by tests that stop a batch before its checkpoint commit."""


def drain(
    log: LocalKafkaLog,
    lake: Lake,
    checkpoint: Checkpoint,
    bundle: ContractBundle,
    *,
    max_batch_records: int | None = None,
    crash_on_batch: int | None = None,
    crash_stage: str = "before_commit",
) -> int:
    """Read every uncommitted offset, write the lake, then commit the checkpoint.

    A batch id is reused when the process dies after the write and before the
    commit, so the parquet part is overwritten instead of duplicated.
    """
    lake.ensure()
    batches = 0
    while True:
        records = log.read_after(checkpoint.offsets)
        if max_batch_records is not None:
            records = records[:max_batch_records]
        if not records:
            return batches
        batch_id = checkpoint.last_batch_id + 1
        if crash_on_batch == batch_id and crash_stage == "before_write":
            raise InjectedCrash(f"crash before write of batch {batch_id}")
        result = process_records(
            records,
            bundle=bundle,
            seen_ids=checkpoint.seen_ids,
            batch_id=batch_id,
        )
        written = {
            "bronze/raw": lake.write_rows("bronze/raw", batch_id, result.raw_rows),
            "bronze/quarantine": lake.write_rows("bronze/quarantine", batch_id, result.quarantine_rows),
            "silver/canonical": lake.write_rows("silver/canonical", batch_id, result.canonical_rows),
        }
        if crash_on_batch == batch_id and crash_stage == "before_commit":
            raise InjectedCrash(f"crash before commit of batch {batch_id}")
        checkpoint.commit(result, batch_id)
        for dataset, path in written.items():
            mirror_file(path, dataset, batch_id)
        batches += 1
    return batches


def replay(
    log: LocalKafkaLog,
    lake: Lake,
    checkpoint: Checkpoint,
    bundle: ContractBundle,
    *,
    reason: str = "operator-replay",
) -> dict:
    """Re-read the log from offset 0 without moving the live consumer offsets.

    Already-seen event ids stay out of silver. Every record is appended to
    bronze replay history so the replay itself is auditable.
    """
    lake.ensure()
    records = log.read_all()
    batch_id = checkpoint.replay_seq + 1
    replay_id = f"{reason}-{batch_id:05d}"
    result = process_records(
        records,
        bundle=bundle,
        seen_ids=checkpoint.seen_ids,
        batch_id=batch_id,
        replay_id=replay_id,
    )
    path = lake.write_rows("bronze/replay", batch_id, result.replay_rows)
    if result.canonical_rows:
        silver_batch = 10_000 + batch_id
        silver_path = lake.write_rows("silver/canonical", silver_batch, result.canonical_rows)
        mirror_file(silver_path, "silver/canonical", silver_batch)
    checkpoint.commit(result, batch_id, replay=True)
    mirror_file(path, "bronze/replay", batch_id)
    return {
        "replay_id": replay_id,
        "replay_rows": len(result.replay_rows),
        "inserted": result.inserted,
        "duplicates": sum(1 for row in result.replay_rows if row["outcome"] == "duplicate"),
    }


def state_paths(state_root: Path) -> dict[str, Path]:
    return {
        "log": state_root / "kafka" / "log.jsonl",
        "checkpoint": state_root / "checkpoints" / "state.json",
        "lake": state_root / "lake",
        "warehouse": state_root / "warehouse.duckdb",
        "observability": state_root / "observability",
    }
