from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RAW_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("event_id", pa.string()),
        ("topic", pa.string()),
        ("partition", pa.int64()),
        ("offset", pa.int64()),
        ("event_ts", pa.string()),
        ("payload_json", pa.string()),
        ("is_valid", pa.bool_()),
        ("is_duplicate", pa.bool_()),
        ("quarantine_reason", pa.string()),
        ("batch_id", pa.int64()),
        ("ingested_at", pa.string()),
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("event_id", pa.string()),
        ("topic", pa.string()),
        ("partition", pa.int64()),
        ("offset", pa.int64()),
        ("reason", pa.string()),
        ("payload_json", pa.string()),
        ("batch_id", pa.int64()),
        ("ingested_at", pa.string()),
    ]
)

CANONICAL_SCHEMA = pa.schema(
    [
        ("canonical_event_id", pa.string()),
        ("source_system", pa.string()),
        ("source_event_id", pa.string()),
        ("schema_version", pa.string()),
        ("event_type", pa.string()),
        ("canonical_account_id", pa.string()),
        ("source_account_key", pa.string()),
        ("identity_status", pa.string()),
        ("counterparty", pa.string()),
        ("instrument", pa.string()),
        ("amount", pa.string()),
        ("currency", pa.string()),
        ("direction", pa.string()),
        ("event_ts", pa.string()),
        ("effective_date", pa.string()),
        ("status", pa.string()),
        ("attributes_json", pa.string()),
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int64()),
        ("kafka_offset", pa.int64()),
        ("batch_id", pa.int64()),
        ("ingested_at", pa.string()),
    ]
)

REPLAY_SCHEMA = pa.schema(
    [
        ("replay_id", pa.string()),
        ("source", pa.string()),
        ("event_id", pa.string()),
        ("topic", pa.string()),
        ("partition", pa.int64()),
        ("offset", pa.int64()),
        ("outcome", pa.string()),
        ("replayed_at", pa.string()),
        ("batch_id", pa.int64()),
    ]
)

DATASETS = {
    "bronze/raw": RAW_SCHEMA,
    "bronze/quarantine": QUARANTINE_SCHEMA,
    "bronze/replay": REPLAY_SCHEMA,
    "silver/canonical": CANONICAL_SCHEMA,
}


def s3_key(dataset: str, batch_id: int | str) -> str:
    name = "part-empty.parquet" if batch_id == "empty" else f"part-{int(batch_id):05d}.parquet"
    return f"{dataset}/{name}"


class Lake:
    def __init__(self, root: Path):
        self.root = root

    def ensure(self) -> None:
        for dataset, schema in DATASETS.items():
            path = self.root / dataset / "part-empty.parquet"
            if not path.exists():
                self._write(path, [], schema)

    def write_rows(self, dataset: str, batch_id: int, rows: list[dict]) -> Path:
        schema = DATASETS[dataset]
        path = self.root / dataset / f"part-{batch_id:05d}.parquet"
        self._write(path, rows, schema)
        return path

    def dataset_glob(self, dataset: str) -> str:
        return (self.root / dataset / "*.parquet").as_posix()

    def read_rows(self, dataset: str) -> list[dict]:
        folder = self.root / dataset
        files = sorted(folder.glob("*.parquet"))
        if not files:
            return []
        table = pq.read_table(files, schema=DATASETS[dataset])
        return table.to_pylist()

    def _write(self, path: Path, rows: list[dict], schema: pa.Schema) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        aligned = [{field.name: _coerce(row.get(field.name), field.type) for field in schema} for row in rows]
        table = pa.Table.from_pylist(aligned, schema=schema)
        temporary = path.with_suffix(".parquet.tmp")
        pq.write_table(table, temporary)
        temporary.replace(path)


def _coerce(value, typ: pa.DataType):
    if value is None:
        return None
    if pa.types.is_boolean(typ):
        return bool(value)
    if pa.types.is_integer(typ):
        return int(value)
    return str(value)
