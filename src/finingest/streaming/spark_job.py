"""Spark Structured Streaming entrypoint.

The query reads the five financial topics, checkpoints Kafka offsets in Spark,
and calls the same micro-batch processor used by the local engine. Financial
event ids are deduped exactly with the checkpointed seen-set. A watermark
dropDuplicates would drop a late but legitimate settlement, so it is not used.

Idempotency: foreachBatch receives a stable batch id. The lake part files are
named with that id and overwritten on retry. If this process already committed
the batch id, a Spark retry returns immediately and leaves the files in place.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 ^
        src/finingest/streaming/spark_job.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from finingest.contracts import load_contracts  # noqa: E402
from finingest.models import Envelope, KafkaRecord  # noqa: E402
from finingest.paths import default_state_root, project_root  # noqa: E402
from finingest.storage.lake import Lake  # noqa: E402
from finingest.storage.s3mirror import mirror_file  # noqa: E402
from finingest.streaming.checkpoint import Checkpoint  # noqa: E402
from finingest.streaming.engine import state_paths  # noqa: E402
from finingest.streaming.processor import process_records  # noqa: E402


def build_query(spark, bundle, lake: Lake, checkpoint: Checkpoint):
    topics = ",".join(bundle.topics())
    brokers = os.environ.get("FININGEST_KAFKA_BOOTSTRAP", "localhost:9092")
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    def foreach_batch(batch_df, batch_id: int) -> None:
        if batch_id <= checkpoint.last_batch_id:
            return
        import json

        records: list[KafkaRecord] = []
        for row in batch_df.toLocalIterator():
            payload = json.loads(bytes(row.value).decode("utf-8"))
            records.append(
                KafkaRecord(
                    topic=row.topic,
                    partition=int(row.partition),
                    offset=int(row.offset),
                    key=bytes(row.key).decode("utf-8") if row.key else "",
                    envelope=Envelope(
                        source=payload["source"],
                        schema_version=str(payload["schema_version"]),
                        event_id=payload["event_id"],
                        event_ts=payload["event_ts"],
                        payload=payload["payload"],
                    ),
                    timestamp_ms=int(row.timestamp or 0),
                )
            )
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
        checkpoint.commit(result, batch_id)
        for dataset, path in written.items():
            mirror_file(path, dataset, batch_id)

    spark_checkpoint = os.environ.get(
        "FININGEST_SPARK_CHECKPOINT",
        str(default_state_root(project_root()) / "checkpoints" / "spark"),
    )
    return (
        source.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", spark_checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )


def main() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SystemExit("Install requirements-spark.txt and a JDK before starting the Spark job.") from exc
    root = project_root()
    bundle = load_contracts(root)
    paths = state_paths(default_state_root(root))
    lake = Lake(paths["lake"])
    lake.ensure()
    checkpoint = Checkpoint(paths["checkpoint"], bundle.raw["consumer_group"])
    spark = (
        SparkSession.builder.appName("finingest-structured-streaming")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    query = build_query(spark, bundle, lake, checkpoint)
    query.awaitTermination()


if __name__ == "__main__":
    main()
