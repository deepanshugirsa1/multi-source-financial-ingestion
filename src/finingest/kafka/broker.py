from __future__ import annotations

import json

from finingest.contracts import ContractBundle
from finingest.kafka.log import partition_for
from finingest.models import Envelope, KafkaRecord, ProducedEvent


class BrokerLog:
    """Real Kafka producer and assigned consumer.

    Partitions are chosen with the same crc32 key function as the local log,
    and the caller passes the partition explicitly so replay order does not
    depend on the broker's default partitioner.
    """

    def __init__(self, bootstrap: str, bundle: ContractBundle, group: str = "spark-bronze"):
        self.bootstrap = bootstrap
        self.bundle = bundle
        self.group = group
        self._ensure_topics()

    def append(self, events: list[ProducedEvent], bundle: ContractBundle | None = None) -> None:
        from kafka import KafkaProducer

        bundle = bundle or self.bundle
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
        )
        try:
            for event in events:
                topic = bundle.topic_for(event.envelope.source)
                partition = partition_for(event.key, self.bundle.partitions)
                producer.send(
                    topic,
                    key=event.key,
                    value=event.envelope.to_dict(),
                    partition=partition,
                )
            producer.flush()
        finally:
            producer.close()

    def read_after(self, committed: dict[str, dict[str, int]], limit: int = 5000) -> list[KafkaRecord]:
        from kafka import KafkaConsumer, TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            enable_auto_commit=False,
            consumer_timeout_ms=4000,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            key_deserializer=lambda raw: raw.decode("utf-8") if raw else "",
        )
        records: list[KafkaRecord] = []
        try:
            assignments = []
            for topic in self.bundle.topics():
                for partition in range(self.bundle.partitions):
                    assignments.append(TopicPartition(topic, partition))
            consumer.assign(assignments)
            for topic_partition in assignments:
                last = committed.get(topic_partition.topic, {}).get(str(topic_partition.partition), -1)
                consumer.seek(topic_partition, last + 1)
            for message in consumer:
                payload = message.value
                records.append(
                    KafkaRecord(
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        key=message.key or "",
                        envelope=Envelope(
                            source=payload["source"],
                            schema_version=str(payload["schema_version"]),
                            event_id=payload["event_id"],
                            event_ts=payload["event_ts"],
                            payload=payload["payload"],
                        ),
                        timestamp_ms=int(message.timestamp or 0),
                    )
                )
                if len(records) >= limit:
                    break
        finally:
            consumer.close()
        return records

    def read_all(self) -> list[KafkaRecord]:
        return self.read_after({})

    def _ensure_topics(self) -> None:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap, client_id="finingest")
        topics = [
            NewTopic(name=topic, num_partitions=self.bundle.partitions, replication_factor=1)
            for topic in self.bundle.topics()
        ]
        try:
            try:
                admin.create_topics(topics, validate_only=False)
            except TopicAlreadyExistsError:
                pass
        finally:
            admin.close()
