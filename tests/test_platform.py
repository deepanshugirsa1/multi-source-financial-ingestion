from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from finingest.connectors.generate import build_catalog
from finingest.contracts import ContractError, load_contracts, normalize
from finingest.kafka.log import LocalKafkaLog, partition_for
from finingest.models import Envelope
from finingest.storage.lake import Lake, s3_key
from finingest.streaming.checkpoint import Checkpoint
from finingest.streaming.engine import InjectedCrash, drain, replay
from finingest.tasks import publish_events, reset_state, stream_to_lake, validate_contracts

ROOT = Path(__file__).resolve().parents[1]
DAG = (ROOT / "orchestration" / "dags" / "financial_ingestion.py").read_text(encoding="utf-8")


def test_contracts_cover_five_sources_and_versions():
    summary = validate_contracts(ROOT)
    assert summary["sources"] == [
        "card_processor",
        "ach_network",
        "brokerage",
        "loan_servicer",
        "treasury_wires",
    ]
    assert summary["versions"]["card_processor"] == ["1.0", "2.0"]
    assert summary["versions"]["loan_servicer"] == ["1.0", "2.0"]
    assert summary["crosswalk_keys"] == 10


def test_card_v2_minor_units_and_forward_compatible_field():
    bundle = load_contracts(ROOT)
    envelope = Envelope(
        source="card_processor",
        schema_version="2.0",
        event_id="A1",
        event_ts="2026-11-23T00:00:00Z",
        payload={
            "authorization_id": "A1",
            "card_token": "card_tok_1001",
            "merchant_id": "m-1",
            "amount_minor": 2599,
            "currency": "USD",
            "authorized_at": "2026-11-23T00:00:00Z",
            "response_code": "00",
            "loyalty_id": "L9",
        },
    )
    row = normalize(envelope, bundle, ingested_at="2026-11-23T00:00:01Z")
    assert row["amount"] == "25.99"
    assert row["canonical_account_id"] == "A-1001"
    assert row["identity_status"] == "resolved"
    assert "loyalty_id" in row["attributes_json"]


def test_trade_notional_is_quantity_times_price():
    bundle = load_contracts(ROOT)
    envelope = Envelope(
        source="brokerage",
        schema_version="1.0",
        event_id="TRD-1",
        event_ts="2026-11-23T00:00:00Z",
        payload={
            "trade_id": "TRD-1",
            "account_number": "BRK-44002",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": "2",
            "price": "10.50",
            "trade_time": "2026-11-23T00:00:00Z",
            "settlement_date": "2026-11-24",
        },
    )
    row = normalize(envelope, bundle, ingested_at="2026-11-23T00:00:01Z")
    assert row["amount"] == "21.00"
    assert row["direction"] == "outflow"
    assert row["effective_date"] == "2026-11-24"


def test_unknown_version_and_missing_amount_are_contract_errors():
    bundle = load_contracts(ROOT)
    with pytest.raises(ContractError) as unknown:
        normalize(
            Envelope("ach_network", "9.9", "X", "2026-11-23T00:00:00Z", {"trace_number": "X"}),
            bundle,
            ingested_at="2026-11-23T00:00:01Z",
        )
    assert unknown.value.reason == "unknown_schema_version"
    with pytest.raises(ContractError) as missing:
        normalize(
            Envelope(
                "card_processor",
                "1.0",
                "AUTH-X",
                "2026-11-23T00:00:00Z",
                {
                    "authId": "AUTH-X",
                    "panToken": "card_tok_1001",
                    "merchantId": "m",
                    "ccy": "USD",
                    "authTs": "2026-11-23T00:00:00Z",
                    "decision": "APPROVED",
                },
            ),
            bundle,
            ingested_at="2026-11-23T00:00:01Z",
        )
    assert missing.value.reason.startswith("missing_field:")


def test_partition_assignment_is_stable():
    assert partition_for("card_tok_1001", 3) == partition_for("card_tok_1001", 3)
    assert 0 <= partition_for("card_tok_1001", 3) < 3


def test_s3_key_layout():
    assert s3_key("silver/canonical", 3) == "silver/canonical/part-00003.parquet"
    assert s3_key("bronze/raw", "empty") == "bronze/raw/part-empty.parquet"


def test_catalog_without_faults_is_fully_canonical(tmp_path: Path):
    moment = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    bundle = load_contracts(ROOT)
    log = LocalKafkaLog(tmp_path / "kafka" / "log.jsonl", bundle.partitions)
    log.append(build_catalog(per_source=4, include_faults=False, root=ROOT, seed_time=moment), bundle)
    lake = Lake(tmp_path / "lake")
    checkpoint = Checkpoint(tmp_path / "checkpoints" / "state.json")
    drain(log, lake, checkpoint, bundle)
    canonical = [row for row in lake.read_rows("silver/canonical") if row.get("canonical_event_id")]
    quarantine = [row for row in lake.read_rows("bronze/quarantine") if row.get("event_id")]
    assert len(canonical) == 20
    assert quarantine == []
    assert {row["schema_version"] for row in canonical if row["source_system"] == "card_processor"} == {"1.0", "2.0"}
    assert len(checkpoint.seen_ids) == 20
    assert set(checkpoint.offsets) == set(bundle.topics())


def test_crash_before_commit_retries_the_same_batch_without_duplicates(tmp_path: Path):
    moment = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    bundle = load_contracts(ROOT)
    log = LocalKafkaLog(tmp_path / "kafka" / "log.jsonl", bundle.partitions)
    log.append(build_catalog(per_source=4, include_faults=False, root=ROOT, seed_time=moment), bundle)
    lake = Lake(tmp_path / "lake")
    checkpoint = Checkpoint(tmp_path / "checkpoints" / "state.json")
    with pytest.raises(InjectedCrash):
        drain(log, lake, checkpoint, bundle, max_batch_records=8, crash_on_batch=1, crash_stage="before_commit")
    assert checkpoint.last_batch_id == 0
    drain(log, lake, checkpoint, bundle, max_batch_records=8)
    canonical = [row for row in lake.read_rows("silver/canonical") if row.get("canonical_event_id")]
    assert len(canonical) == 20
    assert len({row["canonical_event_id"] for row in canonical}) == 20


def test_replay_and_redelivery_do_not_change_silver(tmp_path: Path):
    reset_state(tmp_path)
    moment = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    bundle = load_contracts(ROOT)
    publish_events(ROOT, tmp_path, per_source=3, include_faults=True)
    first = stream_to_lake(ROOT, tmp_path)
    log = LocalKafkaLog(tmp_path / "kafka" / "log.jsonl", bundle.partitions)
    lake = Lake(tmp_path / "lake")
    checkpoint = Checkpoint(tmp_path / "checkpoints" / "state.json")
    replayed = replay(log, lake, checkpoint, bundle)
    second = stream_to_lake(ROOT, tmp_path)
    assert replayed["inserted"] == 0
    assert first["canonical"] == second["canonical"]
    raw = [row for row in lake.read_rows("bronze/raw") if row.get("event_id")]
    assert sum(1 for row in raw if row["is_duplicate"]) == 2
    assert len([row for row in lake.read_rows("bronze/quarantine") if row.get("event_id")]) == 2
    unresolved = [
        row for row in lake.read_rows("silver/canonical") if row.get("identity_status") == "unresolved"
    ]
    assert len(unresolved) == 1
    # Same event published again is a new offset and a duplicate, not a second canonical row.
    again = build_catalog(per_source=3, include_faults=False, root=ROOT, seed_time=moment)[:1]
    log.append(again, bundle)
    third = stream_to_lake(ROOT, tmp_path)
    assert third["canonical"] == first["canonical"]


def test_second_batch_with_new_ids_advances_offsets(tmp_path: Path):
    reset_state(tmp_path)
    publish_events(ROOT, tmp_path, per_source=2, include_faults=False)
    first = stream_to_lake(ROOT, tmp_path)
    publish_events(ROOT, tmp_path, per_source=2, include_faults=False, id_suffix="-b2")
    second = stream_to_lake(ROOT, tmp_path)
    assert second["canonical"] == first["canonical"] + 10
    assert second["batches"] == 1


def test_airflow_dag_wires_the_task_sequence():
    for task_id in (
        "validate_contracts",
        "publish_events",
        "stream_to_lake",
        "replay_history",
        "dbt_build",
        "data_quality",
    ):
        assert task_id in DAG
    assert "contracts >> publish >> stream >> replay >> transform >> quality" in DAG


def test_freshness_window_used_by_the_catalog():
    moment = datetime.now(timezone.utc) - timedelta(minutes=90)
    events = build_catalog(per_source=1, include_faults=False, root=ROOT, seed_time=moment)
    assert len(events) == 5
    assert {event.envelope.source for event in events} == {
        "card_processor",
        "ach_network",
        "brokerage",
        "loan_servicer",
        "treasury_wires",
    }
