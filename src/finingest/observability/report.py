from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from finingest.contracts import ContractBundle
from finingest.storage.lake import Lake
from finingest.streaming.checkpoint import Checkpoint


class QualityError(RuntimeError):
    def __init__(self, report: dict):
        self.report = report
        failed = ", ".join(report.get("failures", [])) or "quality check failed"
        super().__init__(failed)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_lineage(bundle: ContractBundle) -> list[dict]:
    edges = []
    for source in bundle.raw["sources"]:
        edges.append({"from": f"connector:{source['name']}", "to": f"kafka:{source['topic']}"})
        edges.append({"from": f"kafka:{source['topic']}", "to": "bronze.raw_events"})
    edges.extend(
        [
            {"from": "bronze.raw_events", "to": "bronze.quarantine"},
            {"from": "bronze.raw_events", "to": "silver.canonical_events"},
            {"from": "seed:identity_crosswalk", "to": "silver.canonical_events"},
            {"from": "silver.canonical_events", "to": "gold.fct_financial_events"},
            {"from": "gold.fct_financial_events", "to": "gold.fct_account_daily"},
            {"from": "seed:identity_crosswalk", "to": "gold.dim_account"},
            {"from": "kafka log", "to": "bronze.replay_history"},
        ]
    )
    return edges


def evaluate(
    lake: Lake,
    checkpoint: Checkpoint,
    bundle: ContractBundle,
    *,
    now: datetime | None = None,
    warehouse: Path | None = None,
) -> dict:
    moment = now or datetime.now(timezone.utc)
    raw = [row for row in lake.read_rows("bronze/raw") if row.get("event_id")]
    canonical = [row for row in lake.read_rows("silver/canonical") if row.get("canonical_event_id")]
    quarantine = [row for row in lake.read_rows("bronze/quarantine") if row.get("event_id")]
    replays = [row for row in lake.read_rows("bronze/replay") if row.get("replay_id")]
    raw_count = len(raw)
    duplicate_count = sum(1 for row in raw if row["is_duplicate"])
    quarantine_count = len(quarantine)
    unresolved = sum(1 for row in canonical if row["identity_status"] == "unresolved")
    rates = {
        "quarantine_rate": (quarantine_count / raw_count) if raw_count else 0.0,
        "duplicate_rate": (duplicate_count / raw_count) if raw_count else 0.0,
        "unresolved_identity_rate": (unresolved / len(canonical)) if canonical else 0.0,
    }
    freshness = []
    failures: list[str] = []
    slas = bundle.slas
    for source in bundle.raw["sources"]:
        name = source["name"]
        stamps = [_parse_ts(row["event_ts"]) for row in raw if row["source"] == name and row["is_valid"] and not row["is_duplicate"]]
        if not stamps:
            freshness.append({"source": name, "lag_hours": None, "ok": False, "events": 0})
            failures.append(f"freshness:{name}:no_events")
            continue
        latest = max(stamps)
        lag_hours = (moment - latest).total_seconds() / 3600
        limit = float(slas["freshness_hours"][name])
        ok = lag_hours <= limit
        freshness.append(
            {
                "source": name,
                "latest_event_ts": latest.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "lag_hours": round(lag_hours, 3),
                "sla_hours": limit,
                "ok": ok,
                "events": len(stamps),
            }
        )
        if not ok:
            failures.append(f"freshness:{name}:{lag_hours:.2f}h")
    if rates["quarantine_rate"] > float(slas["max_quarantine_rate"]):
        failures.append("quarantine_rate")
    if rates["duplicate_rate"] > float(slas["max_duplicate_rate"]):
        failures.append("duplicate_rate")
    if rates["unresolved_identity_rate"] > float(slas["max_unresolved_identity_rate"]):
        failures.append("unresolved_identity_rate")
    gold_rows = None
    if warehouse and warehouse.exists():
        import duckdb

        con = duckdb.connect(str(warehouse), read_only=True)
        try:
            gold_rows = con.execute("select count(*) from fct_financial_events").fetchone()[0]
            daily_rows = con.execute("select count(*) from fct_account_daily").fetchone()[0]
            accounts = con.execute("select count(*) from dim_account").fetchone()[0]
        finally:
            con.close()
        if gold_rows == 0:
            failures.append("gold_empty")
    else:
        daily_rows = None
        accounts = None
    report = {
        "generated_at": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "raw_count": raw_count,
        "canonical_count": len(canonical),
        "resolved_count": sum(1 for row in canonical if row["identity_status"] == "resolved"),
        "unresolved_count": unresolved,
        "quarantine_count": quarantine_count,
        "duplicate_count": duplicate_count,
        "replay_count": len(replays),
        "rates": {key: round(value, 4) for key, value in rates.items()},
        "freshness": freshness,
        "offsets": checkpoint.offsets,
        "last_batch_id": checkpoint.last_batch_id,
        "consumer_group": checkpoint.consumer_group,
        "gold_rows": gold_rows,
        "daily_rows": daily_rows,
        "account_rows": accounts,
        "lineage": build_lineage(bundle),
        "failures": failures,
        "ok": not failures,
        "recovery": {
            "resume": "python scripts/run_e2e.py --resume",
            "replay": "python scripts/replay.py --reason operator-replay",
        },
    }
    return report


def write_report(report: dict, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "report.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    lineage = folder / "lineage.json"
    lineage.write_text(json.dumps({"edges": report["lineage"]}, indent=2), encoding="utf-8")
    if not report["ok"]:
        failure_log = folder / "failures.jsonl"
        with failure_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"generated_at": report["generated_at"], "failures": report["failures"], "recovery": report["recovery"]}) + "\n")
    return path
