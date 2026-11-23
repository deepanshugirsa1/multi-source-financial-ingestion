from pathlib import Path

from finingest.tasks import run_pipeline

ROOT = Path(__file__).resolve().parents[1]


def test_end_to_end_lake_and_gold(tmp_path: Path):
    report = run_pipeline(ROOT, tmp_path, reset=True, per_source=10, include_faults=True)
    assert report["ok"] is True
    assert report["raw_count"] == 55
    assert report["canonical_count"] == 51
    assert report["resolved_count"] == 50
    assert report["unresolved_count"] == 1
    assert report["quarantine_count"] == 2
    assert report["duplicate_count"] == 2
    assert report["replay_count"] == 55
    assert report["gold_rows"] == 50
    assert report["account_rows"] == 6
    assert report["daily_rows"] > 0
    assert report["replay"]["inserted"] == 0
    assert all(item["ok"] for item in report["freshness"])
