from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from finingest.paths import default_state_root, project_root  # noqa: E402
from finingest.tasks import dbt_build, run_pipeline, run_quality_checks, stream_to_lake, validate_contracts  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the financial ingestion platform end to end.")
    parser.add_argument("--transport", choices=["local", "kafka"], default="local")
    parser.add_argument("--resume", action="store_true", help="Drain from the committed offsets without republishing.")
    parser.add_argument("--state", type=Path, default=None)
    args = parser.parse_args()
    root = project_root()
    state = args.state or default_state_root(root)
    if args.resume:
        validate_contracts(root)
        stream_to_lake(root, state, transport=args.transport)
        dbt_build(root, state)
        report = run_quality_checks(root, state)
    else:
        report = run_pipeline(root, state, reset=True, transport=args.transport)
    summary = {
        "raw_count": report["raw_count"],
        "canonical_count": report["canonical_count"],
        "resolved_count": report["resolved_count"],
        "unresolved_count": report["unresolved_count"],
        "quarantine_count": report["quarantine_count"],
        "duplicate_count": report["duplicate_count"],
        "replay_count": report["replay_count"],
        "gold_rows": report["gold_rows"],
        "daily_rows": report["daily_rows"],
        "account_rows": report["account_rows"],
        "rates": report["rates"],
        "freshness": report["freshness"],
        "ok": report["ok"],
        "state": str(state),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
