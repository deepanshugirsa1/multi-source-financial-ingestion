from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from finingest.paths import default_state_root, project_root  # noqa: E402
from finingest.tasks import replay_log  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay the retained log without moving consumer offsets.")
    parser.add_argument("--reason", default="operator-replay")
    parser.add_argument("--transport", choices=["local", "kafka"], default="local")
    parser.add_argument("--state", type=Path, default=None)
    args = parser.parse_args()
    root = project_root()
    state = args.state or default_state_root(root)
    result = replay_log(root, state, reason=args.reason, transport=args.transport)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
