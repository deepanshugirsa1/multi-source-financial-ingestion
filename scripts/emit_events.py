from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from finingest.paths import default_state_root, project_root  # noqa: E402
from finingest.tasks import publish_events, stream_to_lake  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish another batch and advance the consumer from its committed offsets.")
    parser.add_argument("--per-source", type=int, default=2)
    parser.add_argument("--suffix", default="-b2")
    parser.add_argument("--transport", choices=["local", "kafka"], default="local")
    parser.add_argument("--state", type=Path, default=None)
    args = parser.parse_args()
    root = project_root()
    state = args.state or default_state_root(root)
    published = publish_events(
        root,
        state,
        per_source=args.per_source,
        include_faults=False,
        transport=args.transport,
        id_suffix=args.suffix,
    )
    drained = stream_to_lake(root, state, transport=args.transport)
    print(json.dumps({"published": published, "drained": drained}, indent=2))


if __name__ == "__main__":
    main()
