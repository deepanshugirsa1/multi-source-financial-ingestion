import argparse, json, time
from pathlib import Path

from finingest.paths import project_root
from finingest.tasks import run_pipeline

p = argparse.ArgumentParser()
p.add_argument("--per-source", type=int, default=20000)
p.add_argument("--state", type=Path, required=True)
p.add_argument("--transport", default="kafka")
a = p.parse_args()

t0 = time.perf_counter()
r = run_pipeline(project_root(), a.state, reset=True, transport=a.transport, per_source=a.per_source)
d = time.perf_counter() - t0

print(json.dumps({
    "transport": a.transport,
    "per_source": a.per_source,
    "raw_count": r["raw_count"],
    "canonical_count": r["canonical_count"],
    "gold_rows": r["gold_rows"],
    "duplicate_rate": r["rates"]["duplicate_rate"],
    "seconds_end_to_end": round(d, 2),
    "events_per_sec_end_to_end": round(r["raw_count"] / d),
}, indent=2))
