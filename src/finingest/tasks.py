from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

from finingest.connectors.generate import build_catalog
from finingest.contracts import load_contracts
from finingest.kafka.broker import BrokerLog
from finingest.kafka.log import LocalKafkaLog
from finingest.observability.report import QualityError, evaluate, write_report
from finingest.paths import default_state_root, project_root
from finingest.storage.lake import Lake
from finingest.streaming.checkpoint import Checkpoint
from finingest.streaming.engine import drain, replay, state_paths


def validate_contracts(root: Path | None = None, **_context) -> dict:
    bundle = load_contracts(root or project_root())
    names = [source["name"] for source in bundle.raw["sources"]]
    versions = {
        source["name"]: [str(version["version"]) for version in source["versions"]]
        for source in bundle.raw["sources"]
    }
    return {"sources": names, "versions": versions, "crosswalk_keys": len(bundle.crosswalk)}


def _force_remove(path: Path) -> None:
    def onexc(func, failed, exc):
        failed_path = Path(failed)
        try:
            os.chmod(failed_path, stat.S_IWRITE)
            func(failed)
        except OSError:
            if failed_path.is_dir():
                return
            raise PermissionError(
                f"Could not clear {failed_path}. Close anything using it, or omit --state "
                "so the run writes to %LOCALAPPDATA%\\finingest."
            ) from exc

    if path.is_dir():
        shutil.rmtree(path, onexc=onexc)
    else:
        try:
            path.unlink()
        except PermissionError:
            os.chmod(path, stat.S_IWRITE)
            path.unlink()


def reset_state(state_root: Path) -> None:
    state_root.mkdir(parents=True, exist_ok=True)
    for child in list(state_root.iterdir()):
        if child.name == ".gitkeep":
            continue
        _force_remove(child)


def publish_events(
    root: Path | None = None,
    state_root: Path | None = None,
    *,
    per_source: int = 10,
    include_faults: bool = True,
    transport: str = "local",
    reset_topics: bool = False,
    id_suffix: str = "",
    **_context,
) -> dict:
    root = root or project_root()
    state_root = state_root or default_state_root(root)
    bundle = load_contracts(root)
    events = build_catalog(
        per_source=per_source,
        include_faults=include_faults,
        root=root,
        id_suffix=id_suffix,
    )
    if transport == "kafka":
        broker = BrokerLog(os.environ["FININGEST_KAFKA_BOOTSTRAP"], bundle, bundle.raw["consumer_group"])
        if reset_topics:
            broker.reset_topics()
        broker.append(events, bundle)
    else:
        paths = state_paths(state_root)
        log = LocalKafkaLog(paths["log"], bundle.partitions)
        log.append(events, bundle)
    return {"published": len(events), "transport": transport}


def stream_to_lake(
    root: Path | None = None,
    state_root: Path | None = None,
    *,
    transport: str = "local",
    max_batch_records: int | None = None,
    crash_on_batch: int | None = None,
    crash_stage: str = "before_commit",
    **_context,
) -> dict:
    root = root or project_root()
    state_root = state_root or default_state_root(root)
    bundle = load_contracts(root)
    paths = state_paths(state_root)
    if transport == "kafka":
        log = BrokerLog(os.environ["FININGEST_KAFKA_BOOTSTRAP"], bundle, bundle.raw["consumer_group"])
    else:
        log = LocalKafkaLog(paths["log"], bundle.partitions)
    lake = Lake(paths["lake"])
    checkpoint = Checkpoint(paths["checkpoint"], bundle.raw["consumer_group"])
    batches = drain(
        log,
        lake,
        checkpoint,
        bundle,
        max_batch_records=max_batch_records,
        crash_on_batch=crash_on_batch,
        crash_stage=crash_stage,
    )
    canonical = [row for row in lake.read_rows("silver/canonical") if row.get("canonical_event_id")]
    return {"batches": batches, "canonical": len(canonical), "offsets": checkpoint.offsets}


def replay_log(
    root: Path | None = None,
    state_root: Path | None = None,
    *,
    reason: str = "operator-replay",
    transport: str = "local",
    **_context,
) -> dict:
    root = root or project_root()
    state_root = state_root or default_state_root(root)
    bundle = load_contracts(root)
    paths = state_paths(state_root)
    if transport == "kafka":
        log = BrokerLog(os.environ["FININGEST_KAFKA_BOOTSTRAP"], bundle, bundle.raw["consumer_group"])
    else:
        log = LocalKafkaLog(paths["log"], bundle.partitions)
    lake = Lake(paths["lake"])
    checkpoint = Checkpoint(paths["checkpoint"], bundle.raw["consumer_group"])
    return replay(log, lake, checkpoint, bundle, reason=reason)


def _dbt_executable() -> str:
    name = "dbt.exe" if os.name == "nt" else "dbt"
    candidate = Path(sys.executable).with_name(name)
    if candidate.exists():
        return str(candidate)
    found = shutil.which("dbt")
    if not found:
        raise RuntimeError("dbt is not installed in this Python environment")
    return found


def dbt_build(root: Path | None = None, state_root: Path | None = None, **_context) -> None:
    root = root or project_root()
    state_root = state_root or default_state_root(root)
    paths = state_paths(state_root)
    env = os.environ.copy()
    env["FININGEST_DUCKDB"] = paths["warehouse"].as_posix()
    env["FININGEST_SILVER_GLOB"] = (paths["lake"] / "silver" / "canonical" / "*.parquet").as_posix()
    env["FININGEST_BRONZE_GLOB"] = (paths["lake"] / "bronze" / "raw" / "*.parquet").as_posix()
    env["FININGEST_QUARANTINE_GLOB"] = (paths["lake"] / "bronze" / "quarantine" / "*.parquet").as_posix()
    env["FININGEST_REPLAY_GLOB"] = (paths["lake"] / "bronze" / "replay" / "*.parquet").as_posix()
    env["DBT_PROFILES_DIR"] = str(root / "transform")
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    env["DO_NOT_TRACK"] = "1"
    subprocess.run(
        [
            _dbt_executable(),
            "build",
            "--project-dir",
            str(root / "transform"),
            "--profiles-dir",
            str(root / "transform"),
            "--no-use-colors",
        ],
        check=True,
        env=env,
    )


def run_quality_checks(
    root: Path | None = None,
    state_root: Path | None = None,
    **_context,
) -> dict:
    root = root or project_root()
    state_root = state_root or default_state_root(root)
    bundle = load_contracts(root)
    paths = state_paths(state_root)
    lake = Lake(paths["lake"])
    checkpoint = Checkpoint(paths["checkpoint"], bundle.raw["consumer_group"])
    report = evaluate(lake, checkpoint, bundle, warehouse=paths["warehouse"])
    write_report(report, paths["observability"])
    if not report["ok"]:
        raise QualityError(report)
    return report


def run_pipeline(
    root: Path | None = None,
    state_root: Path | None = None,
    *,
    reset: bool = True,
    transport: str = "local",
    per_source: int = 10,
    include_faults: bool = True,
) -> dict:
    root = root or project_root()
    state_root = state_root or default_state_root(root)
    if reset:
        reset_state(state_root)
    validate_contracts(root)
    publish_events(
        root,
        state_root,
        per_source=per_source,
        include_faults=include_faults,
        transport=transport,
        reset_topics=reset,
    )
    before = stream_to_lake(root, state_root, transport=transport)
    replay_result = replay_log(root, state_root, transport=transport)
    after = stream_to_lake(root, state_root, transport=transport)
    if replay_result["inserted"] != 0 or before["canonical"] != after["canonical"]:
        raise RuntimeError("replay changed the canonical dataset")
    dbt_build(root, state_root)
    report = run_quality_checks(root, state_root)
    report["replay"] = replay_result
    return report
