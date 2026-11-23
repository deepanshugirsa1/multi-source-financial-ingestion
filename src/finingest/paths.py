from __future__ import annotations

import os
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_state_root(root: Path | None = None) -> Path:
    """Runtime files live outside OneDrive. Sync locks those folders and reset fails."""
    root = root or project_root()
    if "OneDrive" in root.parts:
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / "finingest"
    return root / "data"


def contracts_path(root: Path | None = None) -> Path:
    return (root or project_root()) / "contracts" / "sources.yaml"


def sla_path(root: Path | None = None) -> Path:
    return (root or project_root()) / "contracts" / "slas.yaml"


def crosswalk_path(root: Path | None = None) -> Path:
    return (root or project_root()) / "transform" / "seeds" / "identity_crosswalk.csv"
