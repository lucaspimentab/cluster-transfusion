from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional


def repo_root() -> Path:
    path = Path(__file__).resolve()
    for parent in [path] + list(path.parents):
        if (parent / "dataset").exists():
            return parent
    return Path.cwd()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def parse_k_list(k_list: str) -> List[int]:
    if not k_list:
        return []
    return [int(x.strip()) for x in k_list.split(",") if x.strip()]


def resolve_outputs_dir(
    root: Path, run_id: Optional[str] = None, window: Optional[int] = None, subdir: Optional[str] = None
) -> Path:
    base = root / "outputs"
    run_id = run_id or os.getenv("RUN_ID")
    if run_id:
        base = base / "runs" / run_id
    if window:
        base = base / f"w{window}"
    if subdir:
        base = base / subdir
    return ensure_dir(base)


def resolve_shared_dir(root: Path, run_id: Optional[str] = None) -> Path:
    return resolve_outputs_dir(root, run_id, subdir="shared")


def write_latest_run(root: Path, run_id: str) -> None:
    base = root / "outputs"
    ensure_dir(base)
    (base / "latest_run.txt").write_text(run_id, encoding="utf-8")
