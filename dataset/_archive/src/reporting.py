from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any

import csv


@dataclass
class MissingnessItem:
    step: str
    column: str
    reason: str
    source: str


@dataclass
class MissingnessTracker:
    items: List[MissingnessItem] = field(default_factory=list)

    def add(self, step: str, column: str, reason: str, source: str) -> None:
        self.items.append(MissingnessItem(step=step, column=column, reason=reason, source=source))

    def to_csv(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["step", "column", "reason", "source"])
            for item in self.items:
                writer.writerow([item.step, item.column, item.reason, item.source])


def write_feature_dictionary(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
