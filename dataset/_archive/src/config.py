from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any

import yaml


@dataclass
class LabItemConfig:
    itemids: List[int]
    label_regex: List[str]


@dataclass
class Config:
    defaults: Dict[str, Any]
    lab_items: Dict[str, LabItemConfig]


def load_defaults(config_dir: Path) -> Dict[str, Any]:
    path = config_dir / "defaults.yaml"
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("defaults", {})


def load_lab_itemids(config_dir: Path) -> Dict[str, LabItemConfig]:
    path = config_dir / "lab_itemids.yaml"
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    labs = {}
    for name, spec in data.get("labs", {}).items():
        itemids = [int(x) for x in spec.get("itemids", [])]
        regex = [str(x) for x in spec.get("label_regex", [])]
        labs[name] = LabItemConfig(itemids=itemids, label_regex=regex)
    return labs


def load_config(config_dir: Path) -> Config:
    return Config(
        defaults=load_defaults(config_dir),
        lab_items=load_lab_itemids(config_dir),
    )
