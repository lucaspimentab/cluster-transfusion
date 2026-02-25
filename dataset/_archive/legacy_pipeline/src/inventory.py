from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Set, Tuple, Optional


@dataclass
class Inventory:
    tables: Set[Tuple[str, str]]
    columns: Dict[Tuple[str, str], Set[str]]

    def has_table(self, schema: str, table: str) -> bool:
        return (schema, table) in self.tables

    def columns_for(self, schema: str, table: str) -> Set[str]:
        return self.columns.get((schema, table), set())


def _resolve_inventory_paths(base_dir: Path) -> Optional[Tuple[Path, Path]]:
    tables = base_dir / "mimiciv31_tables_from_xlsx.csv"
    cols = base_dir / "mimiciv31_columns_from_xlsx.csv"
    if tables.exists() and cols.exists():
        return tables, cols
    return None


def load_inventory(base_dir: Path) -> Inventory:
    resolved = _resolve_inventory_paths(base_dir)
    if resolved is None:
        return Inventory(tables=set(), columns={})
    tables_path, cols_path = resolved

    tables: Set[Tuple[str, str]] = set()
    with tables_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tables.add((row["table_schema"], row["table_name"]))

    columns: Dict[Tuple[str, str], Set[str]] = {}
    with cols_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["table_schema"], row["table_name"])
            if key not in columns:
                columns[key] = set()
            columns[key].add(row["column"])

    return Inventory(tables=tables, columns=columns)
