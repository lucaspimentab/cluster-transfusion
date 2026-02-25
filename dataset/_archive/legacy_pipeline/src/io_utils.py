from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple, Dict, List, Set

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from tqdm import tqdm

_POSTGRES_CONN_STR: Optional[str] = None
_POSTGRES_TABLES: Optional[Set[Tuple[str, str]]] = None


@dataclass
class TablePath:
    path: Path
    fmt: str  # parquet, parquet_dataset, csv, csv.gz
    schema: Optional[str] = None
    table: Optional[str] = None


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def duckdb_read_parquet(con: duckdb.DuckDBPyConnection, path: Path):
    path_str = str(path).replace("\\", "/")
    return con.sql(f"SELECT * FROM read_parquet('{path_str}', hive_partitioning=1)")


def set_postgres_context(conn_str: str, tables: Set[Tuple[str, str]]) -> None:
    global _POSTGRES_CONN_STR, _POSTGRES_TABLES
    _POSTGRES_CONN_STR = conn_str
    _POSTGRES_TABLES = tables


def _escape_conn_str(conn_str: str) -> str:
    return conn_str.replace("'", "''")


def find_table_path(data_dir: Path, schema: str, table: str) -> Optional[TablePath]:
    if _POSTGRES_TABLES is not None:
        if (schema, table) in _POSTGRES_TABLES:
            return TablePath(path=Path(f"{schema}.{table}"), fmt="postgres", schema=schema, table=table)
        return None

    base = data_dir / schema
    if not base.exists():
        return None

    dataset_dir = base / table
    if dataset_dir.exists() and dataset_dir.is_dir():
        parquet_files = list(dataset_dir.glob("*.parquet"))
        if parquet_files:
            return TablePath(path=dataset_dir, fmt="parquet_dataset")

    parquet_file = base / f"{table}.parquet"
    if parquet_file.exists():
        return TablePath(path=parquet_file, fmt="parquet")

    csv_gz_file = base / f"{table}.csv.gz"
    if csv_gz_file.exists():
        return TablePath(path=csv_gz_file, fmt="csv.gz")

    csv_file = base / f"{table}.csv"
    if csv_file.exists():
        return TablePath(path=csv_file, fmt="csv")

    return None


def duckdb_relation(con: duckdb.DuckDBPyConnection, table_path: TablePath):
    path_str = str(table_path.path)
    if table_path.fmt == "postgres":
        if not _POSTGRES_CONN_STR or not table_path.schema or not table_path.table:
            raise ValueError("Postgres context not configured.")
        conn_str = _escape_conn_str(_POSTGRES_CONN_STR)
        return con.sql(
            f"SELECT * FROM postgres_scan('{conn_str}', '{table_path.schema}', '{table_path.table}')"
        )
    if table_path.fmt in ("parquet", "parquet_dataset"):
        return con.from_parquet(path_str)
    if table_path.fmt in ("csv", "csv.gz"):
        return con.read_csv_auto(path_str)
    raise ValueError(f"Unsupported format: {table_path.fmt}")


def dataset_from_path(table_path: TablePath) -> ds.Dataset:
    if table_path.fmt == "postgres":
        raise ValueError("Postgres tables are not exposed as PyArrow datasets.")
    if table_path.fmt in ("parquet", "parquet_dataset"):
        return ds.dataset(str(table_path.path), format="parquet")
    if table_path.fmt in ("csv", "csv.gz"):
        return ds.dataset(str(table_path.path), format="csv")
    raise ValueError(f"Unsupported format: {table_path.fmt}")


def write_dataset_in_chunks(
    batch_iter: Iterable[pa.RecordBatch],
    out_dir: Path,
    partition_cols: List[str],
    compression: str,
    max_rows_per_file: int,
    max_rows_per_group: int,
) -> None:
    ensure_dir(out_dir)
    buffered: List[pa.RecordBatch] = []
    buffered_rows = 0

    file_format = ds.ParquetFileFormat()
    file_options = file_format.make_write_options(compression=compression)
    partitioning = partition_cols if partition_cols else None

    for batch in batch_iter:
        buffered.append(batch)
        buffered_rows += batch.num_rows
        if buffered_rows >= max_rows_per_file:
            table = pa.Table.from_batches(buffered)
            ds.write_dataset(
                table,
                str(out_dir),
                format="parquet",
                partitioning=partitioning,
                file_options=file_options,
                max_rows_per_file=max_rows_per_file,
                max_rows_per_group=max_rows_per_group,
                existing_data_behavior="overwrite_or_ignore",
            )
            buffered = []
            buffered_rows = 0

    if buffered:
        table = pa.Table.from_batches(buffered)
        ds.write_dataset(
            table,
            str(out_dir),
            format="parquet",
            partitioning=partitioning,
            file_options=file_options,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            existing_data_behavior="overwrite_or_ignore",
        )


def write_samples(dataset_path: Path, out_dir: Path, step_name: str, n10k: int = 10000, n100k: int = 100000) -> None:
    ensure_dir(out_dir)
    ds_in = ds.dataset(str(dataset_path), format="parquet")

    def _head_table(n_rows: int) -> pa.Table:
        scanner = ds_in.scanner()
        if hasattr(scanner, "head"):
            return scanner.head(n_rows)
        batches = []
        remaining = n_rows
        for batch in scanner.to_batches():
            if batch.num_rows <= remaining:
                batches.append(batch)
                remaining -= batch.num_rows
            else:
                batches.append(batch.slice(0, remaining))
                remaining = 0
            if remaining <= 0:
                break
        return pa.Table.from_batches(batches)

    def _write_sample(n_rows: int, suffix: str) -> None:
        table = _head_table(n_rows)
        out_path = out_dir / f"{step_name}_{suffix}.csv"
        with out_path.open("wb") as f:
            pa_csv.write_csv(table, f)

    _write_sample(n10k, "sample10k")
    _write_sample(n100k, "sample100k")


def compute_missingness(dataset_path: Path, columns: List[str]) -> Tuple[int, Dict[str, int]]:
    if dataset_path.is_dir():
        files = sorted(dataset_path.rglob("*.parquet"))
    else:
        files = [dataset_path]

    null_counts: Dict[str, int] = {c: 0 for c in columns}
    total_rows = 0
    total_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files)

    progress = tqdm(total=total_rows, desc="missingness", unit="rows")
    try:
        for file_path in files:
            pf = pq.ParquetFile(str(file_path))
            file_rows = pf.metadata.num_rows
            file_cols = pf.schema_arrow.names
            cols_in_file = [c for c in columns if c in file_cols]
            missing_cols = [c for c in columns if c not in file_cols]
            for col in missing_cols:
                null_counts[col] += file_rows
            if not cols_in_file:
                progress.update(file_rows)
                continue
            for batch in pf.iter_batches(columns=cols_in_file):
                for col in cols_in_file:
                    null_counts[col] += batch.column(col).null_count
                progress.update(batch.num_rows)
    finally:
        progress.close()

    return total_rows, null_counts


def partition_dirs(root: Path, partition_prefix: str = "stay_bucket=") -> List[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith(partition_prefix)])
