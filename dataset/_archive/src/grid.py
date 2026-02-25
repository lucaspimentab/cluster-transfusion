from __future__ import annotations

from pathlib import Path

import duckdb

from .io_utils import write_dataset_in_chunks, ensure_dir, duckdb_read_parquet
from .logging_utils import log_event


def build_time_grid(
    con: duckdb.DuckDBPyConnection,
    cohort_path: Path,
    out_dir: Path,
    logger,
) -> Path:
    con.execute("PRAGMA threads=4")
    con.register("cohort", duckdb_read_parquet(con, cohort_path))

    grid_query = """
        SELECT c.subject_id,
               c.stay_id,
               c.hadm_id,
               c.intime,
               c.outtime,
               r.idx * 5 AS tbin,
               c.intime + r.idx * 5 * INTERVAL '1' MINUTE AS charttime_bin,
               (c.stay_id % 100) AS stay_bucket
        FROM cohort c
        JOIN range(0, c.n_bins) r(idx) ON TRUE
    """

    log_event(logger, "building_time_grid")
    rel = con.sql(grid_query)
    reader = rel.fetch_record_batch()

    grid_out = out_dir / "intermediate" / "step00_grid"
    ensure_dir(grid_out)
    write_dataset_in_chunks(
        reader,
        grid_out,
        partition_cols=["stay_bucket"],
        compression="zstd",
        max_rows_per_file=1_000_000,
        max_rows_per_group=100_000,
    )

    return grid_out
