from __future__ import annotations

import argparse

from lib.duckdb_utils import connect_duckdb, enable_profiling
from lib.logging_utils import Timer, log_event, setup_logging
from lib.schema_utils import get_columns, infer_id_column, infer_time_column, infer_transfusion_columns, time_expression
from lib.utils import repo_root, resolve_shared_dir


def parse_args():
    parser = argparse.ArgumentParser(description="Step 0: Build outcomes cohort with transfusion flag")
    parser.add_argument("--run_id", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    root = repo_root()
    outputs_dir = resolve_shared_dir(root, args.run_id)
    logger, _ = setup_logging("step0_build_outcomes_cohort", outputs_dir)

    con = connect_duckdb()
    profile_path = outputs_dir / "profiling" / "step0_build_outcomes_cohort.json"
    enable_profiling(con, profile_path, logger=logger)

    timegrid_glob = (root / "dataset" / "timegrid_features" / "*" / "*.parquet").as_posix()
    timegrid_rel = f"read_parquet('{timegrid_glob}', union_by_name=true)"

    outcomes_full = root / "dataset" / "outputs_outcomes" / "outcomes_by_stay_full.csv"
    if not outcomes_full.exists():
        raise FileNotFoundError("Missing outcomes_by_stay_full.csv")
    outcomes_rel = f"read_csv_auto('{outcomes_full.as_posix()}', SAMPLE_SIZE=100000)"

    timegrid_cols = get_columns(con, timegrid_rel)
    timegrid_names = [c[0] for c in timegrid_cols]
    stay_col = infer_id_column(timegrid_names)
    time_col = infer_time_column(timegrid_names)
    time_type = None
    for name, dtype in timegrid_cols:
        if name == time_col:
            time_type = dtype
            break
    transfusion_cols = infer_transfusion_columns(timegrid_names)

    if not stay_col or not time_col or not time_type:
        raise RuntimeError("Could not infer stay_id or time column from timegrid.")
    if not transfusion_cols:
        raise RuntimeError("No transfusion-related columns found in timegrid features.")

    outcomes_cols = get_columns(con, outcomes_rel)
    outcomes_names = [c[0] for c in outcomes_cols]
    outcomes_stay_col = infer_id_column(outcomes_names)

    time_expr = time_expression(time_col, time_type, alias="tg")

    log_event(
        logger,
        "schema_inferred",
        stay_col=stay_col,
        time_col=time_col,
        transfusion_cols=transfusion_cols,
        outcomes_stay_col=outcomes_stay_col,
    )

    if args.dry_run:
        log_event(logger, "dry_run")
        return

    transf_terms = [f"COALESCE(tg.\"{c}\", 0) > 0" for c in transfusion_cols]
    transf_expr = " OR ".join(transf_terms)

    with Timer(logger, "build_outcomes_cohort"):
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW transfused_by_stay AS
            SELECT
                tg.\"{stay_col}\" AS stay_id,
                max(CASE WHEN {transf_expr} THEN 1 ELSE 0 END) AS transfused,
                min(CASE WHEN {transf_expr} THEN {time_expr} END) AS t0_transf
            FROM {timegrid_rel} tg
            GROUP BY tg.\"{stay_col}\"
            """
        )

        cohort_query = f"""
        SELECT
            o.*,
            COALESCE(t.transfused, 0) AS transfused,
            t.t0_transf
        FROM {outcomes_rel} o
        LEFT JOIN transfused_by_stay t
          ON o.\"{outcomes_stay_col}\" = t.stay_id
        """

        cohort_path = outputs_dir / "outcomes_cohort.parquet"
        cohort_csv = outputs_dir / "outcomes_cohort.csv"
        con.execute(f"COPY ({cohort_query}) TO '{cohort_path.as_posix()}' (FORMAT PARQUET)")
        con.execute(f"COPY ({cohort_query}) TO '{cohort_csv.as_posix()}' (FORMAT CSV, HEADER TRUE)")

    counts = con.execute(
        f"SELECT count(*) AS n_rows, sum(transfused) AS n_transfused FROM read_parquet('{cohort_path.as_posix()}')"
    ).fetchone()
    n_rows = int(counts[0])
    n_transfused = int(counts[1] or 0)
    n_controls = n_rows - n_transfused
    log_event(
        logger,
        "outcomes_cohort_written",
        path=str(cohort_path),
        n_rows=n_rows,
        n_transfused=n_transfused,
        n_controls=n_controls,
    )


if __name__ == "__main__":
    main()
