from __future__ import annotations

import argparse

from lib.duckdb_utils import connect_duckdb, enable_profiling
from lib.features import (
    build_baseline_features,
    build_t0_table,
    select_base_numeric_columns,
    select_delta_columns,
    select_slope_columns,
)
from lib.logging_utils import Timer, log_event, setup_logging
from lib.schema_utils import (
    get_columns,
    infer_id_column,
    infer_outcomes_transfusion_column,
    infer_time_column,
    infer_transfusion_columns,
    time_expression,
    write_schema_report,
)
from lib.utils import repo_root, resolve_outputs_dir, resolve_shared_dir


def parse_args():
    parser = argparse.ArgumentParser(description="Step 1: Build baseline features using DuckDB")
    parser.add_argument("--run_id", type=str, default=None)
    parser.add_argument("--window", type=int, default=24, choices=[24, 48, 72])
    parser.add_argument("--limit_stays", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    root = repo_root()
    window_dir = resolve_outputs_dir(root, args.run_id, window=args.window)
    features_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="features")
    shared_dir = resolve_shared_dir(root, args.run_id)

    logger, _ = setup_logging("step1_build_baseline_features", window_dir)
    con = connect_duckdb()
    profile_path = window_dir / "profiling" / "step1_build_baseline_features.json"
    enable_profiling(con, profile_path, logger=logger)

    timegrid_glob = (root / "dataset" / "timegrid_features" / "*" / "*.parquet").as_posix()
    timegrid_rel = f"read_parquet('{timegrid_glob}', union_by_name=true)"

    outcomes_rels = {}
    outcomes_full = root / "dataset" / "outputs_outcomes" / "outcomes_by_stay_full.csv"
    if outcomes_full.exists():
        outcomes_rels["outcomes_by_stay_full"] = f"read_csv_auto('{outcomes_full.as_posix()}', SAMPLE_SIZE=100000)"

    outcomes_base = root / "dataset" / "outputs_outcomes" / "outcomes_by_stay.csv"
    if outcomes_base.exists():
        outcomes_rels["outcomes_by_stay"] = f"read_csv_auto('{outcomes_base.as_posix()}', SAMPLE_SIZE=100000)"

    schema_info = write_schema_report(con, shared_dir, timegrid_rel, outcomes_rels, logger)

    timegrid_cols = schema_info["timegrid_cols"]
    timegrid_names = [c[0] for c in timegrid_cols]
    stay_col = schema_info["stay_col"] or infer_id_column(timegrid_names)
    time_col = schema_info["time_col"] or infer_time_column(timegrid_names)
    time_type = schema_info["time_type"]

    if not stay_col or not time_col or not time_type:
        raise RuntimeError("Could not infer stay_id or time column from timegrid schema.")

    transfusion_cols = schema_info["transfusion_cols"] or infer_transfusion_columns(timegrid_names)
    if not transfusion_cols:
        raise RuntimeError("No transfusion-related columns found in timegrid features.")

    outcomes_transf_col = None
    outcomes_stay_col = None
    outcomes_rel = None
    if "outcomes_by_stay" in outcomes_rels:
        outcomes_rel = outcomes_rels["outcomes_by_stay"]
        outcomes_cols = get_columns(con, outcomes_rel)
        outcomes_names = [c[0] for c in outcomes_cols]
        outcomes_transf_col = infer_outcomes_transfusion_column(outcomes_names)
        outcomes_stay_col = infer_id_column(outcomes_names)

    time_expr = time_expression(time_col, time_type, alias="tg")

    log_event(
        logger,
        "schema_inferred",
        stay_col=stay_col,
        time_col=time_col,
        transfusion_cols=transfusion_cols,
        outcomes_transf_col=outcomes_transf_col,
    )

    if args.dry_run:
        log_event(logger, "dry_run", window=args.window)
        return

    t0_path = shared_dir / "t0_table.parquet"
    with Timer(logger, "build_t0_table"):
        build_t0_table(
            con,
            timegrid_rel,
            stay_col,
            time_expr,
            transfusion_cols,
            t0_path,
            logger,
            outcomes_rel=outcomes_rel,
            outcomes_stay_col=outcomes_stay_col,
            outcomes_transf_col=outcomes_transf_col,
        )

    exclude_cols = [stay_col, time_col] + transfusion_cols
    numeric_cols = select_base_numeric_columns(timegrid_cols, exclude_cols=exclude_cols)
    delta_cols = select_delta_columns(numeric_cols)
    slope_cols = select_slope_columns(delta_cols)

    baseline_path = features_dir / "baseline_features.parquet"
    with Timer(logger, "build_baseline_features", window=args.window):
        build_baseline_features(
            con,
            timegrid_rel,
            stay_col,
            time_expr,
            t0_path,
            numeric_cols,
            delta_cols,
            slope_cols,
            args.window,
            baseline_path,
            logger,
            limit_stays=args.limit_stays,
        )

    counts = con.execute(
        f"SELECT count(*) AS n_rows, count(DISTINCT stay_id) AS n_stays FROM read_parquet('{baseline_path.as_posix()}')"
    ).fetchone()
    dup = int(counts[0]) - int(counts[1])
    log_event(logger, "baseline_counts", n_rows=int(counts[0]), n_stays=int(counts[1]), duplicate_stay_rows=dup)

    log_event(logger, "step1_complete", baseline_path=str(baseline_path))


if __name__ == "__main__":
    main()
