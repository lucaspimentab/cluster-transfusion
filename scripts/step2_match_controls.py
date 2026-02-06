from __future__ import annotations

import argparse
import warnings
from datetime import datetime, timezone

import pandas as pd

from lib.duckdb_utils import connect_duckdb, enable_profiling
from lib.logging_utils import Timer, log_event, setup_logging
from lib.matching import compute_smd, fallback_exact_matching, match_psm, select_covariate_columns
from lib.utils import repo_root, resolve_outputs_dir
from lib.validation import validate_no_duplicate_pairs, validate_smd_reduction, validate_unique_values


def parse_args():
    parser = argparse.ArgumentParser(description="Step 2: Match controls with propensity score matching")
    parser.add_argument("--run_id", type=str, default=None)
    parser.add_argument("--window", type=int, default=24, choices=[24, 48, 72])
    parser.add_argument("--caliper", type=float, default=0.3)
    parser.add_argument("--ratio", type=int, default=1)
    parser.add_argument("--replace", dest="replace", action="store_true", default=True)
    parser.add_argument("--no-replace", dest="replace", action="store_false")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit_stays", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    warnings.filterwarnings("once", message="Skipping features without any observed values", category=UserWarning)
    args = parse_args()
    root = repo_root()
    window_dir = resolve_outputs_dir(root, args.run_id, window=args.window)
    features_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="features")
    matching_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="matching")

    logger, _ = setup_logging("step2_match_controls", window_dir)

    baseline_path = features_dir / "baseline_features.parquet"
    if not baseline_path.exists():
        raise FileNotFoundError(f"Missing {baseline_path}")

    con = connect_duckdb()
    profile_path = window_dir / "profiling" / "step2_match_controls.json"
    enable_profiling(con, profile_path, logger=logger)
    df = con.execute(f"SELECT * FROM read_parquet('{baseline_path.as_posix()}')").df()

    if "transfused" not in df.columns:
        raise RuntimeError("Baseline features missing transfused label.")

    df = df.rename(columns={"transfused": "treatment"})

    if args.limit_stays:
        transf = df[df["treatment"] == 1]
        ctrl = df[df["treatment"] == 0]
        transf = transf.sample(n=min(args.limit_stays, len(transf)), random_state=args.seed)
        ctrl = ctrl.sample(n=min(args.limit_stays, len(ctrl)), random_state=args.seed)
        df = pd.concat([transf, ctrl], ignore_index=True)

    covariate_cols = select_covariate_columns(df.columns.tolist())
    if not covariate_cols:
        log_event(logger, "no_covariates_detected")

    if args.dry_run:
        log_event(logger, "dry_run", window=args.window, n_rows=len(df), n_transfused=int(df["treatment"].sum()))
        return
    log_event(logger, "matching_params", caliper=args.caliper, ratio=args.ratio, replace=bool(args.replace))

    pairs = pd.DataFrame()
    if len(covariate_cols) >= 2:
        with Timer(logger, "psm_matching"):
            try:
                pairs = match_psm(
                    df,
                    "treatment",
                    covariate_cols,
                    args.caliper,
                    args.seed,
                    logger,
                    ratio=args.ratio,
                    replace=args.replace,
                )
            except Exception as exc:
                log_event(logger, "psm_error", error=str(exc))
    else:
        log_event(logger, "psm_skipped", reason="insufficient_covariates", n_covariates=len(covariate_cols))

    if pairs.empty:
        log_event(logger, "psm_failed_fallback")
        pairs = fallback_exact_matching(
            df,
            "treatment",
            covariate_cols,
            age_caliper=5,
            logger=logger,
            ratio=args.ratio,
            replace=args.replace,
        )

    if pairs.empty:
        log_event(logger, "matching_failed")
        return

    pairs["matched_at"] = datetime.now(timezone.utc).isoformat()
    pairs["window_hours"] = args.window
    pairs["criteria"] = "psm" if pairs["propensity_score"].notna().any() else "fallback_exact"
    pairs["match_ratio"] = args.ratio
    pairs["replace"] = bool(args.replace)

    matched_path = matching_dir / "matched_pairs.parquet"
    matched_csv = matching_dir / "matched_pairs.csv"
    if matched_path.exists():
        existing = con.execute(f"SELECT * FROM read_parquet('{matched_path.as_posix()}')").df()
        pairs = pd.concat([existing, pairs], ignore_index=True)
        pairs = pairs.drop_duplicates(subset=["stay_id_transf", "stay_id_ctrl", "window_hours"])

    con.register("pairs_df", pairs)
    con.execute("CREATE OR REPLACE TEMP TABLE pairs AS SELECT * FROM pairs_df")
    con.execute(f"COPY pairs TO '{matched_path.as_posix()}' (FORMAT PARQUET)")
    con.execute(f"COPY pairs TO '{matched_csv.as_posix()}' (FORMAT CSV, HEADER TRUE)")

    diag_path = matching_dir / "balance_diagnostics.csv"
    if covariate_cols:
        smd_pre = compute_smd(df, "treatment", covariate_cols)
        matched_ids = set(pairs["stay_id_transf"]).union(set(pairs["stay_id_ctrl"]))
        matched_df = df[df["stay_id"].isin(matched_ids)].copy()
        smd_post = compute_smd(matched_df, "treatment", covariate_cols)

        diag = smd_pre.merge(smd_post, on="covariate", suffixes=("_pre", "_post"))
        diag.to_csv(diag_path, index=False)
        validate_smd_reduction(smd_pre, smd_post, logger)
    else:
        log_event(logger, "smd_skipped", reason="no_covariates")

    validate_unique_values(pairs, "stay_id_transf", logger, "matched_pairs")
    validate_unique_values(pairs, "stay_id_ctrl", logger, "matched_pairs")
    validate_no_duplicate_pairs(pairs, logger, allow_transf_duplicates=args.ratio > 1 or args.replace)

    log_event(
        logger,
        "step2_complete",
        matched_path=str(matched_path),
        matched_csv=str(matched_csv),
        diag_path=str(diag_path),
        n_pairs=len(pairs),
    )


if __name__ == "__main__":
    main()
