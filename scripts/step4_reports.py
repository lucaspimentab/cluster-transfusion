from __future__ import annotations

import argparse
import re

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from lib.duckdb_utils import connect_duckdb, enable_profiling
from lib.logging_utils import log_event, setup_logging
from lib.utils import parse_k_list, repo_root, resolve_outputs_dir, resolve_shared_dir

REQUIRED_METRICS = {
    "mortality_anytime": "mortality",
    "vm_time_hours": "ventilation",
    "ventilation_hours": "ventilation",
    "rrt_on": "dialysis_rrt",
    "any_vasopressor": "vasopressor",
    "nee_mcgkgmin_max": "vasopressor",
    "icu_los_hours": "icu_los",
}

PHENOTYPE_BASES = [
    "hemoglobin",
    "hematocrit",
    "platelets",
    "lactate",
    "creatinine",
    "urea",
    "bilirubin_total",
    "bilirubin_direct",
    "sofa",
    "gcs",
    "heart_rate",
    "sbp",
    "dbp",
    "mbp",
    "resp_rate",
    "temperature",
    "spo2",
    "glucose_lab",
    "glucose_vital",
    "nee_mcgkgmin",
    "any_vasopressor",
    "age",
    "bmi",
    "weight_kg",
    "height_cm",
    "myocardial_infarct",
    "congestive_heart_failure",
    "peripheral_vascular_disease",
    "cerebrovascular_disease",
    "dementia",
    "chronic_pulmonary_disease",
    "rheumatic_disease",
    "peptic_ulcer_disease",
    "mild_liver_disease",
    "diabetes_without_cc",
    "diabetes_with_cc",
    "renal_disease",
    "malignant_cancer",
    "severe_liver_disease",
    "metastatic_solid_tumor",
    "aids",
    "hypertension",
    "htn",
    "cirrhosis",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Step 4: Embedding cluster reports")
    parser.add_argument("--run_id", type=str, default=None)
    parser.add_argument("--window", type=int, default=24, choices=[24, 48, 72])
    parser.add_argument("--embedding", type=str, default="minirocket", choices=["minirocket"])
    parser.add_argument("--k_list", type=str, default="2,3,4,5,6")
    parser.add_argument("--n_clusters", type=int, default=None)
    parser.add_argument("--silhouette_threshold", type=float, default=0.10)
    parser.add_argument("--silhouette_sample", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _select_metrics(columns):
    patterns = {
        "mortality": [r"mortality", r"death", r"died", r"expire"],
        "ventilation": [r"ventilation", r"vent", r"vm_time"],
        "dialysis_rrt": [r"rrt", r"dialysis"],
        "vasopressor": [r"vasopressor", r"norepi", r"epinephrine", r"press", r"amine", r"vasopressin"],
        "organ_dysfunction": [r"sofa", r"gcs", r"aps", r"saps"],
        "icu_los": [r"icu_los", r"icu_los_hours", r"icu_length"],
    }
    exclude = {"stay_id", "transfused", "t0_transf"}
    selected = {}
    for col in columns:
        if col in exclude:
            continue
        lcol = col.lower()
        for cat, pats in patterns.items():
            if any(re.search(p, lcol) for p in pats):
                selected[col] = cat
                break
    for col, cat in REQUIRED_METRICS.items():
        if col in columns:
            selected[col] = cat
    return [(col, cat) for col, cat in selected.items()]


def _summarize_diff(transf_df, ctrl_df, metrics, n_pairs=None):
    rows = []
    for col, cat in metrics:
        if col not in transf_df.columns or col not in ctrl_df.columns:
            continue
        t = transf_df[col]
        c = ctrl_df[col]
        rows.append(
            {
                "metric": col,
                "category": cat,
                "n_pairs": int(n_pairs) if n_pairs is not None else None,
                "transfused_mean": float(t.mean()),
                "control_mean": float(c.mean()),
                "diff_mean": float(t.mean() - c.mean()),
                "transfused_median": float(t.median()),
                "control_median": float(c.median()),
                "diff_median": float(t.median() - c.median()),
            }
        )
    return pd.DataFrame(rows)


def _compute_silhouette(X, labels, sample_n, seed):
    if sample_n and X.shape[0] > sample_n:
        rng = np.random.default_rng(seed)
        idx = rng.choice(X.shape[0], size=sample_n, replace=False)
        return float(silhouette_score(X[idx], labels[idx]))
    return float(silhouette_score(X, labels))


def _select_phenotype_columns(columns):
    exclude_exact = {"stay_id", "transfused", "t0", "window_hours", "n_rows_pre"}
    selected = [col for col in columns if col not in exclude_exact]
    return sorted(set(selected))


def _summarize_phenotype(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    rows = []
    if df.empty or not cols:
        return pd.DataFrame(rows)
    for col in cols:
        series = df[col]
        rows.append({"feature": col, "stat": "mean", "value": float(series.mean())})
        rows.append({"feature": col, "stat": "median", "value": float(series.median())})
        rows.append({"feature": col, "stat": "missing_rate", "value": float(series.isna().mean())})
    return pd.DataFrame(rows)


def _cleanup_old_outputs(reports_dir, matching_dir, embedding: str) -> None:
    patterns = [
        f"cluster_benefit_{embedding}*.csv",
        f"cluster_metrics_{embedding}*.csv",
        f"cluster_phenotype_{embedding}*.csv",
    ]
    for pattern in patterns:
        for path in reports_dir.glob(pattern):
            path.unlink(missing_ok=True)
        for path in matching_dir.glob(pattern):
            path.unlink(missing_ok=True)


def main():
    args = parse_args()
    root = repo_root()
    window_dir = resolve_outputs_dir(root, args.run_id, window=args.window)
    reports_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="reports")
    matching_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="matching")
    embeddings_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="embeddings")
    features_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="features")
    shared_dir = resolve_shared_dir(root, args.run_id)

    logger, _ = setup_logging("step4_reports", window_dir)

    matched_path = matching_dir / "matched_pairs.parquet"
    cohort_path = shared_dir / "outcomes_cohort.parquet"
    embeddings_path = embeddings_dir / f"embeddings_{args.embedding}.parquet"
    baseline_path = features_dir / "baseline_features.parquet"

    if not matched_path.exists():
        raise FileNotFoundError("Missing matched_pairs.parquet")
    if not cohort_path.exists():
        raise FileNotFoundError("Missing outcomes_cohort.parquet. Run step0_build_outcomes_cohort.py first.")
    if not embeddings_path.exists():
        raise FileNotFoundError(f"Missing embeddings: {embeddings_path}")

    con = connect_duckdb()
    profile_path = window_dir / "profiling" / "step4_reports.json"
    enable_profiling(con, profile_path, logger=logger)

    pairs = con.execute(
        f"SELECT * FROM read_parquet('{matched_path.as_posix()}') WHERE window_hours = {args.window}"
    ).df()
    if pairs.empty:
        log_event(logger, "no_pairs_for_window", window=args.window)
        return

    cohort_df = con.execute(f"SELECT * FROM read_parquet('{cohort_path.as_posix()}')").df()
    emb_df = con.execute(f"SELECT * FROM read_parquet('{embeddings_path.as_posix()}')").df()

    if args.dry_run:
        log_event(
            logger,
            "dry_run",
            n_pairs=len(pairs),
            window=args.window,
            embedding=args.embedding,
            k_list=args.k_list,
        )
        return

    emb_cols = [c for c in emb_df.columns if c.startswith("emb_")]
    if not emb_cols:
        raise RuntimeError("No embedding columns found (expected emb_*)")

    X = emb_df[emb_cols].to_numpy()
    X = StandardScaler().fit_transform(X)

    _cleanup_old_outputs(reports_dir, matching_dir, args.embedding)

    if args.n_clusters:
        k_values = [int(args.n_clusters)]
    else:
        k_values = parse_k_list(args.k_list)
    k_values = sorted({k for k in k_values if k >= 2})

    metrics = _select_metrics(cohort_df.columns)
    baseline = None
    phenotype_cols: list[str] = []
    if baseline_path.exists():
        baseline = con.execute(f"SELECT * FROM read_parquet('{baseline_path.as_posix()}')").df()
        phenotype_cols = _select_phenotype_columns(baseline.columns)

    metrics_rows = []
    combined_rows = []
    phenotype_rows = []

    for k in k_values:
        if X.shape[0] < k:
            metrics_rows.append(
                {
                    "k": k,
                    "n_samples": int(X.shape[0]),
                    "inertia": None,
                    "silhouette": None,
                    "status": "skip_not_enough_samples",
                }
            )
            continue

        model = KMeans(n_clusters=k, random_state=args.seed, n_init=20)
        labels = model.fit_predict(X)
        inertia = float(model.inertia_)
        silhouette = None
        if k > 1 and X.shape[0] > k:
            try:
                silhouette = _compute_silhouette(X, labels, args.silhouette_sample, args.seed)
            except Exception:
                silhouette = None

        metrics_rows.append(
            {
                "k": k,
                "n_samples": int(X.shape[0]),
                "inertia": inertia,
                "silhouette": silhouette,
                "status": "ok" if silhouette is not None else "no_silhouette",
            }
        )

        if silhouette is None or silhouette < args.silhouette_threshold:
            continue

        clusters_df = pd.DataFrame({"stay_id": emb_df["stay_id"], "cluster": labels})
        k_rows = []
        for cluster_id in sorted(clusters_df["cluster"].unique()):
            transf_ids = clusters_df[clusters_df["cluster"] == cluster_id]["stay_id"]
            if transf_ids.empty:
                continue
            pairs_subset = pairs[pairs["stay_id_transf"].isin(transf_ids)]
            if pairs_subset.empty:
                continue
            transf = pairs_subset.merge(cohort_df, left_on="stay_id_transf", right_on="stay_id", how="left")
            ctrl = pairs_subset.merge(cohort_df, left_on="stay_id_ctrl", right_on="stay_id", how="left")
            summary = _summarize_diff(transf, ctrl, metrics, n_pairs=len(pairs_subset))
            summary["cluster"] = int(cluster_id)
            summary["k"] = int(k)
            summary["n_transfused"] = int(transf["stay_id_transf"].nunique())
            summary["n_control"] = int(ctrl["stay_id_ctrl"].nunique())
            k_rows.append(summary)

            if baseline is not None:
                for delta_col in [c for c in baseline.columns if c.endswith("_delta") and "sofa" in c.lower()]:
                    t_vals = baseline[baseline["stay_id"].isin(pairs_subset["stay_id_transf"])][delta_col]
                    c_vals = baseline[baseline["stay_id"].isin(pairs_subset["stay_id_ctrl"])][delta_col]
                    k_rows.append(
                        pd.DataFrame(
                            [
                                {
                                    "metric": delta_col,
                                    "category": "sofa_delta",
                                    "n_pairs": int(len(pairs_subset)),
                                    "transfused_mean": float(t_vals.mean()),
                                    "control_mean": float(c_vals.mean()),
                                    "diff_mean": float(t_vals.mean() - c_vals.mean()),
                                    "transfused_median": float(t_vals.median()),
                                    "control_median": float(c_vals.median()),
                                    "diff_median": float(t_vals.median() - c_vals.median()),
                                    "cluster": int(cluster_id),
                                    "k": int(k),
                                    "n_transfused": int(transf["stay_id_transf"].nunique()),
                                    "n_control": int(ctrl["stay_id_ctrl"].nunique()),
                                }
                            ]
                        )
                    )

            if baseline is not None and phenotype_cols:
                base_transf = baseline[baseline["stay_id"].isin(transf_ids)]
                phen = _summarize_phenotype(base_transf, phenotype_cols)
                if not phen.empty:
                    phen["cluster"] = int(cluster_id)
                    phen["k"] = int(k)
                    phen["n_transfused"] = int(base_transf["stay_id"].nunique())
                    phen["n_control"] = int(ctrl["stay_id_ctrl"].nunique())
                    phenotype_rows.append(phen)

        if k_rows:
            k_df = pd.concat(k_rows, ignore_index=True)
            k_report_path = reports_dir / f"cluster_benefit_{args.embedding}_k{k}.csv"
            k_df.to_csv(k_report_path, index=False)
            k_report_match = matching_dir / f"cluster_benefit_{args.embedding}_k{k}.csv"
            k_df.to_csv(k_report_match, index=False)
            combined_rows.append(k_df)

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_path = reports_dir / f"cluster_metrics_{args.embedding}.csv"
    metrics_df.to_csv(metrics_path, index=False)
    metrics_match = matching_dir / f"cluster_metrics_{args.embedding}.csv"
    metrics_df.to_csv(metrics_match, index=False)

    if phenotype_rows:
        phenotype_df = pd.concat(phenotype_rows, ignore_index=True)
        phen_path = reports_dir / f"cluster_phenotype_{args.embedding}.csv"
        phenotype_df.to_csv(phen_path, index=False)
        phen_match = matching_dir / f"cluster_phenotype_{args.embedding}.csv"
        phenotype_df.to_csv(phen_match, index=False)

    if combined_rows:
        combined = pd.concat(combined_rows, ignore_index=True)
        combined_path = reports_dir / f"cluster_benefit_{args.embedding}.csv"
        combined.to_csv(combined_path, index=False)
        combined_match = matching_dir / f"cluster_benefit_{args.embedding}.csv"
        combined.to_csv(combined_match, index=False)
        log_event(
            logger,
            "step4_complete",
            cluster_report_path=str(combined_path),
            metrics_path=str(metrics_path),
            embedding=args.embedding,
            k_list="%s" % k_values,
            silhouette_threshold=args.silhouette_threshold,
        )
    else:
        log_event(
            logger,
            "step4_complete",
            metrics_path=str(metrics_path),
            embedding=args.embedding,
            k_list="%s" % k_values,
            silhouette_threshold=args.silhouette_threshold,
        )


if __name__ == "__main__":
    main()
