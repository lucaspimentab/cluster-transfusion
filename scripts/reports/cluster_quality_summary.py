from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Resumo de qualidade de cluster (k, silhouette, tamanhos e desbalanceamento)."
    )
    p.add_argument("--run_id", type=str, default="run_cal03_replace_full_w48")
    p.add_argument("--window", type=int, default=48)
    p.add_argument("--metrics_csv", type=str, default="")
    p.add_argument("--assignments_csv", type=str, default="")
    p.add_argument("--out_csv", type=str, default="")
    return p.parse_args()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    args = parse_args()
    root = _repo_root()
    wdir = root / "outputs" / "runs" / args.run_id / f"w{args.window}"

    metrics_csv = (
        Path(args.metrics_csv)
        if args.metrics_csv
        else wdir / "reports" / "cluster_metrics_minirocket.csv"
    )
    assignments_csv = (
        Path(args.assignments_csv)
        if args.assignments_csv
        else wdir / "reports" / "cluster_assignments_mortality_minirocket.csv"
    )
    out_csv = (
        Path(args.out_csv)
        if args.out_csv
        else wdir / "reports_scan_auto_discovery" / "cluster_k_quality_minirocket.csv"
    )

    m = pd.read_csv(metrics_csv)
    a = pd.read_csv(assignments_csv)

    rows = []
    for k, g in a.groupby("k"):
        sizes = g["cluster"].value_counts().sort_values().to_numpy()
        rows.append(
            {
                "k": int(k),
                "n_samples_assign": int(len(g)),
                "n_clusters_found": int(g["cluster"].nunique()),
                "cluster_size_min": int(np.min(sizes)),
                "cluster_size_p25": float(np.percentile(sizes, 25)),
                "cluster_size_median": float(np.percentile(sizes, 50)),
                "cluster_size_p75": float(np.percentile(sizes, 75)),
                "cluster_size_max": int(np.max(sizes)),
                "imbalance_ratio_max_min": float(np.max(sizes) / np.min(sizes))
                if np.min(sizes) > 0
                else np.nan,
            }
        )

    s = pd.DataFrame(rows)
    out = m.merge(s, on="k", how="left").sort_values("k").reset_index(drop=True)

    out["silhouette_rank_desc"] = out["silhouette"].rank(
        method="dense", ascending=False
    ).astype(int)
    out["inertia_rank_asc"] = out["inertia"].rank(method="dense", ascending=True).astype(int)
    out["imbalance_rank_asc"] = out["imbalance_ratio_max_min"].rank(
        method="dense", ascending=True
    ).astype(int)
    out["is_best_silhouette"] = out["silhouette"] == out["silhouette"].max()

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    print(f"[cluster-quality] wrote={out_csv}")
    print(
        out[
            [
                "k",
                "n_samples",
                "n_samples_assign",
                "silhouette",
                "n_clusters_found",
                "cluster_size_min",
                "cluster_size_max",
                "imbalance_ratio_max_min",
                "is_best_silhouette",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
