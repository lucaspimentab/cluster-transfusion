from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ALLOWED_SUFFIXES = (
    "_mean",
    "_median",
    "_min",
    "_max",
    "_std",
    "_slope",
    "_pre_mean",
    "_post_mean",
    "_delta",
)

COND_RE = re.compile(
    r"^\s*([A-Za-z0-9_]+)\s*(<=|>)\s*q[0-9.]+\s*\(([-+0-9.eE]+)\)\s*$"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cruza subgrupos do scan com clusters MiniRocket (k=2 e k=3)."
    )
    p.add_argument("--run_id", type=str, default="run_cal03_replace_full_w48")
    p.add_argument("--window", type=int, default=48)
    p.add_argument("--scan_csv", type=str, default="")
    p.add_argument("--cluster_csv", type=str, default="")
    p.add_argument("--top_groups", type=int, default=30)
    return p.parse_args()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _base_name(col: str) -> str:
    for suf in ALLOWED_SUFFIXES:
        if col.endswith(suf):
            return col[: -len(suf)]
    return col


def _parse_rule_label(label: str) -> list[tuple[str, str, float]]:
    out: list[tuple[str, str, float]] = []
    for part in [x.strip() for x in str(label).split(" AND ") if x.strip()]:
        m = COND_RE.match(part)
        if not m:
            continue
        out.append((m.group(1), m.group(2), float(m.group(3))))
    return out


def _rule_mask(df: pd.DataFrame, conds: list[tuple[str, str, float]]) -> np.ndarray:
    m = np.ones(len(df), dtype=bool)
    for feat, op, thr in conds:
        if feat not in df.columns:
            return np.zeros(len(df), dtype=bool)
        vals = pd.to_numeric(df[feat], errors="coerce").to_numpy(dtype=float)
        finite = np.isfinite(vals)
        if op == "<=":
            m &= finite & (vals <= thr)
        else:
            m &= finite & (vals > thr)
    return m


def _cluster_pct(ids: np.ndarray, stay_to_cluster: pd.Series) -> dict[int, float]:
    cl = pd.Series(ids).map(stay_to_cluster).dropna().astype(int)
    if cl.empty:
        return {}
    vc = cl.value_counts(normalize=True).sort_index()
    return {int(k): float(v * 100.0) for k, v in vc.items()}


def _dominant_cluster(pct: dict[int, float]) -> tuple[str, float]:
    if not pct:
        return ("NA", np.nan)
    k = max(pct, key=lambda c: pct[c])
    return (f"c{k}", pct[k])


def main() -> None:
    args = parse_args()
    root = _repo_root()
    run_dir = root / "outputs" / "runs" / args.run_id
    wdir = run_dir / f"w{args.window}"
    reports_scan = wdir / "reports_scan_auto_discovery"

    scan_csv = (
        Path(args.scan_csv)
        if args.scan_csv
        else reports_scan / "auto_discovery_rules_strong.csv"
    )
    cluster_csv = (
        Path(args.cluster_csv)
        if args.cluster_csv
        else wdir / "reports" / "cluster_assignments_mortality_minirocket.csv"
    )

    out_groups_csv = reports_scan / "scan_cluster_cross_k2_k3_groups.csv"
    out_sign_csv = reports_scan / "scan_cluster_cross_k2_k3_sign.csv"
    out_txt = reports_scan / "relatorio_scan_cluster_cross_k2_k3.txt"

    matched_path = wdir / "matching" / "matched_pairs.parquet"
    features_path = wdir / "features" / "baseline_features.parquet"

    con = duckdb.connect()
    pairs = con.execute(
        f"SELECT stay_id_transf, stay_id_ctrl FROM read_parquet('{matched_path.as_posix()}') "
        f"WHERE window_hours = {args.window}"
    ).df()
    base = con.execute(f"SELECT * FROM read_parquet('{features_path.as_posix()}')").df()

    pairs["stay_id_transf"] = pairs["stay_id_transf"].astype(int)
    pairs["stay_id_ctrl"] = pairs["stay_id_ctrl"].astype(int)
    base["stay_id"] = base["stay_id"].astype(int)

    pair_df = (
        pairs.merge(base, left_on="stay_id_transf", right_on="stay_id", how="left")
        .drop(columns=["stay_id"])
        .reset_index(drop=True)
    )

    rules = pd.read_csv(scan_csv)
    if rules.empty:
        raise RuntimeError("scan_csv vazio.")
    rules["sign"] = np.where(rules["diff_pp"] < 0, "benefit", "harm")

    # parse masks
    mask_by_code: dict[str, np.ndarray] = {}
    bases_by_code: dict[str, tuple[str, ...]] = {}
    for r in rules.itertuples(index=False):
        conds = _parse_rule_label(r.label)
        mask_by_code[r.code] = _rule_mask(pair_df, conds)
        bases_by_code[r.code] = tuple(sorted({_base_name(f) for f, _, _ in conds}))

    rules["base_key"] = rules["code"].map(bases_by_code)

    # cluster maps (k=2 and k=3)
    ca = pd.read_csv(cluster_csv)
    ca = ca[(ca["group"] == "transfused") & (ca["k"].isin([2, 3]))].copy()
    c2 = ca[ca["k"] == 2][["stay_id", "cluster"]].drop_duplicates("stay_id").set_index("stay_id")["cluster"]
    c3 = ca[ca["k"] == 3][["stay_id", "cluster"]].drop_duplicates("stay_id").set_index("stay_id")["cluster"]

    base_ids = pair_df["stay_id_transf"].astype(int).to_numpy()
    base2 = _cluster_pct(base_ids, c2)
    base3 = _cluster_pct(base_ids, c3)

    # group-level
    group_rows: list[dict] = []
    for (sign, base_key), g in rules.groupby(["sign", "base_key"], dropna=False):
        code_list = g["code"].tolist()
        union_mask = np.logical_or.reduce([mask_by_code[c] for c in code_list])
        ids = pair_df.loc[union_mask, "stay_id_transf"].astype(int).to_numpy()
        p2 = _cluster_pct(ids, c2)
        p3 = _cluster_pct(ids, c3)
        d2, d2p = _dominant_cluster(p2)
        d3, d3p = _dominant_cluster(p3)
        row = {
            "sign": sign,
            "bases": " + ".join([b.replace("_", " ") for b in base_key]),
            "n_rules": int(len(g)),
            "n_pairs_union": int(len(np.unique(ids))),
            "diff_med_pp": float(g["diff_pp"].median()),
            "diff_min_pp": float(g["diff_pp"].min()),
            "diff_max_pp": float(g["diff_pp"].max()),
            "dominant_k2": d2,
            "dominant_k2_pct": d2p,
            "dominant_k3": d3,
            "dominant_k3_pct": d3p,
        }
        for c in sorted(set(list(base2.keys()) + list(p2.keys()))):
            row[f"k2_c{c}_pct"] = float(p2.get(c, 0.0))
            row[f"k2_c{c}_enrich_pp"] = float(p2.get(c, 0.0) - base2.get(c, 0.0))
        for c in sorted(set(list(base3.keys()) + list(p3.keys()))):
            row[f"k3_c{c}_pct"] = float(p3.get(c, 0.0))
            row[f"k3_c{c}_enrich_pp"] = float(p3.get(c, 0.0) - base3.get(c, 0.0))
        k2_enrich = {
            int(c): row[f"k2_c{int(c)}_enrich_pp"]
            for c in sorted(set(list(base2.keys()) + list(p2.keys())))
        }
        k3_enrich = {
            int(c): row[f"k3_c{int(c)}_enrich_pp"]
            for c in sorted(set(list(base3.keys()) + list(p3.keys())))
        }
        if k2_enrich:
            c2b = max(k2_enrich, key=lambda k: k2_enrich[k])
            row["k2_most_enriched_cluster"] = f"c{c2b}"
            row["k2_most_enriched_pp"] = float(k2_enrich[c2b])
        else:
            row["k2_most_enriched_cluster"] = "NA"
            row["k2_most_enriched_pp"] = np.nan
        if k3_enrich:
            c3b = max(k3_enrich, key=lambda k: k3_enrich[k])
            row["k3_most_enriched_cluster"] = f"c{c3b}"
            row["k3_most_enriched_pp"] = float(k3_enrich[c3b])
        else:
            row["k3_most_enriched_cluster"] = "NA"
            row["k3_most_enriched_pp"] = np.nan
        group_rows.append(row)

    gdf = pd.DataFrame(group_rows)
    gdf = gdf.sort_values(["sign", "n_rules", "n_pairs_union", "diff_med_pp"], ascending=[True, False, False, True])

    # sign-level (benefit vs harm)
    sign_rows = []
    for sign in ["benefit", "harm"]:
        codes = rules.loc[rules["sign"] == sign, "code"].tolist()
        if not codes:
            continue
        union_mask = np.logical_or.reduce([mask_by_code[c] for c in codes])
        ids = pair_df.loc[union_mask, "stay_id_transf"].astype(int).to_numpy()
        p2 = _cluster_pct(ids, c2)
        p3 = _cluster_pct(ids, c3)
        d2, d2p = _dominant_cluster(p2)
        d3, d3p = _dominant_cluster(p3)
        row = {
            "sign": sign,
            "n_pairs_union": int(len(np.unique(ids))),
            "dominant_k2": d2,
            "dominant_k2_pct": d2p,
            "dominant_k3": d3,
            "dominant_k3_pct": d3p,
        }
        for c in sorted(set(list(base2.keys()) + list(p2.keys()))):
            row[f"k2_c{c}_pct"] = float(p2.get(c, 0.0))
            row[f"k2_c{c}_enrich_pp"] = float(p2.get(c, 0.0) - base2.get(c, 0.0))
        for c in sorted(set(list(base3.keys()) + list(p3.keys()))):
            row[f"k3_c{c}_pct"] = float(p3.get(c, 0.0))
            row[f"k3_c{c}_enrich_pp"] = float(p3.get(c, 0.0) - base3.get(c, 0.0))
        k2_enrich = {
            int(c): row[f"k2_c{int(c)}_enrich_pp"]
            for c in sorted(set(list(base2.keys()) + list(p2.keys())))
        }
        k3_enrich = {
            int(c): row[f"k3_c{int(c)}_enrich_pp"]
            for c in sorted(set(list(base3.keys()) + list(p3.keys())))
        }
        if k2_enrich:
            c2b = max(k2_enrich, key=lambda k: k2_enrich[k])
            row["k2_most_enriched_cluster"] = f"c{c2b}"
            row["k2_most_enriched_pp"] = float(k2_enrich[c2b])
        else:
            row["k2_most_enriched_cluster"] = "NA"
            row["k2_most_enriched_pp"] = np.nan
        if k3_enrich:
            c3b = max(k3_enrich, key=lambda k: k3_enrich[k])
            row["k3_most_enriched_cluster"] = f"c{c3b}"
            row["k3_most_enriched_pp"] = float(k3_enrich[c3b])
        else:
            row["k3_most_enriched_cluster"] = "NA"
            row["k3_most_enriched_pp"] = np.nan
        sign_rows.append(row)

    sdf = pd.DataFrame(sign_rows)

    # save CSVs
    out_groups_csv.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_csv(out_groups_csv, index=False)
    sdf.to_csv(out_sign_csv, index=False)

    # text summary
    lines = []
    lines.append("CRUZAMENTO SCAN x CLUSTER (K=2 e K=3)")
    lines.append(f"run_id={args.run_id} | window={args.window}")
    lines.append("")
    lines.append(
        "Baseline transfundidos | "
        + "k2: "
        + ", ".join([f"c{k}={v:.1f}%" for k, v in sorted(base2.items())])
        + " | k3: "
        + ", ".join([f"c{k}={v:.1f}%" for k, v in sorted(base3.items())])
    )
    lines.append("")
    lines.append("Resumo por sinal:")
    for r in sdf.itertuples(index=False):
        lines.append(
            f"- {r.sign}: n={int(r.n_pairs_union)} | k2 dominante={r.dominant_k2} ({float(r.dominant_k2_pct):.1f}%) | "
            f"k3 dominante={r.dominant_k3} ({float(r.dominant_k3_pct):.1f}%) | "
            f"enriquecido k2={r.k2_most_enriched_cluster} ({float(r.k2_most_enriched_pp):+.1f} pp) | "
            f"enriquecido k3={r.k3_most_enriched_cluster} ({float(r.k3_most_enriched_pp):+.1f} pp)"
        )
    lines.append("")
    lines.append(f"Top grupos (n={args.top_groups}) por n_rules:")
    cols_show = ["sign", "bases", "n_rules", "n_pairs_union", "dominant_k2", "dominant_k2_pct", "dominant_k3", "dominant_k3_pct", "diff_med_pp"]
    top = gdf.head(args.top_groups)
    for r in top[cols_show].itertuples(index=False):
        lines.append(
            f"- {r.sign} | {r.bases} | n_rules={int(r.n_rules)} | n={int(r.n_pairs_union)} | "
            f"k2={r.dominant_k2} ({float(r.dominant_k2_pct):.1f}%) | "
            f"k3={r.dominant_k3} ({float(r.dominant_k3_pct):.1f}%) | diff_med={float(r.diff_med_pp):+.2f} pp"
        )

    out_txt.write_text("\n".join(lines), encoding="utf-8")

    print(f"[scan-cluster-cross] groups_csv={out_groups_csv}")
    print(f"[scan-cluster-cross] sign_csv={out_sign_csv}")
    print(f"[scan-cluster-cross] txt={out_txt}")


if __name__ == "__main__":
    main()
