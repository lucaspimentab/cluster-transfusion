from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
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

COMORB_BASES = (
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
)

COND_RE = re.compile(
    r"^\s*([A-Za-z0-9_]+)\s*(<=|>)\s*q[0-9.]+\s*\(([-+0-9.eE]+)\)\s*$"
)


@dataclass
class ParsedCond:
    feature: str
    op: str
    thr: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Gera relatorio formalizado a partir do scan auto discovery."
    )
    p.add_argument("--run_id", type=str, default="run_cal03_replace_full_w48")
    p.add_argument("--window", type=int, default=48)
    p.add_argument("--in_csv", type=str, default="")
    p.add_argument("--out_txt", type=str, default="")
    p.add_argument("--top_benefit_groups", type=int, default=12)
    p.add_argument("--top_harm_groups", type=int, default=12)
    p.add_argument("--cluster_k", type=int, default=3)
    p.add_argument(
        "--cluster_assignments_csv",
        type=str,
        default="",
        help="CSV de cluster assignments (default: reports/cluster_assignments_mortality_minirocket.csv).",
    )
    p.add_argument(
        "--merge_similar_jaccard",
        type=float,
        default=0.75,
        help="Jaccard minimo para fundir grupos parecidos (mesmo sinal).",
    )
    p.add_argument(
        "--merge_min_shared_bases",
        type=int,
        default=2,
        help="Minimo de bases em comum para fundir grupos parecidos.",
    )
    p.add_argument(
        "--disable_similarity_merge",
        action="store_true",
        help="Desativa fusao de grupos parecidos.",
    )
    return p.parse_args()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _base_name(col: str) -> str:
    for suf in ALLOWED_SUFFIXES:
        if col.endswith(suf):
            return col[: -len(suf)]
    return col


def _fmt_signed(v: float, d: int = 2) -> str:
    return f"{v:+.{d}f}"


def _fmt_float(v: float, d: int = 2) -> str:
    if pd.isna(v):
        return "NA"
    return f"{v:.{d}f}"


def _parse_rule_label(label: str) -> list[ParsedCond]:
    out: list[ParsedCond] = []
    parts = [x.strip() for x in str(label).split(" AND ") if x.strip()]
    for part in parts:
        m = COND_RE.match(part)
        if not m:
            continue
        feat, op, thr = m.group(1), m.group(2), float(m.group(3))
        out.append(ParsedCond(feature=feat, op=op, thr=thr))
    return out


def _rule_mask(df: pd.DataFrame, conds: list[ParsedCond]) -> np.ndarray:
    m = np.ones(len(df), dtype=bool)
    for c in conds:
        if c.feature not in df.columns:
            return np.zeros(len(df), dtype=bool)
        vals = pd.to_numeric(df[c.feature], errors="coerce").to_numpy(dtype=float)
        finite = np.isfinite(vals)
        if c.op == "<=":
            m &= finite & (vals <= c.thr)
        else:
            m &= finite & (vals > c.thr)
    return m


def _secondary_summary(df: pd.DataFrame, mask: np.ndarray) -> str:
    if int(mask.sum()) == 0:
        return "NA"
    d = df.loc[mask]
    cont = [
        ("icu_los_hours", "ICU LOS", "h"),
        ("vm_time_hours", "VM time", "h"),
        ("ventilation_hours", "Ventilation", "h"),
        ("nee_mcgkgmin_max", "NEE max", "mcg/kg/min"),
    ]
    bin_out = [
        ("any_vasopressor", "Any vasopressor", "pp"),
        ("rrt_on", "RRT", "pp"),
    ]
    parts: list[str] = []
    for c, nm, unit in cont:
        ct, cc = f"{c}_t", f"{c}_c"
        if ct in d.columns and cc in d.columns:
            diff = float(d[ct].mean() - d[cc].mean())
            parts.append(f"{nm} {_fmt_signed(diff, 1)} {unit}")
    for c, nm, unit in bin_out:
        ct, cc = f"{c}_t", f"{c}_c"
        if ct in d.columns and cc in d.columns:
            diff = float((d[ct].mean() - d[cc].mean()) * 100.0)
            parts.append(f"{nm} {_fmt_signed(diff, 2)} {unit}")
    return " | ".join(parts) if parts else "NA"


def _volume_summary(df: pd.DataFrame, mask: np.ndarray) -> str:
    if int(mask.sum()) == 0:
        return "NA"
    d = df.loc[mask]
    cols = [
        ("rbc_amount_ml_event_max", "evento_max"),
        ("rbc_totalamount_ml_icu_max", "total_icu_max"),
    ]
    out = []
    for c, nm in cols:
        if c not in d.columns:
            continue
        s = pd.to_numeric(d[c], errors="coerce").dropna()
        if s.empty:
            continue
        q25, q50, q75 = np.percentile(s, [25, 50, 75])
        out.append(
            f"{nm} mediana={q50:.1f} [p25={q25:.1f}, p75={q75:.1f}] "
            f"(min={s.min():.1f}, max={s.max():.1f})"
        )
    return "; ".join(out) if out else "NA"


def _feature_stats(df: pd.DataFrame, mask: np.ndarray, feats: list[str]) -> str:
    d = df.loc[mask]
    parts = []
    for f in feats:
        if f not in d.columns:
            continue
        s = pd.to_numeric(d[f], errors="coerce").dropna()
        if s.empty:
            continue
        parts.append(f"{f}: media={s.mean():.2f}, p50={s.median():.2f}")
    return " | ".join(parts) if parts else "NA"


def _static_stats(df: pd.DataFrame, mask: np.ndarray) -> str:
    d = df.loc[mask]
    cols = [
        ("age_mean", "idade_media"),
        ("apsiii_mean", "APSIII_medio"),
        ("sapsii_mean", "SAPSII_medio"),
        ("bmi_mean", "BMI_medio"),
        ("hemoglobin_min", "Hb_media"),
    ]
    vals = []
    for c, nm in cols:
        if c in d.columns:
            vals.append(f"{nm}={_fmt_float(float(pd.to_numeric(d[c], errors='coerce').mean()), 2)}")
    if "hemoglobin_min" in d.columns:
        vals.append(
            f"Hb_p50={_fmt_float(float(pd.to_numeric(d['hemoglobin_min'], errors='coerce').median()), 2)}"
        )
    return ", ".join(vals) if vals else "NA"


def _comorb_summary(
    pair_df: pd.DataFrame,
    mask: np.ndarray,
    comorb_by_stay: pd.DataFrame,
    comorb_cols: list[str],
) -> tuple[str, str]:
    d = pair_df.loc[mask, ["stay_id_transf", "stay_id_ctrl"]]
    if d.empty or not comorb_cols:
        return ("NA", "NA")
    t = comorb_by_stay.reindex(d["stay_id_transf"].astype(int).to_numpy())
    c = comorb_by_stay.reindex(d["stay_id_ctrl"].astype(int).to_numpy())
    p_t = t[comorb_cols].mean(skipna=True) * 100.0
    p_c = c[comorb_cols].mean(skipna=True) * 100.0
    delta = p_t - p_c

    transf_parts = []
    delta_parts = []
    for col in comorb_cols:
        nm = col.replace("_mean", "").replace("_", " ")
        transf_parts.append(f"{nm}={_fmt_float(float(p_t.get(col, np.nan)), 1)}%")
        delta_parts.append(f"{nm}={_fmt_signed(float(delta.get(col, np.nan)), 1)} pp")
    return (" | ".join(transf_parts), " | ".join(delta_parts))


def _cluster_summary(
    pair_df: pd.DataFrame,
    mask: np.ndarray,
    cluster_by_stay: dict[int, int] | None,
    cluster_k: int,
) -> str:
    if not cluster_by_stay:
        return "NA"
    sids = pair_df.loc[mask, "stay_id_transf"].astype(int).to_numpy()
    if len(sids) == 0:
        return "NA"
    cl = pd.Series(sids).map(cluster_by_stay).dropna()
    if cl.empty:
        return "NA"
    cl = cl.astype(int)
    vc = cl.value_counts().sort_values(ascending=False)
    total = int(vc.sum())
    dom = int(vc.index[0])
    dom_n = int(vc.iloc[0])
    dom_pct = 100.0 * dom_n / total
    dist = " | ".join([f"c{int(k)}={int(n)} ({100.0*int(n)/total:.1f}%)" for k, n in vc.items()])
    return (
        f"k={cluster_k} dominante=c{dom} ({dom_pct:.1f}%, n={dom_n}/{total}) | "
        f"distribuicao: {dist}"
    )


def _jaccard(a: np.ndarray, b: np.ndarray) -> float:
    inter = int(np.logical_and(a, b).sum())
    uni = int(np.logical_or(a, b).sum())
    if uni == 0:
        return 0.0
    return inter / uni


def _merge_similar_groups(
    groups_raw: list[dict],
    jaccard_thr: float,
    min_shared_bases: int,
) -> list[dict]:
    n = len(groups_raw)
    if n <= 1:
        return groups_raw
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        gi = groups_raw[i]
        bi = set(gi["base_key"])
        for j in range(i + 1, n):
            gj = groups_raw[j]
            if gi["sign"] != gj["sign"]:
                continue
            bj = set(gj["base_key"])
            if len(bi & bj) < min_shared_bases:
                continue
            jac = _jaccard(gi["union_mask"], gj["union_mask"])
            if jac >= jaccard_thr:
                union(i, j)

    comp: dict[int, list[int]] = {}
    for i in range(n):
        r = find(i)
        comp.setdefault(r, []).append(i)

    merged: list[dict] = []
    for _, idxs in comp.items():
        if len(idxs) == 1:
            g = dict(groups_raw[idxs[0]])
            g["n_subgrupos_fundidos"] = 1
            merged.append(g)
            continue
        members = [groups_raw[i] for i in idxs]
        sign = members[0]["sign"]
        base_union = sorted(set().union(*[set(m["base_key"]) for m in members]))
        code_union = sorted(set().union(*[set(m["code_list"]) for m in members]))
        mask_union = np.logical_or.reduce([m["union_mask"] for m in members])
        merged.append(
            {
                "sign": sign,
                "base_key": tuple(base_union),
                "code_list": code_union,
                "union_mask": mask_union,
                "n_subgrupos_fundidos": len(members),
            }
        )
    return merged


def main() -> None:
    args = parse_args()
    root = _repo_root()
    run_dir = root / "outputs" / "runs" / args.run_id
    wdir = run_dir / f"w{args.window}"
    reports_dir = wdir / "reports_scan_auto_discovery"

    in_csv = Path(args.in_csv) if args.in_csv else reports_dir / "auto_discovery_rules_strong.csv"
    out_txt = (
        Path(args.out_txt)
        if args.out_txt
        else reports_dir / "relatorio_scan_auto_discovery_formalizado.txt"
    )

    matched_path = wdir / "matching" / "matched_pairs.parquet"
    features_path = wdir / "features" / "baseline_features.parquet"
    outcomes_path = run_dir / "shared" / "outcomes_cohort.parquet"

    con = duckdb.connect()
    pairs = con.execute(
        f"SELECT stay_id_transf, stay_id_ctrl FROM read_parquet('{matched_path.as_posix()}') "
        f"WHERE window_hours = {args.window}"
    ).df()
    base = con.execute(f"SELECT * FROM read_parquet('{features_path.as_posix()}')").df()
    outcomes = con.execute(f"SELECT * FROM read_parquet('{outcomes_path.as_posix()}')").df()

    pairs["stay_id_transf"] = pairs["stay_id_transf"].astype(int)
    pairs["stay_id_ctrl"] = pairs["stay_id_ctrl"].astype(int)
    base["stay_id"] = base["stay_id"].astype(int)
    outcomes["stay_id"] = outcomes["stay_id"].astype(int)

    out_t = outcomes.rename(columns={c: f"{c}_t" for c in outcomes.columns if c != "stay_id"}).rename(
        columns={"stay_id": "stay_id_transf"}
    )
    out_c = outcomes.rename(columns={c: f"{c}_c" for c in outcomes.columns if c != "stay_id"}).rename(
        columns={"stay_id": "stay_id_ctrl"}
    )

    pair_df = (
        pairs.merge(out_t, on="stay_id_transf", how="left")
        .merge(out_c, on="stay_id_ctrl", how="left")
        .merge(base, left_on="stay_id_transf", right_on="stay_id", how="left")
        .drop(columns=["stay_id"])
    )

    # comorbidades (percentual por subgrupo)
    comorb_cols = [f"{b}_mean" for b in COMORB_BASES if f"{b}_mean" in base.columns]
    comorb_by_stay = base[["stay_id"] + comorb_cols].copy().set_index("stay_id") if comorb_cols else pd.DataFrame()

    # clusterizacao previa (para representar de qual cluster vem cada subgrupo fisiologico)
    default_cluster_csv = wdir / "reports" / "cluster_assignments_mortality_minirocket.csv"
    cluster_csv = Path(args.cluster_assignments_csv) if args.cluster_assignments_csv else default_cluster_csv
    cluster_by_stay: dict[int, int] | None = None
    if cluster_csv.exists():
        cadf = pd.read_csv(cluster_csv)
        need_cols = {"stay_id", "cluster", "k", "group"}
        if need_cols.issubset(set(cadf.columns)):
            cadf = cadf[(cadf["k"] == args.cluster_k) & (cadf["group"] == "transfused")].copy()
            cluster_by_stay = (
                cadf[["stay_id", "cluster"]].drop_duplicates("stay_id").set_index("stay_id")["cluster"].to_dict()
            )
    quality_csv = reports_dir / "cluster_k_quality_minirocket.csv"

    rules = pd.read_csv(in_csv)
    if rules.empty:
        raise RuntimeError("CSV de regras vazio.")

    parsed_cache: dict[str, list[ParsedCond]] = {}
    mask_cache: dict[str, np.ndarray] = {}
    base_set_cache: dict[str, tuple[str, ...]] = {}

    for r in rules.itertuples(index=False):
        conds = _parse_rule_label(r.label)
        parsed_cache[r.code] = conds
        m = _rule_mask(pair_df, conds)
        mask_cache[r.code] = m
        base_set_cache[r.code] = tuple(sorted({_base_name(c.feature) for c in conds}))

    rules["sign"] = np.where(rules["diff_pp"] < 0, "benefit", "harm")
    rules["base_key"] = rules["code"].map(base_set_cache)

    groups_raw: list[dict] = []
    for (sign, base_key), g in rules.groupby(["sign", "base_key"], dropna=False):
        code_list = g["code"].tolist()
        masks = [mask_cache[c] for c in code_list]
        union_mask = np.logical_or.reduce(masks) if masks else np.zeros(len(pair_df), dtype=bool)
        groups_raw.append(
            {
                "sign": sign,
                "base_key": base_key,
                "code_list": code_list,
                "union_mask": union_mask,
            }
        )

    if args.disable_similarity_merge:
        groups_merged = [{**g, "n_subgrupos_fundidos": 1} for g in groups_raw]
    else:
        groups_merged = _merge_similar_groups(
            groups_raw,
            jaccard_thr=args.merge_similar_jaccard,
            min_shared_bases=args.merge_min_shared_bases,
        )

    group_rows = []
    for g in groups_merged:
        gr = rules[rules["code"].isin(g["code_list"])].copy()
        idx_rep = gr["abs_diff_pp"].idxmax()
        rep = rules.loc[idx_rep]
        group_rows.append(
            {
                "sign": g["sign"],
                "base_key": g["base_key"],
                "code_list": g["code_list"],
                "union_mask": g["union_mask"],
                "n_subgrupos_fundidos": int(g["n_subgrupos_fundidos"]),
                "n_rules": int(len(gr)),
                "diff_min": float(gr["diff_pp"].min()),
                "diff_max": float(gr["diff_pp"].max()),
                "diff_med": float(gr["diff_pp"].median()),
                "abs_diff_med": float(np.abs(gr["diff_pp"]).median()),
                "n_pairs_min": int(gr["n_pairs"].min()),
                "n_pairs_max": int(gr["n_pairs"].max()),
                "n_exato_total": int(g["union_mask"].sum()),
                "rep_code": rep["code"],
            }
        )

    gdf = pd.DataFrame(group_rows)
    g_b = gdf[gdf["sign"] == "benefit"].sort_values(
        ["n_subgrupos_fundidos", "n_rules", "abs_diff_med"], ascending=[False, False, False]
    )
    g_h = gdf[gdf["sign"] == "harm"].sort_values(
        ["abs_diff_med", "n_subgrupos_fundidos", "n_rules"], ascending=[False, False, False]
    )
    g_b = g_b.head(args.top_benefit_groups)
    g_h = g_h.head(args.top_harm_groups)

    lines: list[str] = []
    lines.append("RELATORIO FORMALIZADO - SCAN AUTO DISCOVERY (FENOTIPOS FISIOLOGICOS)")
    lines.append("Versao consolidada: agrupamento por combinacao de bases fisiologicas.")
    if not args.disable_similarity_merge:
        lines.append(
            f"Fusao de grupos parecidos: ON (jaccard>={args.merge_similar_jaccard:.2f}, bases_em_comum>={args.merge_min_shared_bases})"
        )
    else:
        lines.append("Fusao de grupos parecidos: OFF")
    lines.append("")
    lines.append(
        f"Total grupos: {len(g_b) + len(g_h)} (maleficio={len(g_h)}, beneficio={len(g_b)})"
    )
    lines.append("")

    def write_section(title: str, sec_df: pd.DataFrame) -> None:
        lines.append(title)
        for _, row in sec_df.iterrows():
            rep = rules[rules["code"] == row["rep_code"]].iloc[0]
            rep_mask = mask_cache[row["rep_code"]]
            rep_conds = parsed_cache[row["rep_code"]]
            rep_feats = [c.feature for c in rep_conds]
            union_codes = row["code_list"]
            union_mask = row["union_mask"]

            base_txt = " + ".join(str(b).replace("_", " ") for b in row["base_key"])
            merged_tag = (
                f" | subgrupos_fundidos={int(row['n_subgrupos_fundidos'])}"
                if int(row["n_subgrupos_fundidos"]) > 1
                else ""
            )
            lines.append(
                f"- Bases [{base_txt}] | diff_global={_fmt_signed(float(row['diff_min']),2)} a {_fmt_signed(float(row['diff_max']),2)} pp | "
                f"n_regras={int(row['n_rules'])} | n_exato_total={int(row['n_exato_total'])}{merged_tag}"
            )
            lines.append(
                f"  representante: {rep['label']} | n={int(rep['n_pairs'])} | diff={_fmt_signed(float(rep['diff_pp']),2)} pp | "
                f"IC95% {_fmt_signed(float(rep['ci_lo_pp']),2)} a {_fmt_signed(float(rep['ci_hi_pp']),2)} pp | "
                f"q_fdr={float(rep['q_fdr']):.6g} | stability={float(rep['stability']):.2f}"
            )
            lines.append(
                f"  estaticos_consolidados: {_static_stats(pair_df, union_mask)}"
            )
            com_t, com_d = _comorb_summary(pair_df, union_mask, comorb_by_stay, comorb_cols)
            lines.append(f"  comorbidades_%_transfundido: {com_t}")
            lines.append(f"  comorbidades_delta_vs_controle_pp: {com_d}")
            lines.append(
                f"  cluster_representacao: {_cluster_summary(pair_df, union_mask, cluster_by_stay, args.cluster_k)}"
            )
            lines.append(
                f"  exames_representativo: {_feature_stats(pair_df, rep_mask, rep_feats)}"
            )
            lines.append(
                f"  volume_transfusao_ml_representativo: {_volume_summary(pair_df, rep_mask)}"
            )
            lines.append(
                f"  desfechos_secundarios_representativo: {_secondary_summary(pair_df, rep_mask)}"
            )

    write_section("GRUPOS DE MALEFICIO", g_h)
    lines.append("")
    write_section("GRUPOS DE BENEFICIO", g_b)
    lines.append("")
    lines.append("NOTA DE CLUSTERIZACAO PARA ANOTACAO DOS SUBGRUPOS")
    lines.append(f"cluster_k_utilizado={args.cluster_k}")
    if quality_csv.exists():
        qdf = pd.read_csv(quality_csv)
        qdf = qdf.sort_values("k")
        sel = qdf[qdf["k"] == args.cluster_k]
        if not sel.empty:
            r = sel.iloc[0]
            lines.append(
                f"k={int(r['k'])}: silhouette={float(r['silhouette']):.3f}, "
                f"imbalance_max_min={float(r['imbalance_ratio_max_min']):.2f}, "
                f"cluster_size_min={int(r['cluster_size_min'])}, cluster_size_max={int(r['cluster_size_max'])}"
            )
        lines.append("comparativo_k:")
        for _, r in qdf.iterrows():
            lines.append(
                f"- k={int(r['k'])}: silhouette={float(r['silhouette']):.3f}, "
                f"imbalance_max_min={float(r['imbalance_ratio_max_min']):.2f}, "
                f"size_min={int(r['cluster_size_min'])}, size_max={int(r['cluster_size_max'])}"
            )
    else:
        lines.append("comparativo_k: NA (cluster_k_quality_minirocket.csv nao encontrado)")
    lines.append("")
    lines.append(f"Fonte tabular: {in_csv.relative_to(root).as_posix()}")

    out_txt.write_text("\n".join(lines), encoding="utf-8")
    print(f"[scan-auto-formal] wrote={out_txt}")


if __name__ == "__main__":
    main()
