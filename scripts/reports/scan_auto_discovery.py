from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


PHYSIO_BASES = {
    "hemoglobin",
    "hematocrit",
    "platelets",
    "pt",
    "ptt",
    "fibrinogen",
    "d_dimer",
    "lactate",
    "glucose_lab",
    "creatinine",
    "urea",
    "magnesium",
    "phosphorus",
    "potassium",
    "sodium",
    "albumin",
    "hba1c",
    "alt",
    "ast",
    "alkaline_phosphatase",
    "bilirubin_total",
    "bilirubin_direct",
    "bilirubin_indirect",
    "ck_total",
    "ldh",
    "wbc",
    "lymphocytes_abs",
    "heart_rate",
    "sbp",
    "dbp",
    "mbp",
    "resp_rate",
    "temperature",
    "spo2",
    "glucose_vital",
    "any_vasopressor",
    "nee_mcgkgmin",
    "sofa",
    "apsiii",
    "sapsii",
    "gcs",
    "rrt_on",
}

CORE_PHYSIO_BASES = {
    "hemoglobin",
    "platelets",
    "lactate",
    "creatinine",
    "urea",
    "bilirubin_total",
    "heart_rate",
    "sbp",
    "dbp",
    "mbp",
    "resp_rate",
    "temperature",
    "spo2",
    "any_vasopressor",
    "nee_mcgkgmin",
    "sofa",
    "apsiii",
    "sapsii",
    "gcs",
    "rrt_on",
}

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

EXCLUDE_EXACT = {
    "stay_id",
    "transfused",
    "t0",
    "window_hours",
    "n_rows_pre",
    "subject_id_mean",
    "subject_id_median",
    "subject_id_min",
    "subject_id_max",
    "subject_id_std",
}

AXIS_MAP = {
    "hemoglobin": "anemia",
    "hematocrit": "anemia",
    "platelets": "organ_failure",
    "pt": "organ_failure",
    "ptt": "organ_failure",
    "fibrinogen": "organ_failure",
    "d_dimer": "organ_failure",
    "lactate": "perfusion",
    "creatinine": "organ_failure",
    "urea": "organ_failure",
    "bilirubin_total": "organ_failure",
    "bilirubin_direct": "organ_failure",
    "bilirubin_indirect": "organ_failure",
    "albumin": "organ_failure",
    "rrt_on": "organ_failure",
    "heart_rate": "perfusion",
    "sbp": "perfusion",
    "dbp": "perfusion",
    "mbp": "perfusion",
    "any_vasopressor": "perfusion",
    "nee_mcgkgmin": "perfusion",
    "resp_rate": "respiratory",
    "spo2": "respiratory",
    "temperature": "metabolic",
    "apsiii": "severity",
    "sapsii": "severity",
    "sofa": "severity",
    "gcs": "severity",
}


@dataclass
class Condition:
    cond_id: int
    feature: str
    feature_base: str
    axis: str
    op: str
    thr: float
    q: float
    mask: np.ndarray
    label: str


@dataclass
class BeamRule:
    cond_ids: tuple[int, ...]
    used_bases: frozenset[str]
    used_axes: frozenset[str]
    mask: np.ndarray
    score: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scan automatico de subgrupos fisiologicos com diversidade de eixos e estabilidade."
    )
    p.add_argument("--run_id", type=str, default="run_cal03_replace_full_w48")
    p.add_argument("--window", type=int, default=48)
    p.add_argument("--feature_set", type=str, default="all", choices=["all", "core"])
    p.add_argument(
        "--drop_bases",
        type=str,
        default="",
        help="Bases a remover da descoberta (csv), ex: apsiii,sapsii,sofa,gcs",
    )
    p.add_argument("--quantiles", type=str, default="0.2,0.35,0.5,0.65,0.8")
    p.add_argument("--min_pairs", type=int, default=200)
    p.add_argument("--min_abs_diff_pp", type=float, default=15.0)
    p.add_argument("--min_prevalence", type=float, default=0.05)
    p.add_argument("--max_prevalence", type=float, default=0.40)
    p.add_argument("--min_depth", type=int, default=2)
    p.add_argument("--max_depth", type=int, default=3)
    p.add_argument("--min_axes", type=int, default=2)
    p.add_argument("--pool_size", type=int, default=250)
    p.add_argument(
        "--pool_mode",
        type=str,
        default="balanced",
        choices=["balanced", "absolute"],
        help="Como selecionar o pool univariado: balanced (metade beneficio/metade maleficio) ou absolute.",
    )
    p.add_argument("--beam_width", type=int, default=60)
    p.add_argument("--top_bootstrap", type=int, default=250)
    p.add_argument("--bootstrap_iters", type=int, default=80)
    p.add_argument("--bootstrap_min_stability", type=float, default=0.70)
    p.add_argument("--random_state", type=int, default=42)
    p.add_argument("--hb_col", type=str, default="hemoglobin_min")
    return p.parse_args()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _base_name(col: str) -> str:
    for suf in ALLOWED_SUFFIXES:
        if col.endswith(suf):
            return col[: -len(suf)]
    return col


def _axis_name(base: str) -> str:
    return AXIS_MAP.get(base, "other")


def _bh_fdr(pvals: np.ndarray) -> np.ndarray:
    n = len(pvals)
    if n == 0:
        return np.array([])
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = np.empty(n, dtype=float)
    prev = 1.0
    for i in range(n - 1, -1, -1):
        rank = i + 1
        val = ranked[i] * n / rank
        prev = min(prev, val)
        q[i] = prev
    out = np.empty(n, dtype=float)
    out[order] = np.clip(q, 0.0, 1.0)
    return out


def _two_prop_ci(mort_t: np.ndarray, mort_c: np.ndarray) -> tuple[float, float, float]:
    n = len(mort_t)
    if n == 0:
        return np.nan, np.nan, np.nan
    p1 = float(np.mean(mort_t))
    p0 = float(np.mean(mort_c))
    diff = p1 - p0
    se = math.sqrt(max(1e-12, (p1 * (1 - p1) / n) + (p0 * (1 - p0) / n)))
    d = 1.96 * se
    return diff, diff - d, diff + d


def _p_from_ci(diff: float, ci_lo: float, ci_hi: float) -> float:
    if np.isnan(diff) or np.isnan(ci_lo) or np.isnan(ci_hi):
        return 1.0
    se = (ci_hi - diff) / 1.96 if ci_hi > diff else (diff - ci_lo) / 1.96
    if se <= 0:
        return 1.0
    z = abs(diff / se)
    return float(np.clip(math.erfc(z / math.sqrt(2.0)), 0.0, 1.0))


def _bootstrap_stability(
    mort_t: np.ndarray,
    mort_c: np.ndarray,
    expected_sign: int,
    n_iter: int,
    rng: np.random.Generator,
) -> float:
    n = len(mort_t)
    if n == 0:
        return 0.0
    ok = 0
    for _ in range(n_iter):
        idx = rng.integers(0, n, size=n)
        d = float(mort_t[idx].mean() - mort_c[idx].mean())
        if expected_sign < 0 and d < 0:
            ok += 1
        elif expected_sign > 0 and d > 0:
            ok += 1
    return ok / n_iter


def main() -> None:
    args = parse_args()
    quantiles = sorted({float(x.strip()) for x in args.quantiles.split(",") if x.strip()})
    rng = np.random.default_rng(args.random_state)

    root = _repo_root()
    run_dir = root / "outputs" / "runs" / args.run_id
    wdir = run_dir / f"w{args.window}"
    out_dir = wdir / "reports_scan_auto_discovery"
    if out_dir.exists():
        for p in out_dir.iterdir():
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                for sp in p.rglob("*"):
                    if sp.is_file():
                        sp.unlink()
                for sp in sorted(p.rglob("*"), reverse=True):
                    if sp.is_dir():
                        sp.rmdir()
                p.rmdir()
    out_dir.mkdir(parents=True, exist_ok=True)

    matched_path = wdir / "matching" / "matched_pairs.parquet"
    features_path = wdir / "features" / "baseline_features.parquet"
    outcomes_path = run_dir / "shared" / "outcomes_cohort.parquet"

    con = duckdb.connect()
    pairs = con.execute(
        f"SELECT stay_id_transf, stay_id_ctrl FROM read_parquet('{matched_path.as_posix()}') "
        f"WHERE window_hours = {args.window}"
    ).df()
    base = con.execute(f"SELECT * FROM read_parquet('{features_path.as_posix()}')").df()
    outcomes = con.execute(
        f"SELECT stay_id, mortality_anytime FROM read_parquet('{outcomes_path.as_posix()}')"
    ).df()

    pairs["stay_id_transf"] = pairs["stay_id_transf"].astype(int)
    pairs["stay_id_ctrl"] = pairs["stay_id_ctrl"].astype(int)
    base["stay_id"] = base["stay_id"].astype(int)
    outcomes["stay_id"] = outcomes["stay_id"].astype(int)

    pair_df = (
        pairs.merge(
            outcomes.rename(columns={"stay_id": "stay_id_transf", "mortality_anytime": "mort_t"}),
            on="stay_id_transf",
            how="left",
        )
        .merge(
            outcomes.rename(columns={"stay_id": "stay_id_ctrl", "mortality_anytime": "mort_c"}),
            on="stay_id_ctrl",
            how="left",
        )
        .merge(base, left_on="stay_id_transf", right_on="stay_id", how="left")
        .drop(columns=["stay_id"])
    )
    n_total = len(pair_df)

    selected_bases = PHYSIO_BASES if args.feature_set == "all" else CORE_PHYSIO_BASES
    drop_bases = {x.strip() for x in args.drop_bases.split(",") if x.strip()}
    if drop_bases:
        selected_bases = {b for b in selected_bases if b not in drop_bases}
    feat_cols = []
    for c in pair_df.columns:
        if c in EXCLUDE_EXACT:
            continue
        if not pd.api.types.is_numeric_dtype(pair_df[c]):
            continue
        if not c.endswith(ALLOWED_SUFFIXES):
            continue
        if _base_name(c) in selected_bases:
            feat_cols.append(c)
    feat_cols = sorted(set(feat_cols))
    if not feat_cols:
        raise RuntimeError("Nenhuma feature fisiologica elegivel.")

    hb_col = args.hb_col if args.hb_col in pair_df.columns else None
    mort_t_arr = pair_df["mort_t"].to_numpy(dtype=float)
    mort_c_arr = pair_df["mort_c"].to_numpy(dtype=float)

    def eval_mask(
        mask: np.ndarray,
        cond_ids: tuple[int, ...],
        label: str,
        used_features: list[str],
        used_axes: list[str],
    ) -> dict | None:
        n = int(mask.sum())
        if n < args.min_pairs:
            return None
        prev = n / n_total
        if prev < args.min_prevalence or prev > args.max_prevalence:
            return None
        if len(used_axes) < args.min_axes:
            return None
        mt = mort_t_arr[mask]
        mc = mort_c_arr[mask]
        diff, ci_lo, ci_hi = _two_prop_ci(mt, mc)
        p = _p_from_ci(diff, ci_lo, ci_hi)
        diff_pp = diff * 100.0
        score = abs(diff_pp) * math.sqrt(n / 100.0) * (1.0 + 0.08 * (len(set(used_axes)) - 1))
        out = {
            "code": f"R{'-'.join(map(str, cond_ids))}",
            "label": label,
            "n_conditions": len(cond_ids),
            "features_used": ",".join(used_features),
            "axes_used": ",".join(used_axes),
            "n_axes": len(set(used_axes)),
            "n_pairs": n,
            "prevalence": prev,
            "mort_t": float(mt.mean()),
            "mort_c": float(mc.mean()),
            "diff": float(diff),
            "ci_lo": float(ci_lo),
            "ci_hi": float(ci_hi),
            "p_perm": float(p),
            "score": float(score),
        }
        if hb_col:
            hb_vals = pair_df.loc[mask, hb_col].astype(float)
            out["hb_mean"] = float(hb_vals.mean())
            out["hb_p50"] = float(hb_vals.median())
        else:
            out["hb_mean"] = np.nan
            out["hb_p50"] = np.nan
        return out

    # 1) univariate pool
    conditions: list[Condition] = []
    uni_rows = []
    cid = 0
    for c in feat_cols:
        vals = pair_df[c].replace([np.inf, -np.inf], np.nan).astype(float).to_numpy()
        finite = np.isfinite(vals)
        if finite.sum() == 0:
            continue
        base_name = _base_name(c)
        axis = _axis_name(base_name)
        for q in quantiles:
            thr = float(np.nanquantile(vals, q))
            for op in ("<=", ">"):
                if op == "<=":
                    m = finite & (vals <= thr)
                else:
                    m = finite & (vals > thr)
                n = int(m.sum())
                if n < args.min_pairs:
                    continue
                prev = n / n_total
                if prev < args.min_prevalence or prev > args.max_prevalence:
                    continue
                label = f"{c} {op} q{q:.2f} ({thr:.4g})"
                rec = eval_mask(m, (cid,), label, [c], [axis] if args.min_axes <= 1 else [axis, axis])
                if rec is None:
                    continue
                rec["feature"] = c
                rec["feature_base"] = base_name
                rec["axis"] = axis
                rec["op"] = op
                rec["thr"] = thr
                rec["q"] = q
                uni_rows.append(rec)
                conditions.append(Condition(cid, c, base_name, axis, op, thr, q, m, label))
                cid += 1

    uni_df = pd.DataFrame(uni_rows).sort_values("score", ascending=False)
    if uni_df.empty:
        raise RuntimeError("Nao foi possivel gerar condicoes univariadas.")

    if args.pool_mode == "balanced":
        half = max(1, args.pool_size // 2)
        uni_b = uni_df[uni_df["diff"] < 0].head(half)
        uni_h = uni_df[uni_df["diff"] > 0].head(half)
        pool_df = pd.concat([uni_b, uni_h], ignore_index=True)
        if len(pool_df) < args.pool_size:
            missing = args.pool_size - len(pool_df)
            extra = uni_df[~uni_df["code"].isin(pool_df["code"])].head(missing)
            pool_df = pd.concat([pool_df, extra], ignore_index=True)
        pool_ids = set(pool_df["code"].str.replace("R", "", regex=False).astype(int).tolist())
    else:
        pool_ids = set(
            uni_df.head(args.pool_size)["code"].str.replace("R", "", regex=False).astype(int).tolist()
        )
    cond_pool = [c for c in conditions if c.cond_id in pool_ids]
    cond_pool = sorted(cond_pool, key=lambda x: x.cond_id)
    cond_by_id = {c.cond_id: c for c in cond_pool}

    # 2) beam search with diversity constraints
    all_rules = []
    seen = set()
    beam: list[BeamRule] = []
    mask_by_code: dict[str, np.ndarray] = {}

    # depth 1 seeds (only for expansion, not final if min_depth>1)
    for c in cond_pool:
        rec = eval_mask(c.mask, (c.cond_id,), c.label, [c.feature], [c.axis] if args.min_axes <= 1 else [c.axis, c.axis])
        if rec is None:
            continue
        key = (c.cond_id,)
        if key in seen:
            continue
        seen.add(key)
        code = rec["code"]
        mask_by_code[code] = c.mask
        if args.min_depth <= 1:
            all_rules.append(rec)
        beam.append(BeamRule((c.cond_id,), frozenset([c.feature_base]), frozenset([c.axis]), c.mask, rec["score"]))
    beam = sorted(beam, key=lambda x: x.score, reverse=True)[: args.beam_width]

    # depth >=2
    for depth in range(2, args.max_depth + 1):
        next_beam: list[BeamRule] = []
        for r in beam:
            max_id = max(r.cond_ids)
            for c in cond_pool:
                if c.cond_id <= max_id:
                    continue
                if c.feature_base in r.used_bases:
                    continue
                key = tuple(list(r.cond_ids) + [c.cond_id])
                if key in seen:
                    continue
                m = r.mask & c.mask
                if int(m.sum()) < args.min_pairs:
                    continue
                used_feats = [cond_by_id[i].feature for i in key]
                used_axes = [cond_by_id[i].axis for i in key]
                labels = [cond_by_id[i].label for i in key]
                rec = eval_mask(m, key, " AND ".join(labels), used_feats, used_axes)
                if rec is None:
                    continue
                seen.add(key)
                code = rec["code"]
                mask_by_code[code] = m
                if depth >= args.min_depth:
                    all_rules.append(rec)
                next_beam.append(
                    BeamRule(
                        key,
                        frozenset(list(r.used_bases) + [c.feature_base]),
                        frozenset(list(r.used_axes) + [c.axis]),
                        m,
                        rec["score"],
                    )
                )
        if not next_beam:
            break
        beam = sorted(next_beam, key=lambda x: x.score, reverse=True)[: args.beam_width]

    out = pd.DataFrame(all_rules).drop_duplicates(subset=["label"]).copy()
    if out.empty:
        raise RuntimeError("Busca automatica nao retornou regras.")

    out["q_fdr"] = _bh_fdr(out["p_perm"].to_numpy(float))
    out["diff_pp"] = out["diff"] * 100.0
    out["ci_lo_pp"] = out["ci_lo"] * 100.0
    out["ci_hi_pp"] = out["ci_hi"] * 100.0
    out["abs_diff_pp"] = out["diff_pp"].abs()
    out["robust"] = ((out["ci_lo"] > 0) | (out["ci_hi"] < 0)) & (out["q_fdr"] < 0.05)

    strong = out[
        (out["n_pairs"] >= args.min_pairs)
        & (out["abs_diff_pp"] >= args.min_abs_diff_pp)
        & (out["robust"])
    ].copy()

    # 3) dedup by semantic signature (sign + base/op set)
    def signature(row: pd.Series) -> str:
        parts = []
        for cond_txt in row["label"].split(" AND "):
            token = cond_txt.split(" q")[0].strip()  # ex: apsiii_mean <=
            chunks = token.split()
            if len(chunks) >= 2:
                feat = chunks[0]
                op = chunks[1]
                parts.append(f"{_base_name(feat)} {op}")
            else:
                parts.append(token)
        sign = "benefit" if row["diff_pp"] < 0 else "harm"
        return sign + " | " + " ; ".join(sorted(parts))

    if not strong.empty:
        strong["signature"] = strong.apply(signature, axis=1)
        strong = strong.sort_values("score", ascending=False).drop_duplicates(subset=["signature"])

    # 4) bootstrap stability on top candidates (balanceado por sinal)
    if not strong.empty:
        strong = strong.sort_values("score", ascending=False).copy()
        idx_eval = set()
        for sign in (-1, 1):
            part = strong[strong["diff_pp"] * sign > 0].sort_values("score", ascending=False)
            idx_eval.update(part.head(args.top_bootstrap).index.tolist())
        stability = []
        for row in strong.itertuples():
            if row.Index not in idx_eval:
                stability.append(np.nan)
                continue
            mask = mask_by_code.get(row.code)
            if mask is None:
                stability.append(np.nan)
                continue
            mt = mort_t_arr[mask]
            mc = mort_c_arr[mask]
            sign = -1 if row.diff_pp < 0 else 1
            st = _bootstrap_stability(mt, mc, sign, args.bootstrap_iters, rng)
            stability.append(float(st))
        strong["stability"] = stability
        strong = strong[
            strong["stability"].notna() & (strong["stability"] >= args.bootstrap_min_stability)
        ].copy()

    strong_b = strong[strong["diff_pp"] < 0].sort_values("diff_pp")
    strong_h = strong[strong["diff_pp"] > 0].sort_values("diff_pp", ascending=False)

    out.sort_values("score", ascending=False).to_csv(out_dir / "auto_discovery_rules_all.csv", index=False)
    uni_df.to_csv(out_dir / "auto_discovery_univariate_pool.csv", index=False)
    strong.to_csv(out_dir / "auto_discovery_rules_strong.csv", index=False)
    strong_b.to_csv(out_dir / "auto_discovery_rules_strong_benefit.csv", index=False)
    strong_h.to_csv(out_dir / "auto_discovery_rules_strong_harm.csv", index=False)

    lines = []
    lines.append("SCAN AUTO DISCOVERY (SEM REGRAS PREDEFINIDAS, COM DIVERSIDADE + ESTABILIDADE)")
    lines.append(f"run_id={args.run_id} | window={args.window}")
    lines.append(
        f"feature_set={args.feature_set} | n_features={len(feat_cols)} | n_quantiles={len(quantiles)} | "
        f"min_pairs={args.min_pairs} | prevalence=[{args.min_prevalence:.2f},{args.max_prevalence:.2f}]"
    )
    lines.append(
        f"depth=[{args.min_depth},{args.max_depth}] | min_axes={args.min_axes} | "
        f"pool={args.pool_size} | beam={args.beam_width} | bootstrap={args.bootstrap_iters}"
    )
    lines.append(
        f"rules_total={len(out)} | strong_final={len(strong)} | "
        f"benefit={len(strong_b)} | harm={len(strong_h)}"
    )
    lines.append("")
    lines.append("TOP BENEFICIO (descoberto automaticamente):")
    for r in strong_b.head(20).itertuples(index=False):
        lines.append(
            f"- {r.label} | n={int(r.n_pairs)} | diff={float(r.diff_pp):+.2f} pp | "
            f"axes={r.axes_used} | stability={float(r.stability):.2f}"
        )
    lines.append("")
    lines.append("TOP MALEFICIO (descoberto automaticamente):")
    for r in strong_h.head(20).itertuples(index=False):
        lines.append(
            f"- {r.label} | n={int(r.n_pairs)} | diff={float(r.diff_pp):+.2f} pp | "
            f"axes={r.axes_used} | stability={float(r.stability):.2f}"
        )
    (out_dir / "relatorio_scan_auto_discovery.txt").write_text("\n".join(lines), encoding="utf-8")

    print(f"[auto-discovery] out_dir={out_dir}")
    print(
        f"[auto-discovery] rules_total={len(out)} strong_final={len(strong)} "
        f"benefit={len(strong_b)} harm={len(strong_h)}"
    )


if __name__ == "__main__":
    main()
