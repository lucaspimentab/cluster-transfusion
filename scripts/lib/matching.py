from __future__ import annotations

from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .logging_utils import log_event


def select_covariate_columns(columns: List[str]) -> List[str]:
    patterns = [
        "age",
        "gender",
        "sex",
        "bmi",
        "hematocrit",
        "hgb",
        "hb",
        "sofa",
        "gcs",
        "vasopressor",
        "press",
        "norepi",
        "epi",
        "lactate",
        "creatinine",
        "urea",
        "bun",
        "bilirubin",
        "platelet",
        "hemoglobin",
        "heart_rate",
        "sbp",
        "dbp",
        "map",
        "mbp",
        "resp",
        "spo2",
        "temperature",
        "fio2",
        "pao2",
        "diabetes",
        "renal",
        "liver",
        "cirrhosis",
        "cancer",
        "malignant",
        "heart_failure",
        "congestive_heart_failure",
        "hypertension",
        "htn",
        "saps",
        "aps",
    ]
    exclude = ["rbc_", "transfusion", "transfused", "t0", "window_hours", "n_rows_pre"]

    selected = []
    for col in columns:
        lcol = col.lower()
        if any(ex in lcol for ex in exclude):
            continue
        if any(p in lcol for p in patterns):
            selected.append(col)
    return sorted(set(selected))


def _encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            if col.lower() in {"gender", "sex"}:
                out[col] = out[col].astype(str).str.upper().map({"M": 1, "F": 0})
            out[col] = out[col].astype("category").cat.codes.replace(-1, np.nan)
    return out


def prepare_design_matrix(df: pd.DataFrame, covariate_cols: List[str]) -> Tuple[np.ndarray, List[str]]:
    X_df = df[covariate_cols].copy()
    X_df = _encode_categoricals(X_df)
    pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    X = pipeline.fit_transform(X_df)
    return X, covariate_cols


def fit_propensity_scores(df: pd.DataFrame, treatment_col: str, covariate_cols: List[str]):
    X, _ = prepare_design_matrix(df, covariate_cols)
    y = df[treatment_col].astype(int).values
    model = LogisticRegression(max_iter=1000, solver="liblinear")
    model.fit(X, y)
    scores = model.predict_proba(X)[:, 1]
    return scores


def match_psm(
    df: pd.DataFrame,
    treatment_col: str,
    covariate_cols: List[str],
    caliper: float,
    seed: int,
    logger,
    ratio: int = 1,
    replace: bool = False,
) -> pd.DataFrame:
    scores = fit_propensity_scores(df, treatment_col, covariate_cols)
    df = df.copy()
    df["propensity_score"] = scores

    eps = 1e-6
    logits = np.log(np.clip(scores, eps, 1 - eps) / np.clip(1 - scores, eps, 1 - eps))
    df["propensity_logit"] = logits

    transf = df[df[treatment_col] == 1].copy()
    ctrl = df[df[treatment_col] == 0].copy()

    caliper_value = caliper * np.nanstd(logits)
    log_event(logger, "psm_caliper", caliper=caliper, caliper_value=caliper_value)

    if len(ctrl) == 0 or len(transf) == 0:
        return pd.DataFrame()

    ratio = max(int(ratio), 1)
    nn = NearestNeighbors(n_neighbors=min(max(20, ratio * 5), len(ctrl)))
    nn.fit(ctrl[["propensity_logit"]].values)
    distances, indices = nn.kneighbors(transf[["propensity_logit"]].values)

    used_ctrl = set()
    rows = []
    for i, transf_row in transf.reset_index(drop=True).iterrows():
        matched_count = 0
        for dist, idx in zip(distances[i], indices[i]):
            if dist > caliper_value:
                continue
            ctrl_idx = ctrl.index[idx]
            if not replace and ctrl_idx in used_ctrl:
                continue
            if not replace:
                used_ctrl.add(ctrl_idx)
            ctrl_row = ctrl.loc[ctrl_idx]
            rows.append(
                {
                    "stay_id_transf": transf_row["stay_id"],
                    "stay_id_ctrl": ctrl_row["stay_id"],
                    "propensity_score": transf_row["propensity_score"],
                    "distance": float(dist),
                }
            )
            matched_count += 1
            if matched_count >= ratio:
                break

    return pd.DataFrame(rows)


def fallback_exact_matching(
    df: pd.DataFrame,
    treatment_col: str,
    covariate_cols: List[str],
    age_caliper: float,
    logger,
    ratio: int = 1,
    replace: bool = False,
) -> pd.DataFrame:
    df = df.copy()
    transf = df[df[treatment_col] == 1].copy()
    ctrl = df[df[treatment_col] == 0].copy()

    if transf.empty or ctrl.empty:
        return pd.DataFrame()

    exact_cols = [c for c in covariate_cols if any(x in c.lower() for x in ["gender", "sex", "diabetes", "renal", "liver", "cancer", "heart_failure"])]
    exact_cols = [c for c in exact_cols if c in df.columns]

    ratio = max(int(ratio), 1)
    used_ctrl = set()
    rows = []

    for _, trow in transf.iterrows():
        candidates = ctrl.copy()
        for col in exact_cols:
            candidates = candidates[candidates[col] == trow[col]]
        if "age_mean" in df.columns:
            candidates = candidates[(candidates["age_mean"].sub(trow["age_mean"]).abs() <= age_caliper)]
        elif "age" in df.columns:
            candidates = candidates[(candidates["age"].sub(trow["age"]).abs() <= age_caliper)]

        if candidates.empty:
            continue

        severity_col = None
        for col in ["sofa_mean", "sofa_pre_mean", "sofa", "aps", "saps"]:
            if col in df.columns:
                severity_col = col
                break
        if severity_col:
            candidates = candidates.assign(
                severity_diff=(candidates[severity_col] - trow[severity_col]).abs()
            ).sort_values("severity_diff")

        matched_count = 0
        for _, crow in candidates.iterrows():
            if not replace and crow["stay_id"] in used_ctrl:
                continue
            if not replace:
                used_ctrl.add(crow["stay_id"])
            rows.append(
                {
                    "stay_id_transf": trow["stay_id"],
                    "stay_id_ctrl": crow["stay_id"],
                    "propensity_score": None,
                    "distance": None,
                }
            )
            matched_count += 1
            if matched_count >= ratio:
                break

    log_event(logger, "fallback_exact_matching", matched=len(rows))
    return pd.DataFrame(rows)


def compute_smd(df: pd.DataFrame, treatment_col: str, covariate_cols: List[str]) -> pd.DataFrame:
    rows = []
    for col in covariate_cols:
        if col not in df.columns:
            continue
        t = df[df[treatment_col] == 1][col].astype(float)
        c = df[df[treatment_col] == 0][col].astype(float)
        mean_t = t.mean()
        mean_c = c.mean()
        var_t = t.var(ddof=0)
        var_c = c.var(ddof=0)
        pooled = np.sqrt((var_t + var_c) / 2.0) if (var_t + var_c) > 0 else 0.0
        smd = (mean_t - mean_c) / pooled if pooled > 0 else 0.0
        rows.append({"covariate": col, "smd": smd, "abs_smd": abs(smd)})
    return pd.DataFrame(rows)
