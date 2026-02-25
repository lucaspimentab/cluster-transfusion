from __future__ import annotations

from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd

from ..constants import COMORBID_COLS, DEMOGRAPHIC_COLS, ID_TIME_COLS


DEFAULT_ROLLING_WINDOW = 12  # 12 bins = 60 min
DEFAULT_EMA_SPAN = 12

_EXCLUDE_COLS = set(ID_TIME_COLS + DEMOGRAPHIC_COLS + COMORBID_COLS + ["stay_bucket"])


def _is_binary(series: pd.Series) -> bool:
    vals = series.dropna().unique()
    if len(vals) == 0:
        return False
    return np.isin(vals, [0, 1]).all()


def _candidate_cols(df: pd.DataFrame) -> List[str]:
    num_cols = df.select_dtypes(include=[np.number, "bool"]).columns.tolist()
    cols = [c for c in num_cols if c not in _EXCLUDE_COLS and not c.endswith("_imputed")]
    return cols


def _split_dynamic_static(df: pd.DataFrame, cols: Iterable[str]) -> Tuple[List[str], List[str]]:
    cols = list(cols)
    if not cols or "stay_id" not in df.columns:
        return [], []
    nunique = df.groupby("stay_id", sort=False)[cols].nunique(dropna=True)
    dynamic = [c for c in cols if nunique[c].max() > 1]
    static = [c for c in cols if c not in dynamic]
    return dynamic, static


def add_engineered_features_bucket(
    df: pd.DataFrame,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
    ema_span: int = DEFAULT_EMA_SPAN,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if "stay_id" not in df.columns or "tbin" not in df.columns:
        raise ValueError("Expected stay_id and tbin in dataframe for feature engineering.")

    df = df.sort_values(["stay_id", "tbin"], kind="mergesort").copy()

    cand_cols = _candidate_cols(df)
    dynamic_cols, _ = _split_dynamic_static(df, cand_cols)

    binary_cols = [c for c in dynamic_cols if _is_binary(df[c])]
    cont_cols = [c for c in dynamic_cols if c not in binary_cols]

    g = df.groupby("stay_id", sort=False)
    new_cols = {}

    for col in cont_cols:
        new_cols[f"{col}_n_meas"] = g[col].transform(lambda x: x.notna().cumsum())
        new_cols[f"{col}_diff"] = g[col].diff().fillna(0)
        new_cols[f"{col}_cummin"] = g[col].cummin()
        new_cols[f"{col}_cummax"] = g[col].cummax()
        new_cols[f"{col}_cumean"] = g[col].expanding().mean().reset_index(level=0, drop=True)
        new_cols[f"{col}_roll_mean_{rolling_window}"] = g[col].transform(
            lambda x: x.rolling(rolling_window, min_periods=1).mean()
        )
        new_cols[f"{col}_roll_std_{rolling_window}"] = g[col].transform(
            lambda x: x.rolling(rolling_window, min_periods=1).std()
        ).fillna(0)
        new_cols[f"{col}_ema_{ema_span}"] = g[col].transform(lambda x: x.ewm(span=ema_span, adjust=False).mean())

    for col in binary_cols:
        new_cols[f"{col}_diff"] = g[col].diff().fillna(0)
        new_cols[f"{col}_cumsum"] = g[col].cumsum()
        new_cols[f"{col}_roll_mean_{rolling_window}"] = g[col].transform(
            lambda x: x.rolling(rolling_window, min_periods=1).mean()
        )

    if new_cols:
        df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df
