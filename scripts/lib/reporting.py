from __future__ import annotations

from typing import Dict, List

import pandas as pd


def summarize_outcomes(df: pd.DataFrame, outcome_cols: List[str]) -> Dict[str, float]:
    summary = {}
    for col in outcome_cols:
        if col not in df.columns:
            continue
        summary[col] = float(df[col].mean())
    return summary
