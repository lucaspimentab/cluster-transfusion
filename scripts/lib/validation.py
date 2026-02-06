from __future__ import annotations

import pandas as pd

from .logging_utils import log_event


def validate_unique_values(df: pd.DataFrame, col: str, logger, label: str, min_unique: int = 2):
    if col not in df.columns:
        log_event(logger, "validate_missing_column", label=label, column=col)
        return False
    unique_count = df[col].nunique(dropna=True)
    log_event(logger, "validate_unique_values", label=label, column=col, unique_count=unique_count)
    if unique_count < min_unique:
        log_event(logger, "validate_unique_values_failed", label=label, column=col, unique_count=unique_count)
        return False
    return True


def validate_no_duplicate_pairs(pairs: pd.DataFrame, logger, allow_transf_duplicates: bool = False):
    ok = True
    if "stay_id_transf" in pairs.columns:
        dup = pairs["stay_id_transf"].duplicated().sum()
        log_event(logger, "validate_pairs_transf_duplicates", duplicates=int(dup))
        if not allow_transf_duplicates:
            ok = ok and dup == 0
    if "stay_id_ctrl" in pairs.columns:
        dup = pairs["stay_id_ctrl"].duplicated().sum()
        log_event(logger, "validate_pairs_ctrl_duplicates", duplicates=int(dup))
        ok = ok and dup == 0
    if set(["stay_id_transf", "stay_id_ctrl"]).issubset(pairs.columns):
        self_match = (pairs["stay_id_transf"] == pairs["stay_id_ctrl"]).sum()
        log_event(logger, "validate_pairs_self_match", self_match=int(self_match))
        ok = ok and self_match == 0
    return ok


def validate_smd_reduction(smd_pre: pd.DataFrame, smd_post: pd.DataFrame, logger):
    if smd_pre.empty or smd_post.empty:
        log_event(logger, "validate_smd_skipped", reason="empty")
        return False
    pre_mean = smd_pre["abs_smd"].mean()
    post_mean = smd_post["abs_smd"].mean()
    log_event(logger, "validate_smd_reduction", pre_mean=pre_mean, post_mean=post_mean)
    return post_mean < pre_mean
