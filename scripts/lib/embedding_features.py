from __future__ import annotations

import re
from typing import Dict, List, Tuple

import duckdb

NUMERIC_TYPES = {"INTEGER", "BIGINT", "DOUBLE", "FLOAT", "REAL", "SMALLINT", "TINYINT", "UBIGINT", "HUGEINT", "BOOLEAN"}

EXCLUDE_NAME_PATTERNS = [
    r"transfusion",
    r"rbc_",
]

EXCLUDE_EXACT = {"subject_id", "stay_id", "tbin", "charttime_bin"}

PREFERRED_PATTERNS = [
    "heart_rate",
    "sbp",
    "dbp",
    "map",
    "spo2",
    "resp",
    "temp",
    "lactate",
    "creatinine",
    "hemoglobin",
    "platelet",
    "sofa",
]


def select_embedding_columns(
    columns_with_types: List[Tuple[str, str]],
    max_features: int,
    use_all: bool,
    exclude_cols: List[str] | None = None,
) -> List[str]:
    exclude_set = {c.lower() for c in (exclude_cols or [])}
    base_cols: List[str] = []
    for name, dtype in columns_with_types:
        dtype_upper = dtype.upper()
        if dtype_upper not in NUMERIC_TYPES and not dtype_upper.startswith("DECIMAL"):
            continue
        lname = name.lower()
        if lname in EXCLUDE_EXACT or lname in exclude_set:
            continue
        if any(re.search(pat, lname) for pat in EXCLUDE_NAME_PATTERNS):
            continue
        base_cols.append(name)

    if use_all or max_features <= 0:
        return base_cols

    preferred = [c for c in base_cols if any(p in c.lower() for p in PREFERRED_PATTERNS)]
    if len(preferred) >= max_features:
        return preferred[:max_features]
    remaining = [c for c in base_cols if c not in preferred]
    return preferred + remaining[: max(0, max_features - len(preferred))]


def compute_missing_rates(
    con: duckdb.DuckDBPyConnection,
    timegrid_rel: str,
    t0_rel: str,
    stay_col: str,
    time_expr: str,
    feature_cols: List[str],
    window_minutes: int,
) -> Dict[str, float]:
    if not feature_cols:
        return {}
    agg = ", ".join(
        [f"avg(CASE WHEN tg.\"{c}\" IS NULL THEN 1 ELSE 0 END) AS \"{c}\"" for c in feature_cols]
    )
    query = f"""
    SELECT {agg}
    FROM {timegrid_rel} tg
    JOIN {t0_rel} t0 ON tg.\"{stay_col}\" = t0.stay_id
    WHERE t0.transfused = 1
      AND {time_expr} BETWEEN t0.t0 - {window_minutes} AND t0.t0
    """
    row = con.execute(query).fetchone()
    if row is None:
        return {c: 1.0 for c in feature_cols}
    return {c: float(row[i]) if row[i] is not None else 1.0 for i, c in enumerate(feature_cols)}
