from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple, Optional

import duckdb

from .logging_utils import log_event

NUMERIC_TYPES = {
    "INTEGER",
    "BIGINT",
    "DOUBLE",
    "FLOAT",
    "REAL",
    "DECIMAL",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "HUGEINT",
    "BOOLEAN",
}

EXCLUDE_PATTERNS = [
    r"_n_meas",
    r"_diff",
    r"_cum",
    r"_roll_",
    r"_ema_",
    r"_delta",
    r"_slope",
]

DELTA_PATTERNS = [
    r"sofa",
    r"lactate",
    r"hemoglobin",
    r"platelet",
    r"creatinine",
    r"bilirubin",
    r"heart_rate",
    r"sbp",
    r"dbp",
    r"map",
    r"resp",
    r"spo2",
    r"fio2",
    r"pao2",
    r"gcs",
    r"vasopressor",
]


def quote_ident(name: str) -> str:
    return f'"{name}"'


def select_base_numeric_columns(
    columns_with_types: List[Tuple[str, str]],
    exclude_cols: Optional[List[str]] = None,
) -> List[str]:
    exclude_cols = exclude_cols or []
    selected = []
    for name, dtype in columns_with_types:
        if name in exclude_cols:
            continue
        dtype_upper = dtype.upper()
        if dtype_upper not in NUMERIC_TYPES and not dtype_upper.startswith("DECIMAL"):
            continue
        if any(re.search(pat, name) for pat in EXCLUDE_PATTERNS):
            continue
        selected.append(name)
    return selected


def select_delta_columns(columns: List[str]) -> List[str]:
    selected = []
    for name in columns:
        lname = name.lower()
        if any(re.search(pat, lname) for pat in DELTA_PATTERNS):
            selected.append(name)
    return selected


def select_slope_columns(columns: List[str]) -> List[str]:
    return select_delta_columns(columns)


def build_t0_table(
    con: duckdb.DuckDBPyConnection,
    timegrid_rel: str,
    stay_col: str,
    time_expr: str,
    transfusion_cols: List[str],
    output_path: Path,
    logger,
    outcomes_rel: Optional[str] = None,
    outcomes_stay_col: Optional[str] = None,
    outcomes_transf_col: Optional[str] = None,
):
    if not transfusion_cols:
        raise ValueError("No transfusion columns detected in timegrid.")

    alias = "tg"
    stay_expr = f"{alias}.{quote_ident(stay_col)}"
    transf_terms = [f"COALESCE({alias}.{quote_ident(c)}, 0) > 0" for c in transfusion_cols]
    transf_expr = " OR ".join(transf_terms)

    base_query = f"""
    WITH base AS (
        SELECT
            {stay_expr} AS stay_id,
            min({time_expr}) AS min_time,
            max({time_expr}) AS max_time,
            max(CASE WHEN {transf_expr} THEN 1 ELSE 0 END) AS any_transf_timegrid,
            min(CASE WHEN {transf_expr} THEN {time_expr} END) AS t0_transf
        FROM {timegrid_rel} {alias}
        GROUP BY stay_id
    ), med AS (
        SELECT median(t0_transf - min_time) AS med_offset
        FROM base
        WHERE t0_transf IS NOT NULL
    )
    SELECT
        base.stay_id,
        base.min_time,
        base.max_time,
        base.any_transf_timegrid,
        CASE
            WHEN base.t0_transf IS NOT NULL THEN base.t0_transf
            WHEN med.med_offset IS NOT NULL THEN LEAST(base.min_time + med.med_offset, base.max_time)
            ELSE base.min_time
        END AS t0
    FROM base, med
    """

    con.execute(f"CREATE OR REPLACE TEMP VIEW t0_base AS {base_query}")

    if outcomes_rel and outcomes_stay_col and outcomes_transf_col:
        join_query = f"""
        SELECT
            t0_base.*,
            CASE
                WHEN o.{quote_ident(outcomes_transf_col)} IS NOT NULL THEN CAST(o.{quote_ident(outcomes_transf_col)} AS INTEGER)
                ELSE t0_base.any_transf_timegrid
            END AS transfused,
            CASE
                WHEN o.{quote_ident(outcomes_transf_col)} IS NOT NULL THEN 'outcomes'
                ELSE 'timegrid'
            END AS transfused_source
        FROM t0_base
        LEFT JOIN {outcomes_rel} o
        ON t0_base.stay_id = o.{quote_ident(outcomes_stay_col)}
        """
        con.execute(f"CREATE OR REPLACE TEMP VIEW t0_final AS {join_query}")
    else:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW t0_final AS "
            "SELECT t0_base.*, any_transf_timegrid AS transfused, 'timegrid' AS transfused_source FROM t0_base"
        )

    con.execute(
        f"COPY (SELECT * FROM t0_final) TO '{output_path.as_posix()}' (FORMAT PARQUET)"
    )

    counts = con.execute(
        "SELECT count(*) AS n_stays, sum(transfused) AS n_transfused FROM t0_final"
    ).fetchone()
    med_offset = None
    try:
        med_offset = con.execute(
            "SELECT median(t0 - min_time) FROM t0_final WHERE transfused = 1"
        ).fetchone()[0]
    except Exception as exc:
        log_event(logger, "t0_median_offset_failed", error=str(exc))
    log_event(
        logger,
        "t0_table_written",
        path=str(output_path),
        n_stays=int(counts[0]),
        n_transfused=int(counts[1] or 0),
        median_t0_offset_minutes=float(med_offset) if med_offset is not None else None,
    )


def build_baseline_features(
    con: duckdb.DuckDBPyConnection,
    timegrid_rel: str,
    stay_col: str,
    time_expr: str,
    t0_path: Path,
    numeric_cols: List[str],
    delta_cols: List[str],
    slope_cols: List[str],
    window_hours: int,
    output_path: Path,
    logger,
    limit_stays: Optional[int] = None,
):
    window_minutes = int(window_hours * 60)
    t0_rel = f"read_parquet('{t0_path.as_posix()}')"

    if limit_stays:
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW t0_used AS "
            f"SELECT * FROM {t0_rel} ORDER BY stay_id LIMIT {int(limit_stays)}"
        )
        t0_rel = "t0_used"

    alias = "tg"
    stay_expr = f"{alias}.{quote_ident(stay_col)}"

    agg_parts = ["count(*) AS n_rows_pre"]
    for name in numeric_cols:
        col = f"{alias}.{quote_ident(name)}"
        safe = name
        agg_parts.append(f"avg({col}) AS {quote_ident(safe + '_mean')}")
        agg_parts.append(f"median({col}) AS {quote_ident(safe + '_median')}")
        agg_parts.append(f"min({col}) AS {quote_ident(safe + '_min')}")
        agg_parts.append(f"max({col}) AS {quote_ident(safe + '_max')}")
        agg_parts.append(f"stddev_pop({col}) AS {quote_ident(safe + '_std')}")

    for name in slope_cols:
        col = f"{alias}.{quote_ident(name)}"
        agg_parts.append(
            f"CASE WHEN var_pop({time_expr}) > 0 THEN covar_pop({time_expr}, {col}) / var_pop({time_expr}) END AS {quote_ident(name + '_slope')}"
        )

    agg_sql = ",\n        ".join(agg_parts)

    pre_query = f"""
    SELECT
        t0.stay_id,
        t0.transfused,
        t0.t0,
        {window_hours} AS window_hours,
        {agg_sql}
    FROM {timegrid_rel} {alias}
    JOIN {t0_rel} t0
        ON {stay_expr} = t0.stay_id
    WHERE {time_expr} BETWEEN t0.t0 - {window_minutes} AND t0.t0
    GROUP BY t0.stay_id, t0.transfused, t0.t0
    """

    con.execute(f"CREATE OR REPLACE TEMP VIEW baseline_pre AS {pre_query}")

    if delta_cols:
        delta_parts = []
        for name in delta_cols:
            col = f"{alias}.{quote_ident(name)}"
            delta_parts.append(
                f"avg(CASE WHEN {time_expr} BETWEEN t0.t0 - {window_minutes} AND t0.t0 THEN {col} END) AS {quote_ident(name + '_pre_mean')}"
            )
            delta_parts.append(
                f"avg(CASE WHEN {time_expr} BETWEEN t0.t0 AND t0.t0 + {window_minutes} THEN {col} END) AS {quote_ident(name + '_post_mean')}"
            )
        delta_sql = ",\n        ".join(delta_parts)
        delta_query = f"""
        SELECT
            t0.stay_id,
            {delta_sql}
        FROM {timegrid_rel} {alias}
        JOIN {t0_rel} t0
            ON {stay_expr} = t0.stay_id
        WHERE {time_expr} BETWEEN t0.t0 - {window_minutes} AND t0.t0 + {window_minutes}
        GROUP BY t0.stay_id
        """
        con.execute(f"CREATE OR REPLACE TEMP VIEW baseline_delta AS {delta_query}")

        delta_cols_expr = []
        for name in delta_cols:
            delta_cols_expr.append(f"baseline_delta.{quote_ident(name + '_pre_mean')}")
            delta_cols_expr.append(f"baseline_delta.{quote_ident(name + '_post_mean')}")
            delta_cols_expr.append(
                f"(baseline_delta.{quote_ident(name + '_post_mean')} - baseline_delta.{quote_ident(name + '_pre_mean')}) AS {quote_ident(name + '_delta')}"
            )
        delta_select = ",\n        ".join(delta_cols_expr)

        final_query = f"""
        SELECT
            baseline_pre.*,
            {delta_select}
        FROM baseline_pre
        LEFT JOIN baseline_delta USING (stay_id)
        """
    else:
        final_query = "SELECT * FROM baseline_pre"

    con.execute(f"COPY ({final_query}) TO '{output_path.as_posix()}' (FORMAT PARQUET)")
    counts = con.execute(f"SELECT count(*) FROM read_parquet('{output_path.as_posix()}')").fetchone()[0]
    log_event(logger, "baseline_features_written", path=str(output_path), rows=int(counts), window_hours=window_hours)
