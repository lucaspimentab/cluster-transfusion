from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb

from .logging_utils import log_event
from .utils import ensure_dir

ID_CANDIDATES = [
    "stay_id",
    "icustay_id",
    "icu_stay_id",
    "patient_stay_id",
    "encounter_id",
]

TIME_CANDIDATES = [
    "tbin",
    "minutes_from_t0",
    "mins_from_t0",
    "minute_from_t0",
    "hours_from_t0",
    "charttime_bin",
    "charttime",
    "ts",
    "time",
    "event_time",
    "time_bin",
]

TRANSFUSION_PATTERNS = [
    r"transfusion",
    r"rbc_transfusion",
    r"rbc_amount",
    r"rbc_totalamount",
    r"blood_product",
    r"prbc",
]

TRANSFUSION_DERIVED_PATTERNS = [
    r"_n_meas",
    r"_diff",
    r"_cumsum",
    r"_cum",
    r"_roll_",
    r"_ema_",
]


def get_columns(con: duckdb.DuckDBPyConnection, relation_sql: str) -> List[Tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {relation_sql}").fetchall()
    return [(row[0], row[1]) for row in rows]


def infer_id_column(columns: List[str]) -> Optional[str]:
    cols_lower = [c.lower() for c in columns]
    for cand in ID_CANDIDATES:
        if cand in cols_lower:
            return columns[cols_lower.index(cand)]
    for name in columns:
        lname = name.lower()
        if "stay" in lname and "id" in lname:
            return name
    return columns[0] if columns else None


def infer_time_column(columns: List[str]) -> Optional[str]:
    cols_lower = [c.lower() for c in columns]
    for cand in TIME_CANDIDATES:
        if cand in cols_lower:
            return columns[cols_lower.index(cand)]
    for name in columns:
        lname = name.lower()
        if "time" in lname or lname.endswith("ts"):
            return name
    return None


def infer_transfusion_columns(columns: List[str]) -> List[str]:
    candidates = []
    for name in columns:
        lname = name.lower()
        if "transferrin" in lname:
            continue
        if any(re.search(pat, lname) for pat in TRANSFUSION_PATTERNS):
            candidates.append(name)
        elif lname.startswith("rbc_") and ("amount" in lname or "transf" in lname):
            candidates.append(name)
    base_candidates = [c for c in candidates if not any(re.search(p, c.lower()) for p in TRANSFUSION_DERIVED_PATTERNS)]
    use_candidates = base_candidates if base_candidates else candidates

    flags = [c for c in use_candidates if "flag" in c.lower()]
    if flags:
        return flags

    amounts = [c for c in use_candidates if "amount" in c.lower() or "ml" in c.lower()]
    ordered = amounts + [c for c in use_candidates if c not in amounts]
    return ordered


def infer_outcomes_transfusion_column(columns: List[str]) -> Optional[str]:
    for name in columns:
        lname = name.lower()
        if "transfusion" in lname or lname in {"transfused", "rbc_transfusion", "rbc_transfused"}:
            return name
    return None


def time_expression(time_col: str, time_type: str, alias: Optional[str] = None) -> str:
    prefix = f"{alias}." if alias else ""
    col = f'{prefix}"{time_col}"'
    if "TIMESTAMP" in time_type.upper():
        return f"epoch({col})/60.0"
    mult = 60.0 if "hour" in time_col.lower() else 1.0
    return f"({col})*{mult}"


def infer_time_step_minutes(con: duckdb.DuckDBPyConnection, relation_sql: str, time_expr: str) -> Optional[float]:
    try:
        query = f"""
        WITH sample AS (
            SELECT {time_expr} AS t
            FROM {relation_sql}
            LIMIT 5000
        ), diffs AS (
            SELECT t - lag(t) OVER (ORDER BY t) AS dt
            FROM sample
        )
        SELECT median(dt) FROM diffs WHERE dt IS NOT NULL AND dt > 0
        """
        value = con.execute(query).fetchone()[0]
        return float(value) if value is not None else None
    except Exception:
        return None


def write_schema_report(
    con: duckdb.DuckDBPyConnection,
    outputs_dir: Path,
    timegrid_rel: str,
    outcomes_rels: Dict[str, str],
    logger,
):
    ensure_dir(outputs_dir)
    report_path = outputs_dir / "schema_report.txt"

    timegrid_cols = get_columns(con, timegrid_rel)
    timegrid_names = [c[0] for c in timegrid_cols]
    stay_col = infer_id_column(timegrid_names)
    time_col = infer_time_column(timegrid_names)
    time_type = None
    if time_col:
        for name, dtype in timegrid_cols:
            if name == time_col:
                time_type = dtype
                break
    transfusion_cols = infer_transfusion_columns(timegrid_names)

    time_expr = time_expression(time_col, time_type, alias="tg") if time_col and time_type else None
    time_step = infer_time_step_minutes(con, f"{timegrid_rel} tg", time_expr) if time_expr else None

    outcomes_info = {}
    for label, rel in outcomes_rels.items():
        cols = get_columns(con, rel)
        outcomes_info[label] = cols

    lines = []
    lines.append("SCHEMA REPORT")
    lines.append("=================")
    lines.append("")
    lines.append("TIMEGRID")
    lines.append(f"- source: {timegrid_rel}")
    lines.append(f"- columns: {len(timegrid_cols)}")
    lines.append(f"- inferred stay_id: {stay_col}")
    lines.append(f"- inferred time column: {time_col}")
    lines.append(f"- inferred time type: {time_type}")
    lines.append(f"- inferred transfusion columns: {transfusion_cols}")
    lines.append(f"- inferred time step minutes: {time_step}")
    lines.append("")
    lines.append("TIMEGRID COLUMNS:")
    for name, dtype in timegrid_cols:
        lines.append(f"  - {name}: {dtype}")
    lines.append("")

    for label, cols in outcomes_info.items():
        lines.append(f"OUTCOMES ({label})")
        lines.append(f"- columns: {len(cols)}")
        lines.append("COLUMNS:")
        for name, dtype in cols:
            lines.append(f"  - {name}: {dtype}")
        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    log_event(logger, "schema_report_written", path=str(report_path))

    return {
        "stay_col": stay_col,
        "time_col": time_col,
        "time_type": time_type,
        "transfusion_cols": transfusion_cols,
        "time_step_minutes": time_step,
        "time_expr": time_expr,
        "timegrid_cols": timegrid_cols,
        "outcomes_info": outcomes_info,
    }
