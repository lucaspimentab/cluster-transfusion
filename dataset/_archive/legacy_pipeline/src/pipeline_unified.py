"""
Unified MIMIC-IV pipeline (single file)

This file merges the previous `pipeline_postgres.py` runner and the `pipeline_pandas_steps.py`
(step implementations) into one module, removing legacy SQL-CTE helpers that were no longer used
by the current execution path (partition-wise pandas pipeline).

Notes:
- This module still depends on your project package modules (config/constants/impute/io_utils/etc.).
- The public entry point remains: `run_pipeline_postgres(...)`.
"""



from __future__ import annotations

import gc
import re
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

from .config import load_config, LabItemConfig
from .constants import (
    REQUIRED_COLUMNS_BY_STEP,
    STEP_NAMES,
    STAY_BUCKET_MOD,
    ID_TIME_COLS,
    VITAL_COLS,
    LAB_COLS,
    DEMOGRAPHIC_COLS,
    THERAPY_COLS,
    TRANSFUSION_COLS,
    SCORE_COLS,
    FLUID_COLS,
    COMORBID_COLS,
)
from .impute import impute_dataset
from .io_utils import ensure_dir, write_dataset_in_chunks, compute_missingness
from .logging_utils import setup_logging, log_event
from .reporting import MissingnessTracker, write_feature_dictionary
from .features.labs_vitals import VITAL_PATTERNS
from .features.transfusion import RBC_LABEL_PATTERNS
from .features.engineered_features import add_engineered_features_bucket


# =============================================================================
# Local step configuration overrides
# -----------------------------------------------------------------------------
# This unified pipeline reorders steps so that:
#   STEP 01 = DEMOGRAPHICS
#   STEP 02 = LABS + VITALS
# The remaining steps keep their original meaning/order.
# =============================================================================

# Step names used for folder layout on disk (STEP_NAMES[step_num])
STEP_NAMES = {
    1: "demographics",
    2: "labs_vitals",
    3: "therapies",
    4: "transfusions",
    5: "scores",
    6: "fluids",
    7: "comorbidities",
    8: "engineered_features",
}

# Columns expected to be present at the end of each step (used by imputation + validation)
REQUIRED_COLUMNS_BY_STEP = {
    1: ID_TIME_COLS + DEMOGRAPHIC_COLS,
    2: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS,
    3: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS + THERAPY_COLS,
    4: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS + THERAPY_COLS + TRANSFUSION_COLS,
    5: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS,
    6: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS + FLUID_COLS,
    7: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS + FLUID_COLS + COMORBID_COLS,
    8: ID_TIME_COLS + DEMOGRAPHIC_COLS + LAB_COLS + VITAL_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS + FLUID_COLS + COMORBID_COLS,
}

# Columns you explicitly don't want to keep in outputs/samples
META_COLS_TO_DROP = ["intime", "outtime", "staybucket", "hadm_id", "ham_id", "charttime"]


def _normalize_time_bins(df: pd.DataFrame) -> pd.DataFrame:
    """Align time columns to the 5-minute grid."""
    if df is None or df.empty:
        return df

    # tbin: garantir múltiplo de 5 e inteiro
    if "tbin" in df.columns:
        t = pd.to_numeric(df["tbin"], errors="coerce")
        t = (np.floor(t / 5.0) * 5.0).astype("Int64")
        df["tbin"] = t

    # charttime_bin: floor para 5 minutos
    if "charttime_bin" in df.columns:
        df["charttime_bin"] = pd.to_datetime(df["charttime_bin"], errors="coerce").dt.floor("5min")

    return df


def _reorder_time_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure column order has charttime_bin before tbin (if both exist)."""
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    if "charttime_bin" in cols and "tbin" in cols:
        cols.remove("charttime_bin")
        cols.remove("tbin")
        # Keep ids first if present, then charttime_bin, tbin, then the rest
        front = []
        for c in ["subject_id", "stay_id"]:
            if c in df.columns and c in cols:
                cols.remove(c)
                front.append(c)
        # keep partition col near front if present
        if "stay_bucket" in df.columns and "stay_bucket" in cols:
            cols.remove("stay_bucket")
            front.append("stay_bucket")
        ordered = front + ["charttime_bin", "tbin"] + cols
        df = df[ordered]
    return df


def _clip_and_round_numeric(df: pd.DataFrame, clip_abs: float = 1e6, decimals: int = 2) -> pd.DataFrame:
    """Replace inf with NA, clip float columns to ±clip_abs, and round to `decimals`."""
    if df is None or df.empty:
        return df

    for col in df.columns:
        s = df[col]
        # skip ids and non-numeric
        if col in {"subject_id", "stay_id", "tbin", "stay_bucket"}:
            continue
        if pd.api.types.is_datetime64_any_dtype(s) or pd.api.types.is_datetime64tz_dtype(s):
            continue
        if pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):
            continue

        if pd.api.types.is_integer_dtype(s):
            continue  # keep integers intact

        # float-like
        v = pd.to_numeric(s, errors="coerce")
        v = v.replace([np.inf, -np.inf], np.nan)
        v = v.clip(lower=-clip_abs, upper=clip_abs)
        df[col] = v.round(decimals)

    return df


def _postprocess_common(step_num: int, df: Optional[pd.DataFrame], drop_imputed: bool = False) -> pd.DataFrame:
    """Common postprocess applied after each step (and after imputation when present).

    - Align time bins (tbin multiple of 5; charttime_bin floored to 5min)
    - Drop unwanted meta cols (keeps stay_bucket for partitioning)
    - Remove *_imputed columns if requested (e.g., STEP 01 demographics)
    - Replace +/-inf with NA, clip numeric features to a safe range, round floats to 2 decimals
    - Ensure charttime_bin appears before tbin
    """
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame() if df is None else df

    df = _normalize_time_bins(df)
    df = _clip_and_round_numeric(df, clip_abs=1e6, decimals=2)

    # Drop meta cols (but keep stay_bucket if needed for partitioning)
    drop_cols = [c for c in META_COLS_TO_DROP if c in df.columns]
    drop_cols = [c for c in drop_cols if c != "stay_bucket"]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    if drop_imputed:
        imputed_cols = [c for c in df.columns if c.endswith("_imputed")]
        if imputed_cols:
            df = df.drop(columns=imputed_cols)

    df = _reorder_time_cols(df)
    return df
def _table_exists(table_index: Set[Tuple[str, str]], schema: str, table: str) -> bool:
    return (schema, table) in table_index

def _fetch_columns(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (schema, table),
        )
        rows = cursor.fetchall()
    return [r[0] for r in rows]

def _fetch_dataframe(conn, query: str, params: Optional[Tuple[Any, ...]] = None) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)

STRING_COLS = {"gender", "rrt_type"}
DATETIME_COLS = {"charttime_bin", "intime", "outtime"}
ID_COLS = {"subject_id", "stay_id", "hadm_id", "tbin", "stay_bucket"}


STRING_COLS = {"gender", "rrt_type"}
DATETIME_COLS = {"charttime_bin", "intime", "outtime"}
ID_COLS = {"subject_id", "stay_id", "hadm_id", "tbin", "stay_bucket"}


def _normalize_batch_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col in STRING_COLS:
            df[col] = df[col].astype("string")
        elif col in DATETIME_COLS:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            numeric = pd.to_numeric(df[col], errors="coerce")
            if col in ID_COLS:
                df[col] = numeric.astype("Int64")
            else:
                df[col] = numeric.astype("float64")
    return df

def _remove_path(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)

def _assert_dataset_exists(path: Path, step: int) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing intermediate dataset for step {step}: {path}")

def _feature_cols_for_step(step: int) -> List[str]:
    """
    Columns to be imputed in a given step.
    IMPORTANT: demographics are NEVER imputed (so they never produce *_imputed).
    """
    required = REQUIRED_COLUMNS_BY_STEP[step]
    return [c for c in required if c not in ID_TIME_COLS and c not in DEMOGRAPHIC_COLS]


def _write_feature_dictionary(out_dir: Path) -> None:
    rows = []

    def add_rows(cols: List[str], description: str, source: str, unit: str) -> None:
        for col in cols:
            rows.append(
                {
                    "feature": col,
                    "description": description,
                    "source": source,
                    "unit": unit,
                    "imputation": "ffill_by_stay_then_default",
                }
            )

    add_rows(VITAL_COLS, "vital sign", "vitalsign or chartevents", "varies")
    add_rows(LAB_COLS, "laboratory value", "labevents", "varies")
    add_rows(DEMOGRAPHIC_COLS, "demographics and baseline", "patients/derived", "varies")
    add_rows(THERAPY_COLS, "therapy/support", "derived/inputevents", "varies")
    add_rows(TRANSFUSION_COLS, "rbc transfusion", "inputevents", "ml")
    add_rows(SCORE_COLS, "severity score", "derived", "score")
    add_rows(FLUID_COLS, "fluids and urine", "derived/input/output", "ml")
    add_rows(COMORBID_COLS, "charlson comorbidity", "derived charlson", "flag")

    write_feature_dictionary(out_dir / "reports" / "feature_dictionary.csv", rows)

def _compute_sofa_delta(df: pd.DataFrame) -> pd.DataFrame:
    if "sofa" not in df.columns:
        return df
    df = df.sort_values(["stay_id", "tbin"], kind="mergesort")
    baseline = (
        df[df["tbin"] <= 1440]
        .groupby("stay_id", sort=False)["sofa"]
        .first()
        .rename("sofa_baseline")
    )
    df = df.join(baseline, on="stay_id")
    df["sofa_delta"] = df["sofa"] - df["sofa_baseline"]
    df = df.drop(columns=["sofa_baseline"])
    if "sofa_delta_imputed" in df.columns:
        if "sofa_imputed" in df.columns:
            df["sofa_delta_imputed"] = df["sofa_imputed"].fillna(0).astype("int8")
        else:
            df["sofa_delta_imputed"] = df["sofa"].isna().astype("int8")
    return df

def _resolve_height_table(
    conn,
    table_index: Set[Tuple[str, str]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    for candidate in ("height", "first_day_height"):
        if not _table_exists(table_index, "mimiciv_derived", candidate):
            continue
        cols = _fetch_columns(conn, "mimiciv_derived", candidate)
        height_col = "height" if "height" in cols else ("height_cm" if "height_cm" in cols else None)
        if height_col is None:
            continue
        time_col = "charttime" if "charttime" in cols else None
        return candidate, height_col, time_col
    return None, None, None

def _resolve_weight_table(
    conn,
    table_index: Set[Tuple[str, str]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    for candidate in ("weight_duration", "weight_durations", "first_day_weight"):
        if not _table_exists(table_index, "mimiciv_derived", candidate):
            continue
        cols = _fetch_columns(conn, "mimiciv_derived", candidate)
        weight_col = "weight" if "weight" in cols else ("weight_kg" if "weight_kg" in cols else None)
        if weight_col is None:
            continue
        time_col = None
        for c in ("starttime", "charttime", "weighttime"):
            if c in cols:
                time_col = c
                break
        return candidate, weight_col, time_col
    return None, None, None

def _resolve_lab_itemids_pg(conn, lab_items: Dict[str, LabItemConfig], table_index: Set[Tuple[str, str]]) -> Dict[str, List[int]]:
    df_lab: Optional[pd.DataFrame] = None
    if _table_exists(table_index, "mimiciv_hosp", "d_labitems"):
        df_lab = _fetch_dataframe(conn, "SELECT itemid, label, fluid FROM mimiciv_hosp.d_labitems")
        df_lab["label"] = df_lab["label"].astype(str)
    mapping: Dict[str, List[int]] = {}
    for lab_name, spec in lab_items.items():
        itemids = list(spec.itemids)
        if df_lab is not None and not itemids:
            patterns = spec.label_regex
            if patterns:
                mask = pd.Series(False, index=df_lab.index)
                for pat in patterns:
                    mask = mask | df_lab["label"].str.contains(pat, case=False, regex=True, na=False)
                matched = df_lab.loc[mask, "itemid"].dropna().unique().tolist()
                itemids = [int(x) for x in matched]
        mapping[lab_name] = sorted(set(itemids))
    return mapping

def _resolve_vital_itemids_pg(conn, table_index: Set[Tuple[str, str]]) -> Dict[str, List[int]]:
    if not _table_exists(table_index, "mimiciv_icu", "d_items"):
        return {k: [] for k in VITAL_PATTERNS}
    df_items = _fetch_dataframe(conn, "SELECT itemid, label FROM mimiciv_icu.d_items")
    df_items["label"] = df_items["label"].astype(str)
    mapping: Dict[str, List[int]] = {}
    for vital, patterns in VITAL_PATTERNS.items():
        mask = pd.Series(False, index=df_items.index)
        for pat in patterns:
            mask = mask | df_items["label"].str.contains(pat, case=False, regex=True, na=False)
        itemids = df_items.loc[mask, "itemid"].dropna().unique().tolist()
        mapping[vital] = [int(x) for x in itemids]
    return mapping

def _resolve_rbc_itemids_pg(conn, table_index: Set[Tuple[str, str]]) -> List[int]:
    if not _table_exists(table_index, "mimiciv_icu", "d_items"):
        return []
    with conn.cursor() as cursor:
        clauses = " OR ".join(["label ILIKE %s" for _ in RBC_LABEL_PATTERNS])
        params = [f"%{pat}%" for pat in RBC_LABEL_PATTERNS]
        cursor.execute(f"SELECT itemid FROM mimiciv_icu.d_items WHERE {clauses}", params)
        rows = cursor.fetchall()
    return [int(r[0]) for r in rows]

def _create_temp_cohort(conn, table_index: Set[Tuple[str, str]], max_stays: int) -> None:
    if not _table_exists(table_index, "mimiciv_icu", "icustays"):
        raise FileNotFoundError("Missing mimiciv_icu.icustays in Postgres")

    has_patients = _table_exists(table_index, "mimiciv_hosp", "patients")
    has_admissions = _table_exists(table_index, "mimiciv_hosp", "admissions")
    has_age = _table_exists(table_index, "mimiciv_derived", "age")

    age_has_stay_id = False
    if has_age:
        age_cols = _fetch_columns(conn, "mimiciv_derived", "age")
        age_has_stay_id = "stay_id" in age_cols

    age_join = ""
    if has_age and age_has_stay_id:
        age_join = "LEFT JOIN mimiciv_derived.age age ON icu.stay_id = age.stay_id"
    elif has_age:
        age_join = "LEFT JOIN mimiciv_derived.age age ON icu.subject_id = age.subject_id AND icu.hadm_id = age.hadm_id"

    if has_age:
        age_expr = "age.age"
    else:
        if has_patients and has_admissions:
            age_expr = "patients.anchor_age + (EXTRACT(year FROM admissions.admittime) - patients.anchor_year)"
        else:
            age_expr = "NULL::DOUBLE PRECISION"

    gender_join = "LEFT JOIN mimiciv_hosp.patients patients ON icu.subject_id = patients.subject_id" if has_patients else ""
    admit_join = "LEFT JOIN mimiciv_hosp.admissions admissions ON icu.hadm_id = admissions.hadm_id" if has_admissions else ""
    gender_expr = "patients.gender" if has_patients else "NULL::VARCHAR"

    limit_clause = f"LIMIT {max_stays}" if max_stays and max_stays > 0 else ""

    query = f"""
        DROP TABLE IF EXISTS cohort;
        CREATE TEMP TABLE cohort AS
        WITH icu AS (
            SELECT subject_id, stay_id, hadm_id, intime, outtime
            FROM mimiciv_icu.icustays
            WHERE intime IS NOT NULL AND outtime IS NOT NULL
        ),
        enriched AS (
            SELECT icu.subject_id,
                   icu.stay_id,
                   icu.hadm_id,
                   icu.intime,
                   icu.outtime,
                   {age_expr} AS age,
                   {gender_expr} AS gender
            FROM icu
            {gender_join}
            {admit_join}
            {age_join}
        ),
        filtered AS (
            SELECT *,
                   EXTRACT(EPOCH FROM (outtime - intime)) AS los_seconds,
                   ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
            FROM enriched
        )
        SELECT subject_id,
               stay_id,
               hadm_id,
               intime,
               outtime,
               age,
               gender,
               CEIL(los_seconds / 300.0)::INTEGER AS n_bins,
               (los_seconds / 3600.0)::DOUBLE PRECISION AS icu_los_hours
        FROM filtered
        WHERE age >= 18
          AND los_seconds >= 86400
          AND rn = 1
        {limit_clause};
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cohort_stay ON cohort (stay_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cohort_subj ON cohort (subject_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cohort_hadm ON cohort (hadm_id)")

def _create_temp_grid_pandas(conn) -> pd.DataFrame:
    """
    Cria grid de tempo em pandas - VERSÃO SUPER OTIMIZADA.
    Usa vetorização pura, sem loops Python.
    """
    import time
    t_start = time.time()
    
    # Fetch cohort data
    print("[GRID] Buscando cohort do Postgres...")
    query = """
        SELECT subject_id,
               stay_id,
               hadm_id,
               intime,
               outtime,
               n_bins
        FROM cohort
    """
    df_cohort = _fetch_dataframe(conn, query)
    if df_cohort.empty:
        print("[GRID] Cohort vazio!")
        return df_cohort
    
    n_stays = len(df_cohort)
    total_bins = df_cohort["n_bins"].sum()
    print(f"[GRID] {n_stays} stays com {total_bins} bins totais")
    
    df_cohort["intime"] = pd.to_datetime(df_cohort["intime"])
    df_cohort["outtime"] = pd.to_datetime(df_cohort["outtime"])
    
    # Expandir cada linha pelo seu n_bins
    print("[GRID] Expandindo stays...")
    t1 = time.time()
    
    # Usar repeat para duplicar linhas eficientemente
    df_expanded = df_cohort.loc[df_cohort.index.repeat(df_cohort["n_bins"])].reset_index(drop=True)
    
    print(f"[GRID] Expanded em {time.time() - t1:.2f}s -> {len(df_expanded)} linhas")
    
    # Criar tbin (0, 5, 10, ...) para cada stay
    print("[GRID] Gerando tbins...")
    t1 = time.time()
    
    tbins_list = []
    charttimes_list = []
    
    for n_bins in df_cohort["n_bins"]:
        n_bins = int(n_bins)
        tbins_list.extend(np.arange(0, n_bins * 5, 5, dtype=np.int64))
        charttimes_list.append(np.arange(n_bins, dtype=np.int64) * 5)
    
    df_expanded["tbin"] = np.concatenate([np.arange(0, int(n) * 5, 5) for n in df_cohort["n_bins"]])
    
    print(f"[GRID] Tbins em {time.time() - t1:.2f}s")
    
    # Charttimes = intime + tbin em minutos
    print("[GRID] Gerando charttimes...")
    t1 = time.time()
    
    charttimes = df_expanded["intime"].values + pd.to_timedelta(df_expanded["tbin"].values, unit="m")
    df_expanded["charttime_bin"] = charttimes
    
    print(f"[GRID] Charttimes em {time.time() - t1:.2f}s")
    
    # Stay bucket
    df_expanded["stay_bucket"] = df_expanded["stay_id"] % STAY_BUCKET_MOD
    
    # Selecionar apenas colunas necessárias
    df_grid = df_expanded[["subject_id", "stay_id", "hadm_id", "intime", "outtime", "tbin", "charttime_bin", "stay_bucket"]].copy()
    
    print(f"[GRID] ✅ Grid criado em {time.time() - t_start:.2f}s ({len(df_grid)} linhas)")
    return df_grid

def _create_temp_grid(conn) -> pd.DataFrame:
    """
    Cria grid de tempo em PANDAS e RETORNA como DataFrame.
    Sem criar tabela SQL - processamento entirely em memória (como gitbatch).
    """
    import time
    t_start = time.time()
    
    print("[GRID] Iniciando criação do grid...")
    
    df_grid = _create_temp_grid_pandas(conn)
    
    if df_grid.empty:
        print("[GRID] Grid vazio!")
        return df_grid
    
    print(f"[GRID] ✅✅ GRID CRIADO em {time.time() - t_start:.2f}s ({len(df_grid):,} linhas)")
    print(f"[GRID] Mantido em PANDAS - sem SQL intermediário")
    return df_grid



# =============================================================================
# PIPELINE STEPS (pandas)
# =============================================================================


# ============================================================================
# STEP 01: LABS + VITALS
# ============================================================================

def _fetch_labs_pandas(
    conn,
    df_grid: pd.DataFrame,
    lab_itemids: Dict[str, List[int]],
) -> pd.DataFrame:
    """
    Fetch dados de labs do Postgres e fazer merge com grid em PANDAS.
    Substitui _labs_cte_sql + SQL JOIN.
    """
    all_itemids = sorted({item for items in lab_itemids.values() for item in items})
    if not all_itemids:
        # Retornar DataFrame com colunas de labs em NULL
        result = df_grid[["stay_id", "tbin"]].drop_duplicates()
        for col in LAB_COLS:
            result[col] = None
        return result
    
    item_list = ",".join(str(i) for i in all_itemids)
    
    # Fetch dos dados brutos
    query = f"""
        SELECT l.subject_id, l.hadm_id, l.itemid, l.charttime, l.valuenum
        FROM mimiciv_hosp.labevents l
        INNER JOIN (
            SELECT DISTINCT subject_id, hadm_id
            FROM cohort
        ) c ON l.subject_id = c.subject_id AND l.hadm_id = c.hadm_id
        WHERE l.itemid IN ({item_list})
    """
    
    print("[STEP 01] Fetching labs from database...")
    df_labs_raw = _fetch_dataframe(conn, query)
    if not df_labs_raw.empty:
        print(f"[STEP 01] ✅ Fetched {len(df_labs_raw):,} lab records")
    
    if df_labs_raw.empty:
        result = df_grid[["stay_id", "tbin"]].drop_duplicates()
        for col in LAB_COLS:
            result[col] = None
        return result
    
    # Merge com grid para pegar stay_id e calcular tbin
    print("[STEP 01] Merging labs with grid...")
    df_labs_raw = df_labs_raw.merge(
        df_grid[["subject_id", "hadm_id", "intime", "stay_id"]].drop_duplicates(),
        on=["subject_id", "hadm_id"],
        how="left"
    )
    
    df_labs_raw = df_labs_raw.dropna(subset=["stay_id"])
    print(f"[STEP 01] ✅ After grid merge: {len(df_labs_raw):,} records matched")
    df_labs_raw["charttime"] = pd.to_datetime(df_labs_raw["charttime"])
    df_labs_raw["intime"] = pd.to_datetime(df_labs_raw["intime"])
    
    # Calcular tbin (em minutos, arredondado para múltiplo de 5)
    df_labs_raw["tbin"] = (
        (df_labs_raw["charttime"] - df_labs_raw["intime"]).dt.total_seconds() / 60
    ).astype("int64") // 5 * 5
    
    # Mapear itemid → lab_name
    itemid_to_lab = {}
    for lab_name, items in lab_itemids.items():
        for itemid in items:
            itemid_to_lab[itemid] = lab_name
    
    df_labs_raw["lab_name"] = df_labs_raw["itemid"].map(itemid_to_lab)
    df_labs_raw = df_labs_raw.dropna(subset=["lab_name"])
    
    # Pegar último valor de cada (stay_id, tbin, lab_name)
    df_labs_raw = df_labs_raw.sort_values(["stay_id", "tbin", "lab_name", "charttime"])
    df_labs_ranked = df_labs_raw.drop_duplicates(
        subset=["stay_id", "tbin", "lab_name"],
        keep="last"
    )[["stay_id", "tbin", "lab_name", "valuenum"]]
    
    # Pivot: cada lab fica em coluna
    df_labs_pivot = df_labs_ranked.pivot_table(
        index=["stay_id", "tbin"],
        columns="lab_name",
        values="valuenum",
        aggfunc="last"
    ).reset_index()
    
    # Garantir que todas as colunas de lab existem (preenchidas com NULL se não têm dados)
    for col in LAB_COLS:
        if col not in df_labs_pivot.columns:
            df_labs_pivot[col] = None
    
    return df_labs_pivot[["stay_id", "tbin"] + LAB_COLS]


def _fetch_vitals_pandas(
    conn,
    df_grid: pd.DataFrame,
    use_derived: bool,
    vital_itemids: Dict[str, List[int]],
) -> pd.DataFrame:
    """
    Fetch dados de vitals do Postgres e fazer merge com grid em PANDAS.
    Substitui _vitals_cte_sql + SQL JOIN.
    """
    if use_derived:
        # Usar tabela mimiciv_derived.vitalsign
        query = """
            SELECT v.stay_id, v.charttime,
                   v.heart_rate, COALESCE(v.sbp, v.sbp_ni) AS sbp,
                   COALESCE(v.dbp, v.dbp_ni) AS dbp,
                   COALESCE(v.mbp, v.mbp_ni) AS mbp,
                   v.resp_rate, v.temperature, v.spo2, v.glucose
            FROM mimiciv_derived.vitalsign v
            INNER JOIN (
                SELECT DISTINCT stay_id
                FROM cohort
            ) c USING (stay_id)
        """
        df_vitals_raw = _fetch_dataframe(conn, query)
        
        if df_vitals_raw.empty:
            result = df_grid[["stay_id", "tbin"]].drop_duplicates()
            for col in VITAL_COLS:
                result[col] = None
            return result
        
        df_vitals_raw["charttime"] = pd.to_datetime(df_vitals_raw["charttime"])
        
        # Merge com grid
        df_vitals_raw = df_vitals_raw.merge(
            df_grid[["stay_id", "intime"]].drop_duplicates(),
            on="stay_id",
            how="left"
        )
        
        # Calcular tbin
        df_vitals_raw["tbin"] = (
            (df_vitals_raw["charttime"] - df_vitals_raw["intime"]).dt.total_seconds() / 60
        ).astype("int64") // 5 * 5
        
        # Pegar último valor de cada (stay_id, tbin)
        df_vitals_raw = df_vitals_raw.sort_values(["stay_id", "tbin", "charttime"])
        df_vitals = df_vitals_raw.drop_duplicates(
            subset=["stay_id", "tbin"],
            keep="last"
        )[["stay_id", "tbin", "heart_rate", "sbp", "dbp", "mbp", "resp_rate", "temperature", "spo2", "glucose"]]
        
        # Renomear glucose_vital
        df_vitals = df_vitals.rename(columns={"glucose": "glucose_vital"})
        
    else:
        # Usar chartevents
        all_itemids = sorted({item for items in vital_itemids.values() for item in items})
        if not all_itemids:
            result = df_grid[["stay_id", "tbin"]].drop_duplicates()
            for col in VITAL_COLS:
                result[col] = None
            return result
        
        item_list = ",".join(str(i) for i in all_itemids)
        query = f"""
            SELECT ce.stay_id, ce.itemid, ce.charttime, ce.valuenum
            FROM mimiciv_icu.chartevents ce
            INNER JOIN (
                SELECT DISTINCT stay_id
                FROM cohort
            ) c USING (stay_id)
            WHERE ce.itemid IN ({item_list})
        """
        
        df_vitals_raw = _fetch_dataframe(conn, query)
        
        if df_vitals_raw.empty:
            result = df_grid[["stay_id", "tbin"]].drop_duplicates()
            for col in VITAL_COLS:
                result[col] = None
            return result
        
        # Merge com grid
        df_vitals_raw = df_vitals_raw.merge(
            df_grid[["stay_id", "intime"]].drop_duplicates(),
            on="stay_id",
            how="left"
        )
        
        df_vitals_raw["charttime"] = pd.to_datetime(df_vitals_raw["charttime"])
        df_vitals_raw["intime"] = pd.to_datetime(df_vitals_raw["intime"])
        
        # Calcular tbin
        df_vitals_raw["tbin"] = (
            (df_vitals_raw["charttime"] - df_vitals_raw["intime"]).dt.total_seconds() / 60
        ).astype("int64") // 5 * 5
        
        # Mapear itemid → vital_name
        itemid_to_vital = {}
        for vital_name, items in vital_itemids.items():
            for itemid in items:
                itemid_to_vital[itemid] = vital_name
        
        df_vitals_raw["vital_name"] = df_vitals_raw["itemid"].map(itemid_to_vital)
        df_vitals_raw = df_vitals_raw.dropna(subset=["vital_name"])
        
        # Pegar último valor de cada (stay_id, tbin, vital_name)
        df_vitals_raw = df_vitals_raw.sort_values(["stay_id", "tbin", "vital_name", "charttime"])
        df_vitals_ranked = df_vitals_raw.drop_duplicates(
            subset=["stay_id", "tbin", "vital_name"],
            keep="last"
        )[["stay_id", "tbin", "vital_name", "valuenum"]]
        
        # Pivot
        df_vitals = df_vitals_ranked.pivot_table(
            index=["stay_id", "tbin"],
            columns="vital_name",
            values="valuenum",
            aggfunc="last"
        ).reset_index()
    
    # Garantir todas as colunas
    for col in VITAL_COLS:
        if col not in df_vitals.columns:
            df_vitals[col] = None
    
    return df_vitals[["stay_id", "tbin"] + VITAL_COLS]


def step_01_labs_vitals_pandas(
    conn,
    df_grid: pd.DataFrame,
    lab_itemids: Dict[str, List[int]],
    use_derived_vitals: bool,
    vital_itemids: Dict[str, List[int]],
) -> pd.DataFrame:
    """
    STEP 01: Fetch labs + vitals e fazer merge com grid.
    Tudo em PANDAS, sem SQL intermediário.
    """
    print("[STEP 01] Iniciando fetch de labs + vitals em pandas...")
    t_start = time.time()
    
    # Manter apenas colunas ID+tempo do grid
    df_result = df_grid.copy()
    
    # Fetch labs
    print("[STEP 01] Fetching labs...")
    df_labs = _fetch_labs_pandas(conn, df_grid, lab_itemids)
    df_result = df_result.merge(
        df_labs,
        on=["stay_id", "tbin"],
        how="left"
    )
    
    # Fetch vitals
    print("[STEP 01] Fetching vitals...")
    df_vitals = _fetch_vitals_pandas(conn, df_grid, use_derived_vitals, vital_itemids)
    df_result = df_result.merge(
        df_vitals,
        on=["stay_id", "tbin"],
        how="left"
    )
    
    print(f"[STEP 01] ✅ Concluído em {time.time() - t_start:.2f}s ({len(df_result):,} linhas)")
    return df_result


# ============================================================================
# STEP 02: DEMOGRAPHICS
# ============================================================================

def _fetch_demographics_pandas(
    conn,
    df_grid: pd.DataFrame,
    height_info: Optional[Tuple[str, str, str]],  # (table, col, time_col)
    weight_info: Optional[Tuple[str, str, str]],
) -> pd.DataFrame:
    """
    Fetch demographics (age, gender, height, weight, BMI) em PANDAS.
    """
    # Base: age + gender da cohort
    query = """
        SELECT stay_id, age, gender
        FROM cohort
    """
    df_demo = _fetch_dataframe(conn, query)
    df_demo["age"] = pd.to_numeric(df_demo["age"], errors="coerce").round().astype("Int64")

    # Segurança: garante 1 linha por stay_id
    if not df_demo.empty:
        df_demo = df_demo.drop_duplicates(subset=["stay_id"], keep="first")

    # ----------------------------------------------------------------------
    # Height
    # ----------------------------------------------------------------------
    if height_info:
        height_table, height_col, height_time_col = height_info

        if height_time_col:
            # ✅ 1 linha por stay_id já no SQL (mais próximo do intime)
            query = f"""
                SELECT DISTINCT ON (c.stay_id)
                       c.stay_id,
                       h.{height_col} AS height_cm
                FROM mimiciv_derived.{height_table} h
                INNER JOIN cohort c USING (stay_id)
                ORDER BY c.stay_id,
                         ABS(EXTRACT(EPOCH FROM (h.{height_time_col} - c.intime))) ASC
            """
            df_height = _fetch_dataframe(conn, query)

            if df_height.empty:
                df_demo["height_cm"] = None
            else:
                # df_height já deve vir 1 por stay_id, mas garantimos
                df_height = df_height.drop_duplicates(subset=["stay_id"], keep="first")
                df_demo = df_demo.merge(df_height, on="stay_id", how="left")

        else:
            # Sem time_col: pega uma linha por stay_id (ordem não definida sem critério de tempo)
            query = f"""
                SELECT DISTINCT ON (stay_id)
                       stay_id,
                       {height_col} AS height_cm
                FROM mimiciv_derived.{height_table}
                ORDER BY stay_id
            """
            df_height = _fetch_dataframe(conn, query)

            if df_height.empty:
                df_demo["height_cm"] = None
            else:
                df_height = df_height.drop_duplicates(subset=["stay_id"], keep="first")
                df_demo = df_demo.merge(df_height, on="stay_id", how="left")
    else:
        df_demo["height_cm"] = None

    # ----------------------------------------------------------------------
    # Weight
    # ----------------------------------------------------------------------
    if weight_info:
        weight_table, weight_col, weight_time_col = weight_info

        if weight_time_col:
            # ✅ 1 linha por stay_id já no SQL (mais próximo do intime)
            query = f"""
                SELECT DISTINCT ON (c.stay_id)
                       c.stay_id,
                       w.{weight_col} AS weight_kg
                FROM mimiciv_derived.{weight_table} w
                INNER JOIN cohort c USING (stay_id)
                ORDER BY c.stay_id,
                         ABS(EXTRACT(EPOCH FROM (w.{weight_time_col} - c.intime))) ASC
            """
            df_weight = _fetch_dataframe(conn, query)

            if df_weight.empty:
                df_demo["weight_kg"] = None
            else:
                df_weight = df_weight.drop_duplicates(subset=["stay_id"], keep="first")
                df_demo = df_demo.merge(df_weight, on="stay_id", how="left")

        else:
            # Sem time_col: pega uma linha por stay_id
            query = f"""
                SELECT DISTINCT ON (stay_id)
                       stay_id,
                       {weight_col} AS weight_kg
                FROM mimiciv_derived.{weight_table}
                ORDER BY stay_id
            """
            df_weight = _fetch_dataframe(conn, query)

            if df_weight.empty:
                df_demo["weight_kg"] = None
            else:
                df_weight = df_weight.drop_duplicates(subset=["stay_id"], keep="first")
                df_demo = df_demo.merge(df_weight, on="stay_id", how="left")
    else:
        df_demo["weight_kg"] = None

    # BMI (vetorizado + conversão segura)
    df_demo["weight_kg"] = pd.to_numeric(df_demo["weight_kg"], errors="coerce")
    df_demo["height_cm"] = pd.to_numeric(df_demo["height_cm"], errors="coerce")

    h_m = df_demo["height_cm"] / 100.0
    df_demo["bmi"] = df_demo["weight_kg"] / (h_m ** 2)

    # inválidos (altura <= 0 ou missing)
    df_demo.loc[(h_m <= 0) | df_demo["weight_kg"].isna() | h_m.isna(), "bmi"] = pd.NA

    return df_demo


def step_02_demographics_pandas(
    conn,
    df_grid: pd.DataFrame,
    df_step01: pd.DataFrame,
    height_info: Optional[Tuple[str, str, str]],
    weight_info: Optional[Tuple[str, str, str]],
) -> pd.DataFrame:
    """
    STEP 02: Adicionar demographics ao resultado de step 01.
    """
    print("[STEP 02] Fetching demographics...")
    t_start = time.time()

    df_demo = _fetch_demographics_pandas(conn, df_grid, height_info, weight_info)

    if df_demo is None:
        raise RuntimeError("BUG: _fetch_demographics_pandas retornou None (deveria retornar DataFrame).")

    # Segurança: garante 1 linha por stay_id (evita duplicar linhas do step01 no merge)
    if not df_demo.empty:
        df_demo = df_demo.drop_duplicates(subset=["stay_id"], keep="first")

    # Fazer merge: cada linha de step01 pega os dados de demo de seu stay_id
    df_result = df_step01.merge(df_demo, on="stay_id", how="left")

    print(f"[STEP 02] ✅ Concluído em {time.time() - t_start:.2f}s")
    return df_result


# ============================================================================
# STEP 03: THERAPIES (Vasopressor + Ventilation + RRT) — CORRIGIDO (TUDO JUNTO)
# Mantém: norepi, epi, dobutamine, vasopressin, phenylephrine
# Remove: dopamine, milrinone
# Saídas adicionadas por bucket:
#   - norepinephrine_rate_mcgkgmin
#   - epinephrine_rate_mcgkgmin
#   - dobutamine_rate_mcgkgmin
#   - vasopressin_rate_unitsmin_or_equiv
#   - phenylephrine_rate_mcgkgmin
#   - any_vasopressor (0/1)
#   - nee_mcgkgmin (continuous)
#   - ventilation_on, imv_on, tracheostomy_flag
#   - rrt_on, rrt_type
# ============================================================================

import time
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Helpers (DEVEM existir antes dos fetchers)
# ---------------------------------------------------------------------
def _safe_tbin_from_minutes(minutes: pd.Series) -> pd.Series:
    """
    Converte minutos (float) -> tbin (múltiplos de 5) com segurança para NA/inf.
    Retorna dtype Int64 (nullable).
    """
    m = pd.to_numeric(minutes, errors="coerce")
    m = m.replace([np.inf, -np.inf], np.nan)
    tb = (np.floor(m / 5.0) * 5.0)
    # mantém o mesmo index para evitar warnings
    return pd.Series(tb, index=minutes.index).round().astype("Int64")


def _ensure_cols(df: pd.DataFrame, cols: list, fill_value=None) -> pd.DataFrame:
    """Garante que todas as colunas existam."""
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value
    return df


# ---------------------------------------------------------------------
# FETCHERS (SEM df_grid! intime/outtime vem do cohort via SQL)
# ---------------------------------------------------------------------
def fetch_vasopressors_all(conn) -> pd.DataFrame:
    """
    Busca vasopressores 1x para a coorte.
    Retorna DF: stay_id, tbin, +colunas de rates.
    """
    vaso_tables = {
        "norepinephrine": "norepinephrine_rate_mcgkgmin",
        "epinephrine": "epinephrine_rate_mcgkgmin",
        "dobutamine": "dobutamine_rate_mcgkgmin",
        "vasopressin": "vasopressin_rate_unitsmin_or_equiv",
        "phenylephrine": "phenylephrine_rate_mcgkgmin",  # você decidiu manter
    }

    result = None

    for table, col_name in vaso_tables.items():
        query = f"""
            SELECT
                v.stay_id,
                v.starttime,
                v.endtime,
                COALESCE(v.vaso_rate, v.vaso_amount) AS rate,
                c.intime,
                c.outtime
            FROM mimiciv_derived.{table} v
            INNER JOIN cohort c USING (stay_id)
            WHERE v.starttime <= c.outtime
              AND v.endtime   >= c.intime
        """
        try:
            df_raw = _fetch_dataframe(conn, query)

            if df_raw.empty:
                df_one = pd.DataFrame(columns=["stay_id", "tbin", col_name])
            else:
                df_raw["starttime"] = pd.to_datetime(df_raw["starttime"], errors="coerce")
                df_raw["endtime"] = pd.to_datetime(df_raw["endtime"], errors="coerce")
                df_raw["intime"] = pd.to_datetime(df_raw["intime"], errors="coerce")
                df_raw["outtime"] = pd.to_datetime(df_raw["outtime"], errors="coerce")
                df_raw["rate"] = pd.to_numeric(df_raw["rate"], errors="coerce")

                # minutos relativos ao intime
                start_min = (df_raw["starttime"] - df_raw["intime"]).dt.total_seconds() / 60.0
                end_min = (df_raw["endtime"] - df_raw["intime"]).dt.total_seconds() / 60.0

                tbin_start = _safe_tbin_from_minutes(start_min)
                # degrau: zera no bin seguinte ao end
                tbin_end = (_safe_tbin_from_minutes(end_min) + 5).astype("Int64")

                df_events = pd.concat(
                    [
                        pd.DataFrame({"stay_id": df_raw["stay_id"], "tbin": tbin_start, "rate": df_raw["rate"]}),
                        pd.DataFrame({"stay_id": df_raw["stay_id"], "tbin": tbin_end, "rate": 0.0}),
                    ],
                    ignore_index=True,
                )

                # remove tbins inválidos
                df_events = df_events.dropna(subset=["stay_id", "tbin"])
                df_events["tbin"] = df_events["tbin"].astype("int64")
                df_events = df_events[df_events["tbin"] >= 0]

                # último evento por (stay_id,tbin)
                df_events = df_events.sort_values(["stay_id", "tbin"])
                df_last = df_events.drop_duplicates(["stay_id", "tbin"], keep="last")

                df_one = df_last[["stay_id", "tbin", "rate"]].rename(columns={"rate": col_name})

            if result is None:
                result = df_one
            else:
                result = result.merge(df_one, on=["stay_id", "tbin"], how="outer")

        except Exception as e:
            print(f"[STEP 03] Aviso: erro ao buscar {table}: {e}")
            if result is None:
                result = pd.DataFrame(columns=["stay_id", "tbin", col_name])
            else:
                if col_name not in result.columns:
                    result[col_name] = np.nan

    if result is None:
        result = pd.DataFrame(columns=["stay_id", "tbin"] + list(vaso_tables.values()))

    result = _ensure_cols(result, list(vaso_tables.values()), fill_value=np.nan)
    return result


def fetch_ventilation_all(conn) -> pd.DataFrame:
    query = """
        SELECT
            v.stay_id,
            v.starttime,
            v.endtime,
            LOWER(v.ventilation_status) AS status,
            c.intime,
            c.outtime
        FROM mimiciv_derived.ventilation v
        INNER JOIN cohort c USING (stay_id)
        WHERE v.starttime <= c.outtime
          AND v.endtime   >= c.intime
    """
    try:
        df_raw = _fetch_dataframe(conn, query)
        if df_raw.empty:
            return pd.DataFrame(columns=["stay_id", "tbin", "ventilation_on", "imv_on", "tracheostomy_flag"])

        df_raw["starttime"] = pd.to_datetime(df_raw["starttime"], errors="coerce")
        df_raw["endtime"] = pd.to_datetime(df_raw["endtime"], errors="coerce")
        df_raw["intime"] = pd.to_datetime(df_raw["intime"], errors="coerce")
        df_raw["outtime"] = pd.to_datetime(df_raw["outtime"], errors="coerce")

        start_min = (df_raw["starttime"] - df_raw["intime"]).dt.total_seconds() / 60.0
        end_min = (df_raw["endtime"] - df_raw["intime"]).dt.total_seconds() / 60.0

        tbin_start = _safe_tbin_from_minutes(start_min)
        tbin_end = (_safe_tbin_from_minutes(end_min) + 5).astype("Int64")

        status = df_raw["status"].astype("string")
        vent_on = (~status.str.contains("none|off", na=False)).astype("int64")
        imv_on = (status.str.contains("invasive|ett|trach", na=False)).astype("int64")
        trach = (status.str.contains("trach", na=False)).astype("int64")

        df_events = pd.concat(
            [
                pd.DataFrame(
                    {
                        "stay_id": df_raw["stay_id"],
                        "tbin": tbin_start,
                        "ventilation_on": vent_on,
                        "imv_on": imv_on,
                        "tracheostomy_flag": trach,
                    }
                ),
                pd.DataFrame(
                    {
                        "stay_id": df_raw["stay_id"],
                        "tbin": tbin_end,
                        "ventilation_on": 0,
                        "imv_on": 0,
                        "tracheostomy_flag": 0,
                    }
                ),
            ],
            ignore_index=True,
        )

        df_events = df_events.dropna(subset=["stay_id", "tbin"])
        df_events["tbin"] = df_events["tbin"].astype("int64")
        df_events = df_events[df_events["tbin"] >= 0]

        df_events = df_events.sort_values(["stay_id", "tbin"])
        df_last = df_events.drop_duplicates(["stay_id", "tbin"], keep="last")
        return df_last[["stay_id", "tbin", "ventilation_on", "imv_on", "tracheostomy_flag"]]

    except Exception as e:
        print(f"[STEP 03] Aviso: erro ao buscar ventilation: {e}")
        return pd.DataFrame(columns=["stay_id", "tbin", "ventilation_on", "imv_on", "tracheostomy_flag"])


def fetch_rrt_all(conn) -> pd.DataFrame:
    query = """
        SELECT
            r.stay_id,
            r.charttime,
            COALESCE(r.dialysis_active, r.dialysis_present, 0) AS rrt_on,
            COALESCE(r.dialysis_type, 'unknown') AS rrt_type,
            c.intime,
            c.outtime
        FROM mimiciv_derived.rrt r
        INNER JOIN cohort c USING (stay_id)
        WHERE r.charttime BETWEEN c.intime AND c.outtime
    """
    try:
        df_raw = _fetch_dataframe(conn, query)
        if df_raw.empty:
            return pd.DataFrame(columns=["stay_id", "tbin", "rrt_on", "rrt_type"])

        df_raw["charttime"] = pd.to_datetime(df_raw["charttime"], errors="coerce")
        df_raw["intime"] = pd.to_datetime(df_raw["intime"], errors="coerce")
        df_raw["outtime"] = pd.to_datetime(df_raw["outtime"], errors="coerce")

        minutes = (df_raw["charttime"] - df_raw["intime"]).dt.total_seconds() / 60.0
        tbin = _safe_tbin_from_minutes(minutes)

        df_raw["tbin"] = tbin
        df_raw = df_raw.dropna(subset=["stay_id", "tbin"])
        df_raw["tbin"] = df_raw["tbin"].astype("int64")
        df_raw = df_raw[df_raw["tbin"] >= 0]

        df_raw["rrt_on"] = pd.to_numeric(df_raw["rrt_on"], errors="coerce").fillna(0).astype("int64")
        df_raw["rrt_type"] = df_raw["rrt_type"].astype("string")

        df_raw = df_raw.sort_values(["stay_id", "tbin", "charttime"])
        df_last = df_raw.drop_duplicates(["stay_id", "tbin"], keep="last")
        return df_last[["stay_id", "tbin", "rrt_on", "rrt_type"]]

    except Exception as e:
        print(f"[STEP 03] Aviso: erro ao buscar RRT: {e}")
        return pd.DataFrame(columns=["stay_id", "tbin", "rrt_on", "rrt_type"])


# ---------------------------------------------------------------------
# STEP 03 usando CACHE (para rodar bucket-wise)
# ---------------------------------------------------------------------
def prepare_step03_cache(conn) -> dict:
    """
    Chame 1 vez antes do loop de buckets.
    """
    print("[STEP 03] Preparando cache (vaso/vent/rrt) 1x...")
    t0 = time.time()
    cache = {
        "vaso": fetch_vasopressors_all(conn),
        "vent": fetch_ventilation_all(conn),
        "rrt": fetch_rrt_all(conn),
    }
    print(
        f"[STEP 03] Cache pronto em {time.time() - t0:.2f}s "
        f"(vaso={len(cache['vaso']):,} vent={len(cache['vent']):,} rrt={len(cache['rrt']):,})"
    )
    return cache


def step_03_therapies_bucket(df_prev_bucket: pd.DataFrame, cache: dict) -> pd.DataFrame:
    """
    Aplica Step03 em UM bucket (df_prev_bucket já tem stay_id,tbin).
    """
    t0 = time.time()
    df = df_prev_bucket.copy()

    stay_ids = df["stay_id"].dropna().unique()

    # Mantém as 5 escolhidas (inclui phenylephrine)
    vaso_cols = [
        "norepinephrine_rate_mcgkgmin",
        "epinephrine_rate_mcgkgmin",
        "dobutamine_rate_mcgkgmin",
        "vasopressin_rate_unitsmin_or_equiv",
        "phenylephrine_rate_mcgkgmin",
    ]

    df_vaso = cache["vaso"]
    df_vent = cache["vent"]
    df_rrt = cache["rrt"]

    # merge apenas o que existe no cache (robusto)
    vaso_keep = ["stay_id", "tbin"] + [c for c in vaso_cols if c in df_vaso.columns]
    df = df.merge(
        df_vaso[df_vaso["stay_id"].isin(stay_ids)][vaso_keep],
        on=["stay_id", "tbin"],
        how="left",
    )

    # garante colunas (para não quebrar)
    for c in vaso_cols:
        if c not in df.columns:
            df[c] = np.nan

    # any_vasopressor (inclui vasopressin + phenyl)
    df["any_vasopressor"] = (
        (df["norepinephrine_rate_mcgkgmin"].fillna(0) > 0)
        | (df["epinephrine_rate_mcgkgmin"].fillna(0) > 0)
        | (df["dobutamine_rate_mcgkgmin"].fillna(0) > 0)
        | (df["vasopressin_rate_unitsmin_or_equiv"].fillna(0) > 0)
        | (df["phenylephrine_rate_mcgkgmin"].fillna(0) > 0)
    ).astype("int64")

    # NEE (catecolaminas) — NÃO soma vasopressin (unidades/min)
    df["nee_mcgkgmin"] = (
        df["norepinephrine_rate_mcgkgmin"].fillna(0)
        + df["epinephrine_rate_mcgkgmin"].fillna(0)
        + df["dobutamine_rate_mcgkgmin"].fillna(0) / 100.0
        + df["phenylephrine_rate_mcgkgmin"].fillna(0) / 10.0
    )

    # ventilação
    df = df.merge(
        df_vent[df_vent["stay_id"].isin(stay_ids)],
        on=["stay_id", "tbin"],
        how="left",
    )

    # rrt
    df = df.merge(
        df_rrt[df_rrt["stay_id"].isin(stay_ids)],
        on=["stay_id", "tbin"],
        how="left",
    )

    print(f"[STEP 03] bucket done: rows={len(df):,} in {time.time() - t0:.2f}s")
    return df



# ============================================================================
# STEP 04: TRANSFUSION (RBC) — COMPATÍVEL COM O PIPELINE (4 args)
# Saídas adicionadas ao df temporal (stay_id,tbin):
# - rbc_transfusion_flag (0/1): 1 no bin do STARTTIME do evento
# - rbc_amount_ml_event (float): volume (mL) no bin do evento, 0 nos demais
# - rbc_totalamount_ml_icu (float): cumulativo por stay ao longo do tempo
# ============================================================================

from typing import List
import pandas as pd
import numpy as np
import time

# assume que você já tem em algum lugar do arquivo:
# def _safe_tbin_from_minutes(minutes: pd.Series) -> pd.Series: ...
# def _fetch_dataframe(conn, query: str) -> pd.DataFrame: ...


def _fetch_rbc_events_only(conn, rbc_itemids: List[int]) -> pd.DataFrame:
    """
    Busca apenas os EVENTOS de transfusão de CH (inputevents).
    Retorna DF por (stay_id, tbin) com soma de volumes no mesmo bin.
    """
    if not rbc_itemids:
        return pd.DataFrame(columns=["stay_id", "tbin", "rbc_amount_ml_event", "rbc_transfusion_flag"])

    item_list = ",".join(str(int(x)) for x in rbc_itemids)
    query = f"""
        SELECT
            i.stay_id,
            i.starttime,
            i.amount,
            LOWER(COALESCE(i.amountuom, '')) AS amountuom,
            c.intime,
            c.outtime
        FROM mimiciv_icu.inputevents i
        INNER JOIN cohort c USING (stay_id)
        WHERE i.itemid IN ({item_list})
          AND i.starttime BETWEEN c.intime AND c.outtime
    """

    df = _fetch_dataframe(conn, query)
    if df.empty:
        return pd.DataFrame(columns=["stay_id", "tbin", "rbc_amount_ml_event", "rbc_transfusion_flag"])

    # tipos
    df["starttime"] = pd.to_datetime(df["starttime"], errors="coerce")
    df["intime"] = pd.to_datetime(df["intime"], errors="coerce")
    df["outtime"] = pd.to_datetime(df["outtime"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # filtra unidade mL
    u = df["amountuom"].astype("string").str.strip().str.lower()
    df = df[u.isin(["ml", "milliliter", "milliliters", "millilitre", "millilitres"])].copy()

    # remove inválidos
    df = df.dropna(subset=["stay_id", "starttime", "intime", "amount"])
    df = df[df["amount"] > 0]
    if df.empty:
        return pd.DataFrame(columns=["stay_id", "tbin", "rbc_amount_ml_event", "rbc_transfusion_flag"])

    # tbin do START (5-min)
    minutes = (df["starttime"] - df["intime"]).dt.total_seconds() / 60.0
    df["tbin"] = _safe_tbin_from_minutes(minutes)

    df = df.dropna(subset=["tbin"])
    df["tbin"] = df["tbin"].astype("int64")
    df = df[df["tbin"] >= 0]

    if df.empty:
        return pd.DataFrame(columns=["stay_id", "tbin", "rbc_amount_ml_event", "rbc_transfusion_flag"])

    # agrega por (stay_id,tbin)
    out = (
        df.groupby(["stay_id", "tbin"], as_index=False)["amount"]
          .sum()
          .rename(columns={"amount": "rbc_amount_ml_event"})
    )
    out["rbc_transfusion_flag"] = (out["rbc_amount_ml_event"] > 0).astype("int64")
    return out


def step_04_transfusions_pandas(
    conn,
    df_grid: pd.DataFrame,   # mantido só para compatibilidade (não usamos)
    df_step03: pd.DataFrame,
    rbc_itemids: List[int],
) -> pd.DataFrame:
    """
    STEP 04: adiciona transfusão RBC no dataframe temporal completo (5-min).
    - Faz merge dos eventos no (stay_id,tbin)
    - Preenche com 0 onde não houve evento
    - Calcula cumulativo por stay_id em TODOS os tbins
    """
    print("[STEP 04] Fetching RBC transfusions (events only) ...")
    t0 = time.time()

    df = df_step03.copy()
    if "stay_id" not in df.columns or "tbin" not in df.columns:
        raise ValueError("df_step03 precisa ter colunas 'stay_id' e 'tbin'.")

    events = _fetch_rbc_events_only(conn, rbc_itemids)

    df = df.merge(
        events,
        on=["stay_id", "tbin"],
        how="left",
    )

    # onde não tem evento => ZERO (não NaN)
    df["rbc_amount_ml_event"] = pd.to_numeric(df.get("rbc_amount_ml_event"), errors="coerce").fillna(0.0)
    df["rbc_transfusion_flag"] = pd.to_numeric(df.get("rbc_transfusion_flag"), errors="coerce").fillna(0).astype("int64")

    # cumulativo por stay ao longo do tempo
    df = df.sort_values(["stay_id", "tbin"], kind="mergesort")
    df["rbc_totalamount_ml_icu"] = df.groupby("stay_id", sort=False)["rbc_amount_ml_event"].cumsum()

    print(f"[STEP 04] ✅ Done in {time.time() - t0:.2f}s")
    return df


# ============================================================================
# STEP 05: SEVERITY SCORES (SOFA, SAPSII, APSIII)
# ============================================================================



def _fetch_sofa_pandas(conn, df_grid: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch SOFA score por tbin (bin de 5 min) usando mimiciv_derived.sofa.sofa_24hours.

    Retorna: [stay_id, tbin, sofa]
    """
    query = """
        SELECT s.stay_id, s.starttime, s.sofa_24hours AS sofa
        FROM mimiciv_derived.sofa s
        JOIN cohort c USING (stay_id)
        WHERE s.starttime BETWEEN c.intime AND c.outtime
    """
    try:
        df_sofa_raw = _fetch_dataframe(conn, query)

        if df_sofa_raw.empty:
            out = df_grid[["stay_id", "tbin"]].drop_duplicates().copy()
            out["sofa"] = pd.NA
            return out

        df_sofa_raw["starttime"] = pd.to_datetime(df_sofa_raw["starttime"], errors="coerce")

        # intime do grid (uma por stay)
        df_intime = df_grid[["stay_id", "intime"]].drop_duplicates()
        df_intime["intime"] = pd.to_datetime(df_intime["intime"], errors="coerce")

        df_sofa_raw = df_sofa_raw.merge(df_intime, on="stay_id", how="left")
        df_sofa_raw = df_sofa_raw.dropna(subset=["starttime", "intime"])

        # tbin em minutos (5-min bins)
        df_sofa_raw["tbin"] = (
            (df_sofa_raw["starttime"] - df_sofa_raw["intime"]).dt.total_seconds() / 60.0
        )
        df_sofa_raw["tbin"] = (df_sofa_raw["tbin"].astype("int64") // 5) * 5

        # mantém 1 linha por (stay_id, tbin): a mais recente no bin
        df_sofa_raw = df_sofa_raw.sort_values(["stay_id", "tbin", "starttime"])
        df_sofa = (
            df_sofa_raw.drop_duplicates(["stay_id", "tbin"], keep="last")
                      [["stay_id", "tbin", "sofa"]]
        )

        return df_sofa

    except Exception as e:
        print("[STEP 05][SOFA] ERRO:", repr(e))
        out = df_grid[["stay_id", "tbin"]].drop_duplicates().copy()
        out["sofa"] = pd.NA
        return out


def _fetch_score_by_stay_pandas(conn, table: str, col: str) -> pd.DataFrame:
    """
    Fetch score por stay_id (não varia por tbin).
    - Se a tabela tiver starttime: pega o registro mais cedo por stay (DISTINCT ON ... ORDER BY starttime)
    - Se NÃO tiver starttime (caso do apsiii no seu banco): usa MAX(col) por stay_id.

    Retorna DataFrame com col name = table (ex.: 'sapsii', 'apsiii')
    """
    try:
        # detecta se existe coluna starttime
        q_cols = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'mimiciv_derived' AND table_name = %s
        """
        df_cols = pd.read_sql_query(q_cols, conn, params=[table])
        cols = set(df_cols["column_name"].tolist())

        if col not in cols:
            print(f"[STEP 05][{table}] coluna '{col}' não existe. cols={sorted(cols)[:40]}...")
            return pd.DataFrame()

        if "starttime" in cols:
            query = f"""
                SELECT DISTINCT ON (stay_id) stay_id, {col} AS {table}
                FROM mimiciv_derived.{table}
                ORDER BY stay_id, starttime
            """
        else:
            # APSIII no seu caso
            query = f"""
                SELECT stay_id, MAX({col}) AS {table}
                FROM mimiciv_derived.{table}
                GROUP BY stay_id
            """

        df = _fetch_dataframe(conn, query)
        if df.empty:
            return pd.DataFrame()

        df = df.drop_duplicates(subset=["stay_id"])
        return df

    except Exception as e:
        print(f"[STEP 05][{table}] ERRO:", repr(e))
        return pd.DataFrame()


def step_05_scores_pandas(conn, df_grid: pd.DataFrame, df_step04: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 05: Adicionar severity scores (SOFA, SAPSII, APSIII) + sofa_delta.

    Regras:
    - SOFA: por (stay_id, tbin) via mimiciv_derived.sofa.sofa_24hours
    - SAPSII: por stay_id (replica em todos os tbins do stay)
    - APSIII: por stay_id (replica em todos os tbins do stay)
    - sofa_delta: sofa - baseline_sofa, onde baseline_sofa = primeiro SOFA não-nulo até 24h (<=1440 min)
    """
    print("[STEP 05] Fetching severity scores...")
    t_start = time.time()

    df_result = df_step04.copy()

    # SOFA (por tbin)
    print("[STEP 05] Fetching SOFA...")
    df_sofa = _fetch_sofa_pandas(conn, df_grid)
    df_result = df_result.merge(df_sofa, on=["stay_id", "tbin"], how="left")

    # SAPSII (por stay_id)
    print("[STEP 05] Fetching SAPSII...")
    df_sapsii = _fetch_score_by_stay_pandas(conn, "sapsii", "sapsii")
    if not df_sapsii.empty:
        df_result = df_result.merge(df_sapsii, on="stay_id", how="left")
    else:
        df_result["sapsii"] = pd.NA

    # APSIII (por stay_id)
    print("[STEP 05] Fetching APSIII...")
    df_apsiii = _fetch_score_by_stay_pandas(conn, "apsiii", "apsiii")
    if not df_apsiii.empty:
        df_result = df_result.merge(df_apsiii, on="stay_id", how="left")
    else:
        df_result["apsiii"] = pd.NA

    # sofa_delta (baseline = primeiro SOFA não-nulo até 24h)
    print("[STEP 05] Computing SOFA delta...")
    df_sorted = df_result.sort_values(["stay_id", "tbin"])

    # pega o primeiro SOFA NÃO-NA dentro de tbin <= 1440
    within_24h = df_sorted[df_sorted["tbin"] <= 1440][["stay_id", "sofa"]].copy()
    baseline = (
        within_24h.dropna(subset=["sofa"])
                  .groupby("stay_id")["sofa"]
                  .first()
                  .rename("sofa_baseline")
    )

    df_result = df_result.join(baseline, on="stay_id")

    # delta só quando há sofa e baseline
    df_result["sofa_delta"] = df_result["sofa"] - df_result["sofa_baseline"]
    df_result = df_result.drop(columns=["sofa_baseline"])

    # (opcional) sanity prints rápidos
    n_sofa = int(df_result["sofa"].notna().sum()) if "sofa" in df_result else 0
    n_sapsii = int(df_result["sapsii"].notna().sum()) if "sapsii" in df_result else 0
    n_apsiii = int(df_result["apsiii"].notna().sum()) if "apsiii" in df_result else 0
    print(f"[STEP 05] sanity: sofa_nonnull_rows={n_sofa:,} sapsii_nonnull_rows={n_sapsii:,} apsiii_nonnull_rows={n_apsiii:,}")

    print(f"[STEP 05] ✅ Concluído em {time.time() - t_start:.2f}s")
    return df_result

# ============================================================================
# STEP 06: FLUIDS (Urine output rate 24h + Fluid balance)
# ============================================================================

from typing import Optional, Tuple
import time
import pandas as pd


def _fetch_fluids_pandas(conn) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Retorna:
      df_urine: [stay_id, day, uo_mlkghr_24hr]
      df_fluid: [stay_id, day, fluid_balance_daily_ml, fluid_balance_total_ml]
    Usa JOIN cohort c (igual Step 5).
    """
    df_urine = None
    df_fluid = None

    # -------------------------
    # Urine output rate 24h (mL/kg/h) - coluna correta: uo_mlkghr_24hr
    # -------------------------
    try:
        query_urine = """
            SELECT u.stay_id,
                   date_trunc('day', u.charttime) AS day,
                   MAX(u.uo_mlkghr_24hr) AS uo_mlkghr_24hr
            FROM mimiciv_derived.urine_output_rate u
            JOIN cohort c USING (stay_id)
            WHERE u.charttime BETWEEN c.intime AND c.outtime
            GROUP BY u.stay_id, day
        """
        df_urine = _fetch_dataframe(conn, query_urine)
        if df_urine.empty:
            df_urine = None
    except Exception as e:
        print("[STEP 06][URINE_RATE] ERRO:", repr(e))
        try:
            conn.rollback()
        except Exception:
            pass
        df_urine = None

    # -------------------------
    # Fluid balance diário e acumulado
    # -------------------------
    try:
        query_fluid = """
            WITH input_daily AS (
                SELECT i.stay_id,
                       date_trunc('day', i.starttime) AS day,
                       SUM(i.amount) AS input_ml
                FROM mimiciv_icu.inputevents i
                JOIN cohort c USING (stay_id)
                WHERE i.starttime BETWEEN c.intime AND c.outtime
                  AND LOWER(i.amountuom) IN ('ml', 'milliliter', 'milliliters')
                GROUP BY i.stay_id, day
            ),
            output_daily AS (
                SELECT o.stay_id,
                       date_trunc('day', o.charttime) AS day,
                       SUM(o.value) AS output_ml
                FROM mimiciv_icu.outputevents o
                JOIN cohort c USING (stay_id)
                WHERE o.charttime BETWEEN c.intime AND c.outtime
                  AND LOWER(o.valueuom) IN ('ml', 'milliliter', 'milliliters')
                GROUP BY o.stay_id, day
            ),
            fluid_daily AS (
                SELECT COALESCE(i.stay_id, o.stay_id) AS stay_id,
                       COALESCE(i.day, o.day) AS day,
                       COALESCE(i.input_ml, 0) - COALESCE(o.output_ml, 0) AS fluid_balance_daily_ml
                FROM input_daily i
                FULL OUTER JOIN output_daily o
                    ON i.stay_id = o.stay_id AND i.day = o.day
            )
            SELECT stay_id,
                   day,
                   fluid_balance_daily_ml,
                   SUM(fluid_balance_daily_ml) OVER (PARTITION BY stay_id ORDER BY day)
                       AS fluid_balance_total_ml
            FROM fluid_daily
        """
        df_fluid = _fetch_dataframe(conn, query_fluid)
        if df_fluid.empty:
            df_fluid = None
    except Exception as e:
        print("[STEP 06][FLUID] ERRO:", repr(e))
        try:
            conn.rollback()
        except Exception:
            pass
        df_fluid = None

    return df_urine, df_fluid


def step_06_fluids_pandas(conn, df_grid: pd.DataFrame, df_step05: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 06: adiciona:
      - uo_mlkghr_24hr (mL/kg/h, janela 24h)
      - fluid_balance_daily_ml
      - fluid_balance_total_ml
    """
    print("[STEP 06] Fetching fluids...")
    t_start = time.time()

    df_result = df_step05.copy()

    if "charttime_bin" not in df_result.columns:
        raise ValueError("df_step05 precisa ter a coluna 'charttime_bin'.")

    df_result["charttime_bin"] = pd.to_datetime(df_result["charttime_bin"], errors="coerce")
    df_result["day"] = df_result["charttime_bin"].dt.floor("D")

    df_urine, df_fluid = _fetch_fluids_pandas(conn)

    # merge urine rate 24h
    if df_urine is not None:
        df_urine["day"] = pd.to_datetime(df_urine["day"], errors="coerce")
        df_result = df_result.merge(
            df_urine[["stay_id", "day", "uo_mlkghr_24hr"]],
            on=["stay_id", "day"],
            how="left",
        )
    else:
        df_result["uo_mlkghr_24hr"] = pd.NA

    # merge fluid
    if df_fluid is not None:
        df_fluid["day"] = pd.to_datetime(df_fluid["day"], errors="coerce")
        df_result = df_result.merge(
            df_fluid[["stay_id", "day", "fluid_balance_daily_ml", "fluid_balance_total_ml"]],
            on=["stay_id", "day"],
            how="left",
        )
    else:
        df_result["fluid_balance_daily_ml"] = pd.NA
        df_result["fluid_balance_total_ml"] = pd.NA

    df_result = df_result.drop(columns=["day"])

    # sanity prints
    n_uo = int(df_result["uo_mlkghr_24hr"].notna().sum()) if "uo_mlkghr_24hr" in df_result else 0
    n_fbd = int(df_result["fluid_balance_daily_ml"].notna().sum()) if "fluid_balance_daily_ml" in df_result else 0
    n_fbt = int(df_result["fluid_balance_total_ml"].notna().sum()) if "fluid_balance_total_ml" in df_result else 0
    print(f"[STEP 06] sanity: uo_mlkghr_24hr_nonnull_rows={n_uo:,} fluid_daily_nonnull_rows={n_fbd:,} fluid_total_nonnull_rows={n_fbt:,}")

    print(f"[STEP 06] ✅ Concluído em {time.time() - t_start:.2f}s")
    return df_result



# ============================================================================
# STEP 07: COMORBIDITIES (Charlson)
# ============================================================================

import time
import pandas as pd
from typing import Optional

# suas 16 comorbidades (como você pediu)
COMORBID_COLS = [
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
]


def _fetch_charlson_pandas(conn) -> Optional[pd.DataFrame]:
    """Fetch Charlson por hadm_id."""
    try:
        cols = ", ".join(["hadm_id"] + COMORBID_COLS)
        query = f"SELECT {cols} FROM mimiciv_derived.charlson"
        df = _fetch_dataframe(conn, query)
        if df.empty:
            return None
        return df.drop_duplicates(subset=["hadm_id"])
    except Exception as e:
        print("[STEP 07][CHARLSON] ERRO:", repr(e))
        return None


def step_07_comorbidities_pandas(
    conn,
    df_grid: pd.DataFrame,
    df_step06: pd.DataFrame,
) -> pd.DataFrame:
    """
    STEP 07: merge das comorbidades Charlson no dataset final.
    Precisa ter hadm_id.
    """
    print("[STEP 07] Fetching comorbidities (Charlson)...")
    t_start = time.time()

    df_result = df_step06.copy()

    # garante hadm_id
    if "hadm_id" not in df_result.columns:
        # tenta recuperar hadm_id do df_grid via stay_id
        if "hadm_id" in df_grid.columns:
            df_map = df_grid[["stay_id", "hadm_id"]].drop_duplicates()
            df_result = df_result.merge(df_map, on="stay_id", how="left")
        else:
            raise ValueError("Não achei 'hadm_id' em df_step06 nem em df_grid. Preciso de hadm_id para Charlson.")

    df_charlson = _fetch_charlson_pandas(conn)

    if df_charlson is not None and not df_charlson.empty:
        df_result = df_result.merge(df_charlson, on="hadm_id", how="left")
    else:
        for col in COMORBID_COLS:
            df_result[col] = pd.NA

    print(f"[STEP 07] ✅ Concluído em {time.time() - t_start:.2f}s")
    return df_result




# =============================================================================
# PIPELINE RUNNER
# =============================================================================


def _save_dataframe_to_parquet(
    df: pd.DataFrame,
    out_path: Path,
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Salvar DataFrame em parquet com particionamento, permitindo escrita incremental."""
    ensure_dir(out_path)

    table = pa.Table.from_pandas(df, preserve_index=False)

    if partition_cols:
        # IMPORTANTÍSSIMO:
        # - existing_data_behavior="overwrite_or_ignore" permite escrever várias vezes no mesmo diretório
        # - basename_template único evita colisão de nomes de arquivo
        ds.write_dataset(
            table,
            str(out_path),
            format="parquet",
            partitioning=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
        )
    else:
        pq.write_table(table, str(out_path / "data.parquet"))




def run_pipeline_postgres(
    conn,
    table_index: Set[Tuple[str, str]],
    out_dir: Path,
    config_dir: Path,
    max_stays: int,
    start_step: int,
    end_step: int,
    resume_from_step: Optional[int],
    compression: str,
    max_rows_per_file: int,
    max_rows_per_group: int,
    debug: bool,
    export_hdf5: bool = False,
    hdf5_path: Optional[Path] = None,
    hdf5_key: str = "data",
) -> None:
    """
    PIPELINE EM PANDAS, EXECUÇÃO SEGURA EM CLUSTER:
    - Step01 pode continuar "full" (gera dataset e salva particionado)
    - Step02..Step07 rodam por partição stay_bucket (0..STAY_BUCKET_MOD-1)
      para nunca montar 47M linhas em RAM.
    - Resume não faz concat de parquets.
    """
    import time
    import gc


    ensure_dir(out_dir / "reports")
    logger = setup_logging(str(out_dir / "reports" / "pipeline.log"))

    config = load_config(config_dir)
    missingness_tracker = MissingnessTracker()

    if debug and (not max_stays or max_stays > 500):
        max_stays = 500

    # ========================================================================
    # PREPARAÇÃO: Cohort + Grid (em memória)
    # ========================================================================
    t_prep_start = time.time()

    log_event(logger, "building_cohort")
    _create_temp_cohort(conn, table_index, max_stays)

    log_event(logger, "building_time_grid")
    df_grid = _create_temp_grid(conn)
    log_event(logger, "prep_complete", prep_time_secs=f"{time.time() - t_prep_start:.2f}")

    # ========================================================================
    # RESOLVER ITEM IDs
    # ========================================================================
    lab_itemids = _resolve_lab_itemids_pg(conn, config.lab_items, table_index)
    vital_itemids = _resolve_vital_itemids_pg(conn, table_index)
    rbc_itemids = _resolve_rbc_itemids_pg(conn, table_index)
    use_derived_vitals = _table_exists(table_index, "mimiciv_derived", "vitalsign")

    # Height + Weight resolution
    height_info = None
    weight_info = None
    if _table_exists(table_index, "mimiciv_derived", "height") or _table_exists(table_index, "mimiciv_derived", "first_day_height"):
        height_table, height_col, height_time_col = _resolve_height_table(conn, table_index)
        if height_table:
            height_info = (height_table, height_col, height_time_col)
    if _table_exists(table_index, "mimiciv_derived", "weight_duration") or _table_exists(table_index, "mimiciv_derived", "weight_durations"):
        weight_table, weight_col, weight_time_col = _resolve_weight_table(conn, table_index)
        if weight_table:
            weight_info = (weight_table, weight_col, weight_time_col)

    # ========================================================================
    # Helper QA (amostra)
    # ========================================================================

    def _sample_by_stays(df: Optional[pd.DataFrame], stay_col: str = "stay_id", target_rows: int = 10_000, seed: int = 42) -> pd.DataFrame:
        """Sample ~target_rows rows, but keep entire stays (sequence-safe)."""
        if df is None or getattr(df, "empty", True):
            return pd.DataFrame()
        if stay_col not in df.columns:
            return df.head(0)

        counts = df.groupby(stay_col).size()
        if counts.empty:
            return df.head(0)

        rows_per_stay = int(counts.median())
        rows_per_stay = max(rows_per_stay, 1)

        n_stays = int(np.ceil(target_rows / rows_per_stay))
        n_stays = min(n_stays, len(counts))

        rng = np.random.default_rng(seed)
        picked = rng.choice(counts.index.to_numpy(), size=n_stays, replace=False)

        out = df[df[stay_col].isin(picked)].copy()
        sort_cols = [c for c in [stay_col, "tbin"] if c in out.columns]
        if sort_cols:
            out = out.sort_values(sort_cols)
        return out

    def _pick_one_bucket_dir(dataset_path: Path) -> Optional[Path]:
        """Return a path containing parquet files for one bucket.

        Supports:
          - dataset_path/0/... (legacy)
          - dataset_path/stay_bucket=0/... (pyarrow partitioning)
        """
        # legacy
        cand = dataset_path / "0"
        if cand.exists():
            return cand

        # pyarrow style
        cand = dataset_path / "stay_bucket=0"
        if cand.exists():
            return cand

        # any stay_bucket=*
        sb = sorted([p for p in dataset_path.iterdir() if p.is_dir() and p.name.startswith("stay_bucket=")])
        if sb:
            return sb[0]

        # numeric bucket dirs fallback
        num = []
        for p in dataset_path.iterdir():
            if p.is_dir():
                try:
                    int(p.name)
                    num.append(p)
                except ValueError:
                    pass
        num = sorted(num, key=lambda p: int(p.name)) if num else []
        return num[0] if num else None

    def save_step_sample_from_path(step_num: int, step_name: str, dataset_path: Path, n: int = 10_000, seed: int = 42) -> None:
        """Save an imputed sample (~n rows) for a step, reading only a single bucket to stay RAM-safe."""
        bucket_dir = _pick_one_bucket_dir(dataset_path)
        if bucket_dir is None:
            print(f"[SAMPLE] Não encontrei buckets em {dataset_path}, não salvou amostra.")
            return

        files = sorted(bucket_dir.rglob("*.parquet"))
        if not files:
            print(f"[SAMPLE] Bucket sem parquet em {bucket_dir}, não salvou amostra.")
            return

        # Read a few parquet files (enough rows for ~n after sampling), but keep it RAM-safe.
        dfs: List[pd.DataFrame] = []
        rows_acc = 0
        max_files = 8
        for fp in files[:max_files]:
            try:
                dfi = pq.read_table(str(fp)).to_pandas()
            except Exception:
                continue
            if dfi is None or dfi.empty:
                continue
            dfs.append(dfi)
            rows_acc += len(dfi)
            if rows_acc >= n * 2:
                break

        if not dfs:
            print(f"[SAMPLE] Não consegui ler parquet de {bucket_dir}, não salvou amostra.")
            return

        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

        # Common postprocess for samples:
        # - drop unwanted meta cols (including hadm_id/ham_id/intime/outtime/staybucket)
        # - drop *_imputed
        df = _postprocess_common(step_num, df, drop_imputed=True)
        if df is None or getattr(df, 'empty', True):
            print(f"[SAMPLE] Pós-processamento gerou DF vazio para STEP {step_num:02d} ({step_name}).")
            return

        df_s = _sample_by_stays(df, target_rows=n, seed=seed)
        if df_s is None or df_s.empty:
            print(f"[SAMPLE] Amostra vazia para STEP {step_num:02d} ({step_name}).")
            return

        samples_dir = out_dir / "reports" / "samples"
        ensure_dir(samples_dir)

        safe_name = re.sub(r"[^a-zA-Z0-9_]+", "_", step_name.strip().lower())
        sample_file_parquet = samples_dir / f"step_{step_num:02d}_{safe_name}_sample10k_imputed.parquet"
        sample_file_csv = samples_dir / f"step_{step_num:02d}_{safe_name}_sample10k_imputed.csv"

        pq.write_table(pa.Table.from_pandas(df_s, preserve_index=False), sample_file_parquet)
        df_s.to_csv(sample_file_csv, index=False, float_format="%.2f")

        print(f"[SAMPLE] ✅ Salvo em {sample_file_parquet}")
        print(f"[SAMPLE] ✅ Salvo em {sample_file_csv}")

    # ========================================================================
    # Helpers PARTITION-WISE
    # ========================================================================
    def _step_path(step_num: int, preimpute: bool = False) -> Path:
        name = STEP_NAMES[step_num] + ("_preimpute" if preimpute else "")
        return out_dir / "intermediate" / name

    def _bucket_dir(step_path: Path, bucket: int) -> Optional[Path]:
        """
        Retorna o diretório do bucket, suportando:
          - step_path/0/...
          - step_path/stay_bucket=0/...
        """
        cand1 = step_path / str(bucket)
        if cand1.exists():
            return cand1
        cand2 = step_path / f"stay_bucket={bucket}"
        if cand2.exists():
            return cand2
        return None

    def _iter_buckets(step_path: Path) -> List[int]:
        """
        Lista buckets existentes (0..), suportando:
          - pastas numéricas: 0,1,2...
          - partições pyarrow: stay_bucket=0, stay_bucket=1...
        """
        buckets: List[int] = []
        if not step_path.exists():
            return buckets

        for p in step_path.iterdir():
            if not p.is_dir():
                continue

            # pyarrow partitioning
            if p.name.startswith("stay_bucket="):
                try:
                    buckets.append(int(p.name.split("=", 1)[1]))
                except Exception:
                    continue
            else:
                # legacy numeric
                try:
                    buckets.append(int(p.name))
                except Exception:
                    continue

        buckets = sorted(set(buckets))
        return buckets

    def _read_bucket_df(step_path: Path, bucket: int) -> pd.DataFrame:
        """
        Lê TODOS os parquets do bucket (robusto).
        """
        bdir = _bucket_dir(step_path, bucket)
        if bdir is None:
            return pd.DataFrame()

        files = sorted(bdir.rglob("*.parquet"))
        if not files:
            return pd.DataFrame()

        # lê todos e concat (RAM-safe porque é por bucket)
        dfs: List[pd.DataFrame] = []
        for fp in files:
            try:
                dfi = pq.read_table(str(fp)).to_pandas()
            except Exception:
                continue
            if dfi is not None and not dfi.empty:
                dfs.append(dfi)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

    def _pick_one_bucket_dir(dataset_path: Path) -> Optional[Path]:
        """
        Escolhe 1 bucket para amostragem.
        """
        # prefere stay_bucket=0
        cand = dataset_path / "stay_bucket=0"
        if cand.exists():
            return cand

        # fallback legacy 0
        cand = dataset_path / "0"
        if cand.exists():
            return cand

        # qualquer stay_bucket=*
        sb = sorted([p for p in dataset_path.iterdir() if p.is_dir() and p.name.startswith("stay_bucket=")])
        if sb:
            return sb[0]

        # qualquer numérico
        nums = []
        for p in dataset_path.iterdir():
            if p.is_dir():
                try:
                    int(p.name)
                    nums.append(p)
                except ValueError:
                    pass
        nums = sorted(nums, key=lambda p: int(p.name)) if nums else []
        return nums[0] if nums else None

    def save_step_sample_from_path(
        step_num: int,
        step_name: str,
        dataset_path: Path,
        n: int = 10_000,
        seed: int = 42,
    ) -> None:
        """
        Salva amostra (~n linhas) lendo só 1 bucket.
        IMPORTANTE:
          - STEP 01: não tem imputação -> remove *_imputed (não deveria existir)
          - STEP >=2: mantém *_imputed no sample (como você pediu)
        """
        bucket_dir = _pick_one_bucket_dir(dataset_path)
        if bucket_dir is None:
            print(f"[SAMPLE] Não encontrei buckets em {dataset_path}, não salvou amostra.")
            return

        files = sorted(bucket_dir.rglob("*.parquet"))
        if not files:
            print(f"[SAMPLE] Bucket sem parquet em {bucket_dir}, não salvou amostra.")
            return

        dfs: List[pd.DataFrame] = []
        rows_acc = 0
        max_files = 12

        for fp in files[:max_files]:
            try:
                dfi = pq.read_table(str(fp)).to_pandas()
            except Exception:
                continue
            if dfi is None or dfi.empty:
                continue
            dfs.append(dfi)
            rows_acc += len(dfi)
            if rows_acc >= n * 3:
                break

        if not dfs:
            print(f"[SAMPLE] Não consegui ler parquet de {bucket_dir}, não salvou amostra.")
            return

        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

        # STEP 1: remove imputed (por segurança)
        # STEP 2+: mantém imputed
        drop_imputed = (step_num in (1, 8))

        # Pós-processamento (não derruba _imputed no step2+)
        df = _postprocess_common(step_num, df, drop_imputed=drop_imputed)
        if df is None or getattr(df, "empty", True):
            print(f"[SAMPLE] Pós-processamento gerou DF vazio para STEP {step_num:02d} ({step_name}).")
            return

        df_s = _sample_by_stays(df, target_rows=n, seed=seed)
        if df_s is None or df_s.empty:
            print(f"[SAMPLE] Amostra vazia para STEP {step_num:02d} ({step_name}).")
            return

        samples_dir = out_dir / "reports" / "samples"
        ensure_dir(samples_dir)

        safe_name = re.sub(r"[^a-zA-Z0-9_]+", "_", step_name.strip().lower())
        sample_file_parquet = samples_dir / f"step_{step_num:02d}_{safe_name}_sample10k.parquet"
        sample_file_csv = samples_dir / f"step_{step_num:02d}_{safe_name}_sample10k.csv"

        pq.write_table(pa.Table.from_pandas(df_s, preserve_index=False), sample_file_parquet)
        df_s.to_csv(sample_file_csv, index=False, float_format="%.2f")

        print(f"[SAMPLE] ✅ Salvo em {sample_file_parquet}")
        print(f"[SAMPLE] ✅ Salvo em {sample_file_csv}")

    def save_model_ready_sample(
        dataset_path: Path,
        n: int = 10_000,
        seed: int = 42,
    ) -> None:
        files = sorted(dataset_path.rglob("*.parquet"))
        if not files:
            print(f"[SAMPLE] Não encontrei parquets em {dataset_path}, não salvou amostra.")
            return

        dfs: List[pd.DataFrame] = []
        rows_acc = 0
        max_files = 12
        for fp in files[:max_files]:
            try:
                dfi = pq.read_table(str(fp)).to_pandas()
            except Exception:
                continue
            if dfi is None or dfi.empty:
                continue
            dfs.append(dfi)
            rows_acc += len(dfi)
            if rows_acc >= n * 3:
                break

        if not dfs:
            print(f"[SAMPLE] Não consegui ler parquet de {dataset_path}, não salvou amostra.")
            return

        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        drop_cols = [c for c in df.columns if c.endswith("_imputed")]
        drop_cols += [c for c in META_COLS_TO_DROP if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        df_s = _sample_by_stays(df, target_rows=n, seed=seed)
        if df_s is None or df_s.empty:
            print("[SAMPLE] Amostra vazia para model_ready.")
            return

        samples_dir = out_dir / "reports" / "samples"
        ensure_dir(samples_dir)
        sample_file_parquet = samples_dir / "model_ready_sample10k.parquet"
        sample_file_csv = samples_dir / "model_ready_sample10k.csv"
        pq.write_table(pa.Table.from_pandas(df_s, preserve_index=False), sample_file_parquet)
        df_s.to_csv(sample_file_csv, index=False, float_format="%.2f")
        print(f"[SAMPLE] ✅ Salvo em {sample_file_parquet}")
        print(f"[SAMPLE] ✅ Salvo em {sample_file_csv}")

    
    def export_final_to_hdf5(final_dataset_path: Path, out_hdf5: Path, key: str = "data", batch_rows: int = 100_000) -> None:
        """Stream the final (parquet dataset) into a single HDF5 file.

        Notes:
        - Uses pandas.HDFStore with format='table' so it can be appended in chunks.
        - Converts datetime columns to int64 (ns) to keep HDF5 stable.
        - Drops META_COLS_TO_DROP and stay_bucket (partition key) from the exported table.
        """
        import pandas as pd
        import pyarrow.dataset as ds

        out_hdf5.parent.mkdir(parents=True, exist_ok=True)
        if out_hdf5.exists():
            out_hdf5.unlink()

        dataset = ds.dataset(str(final_dataset_path), format="parquet")
        scanner = dataset.scanner(batch_size=batch_rows)

        with pd.HDFStore(str(out_hdf5), mode="w", complevel=5, complib="blosc:zstd") as store:
            first = True
            for batch in scanner.to_batches():
                df_b = batch.to_pandas()
                # normalize time bins again (safe)
                _normalize_time_bins(df_b)
                df_b = _clip_and_round_numeric(df_b, clip_abs=1e6, decimals=2)
                df_b = _reorder_time_cols(df_b)

                # drop unwanted cols
                drop_cols = [c for c in META_COLS_TO_DROP if c in df_b.columns]
                if drop_cols:
                    df_b = df_b.drop(columns=drop_cols)
                if "stay_bucket" in df_b.columns:
                    df_b = df_b.drop(columns=["stay_bucket"])

                # make datetimes HDF5-friendly
                for c in list(df_b.columns):
                    if pd.api.types.is_datetime64_any_dtype(df_b[c]) or pd.api.types.is_datetime64tz_dtype(df_b[c]):
                        df_b[c] = pd.to_datetime(df_b[c], errors="coerce").view("int64")

                if df_b.empty:
                    continue

                store.append(key, df_b, format="table", data_columns=["stay_id"] if "stay_id" in df_b.columns else None, index=False)
                first = False

        print(f"[HDF5] ✅ Export final dataset to {out_hdf5} (key='{key}')")

    def _run_step_partitionwise(
            step_num: int,
            prev_step_num: int,
            step_name: str,
            build_fn,
            impute_no_ffill_cols: Optional[List[str]] = None,
            postprocess_fn=None,
            drop_imputed_final: bool = False,
        ) -> None:
            """
            Roda um step usando:
              - prev final parquet (imputed) como input
              - processa bucket por bucket
              - grava preimpute parquet particionado por stay_bucket
              - roda impute_dataset para gerar final parquet (imputed)
            build_fn deve ser uma função:
              (df_grid_bucket, df_prev_bucket) -> df_out_bucket
            """
            print("\n" + "=" * 80)
            print(f"STEP {step_num:02d}: {step_name} (partition-wise)")
            print("=" * 80)

            t0 = time.time()

            prev_final = _step_path(prev_step_num, preimpute=False)
            _assert_dataset_exists(prev_final, prev_step_num)

            pre_path = _step_path(step_num, preimpute=True)
            final_path = _step_path(step_num, preimpute=False)
            _remove_path(pre_path)
            _remove_path(final_path)
            ensure_dir(pre_path)

            buckets = _iter_buckets(prev_final)
            if not buckets:
                raise RuntimeError(f"Não achei buckets em {prev_final}")

            for b in buckets:
                # filtra grid para o bucket (evita 47M em RAM)
                df_grid_b = df_grid[df_grid["stay_bucket"] == b]
                df_prev_b = _read_bucket_df(prev_final, b)

                if df_prev_b.empty:
                    print(f"[STEP {step_num:02d}] Bucket {b}: vazio, pulando.")
                    continue

                df_out_b = build_fn(df_grid_b, df_prev_b)

                # GARANTIR COLUNA DE PARTICIONAMENTO (obrigatório para ds.write_dataset)
                if "stay_bucket" not in df_out_b.columns:
                    df_out_b["stay_bucket"] = pd.Series(b, index=df_out_b.index, dtype="Int64")
                else:
                    # normaliza tipo e garante consistência com o bucket atual
                    df_out_b["stay_bucket"] = pd.to_numeric(df_out_b["stay_bucket"], errors="coerce").astype("Int64")
                    df_out_b.loc[:, "stay_bucket"] = pd.Series(b, index=df_out_b.index, dtype="Int64")



                # grava no layout stay_bucket=b/part-0.parquet
                _save_dataframe_to_parquet(df_out_b, pre_path, ["stay_bucket"])

                del df_grid_b, df_prev_b, df_out_b
                gc.collect()

            # Impute para gerar final_path
            print(f"[STEP {step_num:02d}] Imputando dados...")
            impute_dataset(
                pre_path,
                final_path,
                config.defaults,
                _feature_cols_for_step(step_num),
                partition_cols=["stay_bucket"],
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                max_rows_per_group=max_rows_per_group,
                no_ffill_cols=impute_no_ffill_cols,
                postprocess_fn=(
                    lambda d: _postprocess_common(
                        step_num,
                        (postprocess_fn(d) if postprocess_fn else d),
                        drop_imputed=drop_imputed_final,
                    )
                ),
            )
            print(f"[STEP {step_num:02d}] ✅ Imputação concluída")

            log_event(logger, f"step{step_num:02d}_complete_partitionwise", time_secs=f"{time.time() - t0:.2f}")
            save_step_sample_from_path(step_num, step_name.lower().replace(" ", "_"), final_path)

        # ========================================================================
    # RESUME (corrigido): NÃO carrega step anterior em RAM
    # ========================================================================
    if resume_from_step is not None and start_step > 1:
        needed = start_step - 1
        prev_final = _step_path(needed, preimpute=False)
        _assert_dataset_exists(prev_final, needed)
        print(f"[RESUME] OK: STEP {needed} existe em disco em {prev_final}. Vou processar bucket-wise a partir do STEP {start_step}.")

    try:
        # ====================================================================
        # STEP 01 (mantive como você tem hoje; é pesado mas funciona)
        # ====================================================================
        if 1 >= start_step and 1 <= end_step:
            print("\n" + "=" * 80)
            print("STEP 01: DEMOGRAPHICS")
            print("=" * 80)

            t_step = time.time()

            # Fetch demographics once (one row per stay_id), then replicate across the time grid.
            df_demo_full = _fetch_demographics_pandas(conn, df_grid, height_info, weight_info)
            if df_demo_full is None:
                raise RuntimeError("BUG: _fetch_demographics_pandas retornou None (deveria retornar DataFrame).")

            if not df_demo_full.empty:
                # age as integer (safer for modeling)
                if "age" in df_demo_full.columns:
                    df_demo_full["age"] = pd.to_numeric(df_demo_full["age"], errors="coerce").round().astype("Int64")
                df_demo_full = df_demo_full.drop_duplicates(subset=["stay_id"], keep="first")

            # Merge demographics into full grid (replicate values over tbin)
            df_step01 = df_grid.merge(df_demo_full, on="stay_id", how="left")

            # Drop meta columns you don't want to keep (but keep stay_bucket for partitioning)
            drop_cols = [c for c in ["intime", "outtime", "staybucket", "hadm_id", "ham_id", "charttime"] if c in df_step01.columns]
            if drop_cols:
                df_step01 = df_step01.drop(columns=drop_cols)

            _normalize_time_bins(df_step01)

            df_step01 = _clip_and_round_numeric(df_step01, clip_abs=1e6, decimals=2)
            df_step01 = _reorder_time_cols(df_step01)
            # Pós-processamento comum do Step01 (sem imputação)
            df_step01 = _postprocess_common(1, df_step01, drop_imputed=True)

            log_event(logger, "step01_complete", time_secs=f"{time.time() - t_step:.2f}", rows=len(df_step01))

            final_path = _step_path(1, preimpute=False)
            _remove_path(final_path)

            print("[STEP 01] Salvando parquet final (sem imputação)...")
            _save_dataframe_to_parquet(df_step01, final_path, ["stay_bucket"])
            print(f"[STEP 01] ✅ Salvo em {final_path}")

            log_event(logger, "step01_saved_no_impute")

            # sample do step1 (sem _imputed)
            save_step_sample_from_path(1, "demographics", final_path, n=10_000)

            del df_step01
            gc.collect()

# ====================================================================
        # STEP 02..07 (partition-wise)
        # ====================================================================
        # ====================================================================
        # STEP 02 (partition-wise): LABS + VITALS em cima do STEP 01
        # ====================================================================
        if 2 >= start_step and 2 <= end_step:
            print("\n" + "=" * 80)
            print("STEP 02: LABS + VITALS (partition-wise, merge em cima do STEP 01)")
            print("=" * 80)

            t0 = time.time()

            prev_final = _step_path(1, preimpute=False)
            _assert_dataset_exists(prev_final, 1)

            pre_path = _step_path(2, preimpute=True)
            final_path = _step_path(2, preimpute=False)
            _remove_path(pre_path)
            _remove_path(final_path)
            ensure_dir(pre_path)

            buckets = _iter_buckets(prev_final)
            if not buckets:
                raise RuntimeError(f"[STEP 02] Não achei buckets no STEP 01 em {prev_final}")

            # helpers locais para queries por bucket (filtra stay_id = ANY(%s))
            def _fetch_labs_pandas_bucket(conn, df_grid_b: pd.DataFrame, lab_itemids: Dict[str, List[int]], stay_ids: List[int]) -> pd.DataFrame:
                all_itemids = sorted({item for items in lab_itemids.values() for item in items})
                base = df_grid_b[["stay_id", "tbin"]].drop_duplicates().copy()

                if not all_itemids or not stay_ids:
                    for col in LAB_COLS:
                        base[col] = None
                    return base[["stay_id", "tbin"] + LAB_COLS]

                query = """
                    SELECT l.subject_id, l.hadm_id, l.itemid, l.charttime, l.valuenum
                    FROM mimiciv_hosp.labevents l
                    INNER JOIN (
                        SELECT subject_id, hadm_id, stay_id, intime
                        FROM cohort
                        WHERE stay_id = ANY(%s)
                    ) c ON l.subject_id = c.subject_id AND l.hadm_id = c.hadm_id
                    WHERE l.itemid = ANY(%s)
                """
                params = (stay_ids, all_itemids)
                df_raw = _fetch_dataframe(conn, query, params)

                if df_raw.empty:
                    for col in LAB_COLS:
                        base[col] = None
                    return base[["stay_id", "tbin"] + LAB_COLS]

                # map (subject_id, hadm_id) -> stay_id,intime via df_grid_b (mais rápido e consistente)
                ref = df_grid_b[["subject_id", "hadm_id", "stay_id", "intime"]].drop_duplicates()
                df_raw = df_raw.merge(ref, on=["subject_id", "hadm_id"], how="left")
                df_raw = df_raw.dropna(subset=["stay_id"])

                df_raw["charttime"] = pd.to_datetime(df_raw["charttime"], errors="coerce")
                df_raw["intime"] = pd.to_datetime(df_raw["intime"], errors="coerce")

                minutes = (df_raw["charttime"] - df_raw["intime"]).dt.total_seconds() / 60.0
                df_raw["tbin"] = _safe_tbin_from_minutes(minutes)
                df_raw = df_raw.dropna(subset=["tbin"])
                df_raw["tbin"] = df_raw["tbin"].astype("int64")

                itemid_to_lab = {}
                for lab_name, items in lab_itemids.items():
                    for itemid in items:
                        itemid_to_lab[int(itemid)] = lab_name
                df_raw["lab_name"] = df_raw["itemid"].map(itemid_to_lab)
                df_raw = df_raw.dropna(subset=["lab_name"])

                df_raw = df_raw.sort_values(["stay_id", "tbin", "lab_name", "charttime"])
                df_last = df_raw.drop_duplicates(["stay_id", "tbin", "lab_name"], keep="last")[["stay_id", "tbin", "lab_name", "valuenum"]]

                df_piv = df_last.pivot_table(index=["stay_id", "tbin"], columns="lab_name", values="valuenum", aggfunc="last").reset_index()
                for col in LAB_COLS:
                    if col not in df_piv.columns:
                        df_piv[col] = None

                return df_piv[["stay_id", "tbin"] + LAB_COLS]

            def _fetch_vitals_pandas_bucket(conn, df_grid_b: pd.DataFrame, use_derived: bool, vital_itemids: Dict[str, List[int]], stay_ids: List[int]) -> pd.DataFrame:
                base = df_grid_b[["stay_id", "tbin"]].drop_duplicates().copy()

                if not stay_ids:
                    for col in VITAL_COLS:
                        base[col] = None
                    return base[["stay_id", "tbin"] + VITAL_COLS]

                if use_derived:
                    query = """
                        SELECT v.stay_id, v.charttime,
                               v.heart_rate, COALESCE(v.sbp, v.sbp_ni) AS sbp,
                               COALESCE(v.dbp, v.dbp_ni) AS dbp,
                               COALESCE(v.mbp, v.mbp_ni) AS mbp,
                               v.resp_rate, v.temperature, v.spo2, v.glucose
                        FROM mimiciv_derived.vitalsign v
                        WHERE v.stay_id = ANY(%s)
                    """
                    df_raw = _fetch_dataframe(conn, query, (stay_ids,))
                    if df_raw.empty:
                        for col in VITAL_COLS:
                            base[col] = None
                        return base[["stay_id", "tbin"] + VITAL_COLS]

                    df_raw["charttime"] = pd.to_datetime(df_raw["charttime"], errors="coerce")
                    ref = df_grid_b[["stay_id", "intime"]].drop_duplicates()
                    df_raw = df_raw.merge(ref, on="stay_id", how="left")
                    df_raw["intime"] = pd.to_datetime(df_raw["intime"], errors="coerce")

                    minutes = (df_raw["charttime"] - df_raw["intime"]).dt.total_seconds() / 60.0
                    df_raw["tbin"] = _safe_tbin_from_minutes(minutes)
                    df_raw = df_raw.dropna(subset=["tbin"])
                    df_raw["tbin"] = df_raw["tbin"].astype("int64")

                    df_raw = df_raw.sort_values(["stay_id", "tbin", "charttime"])
                    df_v = df_raw.drop_duplicates(["stay_id", "tbin"], keep="last")[
                        ["stay_id", "tbin", "heart_rate", "sbp", "dbp", "mbp", "resp_rate", "temperature", "spo2", "glucose"]
                    ].rename(columns={"glucose": "glucose_vital"})
                else:
                    all_itemids = sorted({item for items in vital_itemids.values() for item in items})
                    if not all_itemids:
                        for col in VITAL_COLS:
                            base[col] = None
                        return base[["stay_id", "tbin"] + VITAL_COLS]

                    query = """
                        SELECT ce.stay_id, ce.itemid, ce.charttime, ce.valuenum
                        FROM mimiciv_icu.chartevents ce
                        WHERE ce.stay_id = ANY(%s)
                          AND ce.itemid = ANY(%s)
                    """
                    df_raw = _fetch_dataframe(conn, query, (stay_ids, all_itemids))
                    if df_raw.empty:
                        for col in VITAL_COLS:
                            base[col] = None
                        return base[["stay_id", "tbin"] + VITAL_COLS]

                    ref = df_grid_b[["stay_id", "intime"]].drop_duplicates()
                    df_raw = df_raw.merge(ref, on="stay_id", how="left")
                    df_raw["charttime"] = pd.to_datetime(df_raw["charttime"], errors="coerce")
                    df_raw["intime"] = pd.to_datetime(df_raw["intime"], errors="coerce")

                    minutes = (df_raw["charttime"] - df_raw["intime"]).dt.total_seconds() / 60.0
                    df_raw["tbin"] = _safe_tbin_from_minutes(minutes)
                    df_raw = df_raw.dropna(subset=["tbin"])
                    df_raw["tbin"] = df_raw["tbin"].astype("int64")

                    itemid_to_vital = {}
                    for vital_name, items in vital_itemids.items():
                        for itemid in items:
                            itemid_to_vital[int(itemid)] = vital_name
                    df_raw["vital_name"] = df_raw["itemid"].map(itemid_to_vital)
                    df_raw = df_raw.dropna(subset=["vital_name"])

                    df_raw = df_raw.sort_values(["stay_id", "tbin", "vital_name", "charttime"])
                    df_last = df_raw.drop_duplicates(["stay_id", "tbin", "vital_name"], keep="last")[["stay_id", "tbin", "vital_name", "valuenum"]]

                    df_v = df_last.pivot_table(index=["stay_id", "tbin"], columns="vital_name", values="valuenum", aggfunc="last").reset_index()

                for col in VITAL_COLS:
                    if col not in df_v.columns:
                        df_v[col] = None

                return df_v[["stay_id", "tbin"] + VITAL_COLS]

            for b in buckets:
                df_grid_b = df_grid[df_grid["stay_bucket"] == b]
                df_prev_b = _read_bucket_df(prev_final, b)

                if df_prev_b.empty:
                    print(f"[STEP 02] Bucket {b}: vazio, pulando.")
                    continue

                stay_ids = df_prev_b["stay_id"].dropna().astype("int64").unique().tolist()

                # labs/vitals do bucket
                df_labs_b = _fetch_labs_pandas_bucket(conn, df_grid_b, lab_itemids, stay_ids)
                df_vitals_b = _fetch_vitals_pandas_bucket(conn, df_grid_b, use_derived_vitals, vital_itemids, stay_ids)

                # merge em cima do step anterior (DEMOGRAPHICS primeiro)
                df_out_b = df_prev_b.merge(df_labs_b, on=["stay_id", "tbin"], how="left")
                df_out_b = df_out_b.merge(df_vitals_b, on=["stay_id", "tbin"], how="left")

                # garante stay_bucket
                if "stay_bucket" not in df_out_b.columns:
                    df_out_b["stay_bucket"] = pd.Series(b, index=df_out_b.index, dtype="Int64")
                else:
                    df_out_b["stay_bucket"] = pd.Series(b, index=df_out_b.index, dtype="Int64")

                # grava preimpute
                _save_dataframe_to_parquet(df_out_b, pre_path, ["stay_bucket"])

                del df_grid_b, df_prev_b, df_labs_b, df_vitals_b, df_out_b
                gc.collect()

            print("[STEP 02] Imputando dados faltantes (gera *_imputed)...")
            impute_dataset(
                pre_path,
                final_path,
                config.defaults,
                _feature_cols_for_step(2),
                partition_cols=["stay_bucket"],
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                max_rows_per_group=max_rows_per_group,
                postprocess_fn=lambda d: _postprocess_common(2, d, drop_imputed=False),
            )
            print("[STEP 02] ✅ Imputação concluída")

            log_event(logger, "step02_complete_partitionwise", time_secs=f"{time.time() - t0:.2f}")
            save_step_sample_from_path(2, "labs_vitals", final_path, n=10_000)

            gc.collect()

        if 3 >= start_step and 3 <= end_step:
            # prepara tudo 1x (queries no Postgres 1 vez por processo)
            step03_cache = prepare_step03_cache(conn)

            def build_step03(df_grid_b: pd.DataFrame, df_prev_b: pd.DataFrame) -> pd.DataFrame:
                # df_grid_b não é necessário no modelo cacheado
                return step_03_therapies_bucket(df_prev_b, step03_cache)

            _run_step_partitionwise(
                step_num=3,
                prev_step_num=2,
                step_name="THERAPIES",
                build_fn=build_step03,
            )

            del step03_cache
            gc.collect()



        if 4 >= start_step and 4 <= end_step:
            def build_step04(df_grid_b: pd.DataFrame, df_prev_b: pd.DataFrame) -> pd.DataFrame:
                return step_04_transfusions_pandas(conn, df_grid_b, df_prev_b, rbc_itemids)

            _run_step_partitionwise(
                step_num=4,
                prev_step_num=3,
                step_name="TRANSFUSION",
                build_fn=build_step04,
                impute_no_ffill_cols=["rbc_transfusion_flag", "rbc_amount_ml_event"],
            )

        if 5 >= start_step and 5 <= end_step:
            def build_step05(df_grid_b: pd.DataFrame, df_prev_b: pd.DataFrame) -> pd.DataFrame:
                return step_05_scores_pandas(conn, df_grid_b, df_prev_b)

            _run_step_partitionwise(
                step_num=5,
                prev_step_num=4,
                step_name="SCORES",
                build_fn=build_step05,
                postprocess_fn=_compute_sofa_delta,
            )

        if 6 >= start_step and 6 <= end_step:
            def build_step06(df_grid_b: pd.DataFrame, df_prev_b: pd.DataFrame) -> pd.DataFrame:
                return step_06_fluids_pandas(conn, df_grid_b, df_prev_b)

            _run_step_partitionwise(
                step_num=6,
                prev_step_num=5,
                step_name="FLUIDS",
                build_fn=build_step06,
            )

        if 7 >= start_step and 7 <= end_step:
            def build_step07(df_grid_b: pd.DataFrame, df_prev_b: pd.DataFrame) -> pd.DataFrame:
                return step_07_comorbidities_pandas(conn, df_grid_b, df_prev_b)

            _run_step_partitionwise(
                step_num=7,
                prev_step_num=6,
                step_name="COMORBIDITIES",
                build_fn=build_step07,
            )

        # ====================================================================
        if 8 >= start_step and 8 <= end_step:
            def build_step08(df_grid_b: pd.DataFrame, df_prev_b: pd.DataFrame) -> pd.DataFrame:
                return add_engineered_features_bucket(df_prev_b)

            _run_step_partitionwise(
                step_num=8,
                prev_step_num=7,
                step_name="ENGINEERED_FEATURES",
                build_fn=build_step08,
                drop_imputed_final=True,
            )

        # OUTPUT FINAL (já está por bucket, então só copia/normaliza)
        # ====================================================================
        final_step = min(8, end_step)
        final_input = _step_path(final_step, preimpute=False)
        model_ready = out_dir / "model_ready" / "dataset_parquet"
        _remove_path(model_ready)
        ensure_dir(model_ready)

        ds_in = ds.dataset(str(final_input), format="parquet")
        drop_cols = [c for c in ["hadm_id", "ham_id", "intime", "outtime", "staybucket", "stay_bucket"] if c in ds_in.schema.names]
        drop_cols += [c for c in ds_in.schema.names if c.endswith("_imputed")]
        scanner = ds_in.scanner()

        for batch in scanner.to_batches():
            table = pa.Table.from_batches([batch])
            if drop_cols:
                table = table.drop(drop_cols)
            write_dataset_in_chunks(
                table.to_batches(),
                model_ready,
                partition_cols=[],
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                max_rows_per_group=max_rows_per_group,
            )

        missingness_tracker.to_csv(out_dir / "reports" / "missingness_sources_report.csv")
        _write_feature_dictionary(out_dir)
        log_event(logger, "pipeline_complete", path=str(model_ready))
        save_model_ready_sample(model_ready)

        if export_hdf5:
            out_hdf5 = hdf5_path if hdf5_path is not None else (out_dir / "outputs" / "dataset.h5")
            export_final_to_hdf5(model_ready, out_hdf5, key=hdf5_key)


        print("\n" + "=" * 80)
        print("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        print("=" * 80)

    except Exception as e:
        log_event(logger, "pipeline_error", error=str(e))
        raise
