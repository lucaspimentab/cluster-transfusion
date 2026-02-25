from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb

from .io_utils import find_table_path, duckdb_relation, write_dataset_in_chunks, ensure_dir
from .logging_utils import log_event


def _select_age_query(age_table: str, has_stay_id: bool) -> str:
    if has_stay_id:
        return f"SELECT stay_id, age FROM {age_table}"
    return f"SELECT subject_id, hadm_id, age FROM {age_table}"


def build_cohort(
    con: duckdb.DuckDBPyConnection,
    inventory,
    data_dir: Path,
    out_dir: Path,
    logger,
    max_stays: int = 0,
) -> Path:
    icu_path = find_table_path(data_dir, "mimiciv_icu", "icustays")
    if not icu_path:
        raise FileNotFoundError("Missing mimiciv_icu.icustays in data_dir")

    patients_path = find_table_path(data_dir, "mimiciv_hosp", "patients")
    admissions_path = find_table_path(data_dir, "mimiciv_hosp", "admissions")

    age_path = find_table_path(data_dir, "mimiciv_derived", "age")

    con.execute("PRAGMA threads=4")

    con.register("icustays", duckdb_relation(con, icu_path))
    if patients_path:
        con.register("patients", duckdb_relation(con, patients_path))
    if admissions_path:
        con.register("admissions", duckdb_relation(con, admissions_path))
    if age_path:
        con.register("age_tbl", duckdb_relation(con, age_path))

    has_age = age_path is not None

    if has_age:
        try:
            desc = con.sql("DESCRIBE age_tbl").df()
            age_cols = set(desc["column_name"].tolist())
        except Exception:
            age_cols = set()
        age_has_stay_id = "stay_id" in age_cols
        age_select = _select_age_query("age_tbl", age_has_stay_id)
    else:
        age_select = "SELECT NULL::DOUBLE AS age, NULL::INTEGER AS subject_id, NULL::INTEGER AS hadm_id"
        age_has_stay_id = False

    age_join = ""
    if has_age and age_has_stay_id:
        age_join = "LEFT JOIN (" + age_select + ") age ON icu.stay_id = age.stay_id"
    elif has_age:
        age_join = "LEFT JOIN (" + age_select + ") age ON icu.subject_id = age.subject_id AND icu.hadm_id = age.hadm_id"
    else:
        age_join = ""

    if has_age:
        age_expr = "age.age"
    else:
        if patients_path and admissions_path:
            age_expr = "patients.anchor_age + (EXTRACT(year FROM admissions.admittime) - patients.anchor_year)"
        else:
            age_expr = "NULL::DOUBLE"

    gender_join = "LEFT JOIN patients ON icu.subject_id = patients.subject_id" if patients_path else ""
    admit_join = "LEFT JOIN admissions ON icu.hadm_id = admissions.hadm_id" if admissions_path else ""
    gender_expr = "patients.gender" if patients_path else "NULL::VARCHAR"

    cohort_query = f"""
        WITH icu AS (
            SELECT subject_id, stay_id, hadm_id, intime, outtime
            FROM icustays
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
                   date_diff('second', intime, outtime) AS los_seconds,
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
               CAST(CEIL(los_seconds / 300.0) AS INTEGER) AS n_bins,
               CAST(los_seconds / 3600.0 AS DOUBLE) AS icu_los_hours
        FROM filtered
        WHERE age >= 18
          AND los_seconds >= 86400
          AND rn = 1
        {"LIMIT " + str(max_stays) if max_stays and max_stays > 0 else ""}
    """

    log_event(logger, "building_cohort")
    rel = con.sql(cohort_query)
    reader = rel.fetch_record_batch()

    cohort_out = out_dir / "intermediate" / "step00_cohort"
    ensure_dir(cohort_out)
    write_dataset_in_chunks(
        reader,
        cohort_out,
        partition_cols=[],
        compression="zstd",
        max_rows_per_file=1_000_000,
        max_rows_per_group=100_000,
    )

    return cohort_out
