from __future__ import annotations

from typing import Optional

import duckdb

from ..io_utils import find_table_path, duckdb_relation


def charlson_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    path = find_table_path(data_dir, "mimiciv_derived", "charlson")
    if not path:
        return None
    con.register("charlson", duckdb_relation(con, path))
    query = """
        SELECT c.hadm_id,
               c.myocardial_infarct,
               c.congestive_heart_failure,
               c.peripheral_vascular_disease,
               c.cerebrovascular_disease,
               c.dementia,
               c.chronic_pulmonary_disease,
               c.rheumatic_disease,
               c.peptic_ulcer_disease,
               c.mild_liver_disease,
               c.diabetes_without_cc,
               c.diabetes_with_cc,
               c.renal_disease,
               c.malignant_cancer,
               c.severe_liver_disease,
               c.metastatic_solid_tumor,
               c.aids
        FROM charlson c
    """
    return query
