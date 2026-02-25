from __future__ import annotations

import duckdb

from ..io_utils import find_table_path, duckdb_relation


def demographics_subquery(con: duckdb.DuckDBPyConnection, data_dir, use_height: bool, use_weight: bool) -> str:
    height_path = find_table_path(data_dir, "mimiciv_derived", "height") if use_height else None
    weight_path = find_table_path(data_dir, "mimiciv_derived", "weight_duration") if use_weight else None

    if height_path:
        con.register("height", duckdb_relation(con, height_path))
    if weight_path:
        con.register("weight_duration", duckdb_relation(con, weight_path))

    height_cte = ""
    if height_path:
        height_cte = """
            , height_pick AS (
                SELECT h.stay_id,
                       arg_min(h.height, abs(date_diff('second', h.charttime, c.intime))) AS height_cm
                FROM height h
                JOIN cohort c USING (stay_id)
                WHERE h.charttime BETWEEN c.intime AND c.outtime
                GROUP BY h.stay_id
            )
        """
    else:
        height_cte = ", height_pick AS (SELECT NULL::INTEGER AS stay_id, NULL::DOUBLE AS height_cm WHERE FALSE)"

    weight_cte = ""
    if weight_path:
        weight_cte = """
            , weight_pick AS (
                SELECT w.stay_id,
                       arg_min(w.weight, w.starttime) AS weight_kg
                FROM weight_duration w
                JOIN cohort c USING (stay_id)
                WHERE w.starttime BETWEEN c.intime AND c.outtime
                GROUP BY w.stay_id
            )
        """
    else:
        weight_cte = ", weight_pick AS (SELECT NULL::INTEGER AS stay_id, NULL::DOUBLE AS weight_kg WHERE FALSE)"

    query = f"""
        WITH base AS (
            SELECT stay_id, age, gender
            FROM cohort
        )
        {height_cte}
        {weight_cte}
        SELECT b.stay_id,
               b.age,
               b.gender,
               wp.weight_kg,
               hp.height_cm,
               CASE
                 WHEN wp.weight_kg IS NOT NULL AND hp.height_cm IS NOT NULL AND hp.height_cm > 0
                 THEN wp.weight_kg / pow(hp.height_cm / 100.0, 2)
                 ELSE NULL
               END AS bmi
        FROM base b
        LEFT JOIN weight_pick wp ON b.stay_id = wp.stay_id
        LEFT JOIN height_pick hp ON b.stay_id = hp.stay_id
    """
    return query
