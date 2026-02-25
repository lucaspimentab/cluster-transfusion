from __future__ import annotations

from typing import Optional
import duckdb

from ..io_utils import find_table_path, duckdb_relation


def urine_output_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    path = find_table_path(data_dir, "mimiciv_derived", "urine_output_rate")
    if not path:
        return None

    con.register("urine_output_rate", duckdb_relation(con, path))

    query = """
        SELECT u.stay_id,
               date_trunc('day', u.charttime) AS day,
               MAX(u.uo_mlkghr_24hr) AS uo_mlkghr_24hr
        FROM urine_output_rate u
        JOIN cohort c USING (stay_id)
        WHERE u.charttime BETWEEN c.intime AND c.outtime
        GROUP BY u.stay_id, day
    """
    return query


def fluid_balance_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    input_path = find_table_path(data_dir, "mimiciv_icu", "inputevents")
    output_path = find_table_path(data_dir, "mimiciv_icu", "outputevents")
    if not input_path or not output_path:
        return None

    con.register("inputevents", duckdb_relation(con, input_path))
    con.register("outputevents", duckdb_relation(con, output_path))

    query = """
        WITH input_daily AS (
            SELECT i.stay_id,
                   date_trunc('day', i.starttime) AS day,
                   SUM(i.amount) AS input_ml
            FROM inputevents i
            JOIN cohort c USING (stay_id)
            WHERE i.starttime BETWEEN c.intime AND c.outtime
              AND LOWER(i.amountuom) IN ('ml', 'milliliter', 'milliliters')
            GROUP BY i.stay_id, day
        ),
        output_daily AS (
            SELECT o.stay_id,
                   date_trunc('day', o.charttime) AS day,
                   SUM(o.value) AS output_ml
            FROM outputevents o
            JOIN cohort c USING (stay_id)
            WHERE o.charttime BETWEEN c.intime AND c.outtime
              AND LOWER(o.valueuom) IN ('ml', 'milliliter', 'milliliters')
            GROUP BY o.stay_id, day
        )
        SELECT COALESCE(i.stay_id, o.stay_id) AS stay_id,
               COALESCE(i.day, o.day) AS day,
               COALESCE(i.input_ml, 0) - COALESCE(o.output_ml, 0) AS fluid_balance_daily_ml
        FROM input_daily i
        FULL OUTER JOIN output_daily o
            ON i.stay_id = o.stay_id AND i.day = o.day
    """
    return query
