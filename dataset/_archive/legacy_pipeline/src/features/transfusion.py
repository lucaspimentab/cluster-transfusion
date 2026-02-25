from __future__ import annotations

from typing import Optional

import duckdb

from ..io_utils import find_table_path, duckdb_relation


RBC_LABEL_PATTERNS = ["packed red", "prbc", "red blood cell", "rbc", "packed cells"]


def transfusion_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    d_items_path = find_table_path(data_dir, "mimiciv_icu", "d_items")
    input_path = find_table_path(data_dir, "mimiciv_icu", "inputevents")
    if not d_items_path or not input_path:
        return None

    con.register("d_items", duckdb_relation(con, d_items_path))
    con.register("inputevents", duckdb_relation(con, input_path))

    where_clauses = " OR ".join([f"LOWER(label) LIKE '%{pat}%'" for pat in RBC_LABEL_PATTERNS])
    itemids = con.sql(f"SELECT itemid FROM d_items WHERE {where_clauses}").df()["itemid"].tolist()
    if not itemids:
        return None

    item_list = ",".join(str(int(x)) for x in itemids)
    query = f"""
        WITH r AS (
            SELECT i.stay_id,
                   i.starttime AS charttime,
                   i.amount,
                   LOWER(i.amountuom) AS amountuom,
                   c.intime
            FROM inputevents i
            JOIN cohort c USING (stay_id)
            WHERE i.itemid IN ({item_list})
              AND i.starttime BETWEEN c.intime AND c.outtime
        ),
        filtered AS (
            SELECT stay_id,
                   charttime,
                   amount,
                   amountuom,
                   intime
            FROM r
            WHERE amount IS NOT NULL
              AND amountuom IN ('ml', 'milliliter', 'milliliters')
        ),
        binned AS (
            SELECT stay_id,
                   CAST(FLOOR(date_diff('second', intime, charttime) / 300) AS INTEGER) * 5 AS tbin,
                   amount
            FROM filtered
        )
        SELECT stay_id,
               tbin,
               SUM(amount) AS rbc_amount_ml_event,
               CASE WHEN SUM(amount) > 0 THEN 1 ELSE 0 END AS rbc_transfusion_flag
        FROM binned
        GROUP BY stay_id, tbin
    """
    return query
