from __future__ import annotations

from typing import Optional

import duckdb

from ..io_utils import find_table_path, duckdb_relation


def sofa_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    path = find_table_path(data_dir, "mimiciv_derived", "sofa")
    if not path:
        return None
    con.register("sofa", duckdb_relation(con, path))
    query = """
        WITH s AS (
            SELECT s.stay_id,
                   s.starttime,
                   s.sofa_24hours AS sofa
            FROM sofa s
            JOIN cohort c USING (stay_id)
            WHERE s.starttime BETWEEN c.intime AND c.outtime
        ),
        binned AS (
            SELECT stay_id,
                   CAST(FLOOR(date_diff('second', c.intime, s.starttime) / 300) AS INTEGER) * 5 AS tbin,
                   s.starttime,
                   s.sofa
            FROM s
            JOIN cohort c USING (stay_id)
        )
        SELECT stay_id,
               tbin,
               arg_max(sofa, starttime) AS sofa
        FROM binned
        GROUP BY stay_id, tbin
    """
    return query


def sapsii_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    path = find_table_path(data_dir, "mimiciv_derived", "sapsii")
    if not path:
        return None
    con.register("sapsii", duckdb_relation(con, path))
    return "SELECT stay_id, MAX(sapsii) AS sapsii FROM sapsii GROUP BY stay_id"


def apsiii_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    path = find_table_path(data_dir, "mimiciv_derived", "apsiii")
    if not path:
        return None
    con.register("apsiii", duckdb_relation(con, path))
    return "SELECT stay_id, MAX(apsiii) AS apsiii FROM apsiii GROUP BY stay_id"
