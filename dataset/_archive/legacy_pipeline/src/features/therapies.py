from __future__ import annotations

from typing import Dict, Optional

import duckdb

from ..io_utils import find_table_path, duckdb_relation


# -----------------------------------------------------------------------------
# CONFIG: quais vasopressores entram no dataset
# -----------------------------------------------------------------------------
# Núcleo pedido pelo coordenador:
# - norepinephrine, epinephrine, dobutamine, vasopressin
#
# Phenylephrine:
# - seu CSV mostrou cobertura alta (~23.9% dos stays), então pode ser útil.
# - deixe True se quiser manter; False se quiser seguir estritamente "4 drogas".
INCLUDE_PHENYLEPHRINE = False

# Dopamine e milrinone:
# - removidos (raros / pouco úteis para seu objetivo e aumentam sparsity)
# - se quiser reativar, adicione aqui de volta.
VASO_TABLES: dict[str, str] = {
    "norepinephrine": "norepinephrine_rate_mcgkgmin",
    "epinephrine": "epinephrine_rate_mcgkgmin",
    "dobutamine": "dobutamine_rate_mcgkgmin",
    "vasopressin": "vasopressin_rate_unitsmin_or_equiv",
    "phenylephrine": "phenylephrine_rate_mcgkgmin",
}


def vasopressor_subqueries(con: duckdb.DuckDBPyConnection, data_dir) -> Dict[str, str]:
    """
    Gera subqueries DuckDB para cada vasopressor selecionado.
    Saída de cada subquery: stay_id, tbin (em minutos, múltiplos de 5), <col_rate>

    Regras:
    - Filtra eventos que intersectam a janela [intime, outtime] do cohort
    - Bina em 5 minutos (300s) e usa "step function":
        * starttime -> rate
        * endtime   -> 0 no próximo bin (tbin_end + 5)
    - Usa arg_max(rate, event_time) para resolver múltiplos eventos no mesmo tbin
    """
    subqueries: Dict[str, str] = {}

    for table, col in VASO_TABLES.items():
        path = find_table_path(data_dir, "mimiciv_derived", table)
        if not path:
            continue

        con.register(table, duckdb_relation(con, path))

        query = f"""
            WITH v AS (
                SELECT
                    v.stay_id,
                    v.starttime,
                    v.endtime,
                    COALESCE(v.vaso_rate, v.vaso_amount) AS rate
                FROM {table} v
                JOIN cohort c USING (stay_id)
                WHERE v.starttime <= c.outtime
                  AND v.endtime   >= c.intime
            ),
            events AS (
                -- evento de "liga" na dose
                SELECT
                    v.stay_id,
                    CAST(FLOOR(date_diff('second', c.intime, v.starttime) / 300) AS INTEGER) * 5 AS tbin,
                    v.starttime AS event_time,
                    v.rate AS rate
                FROM v
                JOIN cohort c USING (stay_id)

                UNION ALL

                -- evento de "desliga": zera no bin seguinte ao end
                SELECT
                    v.stay_id,
                    CAST(FLOOR(date_diff('second', c.intime, v.endtime) / 300) AS INTEGER) * 5 + 5 AS tbin,
                    v.endtime + INTERVAL '1 second' AS event_time,
                    0.0 AS rate
                FROM v
                JOIN cohort c USING (stay_id)
                WHERE v.endtime IS NOT NULL
            )
            SELECT
                stay_id,
                tbin,
                arg_max(rate, event_time) AS {col}
            FROM events
            WHERE tbin >= 0
            GROUP BY stay_id, tbin
        """
        subqueries[col] = query

    return subqueries


def ventilation_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    """
    Ventilação:
    - ventilation_on: 1 se status não contém none/off
    - imv_on: 1 se invasiva/ett/trach
    - tracheostomy_flag: 1 se trach
    Mesma lógica de step function: start -> flags, end -> zera.
    """
    path = find_table_path(data_dir, "mimiciv_derived", "ventilation")
    if not path:
        return None

    con.register("ventilation", duckdb_relation(con, path))

    query = """
        WITH v AS (
            SELECT
                v.stay_id,
                v.starttime,
                v.endtime,
                LOWER(v.ventilation_status) AS status
            FROM ventilation v
            JOIN cohort c USING (stay_id)
            WHERE v.starttime <= c.outtime
              AND v.endtime   >= c.intime
        ),
        events AS (
            SELECT
                v.stay_id,
                CAST(FLOOR(date_diff('second', c.intime, v.starttime) / 300) AS INTEGER) * 5 AS tbin,
                v.starttime AS event_time,

                CASE
                    WHEN status LIKE '%none%' OR status LIKE '%off%' THEN 0
                    ELSE 1
                END AS ventilation_on,

                CASE
                    WHEN status LIKE '%invasive%' OR status LIKE '%ett%' OR status LIKE '%trach%' THEN 1
                    ELSE 0
                END AS imv_on,

                CASE
                    WHEN status LIKE '%trach%' THEN 1
                    ELSE 0
                END AS tracheostomy_flag
            FROM v
            JOIN cohort c USING (stay_id)

            UNION ALL

            SELECT
                v.stay_id,
                CAST(FLOOR(date_diff('second', c.intime, v.endtime) / 300) AS INTEGER) * 5 + 5 AS tbin,
                v.endtime + INTERVAL '1 second' AS event_time,
                0 AS ventilation_on,
                0 AS imv_on,
                0 AS tracheostomy_flag
            FROM v
            JOIN cohort c USING (stay_id)
            WHERE v.endtime IS NOT NULL
        )
        SELECT
            stay_id,
            tbin,
            arg_max(ventilation_on, event_time) AS ventilation_on,
            arg_max(imv_on, event_time) AS imv_on,
            arg_max(tracheostomy_flag, event_time) AS tracheostomy_flag
        FROM events
        WHERE tbin >= 0
        GROUP BY stay_id, tbin
    """
    return query


def rrt_subquery(con: duckdb.DuckDBPyConnection, data_dir) -> Optional[str]:
    """
    RRT:
    - rrt_on: COALESCE(dialysis_active, dialysis_present, 0)
    - rrt_type: COALESCE(dialysis_type, 'unknown')
    Aqui é "snapshot" por charttime (não tem start/end), então binning direto no charttime.
    """
    path = find_table_path(data_dir, "mimiciv_derived", "rrt")
    if not path:
        return None

    con.register("rrt", duckdb_relation(con, path))

    query = """
        WITH r AS (
            SELECT
                r.stay_id,
                r.charttime,
                COALESCE(r.dialysis_active, r.dialysis_present, 0) AS rrt_on,
                COALESCE(r.dialysis_type, 'unknown') AS rrt_type
            FROM rrt r
            JOIN cohort c USING (stay_id)
            WHERE r.charttime BETWEEN c.intime AND c.outtime
        ),
        binned AS (
            SELECT
                stay_id,
                CAST(FLOOR(date_diff('second', c.intime, r.charttime) / 300) AS INTEGER) * 5 AS tbin,
                r.charttime,
                r.rrt_on,
                r.rrt_type
            FROM r
            JOIN cohort c USING (stay_id)
        )
        SELECT
            stay_id,
            tbin,
            arg_max(rrt_on, charttime)  AS rrt_on,
            arg_max(rrt_type, charttime) AS rrt_type
        FROM binned
        WHERE tbin >= 0
        GROUP BY stay_id, tbin
    """
    return query
