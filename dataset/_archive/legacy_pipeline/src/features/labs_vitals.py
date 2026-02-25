from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from ..config import LabItemConfig
from ..io_utils import find_table_path, duckdb_relation
from ..inventory import Inventory


@dataclass
class LabMappingResult:
    itemids_by_lab: Dict[str, List[int]]
    itemid_map_rows: List[Dict[str, str]]


VITAL_PATTERNS = {
    "heart_rate": ["heart rate"],
    "sbp": ["systolic blood pressure", "sbp"],
    "dbp": ["diastolic blood pressure", "dbp"],
    "mbp": ["mean blood pressure", "mbp", "map"],
    "resp_rate": ["respiratory rate"],
    "temperature": ["temperature"],
    "spo2": ["spo2", "sao2"],
    "glucose_vital": ["glucose"],
}


def resolve_lab_itemids(
    con: duckdb.DuckDBPyConnection,
    data_dir,
    inventory: Inventory,
    lab_items: Dict[str, LabItemConfig],
) -> LabMappingResult:
    d_labitems_path = find_table_path(data_dir, "mimiciv_hosp", "d_labitems")
    df_lab: Optional[pd.DataFrame] = None
    if d_labitems_path:
        con.register("d_labitems", duckdb_relation(con, d_labitems_path))
        df_lab = con.sql("SELECT itemid, label, fluid FROM d_labitems").df()
    if df_lab is not None and "label" in df_lab.columns:
        df_lab["label"] = df_lab["label"].astype(str)

    mapping: Dict[str, List[int]] = {}
    map_rows: List[Dict[str, str]] = []
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
        for itemid in mapping[lab_name]:
            map_rows.append({"lab": lab_name, "itemid": str(itemid), "source": "config_or_lookup"})

    return LabMappingResult(itemids_by_lab=mapping, itemid_map_rows=map_rows)


def resolve_vital_itemids(
    con: duckdb.DuckDBPyConnection,
    data_dir,
    inventory: Inventory,
) -> Dict[str, List[int]]:
    d_items_path = find_table_path(data_dir, "mimiciv_icu", "d_items")
    df_items: Optional[pd.DataFrame] = None
    if d_items_path:
        con.register("d_items", duckdb_relation(con, d_items_path))
        df_items = con.sql("SELECT itemid, label FROM d_items").df()
    if df_items is None:
        return {k: [] for k in VITAL_PATTERNS}
    if "label" in df_items.columns:
        df_items["label"] = df_items["label"].astype(str)

    mapping: Dict[str, List[int]] = {}
    for vital, patterns in VITAL_PATTERNS.items():
        mask = pd.Series(False, index=df_items.index)
        for pat in patterns:
            mask = mask | df_items["label"].str.contains(pat, case=False, regex=True, na=False)
        itemids = df_items.loc[mask, "itemid"].dropna().unique().tolist()
        mapping[vital] = [int(x) for x in itemids]

    return mapping


def labs_subquery(itemids_by_lab: Dict[str, List[int]]) -> Tuple[str, List[int]]:
    all_itemids = sorted({item for items in itemids_by_lab.values() for item in items})
    if not all_itemids:
        return "SELECT NULL::INTEGER AS stay_id, NULL::INTEGER AS tbin WHERE FALSE", []

    select_exprs = []
    for lab_name, items in itemids_by_lab.items():
        if not items:
            select_exprs.append(f"NULL::DOUBLE AS {lab_name}")
            continue
        item_list = ",".join(str(i) for i in items)
        expr = f"arg_max(valuenum, charttime) FILTER (WHERE itemid IN ({item_list})) AS {lab_name}"
        select_exprs.append(expr)

    select_clause = ",\n               ".join(select_exprs)

    query = f"""
        WITH labs AS (
            SELECT l.subject_id,
                   l.hadm_id,
                   l.charttime,
                   l.itemid,
                   l.valuenum,
                   c.stay_id,
                   c.intime
            FROM labevents l
            JOIN cohort c
              ON l.subject_id = c.subject_id
             AND l.hadm_id = c.hadm_id
             AND l.charttime BETWEEN c.intime AND c.outtime
            WHERE l.itemid IN ({",".join(str(i) for i in all_itemids)})
        ),
        binned AS (
            SELECT stay_id,
                   CAST(FLOOR(date_diff('second', intime, charttime) / 300) AS INTEGER) * 5 AS tbin,
                   itemid,
                   valuenum,
                   charttime
            FROM labs
        )
        SELECT stay_id,
               tbin,
               {select_clause}
        FROM binned
        GROUP BY stay_id, tbin
    """
    return query, all_itemids


def vitals_subquery(use_derived: bool, vital_itemids: Dict[str, List[int]]) -> str:
    if use_derived:
        return """
            WITH v AS (
                SELECT v.stay_id,
                       v.charttime,
                       v.heart_rate,
                       COALESCE(v.sbp, v.sbp_ni) AS sbp,
                       COALESCE(v.dbp, v.dbp_ni) AS dbp,
                       COALESCE(v.mbp, v.mbp_ni) AS mbp,
                       v.resp_rate,
                       v.temperature,
                       v.spo2,
                       v.glucose
                FROM vitalsign v
                JOIN cohort c USING (stay_id)
                WHERE v.charttime BETWEEN c.intime AND c.outtime
            ),
            binned AS (
                SELECT stay_id,
                       CAST(FLOOR(date_diff('second', c.intime, v.charttime) / 300) AS INTEGER) * 5 AS tbin,
                       v.charttime,
                       v.heart_rate,
                       v.sbp,
                       v.dbp,
                       v.mbp,
                       v.resp_rate,
                       v.temperature,
                       v.spo2,
                       v.glucose
                FROM v
                JOIN cohort c USING (stay_id)
            )
            SELECT stay_id,
                   tbin,
                   arg_max(heart_rate, charttime) AS heart_rate,
                   arg_max(sbp, charttime) AS sbp,
                   arg_max(dbp, charttime) AS dbp,
                   arg_max(mbp, charttime) AS mbp,
                   arg_max(resp_rate, charttime) AS resp_rate,
                   arg_max(temperature, charttime) AS temperature,
                   arg_max(spo2, charttime) AS spo2,
                   arg_max(glucose, charttime) AS glucose_vital
            FROM binned
            GROUP BY stay_id, tbin
        """

    all_itemids = sorted({item for items in vital_itemids.values() for item in items})
    if not all_itemids:
        return "SELECT NULL::INTEGER AS stay_id, NULL::INTEGER AS tbin WHERE FALSE"

    select_exprs = []
    for vital_name, items in vital_itemids.items():
        if not items:
            select_exprs.append(f"NULL::DOUBLE AS {vital_name}")
            continue
        item_list = ",".join(str(i) for i in items)
        expr = f"arg_max(valuenum, charttime) FILTER (WHERE itemid IN ({item_list})) AS {vital_name}"
        select_exprs.append(expr)

    select_clause = ",\n               ".join(select_exprs)

    query = f"""
        WITH v AS (
            SELECT c.stay_id,
                   ce.charttime,
                   ce.itemid,
                   ce.valuenum,
                   c.intime
            FROM chartevents ce
            JOIN cohort c ON ce.stay_id = c.stay_id
            WHERE ce.itemid IN ({",".join(str(i) for i in all_itemids)})
              AND ce.charttime BETWEEN c.intime AND c.outtime
        ),
        binned AS (
            SELECT stay_id,
                   CAST(FLOOR(date_diff('second', intime, charttime) / 300) AS INTEGER) * 5 AS tbin,
                   itemid,
                   valuenum,
                   charttime
            FROM v
        )
        SELECT stay_id,
               tbin,
               {select_clause}
        FROM binned
        GROUP BY stay_id, tbin
    """
    return query
