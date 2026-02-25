#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
from pathlib import Path
from typing import Set, Tuple

import psycopg2

from src.pipeline_unified import run_pipeline_postgres


# Edit connection settings if needed (prefer environment variables in public repos).
DB_NAME = os.getenv("MIMIC_DB_NAME", "mimiciv")
DB_USER = os.getenv("MIMIC_DB_USER", "uti_user")
DB_HOST = os.getenv("MIMIC_DB_HOST", "localhost")
DB_PORT = os.getenv("MIMIC_DB_PORT", "5432")
DB_OPTIONS = os.getenv("MIMIC_DB_OPTIONS", "-c client_min_messages=error")


def open_pg_conn():
    db_password = os.getenv("MIMIC_DB_PASSWORD")
    if not db_password:
        raise RuntimeError(
            "Missing MIMIC_DB_PASSWORD environment variable. "
            "Set it before running this script."
        )

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=db_password,
        host=DB_HOST,
        port=DB_PORT,
        options=DB_OPTIONS,
    )
    print("Conexao ao banco estabelecida.")
    conn.autocommit = False
    return conn


def fetch_table_index(conn) -> Set[Tuple[str, str]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('mimiciv_hosp', 'mimiciv_icu', 'mimiciv_derived');
            """
        )
        rows = cursor.fetchall()
    return {(r[0], r[1]) for r in rows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the pipeline directly from Postgres with psycopg2")
    parser.add_argument("--out_dir", type=Path, default=Path("outputs"), help="Output directory")
    parser.add_argument("--config_dir", type=Path, default=Path("configs"), help="Config directory")
    parser.add_argument("--max_stays", type=int, default=0, help="Limit number of stays (0 = all)")
    parser.add_argument("--start_step", type=int, default=1, help="First step to run (1-7)")
    parser.add_argument("--end_step", type=int, default=7, help="Last step to run (1-7)")
    parser.add_argument(
        "--resume_from_step",
        type=int,
        default=None,
        help="Use an existing intermediate step as input (e.g. 4 to resume at step 5)",
    )
    parser.add_argument("--compression", type=str, default="zstd", help="Parquet compression (zstd or snappy)")
    parser.add_argument("--max_rows_per_file", type=int, default=1_000_000, help="Max rows per parquet file")
    parser.add_argument("--max_rows_per_group", type=int, default=100_000, help="Max rows per parquet row group")
    parser.add_argument("--debug", action="store_true", help="Debug mode (limit to 500 stays)")

    # Optional HDF5 export
    parser.add_argument("--export_hdf5", action="store_true", help="Export final dataset to a single HDF5 file")
    parser.add_argument("--hdf5_path", type=Path, default=Path("outputs/dataset.h5"), help="HDF5 output path")
    parser.add_argument("--hdf5_key", type=str, default="data", help="HDF5 key (table name)")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = open_pg_conn()
    try:
        tables = fetch_table_index(conn)
        run_pipeline_postgres(
            conn=conn,
            table_index=tables,
            out_dir=args.out_dir,
            config_dir=args.config_dir,
            max_stays=args.max_stays,
            start_step=args.start_step,
            end_step=args.end_step,
            resume_from_step=args.resume_from_step,
            compression=args.compression,
            max_rows_per_file=args.max_rows_per_file,
            max_rows_per_group=args.max_rows_per_group,
            debug=args.debug,
            export_hdf5=args.export_hdf5,
            hdf5_path=args.hdf5_path,
            hdf5_key=args.hdf5_key,
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
