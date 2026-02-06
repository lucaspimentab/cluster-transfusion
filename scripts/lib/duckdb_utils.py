from __future__ import annotations

import os
from pathlib import Path

import duckdb


def connect_duckdb(db_path=":memory:", threads: int | None = None, memory_limit="8GB", enable_progress: bool | None = None):
    con = duckdb.connect(database=str(db_path))
    if threads:
        con.execute(f"PRAGMA threads={int(threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA enable_object_cache=true")
    if enable_progress is None:
        enable_progress = os.getenv("DUCKDB_PROGRESS", "0") == "1"
    con.execute(f"PRAGMA enable_progress_bar={'true' if enable_progress else 'false'}")
    return con


def enable_profiling(con, profile_path: Path, logger=None):
    if os.getenv("DUCKDB_PROFILING", "1") == "0":
        if logger:
            from .logging_utils import log_event

            log_event(logger, "profiling_disabled", reason="env")
        return False

    profile_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(profile_path, "a", encoding="utf-8"):
            pass
    except Exception as exc:
        if logger:
            from .logging_utils import log_event

            log_event(logger, "profiling_disabled", reason="permission", error=str(exc))
        return False

    con.execute("PRAGMA enable_profiling='json'")
    con.execute(f"PRAGMA profiling_output='{profile_path.as_posix()}'")
    if logger:
        from .logging_utils import log_event

        log_event(logger, "profiling_enabled", path=str(profile_path))
    return True
