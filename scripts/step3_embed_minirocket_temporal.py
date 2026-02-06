from __future__ import annotations

import argparse
import warnings

import numpy as np
import pandas as pd

from lib.duckdb_utils import connect_duckdb, enable_profiling
from lib.embedding import MiniRocketLite, build_time_series_matrix
from lib.embedding_features import compute_missing_rates, select_embedding_columns
from lib.logging_utils import Timer, log_event, setup_logging
from lib.schema_utils import get_columns, infer_id_column, infer_time_column, infer_time_step_minutes, time_expression
from lib.utils import repo_root, resolve_outputs_dir, resolve_shared_dir


def parse_args():
    parser = argparse.ArgumentParser(description="Step 3: Embed transfused (MiniRocket, temporal)")
    parser.add_argument("--run_id", type=str, default=None)
    parser.add_argument("--window", type=int, default=24, choices=[24, 48, 72])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit_stays", type=int, default=None)
    parser.add_argument("--embed_max_features", type=int, default=0)
    parser.add_argument("--embed_use_all", action="store_true")
    parser.add_argument("--embed_missing_threshold", type=float, default=0.8)
    parser.add_argument("--batch_size", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    warnings.filterwarnings("once", message="Skipping features without any observed values", category=UserWarning)
    args = parse_args()
    root = repo_root()
    window_dir = resolve_outputs_dir(root, args.run_id, window=args.window)
    embeddings_dir = resolve_outputs_dir(root, args.run_id, window=args.window, subdir="embeddings")
    shared_dir = resolve_shared_dir(root, args.run_id)

    logger, _ = setup_logging("step3_embed_minirocket_temporal", window_dir)

    t0_path = shared_dir / "t0_table.parquet"
    if not t0_path.exists():
        raise FileNotFoundError("Missing t0_table.parquet; run step1 first.")

    timegrid_glob = (root / "dataset" / "timegrid_features" / "*" / "*.parquet").as_posix()
    timegrid_rel = f"read_parquet('{timegrid_glob}', union_by_name=true)"

    con = connect_duckdb()
    profile_path = window_dir / "profiling" / "step3_embed_minirocket_temporal.json"
    enable_profiling(con, profile_path, logger=logger)

    timegrid_cols = get_columns(con, timegrid_rel)
    timegrid_names = [c[0] for c in timegrid_cols]
    stay_col = infer_id_column(timegrid_names)
    time_col = infer_time_column(timegrid_names)
    time_type = None
    for name, dtype in timegrid_cols:
        if name == time_col:
            time_type = dtype
            break

    if not stay_col or not time_col or not time_type:
        raise RuntimeError("Could not infer stay_id or time column.")

    time_expr = time_expression(time_col, time_type, alias="tg")
    feature_cols = select_embedding_columns(
        timegrid_cols,
        args.embed_max_features,
        args.embed_use_all,
        exclude_cols=[stay_col, time_col],
    )
    if not feature_cols:
        raise RuntimeError("No embedding features detected in timegrid schema.")

    log_event(
        logger,
        "embedding_features_candidate",
        n_features=len(feature_cols),
        max_features=args.embed_max_features,
        use_all=args.embed_use_all,
    )

    window_minutes = int(args.window * 60)
    inferred_step = infer_time_step_minutes(con, f"{timegrid_rel} tg", time_expr)
    step_minutes = int(inferred_step) if inferred_step else 5
    if step_minutes <= 0:
        step_minutes = 5
    log_event(logger, "embedding_time_step", step_minutes=step_minutes)

    threshold = min(max(args.embed_missing_threshold, 0.0), 1.0)
    t0_rel = f"read_parquet('{t0_path.as_posix()}')"
    missing_rates = compute_missing_rates(
        con, timegrid_rel, t0_rel, stay_col, time_expr, feature_cols, window_minutes
    )
    features_df = pd.DataFrame(
        {"feature": feature_cols, "missing_rate": [missing_rates.get(c, 1.0) for c in feature_cols]}
    )
    features_df["selected"] = features_df["missing_rate"] <= threshold
    features_path = embeddings_dir / "embedding_features.csv"
    features_df.sort_values("missing_rate").to_csv(features_path, index=False)

    selected_cols = features_df[features_df["selected"]]["feature"].tolist()
    if not selected_cols:
        raise RuntimeError("All embedding features were filtered out by missing threshold.")
    log_event(
        logger,
        "embedding_features_selected",
        n_features=len(selected_cols),
        missing_threshold=threshold,
        features_path=str(features_path),
    )

    if args.dry_run:
        log_event(logger, "dry_run", n_features=len(selected_cols), window=args.window)
        return

    t0_df = con.execute(f"SELECT stay_id, t0, transfused FROM read_parquet('{t0_path.as_posix()}')").df()
    t0_df["t0"] = pd.to_numeric(t0_df["t0"], errors="coerce").astype(float)
    transfused = t0_df[t0_df["transfused"] == 1]
    if args.limit_stays:
        transfused = transfused.sample(n=min(args.limit_stays, len(transfused)), random_state=args.seed)
    if transfused.empty:
        log_event(logger, "no_transfused_stays")
        return

    stay_ids = transfused["stay_id"].tolist()
    t0_map = dict(zip(transfused["stay_id"].tolist(), transfused["t0"].tolist()))

    embedder = MiniRocketLite(n_kernels=256, random_state=args.seed)

    batch_size = max(1, int(args.batch_size))
    embedding_rows = []

    with Timer(logger, "embedding_batches"):
        for i in range(0, len(stay_ids), batch_size):
            batch_ids = stay_ids[i : i + batch_size]
            batch_df = pd.DataFrame({"stay_id": batch_ids})
            con.register("batch_ids", batch_df)

            query = f"""
            SELECT
                tg.\"{stay_col}\" AS stay_id,
                {time_expr} AS time_min,
                {', '.join([f'tg."{c}"' for c in selected_cols])}
            FROM {timegrid_rel} tg
            JOIN batch_ids b ON tg.\"{stay_col}\" = b.stay_id
            JOIN {t0_rel} t0 ON tg.\"{stay_col}\" = t0.stay_id
            WHERE t0.transfused = 1
              AND {time_expr} BETWEEN t0.t0 - {window_minutes} AND t0.t0
            """
            data = con.execute(query).fetch_arrow_table().to_pandas()

            series_list = []
            valid_ids = []
            for stay_id in batch_ids:
                t0 = t0_map.get(stay_id)
                if t0 is None:
                    continue
                mat = build_time_series_matrix(data, stay_id, t0, window_minutes, step_minutes, selected_cols)
                series_list.append(mat)
                valid_ids.append(stay_id)

            if not series_list:
                continue

            X = np.stack(series_list, axis=0)
            if i == 0:
                embedder.fit(X)
            emb = embedder.transform(X)

            batch_out = pd.DataFrame(emb, columns=[f"emb_{j}" for j in range(emb.shape[1])])
            batch_out.insert(0, "stay_id", valid_ids)
            embedding_rows.append(batch_out)

            log_event(logger, "embedding_batch", batch_start=i, batch_size=len(valid_ids))

    if not embedding_rows:
        log_event(logger, "embedding_failed", reason="no_embeddings")
        return

    embeddings = pd.concat(embedding_rows, ignore_index=True)
    embed_path = embeddings_dir / "embeddings_minirocket.parquet"
    dup = int(embeddings["stay_id"].duplicated().sum())
    log_event(logger, "embedding_counts", n_rows=len(embeddings), n_stays=embeddings["stay_id"].nunique(), duplicate_stay_rows=dup)
    con.register("embeddings_src", embeddings)
    con.execute(f"COPY (SELECT * FROM embeddings_src) TO '{embed_path.as_posix()}' (FORMAT PARQUET)")

    log_event(logger, "step3_complete", embed_path=str(embed_path))


if __name__ == "__main__":
    main()
