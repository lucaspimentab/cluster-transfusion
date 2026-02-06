# Cluster-Transfusion Pipeline (DuckDB + Python)

This repo contains a fresh pipeline to match transfused vs non-transfused ICU stays and generate temporal embeddings for transfused patients using a timegrid stored in Parquet.

## Requirements

- Python 3.10+
- duckdb
- pandas
- numpy
- pyarrow
- scikit-learn
- torch (optional, required for TS2Vec step)

Install with pip (example):
```
pip install duckdb pandas numpy pyarrow scikit-learn
```
For TS2Vec:
```
pip install torch
```

## Inputs

Expected layout:
```
dataset/
  timegrid_features/          # Parquet dataset (5-min bins) with stay_id + time column
  outputs_outcomes/
    outcomes_by_stay.csv       # optional (contains transfusion flag if present)
    outcomes_by_stay_full.csv  # outcomes (mortality/VM/RRT/vasopressor etc.)
configs/
  lab_itemids.yaml
```

The pipeline auto-detects:
- stay_id
- time column (ex: tbin, charttime_bin, minutes_from_t0)
- transfusion columns (ex: rbc_transfusion_flag, rbc_amount_ml_event)

A schema report is written to outputs/schema_report.txt on every run.

## Outputs

All outputs are organized by run under `outputs/runs/<run_id>/`:
- outputs/runs/<run_id>/outcomes_cohort.parquet
- outputs/runs/<run_id>/outcomes_cohort.csv
- outputs/runs/<run_id>/outcomes_cohort_comparison.csv
- outputs/runs/<run_id>/outcomes_cohort_comparison_psm_w24.csv
- outputs/runs/<run_id>/t0_table.parquet
- outputs/runs/<run_id>/baseline_features_w24.parquet (and w48, w72)
- outputs/runs/<run_id>/embeddings_minirocket_w24.parquet (and w48, w72)
- outputs/runs/<run_id>/embeddings_ts2vec_w24.parquet (and w48, w72)
- outputs/runs/<run_id>/matched_pairs.parquet
- outputs/runs/<run_id>/comparison_transf_vs_ctrl_w24.csv
- outputs/runs/<run_id>/balance_diagnostics_w24.csv
- outputs/runs/<run_id>/embedding_features_w24.csv
- outputs/runs/<run_id>/logs/*.jsonl
- outputs/runs/<run_id>/profiling/*.json

The latest run ID is saved in `outputs/latest_run.txt`.

## How to run

Run the full pipeline for all windows (24/48/72):
```
python scripts/run_all.py
```

Run a single window:
```
python scripts/run_all.py --window 24
```
Use a custom run id:
```
python scripts/run_all.py --window 24 --run_id run_test_001
```
Run with 1:2 matching and larger caliper:
```
python scripts/run_all.py --window 24 --ratio 2 --caliper 0.3
```
Allow reuse of controls (with replacement):
```
python scripts/run_all.py --window 24 --ratio 2 --replace
```

Dry run (schema + counts only):
```
python scripts/run_all.py --window 24 --dry-run
```

Console logs are printed in a human-readable format by default. To switch back to raw JSON:
```
set PRETTY_LOGS=0
```
To enable DuckDB progress bars:
```
set DUCKDB_PROGRESS=1
```
To disable profiling output if your editor locks the file:
```
set DUCKDB_PROFILING=0
```

Run each step manually:
```
python scripts/step0_build_outcomes_cohort.py --run_id run_test_001
python scripts/step0_compare_outcomes.py --run_id run_test_001
python scripts/step0_compare_outcomes_psm.py --window 24 --run_id run_test_001
python scripts/step1_build_baseline_features.py --window 24
python scripts/step2_match_controls.py --window 24
python scripts/step4_embed_minirocket_temporal.py --window 24
python scripts/step4_embed_ts2vec_temporal.py --window 24
python scripts/step5_reports.py --window 24
```

Embedding options (Step 4):
```
python scripts/step4_embed_minirocket_temporal.py --window 24 --embed_max_features 60
python scripts/step4_embed_minirocket_temporal.py --window 24 --embed_use_all
python scripts/step4_embed_minirocket_temporal.py --window 24 --embed_missing_threshold 0.8
python scripts/step4_embed_ts2vec_temporal.py --window 24 --embed_use_all --ts2vec_epochs 5
```

## Notes

- The pipeline never loads the full timegrid into pandas. It uses DuckDB read_parquet with pushdown and only materializes aggregated tables in pandas.
- If no transfusion column is found, Step 1 will stop and ask for a transfusion flag to be added.
- Controls get a pseudo-T0 based on the median offset of transfused patients to allow pre-window comparisons.
- The temporal embedding steps include MiniRocketLite and TS2VecLite (PyTorch required for TS2Vec).

## Interpretation

- baseline_features_w*.parquet: pre-T0 summary features per stay, including selected deltas for SOFA/labs.
- matched_pairs.parquet: 1:1 matching pairs with propensity scores and distances.
- balance_diagnostics_w*.csv: standardized mean differences before/after matching.
- comparison_transf_vs_ctrl_w*.csv: outcomes comparison in matched cohorts.
- embeddings_minirocket_w*.parquet: temporal embeddings (MiniRocket) for transfused stays.
- embeddings_ts2vec_w*.parquet: temporal embeddings (TS2VecLite) for transfused stays.
