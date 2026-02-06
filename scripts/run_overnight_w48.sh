#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
timestamp="$(date +"%Y%m%d_%H%M%S")"
logPath="$root/outputs/overnight_w48_${timestamp}.log"

run_window() {
  local run_id="$1"
  local replace_flag="$2"
  echo "=== RUN: ${run_id} ==="
  python "$root/scripts/step0_build_outcomes_cohort.py" --run_id "$run_id"
  python "$root/scripts/step1_build_baseline_features.py" --window 48 --run_id "$run_id"
  python "$root/scripts/step2_match_controls.py" --window 48 --run_id "$run_id" --caliper 0.3 "$replace_flag"
  python "$root/scripts/step3_embed_minirocket_temporal.py" --window 48 --run_id "$run_id"
  python "$root/scripts/step4_embed_ts2vec_temporal.py" --window 48 --run_id "$run_id" --ts2vec_epochs 5
  python "$root/scripts/step5_reports.py" --window 48 --run_id "$run_id" --embedding minirocket
  python "$root/scripts/step5_reports.py" --window 48 --run_id "$run_id" --embedding ts2vec
}

{
  run_window "run_cal03_replace_full_w48" "--replace"
  run_window "run_cal03_noreplace_w48" "--no-replace"
} 2>&1 | tee -a "$logPath"
