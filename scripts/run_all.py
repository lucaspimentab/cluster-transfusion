from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from lib.utils import repo_root, write_latest_run

# Pipeline fixo para reproduzir os resultados finais atuais (MiniRocket).
RUN_ID = "run_cal03_replace_full_w48"
WINDOW = 48
SEED = 42
CALIPER = 0.3
RATIO = 1
REPLACE = True
K_LIST = "2,3,4,5,6"
SCAN_FEATURE_SET = "all"
SCAN_DROP_BASES = "apsiii"
SCAN_MIN_PAIRS = 200
SCAN_MIN_ABS_DIFF_PP = 15.0
SCAN_MIN_PREV = 0.05
SCAN_MAX_PREV = 0.40
SCAN_MIN_DEPTH = 2
SCAN_MAX_DEPTH = 3
SCAN_MIN_AXES = 2
SCAN_POOL_SIZE = 250
SCAN_POOL_MODE = "balanced"
SCAN_BEAM = 60
SCAN_BOOT_TOP = 250
SCAN_BOOT_ITERS = 80
SCAN_BOOT_STAB = 0.70
CLUSTER_K_ANNOT = 2
TOP_GROUPS_CROSS = 40


def run_step(script: Path, args_list: list[str], env: dict | None = None) -> None:
    cmd = [sys.executable, script.as_posix()] + args_list
    subprocess.check_call(cmd, env=env)


def main() -> None:
    root = repo_root()
    scripts_dir = root / "scripts"

    env = os.environ.copy()
    env["RUN_ID"] = RUN_ID
    write_latest_run(root, RUN_ID)

    base_args = ["--run_id", RUN_ID, "--window", str(WINDOW)]
    match_args = base_args[:] + [
        "--seed",
        str(SEED),
        "--caliper",
        str(CALIPER),
        "--ratio",
        str(RATIO),
        "--replace" if REPLACE else "--no-replace",
    ]
    embed_args = base_args[:] + ["--seed", str(SEED)]
    report_args = base_args[:] + [
        "--embedding",
        "minirocket",
        "--k_list",
        K_LIST,
        "--seed",
        str(SEED),
        "--outputs_mode",
        "minimal",
        "--run_scan_suite",
        "--scan_feature_set",
        SCAN_FEATURE_SET,
        "--scan_drop_bases",
        SCAN_DROP_BASES,
        "--scan_min_pairs",
        str(SCAN_MIN_PAIRS),
        "--scan_min_abs_diff_pp",
        str(SCAN_MIN_ABS_DIFF_PP),
        "--scan_min_prevalence",
        str(SCAN_MIN_PREV),
        "--scan_max_prevalence",
        str(SCAN_MAX_PREV),
        "--scan_min_depth",
        str(SCAN_MIN_DEPTH),
        "--scan_max_depth",
        str(SCAN_MAX_DEPTH),
        "--scan_min_axes",
        str(SCAN_MIN_AXES),
        "--scan_pool_size",
        str(SCAN_POOL_SIZE),
        "--scan_pool_mode",
        SCAN_POOL_MODE,
        "--scan_beam_width",
        str(SCAN_BEAM),
        "--scan_top_bootstrap",
        str(SCAN_BOOT_TOP),
        "--scan_bootstrap_iters",
        str(SCAN_BOOT_ITERS),
        "--scan_bootstrap_min_stability",
        str(SCAN_BOOT_STAB),
        "--scan_cluster_k",
        str(CLUSTER_K_ANNOT),
        "--scan_top_groups_cross",
        str(TOP_GROUPS_CROSS),
    ]

    run_step(scripts_dir / "step0_build_outcomes_cohort.py", ["--run_id", RUN_ID], env=env)
    run_step(scripts_dir / "step1_build_baseline_features.py", base_args, env=env)
    run_step(scripts_dir / "step2_match_controls.py", match_args, env=env)
    run_step(scripts_dir / "step3_embed_minirocket_temporal.py", embed_args, env=env)
    run_step(scripts_dir / "step4_reports.py", report_args, env=env)


if __name__ == "__main__":
    main()
