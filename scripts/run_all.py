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
    report_args = base_args[:] + ["--embedding", "minirocket", "--k_list", K_LIST, "--seed", str(SEED)]

    run_step(scripts_dir / "step0_build_outcomes_cohort.py", ["--run_id", RUN_ID], env=env)
    run_step(scripts_dir / "step1_build_baseline_features.py", base_args, env=env)
    run_step(scripts_dir / "step2_match_controls.py", match_args, env=env)
    run_step(scripts_dir / "step3_embed_minirocket_temporal.py", embed_args, env=env)
    run_step(scripts_dir / "step4_reports.py", report_args, env=env)


if __name__ == "__main__":
    main()
