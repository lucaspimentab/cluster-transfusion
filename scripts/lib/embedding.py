from __future__ import annotations

from typing import List, Tuple

import numpy as np
import pandas as pd


class MiniRocketLite:
    def __init__(self, n_kernels: int = 256, kernel_lengths: Tuple[int, ...] = (7, 9, 11), random_state: int = 42):
        self.n_kernels = n_kernels
        self.kernel_lengths = kernel_lengths
        self.random_state = random_state
        self.kernels = []
        self.biases = None

    def fit(self, X: np.ndarray):
        rng = np.random.default_rng(self.random_state)
        n_channels = X.shape[1]
        self.kernels = []
        for _ in range(self.n_kernels):
            length = int(rng.choice(self.kernel_lengths))
            n_ch = int(rng.integers(1, min(3, n_channels) + 1))
            channels = rng.choice(n_channels, size=n_ch, replace=False)
            weights = rng.choice([-1.0, 1.0], size=length)
            self.kernels.append((channels, weights))

        sample_size = min(32, X.shape[0])
        sample_idx = rng.choice(X.shape[0], size=sample_size, replace=False)
        biases = []
        for channels, weights in self.kernels:
            vals = []
            for idx in sample_idx:
                series = np.nanmean(X[idx, channels, :], axis=0)
                conv = np.convolve(series, weights, mode="valid")
                vals.append(np.mean(conv) if conv.size else 0.0)
            biases.append(float(np.median(vals)) if vals else 0.0)
        self.biases = np.array(biases)
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        n_samples = X.shape[0]
        feats = np.zeros((n_samples, len(self.kernels)), dtype=float)
        for k, ((channels, weights), bias) in enumerate(zip(self.kernels, self.biases)):
            for i in range(n_samples):
                series = np.nanmean(X[i, channels, :], axis=0)
                conv = np.convolve(series, weights, mode="valid")
                if conv.size == 0:
                    feats[i, k] = 0.0
                else:
                    feats[i, k] = float(np.mean(conv > bias))
        return feats


def build_time_series_matrix(
    df: pd.DataFrame,
    stay_id: int,
    t0: float,
    window_minutes: int,
    step_minutes: int,
    feature_cols: List[str],
) -> np.ndarray:
    n_steps = int(window_minutes / step_minutes)
    if n_steps < 1:
        n_steps = 1
    try:
        t0_val = float(t0)
    except Exception:
        t0_val = float(pd.to_numeric(pd.Series([t0]), errors="coerce").iloc[0])
    start = t0_val - window_minutes
    sub = df[df["stay_id"] == stay_id].copy()
    if sub.empty:
        return np.zeros((len(feature_cols), n_steps), dtype=float)

    sub["time_min"] = pd.to_numeric(sub["time_min"], errors="coerce").astype(float)
    sub = sub[(sub["time_min"] >= start) & (sub["time_min"] < t0_val)]
    if sub.empty:
        return np.zeros((len(feature_cols), n_steps), dtype=float)

    sub["bin"] = ((sub["time_min"] - start) / step_minutes).astype(int)
    sub = sub[(sub["bin"] >= 0) & (sub["bin"] < n_steps)]
    if sub.empty:
        return np.zeros((len(feature_cols), n_steps), dtype=float)

    grouped = sub.groupby("bin")[feature_cols].mean()
    mat = np.full((len(feature_cols), n_steps), np.nan)
    for bin_idx, row in grouped.iterrows():
        mat[:, int(bin_idx)] = row.values

    mat = pd.DataFrame(mat.T).ffill().bfill().fillna(0.0).T.values
    return mat
