from __future__ import annotations

import math
from typing import Iterable, Tuple

import numpy as np

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
except Exception as exc:  # pragma: no cover - handled at runtime
    torch = None
    nn = None
    F = None


def check_torch_available():
    if torch is None:
        raise ImportError("PyTorch is required for TS2Vec. Install with: pip install torch")


def random_crop(x: "torch.Tensor", min_ratio: float = 0.5) -> "torch.Tensor":
    # x: (B, C, T)
    b, c, t = x.shape
    crop_len = int(max(2, math.floor(t * (min_ratio + (1 - min_ratio) * torch.rand(1).item()))))
    start = int(torch.randint(0, max(1, t - crop_len + 1), (1,)).item())
    cropped = x[:, :, start : start + crop_len]
    if crop_len == t:
        return cropped
    pad_len = t - crop_len
    pad = torch.zeros((b, c, pad_len), device=x.device, dtype=x.dtype)
    return torch.cat([cropped, pad], dim=2)


def nt_xent_loss(z1: "torch.Tensor", z2: "torch.Tensor", temperature: float = 0.2) -> "torch.Tensor":
    z1 = F.normalize(z1, dim=1)
    z2 = F.normalize(z2, dim=1)
    reps = torch.cat([z1, z2], dim=0)  # (2B, D)
    sim = reps @ reps.T
    sim = sim / temperature
    b = z1.shape[0]
    sim.fill_diagonal_(-1e9)
    labels = torch.arange(2 * b, device=sim.device)
    labels[:b] = labels[:b] + b
    labels[b:] = labels[b:] - b
    loss = F.cross_entropy(sim, labels)
    return loss


class TS2VecLite(nn.Module):
    def __init__(self, in_channels: int, hidden_channels: int = 64, embedding_dim: int = 128):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Conv1d(in_channels, hidden_channels, kernel_size=5, padding=2),
            nn.ReLU(),
            nn.Conv1d(hidden_channels, hidden_channels, kernel_size=3, padding=1),
            nn.ReLU(),
            nn.Conv1d(hidden_channels, embedding_dim, kernel_size=3, padding=1),
            nn.ReLU(),
        )

    def forward(self, x: "torch.Tensor") -> "torch.Tensor":
        z = self.encoder(x)
        z = z.mean(dim=-1)
        return z


class TS2VecTrainer:
    def __init__(
        self,
        in_channels: int,
        embedding_dim: int = 128,
        hidden_channels: int = 64,
        lr: float = 1e-3,
        temperature: float = 0.2,
        device: str = "cpu",
    ):
        check_torch_available()
        self.device = torch.device(device)
        self.model = TS2VecLite(in_channels, hidden_channels=hidden_channels, embedding_dim=embedding_dim).to(self.device)
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)
        self.temperature = temperature

    def fit(self, batch_iter: Iterable[np.ndarray], epochs: int = 5, min_crop_ratio: float = 0.5):
        self.model.train()
        for _ in range(epochs):
            for batch in batch_iter:
                self.train_step(batch, min_crop_ratio=min_crop_ratio)

    def train_step(self, batch: np.ndarray, min_crop_ratio: float = 0.5) -> float:
        if batch.size == 0:
            return 0.0
        self.model.train()
        x = torch.tensor(batch, dtype=torch.float32, device=self.device)
        x1 = random_crop(x, min_ratio=min_crop_ratio)
        x2 = random_crop(x, min_ratio=min_crop_ratio)
        z1 = self.model(x1)
        z2 = self.model(x2)
        loss = nt_xent_loss(z1, z2, temperature=self.temperature)
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
        return float(loss.item())

    def transform(self, batch: np.ndarray) -> np.ndarray:
        self.model.eval()
        with torch.no_grad():
            x = torch.tensor(batch, dtype=torch.float32, device=self.device)
            z = self.model(x)
            return z.cpu().numpy()
