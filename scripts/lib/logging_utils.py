from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from .utils import ensure_dir


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PrettyFormatter(logging.Formatter):
    def __init__(self, max_len: int = 140, max_items: int = 6):
        super().__init__()
        self.max_len = max_len
        self.max_items = max_items

    def _shorten(self, value):
        if isinstance(value, list):
            if len(value) <= self.max_items:
                return value
            head = value[: self.max_items]
            return head + [f"... +{len(value) - self.max_items}"]
        if isinstance(value, dict):
            if len(value) <= self.max_items:
                return value
            items = list(value.items())[: self.max_items]
            tail = len(value) - self.max_items
            return dict(items + [("...", f"+{tail}")])
        text = str(value)
        if len(text) > self.max_len:
            return text[: self.max_len - 3] + "..."
        return value

    def format(self, record):
        msg = record.getMessage()
        try:
            payload = json.loads(msg)
        except Exception:
            return msg

        ts = payload.pop("ts", "")
        event = payload.pop("event", "event")
        time_str = ts
        try:
            time_str = datetime.fromisoformat(ts).strftime("%H:%M:%S")
        except Exception:
            pass

        parts = []
        for key, value in payload.items():
            value = self._shorten(value)
            parts.append(f"{key}={value}")
        suffix = (" " + " ".join(parts)) if parts else ""
        return f"[{time_str}] {event}{suffix}"


def setup_logging(run_name: str, outputs_dir: Path):
    log_dir = ensure_dir(outputs_dir / "logs")
    ts = _utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"{run_name}_{ts}.jsonl"

    logger = logging.getLogger(run_name)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.propagate = False

    formatter = logging.Formatter("%(message)s")
    stream = logging.StreamHandler()
    if os.getenv("PRETTY_LOGS", "1") != "0":
        stream.setFormatter(PrettyFormatter())
    else:
        stream.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream)
    logger.addHandler(file_handler)

    log_event(logger, "logger_ready", run_name=run_name, log_path=str(log_path))
    return logger, log_path


def log_event(logger, event: str, **kwargs):
    payload = {"ts": _utcnow().isoformat(), "event": event}
    if kwargs:
        payload.update(kwargs)
    logger.info(json.dumps(payload, default=str))


class Timer:
    def __init__(self, logger, event: str, **kwargs):
        self.logger = logger
        self.event = event
        self.kwargs = kwargs
        self.start = None

    def __enter__(self):
        self.start = time.perf_counter()
        log_event(self.logger, f"{self.event}_start", **self.kwargs)
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed = time.perf_counter() - self.start if self.start else None
        log_event(self.logger, f"{self.event}_end", elapsed_seconds=elapsed, **self.kwargs)
