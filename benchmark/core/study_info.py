from __future__ import annotations

import os
import platform
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import yaml

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

from .config_helpers import normalize_config, validate_config
from .parquet_paths import list_parquet_files


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        cfg = normalize_config(yaml.safe_load(f))
    validate_config(cfg)
    return cfg


def _system_ram_gb() -> float | None:
    if psutil is not None:
        try:
            return round(psutil.virtual_memory().total / (1024**3), 1)
        except Exception:
            pass

    if hasattr(os, "sysconf"):
        try:
            page_count = int(os.sysconf("SC_PHYS_PAGES"))
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
            return round((page_count * page_size) / (1024**3), 1)
        except Exception:
            pass

    return None


def _system_info() -> dict:
    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
        "ram_gb": _system_ram_gb(),
        "python": platform.python_version(),
    }


class StudyInfo:
    """Study metadata discovered from canonical/benchmark Parquet files."""

    def __init__(self, sample_freq: float, channel_labels: list[str],
                 start_stamp: int, end_stamp: int, n_segments: int = 1,
                 total_rows: int | None = None):
        self.sample_freq = sample_freq
        self.channel_labels = channel_labels
        self.channel_columns = [f"ch_{lbl}" for lbl in channel_labels]
        self.n_channels = len(channel_labels)
        self.start_stamp = start_stamp
        self.end_stamp = end_stamp
        self.n_segments = n_segments
        # total_rows is the actual row count across all Parquet files.
        # It differs from end_stamp - start_stamp + 1 whenever there are gaps
        # between segments (e.g. ERD segment boundaries).
        self.total_rows = total_rows
        self.segment_plans = [type("Seg", (), {"last_stamp": end_stamp})()]
        # Populated by from_parquet(); used by stamp_at_row().
        self._stamps: np.ndarray | None = None

    def stamp_at_row(self, idx: int) -> int:
        """Return the samplestamp at row index *idx* (0-based) across all files.

        Uses the full samplestamp array loaded during from_parquet() setup —
        O(1) array lookup, no disk I/O at call time, correct regardless of gaps.
        """
        if self._stamps is None:
            raise RuntimeError(
                "StudyInfo.stamp_at_row() requires Parquet-backed samplestamps. "
                "Construct StudyInfo via StudyInfo.from_parquet()."
            )
        return int(self._stamps[idx])

    @classmethod
    def from_parquet(cls, pq_dir: Path, sample_freq: float) -> "StudyInfo":
        files = list_parquet_files(pq_dir)
        if not files:
            raise FileNotFoundError(f"No .parquet files in {pq_dir}")

        schema = pq.read_schema(str(files[0]))
        ch_cols = [c for c in schema.names if c.startswith("ch_")]
        labels = [c[3:] for c in ch_cols]

        # Read the full samplestamp column from all partition files once during
        # setup.  This is ~8 bytes * total_rows in memory (≈95 MB for 11.8M rows)
        # and makes stamp_at_row() a plain O(1) array lookup — correct regardless
        # of gaps anywhere within or between partition files.
        stamps = np.concatenate([
            pq.read_table(str(f), columns=["samplestamp"])
              .column("samplestamp").to_numpy()
            for f in files
        ]).astype(np.int64)

        obj = cls(
            sample_freq=float(sample_freq),
            channel_labels=labels,
            start_stamp=int(stamps[0]),
            end_stamp=int(stamps[-1]),
            n_segments=len(files),
            total_rows=len(stamps),
        )
        obj._stamps = stamps
        return obj
