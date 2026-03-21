from __future__ import annotations

import os
import platform
from pathlib import Path

import numpy as np
import psutil
import pyarrow.parquet as pq
import yaml

_HAS_NWREADER = False
try:
    from nwreader.waveform_convert import inspect_waveforms
    _HAS_NWREADER = True
except ImportError:
    pass


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _system_info() -> dict:
    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 1),
        "python": platform.python_version(),
    }


class StudyInfo:
    """Study metadata — discovered from Parquet files or from the nwreader SDK."""

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

        Falls back to ``start_stamp + idx`` when the array is unavailable
        (e.g. objects built from the nwreader ERD path).
        """
        if self._stamps is None:
            return self.start_stamp + idx
        return int(self._stamps[idx])

    @classmethod
    def from_parquet(cls, pq_dir: Path, sample_freq: float) -> "StudyInfo":
        files = sorted(pq_dir.glob("*.parquet"))
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


def _study_info(study_dir: Path, source_type: str = "parquet",
                study_cfg: dict | None = None) -> StudyInfo:
    """Get study metadata from Parquet files or via the nwreader SDK."""
    if source_type == "erd" and _HAS_NWREADER:
        raw = inspect_waveforms(
            str(study_dir), ignore_stc=True, convert=True,
            convert_time=True, pad_discont=True,
        )
        if not hasattr(raw, "end_stamp"):
            raw.end_stamp = raw.segment_plans[-1].last_stamp
        # total_rows must come from actual sample counts, never from stamp arithmetic.
        if not hasattr(raw, "total_rows") or raw.total_rows is None:
            total = sum(
                plan.n_samples for plan in raw.segment_plans
                if hasattr(plan, "n_samples")
            )
            if total == 0:
                raise RuntimeError(
                    "Cannot determine total row count from nwreader segment plans. "
                    "segment_plans must expose n_samples. Stamp arithmetic will not be used."
                )
            raw.total_rows = total
        return raw

    if not study_cfg or "sample_freq" not in study_cfg:
        raise ValueError(
            "sample_freq must be specified in the study config when source is "
            "'parquet'. The samplestamp column unit is not guaranteed, so the "
            "sampling frequency cannot be inferred reliably. Add "
            "'sample_freq: <Hz>' to the study entry in your config file."
        )
    cfg_freq = float(study_cfg["sample_freq"])

    pq_dir = study_dir
    if not any(pq_dir.glob("*.parquet")):
        for candidate in Path(study_dir).parent.glob("*_exports/parquet_*"):
            if any(candidate.glob("*.parquet")):
                pq_dir = candidate
                break
    return StudyInfo.from_parquet(pq_dir, sample_freq=cfg_freq)
