from __future__ import annotations

import hashlib
import json
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


_STAMP_CACHE_BATCH_ROWS = 1 << 23  # 8,388,608 int64 rows ≈ 64 MiB per streamed batch.


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


def _safe_cache_stem(path: Path) -> str:
    stem = path.stem if path.suffix else path.name
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)[:48]


def _parquet_file_info(file_path: Path) -> dict[str, int | str]:
    file_path = Path(file_path)
    stat = file_path.stat()
    parquet_file = pq.ParquetFile(str(file_path))
    return {
        "path": str(file_path.resolve()),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "rows": int(parquet_file.metadata.num_rows),
    }


def _stamp_cache_path(pq_path: Path, file_infos: list[dict[str, int | str]]) -> Path:
    pq_path = Path(pq_path)
    payload = {
        "source": str(pq_path.resolve()),
        "files": file_infos,
    }
    token = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return pq_path.parent / f"{_safe_cache_stem(pq_path)}_stamps_{token}.npy"


def _build_stamp_cache(cache_path: Path, files: list[Path], total_rows: int) -> None:
    temp_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if temp_path.exists():
        temp_path.unlink()

    stamps_mm = None
    try:
        stamps_mm = np.lib.format.open_memmap(temp_path, mode="w+", dtype=np.int64, shape=(total_rows,))
        cursor = 0
        for file_path in files:
            parquet_file = pq.ParquetFile(str(file_path))
            for batch in parquet_file.iter_batches(
                batch_size=_STAMP_CACHE_BATCH_ROWS,
                columns=["samplestamp"],
                use_threads=False,
            ):
                chunk = np.asarray(batch.column(0).to_numpy(zero_copy_only=False), dtype=np.int64)
                next_cursor = cursor + len(chunk)
                stamps_mm[cursor:next_cursor] = chunk
                cursor = next_cursor

        if cursor != total_rows:
            raise RuntimeError(
                f"Expected {total_rows} samplestamps while building {cache_path}, got {cursor}."
            )

        stamps_mm.flush()
        stamps_mm_ref = stamps_mm
        stamps_mm = None
        del stamps_mm_ref
        if cache_path.exists():
            cache_path.unlink()
        temp_path.replace(cache_path)
    except Exception:
        if stamps_mm is not None:
            del stamps_mm
        if temp_path.exists():
            temp_path.unlink()
        raise


def _open_stamp_cache(cache_path: Path, total_rows: int) -> np.memmap:
    stamps = np.load(str(cache_path), mmap_mode="r", allow_pickle=False)
    if not isinstance(stamps, np.memmap) or stamps.dtype != np.int64 or stamps.shape != (total_rows,):
        raise ValueError(f"Unexpected stamp cache shape/dtype in {cache_path}")
    return stamps


def _ensure_stamp_cache(pq_path: Path, files: list[Path], file_infos: list[dict[str, int | str]],
                        total_rows: int) -> Path:
    cache_path = _stamp_cache_path(pq_path, file_infos)
    if not cache_path.exists():
        _build_stamp_cache(cache_path, files, total_rows)

    try:
        stamps = _open_stamp_cache(cache_path, total_rows)
        del stamps
        return cache_path
    except Exception:
        if cache_path.exists():
            cache_path.unlink()
        _build_stamp_cache(cache_path, files, total_rows)
        stamps = _open_stamp_cache(cache_path, total_rows)
        del stamps
        return cache_path


class StudyInfo:
    """Study metadata discovered from canonical/benchmark Parquet files."""

    def __init__(self, sample_freq: float, channel_labels: list[str],
                 start_stamp: int, end_stamp: int, n_segments: int = 1,
                 total_rows: int | None = None,
                 segment_plans: list[object] | None = None):
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
        self.segment_plans = segment_plans or [type("Seg", (), {"last_stamp": end_stamp})()]
        # Populated by from_parquet(); used by stamp_at_row(). This is a
        # disk-backed memmap, not an eagerly materialized in-memory array.
        self._stamps: np.memmap | None = None
        self._stamp_cache_path: Path | None = None
        self._segment_row_offsets: tuple[int, ...] = ()

    def close(self) -> None:
        stamps = self._stamps
        self._stamps = None
        if stamps is not None:
            del stamps

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def stamp_at_row(self, idx: int) -> int:
        """Return the samplestamp at row index *idx* (0-based) across all files.

        Uses a disk-backed memmap built during from_parquet() setup — O(1)
        indexed access without loading the full samplestamp column into RAM.
        """
        if self._stamps is not None:
            return int(self._stamps[idx])
        if self._stamp_cache_path is None or self.total_rows is None:
            raise RuntimeError(
                "StudyInfo.stamp_at_row() requires Parquet-backed samplestamps. "
                "Construct StudyInfo via StudyInfo.from_parquet()."
            )
        stamps = _open_stamp_cache(self._stamp_cache_path, int(self.total_rows))
        try:
            return int(stamps[idx])
        finally:
            del stamps

    @classmethod
    def from_parquet(cls, pq_dir: Path, sample_freq: float) -> "StudyInfo":
        pq_dir = Path(pq_dir)
        files = list_parquet_files(pq_dir)
        if not files:
            raise FileNotFoundError(f"No .parquet files in {pq_dir}")

        schema = pq.read_schema(str(files[0]))
        ch_cols = [c for c in schema.names if c.startswith("ch_")]
        labels = [c[3:] for c in ch_cols]

        file_infos = [_parquet_file_info(file_path) for file_path in files]
        row_counts = [int(info["rows"]) for info in file_infos]
        total_rows = sum(row_counts)
        if total_rows <= 0:
            raise ValueError(f"No rows found in parquet input {pq_dir}")

        row_offsets = [0]
        for row_count in row_counts:
            row_offsets.append(row_offsets[-1] + row_count)

        cache_path = _ensure_stamp_cache(pq_dir, files, file_infos, total_rows)
        stamps = _open_stamp_cache(cache_path, total_rows)
        try:
            start_stamp = int(stamps[0])
            end_stamp = int(stamps[-1])
            segment_plans = [
                type("Seg", (), {"last_stamp": int(stamps[row_offsets[i + 1] - 1])})()
                for i in range(len(row_counts))
            ]
        finally:
            del stamps

        obj = cls(
            sample_freq=float(sample_freq),
            channel_labels=labels,
            start_stamp=start_stamp,
            end_stamp=end_stamp,
            n_segments=len(files),
            total_rows=total_rows,
            segment_plans=segment_plans,
        )
        obj._stamp_cache_path = cache_path
        obj._segment_row_offsets = tuple(row_offsets[:-1])
        return obj
