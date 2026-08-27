from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import sys
import threading
import time
from typing import Any

import numpy as np

from .config_helpers import (
    get_channel_subsets,
    get_compression_codec_matrix,
    get_read_positions,
    get_repetitions,
    get_remote_query_cfg,
    get_tuned_parquet_codecs,
    get_window_sizes,
    is_investigation_enabled,
    merge_remote_query_paths,
)


BYTES_PER_FLOAT32 = 4
_MEMORY_POLL_INTERVAL_SECONDS = 0.05
logger = logging.getLogger(__name__)


def _current_process_rss_bytes() -> int | None:
    try:
        import psutil  # type: ignore

        return int(psutil.Process().memory_info().rss)
    except Exception:
        logger.debug("RSS detection via psutil failed; falling back to platform-specific strategies", exc_info=True)

    if sys.platform == "win32":
        try:
            import ctypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", ctypes.c_uint32),
                    ("PageFaultCount", ctypes.c_uint32),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            if not ctypes.windll.psapi.GetProcessMemoryInfo(
                ctypes.windll.kernel32.GetCurrentProcess(),
                ctypes.byref(counters),
                counters.cb,
            ):
                logger.debug("RSS detection via GetProcessMemoryInfo returned failure status; returning None")
                logger.debug("RSS detection failed; returning None")
                return None
            return int(counters.WorkingSetSize)
        except Exception:
            logger.debug("RSS detection via GetProcessMemoryInfo failed; returning None", exc_info=True)
            logger.debug("RSS detection failed; returning None")
            return None

    statm_path = "/proc/self/statm"
    if os.path.exists(statm_path):
        try:
            with open(statm_path, "r", encoding="utf-8") as f:
                parts = f.read().split()
            if len(parts) >= 2:
                return int(parts[1]) * int(os.sysconf("SC_PAGE_SIZE"))
        except Exception:
            logger.debug("RSS detection via /proc/self/statm failed", exc_info=True)

    logger.debug("RSS detection failed; returning None")

    return None


class _PeakRssTracker:
    def __init__(self, poll_interval_seconds: float = _MEMORY_POLL_INTERVAL_SECONDS):
        self._poll_interval_seconds = poll_interval_seconds
        self._peak_bytes: int | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample_once(self) -> None:
        rss = _current_process_rss_bytes()
        if rss is None:
            return
        self._peak_bytes = rss if self._peak_bytes is None else max(self._peak_bytes, rss)

    def _poll_loop(self) -> None:
        while not self._stop.wait(self._poll_interval_seconds):
            self._sample_once()

    def __enter__(self):
        self._sample_once()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(0.05, self._poll_interval_seconds * 2))
            self._thread = None
        self._sample_once()

    @property
    def peak_rss_mib(self) -> float | None:
        if self._peak_bytes is None:
            return None
        return float(self._peak_bytes) / (1024.0 * 1024.0)


def _peak_rss_fields(peak_rss_mib: float | None) -> dict[str, float]:
    if peak_rss_mib is None:
        return {}
    return {"peak_rss_mib": round(float(peak_rss_mib), 1)}


def _max_peak_rss(peaks: list[float | None]) -> float | None:
    valid = [float(peak) for peak in peaks if peak is not None]
    return max(valid) if valid else None


@dataclass(frozen=True)
class TimedMeasurement:
    median_seconds: float
    result: Any
    first_seconds: float
    all_seconds: tuple[float, ...]
    peak_rss_mib: float | None = None

    def __iter__(self):
        yield self.median_seconds
        yield self.result


def _timed(fn, reps: int = 3) -> TimedMeasurement:
    """Run fn() reps times and return median + first-run timing details."""
    times = []
    peaks: list[float | None] = []
    result = None
    for _ in range(reps):
        with _PeakRssTracker() as tracker:
            t0 = time.perf_counter()
            result = fn()
            times.append(time.perf_counter() - t0)
        peaks.append(tracker.peak_rss_mib)
    return TimedMeasurement(
        median_seconds=float(np.median(times)),
        result=result,
        first_seconds=float(times[0]),
        all_seconds=tuple(float(t) for t in times),
        peak_rss_mib=_max_peak_rss(peaks),
    )


def _throughput(n_samples: int, n_channels: int, seconds: float) -> dict:
    """Compute throughput metrics."""
    total_bytes = n_samples * n_channels * BYTES_PER_FLOAT32
    mib = total_bytes / (1024 * 1024)
    return {
        "samples": n_samples,
        "channels": n_channels,
        "bytes": total_bytes,
        "mib": round(mib, 3),
        "mib_per_sec": round(mib / seconds, 3) if seconds > 0 else 0,
        "samples_per_sec": round(n_samples * n_channels / seconds) if seconds > 0 else 0,
    }


def _full_study_duration_hours(info) -> int:
    """Return study duration rounded down to the nearest whole hour."""
    total_sec = info.total_rows / info.sample_freq
    return int(total_sec // 3600)


def _chunk_ranges(start_stamp: int, end_stamp: int, chunk_stamps: int):
    """Yield (chunk_start, chunk_end) stamp ranges."""
    s = start_stamp
    while s <= end_stamp:
        e = min(s + chunk_stamps - 1, end_stamp)
        yield s, e
        s = e + 1


def _estimate_runs(cfg: dict, selected: list) -> int:
    """Rough estimate of total benchmark invocations across all studies."""
    n = 0
    reps = get_repetitions(cfg)
    read_positions = get_read_positions(cfg)
    channel_subsets = get_channel_subsets(cfg)
    window_sizes = get_window_sizes(cfg)
    remote_cfg = get_remote_query_cfg(cfg)
    studies = cfg.get("studies") or [{}]
    for study in studies:
        remote_only = bool(study.get("remote_only"))
        for cat_id, _, _ in selected:
            if remote_only and cat_id != "remote_query":
                continue
            if cat_id == "random_access":
                n += len(read_positions) * 2 * reps
            elif cat_id == "channel_subset":
                n += (len(channel_subsets) + 1) * 2 * reps
            elif cat_id == "remontage":
                n += 2 * reps
            elif cat_id == "filter_pipeline":
                n += 1
            elif cat_id == "window_scaling":
                n += len(window_sizes) * 2 * reps
            elif cat_id == "compression":
                if is_investigation_enabled(cfg, "compression"):
                    n += len(get_compression_codec_matrix(cfg)) * reps
            elif cat_id == "precision_loss":
                if is_investigation_enabled(cfg, "precision_loss"):
                    n += 1
            elif cat_id == "int32_storage":
                if is_investigation_enabled(cfg, "int32_storage"):
                    n += 12 * reps
            elif cat_id == "remote_query":
                if is_investigation_enabled(cfg, "remote_query"):
                    # Manifest paths override the deprecated global path keys.
                    merged = merge_remote_query_paths(remote_cfg, study)
                    parquet_variants = sum(
                        1
                        for key in ("remote_float32_path", "remote_int32_nanovolt_path", "remote_single_file_path")
                        if merged.get(key)
                    )
                    channel_variants = 2  # all channels + 10-20 subset when available
                    n += parquet_variants * channel_variants
                    if merged.get("full_study_chunk_sec"):
                        n += parquet_variants * channel_variants
            elif cat_id == "tuned_comparison":
                tuned_variants = len(get_tuned_parquet_codecs(cfg)) + 1  # one HDF5 variant
                n += tuned_variants * ((2 + len(window_sizes)) * reps + 1)
            elif cat_id == "baseline_comparison":
                n += ((2 + len(window_sizes)) * reps + 1)
    return n


def _print_result(r: dict) -> None:
    """Pretty-print a single benchmark result."""
    fmt = r.get("format", "")
    t = r.get("wall_clock_seconds") or r.get("total_wall_seconds", 0)
    first_t = r.get("first_wall_clock_seconds")

    mode = r.get("mode", fmt)
    benchmark = str(r.get("benchmark", ""))
    prefix = f"[{benchmark}] " if "." in benchmark else ""
    parts = [f"    {prefix}{mode:20s}"]
    if "read_method" in r:
        parts.append(f"via={r['read_method']:>5s}")
    if "position" in r:
        parts.append(f"pos={r['position']:>4s}")
    if "channel_subset" in r:
        parts.append(f"subset={r['channel_subset']}")
    if "channels" in r and isinstance(r["channels"], str):
        parts.append(f"ch={r['channels']:>4s}")
    if "codec" in r:
        parts.append(f"codec={r['codec']:>8s}")
    if "window_seconds" in r:
        parts.append(f"win={r['window_seconds']:>5}s")
    parts.append(f"time={t:.4f}s")
    if first_t is not None and abs(float(first_t) - float(t)) > 5e-7:
        parts.append(f"first={float(first_t):.4f}s")
    if "avg_wall_per_window" in r:
        parts.append(f"avg/win={r['avg_wall_per_window']:.3f}s")
    if "download_seconds" in r:
        dl_tag = "dl~" if r.get("download_estimated") else "dl="
        parts.append(f"{dl_tag}{r['download_seconds']:.1f}s")
    if "mib_per_sec" in r:
        parts.append(f"tput={r['mib_per_sec']:.1f} MiB/s")
    if r.get("peak_rss_mib") is not None:
        parts.append(f"rss={float(r['peak_rss_mib']):.1f} MiB")
    if "compression_ratio" in r and r["compression_ratio"] is not None:
        parts.append(f"ratio={r['compression_ratio']:.1f}x")
    if "worst_max_abs_error" in r:
        parts.append(f"worst_err={r['worst_max_abs_error']:.6f}")
    if "avg_snr_db" in r:
        parts.append(f"avg_snr={r['avg_snr_db']:.1f}dB")

    print("  ".join(parts))


PeakRssTracker = _PeakRssTracker
chunk_ranges = _chunk_ranges
estimate_runs = _estimate_runs
full_study_duration_hours = _full_study_duration_hours
max_peak_rss = _max_peak_rss
peak_rss_fields = _peak_rss_fields
print_result = _print_result
throughput = _throughput
timed = _timed

__all__ = [
    "BYTES_PER_FLOAT32",
    "PeakRssTracker",
    "TimedMeasurement",
    "chunk_ranges",
    "estimate_runs",
    "full_study_duration_hours",
    "max_peak_rss",
    "peak_rss_fields",
    "print_result",
    "throughput",
    "timed",
]
