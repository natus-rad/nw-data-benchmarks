from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any

import numpy as np

from .config_helpers import (
    get_parquet_compression_variants,
    get_tuned_parquet_codecs,
    is_investigation_enabled,
)


BYTES_PER_FLOAT32 = 4


@dataclass(frozen=True)
class TimedMeasurement:
    median_seconds: float
    result: Any
    first_seconds: float
    all_seconds: tuple[float, ...]

    def __iter__(self):
        yield self.median_seconds
        yield self.result


def _timed(fn, reps: int = 3) -> TimedMeasurement:
    """Run fn() reps times and return median + first-run timing details."""
    times = []
    result = None
    for _ in range(reps):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return TimedMeasurement(
        median_seconds=float(np.median(times)),
        result=result,
        first_seconds=float(times[0]),
        all_seconds=tuple(float(t) for t in times),
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
    """Rough estimate of total benchmark invocations."""
    n = 0
    reps = cfg.get("repetitions", 3)
    for cat_id, _, _ in selected:
        if cat_id == "random_access":
            n += len(cfg.get("read_positions", [0, 0.5, 0.75, 0.95])) * 2 * reps
        elif cat_id == "channel_subset":
            n += (len(cfg.get("channel_subsets", [4, 10])) + 1) * 2 * reps
        elif cat_id == "remontage":
            n += 2 * reps
        elif cat_id == "filter_pipeline":
            n += 1
        elif cat_id == "window_scaling":
            n += len(cfg.get("window_sizes", [])) * 2 * reps
        elif cat_id == "compression":
            if is_investigation_enabled(cfg, "compression"):
                n += len(get_parquet_compression_variants(cfg)) * reps
        elif cat_id == "precision_loss":
            if is_investigation_enabled(cfg, "precision_loss"):
                n += 1
        elif cat_id == "tuned_comparison":
            tuned_variants = len(get_tuned_parquet_codecs(cfg)) + 1  # one HDF5 variant
            n += tuned_variants * ((2 + len(cfg.get("window_sizes", []))) * reps + 1)
        elif cat_id == "baseline_comparison":
            n += ((2 + len(cfg.get("window_sizes", []))) * reps + 1)
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
    if "compression_ratio" in r and r["compression_ratio"] is not None:
        parts.append(f"ratio={r['compression_ratio']:.1f}x")
    if "worst_max_abs_error" in r:
        parts.append(f"worst_err={r['worst_max_abs_error']:.6f}")
    if "avg_snr_db" in r:
        parts.append(f"avg_snr={r['avg_snr_db']:.1f}dB")

    print("  ".join(parts))