from __future__ import annotations

from contextlib import nullcontext
import numpy as np

from .bench_utils import _chunk_ranges, _full_study_duration_hours, _throughput, _timed
from .readers import (
    EdfFileReader,
    _edf_file,
    _read_h5_columnar_window,
    _read_h5_rowgroup_window,
    _read_int32_calibrated,
    _read_int32_calibrated_arrow,
    _read_int32_nanovolt,
    _read_int32_nanovolt_arrow,
    _read_parquet_window,
    _read_tuned_pq,
)
from .remote import bench_remote_query
from .setup import _get_tuned_block_sizes
from .signal import _apply_bipolar_montage, _apply_filters, _build_sos


def _available_window_formats(paths: dict):
    formats = []
    if "parquet" in paths:
        formats.append(("parquet", None))
    if "edf" in paths:
        formats.append(("edf", None))
    for h5_key, h5_fn in [
        ("h5_columnar", _read_h5_columnar_window),
        ("h5_rowgroup", _read_h5_rowgroup_window),
    ]:
        if h5_key in paths:
            formats.append((h5_key, h5_fn))
    return formats


def bench_random_access(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    positions = cfg.get("read_positions", [0.0, 0.5, 0.75, 0.95])
    total_stamps = info.total_rows
    n_channels = len(info.channel_labels)

    edf_path = _edf_file(paths["edf"]) if "edf" in paths else None
    edf_cm = EdfFileReader(edf_path) if edf_path else nullcontext(None)
    with edf_cm as edf_reader:
        edf_total = edf_reader.total_samples if edf_reader else 0

        for pos in positions:
            label = f"{int(pos * 100)}%"
            start_stamp = info.stamp_at_row(int(pos * total_stamps))
            end_stamp = start_stamp + window_stamps - 1

            if "parquet" in paths:
                t, data = _timed(
                    lambda s=start_stamp, e=end_stamp: _read_parquet_window(paths["parquet"], info.channel_columns, s, e),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "random_access", "format": "parquet",
                    "position": label, "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_channels, t),
                })

            if edf_reader:
                start_sample = int(pos * edf_total)
                n_samp = min(int(window_sec * info.sample_freq), edf_total - start_sample)
                t, data = _timed(lambda s=start_sample, n=n_samp: edf_reader.read_window(s, n), reps)
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "random_access", "format": "edf",
                    "position": label, "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_channels, t),
                })

            for h5_key, h5_read_fn in [
                ("h5_columnar", _read_h5_columnar_window),
                ("h5_rowgroup", _read_h5_rowgroup_window),
            ]:
                if h5_key not in paths:
                    continue
                t, data = _timed(
                    lambda s=start_stamp, e=end_stamp, fn=h5_read_fn, p=paths[h5_key]: fn(p, info.channel_columns, s, e),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "random_access", "format": h5_key,
                    "position": label, "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_channels, t),
                })

    return results


def bench_channel_subset(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    subsets = cfg.get("channel_subsets", [4, 10])
    mid_stamp = info.stamp_at_row(info.total_rows // 2)
    start_stamp = mid_stamp
    end_stamp = mid_stamp + window_stamps - 1

    edf_path = _edf_file(paths["edf"]) if "edf" in paths else None
    edf_cm = EdfFileReader(edf_path) if edf_path else nullcontext(None)
    with edf_cm as edf_reader:
        edf_total = edf_reader.total_samples if edf_reader else 0
        edf_start = edf_total // 2 if edf_reader else 0
        edf_n = min(int(window_sec * info.sample_freq), edf_total - edf_start) if edf_reader else 0

        all_cols = info.channel_columns
        n_all = len(all_cols)
        counts = sorted(set([min(s, n_all) for s in subsets] + [n_all]))

        for n_ch in counts:
            ch_label = f"{n_ch}" if n_ch < n_all else "all"
            cols = all_cols[:n_ch]
            ch_indices = list(range(n_ch))

            if "parquet" in paths:
                t, data = _timed(lambda c=cols: _read_parquet_window(paths["parquet"], c, start_stamp, end_stamp), reps)
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "channel_subset", "format": "parquet",
                    "channels": ch_label, "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_ch, t),
                })

            if edf_reader:
                t, data = _timed(lambda ci=ch_indices: edf_reader.read_window(edf_start, edf_n, ci), reps)
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "channel_subset", "format": "edf",
                    "channels": ch_label, "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_ch, t),
                })

            for h5_key, h5_read_fn in [
                ("h5_columnar", _read_h5_columnar_window),
                ("h5_rowgroup", _read_h5_rowgroup_window),
            ]:
                if h5_key not in paths:
                    continue
                t, data = _timed(
                    lambda c=cols, fn=h5_read_fn, p=paths[h5_key]: fn(p, c, start_stamp, end_stamp),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "channel_subset", "format": h5_key,
                    "channels": ch_label, "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_ch, t),
                })

    return results


def bench_remontage(info, paths: dict, cfg: dict) -> list[dict]:
    import time

    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.stamp_at_row(info.total_rows // 2)
    labels = list(info.channel_labels)
    n_channels = len(labels)

    edf_path = _edf_file(paths["edf"]) if "edf" in paths else None
    edf_cm = EdfFileReader(edf_path) if edf_path else nullcontext(None)
    with edf_cm as edf_reader:
        edf_total = edf_reader.total_samples if edf_reader else 0
        edf_start = edf_total // 2 if edf_reader else 0
        edf_n = min(int(window_sec * info.sample_freq), edf_total - edf_start) if edf_reader else 0

        for fmt, h5_fn in _available_window_formats(paths):
            def run(f=fmt, fn=h5_fn):
                t_read_start = time.perf_counter()
                if f == "parquet":
                    matrix = _read_parquet_window(paths["parquet"], info.channel_columns, mid_stamp, mid_stamp + window_stamps - 1)
                elif f == "edf":
                    matrix = edf_reader.read_window(edf_start, edf_n)
                else:
                    matrix = fn(paths[f], info.channel_columns, mid_stamp, mid_stamp + window_stamps - 1)
                read_sec = time.perf_counter() - t_read_start
                t_mont_start = time.perf_counter()
                derived = _apply_bipolar_montage(matrix, labels)
                montage_sec = time.perf_counter() - t_mont_start
                return matrix, derived, read_sec, montage_sec

            times_read, times_mont = [], []
            matrix = derived = None
            for _ in range(reps):
                matrix, derived, r, m = run()
                times_read.append(r)
                times_mont.append(m)

            read_sec = float(np.median(times_read))
            mont_sec = float(np.median(times_mont))
            total = read_sec + mont_sec
            n_samples = matrix.shape[1] if (matrix is not None and matrix.ndim == 2) else 0
            results.append({
                "category": "remontage", "format": fmt,
                "window_seconds": window_sec,
                "wall_clock_seconds": round(total, 6),
                "read_seconds": round(read_sec, 6),
                "montage_seconds": round(mont_sec, 6),
                "derived_channels": derived.shape[0] if derived is not None and derived.ndim == 2 else 0,
                **_throughput(n_samples, n_channels, total),
            })
    return results


def bench_filter_pipeline(info, paths: dict, cfg: dict) -> list[dict]:
    import time

    results = []
    labels = list(info.channel_labels)
    n_channels = len(labels)
    sample_freq = info.sample_freq
    sos = _build_sos(sample_freq)

    hours = _full_study_duration_hours(info)
    if hours < 1:
        hours = 1
    bench_stamps = int(hours * 3600 * sample_freq)
    bench_start = info.stamp_at_row(0)
    bench_end = info.stamp_at_row(min(bench_stamps - 1, info.total_rows - 1))
    bench_sec = bench_stamps / sample_freq

    chunk_sec = 300
    chunk_stamps = int(chunk_sec * sample_freq)
    edf_chunk_samples = int(chunk_sec * sample_freq)

    print(f"    Study: {hours}h ({bench_sec:.0f}s), {n_channels} ch, {sample_freq} Hz")

    for fmt, h5_fn in _available_window_formats(paths):
        edf_cm = EdfFileReader(_edf_file(paths["edf"])) if fmt == "edf" else nullcontext(None)
        with edf_cm as edf_reader:
            edf_bench_samples = min(int(hours * 3600 * sample_freq), edf_reader.total_samples) if edf_reader else 0
            t_read_total = t_mont_total = t_filt_total = 0.0
            total_samples_read = 0

            t_wall_start = time.perf_counter()
            if fmt == "edf":
                edf_pos = 0
                while edf_pos < edf_bench_samples:
                    n = min(edf_chunk_samples, edf_bench_samples - edf_pos)

                    t0 = time.perf_counter()
                    matrix = edf_reader.read_window(edf_pos, n)
                    t_read_total += time.perf_counter() - t0

                    t1 = time.perf_counter()
                    derived = _apply_bipolar_montage(matrix, labels)
                    t_mont_total += time.perf_counter() - t1

                    t2 = time.perf_counter()
                    _apply_filters(derived, sos)
                    t_filt_total += time.perf_counter() - t2

                    total_samples_read += matrix.shape[1] if matrix.ndim == 2 else 0
                    edf_pos += n
            else:
                for cs, ce in _chunk_ranges(bench_start, bench_end, chunk_stamps):
                    t0 = time.perf_counter()
                    if fmt == "parquet":
                        matrix = _read_parquet_window(paths["parquet"], info.channel_columns, cs, ce)
                    else:
                        matrix = h5_fn(paths[fmt], info.channel_columns, cs, ce)
                    t_read_total += time.perf_counter() - t0

                    t1 = time.perf_counter()
                    derived = _apply_bipolar_montage(matrix, labels)
                    t_mont_total += time.perf_counter() - t1

                    t2 = time.perf_counter()
                    _apply_filters(derived, sos)
                    t_filt_total += time.perf_counter() - t2

                    total_samples_read += matrix.shape[1] if matrix.ndim == 2 else 0

            t_wall = time.perf_counter() - t_wall_start
            results.append({
                "category": "filter_pipeline_full", "format": fmt,
                "benchmark": "D.1",
                "duration_hours": hours,
                "duration_seconds": bench_sec,
                "sample_freq": sample_freq,
                "channels": n_channels,
                "total_samples": total_samples_read,
                "wall_clock_seconds": round(t_wall, 3),
                "read_seconds": round(t_read_total, 3),
                "montage_seconds": round(t_mont_total, 3),
                "filter_seconds": round(t_filt_total, 3),
                **_throughput(total_samples_read, n_channels, t_wall),
            })

    fft_window_sec = 10
    fft_stride_sec = 2
    fft_window_samples = int(fft_window_sec * sample_freq)
    fft_stride_samples = int(fft_stride_sec * sample_freq)
    n_fft_windows = int((bench_sec - fft_window_sec) / fft_stride_sec) + 1
    print(f"    FFT: {n_fft_windows} windows, {fft_window_sec}s window, {fft_stride_sec}s stride")

    for fmt, h5_fn in _available_window_formats(paths):
        edf_cm = EdfFileReader(_edf_file(paths["edf"])) if fmt == "edf" else nullcontext(None)
        with edf_cm as edf_reader:
            edf_bench_samples = min(int(hours * 3600 * sample_freq), edf_reader.total_samples) if edf_reader else 0
            t_read_total = t_mont_total = t_filt_total = t_fft_total = 0.0
            total_samples_read = 0
            fft_count = 0

            t_wall_start = time.perf_counter()
            read_chunk_sec = 300
            read_chunk_stamps = int(read_chunk_sec * sample_freq)
            tail: np.ndarray | None = None

            if fmt == "edf":
                edf_pos = 0
                edf_chunk = int(read_chunk_sec * sample_freq)
                while edf_pos < edf_bench_samples:
                    n = min(edf_chunk, edf_bench_samples - edf_pos)

                    t0 = time.perf_counter()
                    matrix = edf_reader.read_window(edf_pos, n)
                    t_read_total += time.perf_counter() - t0

                    t1 = time.perf_counter()
                    derived = _apply_bipolar_montage(matrix, labels)
                    t_mont_total += time.perf_counter() - t1

                    t2 = time.perf_counter()
                    filtered = _apply_filters(derived, sos)
                    t_filt_total += time.perf_counter() - t2

                    total_samples_read += matrix.shape[1] if matrix.ndim == 2 else 0
                    combined = np.concatenate([tail, filtered], axis=1) if tail is not None and tail.shape[1] > 0 else filtered
                    n_combined = combined.shape[1]
                    t3 = time.perf_counter()
                    pos = 0
                    while pos + fft_window_samples <= n_combined:
                        np.fft.rfft(combined[:, pos:pos + fft_window_samples], axis=1)
                        fft_count += 1
                        pos += fft_stride_samples
                    t_fft_total += time.perf_counter() - t3
                    tail = combined[:, pos:]
                    edf_pos += n
            else:
                for cs, ce in _chunk_ranges(bench_start, bench_end, read_chunk_stamps):
                    t0 = time.perf_counter()
                    if fmt == "parquet":
                        matrix = _read_parquet_window(paths["parquet"], info.channel_columns, cs, ce)
                    else:
                        matrix = h5_fn(paths[fmt], info.channel_columns, cs, ce)
                    t_read_total += time.perf_counter() - t0

                    t1 = time.perf_counter()
                    derived = _apply_bipolar_montage(matrix, labels)
                    t_mont_total += time.perf_counter() - t1

                    t2 = time.perf_counter()
                    filtered = _apply_filters(derived, sos)
                    t_filt_total += time.perf_counter() - t2

                    total_samples_read += matrix.shape[1] if matrix.ndim == 2 else 0
                    combined = np.concatenate([tail, filtered], axis=1) if tail is not None and tail.shape[1] > 0 else filtered
                    n_combined = combined.shape[1]
                    t3 = time.perf_counter()
                    pos = 0
                    while pos + fft_window_samples <= n_combined:
                        np.fft.rfft(combined[:, pos:pos + fft_window_samples], axis=1)
                        fft_count += 1
                        pos += fft_stride_samples
                    t_fft_total += time.perf_counter() - t3
                    tail = combined[:, pos:]

            t_wall = time.perf_counter() - t_wall_start
            if fft_count != n_fft_windows:
                print(
                    f"  [warn] D.2 {fmt}: expected {n_fft_windows} FFT windows, "
                    f"computed {fft_count} (delta {fft_count - n_fft_windows:+d})"
                )

            results.append({
                "category": "sliding_fft_full", "format": fmt,
                "benchmark": "D.2",
                "duration_hours": hours,
                "duration_seconds": bench_sec,
                "sample_freq": sample_freq,
                "channels": n_channels,
                "total_samples": total_samples_read,
                "fft_window_sec": fft_window_sec,
                "fft_stride_sec": fft_stride_sec,
                "fft_windows_expected": n_fft_windows,
                "fft_windows_computed": fft_count,
                "wall_clock_seconds": round(t_wall, 3),
                "read_seconds": round(t_read_total, 3),
                "montage_seconds": round(t_mont_total, 3),
                "filter_seconds": round(t_filt_total, 3),
                "fft_seconds": round(t_fft_total, 3),
                **_throughput(total_samples_read, n_channels, t_wall),
            })

    return results


def bench_window_scaling(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sizes = cfg.get("window_sizes", [10, 30, 60, 300, 900, 1800, 3600])
    n_channels = len(info.channel_labels)
    total_stamps = info.total_rows

    edf_path = _edf_file(paths["edf"]) if "edf" in paths else None
    edf_cm = EdfFileReader(edf_path) if edf_path else nullcontext(None)
    mid_stamp = info.stamp_at_row(total_stamps // 2)

    with edf_cm as edf_reader:
        edf_total = edf_reader.total_samples if edf_reader else 0

        for window_sec in window_sizes:
            window_stamps = int(window_sec * info.sample_freq)
            if window_stamps > total_stamps:
                continue

            start_stamp = mid_stamp
            end_stamp = start_stamp + window_stamps - 1

            edf_start = edf_total // 2 if edf_reader else 0
            edf_n = min(int(window_sec * info.sample_freq), edf_total - edf_start) if edf_reader else 0

            if "parquet" in paths:
                t, data = _timed(lambda: _read_parquet_window(paths["parquet"], info.channel_columns, start_stamp, end_stamp), reps)
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "window_scaling", "format": "parquet",
                    "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_channels, t),
                })

            if edf_reader:
                t, data = _timed(lambda s=edf_start, n=edf_n: edf_reader.read_window(s, n), reps)
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "window_scaling", "format": "edf",
                    "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_channels, t),
                })

            for h5_key, h5_read_fn in [
                ("h5_columnar", _read_h5_columnar_window),
                ("h5_rowgroup", _read_h5_rowgroup_window),
            ]:
                if h5_key not in paths:
                    continue
                t, data = _timed(
                    lambda fn=h5_read_fn, p=paths[h5_key]: fn(p, info.channel_columns, start_stamp, end_stamp),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "window_scaling", "format": h5_key,
                    "window_seconds": window_sec,
                    "wall_clock_seconds": round(t, 6),
                    **_throughput(n_samples, n_channels, t),
                })

    return results


def bench_compression(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    if "parquet" not in paths:
        return results
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.stamp_at_row(info.total_rows // 2)
    start_stamp = mid_stamp
    end_stamp = mid_stamp + window_stamps - 1
    n_channels = len(info.channel_labels)

    for comp_cfg in cfg.get("parquet_compression", []):
        codec = comp_cfg["codec"]
        level = comp_cfg.get("level")
        label = f"{codec}_{level}" if level else codec
        key = f"parquet_{label}"
        if key not in paths:
            continue

        pq_path = paths[key]
        total_size = sum(f.stat().st_size for f in pq_path.rglob("*.parquet"))
        t, data = _timed(lambda: _read_parquet_window(pq_path, info.channel_columns, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0

        none_key = "parquet_none"
        if none_key in paths:
            uncomp_size = sum(f.stat().st_size for f in paths[none_key].rglob("*.parquet"))
            ratio = round(uncomp_size / total_size, 2) if total_size > 0 else 0
        else:
            ratio = None

        results.append({
            "category": "compression", "format": "parquet",
            "codec": label,
            "file_size_bytes": total_size,
            "file_size_mib": round(total_size / (1024 * 1024), 3),
            "compression_ratio": ratio,
            "window_seconds": window_sec,
            "wall_clock_seconds": round(t, 6),
            **_throughput(n_samples, n_channels, t),
        })

    return results


def bench_precision_loss(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    if "parquet" not in paths:
        return results
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.stamp_at_row(info.total_rows // 2)
    start_stamp = mid_stamp
    end_stamp = start_stamp + window_stamps - 1

    matrix = _read_parquet_window(paths["parquet"], info.channel_columns, start_stamp, end_stamp)
    labels = list(info.channel_labels)
    channel_results = []

    for i, label in enumerate(labels):
        if i >= matrix.shape[0]:
            break
        signal = matrix[i].astype(np.float64)
        if signal.size == 0:
            continue

        phys_min = float(signal.min())
        phys_max = float(signal.max())
        phys_range = phys_max - phys_min
        if phys_range == 0:
            channel_results.append({
                "channel": label,
                "phys_min": phys_min, "phys_max": phys_max,
                "max_abs_error": 0.0, "rms_error": 0.0,
                "snr_db": float("inf"),
                "example_original": float(signal[0]),
                "example_roundtrip": float(signal[0]),
                "example_error": 0.0,
            })
            continue

        digital = np.round((signal - phys_min) / phys_range * 65535 - 32768).astype(np.int16)
        reconstructed = (digital.astype(np.float64) + 32768) / 65535 * phys_range + phys_min

        error = np.abs(signal - reconstructed)
        max_err = float(error.max())
        rms_err = float(np.sqrt(np.mean(error ** 2)))
        signal_power = float(np.mean(signal ** 2))
        noise_power = float(np.mean((signal - reconstructed) ** 2))
        snr_db = 10 * np.log10(signal_power / noise_power) if noise_power > 0 else float("inf")

        mid = len(signal) // 2
        channel_results.append({
            "channel": label,
            "phys_min": round(phys_min, 6), "phys_max": round(phys_max, 6),
            "max_abs_error": round(max_err, 8),
            "rms_error": round(rms_err, 8),
            "snr_db": round(snr_db, 2),
            "example_original": round(float(signal[mid]), 8),
            "example_roundtrip": round(float(reconstructed[mid]), 8),
            "example_error": round(float(error[mid]), 8),
        })

    avg_snr = np.mean([c["snr_db"] for c in channel_results if c["snr_db"] != float("inf")]) if channel_results else 0.0
    worst_err = max(c["max_abs_error"] for c in channel_results) if channel_results else 0.0
    results.append({
        "category": "precision_loss",
        "window_seconds": window_sec,
        "num_channels": len(channel_results),
        "worst_max_abs_error": round(worst_err, 8),
        "avg_snr_db": round(float(avg_snr), 2),
        "channels": channel_results,
    })
    return results


def bench_int32_storage(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    if "parquet" not in paths:
        return results
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.stamp_at_row(info.total_rows // 2)
    start_stamp = mid_stamp
    end_stamp = start_stamp + window_stamps - 1
    n_channels = len(info.channel_labels)
    columns = info.channel_columns

    ground_truth = _read_parquet_window(paths["parquet"], columns, start_stamp, end_stamp)
    float32_size = sum(f.stat().st_size for f in paths["parquet"].rglob("*.parquet"))
    read_methods = [
        ("numpy", "int32_calibrated", _read_int32_calibrated),
        ("numpy", "int32_nanovolt", _read_int32_nanovolt),
        ("arrow", "int32_calibrated", _read_int32_calibrated_arrow),
        ("arrow", "int32_nanovolt", _read_int32_nanovolt_arrow),
    ]

    for read_label, mode, read_fn in read_methods:
        for codec in ("zstd", "snappy", "none"):
            key = f"parquet_{mode}_{codec}"
            if key not in paths:
                continue

            pq_path = paths[key]
            total_size = sum(f.stat().st_size for f in pq_path.rglob("*.parquet"))
            ratio = float32_size / total_size if total_size > 0 else 0
            t, data = _timed(lambda: read_fn(pq_path, columns, start_stamp, end_stamp), reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0

            if ground_truth.shape == data.shape and ground_truth.size > 0:
                error = np.abs(ground_truth.astype(np.float64) - data.astype(np.float64))
                max_err = float(error.max())
                rms_err = float(np.sqrt(np.mean(error ** 2)))
                signal_power = float(np.mean(ground_truth.astype(np.float64) ** 2))
                noise_power = float(np.mean(error ** 2))
                snr_db = 10 * np.log10(signal_power / noise_power) if noise_power > 0 else float("inf")
            else:
                max_err = rms_err = 0.0
                snr_db = float("inf")

            results.append({
                "category": "int32_storage",
                "mode": mode,
                "read_method": read_label,
                "codec": codec,
                "file_size_bytes": total_size,
                "file_size_mib": round(total_size / (1024 * 1024), 3),
                "float32_size_mib": round(float32_size / (1024 * 1024), 3),
                "compression_ratio_vs_float32": round(ratio, 2),
                "window_seconds": window_sec,
                "wall_clock_seconds": round(t, 6),
                "max_abs_error_uv": round(max_err, 10),
                "rms_error_uv": round(rms_err, 10),
                "snr_vs_float32_db": round(snr_db, 2) if snr_db != float("inf") else "inf",
                **_throughput(n_samples, n_channels, t),
            })

    zstd_key = "parquet_zstd_3"
    if zstd_key in paths:
        t, _ = _timed(lambda: _read_parquet_window(paths[zstd_key], columns, start_stamp, end_stamp), reps)
        zstd_size = sum(f.stat().st_size for f in paths[zstd_key].rglob("*.parquet"))
        n_samples = int(window_sec * info.sample_freq)
        results.append({
            "category": "int32_storage",
            "mode": "float32",
            "codec": "zstd_3",
            "file_size_bytes": zstd_size,
            "file_size_mib": round(zstd_size / (1024 * 1024), 3),
            "float32_size_mib": round(float32_size / (1024 * 1024), 3),
            "compression_ratio_vs_float32": round(float32_size / zstd_size, 2) if zstd_size > 0 else 0,
            "window_seconds": window_sec,
            "wall_clock_seconds": round(t, 6),
            "max_abs_error_uv": 0.0,
            "rms_error_uv": 0.0,
            "snr_vs_float32_db": "inf",
            **_throughput(n_samples, n_channels, t),
        })

    return results


def bench_tuned_comparison(info, paths: dict, cfg: dict) -> list[dict]:
    import time

    results = []
    sample_freq = info.sample_freq
    n_channels = len(info.channel_labels)
    ch_cols = info.channel_columns
    reps = cfg.get("repetitions", 3)

    total_stamps = info.total_rows
    mid_stamp = info.stamp_at_row(total_stamps // 2)

    block_sizes = _get_tuned_block_sizes(cfg, sample_freq)
    variants = []
    for label in block_sizes:
        pq_key_snappy = f"tuned_pq_{label}"
        pq_key_lz4 = f"tuned_pq_lz4_{label}"
        h5_key = f"tuned_h5_{label}"
        if pq_key_snappy in paths:
            variants.append((pq_key_snappy, label, "parquet_snappy", paths[pq_key_snappy]))
        if pq_key_lz4 in paths:
            variants.append((pq_key_lz4, label, "parquet_lz4", paths[pq_key_lz4]))
        if h5_key in paths:
            variants.append((h5_key, label, "hdf5_lz4", paths[h5_key]))

    if not variants:
        print("    [skip] No tuned variants found.")
        return results

    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * sample_freq)
    start_stamp = mid_stamp
    end_stamp = mid_stamp + window_stamps - 1

    print(f"\n  --- J.1: Random access ({window_sec}s at 50%) ---")
    for key, block_label, codec, path in variants:
        if "pq" in key:
            t, data = _timed(lambda p=path: _read_tuned_pq(p, ch_cols, start_stamp, end_stamp), reps)
        else:
            t, data = _timed(lambda p=path: _read_h5_columnar_window(p, ch_cols, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({
            "category": "tuned_random_access", "format": codec,
            "block_size": block_label, "variant": key,
            "window_seconds": window_sec,
            "wall_clock_seconds": round(t, 6),
            **_throughput(n_samples, n_channels, t),
        })

    print(f"\n  --- J.2: Channel subset (4 ch, {window_sec}s) ---")
    subset_cols = ch_cols[:4]
    for key, block_label, codec, path in variants:
        if "pq" in key:
            t, data = _timed(lambda p=path: _read_tuned_pq(p, subset_cols, start_stamp, end_stamp), reps)
        else:
            t, data = _timed(lambda p=path: _read_h5_columnar_window(p, subset_cols, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({
            "category": "tuned_channel_subset", "format": codec,
            "block_size": block_label, "variant": key,
            "channels": 4, "window_seconds": window_sec,
            "wall_clock_seconds": round(t, 6),
            **_throughput(n_samples, 4, t),
        })

    print("\n  --- J.3: Window scaling ---")
    window_sizes = cfg.get("window_sizes", [10, 60, 300, 900, 3600])
    for ws in window_sizes:
        ws_stamps = int(ws * sample_freq)
        s = mid_stamp
        e = info.stamp_at_row(min(total_stamps // 2 + ws_stamps - 1, info.total_rows - 1))
        for key, block_label, codec, path in variants:
            if "pq" in key:
                t, data = _timed(lambda p=path, ss=s, ee=e: _read_tuned_pq(p, ch_cols, ss, ee), reps)
            else:
                t, data = _timed(lambda p=path, ss=s, ee=e: _read_h5_columnar_window(p, ch_cols, ss, ee), reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0
            results.append({
                "category": "tuned_window_scaling", "format": codec,
                "block_size": block_label, "variant": key,
                "window_seconds": ws,
                "wall_clock_seconds": round(t, 6),
                **_throughput(n_samples, n_channels, t),
            })

    print("\n  --- J.4: Full-study sequential read ---")
    chunk_sec = 300
    chunk_stamps = int(chunk_sec * sample_freq)
    bench_start = info.start_stamp
    bench_end = info.end_stamp
    for key, block_label, codec, path in variants:
        samples_read = 0
        t_wall_start = time.perf_counter()
        for cs, ce in _chunk_ranges(bench_start, bench_end, chunk_stamps):
            if "pq" in key:
                matrix = _read_tuned_pq(path, ch_cols, cs, ce)
            else:
                matrix = _read_h5_columnar_window(path, ch_cols, cs, ce)
            samples_read += matrix.shape[1] if matrix.ndim == 2 else 0
        t_wall = time.perf_counter() - t_wall_start
        results.append({
            "category": "tuned_full_study", "format": codec,
            "block_size": block_label, "variant": key,
            "total_samples": samples_read,
            "wall_clock_seconds": round(t_wall, 3),
            **_throughput(samples_read, n_channels, t_wall),
        })

    return results


BENCHMARKS = {
    "random_access": ("A: Random-access read position", bench_random_access),
    "channel_subset": ("B: Channel subset reads", bench_channel_subset),
    "remontage": ("C: Re-montaging", bench_remontage),
    "filter_pipeline": ("D: Full-study filter pipeline + sliding FFT", bench_filter_pipeline),
    "window_scaling": ("E: Window size scaling", bench_window_scaling),
    "compression": ("F: Compression comparison", bench_compression),
    "precision_loss": ("G: 16-bit precision loss", bench_precision_loss),
    "int32_storage": ("H: Int32 storage comparison", bench_int32_storage),
    "remote_query": ("I: Remote query — DuckDB Parquet vs EDF download", bench_remote_query),
    "tuned_comparison": ("J: Tuned Parquet vs HDF5 (matched block sizes)", bench_tuned_comparison),
}