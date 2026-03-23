from __future__ import annotations

from contextlib import nullcontext
import numpy as np

from .bench_utils import (
    _PeakRssTracker,
    _chunk_ranges,
    _full_study_duration_hours,
    _max_peak_rss,
    _peak_rss_fields,
    _print_result,
    _throughput,
    _timed,
)
from .config_helpers import (
    get_channel_subsets,
    get_core_include_canonical,
    get_default_window,
    get_parquet_compression_variants,
    get_read_positions,
    get_repetitions,
    get_tuned_chunk_sec,
    get_tuned_hdf5_compression,
    get_tuned_parquet_codecs,
    get_window_sizes,
    get_core_variants_selector,
    is_investigation_enabled,
    tuned_parquet_key,
)
from .parquet_paths import parquet_total_size_bytes
from .readers import (
    EdfFileReader,
    _edf_file,
    _read_h5_columnar_window,
    _read_h5_input_window,
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


def _target_context(target: dict):
    # Benchmark timing intentionally avoids keeping EDF readers open across
    # repeated timed iterations so EDF is measured with reopen cost included.
    return nullcontext(None)


def _normalize_reader_kind(kind: str) -> str:
    return {
        "hdf5_columnar": "h5_columnar",
        "hdf5_rowgroup": "h5_rowgroup",
    }.get(kind, kind)


def _timed_call(fn, reps: int, precision: int = 6):
    timing = _timed(fn, reps)
    median_seconds, result = timing
    first_seconds = float(getattr(timing, "first_seconds", median_seconds))
    fields = {
        "wall_clock_seconds": round(float(median_seconds), precision),
        "first_wall_clock_seconds": round(first_seconds, precision),
        **_peak_rss_fields(getattr(timing, "peak_rss_mib", None)),
    }
    all_seconds = getattr(timing, "all_seconds", None)
    if all_seconds:
        fields["timing_samples_seconds"] = [round(float(value), precision) for value in all_seconds]
    return float(median_seconds), result, fields


def _single_timing_fields(seconds: float, precision: int = 6,
                          peak_rss_mib: float | None = None) -> dict[str, float]:
    rounded = round(float(seconds), precision)
    return {
        "wall_clock_seconds": rounded,
        "first_wall_clock_seconds": rounded,
        **_peak_rss_fields(peak_rss_mib),
    }


def _read_target_window(target: dict, info, columns: list[str],
                        start_stamp: int, end_stamp: int,
                        reader_state=None) -> np.ndarray:
    kind = _normalize_reader_kind(target["reader_kind"])
    path = target["path"]
    if kind == "parquet":
        return _read_parquet_window(path, columns, start_stamp, end_stamp)
    if kind == "tuned_parquet":
        return _read_tuned_pq(path, columns, start_stamp, end_stamp)
    if kind == "h5_columnar":
        return _read_h5_columnar_window(path, columns, start_stamp, end_stamp)
    if kind == "h5_rowgroup":
        return _read_h5_rowgroup_window(path, columns, start_stamp, end_stamp)
    if kind == "hdf5_input":
        return _read_h5_input_window(path, columns, start_stamp, end_stamp)
    if kind == "edf":
        start_sample = int(start_stamp)
        n_samples = max(0, int(end_stamp) - int(start_stamp) + 1)
        channel_indices = [info.channel_columns.index(col) for col in columns]
        if reader_state is None:
            with EdfFileReader(_edf_file(path)) as reader:
                return reader.read_window(start_sample, n_samples, channel_indices)
        return reader_state.read_window(start_sample, n_samples, channel_indices)
    raise ValueError(f"Unknown target reader kind: {kind}")


def _core_targets(paths: dict, cfg: dict, category: str) -> list[dict]:
    selector = get_core_variants_selector(cfg, category)
    include_canonical = get_core_include_canonical(cfg, category)
    root_variants = list(paths.get("__root_variants__", []))
    canonical_target = paths.get("__canonical_target__")

    selected: list[dict]
    if root_variants:
        if selector == []:
            selected = []
        elif selector == "all":
            selected = list(root_variants)
        else:
            by_id = {target["variant_id"]: target for target in root_variants}
            selected = [by_id[variant_id] for variant_id in selector]
    else:
        if selector == []:
            selected = []
        elif selector != "all":
            raise ValueError(
                f"benchmarks.core.{category}.variants cannot list explicit ids when no root variants exist"
            )
        else:
            source_target = paths.get("__source_target__")
            selected = [source_target] if source_target else []

    if include_canonical and canonical_target:
        selected = list(selected) + [{**canonical_target, "sort_index": len(selected)}]
    return selected


def _core_result_fields(target: dict) -> dict:
    return {
        "format": target["artifact_id"],
        "artifact_id": target["artifact_id"],
        "variant_id": target.get("variant_id"),
        "artifact_kind": target.get("artifact_kind"),
        "format_family": target.get("format_family"),
        "display_label": target.get("display_label", target["artifact_id"]),
        "artifact_order": target.get("sort_index", 0),
    }


def _clamp_row_window(total_rows: int, start_row: int, row_count: int) -> tuple[int, int]:
    if total_rows <= 0:
        raise ValueError("Benchmark windows require at least one row")
    row_count = max(1, min(int(row_count), total_rows))
    max_start = max(total_rows - row_count, 0)
    start = min(max(int(start_row), 0), max_start)
    return start, start + row_count - 1


def _mid_row_window(total_rows: int, row_count: int) -> tuple[int, int]:
    return _clamp_row_window(total_rows, total_rows // 2, row_count)


def _position_row_window(total_rows: int, position: float, row_count: int) -> tuple[int, int]:
    return _clamp_row_window(total_rows, int(position * total_rows), row_count)


def _stamp_bounds(info, row_bounds: tuple[int, int]) -> tuple[int, int]:
    start_row, end_row = row_bounds
    return info.stamp_at_row(start_row), info.stamp_at_row(end_row)


def _read_bounds_for_target(target: dict,
                            row_bounds: tuple[int, int],
                            stamp_bounds: tuple[int, int]) -> tuple[int, int]:
    return row_bounds if target["reader_kind"] == "edf" else stamp_bounds


def _row_chunk_windows(total_rows: int, chunk_rows: int,
                       max_rows: int | None = None) -> list[tuple[int, int]]:
    if total_rows <= 0:
        return []
    bench_rows = total_rows if max_rows is None else min(max(int(max_rows), 0), total_rows)
    if bench_rows <= 0:
        return []
    chunk_rows = max(1, int(chunk_rows))
    end_row = bench_rows - 1
    return list(_chunk_ranges(0, end_row, chunk_rows))


def _run_comparison_workload_suite(info, variants: list[dict], cfg: dict,
                                   category_prefix: str, section_letter: str,
                                   skip_message: str) -> list[dict]:
    import time

    results = []

    def _append_logged_result(row: dict) -> None:
        row = dict(row)
        row["_printed_inline"] = True
        results.append(row)
        _print_result(row)

    sample_freq = info.sample_freq
    n_channels = len(info.channel_labels)
    ch_cols = info.channel_columns
    reps = get_repetitions(cfg)

    total_rows = info.total_rows

    if not variants:
        print(f"    [skip] {skip_message}")
        return results

    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * sample_freq))
    window_row_bounds = _mid_row_window(total_rows, window_rows)
    window_stamp_bounds = _stamp_bounds(info, window_row_bounds)

    print(f"\n  --- {section_letter}.1: Random access ({window_sec}s at 50%) ---")
    for variant in variants:
        with _target_context(variant) as reader_state:
            start_bound, end_bound = _read_bounds_for_target(variant, window_row_bounds, window_stamp_bounds)
            t, data, timing_fields = _timed_call(
                lambda v=variant, rs=reader_state, s=start_bound, e=end_bound: _read_target_window(v, info, ch_cols, s, e, rs),
                reps,
            )
        n_samples = data.shape[1] if data.ndim == 2 else 0
        _append_logged_result({
            "category": f"{category_prefix}_random_access",
            "benchmark": f"{section_letter}.1",
            "format": variant["format"],
            **variant["result_fields"],
            "window_seconds": window_sec,
            **timing_fields,
            **_throughput(n_samples, n_channels, t),
        })

    print(f"\n  --- {section_letter}.2: Channel subset (4 ch, {window_sec}s) ---")
    subset_cols = ch_cols[:4]
    for variant in variants:
        with _target_context(variant) as reader_state:
            start_bound, end_bound = _read_bounds_for_target(variant, window_row_bounds, window_stamp_bounds)
            t, data, timing_fields = _timed_call(
                lambda v=variant, rs=reader_state, s=start_bound, e=end_bound: _read_target_window(v, info, subset_cols, s, e, rs),
                reps,
            )
        n_samples = data.shape[1] if data.ndim == 2 else 0
        _append_logged_result({
            "category": f"{category_prefix}_channel_subset",
            "benchmark": f"{section_letter}.2",
            "format": variant["format"],
            **variant["result_fields"],
            "window_seconds": window_sec,
            **timing_fields,
            **_throughput(n_samples, 4, t),
        })

    print(f"\n  --- {section_letter}.3: Window scaling ---")
    window_sizes = get_window_sizes(cfg)
    scaling_bounds = []
    for ws in window_sizes:
        ws_rows = max(1, int(ws * sample_freq))
        row_bounds = _mid_row_window(total_rows, ws_rows)
        scaling_bounds.append((ws, row_bounds, _stamp_bounds(info, row_bounds)))

    for ws, row_bounds, stamp_bounds in scaling_bounds:
        for variant in variants:
            with _target_context(variant) as reader_state:
                start_bound, end_bound = _read_bounds_for_target(variant, row_bounds, stamp_bounds)
                t, data, timing_fields = _timed_call(
                    lambda v=variant, ss=start_bound, ee=end_bound, rs=reader_state: _read_target_window(v, info, ch_cols, ss, ee, rs),
                    reps,
                )
            n_samples = data.shape[1] if data.ndim == 2 else 0
            _append_logged_result({
                "category": f"{category_prefix}_window_scaling",
                "benchmark": f"{section_letter}.3",
                "format": variant["format"],
                **variant["result_fields"],
                "window_seconds": ws,
                **timing_fields,
                **_throughput(n_samples, n_channels, t),
            })

    print(f"\n  --- {section_letter}.4: Full-study sequential read ---")
    chunk_sec = get_tuned_chunk_sec(cfg)
    chunk_rows = max(1, int(chunk_sec * sample_freq))
    row_chunks = _row_chunk_windows(total_rows, chunk_rows)
    stamp_chunks = [_stamp_bounds(info, row_bounds) for row_bounds in row_chunks]
    for variant in variants:
        samples_read = 0
        chunk_bounds = row_chunks if variant["reader_kind"] == "edf" else stamp_chunks
        with _PeakRssTracker() as memory_tracker, _target_context(variant) as reader_state:
            t_wall_start = time.perf_counter()
            for cs, ce in chunk_bounds:
                matrix = _read_target_window(variant, info, ch_cols, cs, ce, reader_state)
                samples_read += matrix.shape[1] if matrix.ndim == 2 else 0
            t_wall = time.perf_counter() - t_wall_start
        _append_logged_result({
            "category": f"{category_prefix}_full_study",
            "benchmark": f"{section_letter}.4",
            "format": variant["format"],
            **variant["result_fields"],
            "total_samples": samples_read,
            **_single_timing_fields(t_wall, precision=3, peak_rss_mib=memory_tracker.peak_rss_mib),
            **_throughput(samples_read, n_channels, t_wall),
        })

    return results


def _tuned_comparison_variants(paths: dict, cfg: dict, sample_freq: float) -> list[dict]:
    block_sizes = _get_tuned_block_sizes(cfg, sample_freq)
    parquet_codecs = get_tuned_parquet_codecs(cfg)
    hdf5_compression = get_tuned_hdf5_compression(cfg)
    variants = []
    for label in block_sizes:
        for codec in parquet_codecs:
            pq_key = tuned_parquet_key(codec, label)
            if pq_key in paths:
                variants.append({
                    "key": pq_key,
                    "format": f"parquet_{codec}",
                    "path": paths[pq_key],
                    "reader_kind": "tuned_parquet",
                    "result_fields": {"block_size": label, "variant": pq_key},
                })
        h5_key = f"tuned_h5_{label}"
        if h5_key in paths:
            variants.append({
                "key": h5_key,
                "format": f"hdf5_{hdf5_compression}",
                "path": paths[h5_key],
                "reader_kind": "h5_columnar",
                "result_fields": {"block_size": label, "variant": h5_key},
            })
    return variants


def _baseline_comparison_variants(paths: dict) -> list[dict]:
    variants = []
    for key, reader_kind in (
        ("baseline_parquet", "parquet"),
        ("baseline_hdf5", "hdf5_input"),
        ("baseline_edf", "edf"),
    ):
        if key in paths:
            variants.append({
                "key": key,
                "format": key,
                "path": paths[key],
                "reader_kind": reader_kind,
                "result_fields": {"artifact": "Baseline input", "variant": key},
            })
    if "baseline_erd" in paths:
        print("    [skip] Benchmark K does not yet support direct ERD reads.")
    return variants


def bench_random_access(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = get_repetitions(cfg)
    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * info.sample_freq))
    positions = get_read_positions(cfg)
    total_rows = info.total_rows
    n_channels = len(info.channel_labels)
    targets = _core_targets(paths, cfg, "random_access")
    position_bounds = [
        (
            f"{int(pos * 100)}%",
            _position_row_window(total_rows, pos, window_rows),
        )
        for pos in positions
    ]
    position_bounds = [
        (label, row_bounds, _stamp_bounds(info, row_bounds))
        for label, row_bounds in position_bounds
    ]

    for target in targets:
        with _target_context(target) as reader_state:
            for label, row_bounds, stamp_bounds in position_bounds:
                start_bound, end_bound = _read_bounds_for_target(target, row_bounds, stamp_bounds)
                t, data, timing_fields = _timed_call(
                    lambda s=start_bound, e=end_bound, rs=reader_state, tgt=target: _read_target_window(
                        tgt, info, info.channel_columns, s, e, rs
                    ),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "random_access",
                    **_core_result_fields(target),
                    "position": label,
                    "window_seconds": window_sec,
                    **timing_fields,
                    **_throughput(n_samples, n_channels, t),
                })

    return results


def bench_channel_subset(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = get_repetitions(cfg)
    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * info.sample_freq))
    subsets = get_channel_subsets(cfg)
    row_bounds = _mid_row_window(info.total_rows, window_rows)
    stamp_bounds = _stamp_bounds(info, row_bounds)
    targets = _core_targets(paths, cfg, "channel_subset")
    all_cols = info.channel_columns
    n_all = len(all_cols)
    counts = sorted(set([min(s, n_all) for s in subsets] + [n_all]))

    for target in targets:
        with _target_context(target) as reader_state:
            start_bound, end_bound = _read_bounds_for_target(target, row_bounds, stamp_bounds)
            for n_ch in counts:
                ch_label = f"{n_ch}" if n_ch < n_all else "all"
                cols = all_cols[:n_ch]
                t, data, timing_fields = _timed_call(
                    lambda c=cols, rs=reader_state, tgt=target: _read_target_window(
                        tgt, info, c, start_bound, end_bound, rs
                    ),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "channel_subset",
                    **_core_result_fields(target),
                    "window_seconds": window_sec,
                    **timing_fields,
                    **_throughput(n_samples, n_ch, t),
                    "channels": ch_label,
                })

    return results


def bench_remontage(info, paths: dict, cfg: dict) -> list[dict]:
    import time

    results = []
    reps = get_repetitions(cfg)
    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * info.sample_freq))
    row_bounds = _mid_row_window(info.total_rows, window_rows)
    stamp_bounds = _stamp_bounds(info, row_bounds)
    labels = list(info.channel_labels)
    n_channels = len(labels)
    targets = _core_targets(paths, cfg, "remontage")

    for target in targets:
        with _target_context(target) as reader_state:
            start_bound, end_bound = _read_bounds_for_target(target, row_bounds, stamp_bounds)

            def run(rs=reader_state, tgt=target):
                t_read_start = time.perf_counter()
                matrix = _read_target_window(tgt, info, info.channel_columns, start_bound, end_bound, rs)
                read_sec = time.perf_counter() - t_read_start
                t_mont_start = time.perf_counter()
                derived = _apply_bipolar_montage(matrix, labels)
                montage_sec = time.perf_counter() - t_mont_start
                return matrix, derived, read_sec, montage_sec

            times_read, times_mont, peaks = [], [], []
            matrix = derived = None
            for _ in range(reps):
                with _PeakRssTracker() as memory_tracker:
                    matrix, derived, r, m = run()
                times_read.append(r)
                times_mont.append(m)
                peaks.append(memory_tracker.peak_rss_mib)

            read_sec = float(np.median(times_read))
            mont_sec = float(np.median(times_mont))
            total = read_sec + mont_sec
            n_samples = matrix.shape[1] if (matrix is not None and matrix.ndim == 2) else 0
            results.append({
                "category": "remontage",
                **_core_result_fields(target),
                "window_seconds": window_sec,
                "wall_clock_seconds": round(total, 6),
                "first_wall_clock_seconds": round(times_read[0] + times_mont[0], 6),
                "timing_samples_seconds": [round(r + m, 6) for r, m in zip(times_read, times_mont)],
                **_peak_rss_fields(_max_peak_rss(peaks)),
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
    bench_rows = min(max(1, int(hours * 3600 * sample_freq)), info.total_rows)
    bench_sec = bench_rows / sample_freq

    chunk_sec = 300
    chunk_rows = max(1, int(chunk_sec * sample_freq))
    row_chunks = _row_chunk_windows(info.total_rows, chunk_rows, max_rows=bench_rows)
    stamp_chunks = [_stamp_bounds(info, row_bounds) for row_bounds in row_chunks]

    print(f"    Study: {hours}h ({bench_sec:.0f}s), {n_channels} ch, {sample_freq} Hz")

    targets = _core_targets(paths, cfg, "filter_pipeline")

    for target in targets:
        with _PeakRssTracker() as memory_tracker, _target_context(target) as reader_state:
            t_read_total = t_mont_total = t_filt_total = 0.0
            total_samples_read = 0
            chunk_bounds = row_chunks if target["reader_kind"] == "edf" else stamp_chunks

            t_wall_start = time.perf_counter()
            for cs, ce in chunk_bounds:
                t0 = time.perf_counter()
                matrix = _read_target_window(target, info, info.channel_columns, cs, ce, reader_state)
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
                "category": "filter_pipeline_full",
                **_core_result_fields(target),
                "benchmark": "D.1",
                "duration_hours": hours,
                "duration_seconds": bench_sec,
                "sample_freq": sample_freq,
                "total_samples": total_samples_read,
                **_single_timing_fields(t_wall, precision=3, peak_rss_mib=memory_tracker.peak_rss_mib),
                "read_seconds": round(t_read_total, 3),
                "montage_seconds": round(t_mont_total, 3),
                "filter_seconds": round(t_filt_total, 3),
                **_throughput(total_samples_read, n_channels, t_wall),
            })

    fft_window_sec = 10
    fft_stride_sec = 2
    fft_window_samples = int(fft_window_sec * sample_freq)
    fft_stride_samples = int(fft_stride_sec * sample_freq)
    n_fft_windows = max(0, ((bench_rows - fft_window_samples) // fft_stride_samples) + 1)
    read_chunk_sec = 300
    read_chunk_rows = max(1, int(read_chunk_sec * sample_freq))
    row_fft_chunks = _row_chunk_windows(info.total_rows, read_chunk_rows, max_rows=bench_rows)
    stamp_fft_chunks = [_stamp_bounds(info, row_bounds) for row_bounds in row_fft_chunks]
    print(f"    FFT: {n_fft_windows} windows, {fft_window_sec}s window, {fft_stride_sec}s stride")

    for target in targets:
        with _PeakRssTracker() as memory_tracker, _target_context(target) as reader_state:
            t_read_total = t_mont_total = t_filt_total = t_fft_total = 0.0
            total_samples_read = 0
            fft_count = 0
            chunk_bounds = row_fft_chunks if target["reader_kind"] == "edf" else stamp_fft_chunks

            t_wall_start = time.perf_counter()
            tail: np.ndarray | None = None

            for cs, ce in chunk_bounds:
                t0 = time.perf_counter()
                matrix = _read_target_window(target, info, info.channel_columns, cs, ce, reader_state)
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
                    f"  [warn] D.2 {target['artifact_id']}: expected {n_fft_windows} FFT windows, "
                    f"computed {fft_count} (delta {fft_count - n_fft_windows:+d})"
                )

            results.append({
                "category": "sliding_fft_full",
                **_core_result_fields(target),
                "benchmark": "D.2",
                "duration_hours": hours,
                "duration_seconds": bench_sec,
                "sample_freq": sample_freq,
                "total_samples": total_samples_read,
                "fft_window_sec": fft_window_sec,
                "fft_stride_sec": fft_stride_sec,
                "fft_windows_expected": n_fft_windows,
                "fft_windows_computed": fft_count,
                **_single_timing_fields(t_wall, precision=3, peak_rss_mib=memory_tracker.peak_rss_mib),
                "read_seconds": round(t_read_total, 3),
                "montage_seconds": round(t_mont_total, 3),
                "filter_seconds": round(t_filt_total, 3),
                "fft_seconds": round(t_fft_total, 3),
                **_throughput(total_samples_read, n_channels, t_wall),
            })

    return results


def bench_window_scaling(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = get_repetitions(cfg)
    window_sizes = get_window_sizes(cfg)
    n_channels = len(info.channel_labels)
    total_rows = info.total_rows
    scaling_bounds = []
    for window_sec in window_sizes:
        window_rows = max(1, int(window_sec * info.sample_freq))
        if window_rows > total_rows:
            continue
        row_bounds = _mid_row_window(total_rows, window_rows)
        scaling_bounds.append((window_sec, row_bounds, _stamp_bounds(info, row_bounds)))

    for target in _core_targets(paths, cfg, "window_scaling"):
        with _target_context(target) as reader_state:
            for window_sec, row_bounds, stamp_bounds in scaling_bounds:
                start_bound, end_bound = _read_bounds_for_target(target, row_bounds, stamp_bounds)
                t, data, timing_fields = _timed_call(
                    lambda s=start_bound, e=end_bound, rs=reader_state, tgt=target: _read_target_window(
                        tgt, info, info.channel_columns, s, e, rs
                    ),
                    reps,
                )
                n_samples = data.shape[1] if data.ndim == 2 else 0
                results.append({
                    "category": "window_scaling",
                    **_core_result_fields(target),
                    "window_seconds": window_sec,
                    **timing_fields,
                    **_throughput(n_samples, n_channels, t),
                })

    return results


def bench_compression(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    if "parquet" not in paths:
        return results
    if not is_investigation_enabled(cfg, "compression"):
        print("    [skip] parquet_investigations.compression.enabled is false.")
        return results
    reps = get_repetitions(cfg)
    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * info.sample_freq))
    row_bounds = _mid_row_window(info.total_rows, window_rows)
    start_stamp, end_stamp = _stamp_bounds(info, row_bounds)
    n_channels = len(info.channel_labels)

    for comp_cfg in get_parquet_compression_variants(cfg):
        codec = comp_cfg["codec"]
        level = comp_cfg.get("level")
        label = f"{codec}_{level}" if level else codec
        key = f"parquet_{label}"
        if key not in paths:
            continue

        pq_path = paths[key]
        total_size = parquet_total_size_bytes(pq_path)
        t, data, timing_fields = _timed_call(lambda: _read_parquet_window(pq_path, info.channel_columns, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0

        none_key = "parquet_none"
        if none_key in paths:
            uncomp_size = parquet_total_size_bytes(paths[none_key])
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
            **timing_fields,
            **_throughput(n_samples, n_channels, t),
        })

    return results


def bench_precision_loss(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    if "parquet" not in paths:
        return results
    if not is_investigation_enabled(cfg, "precision_loss"):
        print("    [skip] parquet_investigations.precision_loss.enabled is false.")
        return results
    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * info.sample_freq))
    row_bounds = _mid_row_window(info.total_rows, window_rows)
    start_stamp, end_stamp = _stamp_bounds(info, row_bounds)

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
    if not is_investigation_enabled(cfg, "int32_storage"):
        print("    [skip] parquet_investigations.int32_storage.enabled is false.")
        return results
    reps = get_repetitions(cfg)
    window_sec = get_default_window(cfg)
    window_rows = max(1, int(window_sec * info.sample_freq))
    row_bounds = _mid_row_window(info.total_rows, window_rows)
    start_stamp, end_stamp = _stamp_bounds(info, row_bounds)
    n_channels = len(info.channel_labels)
    columns = info.channel_columns

    ground_truth = _read_parquet_window(paths["parquet"], columns, start_stamp, end_stamp)
    float32_size = parquet_total_size_bytes(paths["parquet"])
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
            total_size = parquet_total_size_bytes(pq_path)
            ratio = float32_size / total_size if total_size > 0 else 0
            t, data, timing_fields = _timed_call(lambda: read_fn(pq_path, columns, start_stamp, end_stamp), reps)
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
                **timing_fields,
                "max_abs_error_uv": round(max_err, 10),
                "rms_error_uv": round(rms_err, 10),
                "snr_vs_float32_db": round(snr_db, 2) if snr_db != float("inf") else "inf",
                **_throughput(n_samples, n_channels, t),
            })

    zstd_key = "parquet_zstd_3"
    if zstd_key in paths:
        t, data, timing_fields = _timed_call(lambda: _read_parquet_window(paths[zstd_key], columns, start_stamp, end_stamp), reps)
        zstd_size = parquet_total_size_bytes(paths[zstd_key])
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({
            "category": "int32_storage",
            "mode": "float32",
            "codec": "zstd_3",
            "file_size_bytes": zstd_size,
            "file_size_mib": round(zstd_size / (1024 * 1024), 3),
            "float32_size_mib": round(float32_size / (1024 * 1024), 3),
            "compression_ratio_vs_float32": round(float32_size / zstd_size, 2) if zstd_size > 0 else 0,
            "window_seconds": window_sec,
            **timing_fields,
            "max_abs_error_uv": 0.0,
            "rms_error_uv": 0.0,
            "snr_vs_float32_db": "inf",
            **_throughput(n_samples, n_channels, t),
        })

    return results


def bench_tuned_comparison(info, paths: dict, cfg: dict) -> list[dict]:
    variants = _tuned_comparison_variants(paths, cfg, info.sample_freq)
    return _run_comparison_workload_suite(
        info,
        variants,
        cfg,
        category_prefix="tuned",
        section_letter="J",
        skip_message="No tuned variants found.",
    )


def bench_baseline_comparison(info, paths: dict, cfg: dict) -> list[dict]:
    variants = _baseline_comparison_variants(paths)
    return _run_comparison_workload_suite(
        info,
        variants,
        cfg,
        category_prefix="baseline",
        section_letter="K",
        skip_message="No supported baseline input artifacts found.",
    )


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
    "baseline_comparison": ("K: Baseline format comparison using J-style workloads", bench_baseline_comparison),
}
