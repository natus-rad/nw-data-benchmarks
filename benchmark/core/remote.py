from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np

from .azure_storage import _download_edf_from_azure
from .bench_utils import _PeakRssTracker, _chunk_ranges, _peak_rss_fields, _print_result, _throughput
from .config_helpers import get_remote_query_cfg, is_investigation_enabled
from .readers import EdfFileReader, _edf_file
from .signal import CHANNELS_10_20


def _make_duckdb_connection(account: str):
    """Create a DuckDB connection configured for Azure Blob anonymous access."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL azure; LOAD azure;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"SET azure_storage_connection_string = 'DefaultEndpointsProtocol=https;"
        f"AccountName={account};BlobEndpoint=https://{account}.blob.core.windows.net';"
    )
    return con


def _duckdb_remote_read(con, az_path: str, columns: list[str],
                        start_stamp: int, end_stamp: int) -> tuple[float, int]:
    """Query a remote Parquet file via DuckDB Azure extension. Returns (seconds, n_rows).

    Handles both directory paths (with glob) and single-file paths.
    """
    col_list = ", ".join(f'"{c}"' for c in columns)
    # If path ends with .parquet, it's a single file; otherwise it's a directory pattern
    if az_path.endswith(".parquet"):
        pq_source = f"az://{az_path}"
    else:
        pq_source = f"az://{az_path}*.parquet"
    query = (
        f"SELECT {col_list} FROM read_parquet('{pq_source}', hive_partitioning=false) "
        f"WHERE samplestamp >= {start_stamp} AND samplestamp <= {end_stamp}"
    )

    t0 = time.perf_counter()
    result = con.execute(query).fetchnumpy()
    elapsed = time.perf_counter() - t0
    n_rows = len(next(iter(result.values()))) if result else 0
    return elapsed, n_rows


def bench_remote_query(info, paths: dict, cfg: dict,
                       args: argparse.Namespace | None = None) -> list[dict]:
    """Benchmark I: Remote Parquet (DuckDB) vs Remote EDF (full download + local read)."""
    results = []

    def _append_logged_result(row: dict) -> None:
        row = dict(row)
        row["_printed_inline"] = True
        results.append(row)
        _print_result(row)

    remote_cfg = get_remote_query_cfg(cfg)
    if not remote_cfg:
        print("    [skip] No parquet_investigations.remote_query config found.")
        return results
    if not is_investigation_enabled(cfg, "remote_query"):
        print("    [skip] parquet_investigations.remote_query.enabled is false.")
        return results

    sample_freq = info.sample_freq
    n_channels = len(info.channel_labels)
    window_sec = remote_cfg.get("window_sec", 600)
    window_stamps = int(window_sec * sample_freq)
    n_points = remote_cfg.get("n_random_points", 10)
    account = cfg["azure"]["storage_account"]
    container = cfg["azure"]["container"]

    rng = np.random.default_rng(42)
    # Pick random start positions as row indices so we never do stamp arithmetic.
    # max_row_start is the last row index at which a window of window_stamps rows fits.
    max_row_start = info.total_rows - window_stamps
    if max_row_start < 0:
        print(f"    [skip] Study too short for remote_query window ({window_sec}s).")
        return results
    random_row_starts = np.sort(rng.integers(0, max_row_start + 1, size=n_points))
    # Convert fixed row-index windows to actual samplestamp windows so query
    # bounds remain correct even if samplestamps have gaps or non-unit stride.
    windows = []
    for row_start in random_row_starts:
        row_start_int = int(row_start)
        row_end_int = min(row_start_int + window_stamps - 1, info.total_rows - 1)
        start_stamp = info.stamp_at_row(row_start_int)
        end_stamp = info.stamp_at_row(row_end_int)
        windows.append((start_stamp, end_stamp))

    print(f"    {n_points} random windows × {window_sec}s = {n_points * window_sec}s total")
    print(f"    Stamps: {[f'{s}–{e}' for s, e in windows[:3]]} ...")

    all_cols = info.channel_columns
    label_to_col = dict(zip(info.channel_labels, info.channel_columns))
    subset_cols = [label_to_col[lbl] for lbl in CHANNELS_10_20 if lbl in label_to_col]
    n_subset = len(subset_cols)
    channel_variants = [("all", all_cols)]
    if subset_cols:
        channel_variants.append(("10-20 (19ch)", subset_cols))
    print(f"    All channels: {len(all_cols)}, 10-20 subset: {n_subset}")

    parquet_variants = []
    for name_label, blob_path_key in [
        ("float32_snappy", "remote_float32_path"),
        ("int32_nV_snappy", "remote_int32_nanovolt_path"),
        ("single_file_lz4", "remote_single_file_path"),
    ]:
        blob_path = remote_cfg.get(blob_path_key)
        if blob_path:
            parquet_variants.append((name_label, f"{container}/{blob_path}"))

    con = _make_duckdb_connection(account)
    for pq_label, pq_az_path in parquet_variants:
        for ch_label, cols in channel_variants:
            query_cols = list(cols)
            times = []
            total_rows = 0

            print(f"    DuckDB {pq_label} [{ch_label}] ... ", end="", flush=True)
            with _PeakRssTracker() as memory_tracker:
                for s, e in windows:
                    t, n_rows = _duckdb_remote_read(con, pq_az_path, query_cols, s, e)
                    times.append(t)
                    total_rows += n_rows

            total_time = sum(times)
            avg_time = total_time / len(times)
            print(f"done ({total_time:.1f}s)")

            n_ch = len(cols)
            _append_logged_result({
                "category": "remote_query",
                "benchmark": "I.1",
                "format": f"parquet_{pq_label}",
                "method": "duckdb_remote",
                "channel_subset": ch_label,
                "n_channels": n_ch,
                "n_windows": n_points,
                "window_seconds": window_sec,
                "total_wall_seconds": round(total_time, 3),
                "avg_wall_per_window": round(avg_time, 3),
                "min_wall_per_window": round(min(times), 3),
                "max_wall_per_window": round(max(times), 3),
                "total_rows": total_rows,
                **_peak_rss_fields(memory_tracker.peak_rss_mib),
                **_throughput(total_rows, n_ch, total_time),
            })

    # Full-study sequential read via DuckDB (chunked)
    chunk_sec = remote_cfg.get("full_study_chunk_sec")
    if chunk_sec:
        chunk_stamps = int(chunk_sec * sample_freq)
        bench_start = info.stamp_at_row(0)
        bench_end = info.stamp_at_row(info.total_rows - 1)
        total_study_sec = info.total_rows / sample_freq
        n_chunks = -(-info.total_rows // chunk_stamps)  # ceiling div
        print(f"\n    Full-study sequential read ({total_study_sec:.0f}s in {n_chunks} × {chunk_sec}s chunks)")

        for pq_label, pq_az_path in parquet_variants:
            for ch_label, cols in channel_variants:
                query_cols = list(cols)
                total_rows = 0
                print(f"    DuckDB full-study {pq_label} [{ch_label}] ... ", end="", flush=True)
                with _PeakRssTracker() as memory_tracker:
                    t_wall_start = time.perf_counter()
                    for cs, ce in _chunk_ranges(bench_start, bench_end, chunk_stamps):
                        _, n_rows = _duckdb_remote_read(con, pq_az_path, query_cols, cs, ce)
                        total_rows += n_rows
                    t_wall = time.perf_counter() - t_wall_start
                print(f"done ({t_wall:.1f}s)")

                n_ch = len(cols)
                _append_logged_result({
                    "category": "remote_query_full_study",
                    "benchmark": "I.2",
                    "format": f"parquet_{pq_label}",
                    "method": "duckdb_full_study",
                    "channel_subset": ch_label,
                    "n_channels": n_ch,
                    "chunk_seconds": chunk_sec,
                    "n_chunks": n_chunks,
                    "total_rows": total_rows,
                    "total_wall_seconds": round(t_wall, 3),
                    **_peak_rss_fields(memory_tracker.peak_rss_mib),
                    **_throughput(total_rows, n_ch, t_wall),
                })

    con.close()

    edf_local = paths.get("edf")
    if not edf_local or not Path(edf_local).exists():
        return results

    edf_blob_path = remote_cfg.get("remote_edf_path")
    edf_path = _edf_file(Path(edf_local))
    edf_size = edf_path.stat().st_size

    _THEORETICAL_MBPS = 800
    if edf_blob_path:
        print(f"    EDF download ({edf_size / 1024 / 1024:.0f} MiB) ... ", end="", flush=True)
        dl_time, dl_path = _download_edf_from_azure(cfg, edf_blob_path, args)
        print(f"{dl_time:.1f}s")
        edf_read_path = dl_path
        dl_estimated = False
    else:
        dl_time = (edf_size * 8) / (_THEORETICAL_MBPS * 1e6)
        print(
            f"    EDF: no remote path configured — using local file + theoretical "
            f"{_THEORETICAL_MBPS} Mbps download ({dl_time:.1f}s for {edf_size / 1024 / 1024:.0f} MiB)"
        )
        edf_read_path = edf_path
        dl_estimated = True

    with EdfFileReader(edf_read_path) as edf_reader:
        edf_labels = [lbl.strip().upper() for lbl in edf_reader.signal_labels]
        edf_total = edf_reader.total_samples
        edf_label_to_idx = {lbl: i for i, lbl in enumerate(edf_labels)}
        matched = [edf_label_to_idx[lbl.upper()] for lbl in CHANNELS_10_20 if lbl.upper() in edf_label_to_idx]
        subset_indices = matched[:n_subset] if len(matched) >= n_subset else list(range(min(n_subset, n_channels)))
        edf_channel_variants = [("all", None)]
        if subset_indices:
            edf_channel_variants.append(("10-20 (19ch)", subset_indices))

        for ch_label, ch_indices in edf_channel_variants:
            print(f"    EDF local read [{ch_label}] ... ", end="", flush=True)
            local_times = []
            with _PeakRssTracker() as memory_tracker:
                for row_start in random_row_starts:
                    start_sample = int(row_start)
                    n_samp = min(int(window_sec * sample_freq), edf_total - start_sample)
                    if start_sample < 0 or n_samp <= 0:
                        continue

                    t0 = time.perf_counter()
                    edf_reader.read_window(start_sample, n_samp, ch_indices)
                    local_times.append(time.perf_counter() - t0)

            local_total = sum(local_times)
            combined = dl_time + local_total
            n_ch = len(ch_indices) if ch_indices else n_channels
            print(f"done ({combined:.1f}s)")

            _append_logged_result({
                "category": "remote_query",
                "benchmark": "I.1",
                "format": "edf",
                "method": "full_download_then_read",
                "channel_subset": ch_label,
                "n_channels": n_ch,
                "n_windows": n_points,
                "window_seconds": window_sec,
                "download_seconds": round(dl_time, 3),
                "download_estimated": dl_estimated,
                "edf_file_size_mib": round(edf_size / (1024 * 1024), 1),
                "read_seconds": round(local_total, 3),
                "total_wall_seconds": round(combined, 3),
                "avg_wall_per_window": round(local_total / len(local_times), 3) if local_times else 0,
                **_peak_rss_fields(memory_tracker.peak_rss_mib),
            })

    return results
