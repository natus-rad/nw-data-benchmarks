#!/usr/bin/env python3
"""
Standalone EDF comparison script.
Tests random access, whole study read, subset channel read, and re-montage performance.
Requires: pyedflib, numpy
"""

import argparse
import time
import sys
from pathlib import Path
import numpy as np

from benchmark.core.azure_storage import _get_blob_service_client

try:
    import pyedflib
except ImportError:
    print("Error: pyedflib not found. Install with: pip install pyedflib")
    sys.exit(1)


def load_edf_metadata(edf_path):
    """Load EDF file metadata, filtering out annotation signals."""
    with pyedflib.EdfReader(str(edf_path)) as f:
        n_channels = f.signals_in_file
        sample_freq = f.getSampleFrequency(0)
        n_samples = f.getNSamples()[0]
        duration_sec = n_samples / sample_freq
        channel_labels = [f.getLabel(i) for i in range(n_channels)]

        # Get sample counts per channel to identify annotation signals
        samples_per_channel = [f.getNSamples()[i] for i in range(n_channels)]

    # Filter out annotation signals: they typically have much fewer samples than regular channels
    # Annotation signals in EDF+C are metadata stored as signals with very few samples (1-10)
    # while regular EEG channels have consistent sample counts
    eeg_channels = []
    eeg_samples = []
    eeg_labels = []

    # Find the most common sample count (should be the regular EEG channels)
    from collections import Counter
    sample_counts = Counter(samples_per_channel)
    most_common_count = sample_counts.most_common(1)[0][0]

    for i, (label, n_samp) in enumerate(zip(channel_labels, samples_per_channel)):
        if n_samp == most_common_count:
            eeg_channels.append(i)
            eeg_samples.append(n_samp)
            eeg_labels.append(label)

    n_annotation = n_channels - len(eeg_channels)

    return {
        "n_channels": n_channels,
        "n_eeg_channels": len(eeg_channels),
        "n_annotation_channels": n_annotation,
        "eeg_channel_indices": eeg_channels,
        "sample_freq": sample_freq,
        "n_samples": n_samples,
        "samples_per_channel": samples_per_channel,
        "duration_sec": duration_sec,
        "channel_labels": channel_labels,
        "eeg_labels": eeg_labels,
    }


def benchmark_random_access(edf_path, metadata, n_trials=5):
    """Benchmark random 1-minute reads at different positions (EEG channels only).
    Opens a fresh reader for each read to measure file open overhead."""
    sample_freq = metadata["sample_freq"]
    n_samples = metadata["n_samples"]
    eeg_indices = metadata["eeg_channel_indices"]
    samples_per_channel = metadata["samples_per_channel"]
    window_samples = int(60 * sample_freq)  # 1 minute

    if window_samples > n_samples:
        print(f"  Warning: 1-minute window ({window_samples}) > file duration ({n_samples})")
        window_samples = n_samples // 2

    positions = [0, n_samples // 4, n_samples // 2, 3 * n_samples // 4]
    times = []

    # Open a fresh reader for each read (measures file open overhead)
    for pos in positions:
        start_idx = max(0, min(pos, n_samples - window_samples))

        start = time.perf_counter()
        for _ in range(n_trials):
            # Open fresh reader for each trial
            with pyedflib.EdfReader(str(edf_path)) as f:
                # Read only EEG channels (not annotation signals), using digital=False
                _ = [f.readSignal(ch, start=start_idx, n=window_samples, digital=False) for ch in eeg_indices]
        elapsed = time.perf_counter() - start

        avg_time = elapsed / n_trials
        # Calculate actual bytes read: sum of bytes per channel
        actual_bytes = sum(min(window_samples, samples_per_channel[ch]) * 4 for ch in eeg_indices)
        throughput = actual_bytes / (1024 * 1024) / avg_time
        times.append(avg_time)
        print(f"    Position {pos:>10}: {avg_time:.4f}s ({throughput:.1f} MiB/s)")

    return {"median": np.median(times), "times": times}


def benchmark_full_read(edf_path, metadata):
    """Benchmark reading entire file (EEG channels only).
    Opens a fresh reader to measure file open overhead."""
    eeg_indices = metadata["eeg_channel_indices"]
    samples_per_channel = metadata["samples_per_channel"]

    start = time.perf_counter()
    with pyedflib.EdfReader(str(edf_path)) as f:
        _ = [f.readSignal(i, digital=False) for i in eeg_indices]
    elapsed = time.perf_counter() - start

    # Calculate actual bytes read per channel
    total_bytes = sum(samples_per_channel[ch] * 4 for ch in eeg_indices)
    throughput = total_bytes / (1024 * 1024) / elapsed
    print(f"    Full read: {elapsed:.3f}s ({throughput:.1f} MiB/s)")
    return elapsed


def benchmark_channel_subset(edf_path, metadata, subset_sizes=[4, 10]):
    """Benchmark reading subset of EEG channels.
    Opens a fresh reader to measure file open overhead."""
    results = {}
    eeg_indices = metadata["eeg_channel_indices"]
    samples_per_channel = metadata["samples_per_channel"]
    n_eeg = len(eeg_indices)

    for n_channels in subset_sizes:
        if n_channels > n_eeg:
            continue

        start = time.perf_counter()
        with pyedflib.EdfReader(str(edf_path)) as f:
            # Read first n_channels from the EEG channels
            subset_indices = eeg_indices[:n_channels]
            _ = [f.readSignal(i, digital=False) for i in subset_indices]
        elapsed = time.perf_counter() - start

        # Calculate actual bytes read for this subset
        total_bytes = sum(samples_per_channel[ch] * 4 for ch in subset_indices)
        throughput = total_bytes / (1024 * 1024) / elapsed
        results[n_channels] = elapsed
        print(f"    {n_channels} channels: {elapsed:.3f}s ({throughput:.1f} MiB/s)")

    return results


def benchmark_remontage(edf_path, metadata):
    """Benchmark re-montaging (read + linear combination) for EEG channels.
    Opens a fresh reader to measure file open overhead."""
    eeg_indices = metadata["eeg_channel_indices"]
    samples_per_channel = metadata["samples_per_channel"]
    n_eeg = len(eeg_indices)

    if n_eeg < 2:
        print("    Skipped (need at least 2 EEG channels)")
        return None

    start = time.perf_counter()
    with pyedflib.EdfReader(str(edf_path)) as f:
        data = np.array([f.readSignal(i, digital=False) for i in eeg_indices])
    read_time = time.perf_counter() - start

    # Simple montage: compute differences between adjacent channels
    start = time.perf_counter()
    _ = np.diff(data, axis=0)
    montage_time = time.perf_counter() - start

    total_time = read_time + montage_time
    # Calculate actual bytes read for EEG channels
    total_bytes = sum(samples_per_channel[ch] * 4 for ch in eeg_indices)
    throughput = total_bytes / (1024 * 1024) / total_time
    print(f"    Read: {read_time:.3f}s, Montage: {montage_time:.3f}s, Total: {total_time:.3f}s ({throughput:.1f} MiB/s)")
    return {"read": read_time, "montage": montage_time, "total": total_time}


def maybe_download_edf_files(cfgs: dict, output_dir: Path = Path("./data/")):
    azure_cfg = cfgs["azure"]
    azure_dir = azure_cfg["folder"].rstrip("/") + "/"
    azure_files = azure_cfg["files"]
    azure_paths = [azure_dir + azure_file for azure_file in azure_files]

    # ensure local edf dir exists
    local_dir = output_dir / azure_dir
    if not local_dir.exists():
        local_dir.mkdir(parents=True, exist_ok=True)

    # get blob service client
    client = _get_blob_service_client(cfgs)
    container = cfgs["azure"]["container"]
    container_client = client.get_container_client(container)

    # download each file if it does not exist
    for azure_edf_path in azure_paths:
        output_path = output_dir / azure_edf_path
        if not output_path.exists():
            with open(output_path, "wb") as f:
                container_client.download_blob(azure_edf_path).readinto(f)
    return [output_dir / edf_file for edf_file in azure_paths]


def main():
    azure_cfg = {
        "azure": {
            "storage_account": "nwcsandboxstorage",
            "container": "waveforms",
            "folder": "edf",
            "files": ["Suppression.edf", "Suppression_annotated.edf"],
            "anonymous": True,
        }
    }

    # ensure local edfs exist, download if not
    data_dir = Path("./data")
    local_edfs = maybe_download_edf_files(azure_cfg, output_dir=data_dir)

    for edf_file in local_edfs:
        print(f"File: {edf_file.name}")
        meta = load_edf_metadata(edf_file)
        print(f"  Total channels: {meta['n_channels']} (EEG: {meta['n_eeg_channels']}, Annotation: {meta['n_annotation_channels']})")
        print(f"  Sample rate: {meta['sample_freq']} Hz, Duration: {meta['duration_sec']:.1f}s")
        if meta['n_annotation_channels'] > 0:
            print(f"  Note: Filtering out {meta['n_annotation_channels']} annotation signal(s) from benchmarks\n")
        else:
            print()

        print("  A. Random Access (1-minute reads):")
        benchmark_random_access(edf_file, meta)

        print("\n  B. Full Study Read:")
        benchmark_full_read(edf_file, meta)

        print("\n  C. Channel Subset:")
        benchmark_channel_subset(edf_file, meta)

        print("\n  D. Re-montage:")
        benchmark_remontage(edf_file, meta)
        print()


if __name__ == "__main__":
    main()
