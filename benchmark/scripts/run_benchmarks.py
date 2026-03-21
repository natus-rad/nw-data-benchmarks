#!/usr/bin/env python3
"""
EEG Format Benchmark Suite
===========================
Compares read performance, processing throughput, compression, and precision
across EDF, HDF5, and Apache Parquet for clinical EEG / PSG data.

Data source:
  - Default: downloads pre-converted float32 Parquet from Azure Blob (no SDK).
  - Optional: if the nwreader SDK is installed, can start from native ERD data.

Usage:
    python benchmark/scripts/run_benchmarks.py
    python benchmark/scripts/run_benchmarks.py --config benchmark/config/default.yaml
    python benchmark/scripts/run_benchmarks.py --categories random_access channel_subset
    python benchmark/scripts/run_benchmarks.py --dry-run
    python benchmark/scripts/run_benchmarks.py --help
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import time
from pathlib import Path
from typing import Any

import numpy as np
import psutil
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml
import h5py
import hdf5plugin

# ---------------------------------------------------------------------------
# Optional SDK — only needed when source: "erd" in config
# ---------------------------------------------------------------------------
_HAS_NWREADER = False
try:
    from nwreader import convert_waveforms
    from nwreader.waveform_convert import inspect_waveforms
    _HAS_NWREADER = True
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BYTES_PER_FLOAT32 = 4


# ===================================================================
# Config loading
# ===================================================================
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ===================================================================
# Azure Blob download / caching
# ===================================================================
def _get_blob_service_client(cfg: dict, args: argparse.Namespace):
    """Build a BlobServiceClient from config + auth.

    Priority:
      1. Anonymous access (if azure.anonymous: true in config) — no credentials needed.
      2. --sas-token flag or AZURE_STORAGE_SAS_TOKEN env var.
      3. DefaultAzureCredential (az login, managed identity, workload identity, etc.).
    """
    from azure.storage.blob import BlobServiceClient

    account = cfg["azure"]["storage_account"]
    account_url = f"https://{account}.blob.core.windows.net"

    if cfg.get("azure", {}).get("anonymous", False):
        return BlobServiceClient(account_url=account_url)  # no credential = anonymous

    sas = getattr(args, "sas_token", None) or os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    if sas:
        return BlobServiceClient(account_url=account_url, credential=sas)

    # DefaultAzureCredential (az login, managed identity, etc.)
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError as exc:
        raise RuntimeError(
            "Non-anonymous Azure access without a SAS token requires the 'azure-identity' "
            "package. Install it with:\n\n"
            "    pip install azure-identity\n\n"
            "Alternatively, provide a SAS token via the --sas-token flag or the "
            "AZURE_STORAGE_SAS_TOKEN environment variable, or set azure.anonymous: true "
            "in the config if your storage account allows anonymous access."
        ) from exc
    return BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())


def download_study(cfg: dict, study: dict, args: argparse.Namespace) -> Path:
    """Download study data from Azure Blob to the local cache.

    Supports two source types (set via study.source in config):
      - "parquet": downloads float32 Parquet partitions (no SDK needed)
      - "erd": downloads native ERD study folder (requires nwreader SDK)

    Returns the local directory containing the downloaded files.
    """
    if "local_path" in study:
        local = Path(study["local_path"])
        if not local.exists():
            raise FileNotFoundError(f"Study local_path does not exist: {local}")
        print(f"  [local] {study['name']} -> {local}")
        return local

    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    source = study.get("source", "parquet")

    if source == "parquet":
        prefix = study["remote_parquet_url"].rstrip("/")
        study_cache = cache_dir / Path(prefix).name
        check_glob = "*.parquet"
    else:
        prefix = study["blob_prefix"].rstrip("/")
        study_cache = cache_dir / study["name"]
        check_glob = "*.erd"

    if study_cache.exists() and any(study_cache.glob(check_glob)):
        print(f"  [cached] {study['name']} -> {study_cache}")
        return study_cache

    study_cache.mkdir(parents=True, exist_ok=True)
    container = cfg["azure"]["container"]

    print(f"  [download] {study['name']} from {container}/{prefix} ...")
    client = _get_blob_service_client(cfg, args)
    container_client = client.get_container_client(container)

    count = 0
    for blob in container_client.list_blobs(name_starts_with=prefix):
        rel = blob.name[len(prefix):].lstrip("/")
        if not rel:
            continue
        local_path = study_cache / rel
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            container_client.download_blob(blob).readinto(f)
        count += 1
        print(f"    {rel} ({blob.size / 1024 / 1024:.1f} MiB)")
    print(f"  [download] {count} files -> {study_cache}")
    return study_cache


# ===================================================================
# Study setup — convert / derive all format variants
# ===================================================================
def setup_study(study_dir: Path, cfg: dict, cache_dir: Path,
                source_type: str = "parquet",
                study_cfg: dict | None = None) -> dict:
    """Set up all format variants for benchmarking. Returns a paths dict.

    When source_type is "parquet", study_dir already contains float32 Parquet
    files. EDF and other variants are derived from this data.
    When source_type is "erd", the nwreader SDK converts ERD -> EDF + Parquet.

    Args:
        study_cfg: The per-study config dict. An optional ``sample_freq`` key
            is used when converting Parquet -> EDF so the EDF header has the
            correct sampling rate. Ignored for ERD sources.
    """
    # Shorten directory names to avoid Windows path length issues
    raw_name = study_dir.name
    name = raw_name[:40] if len(raw_name) > 40 else raw_name
    output_base = cache_dir / f"{name}_exports"
    output_base.mkdir(parents=True, exist_ok=True)

    # Extract sample_freq override from study config (Parquet source only)
    cfg_freq = None
    if study_cfg and "sample_freq" in study_cfg:
        cfg_freq = float(study_cfg["sample_freq"])

    paths = {"source": study_dir}

    if source_type == "parquet":
        # Source is already float32 Parquet — use it directly
        paths["parquet"] = study_dir
        paths["parquet_snappy"] = study_dir

        # Derive EDF from Parquet data
        edf_path = output_base / f"{name}.edf"
        if not edf_path.exists():
            print("  [convert] Parquet -> EDF ...")
            _parquet_to_edf(study_dir, edf_path,
                            sample_freq=cfg_freq if cfg_freq is not None else 256.0)
        paths["edf"] = edf_path

        # Additional Parquet compression variants for benchmark F
        _setup_parquet_compression_variants(paths, study_dir, output_base, name, cfg)

    else:
        # ERD source — requires nwreader SDK
        if not _HAS_NWREADER:
            raise ImportError(
                "nwreader SDK is required for source: 'erd'. "
                "Install it or use source: 'parquet' with a remote_parquet_url."
            )
        edf_path = output_base / f"{name}.edf"
        if not edf_path.exists():
            print(f"  [convert] {name} -> EDF ...")
            convert_waveforms(str(study_dir), str(edf_path),
                              format="edf", ignore_stc=True)
        paths["edf"] = edf_path

        for comp_cfg in cfg.get("parquet_compression", [{"codec": "snappy"}]):
            codec = comp_cfg["codec"]
            level = comp_cfg.get("level")
            label = f"{codec}_{level}" if level else codec
            pq_path = output_base / f"parquet_{label}"
            if not pq_path.exists() or not any(pq_path.glob("*.parquet")):
                print(f"  [convert] {name} -> Parquet ({label}) ...")
                compression = None if codec == "none" else codec
                convert_waveforms(str(study_dir), str(pq_path),
                                  format="parquet", ignore_stc=True,
                                  compression=compression)
            paths[f"parquet_{label}"] = pq_path

        default_key = next((k for k in paths if k.startswith("parquet_")), None)
        if default_key:
            paths["parquet"] = paths[default_key]

    # Int32 Parquet variants (derived from float32 export)
    if "int32_storage" in cfg.get("benchmarks", []) and paths.get("parquet"):
        _setup_int32_variants(paths, output_base, name)

    return paths


def _parquet_to_edf(pq_dir: Path, edf_path: Path,
                    sample_freq: float = 256.0) -> None:
    """Convert float32 Parquet files to a single EDF file.

    Streams the conversion one Parquet partition at a time so that peak memory
    is bounded by the size of a single partition rather than the whole study.

    EDF headers require per-channel physical_min/max before any data is written,
    so this function makes two passes over the Parquet files:
      Pass 1 — compute per-channel min/max (stats only, no vstack).
      Pass 2 — write the header, then stream each partition to EDF via
               repeated writeSamples() calls (pyedflib appends on each call).

    Args:
        pq_dir: Directory containing source float32 .parquet files.
        edf_path: Destination .edf file path.
        sample_freq: Sampling frequency in Hz. Defaults to 256. Pass the
            value from the study config when the source is Parquet so that
            the EDF header reflects the correct rate.
    """
    import pyedflib

    pq_files = sorted(pq_dir.glob("*.parquet"))
    if not pq_files:
        raise FileNotFoundError(f"No Parquet files in {pq_dir}")

    # Read schema to get channel info
    schema = pq.read_schema(str(pq_files[0]))
    ch_cols = [c for c in schema.names if c.startswith("ch_")]
    labels = [c[3:] for c in ch_cols]
    n_channels = len(labels)

    # --- Pass 1: streaming min/max per channel (no full dataset in memory) ---
    ch_min = np.full(n_channels, np.inf)
    ch_max = np.full(n_channels, -np.inf)
    for f in pq_files:
        t = pq.read_table(str(f), columns=ch_cols)
        for i, col in enumerate(ch_cols):
            arr = t.column(col).to_numpy(zero_copy_only=False).astype(np.float64)
            ch_min[i] = min(ch_min[i], float(arr.min()))
            ch_max[i] = max(ch_max[i], float(arr.max()))
    # Guard against flat channels
    flat = ch_min == ch_max
    ch_max[flat] = ch_min[flat] + 1.0

    # --- Pass 2: write header then stream partitions into EDF ---
    edf_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pyedflib.EdfWriter(str(edf_path), n_channels, file_type=0)
    try:
        for i, label in enumerate(labels):
            writer.setSignalHeader(i, {
                "label": label,
                "dimension": "uV",
                "sample_frequency": sample_freq,
                "physical_min": ch_min[i],
                "physical_max": ch_max[i],
                "digital_min": -32768,
                "digital_max": 32767,
                "transducer": "",
                "prefilter": "",
            })
        for f in pq_files:
            t = pq.read_table(str(f), columns=ch_cols)
            block = [
                t.column(col).to_numpy(zero_copy_only=False).astype(np.float64)
                for col in ch_cols
            ]
            writer.writeSamples(block)
    finally:
        writer.close()


def _setup_parquet_compression_variants(paths: dict, src_dir: Path,
                                        output_base: Path, name: str,
                                        cfg: dict) -> None:
    """Re-compress source Parquet with different codecs for benchmark F."""
    import pyarrow as pa

    src_files = sorted(src_dir.glob("*.parquet"))
    if not src_files:
        return

    for comp_cfg in cfg.get("parquet_compression", []):
        codec = comp_cfg["codec"]
        level = comp_cfg.get("level")
        label = f"{codec}_{level}" if level else codec
        out_dir = output_base / f"parquet_{label}"

        if out_dir.exists() and any(out_dir.glob("*.parquet")):
            paths[f"parquet_{label}"] = out_dir
            continue

        # Skip if this is the same as the source (snappy)
        if codec == "snappy" and not level:
            paths[f"parquet_{label}"] = src_dir
            continue

        print(f"  [convert] {name} -> Parquet ({label}) ...")
        out_dir.mkdir(parents=True, exist_ok=True)
        compression = None if codec == "none" else codec

        output_files = []
        for src_file in src_files:
            table = pq.read_table(str(src_file))
            out_file = out_dir / src_file.name
            pq.write_table(table, str(out_file), compression=compression,
                           compression_level=level)
            output_files.append(src_file.name)

        # Create _metadata and _common_metadata for multi-file datasets
        # These files enable fast filtered reads by allowing parquet clients to skip
        # irrelevant dataset files.
        if len(output_files) > 1:
            _write_parquet_dataset_metadata(out_dir, output_files)

        paths[f"parquet_{label}"] = out_dir


# Nanovolt scale: 1 int32 unit = 0.001 µV (i.e. 1 nV).
# Range: ±2,147,483 µV — far beyond any EEG amplifier.
NANOVOLT_SCALE = 0.001  # µV per int32 unit


def _write_parquet_dataset_metadata(out_dir: Path, output_files: list[str]) -> None:
    """Write _metadata and _common_metadata files for a multi-file Parquet dataset.

    Parquet standard metadata files improve readback performance. Without them,
    PyArrow must open every file to read footers. With them, PyArrow reads one small
    metadata file and can skip irrelevant files entirely.

    Args:
        out_dir: Directory containing the Parquet files
        output_files: List of Parquet file basenames (e.g., ["segment_00000.parquet", ...])
    """
    if not output_files:
        return

    # Read schema from first file
    first_file = out_dir / output_files[0]
    schema = pq.read_schema(str(first_file))

    # Write _common_metadata (schema only)
    pq.write_metadata(schema, str(out_dir / "_common_metadata"))

    # Write _metadata (combined row-group metadata from all files)
    combined_meta = None
    for basename in output_files:
        file_path = out_dir / basename
        file_meta = pq.read_metadata(str(file_path))
        file_meta.set_file_path(basename)
        if combined_meta is None:
            combined_meta = file_meta
        else:
            combined_meta.append_row_groups(file_meta)

    if combined_meta is not None:
        combined_meta.write_metadata_file(str(out_dir / "_metadata"))


def _setup_int32_variants(paths: dict, output_base: Path, name: str) -> None:
    """Create int32 Parquet variants from the default float32 export."""
    import pyarrow as pa

    src_path = paths["parquet"]

    for mode in ("int32_calibrated", "int32_nanovolt"):
        for codec in ("zstd", "snappy", "none"):
            label = f"{mode}_{codec}"
            out_path = output_base / f"parquet_{label}"
            if out_path.exists() and any(out_path.glob("*.parquet")):
                paths[f"parquet_{label}"] = out_path
                continue

            print(f"  [convert] {name} -> Parquet ({label}) ...")
            out_path.mkdir(parents=True, exist_ok=True)

            src_files = sorted(src_path.glob("*.parquet"))

            # For calibrated mode, compute global min/max across ALL files first
            global_calibration = {}
            if mode == "int32_calibrated":
                ch_cols = None
                for src_file in src_files:
                    t = pq.read_table(str(src_file))
                    if ch_cols is None:
                        ch_cols = [c for c in t.column_names if c not in ("samplestamp", "is_gap")]
                    for col_name in ch_cols:
                        arr = t.column(col_name).to_numpy().astype(np.float64)
                        if arr.size == 0:
                            continue
                        prev = global_calibration.get(col_name, {"min": float("inf"), "max": float("-inf")})
                        global_calibration[col_name] = {
                            "min": min(prev["min"], float(arr.min())),
                            "max": max(prev["max"], float(arr.max())),
                        }
                # Convert min/max to gain/offset
                for col_name in list(global_calibration.keys()):
                    mn = global_calibration[col_name]["min"]
                    mx = global_calibration[col_name]["max"]
                    val_range = mx - mn
                    gain = val_range / 2_000_000_000 if val_range > 0 else 1.0
                    global_calibration[col_name] = {"gain": gain, "offset": mn}

            output_files = []
            for src_file in src_files:
                table = pq.read_table(str(src_file))
                schema_meta = dict(table.schema.metadata or {})
                ch_cols = [c for c in table.column_names if c not in ("samplestamp", "is_gap")]

                if mode == "int32_calibrated":
                    new_columns = {}
                    for col_name in ch_cols:
                        arr = table.column(col_name).to_numpy().astype(np.float64)
                        cal = global_calibration.get(col_name, {"gain": 1.0, "offset": 0.0})
                        digital = np.round((arr - cal["offset"]) / cal["gain"]).astype(np.int32)
                        new_columns[col_name] = pa.array(digital)
                    schema_meta[b"int32_calibration"] = json.dumps(global_calibration).encode("utf-8")

                elif mode == "int32_nanovolt":
                    new_columns = {}
                    for col_name in ch_cols:
                        arr = table.column(col_name).to_numpy().astype(np.float64)
                        digital = np.round(arr / NANOVOLT_SCALE).astype(np.int32)
                        new_columns[col_name] = pa.array(digital)
                    schema_meta[b"int32_scale_uv"] = str(NANOVOLT_SCALE).encode("utf-8")

                # Build new table with int32 channel columns
                new_cols_list = []
                new_fields = []
                for col_name in table.column_names:
                    if col_name in new_columns:
                        new_cols_list.append(new_columns[col_name])
                        new_fields.append(pa.field(col_name, pa.int32()))
                    else:
                        new_cols_list.append(table.column(col_name))
                        new_fields.append(table.schema.field(col_name))

                new_schema = pa.schema(new_fields, metadata=schema_meta)
                new_table = pa.table(
                    {f.name: c for f, c in zip(new_fields, new_cols_list)},
                    schema=new_schema,
                )
                compression = None if codec == "none" else codec
                pq.write_table(new_table, str(out_path / src_file.name),
                               compression=compression)
                output_files.append(src_file.name)

            # Create _metadata and _common_metadata for multi-file datasets
            # These files enable fast filtered reads by allowing parquet clients
            # to skip irrelevant files.
            if len(output_files) > 1:
                _write_parquet_dataset_metadata(out_path, output_files)

            paths[f"parquet_{label}"] = out_path


# ===================================================================
# HDF5 conversion — two layouts
# ===================================================================
# Row-group size used in Parquet (300 seconds at 256 Hz = 76800 samples)
H5_CHUNK_SAMPLES = 76800


def _setup_h5_variants(paths: dict, output_base: Path, name: str, info) -> None:
    """Create two HDF5 layouts from the cached Parquet float32 snappy data.

    Layout 1 — column-oriented: one 1D dataset per channel under /channels/,
               plus /samplestamp.  Chunked along time, LZ4 compressed.
    Layout 2 — row-group-aligned: single 2D dataset (samples × channels)
               chunked to match Parquet row-group boundaries, LZ4 compressed.
    """
    src_path = paths["parquet"]
    src_files = sorted(src_path.glob("*.parquet"))
    if not src_files:
        return

    # Discover channel columns from first file
    first_table = pq.read_table(str(src_files[0]), columns=[])
    all_cols = [c for c in pq.read_schema(str(src_files[0])).names
                if c.startswith("ch_")]
    ch_labels = [c[3:] for c in all_cols]  # strip "ch_" prefix
    n_channels = len(all_cols)
    sample_freq = info.sample_freq

    for layout in ("h5_columnar", "h5_rowgroup"):
        h5_path = output_base / f"{name}.{layout}.h5"
        if h5_path.exists():
            paths[layout] = h5_path
            print(f"  [cached] {layout} -> {h5_path}")
            continue

        print(f"  [convert] {name} -> HDF5 ({layout}) ...")

        with h5py.File(str(h5_path), "w") as hf:
            # Store metadata as attributes
            hf.attrs["sample_freq"] = sample_freq
            hf.attrs["channel_labels"] = ch_labels
            hf.attrs["layout"] = layout
            hf.attrs["n_channels"] = n_channels

            if layout == "h5_columnar":
                _write_h5_columnar(hf, src_files, all_cols)
            else:
                _write_h5_rowgroup(hf, src_files, all_cols, n_channels)

        paths[layout] = h5_path
        size_mib = h5_path.stat().st_size / (1024 * 1024)
        print(f"  [convert] {layout}: {size_mib:.1f} MiB")


def _build_chunk_index(stamps_ds: h5py.Dataset) -> np.ndarray:
    """Build a chunk-level min/max stamp index after all data is written.

    Returns an (n_chunks, 3) int64 array: [chunk_start_idx, min_stamp, max_stamp].
    This is the HDF5 analog of Parquet's row-group column statistics.
    """
    total = stamps_ds.shape[0]
    chunk_size = stamps_ds.chunks[0]
    n_chunks = (total + chunk_size - 1) // chunk_size
    index = np.empty((n_chunks, 3), dtype=np.int64)
    for i in range(n_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total)
        chunk_stamps = stamps_ds[start:end]
        index[i] = [start, int(chunk_stamps[0]), int(chunk_stamps[-1])]
    return index


def _write_h5_columnar(hf: h5py.File, src_files: list, all_cols: list[str]) -> None:
    """Layout 1: one 1D dataset per channel, LZ4 compressed.

    Stores samplestamp as a dataset and builds a chunk-level stamp index
    so readers can skip irrelevant chunks (like Parquet row-group stats).
    """
    total_rows = 0
    for f in src_files:
        meta = pq.ParquetFile(str(f)).metadata
        total_rows += meta.num_rows

    chunk_size = min(H5_CHUNK_SAMPLES, total_rows)

    grp = hf.create_group("channels")
    ch_datasets = {}
    for col in all_cols:
        label = col[3:]  # strip "ch_"
        ds = grp.create_dataset(
            label, shape=(total_rows,), dtype=np.float32,
            chunks=(chunk_size,), **hdf5plugin.LZ4(),
        )
        ch_datasets[col] = ds

    stamp_ds = hf.create_dataset(
        "samplestamp", shape=(total_rows,), dtype=np.int64,
        chunks=(chunk_size,), **hdf5plugin.LZ4(),
    )

    offset = 0
    for src_file in src_files:
        table = pq.read_table(str(src_file), columns=["samplestamp"] + all_cols)
        n = table.num_rows
        stamp_ds[offset:offset + n] = table.column("samplestamp").to_numpy()
        for col in all_cols:
            ch_datasets[col][offset:offset + n] = (
                table.column(col).to_numpy().astype(np.float32, copy=False)
            )
        offset += n

    # Build chunk-level stamp index (small — one row per chunk)
    index = _build_chunk_index(stamp_ds)
    hf.create_dataset("chunk_index", data=index)
    hf.attrs["total_samples"] = total_rows


def _write_h5_rowgroup(hf: h5py.File, src_files: list,
                       all_cols: list[str], n_channels: int) -> None:
    """Layout 2: single 2D dataset (samples × channels), row-group-aligned chunks.

    Same chunk index approach as columnar layout.
    """
    total_rows = 0
    for f in src_files:
        meta = pq.ParquetFile(str(f)).metadata
        total_rows += meta.num_rows

    chunk_size = min(H5_CHUNK_SAMPLES, total_rows)

    data_ds = hf.create_dataset(
        "data", shape=(total_rows, n_channels), dtype=np.float32,
        chunks=(chunk_size, n_channels), **hdf5plugin.LZ4(),
    )
    stamp_ds = hf.create_dataset(
        "samplestamp", shape=(total_rows,), dtype=np.int64,
        chunks=(chunk_size,), **hdf5plugin.LZ4(),
    )
    hf.attrs["column_order"] = all_cols

    offset = 0
    for src_file in src_files:
        table = pq.read_table(str(src_file), columns=["samplestamp"] + all_cols)
        n = table.num_rows
        stamp_ds[offset:offset + n] = table.column("samplestamp").to_numpy()
        block = np.column_stack([
            table.column(col).to_numpy().astype(np.float32, copy=False)
            for col in all_cols
        ])
        data_ds[offset:offset + n, :] = block
        offset += n

    index = _build_chunk_index(stamp_ds)
    hf.create_dataset("chunk_index", data=index)
    hf.attrs["total_samples"] = total_rows


# ===================================================================
# Tuned format variants — matched chunk/row-group sizes for fair comparison
# ===================================================================
# Row-group / chunk sizes to test (in samples).
# 300s is the current default; 60m and 120m test whether larger blocks help.
# Default block sizes to test. Override via tuned_block_sizes_minutes in config.
DEFAULT_TUNED_BLOCK_MINUTES = [5, 10, 20, 30, 60, 120]


def _get_tuned_block_sizes(cfg: dict, sample_freq: float) -> dict[str, int]:
    """Build {label: samples} dict from config or defaults."""
    minutes = cfg.get("tuned_block_sizes_minutes", DEFAULT_TUNED_BLOCK_MINUTES)
    sizes = {}
    for m in minutes:
        if m < 1:
            label = f"{int(m * 60)}s"
        else:
            label = f"{m}m"
        sizes[label] = int(m * 60 * sample_freq)
    return sizes


def _setup_tuned_variants(paths: dict, output_base: Path, info,
                          cfg: dict) -> None:
    """Create Parquet and HDF5 columnar variants with different block sizes.

    Parquet uses snappy (its fastest decompression codec).
    HDF5 uses LZ4 (its fastest decompression codec).
    Each format uses its best-performing codec so we compare container
    overhead, not codec speed.
    """
    src_path = paths.get("parquet")
    if not src_path:
        return

    src_files = sorted(Path(src_path).glob("*.parquet"))
    if not src_files:
        return

    schema = pq.read_schema(str(src_files[0]))
    ch_cols = [c for c in schema.names if c.startswith("ch_")]
    block_sizes = _get_tuned_block_sizes(cfg, info.sample_freq)

    for label, block_samples in block_sizes.items():
        _setup_tuned_parquet(paths, output_base, src_files, ch_cols,
                            label, block_samples)
        _setup_tuned_h5(paths, output_base, src_files, ch_cols,
                        label, block_samples, info)


def _setup_tuned_parquet(paths, output_base, src_files, ch_cols,
                         label, row_group_size):
    """Write single consolidated Parquet files with a specific row-group size.

    Creates both snappy and lz4 variants for comparison.

    Streams source partitions one at a time through a row-accumulating buffer
    so that peak memory is bounded by ~2 source partitions rather than the
    whole dataset.  The buffer is flushed in exact row_group_size slices, which
    ensures correct row-group boundaries even when source partitions are smaller
    than the target row-group size (e.g. writing 10m row groups from 5m
    partitions).  Without this, a naïve per-file write_table() call would
    produce undersized row groups at every partition boundary, invalidating the
    Section J comparison.
    """
    import pyarrow as pa

    schema = pq.read_schema(str(src_files[0]))

    key_snappy = f"tuned_pq_{label}"
    out_file_snappy = output_base / f"tuned_pq_{label}.parquet"
    key_lz4 = f"tuned_pq_lz4_{label}"
    out_file_lz4 = output_base / f"tuned_pq_lz4_{label}.parquet"

    need_snappy = not out_file_snappy.exists()
    need_lz4 = not out_file_lz4.exists()

    if need_snappy:
        print(f"  [convert] tuned Parquet (rg={label}, snappy) ...")
    if need_lz4:
        print(f"  [convert] tuned Parquet (rg={label}, lz4) ...")

    if need_snappy or need_lz4:
        w_snappy = (pq.ParquetWriter(str(out_file_snappy), schema,
                                     compression="snappy",
                                     write_statistics=True)
                    if need_snappy else None)
        w_lz4 = (pq.ParquetWriter(str(out_file_lz4), schema,
                                   compression="lz4",
                                   write_statistics=True)
                 if need_lz4 else None)
        try:
            buf: list[pa.Table] = []
            buf_rows = 0

            def _flush(table: pa.Table) -> None:
                if w_snappy:
                    w_snappy.write_table(table)
                if w_lz4:
                    w_lz4.write_table(table)

            for f in src_files:
                table = pq.read_table(str(f))
                buf.append(table)
                buf_rows += table.num_rows

                # Emit complete row groups as soon as the buffer is large enough.
                # After each iteration the buffer holds at most one row group's
                # worth of leftover rows (< row_group_size).
                while buf_rows >= row_group_size:
                    combined = pa.concat_tables(buf)
                    _flush(combined.slice(0, row_group_size))
                    remainder = combined.slice(row_group_size)
                    buf = [remainder] if remainder.num_rows > 0 else []
                    buf_rows = remainder.num_rows

            # Write any remaining rows as a final (possibly partial) row group.
            if buf_rows > 0:
                _flush(pa.concat_tables(buf))
        finally:
            if w_snappy:
                w_snappy.close()
            if w_lz4:
                w_lz4.close()

    # Register paths and print a one-line summary for newly written files.
    for key, out_file, was_written in (
        (key_snappy, out_file_snappy, need_snappy),
        (key_lz4, out_file_lz4, need_lz4),
    ):
        if out_file.exists():
            if was_written:
                size_mib = out_file.stat().st_size / (1024 * 1024)
                n_rg = pq.ParquetFile(str(out_file)).metadata.num_row_groups
                print(f"  [convert] {key}: {size_mib:.1f} MiB, {n_rg} row groups")
            paths[key] = out_file


def _setup_tuned_h5(paths, output_base, src_files, ch_cols,
                    label, chunk_samples, info):
    """Write an HDF5 columnar file with a specific chunk size."""
    key = f"tuned_h5_{label}"
    out_file = output_base / f"tuned_h5_{label}.h5"
    if out_file.exists():
        paths[key] = out_file
        print(f"  [cached] {key} -> {out_file}")
        return

    print(f"  [convert] tuned HDF5 columnar (chunk={label}, LZ4) ...")

    # Count total rows
    total_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in src_files)
    cs = min(chunk_samples, total_rows)

    with h5py.File(str(out_file), "w") as hf:
        hf.attrs["sample_freq"] = info.sample_freq
        hf.attrs["channel_labels"] = [c[3:] for c in ch_cols]
        hf.attrs["n_channels"] = len(ch_cols)
        hf.attrs["layout"] = "columnar"
        hf.attrs["chunk_samples"] = cs

        grp = hf.create_group("channels")
        ch_ds = {}
        for col in ch_cols:
            ch_ds[col] = grp.create_dataset(
                col[3:], shape=(total_rows,), dtype=np.float32,
                chunks=(cs,), **hdf5plugin.LZ4())

        stamp_ds = hf.create_dataset(
            "samplestamp", shape=(total_rows,), dtype=np.int64,
            chunks=(cs,), **hdf5plugin.LZ4())

        offset = 0
        for src_file in src_files:
            table = pq.read_table(str(src_file),
                                  columns=["samplestamp"] + ch_cols)
            n = table.num_rows
            stamp_ds[offset:offset + n] = (
                table.column("samplestamp").to_numpy())
            for col in ch_cols:
                ch_ds[col][offset:offset + n] = (
                    table.column(col).to_numpy().astype(np.float32, copy=False))
            offset += n

        idx = _build_chunk_index(stamp_ds)
        hf.create_dataset("chunk_index", data=idx)
        hf.attrs["total_samples"] = total_rows

    paths[key] = out_file
    size_mib = out_file.stat().st_size / (1024 * 1024)
    n_chunks = (total_rows + cs - 1) // cs
    print(f"  [convert] {key}: {size_mib:.1f} MiB, {n_chunks} chunks")


# ===================================================================
# Timing / measurement utilities
# ===================================================================
def _system_info() -> dict:
    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 1),
        "python": platform.python_version(),
    }


def _timed(fn, reps: int = 3) -> tuple[float, Any]:
    """Run fn() reps times, return (median_seconds, last_result)."""
    times = []
    result = None
    for _ in range(reps):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return float(np.median(times)), result


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


# ===================================================================
# Read helpers
# ===================================================================
class StudyInfo:
    """Study metadata — discovered from Parquet files or from the nwreader SDK."""

    def __init__(self, sample_freq: float, channel_labels: list[str],
                 start_stamp: int, end_stamp: int, n_segments: int = 1):
        self.sample_freq = sample_freq
        self.channel_labels = channel_labels
        self.channel_columns = [f"ch_{lbl}" for lbl in channel_labels]
        self.n_channels = len(channel_labels)
        self.start_stamp = start_stamp
        self.end_stamp = end_stamp
        self.n_segments = n_segments
        # For compatibility with code that checks segment_plans
        self.segment_plans = [type("Seg", (), {"last_stamp": end_stamp})()]

    @classmethod
    def from_parquet(cls, pq_dir: Path, sample_freq: float) -> "StudyInfo":
        """Discover study metadata from Parquet files on disk.

        Args:
            pq_dir: Directory containing .parquet files.
            sample_freq: Sampling frequency in Hz. Must be provided explicitly —
                the samplestamp column is an opaque monotonically increasing
                integer whose unit is not guaranteed, so inference from stamp
                deltas is unreliable and has been removed.
        """
        files = sorted(pq_dir.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No .parquet files in {pq_dir}")

        schema = pq.read_schema(str(files[0]))
        ch_cols = [c for c in schema.names if c.startswith("ch_")]
        labels = [c[3:] for c in ch_cols]

        # Read samplestamp range from first and last files
        first = pq.read_table(str(files[0]), columns=["samplestamp"])
        last = pq.read_table(str(files[-1]), columns=["samplestamp"])
        start_stamp = int(first.column("samplestamp").to_numpy().min())
        end_stamp = int(last.column("samplestamp").to_numpy().max())

        return cls(sample_freq=float(sample_freq), channel_labels=labels,
                   start_stamp=start_stamp, end_stamp=end_stamp,
                   n_segments=len(files))


def _study_info(study_dir: Path, source_type: str = "parquet",
                study_cfg: dict | None = None) -> StudyInfo:
    """Get study metadata from Parquet files or via the nwreader SDK.

    Args:
        study_dir: Path to the Parquet directory (or ERD study folder).
        source_type: ``"parquet"`` or ``"erd"``.
        study_cfg: The per-study config dict. When ``source`` is ``"parquet"``,
            ``sample_freq`` is required and must be present in this dict.
            Ignored when ``source`` is ``"erd"`` (the SDK provides the freq).
    """
    if source_type == "erd" and _HAS_NWREADER:
        raw = inspect_waveforms(str(study_dir), ignore_stc=True,
                                convert=True, convert_time=True, pad_discont=True)
        if not hasattr(raw, "end_stamp"):
            raw.end_stamp = raw.segment_plans[-1].last_stamp
        return raw

    # sample_freq is mandatory for Parquet sources
    if not study_cfg or "sample_freq" not in study_cfg:
        raise ValueError(
            "sample_freq must be specified in the study config when source is "
            "'parquet'. The samplestamp column unit is not guaranteed, so the "
            "sampling frequency cannot be inferred reliably. Add "
            "'sample_freq: <Hz>' to the study entry in your config file."
        )
    cfg_freq = float(study_cfg["sample_freq"])

    # Discover from Parquet files
    pq_dir = study_dir
    if not any(pq_dir.glob("*.parquet")):
        # Maybe the Parquet is in an exports subfolder
        for candidate in Path(study_dir).parent.glob("*_exports/parquet_*"):
            if any(candidate.glob("*.parquet")):
                pq_dir = candidate
                break
    return StudyInfo.from_parquet(pq_dir, sample_freq=cfg_freq)


def _read_parquet_window(parquet_dir: Path, columns: list[str],
                         start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read a stamp window from Parquet, return (channels, samples) float32 matrix.

    Uses Parquet row-group statistics on the samplestamp column to skip
    irrelevant row groups, then reads only the requested channel columns.
    """
    table = pq.read_table(
        str(parquet_dir), columns=columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    cols = [table.column(c).to_numpy().astype(np.float32, copy=False) for c in columns]
    return np.vstack(cols)


def _read_edf_window(edf_path: Path, start_sample: int, n_samples: int,
                     channel_indices: list[int] | None = None) -> np.ndarray:
    """Read a window from EDF, return (channels, samples) float32 matrix."""
    import pyedflib
    with pyedflib.EdfReader(str(edf_path)) as reader:
        n_ch = reader.signals_in_file
        indices = channel_indices if channel_indices is not None else list(range(n_ch))
        rows = []
        for ch in indices:
            rows.append(reader.readSignal(ch, start=start_sample, n=n_samples, digital=False))
        return np.vstack(rows).astype(np.float32, copy=False)


def _edf_total_samples(edf_path: Path) -> int:
    import pyedflib
    with pyedflib.EdfReader(str(edf_path)) as reader:
        return int(reader.getNSamples()[0])


def _edf_file(edf_path: Path) -> Path:
    """Return the .edf file path — handles both file and directory inputs."""
    if edf_path.is_file():
        return edf_path
    files = sorted(edf_path.glob("*.edf"))
    if not files:
        raise FileNotFoundError(f"No .edf files at {edf_path}")
    return files[0]


# ===================================================================
# HDF5 read helpers
# ===================================================================
def _h5_resolve_stamp_range(hf: h5py.File, start_stamp: int,
                            end_stamp: int) -> tuple[int, int]:
    """Use the chunk index to find the array index range for a stamp window.

    1. Read the chunk index (small — one row per HDF5 chunk).
    2. Find which chunks overlap [start_stamp, end_stamp].
    3. Read samplestamp only from those chunks to get exact row boundaries.

    This is the HDF5 equivalent of Parquet's row-group statistics: both
    formats maintain a small index to skip irrelevant data blocks, then
    read only what's needed.
    """
    chunk_idx = hf["chunk_index"][:]  # (n_chunks, 3): [start_idx, min_stamp, max_stamp]
    stamps_ds = hf["samplestamp"]
    chunk_size = stamps_ds.chunks[0]
    total = stamps_ds.shape[0]

    # Find chunks whose stamp range overlaps [start_stamp, end_stamp]
    overlaps = (chunk_idx[:, 1] <= end_stamp) & (chunk_idx[:, 2] >= start_stamp)
    hit_indices = np.where(overlaps)[0]
    if len(hit_indices) == 0:
        return 0, 0  # empty

    # Read samplestamp only from the first and last overlapping chunks
    first_chunk = int(hit_indices[0])
    last_chunk = int(hit_indices[-1])
    read_start = int(chunk_idx[first_chunk, 0])
    read_end = min(int(chunk_idx[last_chunk, 0]) + chunk_size, total)

    stamps = stamps_ds[read_start:read_end]
    mask = (stamps >= start_stamp) & (stamps <= end_stamp)
    positions = np.where(mask)[0]
    if len(positions) == 0:
        return 0, 0

    i_start = read_start + int(positions[0])
    i_end = read_start + int(positions[-1]) + 1
    return i_start, i_end


def _read_h5_columnar_window(h5_path: Path, columns: list[str],
                             start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read a stamp window from columnar HDF5 (one dataset per channel).

    Returns (channels, samples) float32 matrix.
    """
    with h5py.File(str(h5_path), "r") as hf:
        i_start, i_end = _h5_resolve_stamp_range(hf, start_stamp, end_stamp)
        if i_end <= i_start:
            return np.empty((len(columns), 0), dtype=np.float32)
        grp = hf["channels"]
        rows = []
        for col in columns:
            label = col[3:] if col.startswith("ch_") else col
            rows.append(grp[label][i_start:i_end])
        return np.vstack(rows).astype(np.float32, copy=False)


def _read_h5_rowgroup_window(h5_path: Path, columns: list[str],
                             start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read a stamp window from row-group-aligned HDF5 (single 2D dataset).

    Reads only the requested column indices from the 2D dataset.
    Returns (channels, samples) float32 matrix.
    """
    with h5py.File(str(h5_path), "r") as hf:
        i_start, i_end = _h5_resolve_stamp_range(hf, start_stamp, end_stamp)
        if i_end <= i_start:
            return np.empty((len(columns), 0), dtype=np.float32)
        col_order = list(hf.attrs["column_order"])
        col_indices = sorted([col_order.index(c) for c in columns])
        data = hf["data"][i_start:i_end, col_indices]
        # Reorder to match requested column order
        request_order = [col_order.index(c) for c in columns]
        reindex = [col_indices.index(ci) for ci in request_order]
        return data[:, reindex].T.astype(np.float32, copy=False)


def _h5_total_samples(h5_path: Path) -> int:
    """Return total number of samples in an HDF5 file."""
    with h5py.File(str(h5_path), "r") as hf:
        return int(hf.attrs["total_samples"])


# ===================================================================
# Montage & filter helpers
# ===================================================================
# Standard longitudinal bipolar montage pairs (10-20 subset).
BIPOLAR_PAIRS = [
    ("Fp1", "F7"), ("F7", "T3"), ("T3", "T5"), ("T5", "O1"),
    ("Fp2", "F8"), ("F8", "T4"), ("T4", "T6"), ("T6", "O2"),
    ("Fp1", "F3"), ("F3", "C3"), ("C3", "P3"), ("P3", "O1"),
    ("Fp2", "F4"), ("F4", "C4"), ("C4", "P4"), ("P4", "O2"),
    ("Fz", "Cz"), ("Cz", "Pz"),
]


def _apply_bipolar_montage(matrix: np.ndarray, labels: list[str]) -> np.ndarray:
    """Apply bipolar montage: each derived channel = ch_A - ch_B."""
    label_idx = {lbl: i for i, lbl in enumerate(labels)}
    derived = []
    for a, b in BIPOLAR_PAIRS:
        if a in label_idx and b in label_idx:
            derived.append(matrix[label_idx[a]] - matrix[label_idx[b]])
    if not derived:
        return matrix  # fallback: no matching pairs
    return np.vstack(derived)


def _build_sos(sample_freq: float) -> np.ndarray:
    """Build cascaded SOS filter: 60 Hz notch + 0.1-70 Hz bandpass."""
    from scipy.signal import butter, iirnotch, tf2sos
    b_notch, a_notch = iirnotch(60.0, 30.0, sample_freq)
    sos_notch = tf2sos(b_notch, a_notch)
    sos_bp = butter(4, [0.1, 70.0], btype="bandpass", fs=sample_freq, output="sos")
    return np.vstack([sos_notch, sos_bp])


def _apply_filters(matrix: np.ndarray, sos: np.ndarray) -> np.ndarray:
    from scipy.signal import sosfilt
    return sosfilt(sos, matrix, axis=1).astype(np.float32, copy=False)


# ===================================================================
# Benchmark A: Random-access read position
# ===================================================================
def bench_random_access(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    positions = cfg.get("read_positions", [0.0, 0.5, 0.75, 0.95])
    total_stamps = info.end_stamp - info.start_stamp
    n_channels = len(info.channel_labels)

    edf_path = _edf_file(paths["edf"])
    edf_total = _edf_total_samples(edf_path)

    for pos in positions:
        label = f"{int(pos * 100)}%"

        # Parquet
        start_stamp = info.start_stamp + int(pos * total_stamps)
        end_stamp = start_stamp + window_stamps - 1
        t, data = _timed(lambda s=start_stamp, e=end_stamp: _read_parquet_window(
            paths["parquet"], info.channel_columns, s, e), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({"category": "random_access", "format": "parquet",
                        "position": label, "window_seconds": window_sec,
                        "wall_clock_seconds": round(t, 6),
                        **_throughput(n_samples, n_channels, t)})

        # EDF
        start_sample = int(pos * edf_total)
        n_samp = min(int(window_sec * info.sample_freq), edf_total - start_sample)
        t, data = _timed(lambda s=start_sample, n=n_samp: _read_edf_window(edf_path, s, n), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({"category": "random_access", "format": "edf",
                        "position": label, "window_seconds": window_sec,
                        "wall_clock_seconds": round(t, 6),
                        **_throughput(n_samples, n_channels, t)})

        # HDF5 variants
        for h5_key, h5_read_fn in [("h5_columnar", _read_h5_columnar_window),
                                   ("h5_rowgroup", _read_h5_rowgroup_window)]:
            if h5_key not in paths:
                continue
            t, data = _timed(lambda s=start_stamp, e=end_stamp, fn=h5_read_fn, p=paths[h5_key]:
                             fn(p, info.channel_columns, s, e), reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0
            results.append({"category": "random_access", "format": h5_key,
                            "position": label, "window_seconds": window_sec,
                            "wall_clock_seconds": round(t, 6),
                            **_throughput(n_samples, n_channels, t)})

    return results


# ===================================================================
# Benchmark B: Channel subset reads
# ===================================================================
def bench_channel_subset(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    subsets = cfg.get("channel_subsets", [4, 10])
    mid_stamp = info.start_stamp + (info.end_stamp - info.start_stamp) // 2
    start_stamp = mid_stamp
    end_stamp = mid_stamp + window_stamps - 1

    edf_path = _edf_file(paths["edf"])
    edf_total = _edf_total_samples(edf_path)
    edf_start = edf_total // 2
    edf_n = min(int(window_sec * info.sample_freq), edf_total - edf_start)

    all_cols = info.channel_columns
    n_all = len(all_cols)
    counts = sorted(set([min(s, n_all) for s in subsets] + [n_all]))

    for n_ch in counts:
        ch_label = f"{n_ch}" if n_ch < n_all else "all"
        cols = all_cols[:n_ch]
        ch_indices = list(range(n_ch))

        # Parquet
        t, data = _timed(lambda c=cols: _read_parquet_window(paths["parquet"], c, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({"category": "channel_subset", "format": "parquet",
                        "channels": ch_label, "window_seconds": window_sec,
                        "wall_clock_seconds": round(t, 6),
                        **_throughput(n_samples, n_ch, t)})

        # EDF
        t, data = _timed(lambda ci=ch_indices: _read_edf_window(edf_path, edf_start, edf_n, ci), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({"category": "channel_subset", "format": "edf",
                        "channels": ch_label, "window_seconds": window_sec,
                        "wall_clock_seconds": round(t, 6),
                        **_throughput(n_samples, n_ch, t)})

        # HDF5 variants
        for h5_key, h5_read_fn in [("h5_columnar", _read_h5_columnar_window),
                                   ("h5_rowgroup", _read_h5_rowgroup_window)]:
            if h5_key not in paths:
                continue
            t, data = _timed(lambda c=cols, fn=h5_read_fn, p=paths[h5_key]:
                             fn(p, c, start_stamp, end_stamp), reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0
            results.append({"category": "channel_subset", "format": h5_key,
                            "channels": ch_label, "window_seconds": window_sec,
                            "wall_clock_seconds": round(t, 6),
                            **_throughput(n_samples, n_ch, t)})

    return results


# ===================================================================
# Benchmark C: Re-montaging
# ===================================================================
def bench_remontage(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.start_stamp + (info.end_stamp - info.start_stamp) // 2
    labels = list(info.channel_labels)
    n_channels = len(labels)

    edf_path = _edf_file(paths["edf"])
    edf_total = _edf_total_samples(edf_path)
    edf_start = edf_total // 2
    edf_n = min(int(window_sec * info.sample_freq), edf_total - edf_start)

    # Build list of formats to test
    formats_to_test = [("parquet", None), ("edf", None)]
    for h5_key, h5_fn in [("h5_columnar", _read_h5_columnar_window),
                          ("h5_rowgroup", _read_h5_rowgroup_window)]:
        if h5_key in paths:
            formats_to_test.append((h5_key, h5_fn))

    for fmt, h5_fn in formats_to_test:
        def run(f=fmt, fn=h5_fn):
            t_read_start = time.perf_counter()
            if f == "parquet":
                matrix = _read_parquet_window(paths["parquet"], info.channel_columns,
                                              mid_stamp, mid_stamp + window_stamps - 1)
            elif f == "edf":
                matrix = _read_edf_window(edf_path, edf_start, edf_n)
            else:
                matrix = fn(paths[f], info.channel_columns,
                            mid_stamp, mid_stamp + window_stamps - 1)
            read_sec = time.perf_counter() - t_read_start
            t_mont_start = time.perf_counter()
            derived = _apply_bipolar_montage(matrix, labels)
            montage_sec = time.perf_counter() - t_mont_start
            return matrix, derived, read_sec, montage_sec

        times_read, times_mont = [], []
        for _ in range(reps):
            _, derived, r, m = run()
            times_read.append(r)
            times_mont.append(m)

        read_sec = float(np.median(times_read))
        mont_sec = float(np.median(times_mont))
        total = read_sec + mont_sec
        n_samples = int(window_sec * info.sample_freq)
        results.append({
            "category": "remontage", "format": fmt,
            "window_seconds": window_sec,
            "wall_clock_seconds": round(total, 6),
            "read_seconds": round(read_sec, 6),
            "montage_seconds": round(mont_sec, 6),
            "derived_channels": derived.shape[0] if derived.ndim == 2 else 0,
            **_throughput(n_samples, n_channels, total),
        })
    return results


# ===================================================================
# Benchmark D: Full-study processing pipelines
# ===================================================================
def _full_study_duration_hours(info) -> int:
    """Return study duration rounded down to the nearest whole hour."""
    total_sec = (info.end_stamp - info.start_stamp + 1) / info.sample_freq
    return int(total_sec // 3600)


def _chunk_ranges(start_stamp: int, end_stamp: int, chunk_stamps: int):
    """Yield (chunk_start, chunk_end) stamp ranges."""
    s = start_stamp
    while s <= end_stamp:
        e = min(s + chunk_stamps - 1, end_stamp)
        yield s, e
        s = e + 1


def bench_filter_pipeline(info, paths: dict, cfg: dict) -> list[dict]:
    """D.1: Full-study read -> montage -> filter pipeline.
       D.2: Sliding-window FFT across the full study."""
    results = []
    labels = list(info.channel_labels)
    n_channels = len(labels)
    sample_freq = info.sample_freq
    sos = _build_sos(sample_freq)

    hours = _full_study_duration_hours(info)
    if hours < 1:
        hours = 1  # at least 1 hour for very short studies
    bench_stamps = int(hours * 3600 * sample_freq)
    bench_start = info.start_stamp
    bench_end = bench_start + bench_stamps - 1
    bench_sec = bench_stamps / sample_freq

    edf_path = _edf_file(paths["edf"])
    edf_total = _edf_total_samples(edf_path)
    edf_bench_samples = min(int(hours * 3600 * sample_freq), edf_total)

    # Process in 5-minute chunks to avoid memory issues
    chunk_sec = 300
    chunk_stamps = int(chunk_sec * sample_freq)
    edf_chunk_samples = int(chunk_sec * sample_freq)

    print(f"    Study: {hours}h ({bench_sec:.0f}s), {n_channels} ch, {sample_freq} Hz")

    # --- D.1: Full-study filter pipeline ---
    # Build format list: (format_key, read_fn_or_None)
    d1_formats = [("parquet", None), ("edf", None)]
    for h5_key, h5_fn in [("h5_columnar", _read_h5_columnar_window),
                          ("h5_rowgroup", _read_h5_rowgroup_window)]:
        if h5_key in paths:
            d1_formats.append((h5_key, h5_fn))

    for fmt, h5_fn in d1_formats:
        t_read_total = 0.0
        t_mont_total = 0.0
        t_filt_total = 0.0
        total_samples_read = 0

        t_wall_start = time.perf_counter()

        if fmt == "edf":
            edf_pos = 0
            while edf_pos < edf_bench_samples:
                n = min(edf_chunk_samples, edf_bench_samples - edf_pos)

                t0 = time.perf_counter()
                matrix = _read_edf_window(edf_path, edf_pos, n)
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
            # Parquet and HDF5 both use stamp-based chunking
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
        _print_result(results[-1])

    # --- D.2: Sliding-window FFT ---
    fft_window_sec = 10
    fft_stride_sec = 2
    fft_window_samples = int(fft_window_sec * sample_freq)
    fft_stride_samples = int(fft_stride_sec * sample_freq)
    n_fft_windows = int((bench_sec - fft_window_sec) / fft_stride_sec) + 1

    print(f"    FFT: {n_fft_windows} windows, {fft_window_sec}s window, {fft_stride_sec}s stride")

    # Build format list for D.2
    d2_formats = [("parquet", None), ("edf", None)]
    for h5_key, h5_fn in [("h5_columnar", _read_h5_columnar_window),
                          ("h5_rowgroup", _read_h5_rowgroup_window)]:
        if h5_key in paths:
            d2_formats.append((h5_key, h5_fn))

    for fmt, h5_fn in d2_formats:
        t_read_total = 0.0
        t_mont_total = 0.0
        t_filt_total = 0.0
        t_fft_total = 0.0
        total_samples_read = 0
        fft_count = 0

        t_wall_start = time.perf_counter()

        # Read in large chunks, then slide FFT windows within each chunk
        read_chunk_sec = 300  # read 5 min at a time
        read_chunk_stamps = int(read_chunk_sec * sample_freq)

        if fmt == "edf":
            edf_pos = 0
            edf_chunk = int(read_chunk_sec * sample_freq)
            while edf_pos < edf_bench_samples:
                n = min(edf_chunk, edf_bench_samples - edf_pos)

                t0 = time.perf_counter()
                matrix = _read_edf_window(edf_path, edf_pos, n)
                t_read_total += time.perf_counter() - t0

                t1 = time.perf_counter()
                derived = _apply_bipolar_montage(matrix, labels)
                t_mont_total += time.perf_counter() - t1

                t2 = time.perf_counter()
                filtered = _apply_filters(derived, sos)
                t_filt_total += time.perf_counter() - t2

                total_samples_read += matrix.shape[1] if matrix.ndim == 2 else 0

                n_samp = filtered.shape[1] if filtered.ndim == 2 else 0
                t3 = time.perf_counter()
                pos = 0
                while pos + fft_window_samples <= n_samp:
                    np.fft.rfft(filtered[:, pos:pos + fft_window_samples], axis=1)
                    fft_count += 1
                    pos += fft_stride_samples
                t_fft_total += time.perf_counter() - t3

                edf_pos += n
        else:
            # Parquet and HDF5 both use stamp-based chunking
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

                # Slide FFT windows within this chunk
                n_samp = filtered.shape[1] if filtered.ndim == 2 else 0
                t3 = time.perf_counter()
                pos = 0
                while pos + fft_window_samples <= n_samp:
                    np.fft.rfft(filtered[:, pos:pos + fft_window_samples], axis=1)
                    fft_count += 1
                    pos += fft_stride_samples
                t_fft_total += time.perf_counter() - t3

        t_wall = time.perf_counter() - t_wall_start

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
            "fft_windows_computed": fft_count,
            "wall_clock_seconds": round(t_wall, 3),
            "read_seconds": round(t_read_total, 3),
            "montage_seconds": round(t_mont_total, 3),
            "filter_seconds": round(t_filt_total, 3),
            "fft_seconds": round(t_fft_total, 3),
            **_throughput(total_samples_read, n_channels, t_wall),
        })
        _print_result(results[-1])

    return results


# ===================================================================
# Benchmark E: Window size scaling
# ===================================================================
def bench_window_scaling(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sizes = cfg.get("window_sizes", [10, 30, 60, 300, 900, 1800, 3600])
    n_channels = len(info.channel_labels)
    total_stamps = info.end_stamp - info.start_stamp

    edf_path = _edf_file(paths["edf"])
    edf_total = _edf_total_samples(edf_path)

    for window_sec in window_sizes:
        window_stamps = int(window_sec * info.sample_freq)
        if window_stamps > total_stamps:
            continue  # skip windows larger than the study

        # Read from mid-study
        mid_stamp = info.start_stamp + total_stamps // 2
        start_stamp = mid_stamp
        end_stamp = start_stamp + window_stamps - 1

        edf_start = edf_total // 2
        edf_n = min(int(window_sec * info.sample_freq), edf_total - edf_start)

        # Parquet
        t, data = _timed(lambda: _read_parquet_window(
            paths["parquet"], info.channel_columns, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({"category": "window_scaling", "format": "parquet",
                        "window_seconds": window_sec,
                        "wall_clock_seconds": round(t, 6),
                        **_throughput(n_samples, n_channels, t)})

        # EDF
        t, data = _timed(lambda s=edf_start, n=edf_n: _read_edf_window(edf_path, s, n), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        results.append({"category": "window_scaling", "format": "edf",
                        "window_seconds": window_sec,
                        "wall_clock_seconds": round(t, 6),
                        **_throughput(n_samples, n_channels, t)})

        # HDF5 variants
        for h5_key, h5_read_fn in [("h5_columnar", _read_h5_columnar_window),
                                   ("h5_rowgroup", _read_h5_rowgroup_window)]:
            if h5_key not in paths:
                continue
            t, data = _timed(lambda fn=h5_read_fn, p=paths[h5_key]:
                             fn(p, info.channel_columns, start_stamp, end_stamp), reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0
            results.append({"category": "window_scaling", "format": h5_key,
                            "window_seconds": window_sec,
                            "wall_clock_seconds": round(t, 6),
                            **_throughput(n_samples, n_channels, t)})

    return results


# ===================================================================
# Benchmark F: Compression comparison (Parquet only)
# ===================================================================
def bench_compression(info, paths: dict, cfg: dict) -> list[dict]:
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.start_stamp + (info.end_stamp - info.start_stamp) // 2
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

        # File size
        total_size = sum(f.stat().st_size for f in pq_path.rglob("*.parquet"))

        # Read performance
        t, data = _timed(lambda: _read_parquet_window(
            pq_path, info.channel_columns, start_stamp, end_stamp), reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0

        # Uncompressed size for ratio
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


# ===================================================================
# Benchmark G: 16-bit precision loss (EDF limitation)
# ===================================================================
def bench_precision_loss(info, paths: dict, cfg: dict) -> list[dict]:
    """Quantize float32 data to EDF 16-bit and measure round-trip error."""
    results = []
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.start_stamp + (info.end_stamp - info.start_stamp) // 2
    start_stamp = mid_stamp
    end_stamp = start_stamp + window_stamps - 1

    # Read original float32 data from Parquet (ground truth)
    matrix = _read_parquet_window(paths["parquet"], info.channel_columns,
                                  start_stamp, end_stamp)

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

        # Avoid division by zero for flat signals
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

        # EDF quantization: float -> 16-bit int -> float
        digital = np.round((signal - phys_min) / phys_range * 65535 - 32768).astype(np.int16)
        reconstructed = (digital.astype(np.float64) + 32768) / 65535 * phys_range + phys_min

        error = np.abs(signal - reconstructed)
        max_err = float(error.max())
        rms_err = float(np.sqrt(np.mean(error ** 2)))
        signal_power = float(np.mean(signal ** 2))
        noise_power = float(np.mean((signal - reconstructed) ** 2))
        snr_db = 10 * np.log10(signal_power / noise_power) if noise_power > 0 else float("inf")

        # Pick a mid-signal sample as concrete example
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

    # Summary across channels
    if channel_results:
        avg_snr = np.mean([c["snr_db"] for c in channel_results if c["snr_db"] != float("inf")])
        worst_err = max(c["max_abs_error"] for c in channel_results)
    else:
        avg_snr = 0.0
        worst_err = 0.0

    results.append({
        "category": "precision_loss",
        "window_seconds": window_sec,
        "num_channels": len(channel_results),
        "worst_max_abs_error": round(worst_err, 8),
        "avg_snr_db": round(float(avg_snr), 2),
        "channels": channel_results,
    })

    return results


# ===================================================================
# Benchmark H: Int32 storage comparison
# ===================================================================
def _read_int32_calibrated(parquet_dir: Path, columns: list[str],
                           start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read int32 calibrated Parquet and convert back to float32."""
    table = pq.read_table(
        str(parquet_dir), columns=["samplestamp"] + columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    cal = json.loads(table.schema.metadata[b"int32_calibration"].decode("utf-8"))
    # Vectorized: stack all channels, build gain/offset vectors, single multiply+add
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        g = np.float32(cal[c]["gain"])
        o = np.float32(cal[c]["offset"])
        matrix[i] = table.column(c).to_numpy().astype(np.float32) * g + o
    return matrix


def _read_int32_nanovolt(parquet_dir: Path, columns: list[str],
                         start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read int32 nanovolt Parquet and convert back to float32."""
    table = pq.read_table(
        str(parquet_dir), columns=["samplestamp"] + columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    scale = np.float32(table.schema.metadata[b"int32_scale_uv"])
    # Vectorized: read all columns into a matrix, single scalar multiply
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        matrix[i] = table.column(c).to_numpy().astype(np.float32)
    matrix *= scale
    return matrix


def _read_int32_calibrated_arrow(parquet_dir: Path, columns: list[str],
                                 start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read int32 calibrated Parquet using Arrow compute kernels (minimal copies)."""
    import pyarrow as pa

    table = pq.read_table(
        str(parquet_dir), columns=["samplestamp"] + columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    cal = json.loads(table.schema.metadata[b"int32_calibration"].decode("utf-8"))
    # Use float64 for the Arrow math to avoid int32->float32 safe-cast range errors,
    # then cast the final result to float32 once.
    cast_opts = pc.CastOptions(target_type=pa.float64(), allow_int_overflow=True)
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        col = table.column(c)
        col_f = pc.cast(col, options=cast_opts)
        col_f = pc.add(pc.multiply(col_f, cal[c]["gain"]), cal[c]["offset"])
        matrix[i] = pc.cast(col_f, pa.float32()).to_numpy(zero_copy_only=False)
    return matrix


def _read_int32_nanovolt_arrow(parquet_dir: Path, columns: list[str],
                               start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read int32 nanovolt Parquet using Arrow compute kernels (minimal copies)."""
    import pyarrow as pa

    table = pq.read_table(
        str(parquet_dir), columns=["samplestamp"] + columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    scale = float(table.schema.metadata[b"int32_scale_uv"])
    # Multiply int32 by float64 scale directly — Arrow promotes to float64 automatically
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        col = table.column(c)
        col_f = pc.multiply(col, scale)  # int32 * float64 -> float64 in Arrow
        matrix[i] = pc.cast(col_f, pa.float32()).to_numpy(zero_copy_only=False)
    return matrix


def bench_int32_storage(info, paths: dict, cfg: dict) -> list[dict]:
    """Compare int32 storage modes: file size, read performance, precision."""
    results = []
    reps = cfg.get("repetitions", 3)
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * info.sample_freq)
    mid_stamp = info.start_stamp + (info.end_stamp - info.start_stamp) // 2
    start_stamp = mid_stamp
    end_stamp = start_stamp + window_stamps - 1
    n_channels = len(info.channel_labels)
    columns = info.channel_columns

    # Read float32 ground truth
    ground_truth = _read_parquet_window(paths["parquet"], columns, start_stamp, end_stamp)

    # Float32 baseline file size
    float32_size = sum(f.stat().st_size for f in paths["parquet"].rglob("*.parquet"))

    # Read methods: (label_suffix, mode, read_fn_factory)
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

            # Read performance
            t, data = _timed(lambda: read_fn(pq_path, columns, start_stamp, end_stamp), reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0

            # Precision vs float32 ground truth
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

    # Also benchmark float32 + zstd for direct comparison
    zstd_key = "parquet_zstd_3"
    if zstd_key in paths:
        t, _ = _timed(lambda: _read_parquet_window(
            paths[zstd_key], columns, start_stamp, end_stamp), reps)
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


# ===================================================================
# Benchmark I: Remote query — DuckDB remote Parquet vs EDF download
# ===================================================================
# Standard 10-20 montage channels (19 electrodes)
CHANNELS_10_20 = [
    "Fp1", "Fp2", "F7", "F3", "Fz", "F4", "F8",
    "T3", "C3", "Cz", "C4", "T4",
    "T5", "P3", "Pz", "P4", "T6",
    "O1", "O2",
]


def _make_duckdb_connection(account: str, container: str):
    """Create a DuckDB connection configured for Azure Blob anonymous access."""
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL azure; LOAD azure;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Anonymous access — no credentials
    con.execute(f"SET azure_storage_connection_string = 'DefaultEndpointsProtocol=https;"
                f"AccountName={account};BlobEndpoint=https://{account}.blob.core.windows.net';")
    return con


def _duckdb_remote_read(con, az_path: str, columns: list[str],
                        start_stamp: int, end_stamp: int) -> tuple[float, int]:
    """Query a remote Parquet file via DuckDB Azure extension. Returns (seconds, n_rows)."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    # DuckDB azure extension uses az:// protocol
    pq_source = f"az://{az_path}*.parquet"
    query = (f"SELECT {col_list} FROM read_parquet('{pq_source}', hive_partitioning=false) "
             f"WHERE samplestamp >= {start_stamp} AND samplestamp <= {end_stamp}")

    t0 = time.perf_counter()
    result = con.execute(query).fetchnumpy()
    elapsed = time.perf_counter() - t0
    n_rows = len(next(iter(result.values()))) if result else 0
    return elapsed, n_rows


def _download_edf_from_azure(cfg: dict, edf_blob_path: str,
                             args: argparse.Namespace) -> tuple[float, Path]:
    """Download full EDF from Azure, return (seconds, local_path)."""
    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    local_path = cache_dir / "remote_edf_download" / Path(edf_blob_path).name
    local_path.parent.mkdir(parents=True, exist_ok=True)

    client = _get_blob_service_client(cfg, args)
    container = cfg["azure"]["container"]
    container_client = client.get_container_client(container)

    t0 = time.perf_counter()
    with open(local_path, "wb") as f:
        container_client.download_blob(edf_blob_path).readinto(f)
    elapsed = time.perf_counter() - t0

    return elapsed, local_path


def bench_remote_query(info, paths: dict, cfg: dict) -> list[dict]:
    """Benchmark I: Remote Parquet (DuckDB) vs Remote EDF (full download + local read).

    Simulates querying data around 10 random seizure events, each 10 minutes.
    """
    results = []
    remote_cfg = cfg.get("remote_benchmark", {})
    if not remote_cfg:
        print("    [skip] No remote_benchmark config found.")
        return results

    sample_freq = info.sample_freq
    n_channels = len(info.channel_labels)
    window_sec = remote_cfg.get("window_sec", 600)  # 10 min
    window_stamps = int(window_sec * sample_freq)
    n_points = remote_cfg.get("n_random_points", 10)
    account = cfg["azure"]["storage_account"]
    container = cfg["azure"]["container"]

    # Generate reproducible random positions
    rng = np.random.default_rng(42)
    # Keep margin so window fits within the study
    margin = window_stamps + 1
    random_starts = rng.integers(
        info.start_stamp, info.end_stamp - margin, size=n_points
    )
    random_starts.sort()

    windows = [(int(s), int(s + window_stamps - 1)) for s in random_starts]

    print(f"    {n_points} random windows × {window_sec}s = {n_points * window_sec}s total")
    print(f"    Stamps: {[f'{s}–{e}' for s, e in windows[:3]]} ...")

    # --- Determine channel subsets ---
    all_cols = info.channel_columns
    # 19-channel 10-20 subset: match by label
    label_to_col = dict(zip(info.channel_labels, info.channel_columns))
    subset_cols = [label_to_col[lbl] for lbl in CHANNELS_10_20 if lbl in label_to_col]
    n_subset = len(subset_cols)
    print(f"    All channels: {len(all_cols)}, 10-20 subset: {n_subset}")

    # --- Remote Parquet variants ---
    parquet_variants = []
    for name_label, blob_path_key in [
        ("float32_snappy", "remote_float32_path"),
        ("int32_nV_snappy", "remote_int32_nanovolt_path"),
    ]:
        blob_path = remote_cfg.get(blob_path_key)
        if not blob_path:
            continue
        # DuckDB Azure extension uses container/path format
        az_path = f"{container}/{blob_path}"
        parquet_variants.append((name_label, az_path))

    # Create one DuckDB connection for all queries
    con = _make_duckdb_connection(account, container)

    for pq_label, pq_az_path in parquet_variants:
        for ch_label, cols in [("all", all_cols), ("10-20 (19ch)", subset_cols)]:
            query_cols = ["samplestamp"] + list(cols)
            times = []
            total_rows = 0

            print(f"    DuckDB {pq_label} [{ch_label}] ... ", end="", flush=True)
            for s, e in windows:
                t, n_rows = _duckdb_remote_read(con, pq_az_path, query_cols, s, e)
                times.append(t)
                total_rows += n_rows

            total_time = sum(times)
            avg_time = total_time / len(times)
            print(f"{total_time:.1f}s ({avg_time:.2f}s avg/window)")

            n_ch = len(cols)
            results.append({
                "category": "remote_query",
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
                **_throughput(total_rows, n_ch, total_time),
            })

    con.close()

    # --- EDF: full download + local reads ---
    edf_blob_path = remote_cfg.get("remote_edf_path")
    edf_local = paths.get("edf")

    if edf_local and Path(edf_local).exists():
        edf_path = _edf_file(Path(edf_local))
        edf_size = edf_path.stat().st_size
        edf_total = _edf_total_samples(edf_path)

        # Simulate download: if blob path exists, actually download; otherwise estimate
        if edf_blob_path:
            from argparse import Namespace
            args_ns = Namespace(sas_token=None)
            print(f"    EDF download ({edf_size / 1024 / 1024:.0f} MiB) ... ", end="", flush=True)
            dl_time, dl_path = _download_edf_from_azure(cfg, edf_blob_path, args_ns)
            print(f"{dl_time:.1f}s")
            edf_read_path = dl_path
        else:
            # Estimate download time from parquet query bandwidth
            print("    EDF: no remote path configured, using local file + estimated download")
            # Use the EDF file size / measured Azure bandwidth
            # We'll measure bandwidth from one parquet query and extrapolate
            dl_time = None
            edf_read_path = edf_path

        # Now read the 10 windows from local EDF
        for ch_label, ch_indices in [("all", None), ("10-20 (19ch)", list(range(min(n_subset, n_channels))))]:
            print(f"    EDF local read [{ch_label}] ... ", end="", flush=True)
            local_times = []
            for s, e in windows:
                # Map stamp to sample index (samplestamp is a zero-based sample counter)
                start_sample = int(s - info.start_stamp)
                n_samp = min(int(window_sec * sample_freq), edf_total - start_sample)
                if start_sample < 0 or n_samp <= 0:
                    continue

                t0 = time.perf_counter()
                _read_edf_window(edf_read_path, start_sample, n_samp, ch_indices)
                local_times.append(time.perf_counter() - t0)

            local_total = sum(local_times)
            combined = (dl_time if dl_time is not None else 0) + local_total
            n_ch = len(ch_indices) if ch_indices else n_channels
            print(f"{local_total:.1f}s read" + (f" + {dl_time:.1f}s download = {combined:.1f}s" if dl_time is not None else ""))

            results.append({
                "category": "remote_query",
                "format": "edf",
                "method": "full_download_then_read",
                "channel_subset": ch_label,
                "n_channels": n_ch,
                "n_windows": n_points,
                "window_seconds": window_sec,
                "download_seconds": round(dl_time, 3) if dl_time is not None else None,
                "edf_file_size_mib": round(edf_size / (1024 * 1024), 1),
                "read_seconds": round(local_total, 3),
                "total_wall_seconds": round(combined, 3),
                "avg_wall_per_window": round(local_total / len(local_times), 3) if local_times else 0,
            })

    return results


# ===================================================================
# Benchmark J: Tuned format comparison
# ===================================================================
def _read_tuned_pq(path: Path, columns: list[str],
                   start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read from a single consolidated Parquet file."""
    table = pq.read_table(
        str(path), columns=columns,
        filters=[("samplestamp", ">=", start_stamp),
                 ("samplestamp", "<=", end_stamp)])
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    return np.vstack([
        table.column(c).to_numpy().astype(np.float32, copy=False)
        for c in columns])


def bench_tuned_comparison(info, paths: dict, cfg: dict) -> list[dict]:
    """Benchmark J: Tuned Parquet vs HDF5 with matched block sizes.

    Tests each format at multiple block sizes. Parquet tests both snappy
    and LZ4 codecs; HDF5 uses LZ4. Runs random access, channel subset,
    and window scaling sub-benchmarks.
    """
    results = []
    sample_freq = info.sample_freq
    n_channels = len(info.channel_labels)
    ch_cols = info.channel_columns
    reps = cfg.get("repetitions", 3)

    total_stamps = info.end_stamp - info.start_stamp + 1
    mid_stamp = info.start_stamp + total_stamps // 2

    # Collect all tuned variants present in paths
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

    # --- J.1: Random access at 50% position ---
    window_sec = cfg.get("default_window", 60)
    window_stamps = int(window_sec * sample_freq)
    start_stamp = mid_stamp
    end_stamp = mid_stamp + window_stamps - 1

    print(f"\n  --- J.1: Random access ({window_sec}s at 50%) ---")
    for key, block_label, codec, path in variants:
        if "pq" in key:
            t, data = _timed(
                lambda p=path: _read_tuned_pq(p, ch_cols, start_stamp, end_stamp),
                reps)
        else:
            t, data = _timed(
                lambda p=path: _read_h5_columnar_window(
                    p, ch_cols, start_stamp, end_stamp),
                reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        row = {"category": "tuned_random_access", "format": codec,
               "block_size": block_label, "variant": key,
               "window_seconds": window_sec,
               "wall_clock_seconds": round(t, 6),
               **_throughput(n_samples, n_channels, t)}
        results.append(row)
        _print_result(row)

    # --- J.2: Channel subset (4 channels) ---
    print(f"\n  --- J.2: Channel subset (4 ch, {window_sec}s) ---")
    subset_cols = ch_cols[:4]
    for key, block_label, codec, path in variants:
        if "pq" in key:
            t, data = _timed(
                lambda p=path: _read_tuned_pq(
                    p, subset_cols, start_stamp, end_stamp),
                reps)
        else:
            t, data = _timed(
                lambda p=path: _read_h5_columnar_window(
                    p, subset_cols, start_stamp, end_stamp),
                reps)
        n_samples = data.shape[1] if data.ndim == 2 else 0
        row = {"category": "tuned_channel_subset", "format": codec,
               "block_size": block_label, "variant": key,
               "channels": 4, "window_seconds": window_sec,
               "wall_clock_seconds": round(t, 6),
               **_throughput(n_samples, 4, t)}
        results.append(row)
        _print_result(row)

    # --- J.3: Window scaling ---
    print("\n  --- J.3: Window scaling ---")
    window_sizes = cfg.get("window_sizes", [10, 60, 300, 900, 3600])
    for ws in window_sizes:
        ws_stamps = int(ws * sample_freq)
        s = mid_stamp
        e = min(mid_stamp + ws_stamps - 1, info.end_stamp)
        for key, block_label, codec, path in variants:
            if "pq" in key:
                t, data = _timed(
                    lambda p=path, ss=s, ee=e: _read_tuned_pq(
                        p, ch_cols, ss, ee),
                    reps)
            else:
                t, data = _timed(
                    lambda p=path, ss=s, ee=e: _read_h5_columnar_window(
                        p, ch_cols, ss, ee),
                    reps)
            n_samples = data.shape[1] if data.ndim == 2 else 0
            row = {"category": "tuned_window_scaling", "format": codec,
                   "block_size": block_label, "variant": key,
                   "window_seconds": ws,
                   "wall_clock_seconds": round(t, 6),
                   **_throughput(n_samples, n_channels, t)}
            results.append(row)
            _print_result(row)

    # --- J.4: Full-study sequential read (all channels, chunked) ---
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
        row = {"category": "tuned_full_study", "format": codec,
               "block_size": block_label, "variant": key,
               "total_samples": samples_read,
               "wall_clock_seconds": round(t_wall, 3),
               **_throughput(samples_read, n_channels, t_wall)}
        results.append(row)
        _print_result(row)

    return results


# ===================================================================
# Benchmark registry
# ===================================================================
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


# ===================================================================
# Main runner
# ===================================================================
def run_benchmarks(cfg: dict, args: argparse.Namespace) -> dict:
    run_id = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    categories = args.categories if args.categories else cfg.get("benchmarks", list(BENCHMARKS.keys()))

    # Resolve which benchmarks to run
    selected = []
    for cat in categories:
        if cat in BENCHMARKS:
            selected.append((cat, *BENCHMARKS[cat]))
        else:
            print(f"  [warn] Unknown benchmark category: {cat}")

    # Dry run
    if args.dry_run:
        print("\n=== DRY RUN ===")
        print(f"Config: {args.config}")
        print(f"Cache dir: {cache_dir}")
        print("\nStudies:")
        for study in cfg.get("studies", []):
            src = study.get("source", "parquet")
            path = study.get("remote_parquet_url") or study.get("blob_prefix", "")
            print(f"  - {study['name']} (source: {src}): {path}")
        print(f"\nBenchmarks ({len(selected)}):")
        for cat_id, cat_name, _ in selected:
            print(f"  - [{cat_id}] {cat_name}")
        print(f"\nCompression variants ({len(cfg.get('parquet_compression', []))}):")
        for c in cfg.get("parquet_compression", []):
            lbl = f"{c['codec']}" + (f" level={c['level']}" if c.get('level') else "")
            print(f"  - {lbl}")
        print(f"\nWindow sizes: {cfg.get('window_sizes', [])}")
        print(f"Repetitions: {cfg.get('repetitions', 3)}")
        print(f"\nTotal benchmark runs: ~{_estimate_runs(cfg, selected)}")
        return {}

    # Run
    output = {
        "run_id": run_id,
        "system": _system_info(),
        "config_file": str(args.config),
        "studies": [],
        "benchmarks": [],
    }

    for study_cfg in cfg.get("studies", []):
        print(f"\n{'=' * 60}")
        print(f"Study: {study_cfg['name']}")
        print(f"{'=' * 60}")

        source_type = study_cfg.get("source", "parquet")

        # Download / cache
        study_dir = download_study(cfg, study_cfg, args)

        # Setup: convert / derive all format variants
        print("\n  --- Setup ---")
        paths = setup_study(study_dir, cfg, cache_dir, source_type=source_type,
                            study_cfg=study_cfg)

        # Get study info (from Parquet files or SDK)
        info = _study_info(paths.get("parquet", study_dir), source_type,
                           study_cfg=study_cfg)
        study_meta = {
            "name": study_cfg["name"],
            "channels": info.n_channels if hasattr(info, "n_channels") else len(info.channel_labels),
            "sample_freq": info.sample_freq,
            "start_stamp": info.start_stamp,
            "end_stamp": info.end_stamp,
            "total_stamps": info.end_stamp - info.start_stamp + 1,
            "duration_seconds": round((info.end_stamp - info.start_stamp + 1) / info.sample_freq, 1),
            "segments": info.n_segments if hasattr(info, "n_segments") else len(info.segment_plans),
        }
        output["studies"].append(study_meta)
        print(f"  {study_meta['channels']} channels, {study_meta['sample_freq']} Hz, "
              f"{study_meta['total_stamps']} stamps, {study_meta['segments']} segments")

        # HDF5 variants (derived from the default Parquet export)
        if paths.get("parquet"):
            sn = study_dir.name[:40] if len(study_dir.name) > 40 else study_dir.name
            output_base = cache_dir / f"{sn}_exports"
            _setup_h5_variants(paths, output_base, sn, info)

            # Tuned variants for benchmark J
            if "tuned_comparison" in [cat_id for cat_id, _, _ in selected]:
                _setup_tuned_variants(paths, output_base, info, cfg)

        for k, v in paths.items():
            print(f"  {k}: {v}")

        # Run each benchmark
        for cat_id, cat_name, bench_fn in selected:
            print(f"\n  --- {cat_name} ---")
            try:
                cat_results = bench_fn(info, paths, cfg)
                output["benchmarks"].extend(cat_results)
                for r in cat_results:
                    _print_result(r)
            except Exception as e:
                print(f"  [ERROR] {cat_name}: {e}")
                import traceback
                traceback.print_exc()

    # Write results
    out_dir = Path("benchmark") / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{run_id}_benchmark_results.json"
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n{'=' * 60}")
    print(f"Results written to: {out_file}")
    print(f"{'=' * 60}")

    return output


def _estimate_runs(cfg: dict, selected: list) -> int:
    """Rough estimate of total benchmark invocations."""
    n = 0
    reps = cfg.get("repetitions", 3)
    for cat_id, _, _ in selected:
        if cat_id == "random_access":
            n += len(cfg.get("read_positions", [0, 0.5, 0.75, 0.95])) * 2 * reps
        elif cat_id == "channel_subset":
            n += (len(cfg.get("channel_subsets", [4, 10])) + 1) * 2 * reps
        elif cat_id in ("remontage", "filter_pipeline"):
            n += 2 * reps
        elif cat_id == "window_scaling":
            n += len(cfg.get("window_sizes", [])) * 2 * reps
        elif cat_id == "compression":
            n += len(cfg.get("parquet_compression", [])) * reps
        elif cat_id == "precision_loss":
            n += 1  # single computation
    return n


def _print_result(r: dict) -> None:
    """Pretty-print a single benchmark result."""
    fmt = r.get("format", "")
    t = r.get("wall_clock_seconds", 0)

    mode = r.get("mode", fmt)
    parts = [f"    {mode:20s}"]
    if "read_method" in r:
        parts.append(f"via={r['read_method']:>5s}")
    if "position" in r:
        parts.append(f"pos={r['position']:>4s}")
    if "channels" in r and isinstance(r["channels"], str):
        parts.append(f"ch={r['channels']:>4s}")
    if "codec" in r:
        parts.append(f"codec={r['codec']:>8s}")
    if "window_seconds" in r:
        parts.append(f"win={r['window_seconds']:>5}s")
    parts.append(f"time={t:.4f}s")
    if "mib_per_sec" in r:
        parts.append(f"tput={r['mib_per_sec']:.1f} MiB/s")
    if "compression_ratio" in r and r["compression_ratio"] is not None:
        parts.append(f"ratio={r['compression_ratio']:.1f}x")
    if "worst_max_abs_error" in r:
        parts.append(f"worst_err={r['worst_max_abs_error']:.6f}")
    if "avg_snr_db" in r:
        parts.append(f"avg_snr={r['avg_snr_db']:.1f}dB")

    print("  ".join(parts))


# ===================================================================
# CLI
# ===================================================================
def main():
    parser = argparse.ArgumentParser(
        description="EEG Format Benchmark Suite — compare EDF, HDF5, and Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Run all benchmarks with default config (downloads data from Azure)
  python benchmark/scripts/run_benchmarks.py --config benchmark/config/default.yaml

  # Run only specific categories
  python benchmark/scripts/run_benchmarks.py --config benchmark/config/default.yaml \\
      --categories random_access channel_subset window_scaling

  # Preview what will run without executing
  python benchmark/scripts/run_benchmarks.py --config benchmark/config/default.yaml --dry-run

available categories:
  random_access    A: Read position scaling
  channel_subset   B: Columnar read advantage
  remontage        C: Read + bipolar montage
  filter_pipeline  D: Read + montage + filter + FFT (D.1, D.2)
  window_scaling   E: Window size throughput
  compression      F: Parquet codec comparison
  precision_loss   G: EDF 16-bit quantization error
  int32_storage    H: Int32 storage modes
  remote_query     I: Remote Parquet (DuckDB) vs full download
  tuned_comparison J: Tuned Parquet vs HDF5 (matched block sizes)
""",
    )
    parser.add_argument("--config", default="benchmark/config/default.yaml",
                        help="Path to YAML config file (default: benchmark/config/default.yaml)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview benchmarks without running them")
    parser.add_argument("--sas-token", default=None,
                        help="Azure SAS token for private blob access")
    parser.add_argument("--categories", nargs="*", default=None,
                        help="Run only these categories (space-separated)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_benchmarks(cfg, args)


if __name__ == "__main__":
    main()
