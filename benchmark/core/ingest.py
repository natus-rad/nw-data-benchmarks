"""Ingest any supported input format into a canonical Parquet directory.

The canonical Parquet is a directory of .parquet files with columns:
  - samplestamp (int64): monotonically increasing sample index
  - ch_<label> (float32): one column per EEG channel

All downstream variant generation and StudyInfo construction work from
this canonical Parquet, so each input format only needs one ingest path.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def _detect_format(input_path: Path) -> str:
    """Return 'parquet', 'hdf5', 'edf', or 'erd' based on the input path."""
    if input_path.is_dir():
        if any(input_path.glob("*.parquet")):
            return "parquet"
        if any(input_path.glob("*.erd")):
            return "erd"
        raise ValueError(f"Directory {input_path} contains no .parquet or .erd files")
    ext = input_path.suffix.lower()
    if ext in (".h5", ".hdf5", ".he5"):
        return "hdf5"
    if ext == ".edf":
        return "edf"
    if ext == ".parquet":
        return "parquet"
    raise ValueError(f"Unrecognized format for {input_path}")


def ingest(input_path: Path, cache_dir: Path,
           sample_freq: float | None = None) -> tuple[Path, str, float]:
    """Ingest any input format into a canonical Parquet directory.

    Returns (canonical_pq_dir, detected_format, sample_freq).

    If the input is already a Parquet directory, it is used directly
    (no copy). Otherwise, the data is converted and cached.
    """
    fmt = _detect_format(input_path)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if fmt == "parquet":
        if sample_freq is None:
            raise ValueError(
                "sample_freq must be specified in the study config for Parquet input."
            )
        # Parquet input is already canonical — use it directly.
        if input_path.is_dir():
            canonical = input_path
        else:
            # Single .parquet file — wrap in a directory
            canonical = cache_dir / f"{input_path.stem}_canonical"
            if not canonical.exists():
                canonical.mkdir(parents=True, exist_ok=True)
                import shutil
                shutil.copy2(input_path, canonical / input_path.name)
            else:
                print(f"  [cached] canonical Parquet: {canonical}")
        return canonical, fmt, float(sample_freq)

    # For non-Parquet inputs, generate canonical Parquet in cache.
    canonical = cache_dir / f"{input_path.stem}_canonical"

    if canonical.exists() and any(canonical.glob("*.parquet")):
        print(f"  [cached] canonical Parquet: {canonical}")
        # Recover sample_freq from the input if not provided.
        if sample_freq is None:
            sample_freq = _recover_sample_freq(input_path, fmt)
        return canonical, fmt, float(sample_freq)

    canonical.mkdir(parents=True, exist_ok=True)
    print(f"  [ingest] {fmt} -> canonical Parquet ...")

    if fmt == "hdf5":
        sample_freq = _ingest_hdf5(input_path, canonical, sample_freq)
    elif fmt == "edf":
        sample_freq = _ingest_edf(input_path, canonical, sample_freq)
    elif fmt == "erd":
        sample_freq = _ingest_erd(input_path, canonical)

    return canonical, fmt, float(sample_freq)


def _recover_sample_freq(input_path: Path, fmt: str) -> float:
    """Recover sample_freq from the original input file (for cached canonical)."""
    if fmt == "hdf5":
        import h5py
        with h5py.File(str(input_path), "r") as hf:
            freq = float(hf.attrs.get("sample_freq", 0))
            if freq > 0:
                return freq
    elif fmt == "edf":
        from .readers import EdfFileReader
        with EdfFileReader(input_path) as edf:
            return float(edf.sample_frequency)
    raise ValueError(
        f"sample_freq must be specified in the study config for {fmt} input."
    )


def _ingest_hdf5(h5_path: Path, out_dir: Path,
                 sample_freq: float | None) -> float:
    """Read HDF5 and write as canonical Parquet. Returns sample_freq."""
    import h5py

    with h5py.File(str(h5_path), "r") as hf:
        freq = sample_freq or float(hf.attrs.get("sample_freq", 0))
        if freq <= 0:
            raise ValueError(
                f"sample_freq not found in HDF5 attributes of {h5_path}; "
                "pass sample_freq in config"
            )

        # Detect layout and read data
        if "channels" in hf and isinstance(hf["channels"], h5py.Group):
            labels = list(hf["channels"].keys())
            total_rows = hf["channels"][labels[0]].shape[0]
            ch_data = {f"ch_{lbl}": hf["channels"][lbl][:].astype(np.float32)
                       for lbl in labels}
        elif "data" in hf and len(hf["data"].shape) == 2:
            total_rows = hf["data"].shape[0]
            if "channel_labels" in hf.attrs:
                labels = list(hf.attrs["channel_labels"])
            else:
                labels = [f"ch_{i}" for i in range(hf["data"].shape[1])]
            data_matrix = hf["data"][:].astype(np.float32)
            ch_data = {f"ch_{lbl}": data_matrix[:, i]
                       for i, lbl in enumerate(labels)}
        else:
            raise ValueError(f"Cannot determine HDF5 layout for {h5_path}")

        # Samplestamp
        stamp_names = ["samplestamp", "timestamps", "time", "sample_index"]
        stamps = None
        for name in stamp_names:
            if name in hf:
                stamps = hf[name][:].astype(np.int64)
                break
        if stamps is None:
            stamps = np.arange(total_rows, dtype=np.int64)

    columns = {"samplestamp": pa.array(stamps)}
    columns.update({col: pa.array(arr) for col, arr in ch_data.items()})
    table = pa.table(columns)
    out_file = out_dir / "part_00000.parquet"
    pq.write_table(table, str(out_file), compression="snappy")
    print(f"  [ingest] wrote {out_file} ({total_rows:,} rows)")
    return freq


def _ingest_edf(edf_path: Path, out_dir: Path,
                sample_freq: float | None) -> float:
    """Read EDF and write as canonical Parquet. Returns sample_freq."""
    from .readers import EdfFileReader

    with EdfFileReader(edf_path) as edf:
        total = edf.total_samples
        freq = sample_freq or edf.sample_frequency
        labels = edf.signal_labels

        # Read all data (physical values in µV via EdfFileReader.read_all_channels)
        data = edf.read_all_channels()  # shape: (n_channels, total)

    stamps = np.arange(total, dtype=np.int64)
    columns = {"samplestamp": pa.array(stamps)}
    for i, lbl in enumerate(labels):
        columns[f"ch_{lbl}"] = pa.array(data[i].astype(np.float32))

    table = pa.table(columns)
    out_file = out_dir / "part_00000.parquet"
    pq.write_table(table, str(out_file), compression="snappy")
    print(f"  [ingest] wrote {out_file} ({total:,} rows)")
    return float(freq)


def _ingest_erd(erd_dir: Path, out_dir: Path) -> float:
    """Read ERD study directory and write as canonical Parquet.

    Returns sample_freq. Requires the nwreader package.
    """
    try:
        from nwreader import read_waveforms, inspect_waveforms
    except ImportError:
        raise ImportError(
            "nwreader is required to ingest ERD data. "
            "Install it or convert to another format first."
        )

    raw = inspect_waveforms(
        str(erd_dir), ignore_stc=True, convert=True,
        convert_time=True, pad_discont=True,
    )
    labels = list(raw.channel_labels)
    start = raw.start_stamp
    if not hasattr(raw, "end_stamp"):
        raw.end_stamp = raw.segment_plans[-1].last_stamp
    end = raw.end_stamp

    data = read_waveforms(
        str(erd_dir), ignore_stc=True, convert=True,
        convert_time=True, pad_discont=True,
        start_stamp=start, end_stamp=end,
    )

    total_rows = data.shape[1] if data.ndim == 2 else data.shape[0]
    stamps = np.arange(start, start + total_rows, dtype=np.int64)

    columns = {"samplestamp": pa.array(stamps)}
    for i, lbl in enumerate(labels):
        columns[f"ch_{lbl}"] = pa.array(data[i].astype(np.float32))

    table = pa.table(columns)
    out_file = out_dir / "part_00000.parquet"
    pq.write_table(table, str(out_file), compression="snappy")
    print(f"  [ingest] wrote {out_file} ({total_rows:,} rows)")
    return float(raw.sample_freq)

