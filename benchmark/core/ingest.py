"""Ingest any supported input format into a canonical Parquet file.

The canonical Parquet is a single .parquet file with columns:
  - samplestamp (int64): monotonically increasing sample index
  - ch_<label> (float32): one column per EEG channel

All downstream variant generation and StudyInfo construction work from
this canonical Parquet, so each input format only needs one ingest path.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_paths import list_parquet_files


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


def _spec_hash(payload: dict) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:10]


def _row_group_size(sample_freq: float, row_group_minutes: int | None) -> int | None:
    if not row_group_minutes:
        return None
    return max(1, int(float(sample_freq) * 60 * int(row_group_minutes)))


def _canonical_file(cache_dir: Path, input_path: Path, fmt: str,
                    sample_freq: float, canonical_cfg: dict,
                    study_name: str | None = None) -> Path:
    payload = {
        "source": str(Path(input_path)),
        "format": fmt,
        "sample_freq": float(sample_freq),
        "canonical": {
            "compression": canonical_cfg.get("compression", "snappy"),
            "row_group_minutes": canonical_cfg.get("row_group_minutes", 30),
        },
    }
    token = _spec_hash(payload)
    stem = study_name or (input_path.stem if input_path.is_file() else input_path.name)
    safe_stem = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)[:48]
    return cache_dir / f"{safe_stem}_canonical_{token}.parquet"


def _write_table(table: pa.Table, out_file: Path, compression: str,
                 row_group_size: int | None) -> None:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    kwargs = {"compression": compression}
    if row_group_size is not None:
        kwargs["row_group_size"] = row_group_size
    pq.write_table(table, str(out_file), **kwargs)


def _rewrite_parquet_input(src_path: Path, out_file: Path, compression: str,
                           row_group_size: int | None) -> int:
    src_files = list_parquet_files(src_path)
    if not src_files:
        raise ValueError(f"No Parquet files found under {src_path}")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    total_rows = 0
    try:
        for src_file in src_files:
            parquet_file = pq.ParquetFile(str(src_file))
            schema = parquet_file.schema_arrow
            if writer is None:
                writer = pq.ParquetWriter(str(out_file), schema, compression=compression)
            for batch in parquet_file.iter_batches():
                table = pa.Table.from_batches([batch], schema=schema)
                total_rows += table.num_rows
                writer.write_table(table, row_group_size=row_group_size)
    finally:
        if writer is not None:
            writer.close()
    return total_rows


def ingest(input_path: Path, cache_dir: Path,
           sample_freq: float | None = None,
           canonical_cfg: dict | None = None,
           study_name: str | None = None) -> tuple[Path, str, float]:
    """Ingest any input format into a canonical Parquet file.

    Returns (canonical_pq_file, detected_format, sample_freq).

    Canonical Parquet is always materialized into cache, including for
    Parquet input.
    """
    fmt = _detect_format(input_path)
    cache_dir.mkdir(parents=True, exist_ok=True)
    canonical_cfg = canonical_cfg or {"compression": "snappy", "row_group_minutes": 30}

    if fmt == "parquet":
        if sample_freq is None:
            raise ValueError(
                "sample_freq must be specified in the study config for Parquet input."
            )
    elif sample_freq is None:
        sample_freq = _recover_sample_freq(input_path, fmt)

    canonical = _canonical_file(
        cache_dir, input_path, fmt, float(sample_freq), canonical_cfg, study_name=study_name
    )
    if canonical.exists():
        print(f"  [cached] canonical Parquet: {canonical}")
        return canonical, fmt, float(sample_freq)

    print(f"  [ingest] {fmt} -> canonical Parquet ...")
    compression = str(canonical_cfg.get("compression", "snappy"))
    row_group_size = _row_group_size(float(sample_freq), canonical_cfg.get("row_group_minutes"))

    if fmt == "parquet":
        total_rows = _rewrite_parquet_input(input_path, canonical, compression, row_group_size)
        print(f"  [ingest] wrote {canonical} ({total_rows:,} rows)")
    elif fmt == "hdf5":
        sample_freq = _ingest_hdf5(input_path, canonical, sample_freq, compression, row_group_size)
    elif fmt == "edf":
        sample_freq = _ingest_edf(input_path, canonical, sample_freq, compression, row_group_size)
    elif fmt == "erd":
        sample_freq = _ingest_erd(input_path, canonical, compression, row_group_size)

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


def _ingest_hdf5(h5_path: Path, out_file: Path,
                 sample_freq: float | None, compression: str,
                 row_group_size: int | None) -> float:
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
                labels = [str(i) for i in range(hf["data"].shape[1])]
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
    _write_table(table, out_file, compression, row_group_size)
    print(f"  [ingest] wrote {out_file} ({total_rows:,} rows)")
    return freq


def _ingest_edf(edf_path: Path, out_file: Path,
                sample_freq: float | None, compression: str,
                row_group_size: int | None) -> float:
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
    _write_table(table, out_file, compression, row_group_size)
    print(f"  [ingest] wrote {out_file} ({total:,} rows)")
    return float(freq)


def _ingest_erd(erd_dir: Path, out_file: Path, compression: str,
                row_group_size: int | None) -> float:
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
    _write_table(table, out_file, compression, row_group_size)
    print(f"  [ingest] wrote {out_file} ({total_rows:,} rows)")
    return float(raw.sample_freq)

