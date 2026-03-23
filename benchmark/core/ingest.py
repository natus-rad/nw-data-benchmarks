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


def _decode_hdf5_label(label: object) -> str:
    if isinstance(label, (bytes, np.bytes_)):
        return label.decode("utf-8")
    return str(label)


def _row_group_size(sample_freq: float, row_group_minutes: int | None) -> int | None:
    if not row_group_minutes:
        return None
    return max(1, int(float(sample_freq) * 60 * int(row_group_minutes)))


def _write_chunk_rows(row_group_size: int | None,
                      chunk_writer_max_rowgroups: int | None) -> int | None:
    if row_group_size is None:
        return None
    groups = int(chunk_writer_max_rowgroups) if chunk_writer_max_rowgroups else 1
    return max(1, int(row_group_size) * max(1, groups))


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
            "chunk_writer_max_rowgroups": canonical_cfg.get(
                "chunk_writer_max_rowgroups",
                canonical_cfg.get("write_row_groups_per_chunk", 1),
            ),
        },
    }
    token = _spec_hash(payload)
    stem = study_name or (input_path.stem if input_path.is_file() else input_path.name)
    safe_stem = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)[:48]
    return cache_dir / f"{safe_stem}_canonical_{token}.parquet"


def _write_table(table: pa.Table, out_file: Path, compression: str,
                 row_group_size: int | None) -> None:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        str(out_file),
        compression=compression,
        row_group_size=row_group_size,
    )


def _hdf5_batch_rows(total_rows: int, row_group_size: int | None,
                     chunk_rows: int | None = None) -> int:
    for candidate in (row_group_size, chunk_rows, 65_536):
        if candidate and int(candidate) > 0:
            return max(1, min(total_rows, int(candidate)))
    return max(1, total_rows)


def _write_streamed_tables(tables, out_file: Path, compression: str,
                           row_group_size: int | None,
                           write_chunk_rows: int | None = None) -> int:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    target_rows = int(write_chunk_rows) if write_chunk_rows and int(write_chunk_rows) > 0 else None
    if target_rows is None and row_group_size and int(row_group_size) > 0:
        target_rows = int(row_group_size)
    writer = None
    pending: list[pa.Table] = []
    pending_rows = 0
    total_rows = 0
    try:
        for table in tables:
            if writer is None:
                writer = pq.ParquetWriter(str(out_file), table.schema, compression=compression)
            total_rows += table.num_rows
            if target_rows is None:
                writer.write_table(table)
                continue

            start = 0
            while start < table.num_rows:
                remaining = target_rows - pending_rows
                length = min(remaining, table.num_rows - start)
                pending.append(table.slice(start, length))
                pending_rows += length
                start += length
                if pending_rows >= target_rows:
                    merged = pending[0] if len(pending) == 1 else pa.concat_tables(pending)
                    if row_group_size is not None:
                        writer.write_table(merged, row_group_size=row_group_size)
                    else:
                        writer.write_table(merged)
                    pending = []
                    pending_rows = 0

        if writer is not None and pending_rows > 0:
            merged = pending[0] if len(pending) == 1 else pa.concat_tables(pending)
            if row_group_size is not None:
                writer.write_table(merged, row_group_size=row_group_size)
            else:
                writer.write_table(merged)
    finally:
        if writer is not None:
            writer.close()
    return total_rows


def _iter_parquet_input_tables(src_path: Path, batch_rows: int | None = None):
    src_files = list_parquet_files(src_path)
    if not src_files:
        raise ValueError(f"No Parquet files found under {src_path}")
    for src_file in src_files:
        parquet_file = pq.ParquetFile(str(src_file))
        schema = parquet_file.schema_arrow
        if batch_rows is None:
            batches = parquet_file.iter_batches()
        else:
            batches = parquet_file.iter_batches(batch_size=batch_rows)
        for batch in batches:
            yield pa.Table.from_batches([batch], schema=schema)


def _hdf5_stamp_source(hf):
    for name in ("samplestamp", "timestamps", "time", "sample_index"):
        if name in hf:
            return hf[name]
    return None


def _hdf5_stamp_slice(stamp_source, start: int, end: int) -> np.ndarray:
    if stamp_source is None:
        return np.arange(start, end, dtype=np.int64)
    return np.asarray(stamp_source[start:end], dtype=np.int64)


def _iter_hdf5_tables(hf, row_group_size: int | None):
    import h5py

    stamp_source = _hdf5_stamp_source(hf)

    if "channels" in hf and isinstance(hf["channels"], h5py.Group):
        raw_labels = list(hf["channels"].keys())
        channel_datasets = [hf["channels"][lbl] for lbl in raw_labels]
        total_rows = channel_datasets[0].shape[0]
        column_names = [f"ch_{_decode_hdf5_label(lbl)}" for lbl in raw_labels]
        chunk_rows = min((ds.chunks[0] for ds in channel_datasets if ds.chunks), default=None)

        def _column_arrays(start: int, end: int) -> dict[str, pa.Array]:
            columns = {"samplestamp": pa.array(_hdf5_stamp_slice(stamp_source, start, end))}
            for name, ds in zip(column_names, channel_datasets):
                columns[name] = pa.array(np.asarray(ds[start:end], dtype=np.float32))
            return columns

    elif "data" in hf and len(hf["data"].shape) == 2:
        data_ds = hf["data"]
        total_rows = data_ds.shape[0]
        if "channel_labels" in hf.attrs:
            labels = [_decode_hdf5_label(lbl) for lbl in hf.attrs["channel_labels"]]
        else:
            labels = [str(i) for i in range(data_ds.shape[1])]
        column_names = [f"ch_{lbl}" for lbl in labels]
        chunk_rows = data_ds.chunks[0] if data_ds.chunks else None

        def _column_arrays(start: int, end: int) -> dict[str, pa.Array]:
            matrix = np.asarray(data_ds[start:end], dtype=np.float32)
            columns = {"samplestamp": pa.array(_hdf5_stamp_slice(stamp_source, start, end))}
            for idx, name in enumerate(column_names):
                columns[name] = pa.array(matrix[:, idx])
            return columns

    else:
        raise ValueError(f"Cannot determine HDF5 layout for {hf.filename}")

    schema = pa.schema([
        pa.field("samplestamp", pa.int64()),
        *[pa.field(name, pa.float32()) for name in column_names],
    ])
    batch_rows = _hdf5_batch_rows(total_rows, row_group_size, chunk_rows)
    for start in range(0, total_rows, batch_rows):
        end = min(start + batch_rows, total_rows)
        yield pa.table(_column_arrays(start, end), schema=schema)


def _edf_batch_rows(total_rows: int, sample_freq: float,
                    row_group_size: int | None) -> int:
    default_chunk_rows = max(int(np.ceil(float(sample_freq))) * 30, 1)
    chunk_rows = default_chunk_rows if row_group_size is None else min(int(row_group_size), default_chunk_rows)
    return max(1, min(total_rows, chunk_rows))


def _iter_edf_tables(edf, row_group_size: int | None):
    total_rows = int(edf.total_samples)
    labels = list(edf.signal_labels)
    schema = pa.schema([
        pa.field("samplestamp", pa.int64()),
        *[pa.field(f"ch_{lbl}", pa.float32()) for lbl in labels],
    ])
    batch_rows = _edf_batch_rows(total_rows, float(edf.sample_frequency), row_group_size)
    for start in range(0, total_rows, batch_rows):
        n_rows = min(batch_rows, total_rows - start)
        matrix = edf.read_window(start, n_rows)
        columns = {"samplestamp": pa.array(np.arange(start, start + n_rows, dtype=np.int64))}
        for idx, label in enumerate(labels):
            columns[f"ch_{label}"] = pa.array(np.asarray(matrix[idx], dtype=np.float32))
        yield pa.table(columns, schema=schema)


def _rewrite_parquet_input(src_path: Path, out_file: Path, compression: str,
                           row_group_size: int | None,
                           write_chunk_rows: int | None = None) -> int:
    return _write_streamed_tables(
        _iter_parquet_input_tables(src_path, batch_rows=write_chunk_rows),
        out_file,
        compression,
        row_group_size,
        write_chunk_rows=write_chunk_rows,
    )


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
    write_chunk_rows = _write_chunk_rows(
        row_group_size,
        canonical_cfg.get(
            "chunk_writer_max_rowgroups",
            canonical_cfg.get("write_row_groups_per_chunk", 1),
        ),
    )

    if fmt == "parquet":
        total_rows = _rewrite_parquet_input(input_path, canonical, compression, row_group_size, write_chunk_rows)
        print(f"  [ingest] wrote {canonical} ({total_rows:,} rows)")
    elif fmt == "hdf5":
        sample_freq = _ingest_hdf5(input_path, canonical, sample_freq, compression, row_group_size, write_chunk_rows)
    elif fmt == "edf":
        sample_freq = _ingest_edf(input_path, canonical, sample_freq, compression, row_group_size, write_chunk_rows)
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
                 row_group_size: int | None,
                 write_chunk_rows: int | None = None) -> float:
    """Read HDF5 and write as canonical Parquet. Returns sample_freq."""
    import h5py

    with h5py.File(str(h5_path), "r") as hf:
        freq = sample_freq or float(hf.attrs.get("sample_freq", 0))
        if freq <= 0:
            raise ValueError(
                f"sample_freq not found in HDF5 attributes of {h5_path}; "
                "pass sample_freq in config"
            )

        total_rows = _write_streamed_tables(
            _iter_hdf5_tables(hf, row_group_size),
            out_file,
            compression,
            row_group_size,
            write_chunk_rows=write_chunk_rows,
        )

    print(f"  [ingest] wrote {out_file} ({total_rows:,} rows)")
    return freq


def _ingest_edf(edf_path: Path, out_file: Path,
                sample_freq: float | None, compression: str,
                row_group_size: int | None,
                write_chunk_rows: int | None = None) -> float:
    """Read EDF and write as canonical Parquet. Returns sample_freq."""
    from .readers import EdfFileReader

    with EdfFileReader(edf_path) as edf:
        freq = sample_freq or edf.sample_frequency
        total = _write_streamed_tables(
            _iter_edf_tables(edf, row_group_size),
            out_file,
            compression,
            row_group_size,
            write_chunk_rows=write_chunk_rows,
        )

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
