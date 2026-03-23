from __future__ import annotations

import json
from pathlib import Path

import h5py
import hdf5plugin
import numpy as np
import pyarrow.parquet as pq

from .config_helpers import (
    get_parquet_compression_variants,
    get_tuned_block_sizes_minutes,
    get_tuned_hdf5_compression,
    get_tuned_parquet_codecs,
    tuned_parquet_key,
)
from .parquet_paths import list_parquet_files
from .study_info import StudyInfo


def _parquet_to_edf(pq_dir: Path, edf_path: Path,
                    sample_freq: float = 256.0) -> None:
    """Convert float32 Parquet files to a single EDF file."""
    import pyedflib

    pq_files = list_parquet_files(pq_dir)
    if not pq_files:
        raise FileNotFoundError(f"No Parquet files in {pq_dir}")

    schema = pq.read_schema(str(pq_files[0]))
    ch_cols = [c for c in schema.names if c.startswith("ch_")]
    labels = [c[3:] for c in ch_cols]
    n_channels = len(labels)
    batch_rows = max(int(np.ceil(float(sample_freq))) * 30, 1)

    ch_min = np.full(n_channels, np.inf)
    ch_max = np.full(n_channels, -np.inf)
    for f in pq_files:
        parquet_file = pq.ParquetFile(str(f))
        for batch in parquet_file.iter_batches(columns=ch_cols, batch_size=batch_rows):
            for i in range(n_channels):
                arr = batch.column(i).to_numpy(zero_copy_only=False).astype(np.float64, copy=False)
                if arr.size == 0:
                    continue
                ch_min[i] = min(ch_min[i], float(arr.min()))
                ch_max[i] = max(ch_max[i], float(arr.max()))
    flat = ch_min == ch_max
    ch_max[flat] = ch_min[flat] + 1.0

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
            parquet_file = pq.ParquetFile(str(f))
            for batch in parquet_file.iter_batches(columns=ch_cols, batch_size=batch_rows):
                block = [
                    batch.column(i).to_numpy(zero_copy_only=False).astype(np.float64, copy=False)
                    for i in range(n_channels)
                ]
                writer.writeSamples(block)
    finally:
        writer.close()


def _setup_parquet_compression_variants(paths: dict, src_dir: Path,
                                        output_base: Path, name: str,
                                        cfg: dict) -> None:
    """Re-compress source Parquet with different codecs for benchmark F."""
    src_files = list_parquet_files(src_dir)
    if not src_files:
        return

    for comp_cfg in get_parquet_compression_variants(cfg):
        codec = comp_cfg["codec"]
        level = comp_cfg.get("level")
        label = f"{codec}_{level}" if level else codec
        single_file = len(src_files) == 1
        out_path = output_base / (f"parquet_{label}.parquet" if single_file else f"parquet_{label}")

        if out_path.is_file() or (out_path.is_dir() and any(out_path.glob("*.parquet"))):
            print(f"  [cached] parquet_{label} -> {out_path}")
            paths[f"parquet_{label}"] = out_path
            continue
        if codec == "snappy" and not level:
            print(f"  [cached] parquet_{label} -> {src_dir}")
            paths[f"parquet_{label}"] = src_dir
            continue

        print(f"  [convert] {name} -> Parquet ({label}) ...")
        if single_file:
            out_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            out_path.mkdir(parents=True, exist_ok=True)
        compression = None if codec == "none" else codec

        output_files = []
        for src_file in src_files:
            table = pq.read_table(str(src_file))
            out_file = out_path if single_file else out_path / src_file.name
            pq.write_table(table, str(out_file), compression=compression,
                           compression_level=level)
            output_files.append(src_file.name)
        if not single_file and len(output_files) > 1:
            _write_parquet_dataset_metadata(out_path, output_files)

        paths[f"parquet_{label}"] = out_path


NANOVOLT_SCALE = 0.001


def _write_parquet_dataset_metadata(out_dir: Path, output_files: list[str]) -> None:
    """Write _metadata and _common_metadata files for a multi-file Parquet dataset."""
    if not output_files:
        return

    first_file = out_dir / output_files[0]
    schema = pq.read_schema(str(first_file))
    pq.write_metadata(schema, str(out_dir / "_common_metadata"))

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
    src_files = list_parquet_files(src_path)
    if not src_files:
        return

    for mode in ("int32_calibrated", "int32_nanovolt"):
        for codec in ("zstd", "snappy", "none"):
            label = f"{mode}_{codec}"
            single_file = len(src_files) == 1
            out_path = output_base / (f"parquet_{label}.parquet" if single_file else f"parquet_{label}")
            if out_path.is_file() or (out_path.is_dir() and any(out_path.glob("*.parquet"))):
                print(f"  [cached] parquet_{label} -> {out_path}")
                paths[f"parquet_{label}"] = out_path
                continue

            print(f"  [convert] {name} -> Parquet ({label}) ...")
            if single_file:
                out_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                out_path.mkdir(parents=True, exist_ok=True)

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
                        prev = global_calibration.get(
                            col_name, {"min": float("inf"), "max": float("-inf")},
                        )
                        global_calibration[col_name] = {
                            "min": min(prev["min"], float(arr.min())),
                            "max": max(prev["max"], float(arr.max())),
                        }
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
                else:
                    new_columns = {}
                    for col_name in ch_cols:
                        arr = table.column(col_name).to_numpy().astype(np.float64)
                        digital = np.round(arr / NANOVOLT_SCALE).astype(np.int32)
                        new_columns[col_name] = pa.array(digital)
                    schema_meta[b"int32_scale_uv"] = str(NANOVOLT_SCALE).encode("utf-8")

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
                out_file = out_path if single_file else out_path / src_file.name
                pq.write_table(new_table, str(out_file), compression=compression)
                output_files.append(src_file.name)
            if not single_file and len(output_files) > 1:
                _write_parquet_dataset_metadata(out_path, output_files)

            paths[f"parquet_{label}"] = out_path


H5_COLUMNAR_CHUNK_SECONDS = 300


def _count_total_rows(src_files: list) -> int:
    """Return total row count across all source Parquet files."""
    return sum(pq.ParquetFile(str(f)).metadata.num_rows for f in src_files)


def _default_h5_chunk_samples(sample_freq: float, total_rows: int) -> int:
    """Chunk size for general columnar HDF5: ~5 minutes worth of samples."""
    if total_rows <= 0:
        return 1
    chunk_samples = int(round(H5_COLUMNAR_CHUNK_SECONDS * sample_freq))
    return max(1, min(chunk_samples, total_rows))


def _parquet_rowgroup_chunk_samples(src_files: list, total_rows: int) -> int:
    """Choose HDF5 row-group chunk size from actual source Parquet metadata."""
    if total_rows <= 0:
        return 1

    rg_sizes: list[int] = []
    for f in src_files:
        pf = pq.ParquetFile(str(f))
        meta = pf.metadata
        for i in range(meta.num_row_groups):
            rg_sizes.append(int(meta.row_group(i).num_rows))
    if not rg_sizes:
        return 1

    unique = sorted(set(rg_sizes))
    if len(unique) == 1:
        return min(unique[0], total_rows)

    counts: dict[int, int] = {}
    for sz in rg_sizes:
        counts[sz] = counts.get(sz, 0) + 1
    mode_size = max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
    print(
        "  [warn] Source Parquet row-group sizes are not uniform: "
        f"{unique}. Using modal size {mode_size} for h5_rowgroup."
    )
    return min(mode_size, total_rows)


def _setup_h5_variants(paths: dict, output_base: Path, name: str, info) -> None:
    """Create two HDF5 layouts from the cached Parquet float32 snappy data."""
    src_path = paths["parquet"]
    src_files = list_parquet_files(src_path)
    if not src_files:
        return

    all_cols = [c for c in pq.read_schema(str(src_files[0])).names if c.startswith("ch_")]
    ch_labels = [c[3:] for c in all_cols]
    n_channels = len(all_cols)
    sample_freq = info.sample_freq
    total_rows = _count_total_rows(src_files)

    columnar_chunk_samples = _default_h5_chunk_samples(sample_freq, total_rows)
    rowgroup_chunk_samples = _parquet_rowgroup_chunk_samples(src_files, total_rows)

    for layout in ("h5_columnar", "h5_rowgroup"):
        h5_path = output_base / f"{name}.{layout}.h5"
        if h5_path.exists():
            paths[layout] = h5_path
            print(f"  [cached] {layout} -> {h5_path}")
            continue

        print(f"  [convert] {name} -> HDF5 ({layout}) ...")
        with h5py.File(str(h5_path), "w") as hf:
            hf.attrs["sample_freq"] = sample_freq
            hf.attrs["channel_labels"] = ch_labels
            hf.attrs["layout"] = layout
            hf.attrs["n_channels"] = n_channels

            if layout == "h5_columnar":
                hf.attrs["chunk_samples"] = columnar_chunk_samples
                hf.attrs["chunk_policy"] = "time_seconds"
                hf.attrs["chunk_seconds"] = H5_COLUMNAR_CHUNK_SECONDS
                _write_h5_columnar(hf, src_files, all_cols, chunk_size=columnar_chunk_samples)
            else:
                hf.attrs["chunk_samples"] = rowgroup_chunk_samples
                hf.attrs["chunk_policy"] = "parquet_rowgroup"
                _write_h5_rowgroup(
                    hf, src_files, all_cols, n_channels,
                    chunk_size=rowgroup_chunk_samples,
                )

        paths[layout] = h5_path
        size_mib = h5_path.stat().st_size / (1024 * 1024)
        print(f"  [convert] {layout}: {size_mib:.1f} MiB")


def _build_chunk_index(stamps_ds: h5py.Dataset) -> np.ndarray:
    """Build a chunk-level min/max stamp index after all data is written."""
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


def _write_h5_columnar(hf: h5py.File, src_files: list, all_cols: list[str],
                       chunk_size: int) -> None:
    total_rows = _count_total_rows(src_files)
    chunk_size = max(1, min(int(chunk_size), total_rows if total_rows > 0 else 1))

    grp = hf.create_group("channels")
    ch_datasets = {}
    for col in all_cols:
        label = col[3:]
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

    index = _build_chunk_index(stamp_ds)
    hf.create_dataset("chunk_index", data=index)
    hf.attrs["total_samples"] = total_rows


def _write_h5_rowgroup(hf: h5py.File, src_files: list,
                       all_cols: list[str], n_channels: int,
                       chunk_size: int) -> None:
    total_rows = _count_total_rows(src_files)
    chunk_size = max(1, min(int(chunk_size), total_rows if total_rows > 0 else 1))

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


def _get_tuned_block_sizes(cfg: dict, sample_freq: float) -> dict[str, int]:
    """Build {label: samples} dict from config or defaults."""
    minutes = get_tuned_block_sizes_minutes(cfg)
    sizes = {}
    for m in minutes:
        label = f"{int(m * 60)}s" if m < 1 else f"{m}m"
        sizes[label] = int(m * 60 * sample_freq)
    return sizes


def _setup_tuned_variants(paths: dict, output_base: Path, info: StudyInfo,
                          cfg: dict) -> None:
    """Create Parquet and HDF5 columnar variants with different block sizes."""
    src_path = paths.get("parquet")
    if not src_path:
        return

    src_files = list_parquet_files(Path(src_path))
    if not src_files:
        return

    schema = pq.read_schema(str(src_files[0]))
    ch_cols = [c for c in schema.names if c.startswith("ch_")]
    block_sizes = _get_tuned_block_sizes(cfg, info.sample_freq)
    parquet_codecs = get_tuned_parquet_codecs(cfg)
    hdf5_compression = get_tuned_hdf5_compression(cfg)

    for label, block_samples in block_sizes.items():
        _setup_tuned_parquet(paths, output_base, src_files, ch_cols, label, block_samples, parquet_codecs)
        _setup_tuned_h5(paths, output_base, src_files, ch_cols, label, block_samples, info, hdf5_compression)


def _setup_tuned_parquet(paths, output_base, src_files, ch_cols,
                         label, row_group_size, codecs):
    """Write single consolidated Parquet files with a specific row-group size."""
    import pyarrow as pa

    schema = pq.read_schema(str(src_files[0]))
    variants = []
    for codec in codecs:
        key = tuned_parquet_key(codec, label)
        out_file = output_base / f"{key}.parquet"
        need_write = not out_file.exists()
        if need_write:
            print(f"  [convert] tuned Parquet (rg={label}, {codec}) ...")
        variants.append((codec, key, out_file, need_write))

    if any(need_write for _, _, _, need_write in variants):
        output_base.mkdir(parents=True, exist_ok=True)
        writers = {
            key: pq.ParquetWriter(str(out_file), schema, compression=codec, write_statistics=True)
            for codec, key, out_file, need_write in variants
            if need_write
        }
        try:
            buf: list[pa.Table] = []
            buf_rows = 0

            def _flush(table: pa.Table) -> None:
                for writer in writers.values():
                    writer.write_table(table, row_group_size=row_group_size)

            for f in src_files:
                table = pq.read_table(str(f))
                buf.append(table)
                buf_rows += table.num_rows

                while buf_rows >= row_group_size:
                    combined = pa.concat_tables(buf)
                    _flush(combined.slice(0, row_group_size))
                    remainder = combined.slice(row_group_size)
                    buf = [remainder] if remainder.num_rows > 0 else []
                    buf_rows = remainder.num_rows

            if buf_rows > 0:
                _flush(pa.concat_tables(buf))
        finally:
            for writer in writers.values():
                writer.close()

    for codec, key, out_file, was_written in variants:
        if out_file.exists():
            if was_written:
                size_mib = out_file.stat().st_size / (1024 * 1024)
                n_rg = pq.ParquetFile(str(out_file)).metadata.num_row_groups
                print(f"  [convert] {key}: {size_mib:.1f} MiB, {n_rg} row groups")
            else:
                print(f"  [cached] {key} -> {out_file}")
            paths[key] = out_file


def _setup_tuned_h5(paths, output_base, src_files, ch_cols,
                    label, chunk_samples, info, compression):
    """Write an HDF5 columnar file with a specific chunk size."""
    if compression != "lz4":
        raise ValueError("tuned_comparison.hdf5_compression currently supports only lz4")
    key = f"tuned_h5_{label}"
    out_file = output_base / f"tuned_h5_{label}.h5"
    if out_file.exists():
        paths[key] = out_file
        print(f"  [cached] {key} -> {out_file}")
        return

    print(f"  [convert] tuned HDF5 columnar (chunk={label}, {compression.upper()}) ...")
    output_base.mkdir(parents=True, exist_ok=True)
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
                chunks=(cs,), **hdf5plugin.LZ4(),
            )

        stamp_ds = hf.create_dataset(
            "samplestamp", shape=(total_rows,), dtype=np.int64,
            chunks=(cs,), **hdf5plugin.LZ4(),
        )

        offset = 0
        for src_file in src_files:
            table = pq.read_table(str(src_file), columns=["samplestamp"] + ch_cols)
            n = table.num_rows
            stamp_ds[offset:offset + n] = table.column("samplestamp").to_numpy()
            for col in ch_cols:
                ch_ds[col][offset:offset + n] = (
                    table.column(col).to_numpy().astype(np.float32, copy=False)
                )
            offset += n

        idx = _build_chunk_index(stamp_ds)
        hf.create_dataset("chunk_index", data=idx)
        hf.attrs["total_samples"] = total_rows

    paths[key] = out_file
    size_mib = out_file.stat().st_size / (1024 * 1024)
    n_chunks = (total_rows + cs - 1) // cs
    print(f"  [convert] {key}: {size_mib:.1f} MiB, {n_chunks} chunks")
