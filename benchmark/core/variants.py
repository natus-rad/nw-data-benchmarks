"""Generate benchmark variants from a canonical Parquet directory.

Each variant spec in the config produces one output file/directory.
The returned ``paths`` dict uses keys that the benchmark functions
already understand: ``"parquet"``, ``"h5_columnar"``, ``"h5_rowgroup"``,
``"edf"``, plus variant-specific keys for multiple variants of the
same format.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import h5py
import hdf5plugin
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_paths import list_parquet_files
from .setup import _build_chunk_index, _parquet_to_edf
from .study_info import StudyInfo


def _spec_hash(spec: dict) -> str:
    encoded = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:8]


def _safe_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)[:64]


def _register_root_variant(paths: dict, variant_id: str, fmt: str, reader_kind: str,
                           path: Path, sort_index: int) -> None:
    paths.setdefault("__root_variants__", []).append({
        "artifact_id": variant_id,
        "variant_id": variant_id,
        "artifact_kind": "variant",
        "format_family": fmt,
        "reader_kind": reader_kind,
        "path": path,
        "display_label": variant_id,
        "sort_index": sort_index,
    })


def _iter_parquet_tables(src_files: list[Path], *, columns: list[str] | None = None,
                         batch_rows: int | None = None):
    for src_file in src_files:
        parquet_file = pq.ParquetFile(str(src_file))
        for batch in parquet_file.iter_batches(columns=columns, batch_size=batch_rows):
            yield pa.Table.from_batches([batch])


def _write_streamed_parquet(writer: pq.ParquetWriter, tables, row_group_size: int) -> None:
    row_group_size = max(1, int(row_group_size))
    pending: list[pa.Table] = []
    pending_rows = 0
    for table in tables:
        start = 0
        while start < table.num_rows:
            remaining = row_group_size - pending_rows
            length = min(remaining, table.num_rows - start)
            pending.append(table.slice(start, length))
            pending_rows += length
            start += length
            if pending_rows >= row_group_size:
                merged = pending[0] if len(pending) == 1 else pa.concat_tables(pending)
                writer.write_table(merged, row_group_size=merged.num_rows)
                pending = []
                pending_rows = 0

    if pending_rows > 0:
        merged = pending[0] if len(pending) == 1 else pa.concat_tables(pending)
        writer.write_table(merged, row_group_size=merged.num_rows)


def generate_variants(canonical_pq: Path, info: StudyInfo,
                      variant_specs: list[dict],
                      output_base: Path) -> dict[str, Path]:
    """Generate all configured variants from canonical Parquet.

    Returns a ``paths`` dict with keys the benchmark functions expect.
    If no variants are specified, returns just ``{"parquet": canonical_pq}``.
    """
    paths: dict[str, Path] = {
        "parquet": canonical_pq,
        "__root_variants__": [],
    }

    if not variant_specs:
        return paths

    output_base.mkdir(parents=True, exist_ok=True)

    for index, spec in enumerate(variant_specs):
        fmt = spec["format"]
        variant_id = spec["id"]

        if fmt == "parquet":
            _generate_parquet_variant(canonical_pq, output_base, info, spec, variant_id, index, paths)
        elif fmt == "hdf5":
            _generate_hdf5_variant(canonical_pq, output_base, info, spec, variant_id, index, paths)
        elif fmt == "edf":
            _generate_edf_variant(canonical_pq, output_base, info, spec, variant_id, index, paths)
        else:
            print(f"  [warn] Unknown variant format: {fmt}, skipping")

    return paths


def _generate_parquet_variant(canonical_pq: Path, output_base: Path,
                              info: StudyInfo, spec: dict, variant_id: str,
                              sort_index: int, paths: dict) -> None:
    """Re-partition canonical Parquet with specified row group size and codec."""
    rg_minutes = spec.get("row_group_minutes", 5)
    compression = spec.get("compression", "lz4")
    compression = None if compression == "none" else compression
    row_group_size = int(rg_minutes * 60 * info.sample_freq)

    spec_token = _spec_hash({
        "id": variant_id,
        "format": "parquet",
        "row_group_minutes": rg_minutes,
        "compression": spec.get("compression", "lz4"),
    })
    key = f"variant__{variant_id}"
    out_file = output_base / f"{_safe_id(variant_id)}_{spec_token}.parquet"

    if out_file.exists():
        print(f"  [cached] {key}")
    else:
        print(f"  [variant] Parquet ({variant_id}) ...")
        output_base.mkdir(parents=True, exist_ok=True)
        src_files = list_parquet_files(canonical_pq)
        schema = pq.read_schema(str(src_files[0]))

        writer = pq.ParquetWriter(
            str(out_file), schema,
            compression=compression,
            write_statistics=True,
        )
        try:
            _write_streamed_parquet(
                writer,
                _iter_parquet_tables(src_files, batch_rows=row_group_size),
                row_group_size,
            )
        finally:
            writer.close()

        size_mib = out_file.stat().st_size / (1024 * 1024)
        n_rg = pq.ParquetFile(str(out_file)).metadata.num_row_groups
        print(f"  [variant] {variant_id}: {size_mib:.1f} MiB, {n_rg} row groups")

    paths[key] = out_file
    _register_root_variant(paths, variant_id, "parquet", "parquet", out_file, sort_index)


def _generate_hdf5_variant(canonical_pq: Path, output_base: Path,
                            info: StudyInfo, spec: dict, variant_id: str,
                            sort_index: int, paths: dict) -> None:
    """Write HDF5 variant from canonical Parquet."""
    layout = spec.get("layout", "columnar")
    chunk_minutes = spec.get("chunk_minutes", 5)
    dtype = spec.get("dtype", "float32")
    compression = spec.get("compression", "lz4")
    if dtype != "float32":
        raise ValueError("HDF5 variants currently support only dtype=float32")
    if compression != "lz4":
        raise ValueError("HDF5 variants currently support only compression=lz4")
    chunk_samples = int(chunk_minutes * 60 * info.sample_freq)

    layout_key = "h5_columnar" if layout == "columnar" else "h5_rowgroup"
    spec_token = _spec_hash({
        "id": variant_id,
        "format": "hdf5",
        "layout": layout,
        "chunk_minutes": chunk_minutes,
        "dtype": dtype,
        "compression": compression,
    })
    key = f"variant__{variant_id}"
    out_file = output_base / f"{_safe_id(variant_id)}_{spec_token}.h5"

    if out_file.exists():
        print(f"  [cached] {key}")
    else:
        print(f"  [variant] HDF5 {layout} ({variant_id}) ...")
        output_base.mkdir(parents=True, exist_ok=True)
        src_files = list_parquet_files(canonical_pq)
        schema = pq.read_schema(str(src_files[0]))
        ch_cols = [c for c in schema.names if c.startswith("ch_")]
        ch_labels = [c[3:] for c in ch_cols]
        n_channels = len(ch_cols)
        total_rows = sum(pq.read_metadata(str(f)).num_rows for f in src_files)
        cs = max(1, min(chunk_samples, total_rows))

        with h5py.File(str(out_file), "w") as hf:
            hf.attrs["sample_freq"] = info.sample_freq
            hf.attrs["channel_labels"] = ch_labels
            hf.attrs["layout"] = layout
            hf.attrs["n_channels"] = n_channels
            hf.attrs["chunk_samples"] = cs

            if layout == "columnar":
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
                for table in _iter_parquet_tables(src_files, columns=["samplestamp"] + ch_cols, batch_rows=cs):
                    n = table.num_rows
                    stamp_ds[offset:offset + n] = table.column("samplestamp").to_numpy()
                    for col in ch_cols:
                        ch_ds[col][offset:offset + n] = (
                            table.column(col).to_numpy().astype(np.float32, copy=False)
                        )
                    offset += n
            else:
                # rowgroup layout
                data_ds = hf.create_dataset(
                    "data", shape=(total_rows, n_channels), dtype=np.float32,
                    chunks=(cs, n_channels), **hdf5plugin.LZ4(),
                )
                stamp_ds = hf.create_dataset(
                    "samplestamp", shape=(total_rows,), dtype=np.int64,
                    chunks=(cs,), **hdf5plugin.LZ4(),
                )
                hf.attrs["column_order"] = ch_cols
                offset = 0
                for table in _iter_parquet_tables(src_files, columns=["samplestamp"] + ch_cols, batch_rows=cs):
                    n = table.num_rows
                    stamp_ds[offset:offset + n] = table.column("samplestamp").to_numpy()
                    block = np.column_stack([
                        table.column(col).to_numpy().astype(np.float32, copy=False)
                        for col in ch_cols
                    ])
                    data_ds[offset:offset + n, :] = block
                    offset += n

            idx = _build_chunk_index(stamp_ds)
            hf.create_dataset("chunk_index", data=idx)
            hf.attrs["total_samples"] = total_rows

        size_mib = out_file.stat().st_size / (1024 * 1024)
        print(f"  [variant] {variant_id}: {size_mib:.1f} MiB")

    paths[key] = out_file
    # First of each layout type also gets the canonical key.
    if layout_key not in paths:
        paths[layout_key] = out_file
    _register_root_variant(paths, variant_id, "hdf5", layout_key, out_file, sort_index)


def _generate_edf_variant(canonical_pq: Path, output_base: Path,
                           info: StudyInfo, spec: dict, variant_id: str,
                           sort_index: int, paths: dict) -> None:
    """Write EDF variant from canonical Parquet."""
    spec_token = _spec_hash({"id": variant_id, "format": "edf"})
    key = f"variant__{variant_id}"
    out_file = output_base / f"{_safe_id(variant_id)}_{spec_token}.edf"

    if out_file.exists():
        print(f"  [cached] {key}")
    else:
        print(f"  [variant] EDF ({variant_id}) ...")
        _parquet_to_edf(canonical_pq, out_file, sample_freq=info.sample_freq)
        size_mib = out_file.stat().st_size / (1024 * 1024)
        print(f"  [variant] {variant_id}: {size_mib:.1f} MiB")

    paths["edf"] = out_file
    paths[key] = out_file
    _register_root_variant(paths, variant_id, "edf", "edf", out_file, sort_index)


