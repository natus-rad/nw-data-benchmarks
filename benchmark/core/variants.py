"""Generate benchmark variants from a canonical Parquet directory.

Each variant spec in the config produces one output file/directory.
The returned ``paths`` dict uses keys that the benchmark functions
already understand: ``"parquet"``, ``"h5_columnar"``, ``"h5_rowgroup"``,
``"edf"``, plus variant-specific keys for multiple variants of the
same format.
"""
from __future__ import annotations

from pathlib import Path

import h5py
import hdf5plugin
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .setup import _build_chunk_index, _parquet_to_edf
from .study_info import StudyInfo


def generate_variants(canonical_pq: Path, info: StudyInfo,
                      variant_specs: list[dict],
                      output_base: Path) -> dict[str, Path]:
    """Generate all configured variants from canonical Parquet.

    Returns a ``paths`` dict with keys the benchmark functions expect.
    If no variants are specified, returns just ``{"parquet": canonical_pq}``.
    """
    paths: dict[str, Path] = {}

    if not variant_specs:
        # No variants configured — benchmark the canonical Parquet only.
        paths["parquet"] = canonical_pq
        return paths

    output_base.mkdir(parents=True, exist_ok=True)

    # Track how many of each format we've seen for labeling.
    fmt_counts: dict[str, int] = {}

    for spec in variant_specs:
        fmt = spec["format"]
        fmt_counts[fmt] = fmt_counts.get(fmt, 0) + 1
        label = _variant_label(spec)

        if fmt == "parquet":
            _generate_parquet_variant(canonical_pq, output_base, info, spec, label, paths)
        elif fmt == "hdf5":
            _generate_hdf5_variant(canonical_pq, output_base, info, spec, label, paths)
        elif fmt == "edf":
            _generate_edf_variant(canonical_pq, output_base, info, spec, label, paths)
        else:
            print(f"  [warn] Unknown variant format: {fmt}, skipping")

    # Ensure "parquet" key exists (benchmarks check for it).
    # Use the canonical if no Parquet variant was generated.
    if "parquet" not in paths:
        paths["parquet"] = canonical_pq

    return paths


def _variant_label(spec: dict) -> str:
    """Build a human-readable label from a variant spec."""
    fmt = spec["format"]
    if fmt == "edf":
        return "edf"
    parts = []
    if fmt == "parquet":
        rg = spec.get("row_group_minutes", 5)
        parts.append(f"{rg}m")
    elif fmt == "hdf5":
        layout = spec.get("layout", "columnar")
        parts.append(layout[:3])  # "col" or "row"
        chunk = spec.get("chunk_minutes", 5)
        parts.append(f"{chunk}m")
    dtype = spec.get("dtype", "float32")
    parts.append(dtype[:3])  # "flo" or "int"
    comp = spec.get("compression", "lz4")
    parts.append(comp)
    return "_".join(parts)


def _generate_parquet_variant(canonical_pq: Path, output_base: Path,
                              info: StudyInfo, spec: dict, label: str,
                              paths: dict) -> None:
    """Re-partition canonical Parquet with specified row group size and codec."""
    rg_minutes = spec.get("row_group_minutes", 5)
    compression = spec.get("compression", "lz4")
    compression = None if compression == "none" else compression
    row_group_size = int(rg_minutes * 60 * info.sample_freq)

    key = f"parquet_{label}"
    out_file = output_base / f"{key}.parquet"

    if out_file.exists():
        print(f"  [cached] {key}")
    else:
        print(f"  [variant] Parquet ({label}) ...")
        output_base.mkdir(parents=True, exist_ok=True)
        src_files = sorted(canonical_pq.glob("*.parquet"))
        schema = pq.read_schema(str(src_files[0]))

        writer = pq.ParquetWriter(
            str(out_file), schema,
            compression=compression,
            write_statistics=True,
        )
        try:
            buf: list[pa.Table] = []
            buf_rows = 0
            for f in src_files:
                table = pq.read_table(str(f))
                buf.append(table)
                buf_rows += table.num_rows
                while buf_rows >= row_group_size:
                    combined = pa.concat_tables(buf)
                    writer.write_table(combined.slice(0, row_group_size),
                                       row_group_size=row_group_size)
                    remainder = combined.slice(row_group_size)
                    buf = [remainder] if remainder.num_rows > 0 else []
                    buf_rows = remainder.num_rows
            if buf_rows > 0:
                writer.write_table(pa.concat_tables(buf),
                                   row_group_size=row_group_size)
        finally:
            writer.close()

        size_mib = out_file.stat().st_size / (1024 * 1024)
        n_rg = pq.ParquetFile(str(out_file)).metadata.num_row_groups
        print(f"  [variant] {key}: {size_mib:.1f} MiB, {n_rg} row groups")

    # The output is a single file, but benchmarks expect a directory.
    # Wrap it in a directory.
    out_dir = output_base / key
    if not out_dir.exists():
        out_dir.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.move(str(out_file), str(out_dir / "part_00000.parquet"))
    elif out_file.exists():
        # Directory already exists but file is outside — move it in
        import shutil
        shutil.move(str(out_file), str(out_dir / "part_00000.parquet"))

    paths[key] = out_dir
    # First Parquet variant also gets the "parquet" key for backward compat.
    if "parquet" not in paths:
        paths["parquet"] = out_dir


def _generate_hdf5_variant(canonical_pq: Path, output_base: Path,
                            info: StudyInfo, spec: dict, label: str,
                            paths: dict) -> None:
    """Write HDF5 variant from canonical Parquet."""
    layout = spec.get("layout", "columnar")
    chunk_minutes = spec.get("chunk_minutes", 5)
    chunk_samples = int(chunk_minutes * 60 * info.sample_freq)

    layout_key = "h5_columnar" if layout == "columnar" else "h5_rowgroup"
    key = f"{layout_key}_{label}"
    out_file = output_base / f"{key}.h5"

    if out_file.exists():
        print(f"  [cached] {key}")
    else:
        print(f"  [variant] HDF5 {layout} ({label}) ...")
        output_base.mkdir(parents=True, exist_ok=True)
        src_files = sorted(canonical_pq.glob("*.parquet"))
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
                for src_file in src_files:
                    table = pq.read_table(str(src_file), columns=["samplestamp"] + ch_cols)
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
                for src_file in src_files:
                    table = pq.read_table(str(src_file), columns=["samplestamp"] + ch_cols)
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
        print(f"  [variant] {key}: {size_mib:.1f} MiB")

    paths[key] = out_file
    # First of each layout type also gets the canonical key.
    if layout_key not in paths:
        paths[layout_key] = out_file


def _generate_edf_variant(canonical_pq: Path, output_base: Path,
                           info: StudyInfo, spec: dict, label: str,
                           paths: dict) -> None:
    """Write EDF variant from canonical Parquet."""
    key = "edf"
    out_file = output_base / f"{canonical_pq.stem}.edf"

    if out_file.exists():
        print(f"  [cached] {key}")
    else:
        print(f"  [variant] EDF ...")
        _parquet_to_edf(canonical_pq, out_file, sample_freq=info.sample_freq)
        size_mib = out_file.stat().st_size / (1024 * 1024)
        print(f"  [variant] EDF: {size_mib:.1f} MiB")

    paths["edf"] = out_file


