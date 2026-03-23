from __future__ import annotations

import json
from pathlib import Path

import h5py
import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _read_parquet_window(parquet_dir: Path, columns: list[str],
                         start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read a stamp window from Parquet, return (channels, samples) float32 matrix."""
    table = pq.read_table(
        str(parquet_dir), columns=columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    cols = [table.column(c).to_numpy().astype(np.float32, copy=False) for c in columns]
    return np.vstack(cols)


def _edf_file(edf_path: Path) -> Path:
    """Return the .edf file path — handles both file and directory inputs."""
    if edf_path.is_file():
        return edf_path
    files = sorted(edf_path.glob("*.edf"))
    if not files:
        raise FileNotFoundError(f"No .edf files at {edf_path}")
    return files[0]


class EdfFileReader:
    """Thin context manager that keeps a pyedflib reader open across many reads."""

    def __init__(self, edf_path: Path):
        self.path = _edf_file(edf_path)
        self._reader = None
        self.total_samples = 0
        self.signal_labels: list[str] = []

    def __enter__(self) -> "EdfFileReader":
        import pyedflib

        self._reader = pyedflib.EdfReader(str(self.path))
        self.total_samples = int(self._reader.getNSamples()[0])
        self.signal_labels = list(self._reader.getSignalLabels())
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._reader is not None:
            self._reader.close()
            self._reader = None

    @property
    def n_channels(self) -> int:
        return len(self.signal_labels)

    @property
    def sample_frequency(self) -> float:
        """Get sample frequency of the first channel (all channels have same freq in EDF)."""
        if self._reader is None:
            raise RuntimeError("EdfFileReader must be used as a context manager.")
        return float(self._reader.getSampleFrequency(0))

    def read_window(self, start_sample: int, n_samples: int,
                    channel_indices: list[int] | None = None) -> np.ndarray:
        if self._reader is None:
            raise RuntimeError("EdfFileReader must be used as a context manager.")
        indices = channel_indices if channel_indices is not None else list(range(self._reader.signals_in_file))
        rows = []
        for ch in indices:
            rows.append(self._reader.readSignal(ch, start=start_sample, n=n_samples, digital=False))
        return np.vstack(rows).astype(np.float32, copy=False)

    def read_all_channels(self) -> np.ndarray:
        """Read all channels for the entire file. Returns (n_channels, total_samples) array."""
        if self._reader is None:
            raise RuntimeError("EdfFileReader must be used as a context manager.")
        rows = []
        for ch in range(self._reader.signals_in_file):
            rows.append(self._reader.readSignal(ch, start=0, n=self.total_samples, digital=False))
        return np.array(rows, dtype=np.float32)


def _read_edf_window(edf_path: Path, start_sample: int, n_samples: int,
                     channel_indices: list[int] | None = None) -> np.ndarray:
    """Read a window from EDF, return (channels, samples) float32 matrix."""
    with EdfFileReader(edf_path) as reader:
        return reader.read_window(start_sample, n_samples, channel_indices)


def _edf_total_samples(edf_path: Path) -> int:
    with EdfFileReader(edf_path) as reader:
        return reader.total_samples


def _h5_resolve_stamp_range(hf: h5py.File, start_stamp: int,
                            end_stamp: int) -> tuple[int, int]:
    """Use the chunk index to find the array index range for a stamp window."""
    chunk_idx = hf["chunk_index"][:]
    stamps_ds = hf["samplestamp"]
    chunk_size = stamps_ds.chunks[0]
    total = stamps_ds.shape[0]

    overlaps = (chunk_idx[:, 1] <= end_stamp) & (chunk_idx[:, 2] >= start_stamp)
    hit_indices = np.where(overlaps)[0]
    if len(hit_indices) == 0:
        return 0, 0

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
    """Read a stamp window from columnar HDF5 (one dataset per channel)."""
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
    """Read a stamp window from row-group-aligned HDF5 (single 2D dataset)."""
    with h5py.File(str(h5_path), "r") as hf:
        i_start, i_end = _h5_resolve_stamp_range(hf, start_stamp, end_stamp)
        if i_end <= i_start:
            return np.empty((len(columns), 0), dtype=np.float32)
        col_order = [
            x.decode("utf-8") if isinstance(x, (bytes, np.bytes_)) else str(x)
            for x in hf.attrs["column_order"]
        ]
        col_indices = sorted([col_order.index(c) for c in columns])
        data = hf["data"][i_start:i_end, col_indices]
        request_order = [col_order.index(c) for c in columns]
        reindex = [col_indices.index(ci) for ci in request_order]
        return data[:, reindex].T.astype(np.float32, copy=False)


def _read_h5_input_window(h5_path: Path, columns: list[str],
                          start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read a stamp window directly from an input HDF5 file.

    Supports the same baseline HDF5 layouts accepted by ingest.py:
    - a ``channels`` group with one dataset per channel
    - a 2D ``data`` dataset with channel labels in attrs or inferred names
    """
    with h5py.File(str(h5_path), "r") as hf:
        stamps = None
        for name in ("samplestamp", "timestamps", "time", "sample_index"):
            if name in hf:
                stamps = hf[name][:]
                break

        if "channels" in hf and isinstance(hf["channels"], h5py.Group):
            grp = hf["channels"]
            total = grp[next(iter(grp.keys()))].shape[0] if grp.keys() else 0
            labels = list(grp.keys())
            if stamps is None:
                i_start = max(0, int(start_stamp))
                i_end = min(total, int(end_stamp) + 1)
            else:
                i_start = int(np.searchsorted(stamps, start_stamp, side="left"))
                i_end = int(np.searchsorted(stamps, end_stamp, side="right"))
            if i_end <= i_start:
                return np.empty((len(columns), 0), dtype=np.float32)
            rows = []
            for col in columns:
                label = col[3:] if col.startswith("ch_") else col
                if label not in labels:
                    raise KeyError(f"Channel '{label}' not found in {h5_path}")
                rows.append(grp[label][i_start:i_end])
            return np.vstack(rows).astype(np.float32, copy=False)

        if "data" in hf and len(hf["data"].shape) == 2:
            data_ds = hf["data"]
            total = data_ds.shape[0]
            raw_labels = hf.attrs.get("channel_labels")
            if raw_labels is None:
                labels = [f"ch_{i}" for i in range(data_ds.shape[1])]
            else:
                labels = [
                    x.decode("utf-8") if isinstance(x, (bytes, np.bytes_)) else str(x)
                    for x in raw_labels
                ]
            if stamps is None:
                i_start = max(0, int(start_stamp))
                i_end = min(total, int(end_stamp) + 1)
            else:
                i_start = int(np.searchsorted(stamps, start_stamp, side="left"))
                i_end = int(np.searchsorted(stamps, end_stamp, side="right"))
            if i_end <= i_start:
                return np.empty((len(columns), 0), dtype=np.float32)
            col_indices = []
            for col in columns:
                label = col[3:] if col.startswith("ch_") else col
                if label not in labels:
                    raise KeyError(f"Channel '{label}' not found in {h5_path}")
                col_indices.append(labels.index(label))
            return data_ds[i_start:i_end, col_indices].T.astype(np.float32, copy=False)

        raise ValueError(f"Cannot determine baseline HDF5 layout for {h5_path}")


def _h5_total_samples(h5_path: Path) -> int:
    """Return total number of samples in an HDF5 file."""
    with h5py.File(str(h5_path), "r") as hf:
        return int(hf.attrs["total_samples"])


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
    scale = np.float32(float(table.schema.metadata[b"int32_scale_uv"]))
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        matrix[i] = table.column(c).to_numpy().astype(np.float32)
    matrix *= scale
    return matrix


def _read_int32_calibrated_arrow(parquet_dir: Path, columns: list[str],
                                 start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read int32 calibrated Parquet using Arrow compute kernels."""
    import pyarrow as pa

    table = pq.read_table(
        str(parquet_dir), columns=["samplestamp"] + columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    cal = json.loads(table.schema.metadata[b"int32_calibration"].decode("utf-8"))
    cast_opts = pc.CastOptions(target_type=pa.float64(), allow_int_overflow=True)
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        col_f = pc.cast(table.column(c), options=cast_opts)
        col_f = pc.add(pc.multiply(col_f, cal[c]["gain"]), cal[c]["offset"])
        matrix[i] = pc.cast(col_f, pa.float32()).to_numpy(zero_copy_only=False)
    return matrix


def _read_int32_nanovolt_arrow(parquet_dir: Path, columns: list[str],
                               start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read int32 nanovolt Parquet using Arrow compute kernels."""
    import pyarrow as pa

    table = pq.read_table(
        str(parquet_dir), columns=["samplestamp"] + columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    scale = float(table.schema.metadata[b"int32_scale_uv"])
    n_rows = table.num_rows
    matrix = np.empty((len(columns), n_rows), dtype=np.float32)
    for i, c in enumerate(columns):
        col_f = pc.multiply(table.column(c), scale)
        matrix[i] = pc.cast(col_f, pa.float32()).to_numpy(zero_copy_only=False)
    return matrix


def _read_tuned_pq(path: Path, columns: list[str],
                   start_stamp: int, end_stamp: int) -> np.ndarray:
    """Read from a single consolidated Parquet file."""
    table = pq.read_table(
        str(path), columns=columns,
        filters=[("samplestamp", ">=", start_stamp), ("samplestamp", "<=", end_stamp)],
    )
    if table.num_rows == 0:
        return np.empty((len(columns), 0), dtype=np.float32)
    return np.vstack([
        table.column(c).to_numpy().astype(np.float32, copy=False)
        for c in columns
    ])