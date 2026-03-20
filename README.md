# EEG Format Benchmarks

Benchmarks comparing **EDF**, **HDF5**, and **Apache Parquet** for reading,
processing, and storing clinical EEG and PSG waveform data at scale.

### Fairness note

Parquet files include per-row-group min/max column statistics as part of the
format spec — readers like pyarrow use these automatically to skip irrelevant
data during filtered reads, with no extra work from the user.

HDF5 has no equivalent built-in index. To evaluate HDF5's best-case
performance, the benchmark builds a custom `chunk_index` dataset at
conversion time (a small lookup table of stamp ranges per chunk). **The HDF5
results therefore represent an optimistic upper bound** — standard HDF5
files without this index would need to scan the full timestamp dataset on
every read, which is significantly slower. See the
[benchmark report](benchmark/docs/benchmark_report.md) for details.

## Quickstart

```bash
# 1. Clone and install dependencies
git clone <repo-url> && cd nw-data-benchmarks
pip install -r requirements.txt

# 2. Run benchmarks (downloads ~1 GB sample data from Azure on first run)
python benchmark/scripts/run_benchmarks.py

# 3. Run specific categories only
python benchmark/scripts/run_benchmarks.py --categories random_access channel_subset window_scaling
```

The default configuration downloads a 46-channel, 256 Hz, ~12.9-hour EEG study
as float32 Parquet from a public Azure Blob container. No credentials needed.

## Default data source

| Property         | Value |
|------------------|-------|
| Storage account  | `nwcsandboxstorage` |
| Container        | `waveforms` |
| Parquet path     | `parquet/Suppression~ B_54c97daa-...float32.snappy.parquet/` |
| Channels         | 46 (10-20 + auxiliaries) |
| Sample rate      | 256 Hz |
| Duration         | ~12.9 hours (~11.85M samples) |
| Size (Parquet)   | ~1.1 GB (float32, snappy) |

Data is cached locally in `.benchmark_cache/` after the first download.

## What gets benchmarked

| ID | Category | What it measures |
|----|----------|-----------------|
| A  | Random access | Read 1-min window from different positions |
| B  | Channel subset | Read 4, 10, or all 46 channels |
| C  | Re-montage | Read + bipolar montage computation |
| D.1 | Filter pipeline | Full study: read + montage + notch + bandpass |
| D.2 | Sliding FFT | Full study: pipeline + 10s FFT windows |
| E  | Window scaling | Throughput vs. window size (10s to 60min) |
| F  | Compression | Parquet codec comparison (none/snappy/zstd/lz4) |
| G  | Precision | EDF 16-bit quantization error |
| H  | Int32 storage | Int32 nanovolt and calibrated Parquet variants |
| I  | Remote query | DuckDB remote Parquet vs. full-file download |
| J  | Tuned comparison | Parquet vs HDF5 at matched block sizes |

## Formats compared

- **EDF** — European Data Format. Row-oriented, 16-bit, no compression. Derived from Parquet.
- **HDF5 columnar** — One 1D dataset per channel, LZ4 compressed, chunked along time.
- **HDF5 row-group** — Single 2D dataset (samples × channels), LZ4, chunk-aligned to Parquet row groups.
- **Parquet** — Columnar, per-column encoding, snappy/zstd/lz4. Source format.

## Configuration

Edit `benchmark/config/default.yaml` to:

- Point to different study data (Parquet URL or local path)
- Select which benchmarks to run
- Adjust window sizes, channel subsets, repetitions

### Using your own data

Set `source: "parquet"` and provide a `remote_parquet_url` or `local_path`:

```yaml
studies:
  - name: "my_study"
    source: "parquet"
    remote_parquet_url: "parquet/my_study_folder/"
```

Parquet files must have `samplestamp` (int64) and `ch_<label>` (float32) columns.

### Starting from your own HDF5 data

If you have EEG data in HDF5 already, you can convert it to the formats
used by these benchmarks with a short script. The benchmark suite expects:

- **Parquet:** one `samplestamp` column (int64) and one `ch_<label>` column
  (float32, microvolts) per channel. Samplestamp values must be monotonically
  non-decreasing but can have gaps and variable stride.
- **HDF5 columnar:** one 1D dataset per channel under `/channels/<label>`,
  plus a `/samplestamp` dataset and a `/chunk_index` dataset for fast seeks.

Here's an example conversion script:

```python
import h5py, numpy as np, pyarrow as pa, pyarrow.parquet as pq
import hdf5plugin

# -- Load your HDF5 data --
with h5py.File("my_study.h5", "r") as hf:
    # Adapt these lines to match your file's layout
    channels = {name: hf["eeg"][name][:] for name in hf["eeg"]}
    timestamps = hf["timestamps"][:]  # sample-level timestamps

# -- Write Parquet (snappy, 5-minute row groups) --
sample_rate = 256  # adjust to your data
row_group_size = 5 * 60 * sample_rate  # 5 minutes

cols = {"samplestamp": pa.array(timestamps, type=pa.int64())}
for name, data in channels.items():
    cols[f"ch_{name}"] = pa.array(data.astype(np.float32), type=pa.float32())

table = pa.table(cols)
pq.write_table(table, "my_study.parquet",
               compression="snappy", row_group_size=row_group_size,
               write_statistics=True)

# -- Write HDF5 columnar (LZ4, matched chunk size) --
n_samples = len(timestamps)
chunk_size = min(row_group_size, n_samples)

with h5py.File("my_study_columnar.h5", "w") as hf:
    grp = hf.create_group("channels")
    for name, data in channels.items():
        grp.create_dataset(name, data=data.astype(np.float32),
                           chunks=(chunk_size,), **hdf5plugin.LZ4())

    stamp_ds = hf.create_dataset("samplestamp", data=timestamps,
                                 chunks=(chunk_size,), **hdf5plugin.LZ4())

    # Build chunk index for fast seeks (required by the benchmark reader)
    n_chunks = (n_samples + chunk_size - 1) // chunk_size
    index = np.empty((n_chunks, 3), dtype=np.int64)
    for i in range(n_chunks):
        s, e = i * chunk_size, min((i + 1) * chunk_size, n_samples)
        index[i] = [s, timestamps[s], timestamps[e - 1]]
    hf.create_dataset("chunk_index", data=index)
    hf.attrs["total_samples"] = n_samples
```

Then point the config at your local files:

```yaml
studies:
  - name: "my_study"
    source: "parquet"
    local_path: "/path/to/directory/containing/my_study.parquet"
```

The benchmark suite will derive EDF and additional HDF5/Parquet variants
(different block sizes, compression codecs) automatically from the source
Parquet data.

### Using native NeuroWorks data (optional)

If the `nwreader` SDK is installed, you can start from ERD format:

```yaml
studies:
  - name: "my_study"
    source: "erd"
    blob_prefix: "path/to/erd/study"
```

## Output

- **JSON results** → `benchmark/results/` (gitignored, regenerated each run)
- **Markdown reports** → `benchmark/docs/`

## Requirements

- Python 3.10+
- pyarrow 19+ (tested on 23.0.1)
- See `requirements.txt` for full dependency list
- ~5 GB disk for cache (source data + derived format variants)

