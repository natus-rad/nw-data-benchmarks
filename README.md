# EEG/PSG Format Benchmarks

Comprehensive benchmarks comparing **EDF**, **HDF5**, and **Apache Parquet**
for reading, processing, and storing clinical EEG and PSG waveform data.

**Goal:** Investigate how multiple storage formats can be used together in a
**hybrid architecture** — Parquet for immutable signal data (optimized for
compression and cloud access), and HDF5 for metadata, annotations, and study
information (optimized for hierarchical organization and local access).

### Implementation notes

**Parquet:** Uses built-in per-row-group min/max column statistics (part of
the format spec) for predicate pushdown during filtered reads. No custom
indexing required.

**HDF5:** Standard HDF5 has no built-in index for skipping chunks based on
data values. To enable fair comparison, the benchmark builds a custom
`chunk_index` dataset at conversion time — a small lookup table of timestamp
ranges per chunk. This represents HDF5's best-case performance with a
purpose-built index. Standard HDF5 files without this index would require
scanning the full timestamp dataset on every filtered read, which is
significantly slower.

**EDF:** Uses raw byte-offset seeks with no indexing. Fast for sequential
reads on single files, but requires full file scan for filtered access.

See the [benchmark report](benchmark/docs/benchmark_report.md) for detailed
analysis and recommendations for hybrid architectures.

## Quickstart

```bash
# 1. Clone and install dependencies
git clone <repo-url> && cd nw-data-benchmarks
pip install -r requirements.txt

# 2. Run benchmarks (downloads ~1 GB sample data from Azure on first run)
python benchmark/scripts/run_benchmarks.py

#    This also generates benchmark/docs/benchmark_report.md and .html by default.
#    Use --no-report if you only want the JSON results file.

# 3. Run specific categories only
python benchmark/scripts/run_benchmarks.py --categories random_access channel_subset window_scaling

# 4. Regenerate the Markdown benchmark report from the latest JSON results
python benchmark/scripts/generate_benchmark_report.py

# Or generate from a specific results file / custom output path
python benchmark/scripts/generate_benchmark_report.py --input benchmark/results/<file>.json --output benchmark/docs/benchmark_report.md

# Or generate Markdown + HTML explicitly from the standalone script
python benchmark/scripts/generate_benchmark_report.py --html
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
| Size (Parquet)   | ~759 MiB (float32, snappy) |
| Size (HDF5)      | ~1,343 MiB (float32, LZ4 columnar) |
| Size (EDF)       | ~1,040 MiB (int16, uncompressed) |
| Raw float32 baseline | 2,170 MiB (46 ch × 11.85M samples × 4 bytes + timestamps) |

Data is cached locally in `.benchmark_cache/` after the first download.

## What gets benchmarked

| ID | Category | What it measures |
|----|----------|-----------------|
| A  | Random access | Read 1-min window from different positions (0%, 50%, 75%, 95%) |
| B  | Channel subset | Read 4, 10, or all 46 channels (1-min window) |
| C  | Re-montage | Read + bipolar montage computation (1-min window) |
| D.1 | Filter pipeline | Full study: read + montage + notch + bandpass filters |
| D.2 | Sliding FFT | Full study: pipeline + 10s FFT windows with 2s stride |
| E  | Window scaling | Throughput vs. window size (10s to 60min) |
| F  | Compression | Parquet codec comparison (none/snappy/zstd/lz4) |
| G  | Precision | EDF 16-bit quantization error vs. float32 |
| H  | Int32 storage | Int32 nanovolt and calibrated Parquet variants |
| I  | Remote query | DuckDB remote Parquet vs. full-file download |
| J  | Tuned comparison | Parquet (snappy/LZ4) vs HDF5 at matched block sizes (5m–120m) |

## Formats compared

- **EDF** — European Data Format. Row-oriented, 16-bit signed integer, no compression.
  In this benchmark, EDF files are derived from the source Parquet data. Fast
  for sequential reads on single files, but limited to 16-bit precision and no
  columnar access.

- **HDF5 columnar** — One 1D dataset per channel, LZ4 compressed, chunked along time.
  Hierarchical, self-describing, efficient for selective column reads at small
  block sizes. Includes custom chunk index for fast seeks.

- **HDF5 row-group** — Single 2D dataset (samples × channels), LZ4 compressed,
  chunk-aligned to Parquet row groups. Tested for comparison but less efficient
  than columnar layout for selective reads.

- **Parquet** — Columnar, per-column encoding (dictionary, delta, RLE), multiple
  compression codecs (snappy/zstd/lz4). Includes built-in row-group statistics
  for predicate pushdown. Better compression ratio than HDF5, cloud-native,
  supports SQL engines and byte-range queries.

## Configuration

Edit `benchmark/config/default.yaml` to:

- Point `studies[].input` at different study data (local path or remote Azure blob path/URL)
- Select which benchmarks to run
- Adjust window sizes, channel subsets, repetitions

`benchmark/config/default.yaml` is the built-in example that uses the same
public remote Azure Parquet dataset we have been benchmarking against.

### Using your own data

Set `input:` to either:

- a local file or directory, or
- a remote Azure blob path/URL in the configured container

If you use a remote input, your config also needs an `azure:` block with the
storage account, container, and auth mode. `benchmark/config/default.yaml`
already shows a working public-anonymous example.

For the public remote Parquet dataset shape we've been using:

```yaml
azure:
  storage_account: "nwcsandboxstorage"
  container: "waveforms"
  anonymous: true

studies:
  - name: "my_study"
    input: "parquet/my_study_folder/"
    sample_freq: 256
```

For a local Parquet directory:

```yaml
studies:
  - name: "my_study"
    input: "/path/to/my_study.parquet"
    sample_freq: 256
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
    input: "/path/to/directory/containing/my_study.parquet"
    sample_freq: 256
```

The benchmark suite will derive EDF and additional HDF5/Parquet variants
(different block sizes, compression codecs) automatically from the input
data after it is ingested into canonical Parquet.

### Using native NeuroWorks data (optional)

If the `nwreader` SDK is installed, you can start from an ERD study directory:

```yaml
studies:
  - name: "my_study"
    input: "/path/to/erd/study"
```

Remote ERD/HDF5/EDF inputs can also be given via Azure blob paths/URLs as long
the referenced blob or prefix exists in the configured storage container and
the config includes the matching `azure:` settings.

## Output

- **JSON results** → `benchmark/results/` (gitignored, regenerated each run)
- **Markdown report template** → `benchmark/docs/benchmark_report.template.md`
- **Generated Markdown report** → `benchmark/docs/benchmark_report.md`

`run_benchmarks.py` now generates the Markdown + HTML report automatically at
the end of a successful run. Use `--no-report` to skip that post-processing.

You can also regenerate the report directly from results JSON with:

```bash
python benchmark/scripts/generate_benchmark_report.py
```

If `--input` is omitted, the script automatically selects the newest
`*_benchmark_results.json` file in `benchmark/results/`.

## Recommended hybrid architecture

Based on benchmark results, a production system should use:

1. **Parquet for immutable signal data:**
   - Better compression (2.86× vs 2,170 MiB raw; 1.8× smaller than HDF5 LZ4)
   - Cloud-native (byte-range queries, SQL engines like DuckDB/Spark)
   - Scales efficiently to large block sizes (30m+)
   - Supports remote access and distributed processing

2. **HDF5 for metadata, annotations, and study information:**
   - Hierarchical organization (patient info, study metadata, annotations)
   - Self-describing format with flexible schema
   - Efficient for structured data and selective reads
   - Can include video sync metadata and chunk indices

3. **Video storage (optional):**
   - Store separately from HDF5 (video is large and has different access patterns)
   - Include sync metadata in HDF5 for coordinated playback
   - Use standard video codecs (H.264, VP9) for compatibility

4. **EDF for legacy compatibility:**
   - Use only when required for compatibility with existing systems
   - Limited to 16-bit precision and no compression
   - Suitable for simple sequential reads on single files

## Requirements

- Python 3.10+
- pyarrow 23+ (tested on 23.0.1)
- See `requirements.txt` for full dependency list
- ~5 GB disk for cache (source data + derived format variants)

