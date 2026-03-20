# EEG Format Benchmarks

Benchmarks comparing **EDF**, **HDF5**, and **Apache Parquet** for reading, processing, and storing clinical EEG and PSG waveform data at scale.

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
- See `requirements.txt` for dependencies
- ~3 GB disk for cache (source data + derived formats)

