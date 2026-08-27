# EEG/PSG Format Benchmarks

Benchmark read, processing, and storage trade-offs across **EDF**, **HDF5**, and **Apache Parquet** for clinical EEG/PSG waveform data.

This repo benchmarks a study from a local path or Azure blob input, normalizes it to a cached **canonical single-file Parquet**, generates the **variants** you ask for, then writes **JSON + Markdown/HTML reports**.

## Format caveats

- **Parquet** uses built-in row-group statistics for predicate pushdown during filtered reads.
- **HDF5** benchmark results include a benchmark-specific `chunk_index` helper dataset for fast seeks, so they represent **optimized HDF5-with-helper** behavior, not plain generic HDF5 without that helper.
- **EDF** uses raw byte-offset seeks with no indexing and is limited to 16-bit quantized storage.

See the generated [benchmark report](benchmark/docs/benchmark_report.md) for the latest results and recommendations.

## Quickstart

```bash
# 1. Clone and install dependencies
git clone <repo-url> && cd nw-data-benchmarks
pip install -r requirements.txt

# 2. Run the default benchmark suite (see benchmark/config/default.yaml)
python -m benchmark.scripts.run_benchmarks

#    This writes benchmark/results/*.json and generates
#    benchmark/docs/benchmark_report.md + .html by default.
#    Use --no-report to skip report generation.

# 3. Run only selected categories
python -m benchmark.scripts.run_benchmarks --categories random_access channel_subset window_scaling

# 4. Regenerate the report from the latest results JSON
python -m benchmark.scripts.generate_benchmark_report

# 5. Regenerate Markdown + HTML from a specific results file
python -m benchmark.scripts.generate_benchmark_report --input benchmark/results/<file>.json --html
```

The default configuration uses a public Azure HDF5 copy of the study (~12.9 hours, 46 channels, 256 Hz) and caches data in `.benchmark_cache/` on first use. A Parquet copy of the same study is hosted alongside it and is used by the remote-query benchmark (I).

## Default data source

| Property | Value |
|---|---|
| Storage account | `nwcsandboxstorage` |
| Container | `waveforms` |
| Default input (HDF5, columnar) | `external/benchmarks/h5/hdf5_col_30m_d928f99b.h5` |
| Parquet copy | `external/benchmarks/parquet/Suppression~ B_54c97daa-...float32.snappy.parquet/` |
| Channels | 46 (10-20 + auxiliaries) |
| Sample rate | 256 Hz |
| Duration | ~12.9 hours (~11.85M samples) |
| Size (Parquet) | ~759 MiB (float32, snappy) |
| Size (HDF5) | ~1,343 MiB (float32, LZ4 columnar) |
| Size (EDF) | ~1,040 MiB (int16, uncompressed) |
| Raw float32 baseline | 2,170 MiB |

## Pipeline at a glance

`studies[].input` can point to a local file/directory or to an Azure blob path/URL. Once resolved, the pipeline is:

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                    INPUT (local path or Azure blob)                      │
│   .edf  │  .h5/.hdf5   │  .parquet (file/dir)  │  ERD (study dir)        │
└────┬────┴──────┬───────┴──────────┬────────────┴──────┬──────────────────┘
     │           │                  │                   │
     ▼           ▼                  ▼                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│            INGEST / REWRITE TO CANONICAL SINGLE-FILE PARQUET             │
│  _ingest_edf()  _ingest_hdf5()  _rewrite_parquet_input()  _ingest_erd()  │
│                                                                          │
│  Output: cached single-file Parquet with samplestamp + ch_<label> cols   │
│  + StudyInfo metadata and a disk-backed samplestamp cache                │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              VARIANT GENERATION (from canonical Parquet)                 │
│                                                                          │
│  For each entry in config `variants:`:                                   │
│    → Parquet variant: row-group layout + codec                           │
│    → HDF5 variant: columnar/rowgroup layout + chunking + compression     │
│    → EDF variant: EDF rewrite (16-bit quantized)                         │
│                                                                          │
│  Generated root variants are optional. Benchmark K can also reuse the    │
│  resolved baseline/input source artifact directly.                       │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        BENCHMARK EXECUTION                               │
│                                                                          │
│  Core benchmarks (A–E): selected root variants (+ canonical optional)    │
│  Parquet investigations (F–I): separate config section, Parquet-focused  │
│  Comparison workloads (J, K): tuned variants or baseline input data      │
└──────────────────────────────────────────────────────────────────────────┘
```

Notes:

- Canonical Parquet is always materialized into `.benchmark_cache/`, even when the input is already Parquet.
- `StudyInfo` uses a reusable disk-backed stamp cache built from canonical Parquet rather than an eagerly loaded in-memory stamp array.
- `baseline_comparison` (K) measures the original/resolved source artifact directly; it does not generate extra root variants.
- `include_canonical: true` appends the cached canonical Parquet file to a core benchmark category's targets.

## What gets benchmarked

| ID | Category | What it measures |
|---|---|---|
| A | Random access | Read a 1-minute window from multiple positions |
| B | Channel subset | Read a subset of channels from the same window |
| C | Re-montage | Read + bipolar montage computation |
| D.1 | Filter pipeline | Full-study read + montage + filters |
| D.2 | Sliding FFT | Full-study pipeline + overlapping FFT windows |
| E | Window scaling | Throughput vs. window size |
| F | Compression | Parquet codec size/speed trade-offs |
| G | Precision loss | EDF 16-bit quantization error vs. float32 |
| H | Int32 storage | Int32 Parquet storage modes |
| I | Remote query | Remote DuckDB-over-Parquet vs. streamed download paths |
| J | Tuned comparison | Tuned Parquet vs. tuned HDF5 at matched block sizes |
| K | Baseline comparison | J-style workloads on the resolved source artifact |

## Formats compared

- **Parquet** — columnar, compressed, cloud-friendly, and the canonical intermediate used by this benchmark suite.
- **HDF5 columnar** — one 1D dataset per channel, efficient for selective reads at smaller chunk sizes.
- **HDF5 row-group** — one 2D dataset chunked along time, included for comparison.
- **EDF** — single-file sequential baseline and compatibility format.

## Configuration

Use `benchmark/config/default.yaml` as the reference config. The key top-level blocks are:

- `studies` — one or more studies, either `dataset: <key>` references into `benchmark/config/datasets.yaml` or inline entries with `name`, `input`, and `sample_freq` when needed
- `canonical_parquet` — how the cached canonical Parquet is written
- `variants` — the root benchmark targets to generate, each with a stable `id`
- `benchmarks.core` — A–E, with per-category `targets: all | [id, ...] | []`, optional `include_canonical: true`, and knobs under `params:`
- `benchmarks.parquet_investigations` — F–I, each with `enabled` and optional `params:`
- `benchmarks.other` — J (`tuned_comparison`) and K (`baseline_comparison`), same shape

Every benchmark category uses the same `enabled` / `targets` / `params` shape. Legacy configs that spell core `targets:` as `variants:` or write knobs directly on the category still load unchanged.

Minimal shape:

```yaml
studies:
  - name: "my_study"
    input: "/path/to/my_study.parquet"
    sample_freq: 256

canonical_parquet:
  id: canonical
  compression: snappy
  row_group_minutes: 30

variants:
  - id: pq_30m_lz4
    format: parquet
    row_group_minutes: 30
    compression: lz4
  - id: hdf5_col_30m
    format: hdf5
    layout: columnar
    chunk_minutes: 30
    compression: lz4

benchmarks:
  core:
    random_access:
      enabled: true
      targets: all
  parquet_investigations:
    compression:
      enabled: true
  other:
    baseline_comparison:
      enabled: true
```

## Input expectations

- **Parquet input**: file or directory with `samplestamp` (`int64`) and `ch_<label>` (`float32`) columns. `sample_freq` must be specified in config for Parquet input.
- **HDF5 input**: either `/channels/<label>` datasets or a 2D `/data` dataset. Timestamps can come from `samplestamp`, `timestamps`, `time`, or `sample_index`. `sample_freq` can come from an HDF5 attribute or from config.
- **EDF input**: sample frequency is usually read from the file; you can still supply it explicitly.
- **ERD input**: pass the study directory. This path requires the optional `nwreader` dependency.
- **Remote input**: provide an Azure blob path/URL plus an `azure:` block. `benchmark/config/default.yaml` is the working public example.

## Output

- **JSON results** → `benchmark/results/`
- **Markdown report template** → `benchmark/docs/benchmark_report.template.md`
- **Generated Markdown report** → `benchmark/docs/benchmark_report.md`
- **Generated HTML report** → `benchmark/docs/benchmark_report.html`

`run_benchmarks.py` generates the Markdown + HTML report automatically after a successful run. Use `--no-report` to skip that step.

If `--input` is omitted, `generate_benchmark_report.py` automatically picks the newest `*_benchmark_results.json` file in `benchmark/results/`.

## Practical takeaway

- Use **Parquet** for the main immutable waveform payload and remote/cloud workflows.
- Use **HDF5** only with the understanding that the benchmarked seek/read path includes the extra `chunk_index` helper.
- Use **EDF** mainly for compatibility and baseline comparisons.

## Requirements

- Python 3.10+
- See `requirements.txt` for dependencies
- ~5 GB free disk space for cached inputs and derived variants

## HDF5 input quick start

If you want to benchmark your own HDF5 EEG data with minimal setup, start from `benchmark/config/from_hdf5.template.yaml`.

### Fastest path

1. Copy `benchmark/config/from_hdf5.template.yaml` to a new config file.
2. Set `studies[0].input` to your HDF5 file path.
3. If your file does not store `sample_freq` in HDF5 file attributes, set `studies[0].sample_freq` explicitly.
4. If you want to benchmark only your source HDF5 artifact and avoid generating extra comparison artifacts, set `variants: []` and enable only the benchmark sections you want.
5. Run `python -m benchmark.scripts.run_benchmarks --config your_config.yaml`.

### Recommended HDF5-only config shape

If your goal is to test only your existing HDF5 data, use:

- core benchmarks A-E
- optional section K (`baseline_comparison`) for the J-style workload suite on the original HDF5 artifact
- `variants: []`

With that setup:

- core benchmarks read the original HDF5 source directly when no root variants exist
- section K also runs directly on the original HDF5 input artifact
- no extra benchmark variants are generated

### What HDF5 input layouts are supported

The ingest path currently supports either of these layouts:

#### `channels` group layout

- HDF5 group: `channels`
- one dataset per channel under `channels/<label>`
- all channel datasets must have the same length

The dataset names become the benchmark channel labels.

#### `data` matrix layout

- dataset: `data`
- 2D shape `(N, C)` where `N` is samples/rows and `C` is channels/columns

For this layout, channel labels are read from the HDF5 file attribute `channel_labels`.
If `channel_labels` is missing, the benchmark falls back to generated names like `0`, `1`, `2`, which then become canonical Parquet columns like `ch_0`, `ch_1`, `ch_2`.

### Sample frequency and timestamp expectations

The benchmark suite needs sampling frequency.

- It first looks for the HDF5 file attribute `sample_freq`.
- If that attribute is absent or invalid, set `studies[0].sample_freq` in config.
- If neither is provided, HDF5 ingest fails.

For timestamps/sample index, the ingest path looks for one of these datasets at the file root:

- `samplestamp`
- `timestamps`
- `time`
- `sample_index`

If none of those exists, the benchmark creates a default 0-based sample index automatically.

### Practical notes and pitfalls

- Internally, supported HDF5 input is normalized to canonical Parquet columns like `samplestamp` and `ch_<label>`.
- If your file has neither a `channels` group nor a 2D `data` dataset, the current ingest path will fail.
- All channels are expected to describe the same sample span.
- For best results, timestamps/sample indices should be monotonic and match the channel data length.
- Before a full run, use `python -m benchmark.scripts.run_benchmarks --config your_config.yaml --dry-run` to confirm the config loads and the selected benchmark sections match what you expect.
