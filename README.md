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

# 2. Run the default benchmark suite
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

The default configuration uses a public Azure Parquet study (~12.9 hours, 46 channels, 256 Hz) and caches data in `.benchmark_cache/` on first use.

## Default data source

| Property | Value |
|---|---|
| Storage account | `nwcsandboxstorage` |
| Container | `waveforms` |
| Parquet path | `parquet/Suppression~ B_54c97daa-...float32.snappy.parquet/` |
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

- `studies` — one or more studies with `name`, `input`, and `sample_freq` when needed
- `canonical_parquet` — how the cached canonical Parquet is written
- `variants` — the root benchmark targets to generate, each with a stable `id`
- `benchmarks.core` — A–E, with per-category `variants: all | [id, ...] | []` and optional `include_canonical: true`
- `benchmarks.parquet_investigations` — F–I
- `benchmarks.other` — J (`tuned_comparison`) and K (`baseline_comparison`)

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
      variants: all
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
