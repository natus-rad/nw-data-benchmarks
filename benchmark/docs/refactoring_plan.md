# Refactoring Plan — `run_benchmarks.py`

## Goal

Extract the ~2,600-line monolith into a focused module tree without changing any
function signatures or logic. Each step is a pure move, verifiable by running
`python run_benchmarks.py --dry-run` after each extraction.

---

## Proposed Module Structure

```
benchmark/
  scripts/
    run_benchmarks.py          # thin orchestrator (main, run_benchmarks, BENCHMARKS registry)
  lib/
    __init__.py
    azure_storage.py           # group 1 — Azure download helpers
    study_info.py              # group 2 — StudyInfo class, config, system info
    readers/
      __init__.py
      parquet_reader.py        # group 3 — Parquet read helpers
      edf_reader.py            # group 4 — EDF read helpers
      hdf5_reader.py           # group 5 — HDF5 read helpers
    converters/
      __init__.py
      edf_writer.py            # group 6 — Parquet → EDF conversion
      hdf5_writer.py           # group 7 — Parquet → HDF5 conversion (both layouts + tuned)
      parquet_variants.py      # group 8 — Parquet re-compression, int32, tuned block sizes
    signal/
      __init__.py
      montage.py               # group 9 — bipolar montage + channel constants
      filters.py               # group 10 — SOS filter build + apply
    benchmarks/
      __init__.py
      base.py                  # group 11 — shared utilities (_timed, _throughput, etc.)
      bench_random_access.py   # benchmark A
      bench_channel_subset.py  # benchmark B
      bench_remontage.py       # benchmark C
      bench_filter_pipeline.py # benchmark D
      bench_window_scaling.py  # benchmark E
      bench_compression.py     # benchmark F
      bench_precision_loss.py  # benchmark G
      bench_int32_storage.py   # benchmark H (includes _read_int32_* helpers)
      bench_remote_query.py    # benchmark I
      bench_tuned_comparison.py# benchmark J
    remote/
      __init__.py
      duckdb_remote.py         # group 13 — DuckDB Azure connection + query helpers
```

---

## Group Descriptions

### Group 1 — `lib/azure_storage.py`

**Moves:** `_get_blob_service_client`, `download_study`, `_download_edf_from_azure`

The only three functions that touch the Azure SDK. Isolating them means the rest
of the codebase does not import `azure-storage-blob` at all, so the script works
without the SDK installed (until a remote benchmark is actually run).

No OOP required. Optionally, `_get_blob_service_client` and its auth priority
chain (anonymous → SAS → `DefaultAzureCredential`) can become an
`AzureStorageClient(account, container, sas_token)` dataclass to avoid threading
auth through every call — but the free functions are also fine.

---

### Group 2 — `lib/study_info.py`

**Moves:** `StudyInfo` class, `StudyInfo.from_parquet`, `_study_info`,
`load_config`, `_system_info`

`StudyInfo` already exists as a class. `_study_info` (which picks between
Parquet and nwreader sources) should become a second `@classmethod` on
`StudyInfo` — e.g. `StudyInfo.from_source(study_dir, source_type, study_cfg)` —
rather than a free function. `load_config` and `_system_info` are small utilities
that belong here or in a top-level `utils.py`.

---

### Group 3 — `lib/readers/parquet_reader.py`

**Moves:** `_read_parquet_window`, `_count_total_rows`, `_read_tuned_pq`,
`_read_int32_calibrated`, `_read_int32_nanovolt`, `_read_int32_calibrated_arrow`,
`_read_int32_nanovolt_arrow`

All are pure functions: path + stamp range → numpy matrix. A lightweight
`ParquetReader(parquet_dir, columns)` class is worth considering so callers do
not repeat those two arguments on every call, but the free functions work as-is.

---

### Group 4 — `lib/readers/edf_reader.py`

**Moves:** `_read_edf_window`, `_edf_total_samples`, `_edf_file`

**This is the one group where a class has a measurable performance benefit.**
All three functions open a `pyedflib.EdfReader` and immediately close it.
Benchmarks A, B, C, and E call `_read_edf_window` hundreds of times in a tight
loop, paying the open/close cost on every repetition.

A thin `EdfReader(path)` context manager that keeps the file open and exposes
`total_samples`, `read_window(start, n, channels)`, and `signal_labels` removes
that overhead. This is the only reader refactor that should be done eagerly;
the others can wait.

---

### Group 5 — `lib/readers/hdf5_reader.py`

**Moves:** `_h5_resolve_stamp_range`, `_read_h5_columnar_window`,
`_read_h5_rowgroup_window`, `_h5_total_samples`

Same open/close cost concern as EDF. An `HDF5Reader(path)` that keeps the file
open and dispatches to the columnar or rowgroup read path based on
`hf.attrs["layout"]` would reduce repeated file-open overhead. The two layout
variants become methods on the same class.

---

### Group 6 — `lib/converters/edf_writer.py`

**Moves:** `_parquet_to_edf`

One function, one file. The two-pass streaming conversion is complex enough to
warrant its own home. No class needed — the function is already self-contained.

---

### Group 7 — `lib/converters/hdf5_writer.py`

**Moves:** `_count_total_rows`, `_default_h5_chunk_samples`,
`_parquet_rowgroup_chunk_samples`, `_setup_h5_variants`, `_build_chunk_index`,
`_write_h5_columnar`, `_write_h5_rowgroup`, `_setup_tuned_h5`,
`H5_COLUMNAR_CHUNK_SECONDS`

All strongly coupled — they share the chunk-size helpers and `_build_chunk_index`.
`_setup_tuned_h5` belongs here too since it is just another HDF5 write path.

---

### Group 8 — `lib/converters/parquet_variants.py`

**Moves:** `_setup_parquet_compression_variants`, `_write_parquet_dataset_metadata`,
`_setup_int32_variants`, `_setup_tuned_parquet`, `_get_tuned_block_sizes`,
`NANOVOLT_SCALE`

All Parquet-to-Parquet transformations: re-compression, int32 encoding, and tuned
block sizes. They share `_write_parquet_dataset_metadata`. Grouping them makes the
encoding choices easy to find and audit independently.

---

### Group 9 — `lib/signal/montage.py`

**Moves:** `_apply_bipolar_montage`, `BIPOLAR_PAIRS`, `CHANNELS_10_20`

---

### Group 10 — `lib/signal/filters.py`

**Moves:** `_build_sos`, `_apply_filters`

These two are always used together. Short file, but the right home so that future
filter changes are isolated.

---

### Group 11 — `lib/benchmarks/base.py`

**Moves:** `_timed`, `_throughput`, `_chunk_ranges`, `_full_study_duration_hours`,
`_print_result`, `_estimate_runs`, `BYTES_PER_FLOAT32`

Shared utilities used by every `bench_*` function. A `BenchmarkResult` dataclass
could replace raw dicts to catch key-name typos at definition time and enable IDE
autocompletion, but the dicts are fine to keep initially.

---

### Groups 12+ — `lib/benchmarks/bench_*.py`

One file per benchmark category (A through J). Each file contains the `bench_*`
function and any private helpers used exclusively by it:

| File | Contains |
|------|----------|
| `bench_random_access.py` | `bench_random_access` |
| `bench_channel_subset.py` | `bench_channel_subset` |
| `bench_remontage.py` | `bench_remontage` |
| `bench_filter_pipeline.py` | `bench_filter_pipeline` |
| `bench_window_scaling.py` | `bench_window_scaling` |
| `bench_compression.py` | `bench_compression` |
| `bench_precision_loss.py` | `bench_precision_loss` |
| `bench_int32_storage.py` | `bench_int32_storage`, `_read_int32_*` helpers |
| `bench_remote_query.py` | `bench_remote_query` |
| `bench_tuned_comparison.py` | `bench_tuned_comparison`, `_read_tuned_pq` |

---

### Group 13 — `lib/remote/duckdb_remote.py`

**Moves:** `_make_duckdb_connection`, `_duckdb_remote_read`

Isolated so the DuckDB import is optional. If `duckdb` is not installed the rest
of the benchmarks still run; only benchmark I fails at import time.

---

## What NOT to Do

- **Do not change function signatures during extraction.** Move first, refactor second.
- **Do not introduce a benchmark base class.** The `bench_*` functions all share
  the `(info, paths, cfg)` signature already — that is a sufficient protocol
  without a formal ABC.
- **Do not convert the `paths` dict to a class yet.** It is simple and touching
  it would require updating every benchmark function simultaneously.
- **Do not merge the readers into one unified class.** Parquet, EDF, and HDF5
  have different APIs and the abstraction would not be natural.

---

## Recommended Extraction Order (Lowest Risk First)

| Step | Target | Why first |
|------|--------|-----------|
| 1 | `lib/signal/` | No dependencies on anything else; easiest to test in isolation |
| 2 | `lib/readers/edf_reader.py` | Small, self-contained; `EdfReader` class has a real perf benefit |
| 3 | `lib/readers/parquet_reader.py` + `hdf5_reader.py` | Pure functions; trivial moves |
| 4 | `lib/azure_storage.py` | Isolates the Azure SDK import |
| 5 | `lib/converters/` | Three independent writer groups |
| 6 | `lib/study_info.py` | Consolidates `StudyInfo` and its factory methods |
| 7 | `lib/benchmarks/base.py` + individual `bench_*.py` | Do last; depends on everything above |
| 8 | Thin out `run_benchmarks.py` | Final step: only orchestrator code remains |

After each step, verify with:

```bash
python benchmark/scripts/run_benchmarks.py --dry-run --config benchmark/config/default.yaml
```

