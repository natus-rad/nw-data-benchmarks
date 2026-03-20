# EDF vs HDF5 vs Parquet — Benchmark Results

**Study:** 46-channel EEG, 256 Hz, ~12.9 hours (11.85M samples)

All formats store the same float32 waveform data. EDF and HDF5 are derived
from the source Parquet files. HDF5 uses LZ4 compression with chunk sizes
matching Parquet row groups (76,800 samples = 300s at 256 Hz).

Both HDF5 and Parquet use a small index to skip irrelevant data blocks:
- **Parquet** stores per-row-group min/max statistics in the file footer.
- **HDF5** stores a `chunk_index` dataset with `(start_idx, min_stamp, max_stamp)`
  per chunk, built at conversion time.

At read time, both formats scan their index to find overlapping blocks, then
read only the relevant data. This is an apples-to-apples comparison.

## Summary

| Benchmark | Parquet | HDF5 columnar | HDF5 rowgroup | EDF |
|-----------|---------|---------------|---------------|-----|
| Random access (1 min) | 0.047s | 0.039s | 0.038s | 0.063s |
| 4-channel subset (1 min) | 0.029s | 0.005s | 0.034s | 0.005s |
| Full pipeline, 12h | 16.4s | 11.6s | 14.5s | 86.6s |
| Pipeline + FFT, 12h | 35.0s | 19.4s | 22.7s | 65.0s |
| Peak throughput (60 min) | 278 MiB/s | 350 MiB/s | 157 MiB/s | 42 MiB/s |

## A — Random access

Read a 1-minute, 46-channel window from four positions.

| Position | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 0% | 0.047s (57 MiB/s) | 0.048s (57 MiB/s) | 0.047s (57 MiB/s) | 0.058s (46 MiB/s) |
| 50% | 0.046s (58 MiB/s) | 0.033s (81 MiB/s) | 0.029s (92 MiB/s) | 0.075s (36 MiB/s) |
| 75% | 0.048s (56 MiB/s) | 0.035s (77 MiB/s) | 0.039s (69 MiB/s) | 0.061s (44 MiB/s) |
| 95% | 0.053s (51 MiB/s) | 0.039s (69 MiB/s) | 0.031s (87 MiB/s) | 0.060s (45 MiB/s) |

All three compressed formats are faster than EDF. HDF5 has a slight edge
over Parquet for single-window reads at non-zero positions because its chunk
index lookup is simpler than Parquet's row-group footer parsing.

## B — Channel subset

Read a 1-minute window with 4, 10, or all 46 channels.

| Channels | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 4 | 0.029s (8 MiB/s) | 0.005s (48 MiB/s) | 0.034s (7 MiB/s) | 0.005s (45 MiB/s) |
| 10 | 0.047s (13 MiB/s) | 0.013s (46 MiB/s) | 0.030s (20 MiB/s) | 0.015s (40 MiB/s) |
| 46 (all) | 0.050s (54 MiB/s) | 0.034s (81 MiB/s) | 0.030s (90 MiB/s) | 0.063s (43 MiB/s) |

HDF5 columnar excels at small channel subsets — reading 4 channels takes
5 ms because each channel is a separate dataset with independent chunks.
The rowgroup layout must decompress full chunks containing all 46 channels.
EDF is also fast for small subsets because pyedflib reads individual signal
records. Parquet's per-read overhead (footer parsing, row-group stats) is
proportionally larger for small reads.

## C — Re-montaging

Read 1 minute + apply standard bipolar montage (18 derived channels from 19 inputs).

| Format | Read | Montage | Total |
|--------|------|---------|-------|
| Parquet | 0.054s | 0.001s | 0.055s |
| HDF5 col | 0.038s | 0.001s | 0.039s |
| HDF5 rg | 0.031s | 0.001s | 0.032s |
| EDF | 0.063s | 0.001s | 0.064s |

Montage computation is negligible (~1 ms). The bottleneck is I/O.

## D.1 — Full-study filter pipeline (12 hours)

Read + bipolar montage + 60 Hz notch + 0.5–70 Hz bandpass, chunked in 300s blocks.

| Format | Read | Montage | Filter | Total |
|--------|------|---------|--------|-------|
| HDF5 col | 7.5s | 0.56s | 3.5s | **11.6s** |
| HDF5 rg | 10.4s | 0.58s | 3.5s | **14.5s** |
| Parquet | 12.3s | 0.57s | 3.5s | **16.4s** |
| EDF | 82.5s | 0.58s | 3.5s | **86.6s** |

HDF5 columnar is fastest for sequential full-study reads — its chunked LZ4
layout is efficient for large sequential scans. Parquet's row-group metadata
overhead adds up over ~154 chunk reads.

## D.2 — Sliding FFT (12 hours, 10s windows, 2s stride)

| Format | Read | Montage | Filter | FFT | Total | FFT windows |
|--------|------|---------|--------|-----|-------|-------------|
| HDF5 col | 7.2s | 0.56s | 3.3s | 8.4s | **19.4s** | 21,596 |
| HDF5 rg | 10.0s | 0.58s | 3.5s | 8.6s | **22.7s** | 21,596 |
| Parquet | 12.8s | 0.56s | 3.2s | 9.3s | **35.0s** | 21,596 |
| EDF | 47.8s | 0.58s | 3.5s | 9.8s | **65.0s** | 21,596 |

Compute (montage + filter + FFT) is similar across formats. The difference
is I/O.

## E — Window size scaling (throughput in MiB/s)

| Window | Parquet | HDF5 col | HDF5 rg | EDF |
|--------|---------|----------|---------|-----|
| 10s | 12.6 | 12.2 | 15.1 | 37.5 |
| 30s | 34.2 | 33.5 | 33.1 | 41.3 |
| 1 min | 60.3 | 59.7 | 81.1 | 43.9 |
| 5 min | 198.6 | 223.8 | 158.6 | 42.5 |
| 15 min | 263.7 | 238.7 | 146.1 | 43.3 |
| 30 min | 294.0 | 244.6 | 162.4 | 39.4 |
| 60 min | **277.9** | **349.6** | 156.7 | 41.5 |

HDF5 columnar achieves the highest throughput for large sequential reads
(350 MiB/s at 60 min). Parquet peaks at 294 MiB/s. EDF throughput is flat
at ~42 MiB/s regardless of window size.

For small windows (≤30s), EDF is fastest because it has zero metadata
overhead — it just seeks to a byte offset and reads. Both Parquet and HDF5
have per-read overhead (index lookup, file open) that dominates at small sizes.

## Key observations

1. **HDF5 columnar is competitive with or faster than Parquet for local reads.**
   With a chunk-level stamp index (analogous to Parquet row-group statistics),
   HDF5's chunked LZ4 layout delivers high throughput for sequential reads
   and excels at small channel subsets.

2. **Parquet's advantages are elsewhere.** Parquet's strengths — predicate
   pushdown, native SQL engine support (DuckDB, Spark, Athena, BigQuery),
   and cloud-native byte-range queries — matter most for remote access,
   ad-hoc queries, and integration with data platforms. These are not
   captured in local read benchmarks.

3. **EDF is fast for simple local reads.** On a single contiguous file, EDF's
   raw byte-offset seeks are fast. EDF's real limitations are 16-bit precision,
   no compression, no columnar access, and no remote query support.

4. **The best architecture uses both formats.** HDF5 for local waveform storage
   and metadata (fast reads, self-describing, hierarchical). Parquet for cloud
   storage, cross-platform queries, and data pipeline integration.

