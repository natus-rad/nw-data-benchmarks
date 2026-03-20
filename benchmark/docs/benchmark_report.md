# EDF vs HDF5 vs Parquet — Benchmark Results

**Study:** 46-channel EEG, 256 Hz, ~12.9 hours (11.85M samples)

All formats store the same float32 waveform data. EDF and HDF5 are derived from
the source Parquet files. HDF5 uses LZ4 compression with chunk sizes matching
Parquet row groups (76,800 samples = 300s). Both HDF5 read paths use direct
index computation from stored metadata (no samplestamp scan), giving HDF5 the
same kind of seek advantage that Parquet gets from row-group statistics.

## Summary

| Benchmark | Parquet | HDF5 columnar | HDF5 rowgroup | EDF |
|-----------|---------|---------------|---------------|-----|
| Random access (1 min) | 0.044–0.054s | 0.033–0.040s | 0.032–0.039s | 0.055–0.057s |
| 4-channel subset (1 min) | 0.031s | 0.004s | 0.028s | 0.005s |
| Full pipeline, 12h | 14.2s | 9.9s | 12.0s | 48.0s |
| Pipeline + FFT, 12h | 23.1s | 18.0s | 20.3s | 61.7s |
| Peak throughput (60 min) | 244 MiB/s | 364 MiB/s | 166 MiB/s | 42 MiB/s |

## A — Random access

Read a 1-minute, 46-channel window from four positions.

| Position | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 0% | 0.044s (61 MiB/s) | 0.040s (68 MiB/s) | 0.039s (70 MiB/s) | 0.055s (49 MiB/s) |
| 50% | 0.044s (61 MiB/s) | 0.033s (82 MiB/s) | 0.037s (72 MiB/s) | 0.057s (47 MiB/s) |
| 75% | 0.054s (50 MiB/s) | 0.037s (73 MiB/s) | 0.035s (78 MiB/s) | 0.057s (47 MiB/s) |
| 95% | 0.050s (54 MiB/s) | 0.033s (81 MiB/s) | 0.032s (85 MiB/s) | 0.056s (48 MiB/s) |

All three binary formats are significantly faster than EDF for random access.
HDF5 has a slight edge over Parquet for single-window reads because direct
index computation avoids Parquet's row-group metadata overhead.

## B — Channel subset

Read a 1-minute window with 4, 10, or all 46 channels.

| Channels | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 4 | 0.031s (7.7 MiB/s) | 0.004s (66 MiB/s) | 0.028s (8.5 MiB/s) | 0.005s (44 MiB/s) |
| 10 | 0.037s (16 MiB/s) | 0.009s (67 MiB/s) | 0.028s (21 MiB/s) | 0.014s (43 MiB/s) |
| 46 (all) | 0.047s (58 MiB/s) | 0.032s (84 MiB/s) | 0.028s (96 MiB/s) | 0.065s (42 MiB/s) |

HDF5 columnar excels at small channel subsets — reading 4 channels takes only
4 ms because each channel is a separate dataset. Parquet's row-group metadata
overhead is proportionally larger for small reads. EDF reads the full record
regardless of channel count but is fast for small subsets because the data is
contiguous in memory.

## C — Re-montaging

Read 1 minute + apply standard bipolar montage (18 derived channels from 19 inputs).

| Format | Read | Montage | Total |
|--------|------|---------|-------|
| Parquet | 0.043s | 0.001s | 0.044s |
| HDF5 col | 0.044s | 0.001s | 0.045s |
| HDF5 rg | 0.029s | 0.001s | 0.030s |
| EDF | 0.063s | 0.001s | 0.064s |

Montage computation is negligible (~1 ms). The bottleneck is I/O.

## D.1 — Full-study filter pipeline (12 hours)

Read + bipolar montage + 60 Hz notch + 0.5–70 Hz bandpass, chunked in 300s blocks.

| Format | Read | Montage | Filter | Total |
|--------|------|---------|--------|-------|
| HDF5 col | 5.9s | 0.56s | 3.4s | **9.9s** |
| HDF5 rg | 7.9s | 0.58s | 3.5s | **12.0s** |
| Parquet | 10.2s | 0.57s | 3.5s | **14.2s** |
| EDF | 43.9s | 0.58s | 3.5s | **48.0s** |

HDF5 columnar is fastest for sequential full-study reads. Its chunked layout
with LZ4 decompression is very efficient for large sequential scans. Parquet's
row-group metadata overhead adds up over many chunks.

## D.2 — Sliding FFT (12 hours, 10s windows, 2s stride)

| Format | Read | Montage | Filter | FFT | Total | FFT windows |
|--------|------|---------|--------|-----|-------|-------------|
| HDF5 col | 5.7s | 0.56s | 3.3s | 8.4s | **18.0s** | 21,596 |
| HDF5 rg | 7.8s | 0.58s | 3.5s | 8.4s | **20.3s** | 21,596 |
| Parquet | 10.0s | 0.56s | 3.2s | 9.3s | **23.1s** | 21,596 |
| EDF | 47.8s | 0.58s | 3.5s | 9.8s | **61.7s** | 21,596 |

Compute (montage + filter + FFT) is identical across formats. The difference
is entirely I/O.

## E — Window size scaling (throughput)

| Window | Parquet | HDF5 col | HDF5 rg | EDF |
|--------|---------|----------|---------|-----|
| 10s | 9.6 MiB/s | 14.7 MiB/s | 16.3 MiB/s | 37.3 MiB/s |
| 30s | 29.3 MiB/s | 42.9 MiB/s | 48.7 MiB/s | 44.3 MiB/s |
| 1 min | 66.3 MiB/s | 73.6 MiB/s | 95.6 MiB/s | 40.8 MiB/s |
| 5 min | 193.3 MiB/s | 207.6 MiB/s | 164.4 MiB/s | 47.2 MiB/s |
| 15 min | 299.0 MiB/s | 346.1 MiB/s | 171.6 MiB/s | 44.8 MiB/s |
| 30 min | 303.7 MiB/s | 355.1 MiB/s | 158.5 MiB/s | 41.9 MiB/s |
| 60 min | 243.7 MiB/s | **363.7 MiB/s** | 165.6 MiB/s | 42.2 MiB/s |

HDF5 columnar achieves the highest throughput for large sequential reads
(364 MiB/s at 60 min). Parquet peaks at 304 MiB/s. EDF throughput is flat
at ~42 MiB/s regardless of window size.

For small windows (10s), EDF is actually fastest because it has zero metadata
overhead — it just seeks to a byte offset and reads. Parquet's row-group
statistics add per-read overhead that dominates at small sizes.

## Key observations

1. **HDF5 columnar is competitive with or faster than Parquet for local reads.**
   With fair index computation (no samplestamp scan), HDF5's chunked LZ4 layout
   delivers higher throughput than Parquet for sequential reads and small channel
   subsets.

2. **Parquet's advantages are elsewhere.** Parquet's strengths — row-group
   statistics, predicate pushdown, native SQL engine support (DuckDB, Spark,
   Athena, BigQuery), and cloud-native byte-range queries — matter most for
   remote access, ad-hoc queries, and integration with data platforms. These
   are not captured in local read benchmarks.

3. **EDF is not slow for simple reads.** On a single contiguous file, EDF's
   raw byte-offset seeks are fast. EDF's real limitations are 16-bit precision,
   no compression, no columnar access, and no remote query support.

4. **The best architecture uses both formats.** HDF5 for local waveform storage
   and metadata (fast reads, self-describing, hierarchical). Parquet for cloud
   storage, cross-platform queries, and data pipeline integration.

