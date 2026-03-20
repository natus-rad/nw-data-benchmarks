# EDF vs HDF5 vs Parquet — Benchmark Results

**Study:** 46-channel EEG, 256 Hz, ~12.9 hours (11.85M samples)
**PyArrow:** 23.0.1 (compiled C++ dataset scanner with row-group predicate pushdown)

All formats store the same float32 waveform data. EDF and HDF5 are derived
from the source Parquet files. HDF5 uses LZ4 compression with chunk sizes
matching Parquet row groups (76,800 samples = 300s at 256 Hz).

### A note on fairness: HDF5 chunk index

**Parquet includes row-group statistics (min/max per column per row group)
as part of the format specification.** Every Parquet file has these for free.
Readers like pyarrow use them automatically to skip irrelevant row groups
during filtered reads — no extra work required by the user.

**HDF5 has no equivalent built-in index.** Standard HDF5 files store chunked
data but provide no mechanism to skip chunks based on data values. To give
HDF5 a fair shot in these benchmarks, we build a custom `chunk_index`
dataset at conversion time — a small table of
`(chunk_start_idx, min_stamp, max_stamp)` per chunk. This lets our reader
skip to the right chunks without scanning the entire samplestamp dataset.

**This means the HDF5 results below represent best-case performance** with
a purpose-built index that does not exist in standard HDF5 files. Without
the chunk index, HDF5 random access would require reading the full
samplestamp dataset on every call (~90 MB for this study), making it
significantly slower. Parquet's results, by contrast, use only the
format's built-in capabilities.

## Summary

| Benchmark | Parquet | HDF5 columnar | HDF5 rowgroup | EDF |
|-----------|---------|---------------|---------------|-----|
| Random access (1 min) | 0.050s | 0.043s* | 0.043s* | 0.063s |
| 4-channel subset (1 min) | 0.032s | 0.006s* | 0.030s* | 0.006s |
| Full pipeline, 12h | 16.4s | 11.6s* | 14.5s* | 86.6s |
| Peak throughput (60 min) | 288 MiB/s | 330 MiB/s* | 157 MiB/s* | 38 MiB/s |

\* HDF5 results use a custom chunk index built at conversion time. See fairness note above.

## A — Random access

Read a 1-minute, 46-channel window from four positions.

| Position | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 0% | 0.044s (62 MiB/s) | 0.043s (62 MiB/s) | 0.046s (59 MiB/s) | 0.061s (44 MiB/s) |
| 50% | 0.056s (48 MiB/s) | 0.058s (46 MiB/s) | 0.043s (63 MiB/s) | 0.059s (46 MiB/s) |
| 75% | 0.050s (54 MiB/s) | 0.042s (65 MiB/s) | 0.034s (79 MiB/s) | 0.059s (46 MiB/s) |
| 95% | 0.064s (42 MiB/s) | 0.039s (69 MiB/s) | 0.060s (45 MiB/s) | 0.090s (30 MiB/s) |

Parquet and HDF5 are comparable for single-window reads. HDF5 columnar
has a slight edge at non-zero positions (note: using custom chunk index).

## B — Channel subset

Read a 1-minute window with 4, 10, or all 46 channels.

| Channels | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 4 | 0.032s (7 MiB/s) | 0.006s (40 MiB/s) | 0.030s (8 MiB/s) | 0.006s (43 MiB/s) |
| 10 | 0.034s (17 MiB/s) | 0.011s (53 MiB/s) | 0.032s (18 MiB/s) | 0.015s (40 MiB/s) |
| 46 (all) | 0.055s (49 MiB/s) | 0.038s (71 MiB/s) | 0.034s (79 MiB/s) | 0.058s (47 MiB/s) |

HDF5 columnar (with custom chunk index) excels at small channel subsets —
reading 4 channels takes 6 ms because each channel is a separate dataset
with independent chunks. EDF is similarly fast for small subsets because
pyedflib reads individual signal records. Parquet's per-read overhead
(footer parsing, row-group stats) is proportionally larger for small reads.

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
| 10s | 7.1 | 10.5 | 10.7 | 36.2 |
| 30s | 27.3 | 31.1 | 23.1 | 37.2 |
| 1 min | 56.2 | 53.3 | 67.0 | 39.2 |
| 5 min | 163.3 | 170.2 | 149.3 | 39.5 |
| 15 min | 227.3 | 240.2 | 156.3 | 38.5 |
| 30 min | 262.7 | 280.1 | 162.3 | 37.6 |
| 60 min | **288.3** | **329.6** | 156.7 | 38.5 |

HDF5 columnar (with custom chunk index) achieves the highest throughput for
large reads (330 MiB/s at 60 min). Parquet peaks at 288 MiB/s using only
built-in row-group statistics. EDF throughput is flat at ~38 MiB/s.

For small windows (≤30s), EDF is fastest because it has zero metadata
overhead — it just seeks to a byte offset and reads. Both Parquet and HDF5
have per-read overhead (index lookup, file open) that dominates at small sizes.

## J — Tuned format comparison (varying block sizes)

Benchmarks A–E above use the default Parquet files (8 partition files,
76,800 rows/row-group, snappy) and HDF5 (LZ4, 76,800 samples/chunk).
The compression codecs differ: Parquet uses snappy, HDF5 uses LZ4.

This section tests both formats at six block sizes (5m through 120m),
each using its fastest decompression codec. Both formats are written as
a single consolidated file. Same float32 data, same precision.

- **Parquet:** single `.parquet` file, snappy compression, `write_statistics=True`
- **HDF5:** columnar layout (one dataset per channel), LZ4 compression, chunk index

### File sizes

| Block size | Parquet (snappy) | HDF5 columnar (LZ4) | PQ row groups | H5 chunks |
|------------|-----------------|---------------------|---------------|-----------|
| 5m (76,800 samples) | 759 MiB | 1,343 MiB | 155 | 155 |
| 10m (153,600) | 750 MiB | 1,339 MiB | 78 | 78 |
| 20m (307,200) | 729 MiB | 1,337 MiB | 39 | 39 |
| 30m (460,800) | 724 MiB | 1,337 MiB | 26 | 26 |
| 60m (921,600) | 722 MiB | 1,336 MiB | 13 | 13 |
| 120m (1,843,200) | 728 MiB | 1,338 MiB | 7 | 7 |

Parquet compresses ~1.8× better than HDF5 LZ4 at every block size. Parquet's
per-column encoding (dictionary, delta, RLE) is more effective than HDF5's
per-chunk LZ4 on structured EEG data.

### J.1 — Random access (1 min at 50%, all 46 channels)

| Block size | Parquet (snappy) | HDF5 (LZ4) | Faster |
|------------|-----------------|-------------|--------|
| 5m | 0.081s (33 MiB/s) | **0.039s (69 MiB/s)** | H5 2.1× |
| 10m | 0.067s (41 MiB/s) | 0.088s (31 MiB/s) | PQ 1.3× |
| 20m | 0.113s (24 MiB/s) | 0.174s (16 MiB/s) | PQ 1.5× |
| 30m | 0.099s (27 MiB/s) | 0.203s (13 MiB/s) | PQ 2.0× |
| 60m | 0.177s (15 MiB/s) | 0.401s (6.7 MiB/s) | PQ 2.3× |
| 120m | 0.323s (8.4 MiB/s) | 1.252s (2.2 MiB/s) | PQ 3.9× |

HDF5 wins at 5m blocks because its chunk index lookup is cheaper than
Parquet's row-group footer parsing. But as block size grows, HDF5 degrades
faster — it must decompress the entire chunk for all 46 channels, while
Parquet decompresses per-column within a row group. At 120m blocks, Parquet
is 3.9× faster.

### J.2 — Channel subset (4 channels, 1 min at 50%)

| Block size | Parquet (snappy) | HDF5 (LZ4) | Faster |
|------------|-----------------|-------------|--------|
| 5m | 0.114s (2.1 MiB/s) | **0.013s (18 MiB/s)** | H5 8.7× |
| 10m | 0.078s (3.0 MiB/s) | **0.025s (9.4 MiB/s)** | H5 3.1× |
| 20m | 0.060s (3.9 MiB/s) | 0.038s (6.1 MiB/s) | H5 1.6× |
| 30m | 0.055s (4.3 MiB/s) | 0.048s (4.9 MiB/s) | H5 1.2× |
| 60m | 0.085s (2.8 MiB/s) | 0.102s (2.3 MiB/s) | PQ 1.2× |
| 120m | 0.104s (2.3 MiB/s) | 0.167s (1.4 MiB/s) | PQ 1.6× |

HDF5 columnar dominates at small block sizes for channel subsets — each
channel is an independent dataset, so reading 4 channels decompresses only
4 small chunks. As chunk size grows, this advantage shrinks. Parquet's
per-column-within-row-group reads are more stable across block sizes.

### J.3 — Window scaling (throughput in MiB/s, all 46 channels)

| Window | PQ 5m | H5 5m | PQ 10m | H5 10m | PQ 20m | H5 20m | PQ 30m | H5 30m | PQ 60m | H5 60m | PQ 120m | H5 120m |
|--------|-------|-------|--------|--------|--------|--------|--------|--------|--------|--------|---------|---------|
| 10s | 4.1 | **6.7** | 6.9 | 4.8 | 5.7 | 2.5 | 3.6 | 1.3 | 2.8 | 1.0 | 1.3 | 0.5 |
| 30s | 17.7 | **34.2** | 23.5 | 17.6 | 13.8 | 10.7 | 15.1 | 7.3 | 7.1 | 2.7 | 3.8 | 1.5 |
| 1m | 34.5 | **57.7** | **40.3** | 27.4 | 27.7 | 18.1 | 26.7 | 14.0 | 12.5 | 5.9 | 7.8 | 3.3 |
| 5m | **132** | **150** | 91 | 85 | **132** | 80 | 64 | 29 | 74 | 32 | 42 | 17 |
| 15m | **181** | **199** | **191** | 155 | 154 | 135 | 157 | 94 | 132 | 92 | 103 | 46 |
| 30m | **225** | **244** | **215** | 218 | **212** | 207 | 206 | 171 | **224** | 167 | 159 | 95 |
| 60m | **256** | **295** | **235** | **281** | **237** | 222 | **269** | 225 | **216** | 161 | 202 | 170 |

At 5m blocks, HDF5 is faster across the board. At 10m+, Parquet pulls
ahead for small windows because it decompresses per-column (only the
requested columns within a row group), while HDF5 must decompress the
full chunk even though each channel is a separate dataset — the chunk
is still large. For large windows (30m+), both formats converge.

### J.4 — Full-study sequential read (12 hours, 300s query windows)

| Block size | Parquet (snappy) | HDF5 (LZ4) | Faster |
|------------|-----------------|-------------|--------|
| 5m | **13.9s (150 MiB/s)** | **11.4s (183 MiB/s)** | H5 1.2× |
| 10m | 16.6s (126 MiB/s) | 18.2s (115 MiB/s) | PQ 1.1× |
| 20m | 23.0s (91 MiB/s) | 35.5s (59 MiB/s) | PQ 1.5× |
| 30m | 23.6s (88 MiB/s) | 40.1s (52 MiB/s) | PQ 1.7× |
| 60m | 35.3s (59 MiB/s) | 70.5s (30 MiB/s) | PQ 2.0× |
| 120m | 65.3s (32 MiB/s) | 134.8s (15 MiB/s) | PQ 2.1× |

The full-study read uses 300s query windows. When the block size matches
(5m ≈ 300s), both formats are efficient. As block size grows, HDF5 degrades
faster because each 300s query must decompress a much larger chunk. Parquet
handles this better because it decompresses per-column within the row group.

### Takeaway

**Block size is the dominant factor.** Both formats perform best when the
block size is close to the typical query window size. For clinical EEG
review (10s–5m windows), 5-minute blocks are optimal.

At 5m blocks, HDF5 LZ4 is 20–50% faster than Parquet snappy for local
reads. At 10m+ blocks, Parquet's per-column decompression gives it an
advantage — it degrades more gracefully as block size increases.

Parquet compresses 1.8× better than HDF5 LZ4 regardless of block size,
which matters for storage costs at scale.

## Key observations

1. **HDF5 columnar is faster than Parquet for local reads** at matched block
   sizes. LZ4 decompression is faster than snappy, and HDF5's per-channel
   datasets give it a structural advantage for channel-subset reads.

2. **Parquet's advantages are elsewhere.** Parquet's strengths — predicate
   pushdown, native SQL engine support (DuckDB, Spark, Athena, BigQuery),
   and cloud-native byte-range queries — matter most for remote access,
   ad-hoc queries, and integration with data platforms. These are not
   captured in local read benchmarks.

3. **EDF is fast for simple local reads.** On a single contiguous file, EDF's
   raw byte-offset seeks are fast. EDF's real limitations are 16-bit precision,
   no compression, no columnar access, and no remote query support.

4. **Block size should match the access pattern.** 300s blocks work well for
   clinical review (10s–5min windows). Larger blocks waste I/O on random access.

5. **The best architecture uses both formats.** HDF5 for local waveform storage
   and metadata (fast reads, self-describing, hierarchical). Parquet for cloud
   storage, cross-platform queries, and data pipeline integration.

