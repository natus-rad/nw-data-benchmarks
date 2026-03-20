# EEG/PSG Format Benchmarks — Parquet, HDF5, and EDF

**Study:** 46-channel EEG, 256 Hz, ~12.9 hours (11.85M samples)
**PyArrow:** 23.0.1 (compiled C++ dataset scanner with row-group predicate pushdown)

This benchmark suite evaluates **Parquet**, **HDF5**, and **EDF** for storing
and reading clinical EEG/PSG waveform data. All formats store the same float32
waveform data. The goal is to understand how these formats can be used together
in a **hybrid architecture**: Parquet for immutable signal data (optimized for
compression and cloud access), and HDF5 for metadata, annotations, and study
information (optimized for hierarchical organization and local access).

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
*(Single run — not median-of-3 — due to long runtime.)*

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
*(Single run — not median-of-3 — due to long runtime.)*

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

## F — Compression codec comparison (Parquet)

Read 1-minute window with different Parquet compression codecs.

All compression ratios are measured against the **raw float32 binary baseline of
2,170 MiB** (46 channels × 11,854,000 samples × 4 bytes + int64 samplestamp column,
stored with no encoding or container overhead).

| Codec | Time | File size | Ratio vs 2,170 MiB raw |
|-------|------|-----------|------------------------|
| none | 0.087s | 797 MiB | 2.72× |
| snappy | **0.049s** | 759 MiB | **2.86×** |
| lz4 | 0.090s | 725 MiB | **2.99×** |
| zstd-3 | 0.097s | 618 MiB | 3.51× |
| zstd-9 | 0.104s | 612 MiB | 3.54× |

**Snappy is the fastest codec** for read performance (0.049s), despite LZ4
being 4.5% smaller (725 vs 759 MiB) and zstd-3 being 19% smaller (618 vs 759 MiB).
The difference in read speed is small (~2× slower for zstd-9), and the better
compression of zstd may be worth it for storage-constrained scenarios.

**File sizes in context** (all relative to 2,170 MiB raw float32 binary baseline):

| Format | Size | Ratio vs raw | Notes |
|--------|------|--------------|-------|
| Raw float32 binary | 2,170 MiB | 1.0× | No encoding, no container |
| HDF5 columnar (LZ4) | 1,343 MiB | 1.62× | LZ4 codec on raw float32 per chunk |
| ERD (native format) | 736 MiB | 2.95× | Proprietary source format |
| EDF (int16) | 1,040 MiB | 2.09×† | †int16 (2 bytes/sample) — data type reduction, not compression |
| Parquet "none" | 797 MiB | 2.72× | Column encoding (dictionary/RLE), no codec |
| Parquet snappy | 759 MiB | 2.86× | Encoding + snappy codec |
| Parquet LZ4 | 725 MiB | 2.99× | Encoding + LZ4 codec |
| Parquet zstd-3 | 618 MiB | 3.51× | Encoding + zstd-3 codec |
| Parquet zstd-9 | 612 MiB | 3.54× | Encoding + zstd-9 codec |

Key insight: Parquet "none" (2.72×) is already smaller than HDF5 LZ4 (1.62×)
because Parquet applies column-level encoding (dictionary, RLE bit-packing)
even without a compression codec. HDF5 applies only a byte-level codec to raw
float32 data. EDF appears competitive at 1,040 MiB but only because it stores
16-bit integers — its compression ratio vs float32 is a data-type reduction,
not a compression achievement.

## G — Precision loss (EDF 16-bit quantization)

EDF stores data as 16-bit signed integers. This benchmark measures the
quantization error when converting float32 waveforms to 16-bit and back.

**Summary:** Worst-case max error across all channels: **0.033 µV**
Average SNR: **94.15 dB**

EDF's 16-bit precision is sufficient for most clinical EEG applications
(typical noise floor is 1-10 µV). However, for research applications requiring
higher precision, float32 (Parquet/HDF5) is recommended.

## H — Int32 storage variants (nanovolt and calibrated)

Parquet supports int32 storage with calibration factors, reducing file size
while maintaining precision. Two variants are tested:

- **int32_nanovolt:** Raw int32 values in nanovolts (1 µV = 1000 nV)
- **int32_calibrated:** Int32 with per-channel calibration factors

### H.1 — File sizes and compression

All compression ratios are measured against the **raw float32 binary baseline of 2,170 MiB**.

| Mode | Codec | File size | vs. float32 zstd-3 | Ratio vs 2,170 MiB raw |
|------|-------|-----------|---------------------|------------------------|
| float32 | zstd-3 | 617 MiB | baseline | 3.51× |
| int32_nanovolt | none | 701 MiB | +14% | 3.09× |
| int32_nanovolt | snappy | 649 MiB | +5% | 3.34× |
| int32_nanovolt | zstd | 582 MiB | **-6%** | **3.73×** |
| int32_calibrated | none | 755 MiB | +22% | 2.87× |
| int32_calibrated | snappy | 703 MiB | +14% | 3.09× |
| int32_calibrated | zstd | 628 MiB | **+2%** | **3.45×** |

**Int32 storage achieves better compression than float32:**
- **int32_nanovolt with zstd:** 582 MiB (**-6% vs. float32 zstd**, **3.73× vs. 2,170 MiB raw**, −73% reduction)
- **int32_calibrated with zstd:** 628 MiB (**+2% vs. float32 zstd**, **3.45× vs. 2,170 MiB raw**, −71% reduction)

Both int32 variants compress better than the raw float32 baseline (2,170 MiB),
making them attractive for storage-constrained scenarios while maintaining excellent precision.

### H.2 — Read performance and precision

| Mode | Read method | Codec | Time | Precision (SNR) |
|------|-------------|-------|------|-----------------|
| float32 | numpy | zstd-3 | 0.084s | ∞ (exact) |
| int32_nanovolt | numpy | zstd | 0.098s | 144.4 dB |
| int32_nanovolt | arrow | zstd | 0.078s | 159.5 dB |
| int32_calibrated | numpy | zstd | 0.072s | 159.8 dB |
| int32_calibrated | arrow | zstd | 0.094s | 197.2 dB |

*Note: SNR values are rounded to one decimal place from measured values
(e.g., 144.36 → 144.4 dB, 159.54 → 159.5 dB).*

**Arrow read method is faster** for int32_nanovolt (0.078s vs. 0.098s),
while numpy is faster for int32_calibrated. Both int32 variants maintain
excellent precision (>144 dB SNR), making them suitable for clinical use.

## I — Remote query (DuckDB remote Parquet vs EDF download)

Simulates querying 10 random 10-minute windows from Azure Blob Storage.
DuckDB reads Parquet remotely using byte-range requests (only fetching
relevant row groups), while EDF requires downloading the full file first.

| Method | Channels | Total time | Avg/window | Throughput |
|--------|----------|-----------|------------|------------|
| DuckDB float32 snappy | 46 (all) | 25.3s | 2.53s | 10.7 MiB/s |
| DuckDB float32 snappy | 19 (10-20) | 17.6s | 1.76s | 6.3 MiB/s |
| DuckDB int32 nV snappy | 46 (all) | 35.1s | 3.51s | 7.7 MiB/s |
| DuckDB int32 nV snappy | 19 (10-20) | 5.0s | 0.50s | 22.2 MiB/s |
| EDF download + read | 46 (all) | 7.1s* | 0.71s | — |
| EDF download + read | 19 (10-20) | 2.8s* | 0.28s | — |

\* EDF times shown are local read only (download from Azure was not
measured in this run). In a real remote scenario, the full EDF file
(1,040 MiB) must be downloaded first, adding significant latency.

**Key findings:**
- DuckDB remote Parquet can query specific windows without downloading the
  full file, making it ideal for cloud-based workflows where only a subset
  of data is needed.
- The int32 nanovolt format with 19 channels is fastest (0.50s/window)
  because smaller int32 values compress better and fewer columns are
  transferred.
- EDF local reads are fast once the file is cached, but the upfront
  download cost (1 GB+) makes it impractical for ad-hoc remote queries.

*Note: Remote query performance is highly dependent on network conditions,
Azure region proximity, and caching. These results are from a single run
and should be considered indicative rather than definitive.*

## J — Tuned format comparison (varying block sizes and codecs)

Benchmarks A–E above use the default Parquet files (8 partition files,
76,800 rows/row-group, snappy) and HDF5 (LZ4, 76,800 samples/chunk).

This section tests both formats at six block sizes (5m through 120m),
comparing **Parquet with snappy vs. LZ4 compression** against **HDF5 with LZ4**.
Both formats are written as a single consolidated file with the same float32
data and precision.

**Formats tested:**
- **Parquet snappy:** single `.parquet` file, snappy compression, `write_statistics=True`
- **Parquet LZ4:** single `.parquet` file, LZ4 compression, `write_statistics=True` (NEW)
- **HDF5 columnar:** one dataset per channel, LZ4 compression, chunk index

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

> **Note:** This advantage is entirely structural, not codec-dependent. Section F shows
> that Parquet with **no codec at all** (encoding only, no snappy/lz4/zstd) produces a
> 797 MiB file — still **40% smaller** than HDF5 LZ4 (1,343 MiB). Adding snappy on top
> shrinks it a further 5% to 759 MiB and actually **reads faster** (0.049s vs 0.087s)
> because the smaller file means less I/O, and snappy decompression is cheap enough to
> be invisible on NVMe storage. The compression gap vs HDF5 comes from Parquet's
> column-level encoding, not from the choice of codec.

### J.1 — Random access (1 min at 50%, all 46 channels)

| Block size | Parquet (snappy) | Parquet (LZ4) | HDF5 (LZ4) | Fastest |
|------------|-----------------|---------------|-------------|---------|
| 5m | 0.071s (38 MiB/s) | 0.074s (36 MiB/s) | **0.039s (69 MiB/s)** | H5 1.8× |
| 10m | 0.082s (33 MiB/s) | 0.087s (31 MiB/s) | 0.112s (24 MiB/s) | PQ 1.4× |
| 20m | 0.084s (32 MiB/s) | 0.105s (26 MiB/s) | 0.260s (10 MiB/s) | PQ 3.1× |
| 30m | 0.097s (28 MiB/s) | 0.106s (25 MiB/s) | 0.293s (9 MiB/s) | PQ 3.0× |
| 60m | 0.116s (23 MiB/s) | 0.112s (24 MiB/s) | 0.594s (4.5 MiB/s) | PQ 5.3× |
| 120m | 0.247s (11 MiB/s) | 0.222s (12 MiB/s) | 1.097s (2.4 MiB/s) | PQ 4.9× |

**Key findings:**
- **HDF5 wins at 5m blocks** (1.8× faster) due to cheaper chunk index lookup
- **Parquet LZ4 is competitive with snappy** at small blocks, but slightly slower
- **At larger blocks (10m+), Parquet dominates** — LZ4 and snappy are similar
- **HDF5 degrades dramatically** with larger blocks (5.3× slower at 60m)

The reason: HDF5 must decompress entire chunks for all 46 channels, while
Parquet decompresses per-column within row groups. LZ4's better compression
ratio doesn't overcome the per-chunk decompression overhead in HDF5.

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
*(Single run — not median-of-3 — due to long runtime.)*

| Block size | Parquet (snappy) | Parquet (LZ4) | HDF5 (LZ4) | Fastest |
|------------|-----------------|---------------|-------------|---------|
| 5m | **11.7s (177 MiB/s)** | 12.6s (165 MiB/s) | 12.3s (169 MiB/s) | PQ 1.1× |
| 10m | 14.3s (145 MiB/s) | **13.5s (155 MiB/s)** | 17.1s (122 MiB/s) | PQ 1.3× |
| 20m | 16.1s (130 MiB/s) | **13.4s (155 MiB/s)** | 28.4s (73 MiB/s) | PQ 2.1× |
| 30m | 17.7s (118 MiB/s) | **14.3s (145 MiB/s)** | 38.1s (55 MiB/s) | PQ 2.7× |
| 60m | 23.9s (87 MiB/s) | **19.1s (109 MiB/s)** | 71.2s (29 MiB/s) | PQ 3.7× |
| 120m | **34.8s (60 MiB/s)** | 36.9s (56 MiB/s) | 147.0s (14 MiB/s) | PQ 4.2× |

The full-study read uses 300s query windows. At 5m blocks (≈ 300s), all
formats are comparable — Parquet snappy is slightly fastest. As block size
grows, HDF5 degrades significantly because each 300s query must decompress
a much larger chunk. Parquet handles this better because it decompresses
per-column within the row group. Parquet LZ4 outperforms snappy at larger
block sizes (10m+) due to LZ4's better compression reducing I/O.

### Takeaway

**Block size is the dominant factor.** Both formats perform best when the
block size is close to the typical query window size. For clinical EEG
review (10s–5m windows), 5-minute blocks are optimal.

At 5m blocks, HDF5 LZ4 is faster for random access (1.8× in J.1) and
channel subsets (8.7× in J.2). For full-study sequential reads (J.4),
Parquet and HDF5 are comparable at 5m blocks. At 10m+ blocks, Parquet's
per-column decompression gives it an advantage — it degrades more
gracefully as block size increases.

Parquet compresses 1.8× better than HDF5 LZ4 regardless of block size,
which matters for storage costs at scale.

## Key observations

1. **Codec choice has a modest effect on Parquet file size.** In Section F,
   LZ4 compresses ~4.5% better than snappy (725 MiB vs 759 MiB), while zstd-3
   compresses ~19% better (617 MiB). In practice, LZ4 and snappy deliver
   similar read performance across all block sizes (Section J). The choice
   between them is unlikely to be a deciding factor for most workloads.

2. **HDF5 excels at small blocks with selective column reads.** At 5m blocks,
   HDF5 is 1.8× faster for random access and 8.7× faster for 4-channel reads.
   However, HDF5 degrades rapidly with larger blocks — at 120m blocks, Parquet
   is 4.9× faster. This is because HDF5 must decompress entire chunks even when
   reading a subset of channels.

3. **Parquet's per-column decompression scales better.** Parquet decompresses
   only the requested columns within a row group, making it more efficient for
   larger blocks and full-study sequential reads.

4. **Block size should match the access pattern.** 5-minute blocks are optimal
   for clinical review (10s–5min windows). Larger blocks (30m+) are better for
   batch processing and cloud analytics, but hurt random access performance.

5. **A hybrid architecture is optimal:**
   - **Parquet for immutable signal data:** Better compression (1.8× vs HDF5),
     cloud-native (byte-range queries, SQL engines), and scales to large blocks.
   - **HDF5 for metadata, annotations, and study information:** Hierarchical
     organization, self-describing, efficient for structured data. Can include
     video sync metadata and chunk indices for fast seeks.
   - **EDF for legacy compatibility:** Fast for simple sequential reads on
     single files, but limited to 16-bit precision and no compression.

6. **For production systems:** Store waveforms in Parquet (optimized for
   compression and cloud access), and metadata/annotations in HDF5 (optimized
   for hierarchical organization). Video can be stored separately with sync
   metadata in HDF5 for coordinated playback.

