# Benchmark Report

_Generated from `C:/dev/nw/perforce/NW10_GMA4/Source/sdk/nw-data-benchmarks/benchmark/results/2026-03-23T11-28-36_benchmark_results.json` (run `2026-03-23T11-28-36`)._

This report is automatically generated from benchmark result JSON and is intended to replace manual Markdown edits.

## Run Overview

The report is generated directly from benchmark result JSON. Sections for categories not present in the input file are called out explicitly, so partial benchmark runs still produce a readable report. All reported throughput values use the theoretical decoded float32 payload size: rows × channels × 4 bytes.

| Property | Value |
| --- | --- |
| Study | suppression_study |
| Channels | 46 |
| Sample rate | 256.0 Hz |
| Duration | 12.86 h (11,854,000 samples) |
| System | Windows 11 / Python 3.12.9 / 12 CPU threads / 31.7 GiB RAM |
| Categories present | channel_subset, compression, filter_pipeline_full, int32_storage, precision_loss, random_access, remontage, remote_query, remote_query_full_study, sliding_fft_full, tuned_channel_subset, tuned_full_study, tuned_random_access, tuned_window_scaling, window_scaling |

## Executive Summary

Benchmark rows: **232** across **15** categories.

| Area | Winner | Result |
| --- | --- | --- |
| Random access (median 1-minute read) | pq_30m_lz4 | 0.0813s |
| 4-channel subset | pq_30m_lz4 | 0.0546s |
| Full-study filter pipeline | pq_30m_lz4 | 20.45s |
| Peak window-scaling throughput | pq_30m_lz4 @ 3600s | 308.1 MiB/s |
| Best tuned full-study read | Parquet snappy (30m) | 6.247s |

## Key Observations

- **Random access:** pq_30m_lz4 has the lowest median 1-minute read time at 0.0813s, about 1.37× faster than canonical.
- **Compression trade-off:** smallest Parquet artifact is zstd_9 at 615.6 MiB, while the fastest 1-minute read is snappy at 0.1384s.
- **Int32 variants:** the most compact measured variant is int32_nanovolt (zstd) at 579.2 MiB; its reported SNR vs float32 is 144.36 dB.
- **Remote access:** the fastest remote query path in this run is duckdb_remote for 10-20 (19ch) at 1.255s total over 2 windows.

## A. Random Access

**What it tests:** Repeated all-channel reads of the same 60-second window from different positions in the study.

**What varies:** Read position (`0%`, `50%`, `75%`, `95%`).

**What stays fixed:** Window size, channel count, and baseline format/layout readers.

**Question answered:** How sensitive is each format to where in the study a random read occurs?

pq_30m_lz4 has the lowest median 1-minute read time across read positions at 0.0813s.

| Position | pq_30m_lz4 | canonical |
| --- | --- | --- |
| 0% | 0.0835s / 32.3 MiB/s | 0.0954s / 28.3 MiB/s |
| 50% | 0.0792s / 34.0 MiB/s | 0.1327s / 20.3 MiB/s |
| 75% | 0.0786s / 34.3 MiB/s | 0.1269s / 21.2 MiB/s |
| 95% | 0.0888s / 30.4 MiB/s | 0.0928s / 29.0 MiB/s |

## B. Channel Subset

**What it tests:** Reads of the same 60-second window while requesting fewer channels.

**What varies:** Number of requested channels.

**What stays fixed:** Read position, window size, and baseline format/layout readers.

**Question answered:** Which formats benefit most when the workload only needs a subset of channels?

4 channels → pq_30m_lz4 is fastest at 0.0546s. 10 channels → pq_30m_lz4 is fastest at 0.0523s. 46 channels → pq_30m_lz4 is fastest at 0.1104s.

| Channels | pq_30m_lz4 |
| --- | --- |
| 4 | 0.0546s / 4.3 MiB/s |
| 10 | 0.0523s / 11.2 MiB/s |
| 46 | 0.1104s / 24.4 MiB/s |

## C. Re-montage

**What it tests:** A read followed immediately by bipolar montage computation.

**What varies:** Storage format/layout.

**What stays fixed:** Window size, channel set, and the montage operation itself.

**Question answered:** Once downstream signal processing is included, how much of total time is storage I/O versus lightweight computation?

Montage is a relatively small fraction of end-to-end time in this benchmark (average 1.5% of total wall time).

| Format | Read | Montage | Total | Montage share |
| --- | --- | --- | --- | --- |
| pq_30m_lz4 | 0.1537s | 0.0024s | 0.1561s | 1.5% |

## D.1 Full-Study Filter Pipeline

**What it tests:** End-to-end full-study read, montage, and digital filtering.

**What varies:** Storage format/layout.

**What stays fixed:** Entire study duration, channel set, filter pipeline, and processing order.

**Question answered:** Which format is best for whole-study offline processing workloads that must read and transform all signal data?

For the full-study read → montage → filter pipeline, pq_30m_lz4 is fastest at 20.45s.

| Format | Read | Montage | Filter | Total | Throughput |
| --- | --- | --- | --- | --- | --- |
| pq_30m_lz4 | 15.05s | 0.8150s | 4.583s | 20.45s | 94.9 MiB/s |

## D.2 Sliding FFT

**What it tests:** The same full-study read/filter pipeline plus overlapping FFT window computation.

**What varies:** Storage format/layout.

**What stays fixed:** Study duration, preprocessing pipeline, FFT window/stride, and channel set.

**Question answered:** How much does storage choice matter once a heavier downstream spectral-analysis workload is layered on top?

This stage computed 21,596 overlapping FFT windows across the full study.

| Format | Read | Montage | Filter | FFT | Total |
| --- | --- | --- | --- | --- | --- |
| pq_30m_lz4 | 15.40s | 0.9460s | 5.163s | 12.24s | 34.30s |

## E. Window Scaling

**What it tests:** All-channel reads from the middle of the study while increasing requested window size.

**What varies:** Window size.

**What stays fixed:** Read position, channel count, and baseline format/layout readers.

**Question answered:** How does each format transition from small random reads to large sustained reads?

Best measured throughput is 308.1 MiB/s from pq_30m_lz4 at a 3600s window.

| Window | pq_30m_lz4 |
| --- | --- |
| 10s | 4.8 MiB/s |
| 30s | 12.8 MiB/s |
| 60s | 23.5 MiB/s |
| 300s | 55.5 MiB/s |
| 900s | 123.3 MiB/s |
| 1800s | 225.6 MiB/s |
| 3600s | 308.1 MiB/s |

## F. Compression

**What it tests:** Parquet codec tradeoffs between read speed and resulting artifact size.

**What varies:** Codec (`none`, `snappy`, `zstd`, `lz4`).

**What stays fixed:** Signal data, window size, and Parquet layout.

**Question answered:** Which Parquet codec gives the best balance between storage efficiency and read performance?

Against a raw float32 baseline of 2,170.5 MiB, the smallest Parquet artifact is zstd_9 at 615.6 MiB. The fastest 1-minute read is snappy at 0.1384s.

| Codec | 1-minute read | Artifact size | Ratio vs raw float32 |
| --- | --- | --- | --- |
| zstd_9 | 0.1739s | 615.6 MiB | 3.53× |
| zstd_3 | 0.1634s | 618.0 MiB | 3.51× |
| lz4 | 0.1887s | 714.4 MiB | 3.04× |
| snappy | 0.1384s | 755.1 MiB | 2.87× |
| none | 0.1639s | 787.2 MiB | 2.76× |

## G. Precision Loss

**What it tests:** Float32 → EDF 16-bit → float32 round-trip quantization error.

**What varies:** Channel signal statistics.

**What stays fixed:** Window size and round-trip conversion procedure.

**Question answered:** What numeric precision is lost when using EDF-style 16-bit storage instead of float32?

EDF round-trip quantization for a 60s window produced worst-case max absolute error 0.03268992 µV with average SNR 94.15 dB across 46 channels.

Top channels by max absolute error:

| Channel | Max abs error (µV) | RMS error (µV) | SNR (dB) |
| --- | --- | --- | --- |
| DC2 | 0.03268992 | 0.01795972 | 105.32 |
| DC3 | 0.03017531 | 0.01755604 | 105.58 |
| DC1 | 0.02766070 | 0.01632583 | 105.99 |
| DC4 | 0.02263148 | 0.01337124 | 104.11 |
| X2 | 0.00483166 | 0.00278872 | 88.79 |

## H. Int32 Storage

**What it tests:** Alternative int32-based storage encodings versus float32 for size, precision, and read speed.

**What varies:** Encoding mode, codec, and read path.

**What stays fixed:** Signal data and comparison baseline.

**Question answered:** Can int32 encodings reduce storage cost while preserving acceptable fidelity and performance?

The most compact measured storage mode is int32_nanovolt (zstd) at 579.2 MiB.

### H.1 Size / Precision

| Mode | Codec | Artifact size | Ratio vs raw float32 | SNR vs float32 |
| --- | --- | --- | --- | --- |
| int32_nanovolt | zstd | 579.2 MiB | 3.75× | 144.36 |
| float32 | zstd_3 | 618.0 MiB | 3.51× | inf |
| int32_calibrated | zstd | 623.4 MiB | 3.48× | 159.83 |
| int32_nanovolt | snappy | 643.1 MiB | 3.38× | 144.36 |
| int32_nanovolt | none | 692.3 MiB | 3.14× | 144.36 |
| int32_calibrated | snappy | 694.8 MiB | 3.12× | 159.83 |
| int32_calibrated | none | 744.0 MiB | 2.92× | 159.83 |

### H.2 Representative Read Performance

| Mode | Read method | Codec | 1-minute read | Throughput |
| --- | --- | --- | --- | --- |
| float32 | — | zstd_3 | 0.1077s | 25.0 MiB/s |
| int32_nanovolt | arrow | zstd | 0.1198s | 22.5 MiB/s |
| int32_calibrated | arrow | zstd | 0.1663s | 16.2 MiB/s |

## I. Remote Query

**What it tests:** Remote-access workflows for retrieving windows over the network.

**What varies:** Access method and artifact format.

**What stays fixed:** Window count, window duration, and requested channel subsets.

**Question answered:** For remote/cloud access, when is query-in-place better than full-file download first?

All reported remote timings are direct measurements.

| Method | Format | Channel subset | Total time | Avg/window | Throughput |
| --- | --- | --- | --- | --- | --- |
| duckdb_remote | parquet_single_file_lz4 | 10-20 (19ch) | 1.255s | 0.6280s | 17.7 MiB/s |
| duckdb_remote | Parquet int32 nV snappy | 10-20 (19ch) | 3.548s | 1.774s | 6.3 MiB/s |
| duckdb_remote | Parquet float32 snappy | 10-20 (19ch) | 4.209s | 2.104s | 5.3 MiB/s |
| duckdb_remote | parquet_single_file_lz4 | all | 6.660s | 3.330s | 8.1 MiB/s |
| duckdb_remote | Parquet float32 snappy | all | 7.053s | 3.527s | 7.6 MiB/s |
| duckdb_remote | Parquet int32 nV snappy | all | 14.62s | 7.308s | 3.7 MiB/s |

## J. Tuned Format Comparison

This group compares matched block-size variants generated specifically for Benchmark J.

### J.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** Which tuned layout is best for small random-access reads?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 0.2127s | 0.1281s | 0.0759s |
| 10m | 0.0939s | 0.0722s | 0.0860s |
| 20m | 0.0691s | 0.0934s | 0.1511s |
| 30m | 0.0531s | 0.0530s | 0.2447s |
| 60m | 0.1054s | 0.1106s | 0.4669s |
| 120m | 0.2209s | 0.2298s | 0.9347s |

### J.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** Which tuned layout best supports selective channel retrieval?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 0.0544s | 0.0451s | 0.0056s |
| 10m | 0.0310s | 0.0325s | 0.0096s |
| 20m | 0.0347s | 0.0273s | 0.0189s |
| 30m | 0.0302s | 0.0252s | 0.0443s |
| 60m | 0.0470s | 0.0357s | 0.0468s |
| 120m | 0.0552s | 0.0452s | 0.1068s |

### J.3 Throughput vs Window Size

**What it tests:** How read throughput for each format scales as the requested window grows, from small random reads (10s) to large sequential reads (3600s).

**What varies:** Requested window size and tuned storage format.

**What stays fixed:** Channel count, read position (mid-study), and comparison method. The best-performing block size is selected per cell.

**Question answered:** Which tuned format scales best as the read window grows?

Each cell shows the best throughput across all blocks tested for that window × format combination, with the winning block in parentheses.

| Window | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 10s | 10.1 MiB/s (20m block) | 10.6 MiB/s (10m block) | 10.1 MiB/s (5m block) |
| 30s | 29.3 MiB/s (10m block) | 29.2 MiB/s (10m block) | 37.4 MiB/s (5m block) |
| 60s | 53.6 MiB/s (20m block) | 56.8 MiB/s (20m block) | 69.1 MiB/s (5m block) |
| 300s | 201.2 MiB/s (20m block) | 189.9 MiB/s (20m block) | 156.8 MiB/s (5m block) |
| 900s | 285.1 MiB/s (10m block) | 261.7 MiB/s (30m block) | 175.9 MiB/s (5m block) |
| 1800s | 383.3 MiB/s (20m block) | 353.2 MiB/s (10m block) | 273.5 MiB/s (5m block) |
| 3600s | 348.0 MiB/s (10m block) | 379.3 MiB/s (10m block) | 282.1 MiB/s (10m block) |

### J.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the tuned variants.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** Which tuned layout is best for whole-study scans rather than isolated random reads?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 9.239s | 11.80s | 7.964s |
| 10m | 6.811s | 6.502s | 7.868s |
| 20m | 6.323s | 7.791s | 9.141s |
| 30m | 6.247s | 6.285s | 9.998s |
| 60m | 6.317s | 6.330s | 9.541s |
| 120m | 8.077s | 7.236s | 13.89s |

Per-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons.

## K. Baseline Format Comparison

This group runs the Benchmark J workload suite on the resolved baseline input artifact(s) only, without generating tuned comparison variants.

Reported throughput values use the theoretical decoded float32 payload size: rows × channels × 4 bytes.

### K.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study on the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** How does the baseline input format perform on the same random-access workload used in Benchmark J?

*This category was not present in the input results file.*

### K.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels on the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** How well does the baseline input format support selective channel retrieval on the J-style workload?

*This category was not present in the input results file.*

### K.3 Throughput vs Window Size

**What it tests:** How read throughput for the baseline input artifact scales as the requested window grows, from small random reads (10s) to large sequential reads (3600s).

**What varies:** Requested window size.

**What stays fixed:** Channel count, read position (mid-study), and the use of the baseline input artifact only.

**Question answered:** How does the baseline input format scale on the same workload family used for Benchmark J?

*This category was not present in the input results file.*

### K.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** How does the baseline input artifact perform on whole-study scans compared with the tuned variants in Benchmark J?

*This category was not present in the input results file.*
