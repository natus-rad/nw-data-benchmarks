# Benchmark Report

_Generated from `C:/dev/nw/perforce/NW10_GMA4/Source/sdk/nw-data-benchmarks/benchmark/results/2026-03-21T12-01-24_benchmark_results.json` (run `2026-03-21T12-01-24`)._

This report is automatically generated from benchmark result JSON and is intended to replace manual Markdown edits.

## Run Overview

The report is generated directly from benchmark result JSON. Sections for categories not present in the input file are called out explicitly, so partial benchmark runs still produce a readable report.

| Property | Value |
| --- | --- |
| Study | suppression_study |
| Channels | 46 |
| Sample rate | 256.0 Hz |
| Duration | 12.86 h (11,854,003 samples) |
| System | Windows 11 / Python 3.12.9 / 12 CPU threads / 31.7 GiB RAM |
| Categories present | channel_subset, compression, filter_pipeline_full, int32_storage, precision_loss, random_access, remontage, remote_query, sliding_fft_full, tuned_channel_subset, tuned_full_study, tuned_random_access, tuned_window_scaling, window_scaling |

## Executive Summary

Benchmark rows: **273** across **14** categories.

| Area | Winner | Result |
| --- | --- | --- |
| Random access (median 1-minute read) | HDF5 columnar | 0.0404s |
| 4-channel subset | HDF5 columnar | 0.0055s |
| Full-study filter pipeline | Parquet | 14.20s |
| Peak window-scaling throughput | Parquet @ 1800s | 378.6 MiB/s |
| Best tuned full-study read | Parquet LZ4 (20m) | 13.89s |

## Key Observations

- **Random access:** HDF5 columnar has the lowest median 1-minute read time at 0.0404s, about 1.07× faster than HDF5 row-group.
- **Compression trade-off:** smallest Parquet artifact is zstd_9 at 618.4 MiB, while the fastest 1-minute read is snappy at 0.0510s.
- **Int32 variants:** the most compact measured variant is int32_nanovolt (zstd) at 581.3 MiB; its reported SNR vs float32 is 144.36 dB.
- **Remote access:** the fastest remote query path in this run is duckdb_remote for 10-20 (19ch) at 4.793s total over 10 windows.

## A. Random Access

**What it tests:** Repeated all-channel reads of the same 60-second window from different positions in the study.

**What varies:** Read position (`0%`, `50%`, `75%`, `95%`).

**What stays fixed:** Window size, channel count, and baseline format/layout readers.

**Question answered:** How sensitive is each format to where in the study a random read occurs?

HDF5 columnar has the lowest median 1-minute read time across read positions at 0.0404s.

| Position | Parquet | HDF5 columnar | HDF5 row-group | EDF |
| --- | --- | --- | --- | --- |
| 0% | 0.0573s / 47.0 MiB/s | 0.0407s / 66.2 MiB/s | 0.0491s / 54.9 MiB/s | 0.0651s / 41.4 MiB/s |
| 50% | 0.0452s / 59.6 MiB/s | 0.0613s / 44.0 MiB/s | 0.0473s / 56.9 MiB/s | 0.0667s / 40.4 MiB/s |
| 75% | 0.0603s / 44.7 MiB/s | 0.0336s / 80.2 MiB/s | 0.0396s / 68.1 MiB/s | 0.0729s / 37.0 MiB/s |
| 95% | 0.0414s / 65.2 MiB/s | 0.0402s / 67.1 MiB/s | 0.0393s / 68.5 MiB/s | 0.0728s / 37.0 MiB/s |

## B. Channel Subset

**What it tests:** Reads of the same 60-second window while requesting fewer channels.

**What varies:** Number of requested channels.

**What stays fixed:** Read position, window size, and baseline format/layout readers.

**Question answered:** Which formats benefit most when the workload only needs a subset of channels?

4 channels → HDF5 columnar is fastest at 0.0055s. 10 channels → HDF5 columnar is fastest at 0.0098s. 46 channels → HDF5 row-group is fastest at 0.0348s.

| Channels | Parquet | HDF5 columnar | HDF5 row-group | EDF |
| --- | --- | --- | --- | --- |
| 4 | 0.0384s / 6.1 MiB/s | 0.0055s / 42.9 MiB/s | 0.0400s / 5.9 MiB/s | 0.0059s / 39.5 MiB/s |
| 10 | 0.0383s / 15.3 MiB/s | 0.0098s / 59.7 MiB/s | 0.0264s / 22.2 MiB/s | 0.0149s / 39.2 MiB/s |
| 46 | 0.0386s / 69.9 MiB/s | 0.0525s / 51.3 MiB/s | 0.0348s / 77.5 MiB/s | 0.0569s / 47.3 MiB/s |

## C. Re-montage

**What it tests:** A read followed immediately by bipolar montage computation.

**What varies:** Storage format/layout.

**What stays fixed:** Window size, channel set, and the montage operation itself.

**Question answered:** Once downstream signal processing is included, how much of total time is storage I/O versus lightweight computation?

Montage is a relatively small fraction of end-to-end time in this benchmark (average 1.7% of total wall time).

| Format | Read | Montage | Total | Montage share |
| --- | --- | --- | --- | --- |
| HDF5 row-group | 0.0295s | 0.0006s | 0.0300s | 1.9% |
| HDF5 columnar | 0.0326s | 0.0005s | 0.0331s | 1.4% |
| EDF | 0.0561s | 0.0008s | 0.0569s | 1.4% |
| Parquet | 0.0591s | 0.0011s | 0.0602s | 1.9% |

## D.1 Full-Study Filter Pipeline

**What it tests:** End-to-end full-study read, montage, and digital filtering.

**What varies:** Storage format/layout.

**What stays fixed:** Entire study duration, channel set, filter pipeline, and processing order.

**Question answered:** Which format is best for whole-study offline processing workloads that must read and transform all signal data?

For the full-study read → montage → filter pipeline, Parquet is fastest at 14.20s.

| Format | Read | Montage | Filter | Total | Throughput |
| --- | --- | --- | --- | --- | --- |
| Parquet | 9.989s | 0.6250s | 3.578s | 14.20s | 136.7 MiB/s |
| HDF5 row-group | 11.98s | 0.7430s | 4.473s | 17.20s | 112.8 MiB/s |
| HDF5 columnar | 12.73s | 0.7500s | 4.360s | 17.85s | 108.7 MiB/s |
| EDF | 87.81s | 0.6600s | 4.011s | 92.48s | 21.0 MiB/s |

## D.2 Sliding FFT

**What it tests:** The same full-study read/filter pipeline plus overlapping FFT window computation.

**What varies:** Storage format/layout.

**What stays fixed:** Study duration, preprocessing pipeline, FFT window/stride, and channel set.

**Question answered:** How much does storage choice matter once a heavier downstream spectral-analysis workload is layered on top?

This stage computed 21,596 overlapping FFT windows across the full study.

| Format | Read | Montage | Filter | FFT | Total |
| --- | --- | --- | --- | --- | --- |
| HDF5 columnar | 6.598s | 0.5710s | 3.365s | 8.618s | 19.50s |
| Parquet | 12.40s | 0.8420s | 4.645s | 11.10s | 29.48s |
| HDF5 row-group | 11.84s | 0.7970s | 4.659s | 12.03s | 29.81s |
| EDF | 49.74s | 0.6830s | 3.900s | 9.790s | 64.51s |

## E. Window Scaling

**What it tests:** All-channel reads from the middle of the study while increasing requested window size.

**What varies:** Window size.

**What stays fixed:** Read position, channel count, and baseline format/layout readers.

**Question answered:** How does each format transition from small random reads to large sustained reads?

Best measured throughput is 378.6 MiB/s from Parquet at a 1800s window.

| Window | Parquet | HDF5 columnar | HDF5 row-group | EDF |
| --- | --- | --- | --- | --- |
| 10s | 8.9 MiB/s | 9.8 MiB/s | 10.5 MiB/s | 35.4 MiB/s |
| 30s | 19.8 MiB/s | 22.2 MiB/s | 27.7 MiB/s | 29.1 MiB/s |
| 60s | 39.5 MiB/s | 40.8 MiB/s | 48.2 MiB/s | 30.3 MiB/s |
| 300s | 119.3 MiB/s | 156.1 MiB/s | 137.8 MiB/s | 37.5 MiB/s |
| 900s | 334.1 MiB/s | 264.2 MiB/s | 167.1 MiB/s | 40.8 MiB/s |
| 1800s | 378.6 MiB/s | 258.5 MiB/s | 158.4 MiB/s | 43.0 MiB/s |
| 3600s | 209.8 MiB/s | 352.7 MiB/s | 159.1 MiB/s | 36.8 MiB/s |

## F. Compression

**What it tests:** Parquet codec tradeoffs between read speed and resulting artifact size.

**What varies:** Codec (`none`, `snappy`, `zstd`, `lz4`).

**What stays fixed:** Signal data, window size, and Parquet layout.

**Question answered:** Which Parquet codec gives the best balance between storage efficiency and read performance?

Against a raw float32 baseline of 2,170.5 MiB, the smallest Parquet artifact is zstd_9 at 618.4 MiB. The fastest 1-minute read is snappy at 0.0510s.

| Codec | 1-minute read | Artifact size | Ratio vs raw float32 |
| --- | --- | --- | --- |
| zstd_9 | 0.0659s | 618.4 MiB | 3.51× |
| zstd_3 | 0.0915s | 620.8 MiB | 3.50× |
| lz4 | 0.0652s | 717.8 MiB | 3.02× |
| snappy | 0.0510s | 759.1 MiB | 2.86× |
| none | 0.0919s | 791.0 MiB | 2.74× |

## G. Precision Loss

**What it tests:** Float32 → EDF 16-bit → float32 round-trip quantization error.

**What varies:** Channel signal statistics.

**What stays fixed:** Window size and round-trip conversion procedure.

**Question answered:** What numeric precision is lost when using EDF-style 16-bit storage instead of float32?

EDF round-trip quantization for a 60s window produced worst-case max absolute error 0.03268992 µV with average SNR 94.15 dB across 46 channels.

Top channels by max absolute error:

| Channel | Max abs error (µV) | RMS error (µV) | SNR (dB) |
| --- | --- | --- | --- |
| DC2 | 0.03268992 | 0.01795936 | 105.32 |
| DC3 | 0.03017531 | 0.01755519 | 105.58 |
| DC1 | 0.02766070 | 0.01632587 | 105.99 |
| DC4 | 0.02263148 | 0.01337235 | 104.11 |
| X2 | 0.00483166 | 0.00278874 | 88.79 |

## H. Int32 Storage

**What it tests:** Alternative int32-based storage encodings versus float32 for size, precision, and read speed.

**What varies:** Encoding mode, codec, and read path.

**What stays fixed:** Signal data and comparison baseline.

**Question answered:** Can int32 encodings reduce storage cost while preserving acceptable fidelity and performance?

The most compact measured storage mode is int32_nanovolt (zstd) at 581.3 MiB.

### H.1 Size / Precision

| Mode | Codec | Artifact size | Ratio vs raw float32 | SNR vs float32 |
| --- | --- | --- | --- | --- |
| int32_nanovolt | zstd | 581.3 MiB | 3.73× | 144.36 |
| float32 | zstd_3 | 620.8 MiB | 3.50× | inf |
| int32_calibrated | zstd | 627.1 MiB | 3.46× | 159.83 |
| int32_nanovolt | snappy | 645.9 MiB | 3.36× | 144.36 |
| int32_nanovolt | none | 695.3 MiB | 3.12× | 144.36 |
| int32_calibrated | snappy | 699.3 MiB | 3.10× | 159.83 |
| int32_calibrated | none | 748.7 MiB | 2.90× | 159.83 |

### H.2 Representative Read Performance

| Mode | Read method | Codec | 1-minute read | Throughput |
| --- | --- | --- | --- | --- |
| int32_calibrated | numpy | zstd | 0.0554s | 48.6 MiB/s |
| float32 | — | zstd_3 | 0.0556s | 48.5 MiB/s |
| int32_nanovolt | arrow | zstd | 0.0822s | 32.8 MiB/s |

## I. Remote Query

**What it tests:** Remote-access workflows for retrieving windows over the network.

**What varies:** Access method and artifact format.

**What stays fixed:** Window count, window duration, and requested channel subsets.

**Question answered:** For remote/cloud access, when is query-in-place better than full-file download first?

EDF download time in this run is marked as estimated.

| Method | Format | Channel subset | Total time | Avg/window | Throughput |
| --- | --- | --- | --- | --- | --- |
| duckdb_remote | Parquet int32 nV snappy | 10-20 (19ch) | 4.793s | 0.4790s | 23.2 MiB/s |
| full_download_then_read | EDF | 10-20 (19ch) | 13.72s | 0.2810s | — |
| duckdb_remote | Parquet float32 snappy | 10-20 (19ch) | 16.14s | 1.614s | 6.9 MiB/s |
| full_download_then_read | EDF | all | 18.53s | 0.7630s | — |
| duckdb_remote | Parquet float32 snappy | all | 25.78s | 2.578s | 10.5 MiB/s |
| duckdb_remote | Parquet int32 nV snappy | all | 34.50s | 3.450s | 7.8 MiB/s |

## J. Tuned Format Comparison

This group compares matched block-size variants generated specifically for Benchmark J.

### J.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** Which tuned layout is best for small random-access reads?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 0.0664s | 0.0632s | 0.0354s |
| 10m | 0.0666s | 0.0585s | 0.0644s |
| 20m | 0.0485s | 0.0588s | 0.1763s |
| 30m | 0.0552s | 0.0670s | 0.2451s |
| 60m | 0.0947s | 0.1174s | 0.4301s |
| 120m | 0.2379s | 0.2804s | 0.8316s |

### J.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** Which tuned layout best supports selective channel retrieval?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 0.0485s | 0.0485s | 0.0052s |
| 10m | 0.0342s | 0.0465s | 0.0104s |
| 20m | 0.0278s | 0.0354s | 0.0244s |
| 30m | 0.0355s | 0.0340s | 0.0254s |
| 60m | 0.0356s | 0.0342s | 0.0494s |
| 120m | 0.0490s | 0.0441s | 0.1003s |

### J.3 Peak Window-Scaling Throughput

**What it tests:** The best sustained throughput observed for each tuned variant across multiple window sizes.

**What varies:** On-disk block size, tuned storage variant, and requested window size.

**What stays fixed:** Channel count and comparison method.

**Question answered:** Which tuned layout scales best as read size grows?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 350.2 MiB/s @ 1800s | 328.3 MiB/s @ 3600s | 304.2 MiB/s @ 3600s |
| 10m | 333.3 MiB/s @ 3600s | 325.8 MiB/s @ 1800s | 271.0 MiB/s @ 1800s |
| 20m | 304.8 MiB/s @ 1800s | 310.1 MiB/s @ 1800s | 236.9 MiB/s @ 3600s |
| 30m | 329.4 MiB/s @ 1800s | 364.2 MiB/s @ 3600s | 242.0 MiB/s @ 3600s |
| 60m | 307.7 MiB/s @ 3600s | 329.6 MiB/s @ 3600s | 174.7 MiB/s @ 3600s |
| 120m | 317.5 MiB/s @ 3600s | 347.1 MiB/s @ 3600s | 180.3 MiB/s @ 3600s |

### J.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the tuned variants.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** Which tuned layout is best for whole-study scans rather than isolated random reads?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 15.19s | 15.31s | 16.80s |
| 10m | 14.38s | 14.22s | 17.53s |
| 20m | 15.49s | 13.89s | 28.57s |
| 30m | 16.16s | 14.48s | 42.81s |
| 60m | 18.91s | 18.89s | 68.65s |
| 120m | 29.55s | 30.39s | 144.8s |

Per-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons.
