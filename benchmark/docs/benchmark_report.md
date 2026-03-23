# Benchmark Report

_Generated from `2026-03-23T13-01-44_benchmark_results.json` (run `2026-03-23T13-01-44`)._

This report is automatically generated from benchmark result JSON and is intended to replace manual Markdown edits.

## Run Overview

The report is generated directly from benchmark result JSON. Sections for categories not present in the input file are called out explicitly, so partial benchmark runs still produce a readable report. All reported throughput values use the theoretical decoded float32 payload size: rows × channels × 4 bytes. HDF5 timings in this benchmark use a custom benchmark-specific `chunk_index` lookup structure built at conversion time, which intentionally gives HDF5 a best-case seek/read path rather than representing plain generic HDF5 without that helper.

| Property | Value |
| --- | --- |
| Study | suppression_study |
| Channels | 46 |
| Sample rate | 256.0 Hz |
| Duration | 12.86 h (11,854,000 samples) |
| System | Windows 11 / Python 3.12.9 / 12 CPU threads / 31.7 GiB RAM |
| Categories present | baseline_channel_subset, baseline_full_study, baseline_random_access, baseline_window_scaling, channel_subset, filter_pipeline_full, precision_loss, random_access, remontage, remote_query, remote_query_full_study, sliding_fft_full, window_scaling |

## Executive Summary

Benchmark rows: **44** across **13** categories.

| Area | Winner | Result |
| --- | --- | --- |
| Random access (median 1-minute read) | pq_30m_lz4 | 0.0818s |
| 4-channel subset | pq_30m_lz4 | 0.0780s |
| Full-study filter pipeline | pq_30m_lz4 | 23.37s |
| Peak window-scaling throughput | pq_30m_lz4 @ 3600s | 291.8 MiB/s |

## Key Observations

- **Random access:** pq_30m_lz4 has the lowest median 1-minute read time at 0.0818s, about 1.62× faster than canonical.
- **Remote access:** the fastest remote query path in this run is duckdb_remote for 10-20 (19ch) at 1.087s total over 2 windows.

## A. Random Access

**What it tests:** Repeated all-channel reads of the same 60-second window from different positions in the study.

**What varies:** Read position (`0%`, `50%`, `75%`, `95%`).

**What stays fixed:** Window size, channel count, and baseline format/layout readers.

**Question answered:** How sensitive is each format to where in the study a random read occurs?

pq_30m_lz4 has the lowest median 1-minute read time across read positions at 0.0818s.

| Position | pq_30m_lz4 | canonical |
| --- | --- | --- |
| 0% | 0.1054s / 25.6 MiB/s | 0.0755s / 35.7 MiB/s |
| 50% | 0.0782s / 34.4 MiB/s | 0.0659s / 40.9 MiB/s |
| 75% | 0.0853s / 31.6 MiB/s | 0.1887s / 14.3 MiB/s |
| 95% | 0.0631s / 42.7 MiB/s | 0.2595s / 10.4 MiB/s |

## B. Channel Subset

**What it tests:** Reads of the same 60-second window while requesting fewer channels.

**What varies:** Number of requested channels.

**What stays fixed:** Read position, window size, and baseline format/layout readers.

**Question answered:** Which formats benefit most when the workload only needs a subset of channels?

4 channels → pq_30m_lz4 is fastest at 0.0780s. 10 channels → pq_30m_lz4 is fastest at 0.0957s. 46 channels → pq_30m_lz4 is fastest at 0.1080s.

| Channels | pq_30m_lz4 |
| --- | --- |
| 4 | 0.0780s / 3.0 MiB/s |
| 10 | 0.0957s / 6.1 MiB/s |
| 46 | 0.1080s / 25.0 MiB/s |

## C. Re-montage

**What it tests:** A read followed immediately by bipolar montage computation.

**What varies:** Storage format/layout.

**What stays fixed:** Window size, channel set, and the montage operation itself.

**Question answered:** Once downstream signal processing is included, how much of total time is storage I/O versus lightweight computation?

Montage is a relatively small fraction of end-to-end time in this benchmark (average 1.4% of total wall time).

| Format | Read | Montage | Total | Montage share |
| --- | --- | --- | --- | --- |
| pq_30m_lz4 | 0.1679s | 0.0023s | 0.1702s | 1.4% |

## D.1 Full-Study Filter Pipeline

**What it tests:** End-to-end full-study read, montage, and digital filtering.

**What varies:** Storage format/layout.

**What stays fixed:** Entire study duration, channel set, filter pipeline, and processing order.

**Question answered:** Which format is best for whole-study offline processing workloads that must read and transform all signal data?

For the full-study read → montage → filter pipeline, pq_30m_lz4 is fastest at 23.37s.

| Format | Read | Montage | Filter | Total | Throughput |
| --- | --- | --- | --- | --- | --- |
| pq_30m_lz4 | 16.80s | 1.235s | 5.337s | 23.37s | 83.0 MiB/s |

## D.2 Sliding FFT

**What it tests:** The same full-study read/filter pipeline plus overlapping FFT window computation.

**What varies:** Storage format/layout.

**What stays fixed:** Study duration, preprocessing pipeline, FFT window/stride, and channel set.

**Question answered:** How much does storage choice matter once a heavier downstream spectral-analysis workload is layered on top?

This stage computed 21,596 overlapping FFT windows across the full study.

| Format | Read | Montage | Filter | FFT | Total |
| --- | --- | --- | --- | --- | --- |
| pq_30m_lz4 | 15.11s | 1.202s | 4.963s | 11.26s | 33.05s |

## E. Window Scaling

**What it tests:** All-channel reads from the middle of the study while increasing requested window size.

**What varies:** Window size.

**What stays fixed:** Read position, channel count, and baseline format/layout readers.

**Question answered:** How does each format transition from small random reads to large sustained reads?

Best measured throughput is 291.8 MiB/s from pq_30m_lz4 at a 3600s window.

| Window | pq_30m_lz4 |
| --- | --- |
| 10s | 3.1 MiB/s |
| 30s | 12.7 MiB/s |
| 60s | 25.8 MiB/s |
| 300s | 74.6 MiB/s |
| 900s | 157.8 MiB/s |
| 1800s | 183.6 MiB/s |
| 3600s | 291.8 MiB/s |

## F. Compression

**What it tests:** Parquet codec tradeoffs between read speed and resulting artifact size.

**What varies:** Codec (`none`, `snappy`, `zstd`, `lz4`).

**What stays fixed:** Signal data, window size, and Parquet layout.

**Question answered:** Which Parquet codec gives the best balance between storage efficiency and read performance?

*This category was not present in the input results file.*

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

*This category was not present in the input results file.*

## I. Remote Query

**What it tests:** Remote-access workflows for retrieving windows over the network.

**What varies:** Access method and artifact format.

**What stays fixed:** Window count, window duration, and requested channel subsets.

**Question answered:** For remote/cloud access, when is query-in-place better than full-file download first?

Received settings: n_random_points=2; window_sec=600s; full_study_chunk_sec=3600s.

All reported remote timings are direct measurements.

| Method | Format | Channel subset | Total time | Avg/window | Throughput |
| --- | --- | --- | --- | --- | --- |
| duckdb_remote | parquet_single_file_lz4 | 10-20 (19ch) | 1.087s | 0.5430s | 20.5 MiB/s |
| duckdb_remote | Parquet int32 nV snappy | 10-20 (19ch) | 1.112s | 0.5560s | 20.0 MiB/s |
| duckdb_remote | Parquet float32 snappy | 10-20 (19ch) | 3.066s | 1.533s | 7.3 MiB/s |
| duckdb_remote | parquet_single_file_lz4 | all | 4.665s | 2.332s | 11.6 MiB/s |
| duckdb_remote | Parquet float32 snappy | all | 6.241s | 3.120s | 8.6 MiB/s |
| duckdb_remote | Parquet int32 nV snappy | all | 10.86s | 5.428s | 5.0 MiB/s |

## J. Tuned Format Comparison

This group compares matched block-size variants generated specifically for Benchmark J.

### J.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** Which tuned layout is best for small random-access reads?

*This category was not present in the input results file.*

### J.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** Which tuned layout best supports selective channel retrieval?

*This category was not present in the input results file.*

### J.3 Throughput vs Window Size

**What it tests:** How read throughput for each format scales as the requested window grows, from small random reads (10s) to large sequential reads (3600s).

**What varies:** Requested window size and tuned storage format.

**What stays fixed:** Channel count, read position (mid-study), and comparison method. The best-performing block size is selected per cell.

**Question answered:** Which tuned format scales best as the read window grows?

*This category was not present in the input results file.*

### J.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the tuned variants.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** Which tuned layout is best for whole-study scans rather than isolated random reads?

*This category was not present in the input results file.*

## K. Baseline Format Comparison

This group runs the Benchmark J workload suite on the resolved baseline input artifact(s) only, without generating tuned comparison variants.

Reported throughput values use the theoretical decoded float32 payload size: rows × channels × 4 bytes.

### K.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study on the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** How does the baseline input format perform on the same random-access workload used in Benchmark J?

| Artifact | Baseline input Parquet |
| --- | --- |
| Baseline input | 0.1501s / 18.0 MiB/s |

### K.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels on the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** How well does the baseline input format support selective channel retrieval on the J-style workload?

| Artifact | Baseline input Parquet |
| --- | --- |
| Baseline input | 0.1334s / 1.8 MiB/s |

### K.3 Throughput vs Window Size

**What it tests:** How read throughput for the baseline input artifact scales as the requested window grows, from small random reads (10s) to large sequential reads (3600s).

**What varies:** Requested window size.

**What stays fixed:** Channel count, read position (mid-study), and the use of the baseline input artifact only.

**Question answered:** How does the baseline input format scale on the same workload family used for Benchmark J?

Each cell shows the measured throughput for the baseline input artifact(s) at that window size.

| Window | Baseline input Parquet |
| --- | --- |
| 10s | 5.6 MiB/s |
| 30s | 22.7 MiB/s |
| 60s | 43.9 MiB/s |
| 300s | 133.2 MiB/s |
| 900s | 224.7 MiB/s |
| 1800s | 250.1 MiB/s |
| 3600s | 247.2 MiB/s |

### K.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** How does the baseline input artifact perform on whole-study scans compared with the tuned variants in Benchmark J?

| Artifact | Baseline input Parquet |
| --- | --- |
| Baseline input | 9.429s / 220.6 MiB/s |

Benchmark K runs the Benchmark J workload family on the resolved baseline input artifact(s) without generating tuned comparison variants.
