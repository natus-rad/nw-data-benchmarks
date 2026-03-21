# Benchmark Report

_Generated from `C:/dev/nw/perforce/NW10_GMA4/Source/sdk/nw-data-benchmarks/benchmark/results/2026-03-21T07-54-46_benchmark_results.json` (run `2026-03-21T07-54-46`)._

This report is automatically generated from benchmark result JSON and is intended to replace manual Markdown edits.

## Run Overview

The report is generated directly from benchmark result JSON. Sections for categories not present in the input file are called out explicitly, so partial benchmark runs still produce a readable report.

| Property | Value |
| --- | --- |
| Study | suppression_study |
| Channels | 46 |
| Sample rate | 256.0 Hz |
| Duration | 12.86 h (11,854,000 samples) |
| System | Windows 11 / Python 3.12.9 / 12 CPU threads / 31.7 GiB RAM |
| Categories present | channel_subset, compression, filter_pipeline_full, int32_storage, precision_loss, random_access, remontage, remote_query, sliding_fft_full, tuned_channel_subset, tuned_full_study, tuned_random_access, tuned_window_scaling, window_scaling |

## Executive Summary

Benchmark rows: **273** across **14** categories.

| Area | Winner | Result |
| --- | --- | --- |
| Random access (median 1-minute read) | HDF5 row-group | 0.0454s |
| 4-channel subset | HDF5 columnar | 0.0055s |
| Full-study filter pipeline | Parquet | 14.83s |
| Peak window-scaling throughput | Parquet @ 3600s | 307.6 MiB/s |
| Best tuned full-study read | HDF5 LZ4 (5m) | 12.22s |

## Key Observations

- **Random access:** HDF5 row-group has the lowest median 1-minute read time at 0.0454s, about 1.12× faster than Parquet.
- **Compression trade-off:** smallest Parquet artifact is zstd_9 at 618.4 MiB, while the fastest 1-minute read is snappy at 0.0464s.
- **Int32 variants:** the most compact measured variant is int32_nanovolt (zstd) at 581.3 MiB; its reported SNR vs float32 is 144.36 dB.
- **Remote access:** the fastest remote query path in this run is duckdb_remote for 10-20 (19ch) at 4.423s total over 10 windows.

## A. Random Access

**What it tests:** Repeated all-channel reads of the same 60-second window from different positions in the study.

**What varies:** Read position (`0%`, `50%`, `75%`, `95%`).

**What stays fixed:** Window size, channel count, and baseline format/layout readers.

**Question answered:** How sensitive is each format to where in the study a random read occurs?

HDF5 row-group has the lowest median 1-minute read time across read positions at 0.0454s.

| Position | Parquet | HDF5 columnar | HDF5 row-group | EDF |
| --- | --- | --- | --- | --- |
| 0% | 0.0541s / 49.8 MiB/s | 0.0378s / 71.2 MiB/s | 0.0461s / 58.5 MiB/s | 0.0782s / 34.4 MiB/s |
| 50% | 0.0448s / 60.1 MiB/s | 0.0441s / 61.1 MiB/s | 0.0315s / 85.6 MiB/s | 0.0574s / 46.9 MiB/s |
| 75% | 0.0476s / 56.6 MiB/s | 0.0876s / 30.8 MiB/s | 0.0613s / 44.0 MiB/s | 0.0778s / 34.6 MiB/s |
| 95% | 0.0641s / 42.1 MiB/s | 0.0864s / 31.2 MiB/s | 0.0447s / 60.3 MiB/s | 0.1198s / 22.5 MiB/s |

## B. Channel Subset

**What it tests:** Reads of the same 60-second window while requesting fewer channels.

**What varies:** Number of requested channels.

**What stays fixed:** Read position, window size, and baseline format/layout readers.

**Question answered:** Which formats benefit most when the workload only needs a subset of channels?

4 channels → HDF5 columnar is fastest at 0.0055s. 10 channels → HDF5 columnar is fastest at 0.0152s. 46 channels → HDF5 row-group is fastest at 0.0320s.

| Channels | Parquet | HDF5 columnar | HDF5 row-group | EDF |
| --- | --- | --- | --- | --- |
| 4 | 0.0395s / 5.9 MiB/s | 0.0055s / 42.3 MiB/s | 0.0352s / 6.7 MiB/s | 0.0060s / 38.7 MiB/s |
| 10 | 0.0447s / 13.1 MiB/s | 0.0152s / 38.5 MiB/s | 0.0352s / 16.7 MiB/s | 0.0192s / 30.5 MiB/s |
| 46 | 0.0420s / 64.2 MiB/s | 0.0365s / 73.8 MiB/s | 0.0320s / 84.3 MiB/s | 0.0725s / 37.2 MiB/s |

## C. Re-montage

**What it tests:** A read followed immediately by bipolar montage computation.

**What varies:** Storage format/layout.

**What stays fixed:** Window size, channel set, and the montage operation itself.

**Question answered:** Once downstream signal processing is included, how much of total time is storage I/O versus lightweight computation?

Montage is a relatively small fraction of end-to-end time in this benchmark (average 2.1% of total wall time).

| Format | Read | Montage | Total | Montage share |
| --- | --- | --- | --- | --- |
| HDF5 columnar | 0.0419s | 0.0009s | 0.0428s | 2.1% |
| HDF5 row-group | 0.0437s | 0.0011s | 0.0448s | 2.4% |
| Parquet | 0.0502s | 0.0014s | 0.0515s | 2.6% |
| EDF | 0.0727s | 0.0008s | 0.0735s | 1.1% |

## D.1 Full-Study Filter Pipeline

**What it tests:** End-to-end full-study read, montage, and digital filtering.

**What varies:** Storage format/layout.

**What stays fixed:** Entire study duration, channel set, filter pipeline, and processing order.

**Question answered:** Which format is best for whole-study offline processing workloads that must read and transform all signal data?

For the full-study read → montage → filter pipeline, Parquet is fastest at 14.83s.

| Format | Read | Montage | Filter | Total | Throughput |
| --- | --- | --- | --- | --- | --- |
| Parquet | 10.51s | 0.6490s | 3.672s | 14.83s | 130.8 MiB/s |
| HDF5 row-group | 10.54s | 0.6300s | 4.035s | 15.21s | 127.6 MiB/s |
| HDF5 columnar | 12.31s | 0.7490s | 4.379s | 17.44s | 111.3 MiB/s |
| EDF | 106.3s | 0.9150s | 5.261s | 112.4s | 17.3 MiB/s |

## D.2 Sliding FFT

**What it tests:** The same full-study read/filter pipeline plus overlapping FFT window computation.

**What varies:** Storage format/layout.

**What stays fixed:** Study duration, preprocessing pipeline, FFT window/stride, and channel set.

**Question answered:** How much does storage choice matter once a heavier downstream spectral-analysis workload is layered on top?

This stage computed 21,596 overlapping FFT windows across the full study.

| Format | Read | Montage | Filter | FFT | Total |
| --- | --- | --- | --- | --- | --- |
| HDF5 columnar | 7.907s | 0.6920s | 3.796s | 9.664s | 22.45s |
| HDF5 row-group | 8.925s | 0.6150s | 3.887s | 9.328s | 23.13s |
| Parquet | 11.08s | 0.7950s | 4.186s | 10.17s | 26.68s |
| EDF | 49.06s | 0.6680s | 3.885s | 9.471s | 63.49s |

## E. Window Scaling

**What it tests:** All-channel reads from the middle of the study while increasing requested window size.

**What varies:** Window size.

**What stays fixed:** Read position, channel count, and baseline format/layout readers.

**Question answered:** How does each format transition from small random reads to large sustained reads?

Best measured throughput is 307.6 MiB/s from Parquet at a 3600s window.

| Window | Parquet | HDF5 columnar | HDF5 row-group | EDF |
| --- | --- | --- | --- | --- |
| 10s | 5.9 MiB/s | 3.4 MiB/s | 10.6 MiB/s | 22.2 MiB/s |
| 30s | 18.7 MiB/s | 20.1 MiB/s | 29.9 MiB/s | 25.0 MiB/s |
| 60s | 52.0 MiB/s | 76.4 MiB/s | 83.0 MiB/s | 36.2 MiB/s |
| 300s | 145.6 MiB/s | 177.1 MiB/s | 124.6 MiB/s | 42.6 MiB/s |
| 900s | 239.2 MiB/s | 268.8 MiB/s | 119.8 MiB/s | 34.6 MiB/s |
| 1800s | 216.8 MiB/s | 223.7 MiB/s | 92.1 MiB/s | 34.7 MiB/s |
| 3600s | 307.6 MiB/s | 275.3 MiB/s | 160.8 MiB/s | 40.7 MiB/s |

## F. Compression

**What it tests:** Parquet codec tradeoffs between read speed and resulting artifact size.

**What varies:** Codec (`none`, `snappy`, `zstd`, `lz4`).

**What stays fixed:** Signal data, window size, and Parquet layout.

**Question answered:** Which Parquet codec gives the best balance between storage efficiency and read performance?

Against a raw float32 baseline of 2,170.5 MiB, the smallest Parquet artifact is zstd_9 at 618.4 MiB. The fastest 1-minute read is snappy at 0.0464s.

| Codec | 1-minute read | Artifact size | Ratio vs raw float32 |
| --- | --- | --- | --- |
| zstd_9 | 0.1085s | 618.4 MiB | 3.51× |
| zstd_3 | 0.0856s | 620.8 MiB | 3.50× |
| lz4 | 0.0889s | 717.8 MiB | 3.02× |
| snappy | 0.0464s | 759.1 MiB | 2.86× |
| none | 0.1024s | 791.0 MiB | 2.74× |

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
| int32_nanovolt | numpy | zstd | 0.0568s | 47.4 MiB/s |
| float32 | — | zstd_3 | 0.0588s | 45.8 MiB/s |
| int32_calibrated | numpy | zstd | 0.0625s | 43.1 MiB/s |

## I. Remote Query

**What it tests:** Remote-access workflows for retrieving windows over the network.

**What varies:** Access method and artifact format.

**What stays fixed:** Window count, window duration, and requested channel subsets.

**Question answered:** For remote/cloud access, when is query-in-place better than full-file download first?

EDF download time in this run is marked as estimated.

| Method | Format | Channel subset | Total time | Avg/window | Throughput |
| --- | --- | --- | --- | --- | --- |
| duckdb_remote | Parquet int32 nV snappy | 10-20 (19ch) | 4.423s | 0.4420s | 25.2 MiB/s |
| full_download_then_read | EDF | 10-20 (19ch) | 13.54s | 0.2630s | — |
| duckdb_remote | Parquet float32 snappy | 10-20 (19ch) | 15.48s | 1.548s | 7.2 MiB/s |
| full_download_then_read | EDF | all | 17.66s | 0.6750s | — |
| duckdb_remote | Parquet float32 snappy | all | 26.09s | 2.609s | 10.3 MiB/s |
| duckdb_remote | Parquet int32 nV snappy | all | 30.57s | 3.057s | 8.8 MiB/s |

## J. Tuned Format Comparison

This group compares matched block-size variants generated specifically for Benchmark J.

### J.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** Which tuned layout is best for small random-access reads?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 0.0669s | 0.0519s | 0.0553s |
| 10m | 0.0468s | 0.0459s | 0.0916s |
| 20m | 0.0576s | 0.0822s | 0.1630s |
| 30m | 0.0739s | 0.0642s | 0.2379s |
| 60m | 0.0855s | 0.1178s | 0.4937s |
| 120m | 0.1765s | 0.2043s | 0.9281s |

### J.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** Which tuned layout best supports selective channel retrieval?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 0.0549s | 0.0540s | 0.0076s |
| 10m | 0.0416s | 0.0417s | 0.0124s |
| 20m | 0.0328s | 0.0364s | 0.0303s |
| 30m | 0.0513s | 0.0353s | 0.0380s |
| 60m | 0.0815s | 0.0972s | 0.0980s |
| 120m | 0.1106s | 0.0759s | 0.2150s |

### J.3 Peak Window-Scaling Throughput

**What it tests:** The best sustained throughput observed for each tuned variant across multiple window sizes.

**What varies:** On-disk block size, tuned storage variant, and requested window size.

**What stays fixed:** Channel count and comparison method.

**Question answered:** Which tuned layout scales best as read size grows?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 348.8 MiB/s @ 3600s | 316.8 MiB/s @ 1800s | 326.3 MiB/s @ 3600s |
| 10m | 355.8 MiB/s @ 3600s | 382.6 MiB/s @ 3600s | 304.0 MiB/s @ 1800s |
| 20m | 346.6 MiB/s @ 1800s | 327.6 MiB/s @ 3600s | 272.6 MiB/s @ 1800s |
| 30m | 340.6 MiB/s @ 3600s | 351.6 MiB/s @ 3600s | 221.4 MiB/s @ 3600s |
| 60m | 340.6 MiB/s @ 3600s | 336.9 MiB/s @ 3600s | 179.4 MiB/s @ 3600s |
| 120m | 342.1 MiB/s @ 3600s | 332.7 MiB/s @ 3600s | 183.4 MiB/s @ 3600s |

### J.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the tuned variants.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** Which tuned layout is best for whole-study scans rather than isolated random reads?

| Block size | Parquet snappy | Parquet LZ4 | HDF5 LZ4 |
| --- | --- | --- | --- |
| 5m | 14.59s | 16.00s | 12.22s |
| 10m | 14.19s | 13.84s | 21.69s |
| 20m | 14.90s | 12.70s | 28.29s |
| 30m | 16.11s | 18.40s | 38.64s |
| 60m | 18.68s | 19.35s | 68.52s |
| 120m | 29.21s | 28.58s | 126.2s |

Per-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons.
