<!-- AUTO-GENERATED -->

# Benchmark Report

_Generated from `${source_file}` (run `${run_id}`)._

This report is automatically generated from benchmark result JSON and is intended to replace manual Markdown edits.

## Methodology

- **Timing.** Each timed operation runs a configured number of repetitions (`benchmarks.common.repetitions`, default 3) and the reported time is the **median** across repetitions. The first (coldest) repetition is recorded separately and shown as "first ..." in timing cells when it differs from the median. All raw per-repetition samples are preserved in the results JSON (`timing_samples_seconds`).
- **Cache semantics.** The OS page cache is not evicted between repetitions, so median times reflect warm-cache reads. Treat the first-run value as the cold-start proxy.
- **Throughput.** All throughput values in this report are computed from the theoretical decoded float32 payload (rows x channels x 4 bytes) divided by the median wall time - not from compressed bytes on disk.
- **Fairness of open cost.** Every format opens and closes its file on each timed call; no format holds a persistent handle across repetitions, so file-open overhead is included uniformly.
- **HDF5 caveat.** HDF5 artifacts include a benchmark-specific `chunk_index` helper dataset for fast seeks. HDF5 results therefore represent optimized HDF5-with-helper behavior, not generic HDF5.
- **Memory.** Peak RSS is sampled by a background poller at 50 ms resolution inside the timed region; values are indicative, and very short operations may under-report their true peak.

## Run Overview

${overview}

## Executive Summary

Benchmark rows: **${benchmark_count}** across **${category_count}** categories.

${summary}

## Key Observations

${key_observations}

## A. Random Access

**What it tests:** Repeated all-channel reads of the same 60-second window from different positions in the study.

**What varies:** Read position (`0%`, `50%`, `75%`, `95%`).

**What stays fixed:** Window size, channel count, and baseline format/layout readers.

**Question answered:** How sensitive is each format to where in the study a random read occurs?

${a_results}

## B. Channel Subset

**What it tests:** Reads of the same 60-second window while requesting fewer channels.

**What varies:** Number of requested channels.

**What stays fixed:** Read position, window size, and baseline format/layout readers.

**Question answered:** Which formats benefit most when the workload only needs a subset of channels?

${b_results}

## C. Re-montage

**What it tests:** A read followed immediately by bipolar montage computation.

**What varies:** Storage format/layout.

**What stays fixed:** Window size, channel set, and the montage operation itself.

**Question answered:** Once downstream signal processing is included, how much of total time is storage I/O versus lightweight computation?

${c_results}

## D.1 Full-Study Filter Pipeline

**What it tests:** End-to-end full-study read, montage, and digital filtering.

**What varies:** Storage format/layout.

**What stays fixed:** Entire study duration, channel set, filter pipeline, and processing order.

**Question answered:** Which format is best for whole-study offline processing workloads that must read and transform all signal data?

${d1_results}

## D.2 Sliding FFT

**What it tests:** The same full-study read/filter pipeline plus overlapping FFT window computation.

**What varies:** Storage format/layout.

**What stays fixed:** Study duration, preprocessing pipeline, FFT window/stride, and channel set.

**Question answered:** How much does storage choice matter once a heavier downstream spectral-analysis workload is layered on top?

${d2_results}

## E. Window Scaling

**What it tests:** All-channel reads from the middle of the study while increasing requested window size.

**What varies:** Window size.

**What stays fixed:** Read position, channel count, and baseline format/layout readers.

**Question answered:** How does each format transition from small random reads to large sustained reads?

${e_results}

## F. Compression

**What it tests:** Parquet codec tradeoffs between read speed and resulting artifact size.

**What varies:** Codec (`none`, `snappy`, `zstd`, `lz4`).

**What stays fixed:** Signal data, window size, and Parquet layout.

**Question answered:** Which Parquet codec gives the best balance between storage efficiency and read performance?

${f_results}

## G. Precision Loss

**What it tests:** Float32 → EDF 16-bit → float32 round-trip quantization error.

**What varies:** Channel signal statistics.

**What stays fixed:** Window size and round-trip conversion procedure.

**Question answered:** What numeric precision is lost when using EDF-style 16-bit storage instead of float32?

${g_results}

## H. Int32 Storage

**What it tests:** Alternative int32-based storage encodings versus float32 for size, precision, and read speed.

**What varies:** Encoding mode, codec, and read path.

**What stays fixed:** Signal data and comparison baseline.

**Question answered:** Can int32 encodings reduce storage cost while preserving acceptable fidelity and performance?

${h_results}

## I. Remote Query

**What it tests:** Remote-access workflows for retrieving windows over the network.

**What varies:** Access method and artifact format.

**What stays fixed:** Window count, window duration, and requested channel subsets.

**Question answered:** For remote/cloud access, when is query-in-place better than full-file download first?

${i_results}

## J. Tuned Format Comparison

This group compares matched block-size variants generated specifically for Benchmark J.

### J.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** Which tuned layout is best for small random-access reads?

${j1_results}

### J.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels across tuned block sizes.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** Which tuned layout best supports selective channel retrieval?

${j2_results}

### J.3 Throughput vs Window Size

**What it tests:** How read throughput for each format scales as the requested window grows, from small random reads (10s) to large sequential reads (3600s).

**What varies:** Requested window size and tuned storage format.

**What stays fixed:** Channel count, read position (mid-study), and comparison method. The best-performing block size is selected per cell.

**Question answered:** Which tuned format scales best as the read window grows?

${j3_results}

### J.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the tuned variants.

**What varies:** On-disk block size and tuned storage variant.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** Which tuned layout is best for whole-study scans rather than isolated random reads?

${j4_results}${j_notes}

## K. Baseline Format Comparison

This group runs the Benchmark J workload suite on the resolved baseline input artifact(s) only, without generating tuned comparison variants.

Reported throughput values use the theoretical decoded float32 payload size: rows × channels × 4 bytes.

### K.1 Random Access

**What it tests:** A single 60-second all-channel read at mid-study on the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Read position, read size, and channel count.

**Question answered:** How does the baseline input format perform on the same random-access workload used in Benchmark J?

${k1_results}

### K.2 Channel Subset

**What it tests:** A 60-second read of only 4 channels on the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Read position, read duration, and requested channel count.

**Question answered:** How well does the baseline input format support selective channel retrieval on the J-style workload?

${k2_results}

### K.3 Throughput vs Window Size

**What it tests:** How read throughput for the baseline input artifact scales as the requested window grows, from small random reads (10s) to large sequential reads (3600s).

**What varies:** Requested window size.

**What stays fixed:** Channel count, read position (mid-study), and the use of the baseline input artifact only.

**Question answered:** How does the baseline input format scale on the same workload family used for Benchmark J?

${k3_results}

### K.4 Full-Study Sequential Read

**What it tests:** Reading the entire study chunk-by-chunk using the baseline input artifact.

**What varies:** Baseline input format.

**What stays fixed:** Full-study coverage, channel count, and sequential chunked access pattern.

**Question answered:** How does the baseline input artifact perform on whole-study scans compared with the tuned variants in Benchmark J?

${k4_results}${k_notes}