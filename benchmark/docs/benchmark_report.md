# EDF vs HDF5 vs Parquet — Benchmark Results

**Study:** Suppression~ B (46 channels, 256 Hz, ~12.9 hours, 11.85M samples)

## Summary

| Benchmark | Parquet | HDF5 (best) | EDF | Parquet vs EDF |
|-----------|---------|-------------|-----|----------------|
| Random access (1 min, mid-study) | 0.044s | 0.225s | 0.674s | 15× |
| 4-channel subset (1 min) | 0.039s | 0.180s | 0.730s | 19× |
| Full pipeline, 12h | 14.2s | 37.4s | 2m 25s | 10× |
| Pipeline + sliding FFT, 12h | 22.0s | 44.5s | 2m 35s | 7× |
| 60-min throughput | 346 MiB/s | 272 MiB/s | 37 MiB/s | 9× |
| Precision | float32 | float32 | 16-bit | — |

## A — Random access

Read a 1-minute, 46-channel window from four positions.

| Position | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 0% | 0.045s | 0.216s | 0.265s | 0.685s |
| 50% | 0.044s | 0.254s | 0.225s | 0.674s |
| 75% | 0.046s | 0.228s | 0.351s | 0.670s |
| 95% | 0.070s | 0.208s | 0.213s | 0.772s |

Parquet is 11–15× faster than EDF. HDF5 is 2–4× faster than EDF but 3–5× slower than Parquet.

## B — Channel subset

Read a 1-minute window with varying channel counts.

| Channels | Parquet | HDF5 col | HDF5 rg | EDF |
|----------|---------|----------|---------|-----|
| 4 | 0.039s | 0.180s | 0.200s | 0.730s |
| 10 | 0.031s | 0.203s | 0.219s | 0.681s |
| 46 (all) | 0.040s | 0.206s | 0.210s | 0.696s |

EDF reads everything regardless of how many channels you need. Parquet reads only what you ask for.

## C — Re-montaging

Read 1 minute + apply standard bipolar montage (18 derived channels from 19 inputs).

| Format | Read | Montage | Total | vs EDF |
|--------|------|---------|-------|--------|
| Parquet | 0.043s | 0.001s | 0.043s | 16× |
| HDF5 col | 0.205s | 0.001s | 0.206s | 3.4× |
| EDF | 0.707s | 0.001s | 0.707s | 1.0× |

Montage computation is negligible. The bottleneck is I/O.

## D.1 — Full-study filter pipeline (12 hours)

Read + bipolar montage + 60 Hz notch + 0.5–70 Hz bandpass.

| Format | Read | Montage | Filter | Total | vs EDF |
|--------|------|---------|--------|-------|--------|
| Parquet | 10.3s | 0.57s | 3.4s | 14.2s | 10× |
| HDF5 col | 33.5s | 0.57s | 3.4s | 37.4s | 3.9× |
| HDF5 rg | 35.2s | 0.59s | 3.6s | 39.4s | 3.7× |
| EDF | 140.8s | 0.58s | 3.4s | 144.8s | 1.0× |

## D.2 — Sliding FFT (12 hours, 10s windows, 2s stride)

| Format | Read | Montage | Filter | FFT | Total |
|--------|------|---------|--------|-----|-------|
| Parquet | 9.9s | 0.55s | 3.2s | 8.3s | 22.0s |
| HDF5 col | 32.3s | 0.56s | 3.3s | 8.3s | 44.5s |
| EDF | 142.7s | 0.58s | 3.4s | 8.7s | 155.4s |

With Parquet, FFT computation is the largest single cost — I/O is no longer the bottleneck.

## E — Window size scaling

| Window | Parquet | H5 col | EDF | Parquet MiB/s | H5 col MiB/s | EDF MiB/s |
|--------|---------|--------|-----|---------------|--------------|-----------|
| 10s | 0.039s | 0.211s | 0.649s | 11.5 | 2.1 | 0.7 |
| 30s | 0.037s | 0.220s | 0.674s | 36.0 | 6.1 | 2.0 |
| 1 min | 0.042s | 0.244s | 0.807s | 64.0 | 11.0 | 3.3 |
| 5 min | 0.065s | 0.305s | 1.071s | 208.8 | 44.2 | 12.6 |
| 15 min | 0.147s | 0.326s | 1.559s | 275.6 | 123.9 | 25.9 |
| 30 min | 0.280s | 0.426s | 2.669s | 288.5 | 189.9 | 30.3 |
| 60 min | 0.467s | 0.595s | 4.423s | 346.4 | 271.9 | 36.6 |

Parquet peaks at 346 MiB/s. HDF5 columnar reaches 272 MiB/s at 60 min. EDF tops out at 37 MiB/s.

## Recommended architecture

Use Parquet for waveform data (immutable, columnar, cloud-queryable).
Use HDF5 for everything else (annotations, metadata, electrode positions, video references).

Parquet is the better *data* format for analytical workloads — EEG review, seizure search,
and ML pipelines. HDF5 is the better *file* format for hierarchical, mutable metadata.
The two complement each other.

