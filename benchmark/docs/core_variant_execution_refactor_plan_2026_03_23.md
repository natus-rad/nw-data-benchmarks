## Core variant execution refactor plan (2026-03-23)

This document captures the implementation plan for refactoring config + execution so core benchmarks A-E run on explicit variants rather than legacy alias keys.

## Scope and fixed decisions

This plan assumes the following approved decisions:

- multi-study support is not included for now
- report generation remains effectively single-study for now
- no-variant direct-source core runs for ERD will be rejected explicitly for now
- canonical Parquet should always be materialized into cache, even for Parquet inputs
- the row-span / gap-safe refactor is deferred to later work
- root `variants[].id` is sufficient for now and acts as the user-facing variant label
- cache keys may combine variant id with a short hash of relevant spec fields

## 1. Summary of the target config model

The new model should be:

- `canonical_parquet` is a top-level config object for the normalized intermediate Parquet artifact
- root `variants:` is the complete source of truth for user-declared benchmark variants
- every root variant requires a stable string `id`
- core benchmarks A-E are configured under `benchmarks.core.<category>`
- each core category declares both `enabled` and `variants`
- non-core benchmark families move under nested `benchmarks` with minimal behavior change
- canonical Parquet is always created/reused internally, but is never implicitly benchmarked as a core variant

## 2. Proposed config schema

### Top-level structure

```yaml
azure:
  ...
studies:
  - name: demo
    input: /path/to/data.h5
    sample_freq: 256
cache_dir: .benchmark_cache
canonical_parquet:
  compression: snappy
  row_group_minutes: 30
variants:
  - id: pq_5m_lz4
    format: parquet
    row_group_minutes: 5
    compression: lz4
  - id: h5_col_30m
    format: hdf5
    layout: columnar
    chunk_minutes: 30
    dtype: float32
    compression: lz4
benchmarks:
  common:
    repetitions: 3
    default_window: 60
  core:
    random_access:
      enabled: true
      variants: all
      read_positions: [0.0, 0.5, 0.75, 0.95]
    channel_subset:
      enabled: true
      variants: all
      channel_subsets: [4, 10]
    remontage: {enabled: true, variants: all}
    filter_pipeline: {enabled: true, variants: all}
    window_scaling:
      enabled: true
      variants: all
      window_sizes: [10, 30, 60, 300, 900, 1800, 3600]
  parquet_investigations:
    compression: {enabled: false, variants: [...]}
    precision_loss: {enabled: false}
    int32_storage: {enabled: false}
    remote_query: {enabled: false, ...}
  other:
    tuned_comparison: {enabled: false, block_sizes_minutes: [5, 10, 20], parquet_codecs: [snappy, lz4], hdf5_compression: lz4, chunk_sec: 3600}
    baseline_comparison: {enabled: true, chunk_sec: 3600}
```

### `canonical_parquet`

Recommended supported fields:

- `compression`
- `row_group_minutes`

Recommended unsupported fields:

- `id`
- `format`
- HDF5-only fields like `layout` / `chunk_minutes`
- EDF-only fields
- Parquet `dtype` unless real support is added later

Recommendation: `canonical_parquet` remains a separate named config object and is not treated as a normal variant.

### Root `variants:` semantics

Root `variants:` means exactly:

- the full set of user-specified target variants generated from canonical Parquet

and nothing more.

Every entry must require:

- `id: <string>`
- `format: parquet | hdf5 | edf`

## 3. Core benchmark execution model

### A-E categories

- `random_access`
- `channel_subset`
- `remontage`
- `filter_pipeline`
- `window_scaling`

Each category lives under `benchmarks.core.<category>` and has:

- `enabled: true|false`
- `variants: all | [] | [<variant-id>, ...]`

### Variant selector semantics

- `all` = all root variant IDs in config order
- `[]` = run the category on no variants
- `[id1, id2]` = run only those declared variant IDs
- unknown IDs must fail validation clearly

### When root `variants:` is empty

Behavior:

- canonical Parquet is still created/reused internally
- core benchmarks must not benchmark canonical Parquet implicitly
- core benchmarks run on the original input artifact directly for supported inputs:
  - Parquet input -> source Parquet
  - HDF5 input -> source HDF5
  - EDF input -> source EDF
  - ERD input -> explicit validation error for now

Recommended interpretation of `variants: all` when root `variants:` is empty:

- resolve to a single synthetic `source` target descriptor for supported input formats

Explicit `[id, ...]` selectors are invalid when root `variants:` is empty.

### When root `variants:` is non-empty

Behavior:

- canonical Parquet is used only as intermediate source for variant generation
- core benchmarks run only on selected root variant IDs
- core results/reporting identify rows by variant id, not only broad alias keys like `parquet` or `h5_columnar`

## 4. Execution pipeline design

Recommended pipeline per study:

1. resolve the input artifact
2. materialize/reuse canonical Parquet in cache using `canonical_parquet` config
3. build `StudyInfo` from canonical Parquet
4. generate root variants from canonical Parquet when declared
5. build explicit execution target descriptors for:
   - source-direct core runs when no variants exist
   - root variants when variants exist
   - non-core family artifacts (investigations / tuned / baseline)
6. run enabled benchmark families against the correct target set

## 5. Target descriptor model

Introduce an internal descriptor structure for benchmarkable artifacts, e.g.:

- `artifact_id`
- `artifact_kind` = `source` | `variant` | `investigation` | `comparison`
- `format_family`
- `reader_kind`
- `path`
- `source_variant_id` (for root variants)

This replaces the current alias-driven core execution model.

Legacy alias keys like `parquet`, `h5_columnar`, `h5_rowgroup`, and `edf` can remain temporarily as adapters for non-core code paths during migration.

## 6. Result row / reporting changes for A-E

Core A-E result rows should add:

- `artifact_id`
- `variant_id` (null for source-direct runs)
- `artifact_kind`
- `format_family`
- `display_label` (same as `artifact_id` for now)

`format` may remain temporarily for compatibility, but A-E report rendering should pivot primarily by `artifact_id` / `display_label` instead of coarse format family.

Single-study reporting remains acceptable for now, but the report should clearly reject or avoid unsupported multi-study assumptions if necessary.

## 7. Canonical Parquet cache/materialization strategy

Canonical Parquet must always be materialized into cache, including for Parquet inputs.

That means:

- Parquet input is no longer treated as a direct canonical passthrough path
- the canonical cache artifact is always an explicit cached output
- `canonical_parquet` config participates in cache identity

Recommended cache naming:

- human-readable prefix from study name or source stem
- short stable hash of relevant spec fields

For user variants, cache identity should use:

- variant `id`
- plus a short hash of the effective variant spec (`id`, `format`, compression, layout, row-group/chunk config, etc.)

This prevents stale reuse when a variant keeps the same `id` but changes its spec.

## 8. Non-core benchmark family migration

### `benchmarks.parquet_investigations`

Move current parquet investigation config under:

- `benchmarks.parquet_investigations`

Preserve behavior as much as possible. Do not redesign investigation-specific variant generation now.

Recommendation: investigation-generated artifacts should eventually live in a separate output/cache folder from root user variants.

### `benchmarks.other.tuned_comparison`

Move current tuned comparison config under:

- `benchmarks.other.tuned_comparison`

Add explicit `enabled` at that leaf.

### `benchmarks.other.baseline_comparison`

Place section K under:

- `benchmarks.other.baseline_comparison`

Keep current semantics: it benchmarks resolved baseline input artifacts directly and remains independent from root user variants.

## 9. Modules likely to change

- `benchmark/scripts/run_benchmarks.py`
- `benchmark/core/config_helpers.py`
- `benchmark/core/constants.py`
- `benchmark/core/ingest.py`
- `benchmark/core/variants.py`
- `benchmark/core/benchmarks.py`
- `benchmark/core/bench_utils.py`
- `benchmark/core/setup.py`
- `benchmark/scripts/generate_benchmark_report.py`
- config templates and tests

Likely useful new modules:

- `benchmark/core/config_validation.py`
- `benchmark/core/target_registry.py`

## 10. Migration strategy

Recommended migration approach:

1. add config normalization/validation for the new nested `benchmarks` schema
2. preserve temporary backward compatibility for old benchmark config shape
3. require `variants[].id` immediately
4. add target-descriptor-based core execution
5. make A-E result rows/reporting variant-aware
6. move non-core families under nested `benchmarks`
7. later remove legacy alias-driven assumptions once the new model is stable

### Backward-compat mapping

Temporarily normalize old config shapes:

- old root `benchmarks: [list]` -> nested `benchmarks.*.*.enabled`
- old root `parquet_investigations` -> `benchmarks.parquet_investigations`
- old root `tuned_comparison` -> `benchmarks.other.tuned_comparison`
- old root `repetitions`, `default_window`, `read_positions`, `channel_subsets`, `window_sizes` -> `benchmarks.common` and `benchmarks.core.*`

Do not preserve backward compatibility for variants without `id`.

## 11. Risks, edge cases, and validation

Main risks:

- stale caches if spec hashing is incomplete
- partial migration where core code still depends on alias keys
- ERD no-variant path must fail explicitly and clearly
- reporting may collapse variants unless pivot logic is updated everywhere
- deferring row-span/gap-safe work means the new architecture must not make that later refactor harder

Validation/testing needed:

- missing / duplicate variant IDs
- invalid core selector IDs
- no-variant Parquet/HDF5/EDF source-direct execution
- explicit ERD rejection in no-variant core mode
- canonical Parquet always materialized for Parquet input
- `variants: all` respects config order
- A-E result rows include artifact/variant identity
- report tables separate multiple variants of same format family
- backward-compat config normalization

## 12. Recommended priority order

1. config schema + validation layer
2. canonical Parquet materialization refactor
3. variant generation keyed by required `id`
4. target descriptor abstraction
5. A-E refactor to variant-driven execution
6. result/report updates for variant awareness
7. nested benchmark-family migration + cleanup

## Bottom line

This refactor should make core benchmarks A-E run on explicit, user-declared variants rather than legacy alias keys, while keeping canonical Parquet as an internal normalized artifact and preserving minimal behavioral change for non-core benchmark families. The main intentional deferrals are multi-study support and the row-span/gap-safe refactor.
