#!/usr/bin/env python3
"""EEG format benchmark CLI and orchestration entrypoint."""
from __future__ import annotations

import argparse
import datetime
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark.core.azure_storage import resolve_input_path
from benchmark.core.bench_utils import _estimate_runs, _print_result
from benchmark.core.benchmarks import BENCHMARKS
from benchmark.core.config_helpers import (
    get_canonical_parquet_cfg,
    get_parquet_compression_variants,
    get_remote_query_cfg,
    normalize_config,
    selected_categories,
    get_tuned_block_sizes_minutes,
    get_tuned_chunk_sec,
    get_tuned_hdf5_compression,
    get_tuned_parquet_codecs,
    is_investigation_enabled,
    tuned_parquet_key,
    validate_config,
)
from benchmark.core.constants import Category, FormatKey, InputFormat
from benchmark.core.ingest import _canonical_file, _detect_format, _recover_sample_freq, ingest
from benchmark.core.remote import bench_remote_query
from benchmark.core.setup import (
    _setup_int32_variants,
    _setup_parquet_compression_variants, _setup_tuned_variants,
)
from benchmark.core.study_info import StudyInfo, _system_info, load_config
from benchmark.core.variants import _safe_id, _spec_hash as _variant_spec_hash, generate_variants
from benchmark.scripts.generate_benchmark_report import generate_report


_RESULT_SAVE_RETRIES = 10
_RESULT_SAVE_RETRY_DELAY_SECONDS = 0.2


def _selected_benchmarks(cfg: dict, args: argparse.Namespace) -> list[tuple[str, str, object]]:
    cfg = normalize_config(cfg)
    categories = args.categories if args.categories else selected_categories(cfg)
    selected = []
    for cat in categories:
        if cat in BENCHMARKS:
            selected.append((cat, *BENCHMARKS[cat]))
        else:
            print(f"  [warn] Unknown benchmark category: {cat}")
    return selected


def _study_input_value(study_cfg: dict) -> str:
    if "input" not in study_cfg:
        name = study_cfg.get("name", "<unnamed>")
        raise ValueError(
            f"Study '{name}' must define 'input'. Legacy study configs are no longer "
            "supported; migrate source/local_path/remote_parquet_url/blob_prefix to "
            "the universal input format."
        )
    return str(study_cfg["input"])


def _baseline_input_paths(input_path: Path, detected_fmt: str) -> dict[str, Path]:
    if detected_fmt == InputFormat.PARQUET:
        return {"baseline_parquet": Path(input_path)}
    if detected_fmt == InputFormat.HDF5:
        return {"baseline_hdf5": Path(input_path)}
    if detected_fmt == InputFormat.EDF:
        return {"baseline_edf": Path(input_path)}
    if detected_fmt == InputFormat.ERD:
        return {"baseline_erd": Path(input_path)}
    return {}


def _source_target(input_path: Path, detected_fmt: str) -> dict | None:
    if detected_fmt == InputFormat.PARQUET:
        return {
            "artifact_id": "source_parquet",
            "variant_id": None,
            "artifact_kind": "source",
            "format_family": "parquet",
            "reader_kind": "parquet",
            "path": Path(input_path),
            "display_label": "source_parquet",
            "sort_index": 0,
        }
    if detected_fmt == InputFormat.HDF5:
        return {
            "artifact_id": "source_hdf5",
            "variant_id": None,
            "artifact_kind": "source",
            "format_family": "hdf5",
            "reader_kind": "hdf5_input",
            "path": Path(input_path),
            "display_label": "source_hdf5",
            "sort_index": 0,
        }
    if detected_fmt == InputFormat.EDF:
        return {
            "artifact_id": "source_edf",
            "variant_id": None,
            "artifact_kind": "source",
            "format_family": "edf",
            "reader_kind": "edf",
            "path": Path(input_path),
            "display_label": "source_edf",
            "sort_index": 0,
        }
    if detected_fmt == InputFormat.ERD:
        return {
            "artifact_id": "source_erd",
            "variant_id": None,
            "artifact_kind": "source",
            "format_family": "erd",
            "reader_kind": "erd",
            "path": Path(input_path),
            "display_label": "source_erd",
            "sort_index": 0,
        }
    return None


def _canonical_target(canonical_pq: Path, cfg: dict) -> dict:
    canonical_cfg = get_canonical_parquet_cfg(cfg)
    canonical_id = str(canonical_cfg["id"])
    return {
        "artifact_id": canonical_id,
        "variant_id": canonical_id,
        "artifact_kind": "canonical",
        "format_family": "parquet",
        "reader_kind": "parquet",
        "path": Path(canonical_pq),
        "display_label": canonical_id,
        "sort_index": 0,
    }


def _artifact_exists(path: Path | None) -> bool:
    if path is None:
        return False
    return path.is_file() or (path.is_dir() and any(path.glob("*.parquet")))


def _study_output_base(cache_dir: Path, study_name: str) -> Path:
    return cache_dir / f"{study_name[:30]}_variants"


def _best_effort_local_input(input_value: str, cache_dir: Path) -> Path | None:
    local_path = Path(input_value).expanduser()
    if local_path.exists():
        return local_path

    cache_leaf = Path(str(input_value).rstrip("/")).name
    cached_remote = cache_dir / cache_leaf
    if cache_leaf and cached_remote.exists():
        return cached_remote

    return None


def _best_effort_format(input_value: str, local_input: Path | None) -> str | None:
    if local_input is not None:
        try:
            return _detect_format(local_input)
        except Exception:
            pass

    suffix = Path(input_value).suffix.lower()
    if suffix in {".h5", ".hdf5", ".he5"}:
        return InputFormat.HDF5
    if suffix == ".edf":
        return InputFormat.EDF
    if suffix == ".parquet":
        return InputFormat.PARQUET
    return None


def _best_effort_sample_freq(study_cfg: dict, fmt: str | None, local_input: Path | None) -> float | None:
    if study_cfg.get("sample_freq") is not None:
        return float(study_cfg["sample_freq"])
    if fmt in {InputFormat.HDF5, InputFormat.EDF} and local_input is not None:
        try:
            return float(_recover_sample_freq(local_input, fmt))
        except Exception:
            return None
    return None


def _root_variant_output_path(output_base: Path, spec: dict) -> Path:
    variant_id = spec["id"]
    fmt = spec["format"]
    if fmt == "parquet":
        token = _variant_spec_hash({
            "id": variant_id,
            "format": "parquet",
            "row_group_minutes": spec.get("row_group_minutes", 5),
            "compression": spec.get("compression", "lz4"),
        })
        return output_base / f"{_safe_id(variant_id)}_{token}.parquet"
    if fmt == "hdf5":
        token = _variant_spec_hash({
            "id": variant_id,
            "format": "hdf5",
            "layout": spec.get("layout", "columnar"),
            "chunk_minutes": spec.get("chunk_minutes", 5),
            "dtype": spec.get("dtype", "float32"),
            "compression": spec.get("compression", "lz4"),
        })
        return output_base / f"{_safe_id(variant_id)}_{token}.h5"
    if fmt == "edf":
        token = _variant_spec_hash({"id": variant_id, "format": "edf"})
        return output_base / f"{_safe_id(variant_id)}_{token}.edf"
    raise ValueError(f"Unsupported variant format for dry-run planning: {fmt}")


def _tuned_label(minutes: float) -> str:
    return f"{int(minutes * 60)}s" if minutes < 1 else f"{minutes}m"


def _planned_artifacts_for_study(cfg: dict, study_cfg: dict,
                                 selected: list[tuple[str, str, object]]) -> tuple[list[dict], list[str]]:
    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    input_value = _study_input_value(study_cfg)
    local_input = _best_effort_local_input(input_value, cache_dir)
    fmt = _best_effort_format(input_value, local_input)
    sample_freq = _best_effort_sample_freq(study_cfg, fmt, local_input)
    canonical_cfg = get_canonical_parquet_cfg(cfg)
    output_base = _study_output_base(cache_dir, study_cfg["name"])
    entries: list[dict] = []

    canonical_path: Path | None = None
    if local_input is not None and fmt is not None and sample_freq is not None:
        canonical_path = _canonical_file(
            cache_dir,
            local_input,
            fmt,
            float(sample_freq),
            canonical_cfg,
            study_name=study_cfg["name"],
        )
        entries.append({
            "status": "cached" if _artifact_exists(canonical_path) else "would-create",
            "group": "canonical",
            "key": str(canonical_cfg["id"]),
            "path": canonical_path,
            "note": None,
        })
    else:
        reason = "local input path not available in dry-run"
        if local_input is not None and fmt is None:
            reason = "could not detect input format"
        elif local_input is not None and sample_freq is None:
            reason = "sample_freq is not known until runtime"
        entries.append({
            "status": "unknown",
            "group": "canonical",
            "key": str(canonical_cfg["id"]),
            "path": None,
            "note": reason,
        })

    for spec in cfg.get("variants", []):
        path = _root_variant_output_path(output_base, spec)
        entries.append({
            "status": "cached" if _artifact_exists(path) else "would-create",
            "group": "root_variant",
            "key": spec["id"],
            "path": path,
            "note": spec["format"],
        })

    selected_ids = {cat_id for cat_id, _, _ in selected}

    if Category.COMPRESSION in selected_ids and is_investigation_enabled(cfg, "compression"):
        for comp_cfg in get_parquet_compression_variants(cfg):
            codec = comp_cfg["codec"]
            level = comp_cfg.get("level")
            label = f"{codec}_{level}" if level else codec
            if codec == "snappy" and not level:
                entries.append({
                    "status": "reuses-canonical" if canonical_path is not None else "unknown",
                    "group": "compression",
                    "key": f"parquet_{label}",
                    "path": canonical_path,
                    "note": "no separate file; reuses canonical parquet",
                })
                continue
            path = output_base / f"parquet_{label}.parquet"
            entries.append({
                "status": "cached" if _artifact_exists(path) else "would-create",
                "group": "compression",
                "key": f"parquet_{label}",
                "path": path,
                "note": None,
            })

    if Category.INT32_STORAGE in selected_ids and is_investigation_enabled(cfg, "int32_storage"):
        for mode in ("int32_calibrated", "int32_nanovolt"):
            for codec in ("zstd", "snappy", "none"):
                key = f"parquet_{mode}_{codec}"
                path = output_base / f"{key}.parquet"
                entries.append({
                    "status": "cached" if _artifact_exists(path) else "would-create",
                    "group": "int32_storage",
                    "key": key,
                    "path": path,
                    "note": None,
                })

    if Category.TUNED_COMPARISON in selected_ids:
        for minutes in get_tuned_block_sizes_minutes(cfg):
            label = _tuned_label(minutes)
            for codec in get_tuned_parquet_codecs(cfg):
                key = tuned_parquet_key(codec, label)
                path = output_base / f"{key}.parquet"
                entries.append({
                    "status": "cached" if _artifact_exists(path) else "would-create",
                    "group": "tuned_parquet",
                    "key": key,
                    "path": path,
                    "note": None,
                })

            h5_key = f"tuned_h5_{label}"
            h5_path = output_base / f"{h5_key}.h5"
            entries.append({
                "status": "cached" if _artifact_exists(h5_path) else "would-create",
                "group": "tuned_hdf5",
                "key": h5_key,
                "path": h5_path,
                "note": f"compression={get_tuned_hdf5_compression(cfg)}",
            })

    runtime_only = []
    if any(cat in selected_ids for cat in {
        Category.RANDOM_ACCESS,
        Category.CHANNEL_SUBSET,
        Category.REMONTAGE,
        Category.FILTER_PIPELINE,
        Category.WINDOW_SCALING,
    }):
        runtime_only.append("core benchmarks A-E reuse canonical/root variant inputs; no extra cache artifacts")
    if Category.PRECISION_LOSS in selected_ids:
        runtime_only.append("precision_loss reuses the default Parquet artifact; no extra cache artifacts")
    if Category.REMOTE_QUERY in selected_ids:
        runtime_only.append("remote_query does not pre-generate local benchmark variants")
    if Category.BASELINE_COMPARISON in selected_ids:
        runtime_only.append("baseline_comparison reuses the resolved study input artifact; no extra cache artifacts")

    return entries, runtime_only


def _print_dry_run(cfg: dict, args: argparse.Namespace, selected: list[tuple[str, str, object]]) -> None:
    print("\n=== DRY RUN ===")
    print(f"Config: {args.config}")
    print(f"Cache dir: {Path(cfg.get('cache_dir', '.benchmark_cache'))}")
    print("\nStudies:")
    for study in cfg.get("studies", []):
        print(f"  - {study['name']} (input: {_study_input_value(study)})")
    print(f"\nSelected benchmarks ({len(selected)}):")
    for cat_id, cat_name, _ in selected:
        print(f"  - [{cat_id}] {cat_name}")
        core_cfg = cfg.get("benchmarks", {}).get("core", {}).get(cat_id, {})
        if isinstance(core_cfg, dict) and "variants" in core_cfg:
            print(f"      variants={core_cfg['variants']}")
            if core_cfg.get("include_canonical", False):
                print("      include_canonical=true")

    selected_ids = {cat_id for cat_id, _, _ in selected}
    if any(cat in selected_ids for cat in (
        Category.COMPRESSION,
        Category.PRECISION_LOSS,
        Category.INT32_STORAGE,
        Category.REMOTE_QUERY,
    )):
        print("\nParquet investigations config:")
        if Category.COMPRESSION in selected_ids:
            variants = get_parquet_compression_variants(cfg)
            labels = [
                v["codec"] + (f" level={v['level']}" if v.get("level") else "")
                for v in variants
            ]
            print(f"  - compression: enabled={is_investigation_enabled(cfg, 'compression')} variants={labels}")
        if Category.PRECISION_LOSS in selected_ids:
            print(f"  - precision_loss: enabled={is_investigation_enabled(cfg, 'precision_loss')}")
        if Category.INT32_STORAGE in selected_ids:
            print(f"  - int32_storage: enabled={is_investigation_enabled(cfg, 'int32_storage')}")
        if Category.REMOTE_QUERY in selected_ids:
            rq = get_remote_query_cfg(cfg)
            print(
                "  - remote_query: "
                f"enabled={is_investigation_enabled(cfg, 'remote_query')} "
                f"float32={rq.get('remote_float32_path', '(unset)')}"
            )

    if Category.TUNED_COMPARISON in selected_ids:
        print(
            "\nTuned comparison: "
            f"block_sizes_minutes={get_tuned_block_sizes_minutes(cfg)} "
            f"parquet_codecs={get_tuned_parquet_codecs(cfg)} "
            f"hdf5_compression={get_tuned_hdf5_compression(cfg)} "
            f"chunk_sec={get_tuned_chunk_sec(cfg)}"
        )
    if Category.BASELINE_COMPARISON in selected_ids:
        print(
            "\nBaseline format comparison: "
            "uses J-style workloads on the resolved study input artifact "
            f"with chunk_sec={get_tuned_chunk_sec(cfg)}"
        )

    print(f"\nWindow sizes: {cfg.get('window_sizes', [])}")
    print(f"Repetitions: {cfg.get('repetitions', 3)}")
    print(f"Canonical Parquet: {get_canonical_parquet_cfg(cfg)}")
    report_mode = "skip (--no-report)" if getattr(args, "no_report", False) else "auto-generate Markdown + HTML report"
    print(f"Report: {report_mode}")

    print("\nPlanned cache artifacts:")
    for study in cfg.get("studies", []):
        entries, runtime_only = _planned_artifacts_for_study(cfg, study, selected)
        print(f"  - {study['name']}")
        counts = {"cached": 0, "would-create": 0, "reuses-canonical": 0, "unknown": 0}
        for entry in entries:
            counts[entry["status"]] = counts.get(entry["status"], 0) + 1
            path_str = str(entry["path"]) if entry["path"] is not None else "(path unavailable)"
            note = f" ({entry['note']})" if entry.get("note") else ""
            print(
                f"      [{entry['status']}] {entry['group']}: {entry['key']} -> {path_str}{note}"
            )
        for note in runtime_only:
            print(f"      [info] {note}")
        print(
            "      summary: "
            f"cached={counts.get('cached', 0)} "
            f"would-create={counts.get('would-create', 0)} "
            f"reuses-canonical={counts.get('reuses-canonical', 0)} "
            f"unknown={counts.get('unknown', 0)}"
        )
    print(f"\nTotal benchmark runs: ~{_estimate_runs(cfg, selected)}")


def _save_results(output: dict, out_path: Path) -> None:
    """Atomically overwrite the results file with the current output dict."""
    tmp = out_path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    last_exc: PermissionError | None = None
    for attempt in range(_RESULT_SAVE_RETRIES):
        try:
            tmp.replace(out_path)
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt == _RESULT_SAVE_RETRIES - 1:
                break
            time.sleep(_RESULT_SAVE_RETRY_DELAY_SECONDS)

    raise PermissionError(
        f"Unable to replace results file '{out_path}' after {_RESULT_SAVE_RETRIES} attempts. "
        "Another process may be temporarily locking the file (editor preview, antivirus, "
        "indexer, etc.)."
    ) from last_exc


def run_benchmarks(cfg: dict, args: argparse.Namespace) -> None:
    cfg = normalize_config(cfg)
    validate_config(cfg)
    run_id = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    selected = _selected_benchmarks(cfg, args)
    if args.dry_run:
        _print_dry_run(cfg, args, selected)
        return

    out_path = Path(args.output) if args.output else Path("benchmark/results") / f"{run_id}_benchmark_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    output = {
        "run_id": run_id,
        "system": _system_info(),
        "config_file": str(args.config),
        "studies": [],
        "benchmarks": [],
    }

    for study_cfg in cfg.get("studies", []):
        print(f"\n{'=' * 60}\nStudy: {study_cfg['name']}\n{'=' * 60}")
        input_path = resolve_input_path(cfg, study_cfg, args)
        sample_freq = study_cfg.get("sample_freq")
        canonical_pq, detected_fmt, sample_freq = ingest(
            input_path,
            cache_dir,
            sample_freq,
            canonical_cfg=get_canonical_parquet_cfg(cfg),
            study_name=study_cfg["name"],
        )
        info = StudyInfo.from_parquet(canonical_pq, sample_freq=sample_freq)
        short_name = study_cfg["name"][:30]
        output_base = cache_dir / f"{short_name}_variants"
        variant_specs = cfg.get("variants", [])
        paths = generate_variants(canonical_pq, info, variant_specs, output_base)
        paths.update(_baseline_input_paths(input_path, detected_fmt))
        paths["__source_target__"] = _source_target(input_path, detected_fmt)
        paths["__canonical_target__"] = _canonical_target(canonical_pq, cfg)
        paths["__canonical_parquet__"] = canonical_pq
        source_type = detected_fmt
        study_dir = input_path

        selected_ids = {cat_id for cat_id, _, _ in selected}
        if not variant_specs and detected_fmt == InputFormat.ERD and any(
            cat_id in {
                Category.RANDOM_ACCESS,
                Category.CHANNEL_SUBSET,
                Category.REMONTAGE,
                Category.FILTER_PIPELINE,
                Category.WINDOW_SCALING,
            }
            and cfg.get("benchmarks", {}).get("core", {}).get(cat_id, {}).get("variants", "all") != []
            for cat_id in selected_ids
        ):
            raise ValueError(
                "Core source-direct benchmarking is not yet supported for ERD when root variants is empty. "
                "Declare benchmark variants or disable core benchmarks for this run."
            )
        if Category.COMPRESSION in selected_ids and paths.get(FormatKey.PARQUET) and is_investigation_enabled(cfg, "compression"):
            _setup_parquet_compression_variants(
                paths, paths[FormatKey.PARQUET], output_base, short_name, cfg)
        if Category.INT32_STORAGE in selected_ids and paths.get(FormatKey.PARQUET) and is_investigation_enabled(cfg, "int32_storage"):
            _setup_int32_variants(paths, output_base, short_name)
        if Category.TUNED_COMPARISON in selected_ids and paths.get(FormatKey.PARQUET):
            _setup_tuned_variants(paths, output_base, info, cfg)

        study_meta = {
            "name": study_cfg["name"],
            "source_type": source_type,
            "local_source": str(study_dir),
            "sample_freq": info.sample_freq,
            "channels": len(info.channel_labels),
            "start_stamp": info.start_stamp,
            "end_stamp": info.end_stamp,
            "total_stamps": info.total_rows,
            "duration_seconds": round(info.total_rows / info.sample_freq, 1),
            "segments": info.n_segments if hasattr(info, "n_segments") else len(info.segment_plans),
            "paths": {k: str(v) for k, v in paths.items() if isinstance(v, Path)},
        }
        output["studies"].append(study_meta)

        for _, cat_name, bench_fn in selected:
            print(f"\n-- {cat_name} --")
            results = bench_fn(info, paths, cfg, args) if bench_fn is bench_remote_query else bench_fn(info, paths, cfg)
            for result in results:
                result = dict(result)
                result["study"] = study_cfg["name"]
                output["benchmarks"].append(result)
                _print_result(result)
            _save_results(output, out_path)
            print(f"  [checkpoint -> {out_path}]")

    print(f"\nResults saved to {out_path}")
    if not getattr(args, "no_report", False):
        report_md, report_html = generate_report(out_path, html=True)
        print(f"Markdown report: {report_md}")
        if report_html is not None:
            print(f"HTML report: {report_html}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark EEG format operations.")
    parser.add_argument("--config", default="benchmark/config/default.yaml", help="Path to YAML config file")
    parser.add_argument("--categories", nargs="*", help="Benchmark categories to run")
    parser.add_argument("--output", default=None, help="Output JSON file path (default: benchmark/results/<run_id>_benchmark_results.json)")
    parser.add_argument("--dry-run", action="store_true", help="Print planned work without downloading or running benchmarks")
    parser.add_argument("--no-report", action="store_true", help="Skip the default post-run Markdown+HTML report generation")
    parser.add_argument("--sas-token", default=None, help="Azure Blob SAS token (optional). Overrides AZURE_STORAGE_SAS_TOKEN if provided")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_benchmarks(cfg, args)


if __name__ == "__main__":
    main()

