#!/usr/bin/env python3
"""EEG format benchmark CLI and orchestration entrypoint."""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark.core.azure_storage import resolve_input_path
from benchmark.core.bench_utils import _estimate_runs, _print_result
from benchmark.core.benchmarks import BENCHMARKS
from benchmark.core.constants import Category, FormatKey
from benchmark.core.ingest import ingest
from benchmark.core.remote import bench_remote_query
from benchmark.core.setup import (
    _setup_int32_variants,
    _setup_parquet_compression_variants, _setup_tuned_variants,
)
from benchmark.core.study_info import StudyInfo, _system_info, load_config
from benchmark.core.variants import generate_variants


def _selected_benchmarks(cfg: dict, args: argparse.Namespace) -> list[tuple[str, str, object]]:
    categories = args.categories if args.categories else cfg.get("benchmarks", list(BENCHMARKS.keys()))
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


def _print_dry_run(cfg: dict, args: argparse.Namespace, selected: list[tuple[str, str, object]]) -> None:
    print("\n=== DRY RUN ===")
    print(f"Config: {args.config}")
    print(f"Cache dir: {Path(cfg.get('cache_dir', '.benchmark_cache'))}")
    print("\nStudies:")
    for study in cfg.get("studies", []):
        print(f"  - {study['name']} (input: {_study_input_value(study)})")
    print(f"\nCore benchmarks ({len(selected)}):")
    for cat_id, cat_name, _ in selected:
        print(f"  - [{cat_id}] {cat_name}")

    # Parquet investigations config section
    pq_inv = cfg.get("parquet_investigations", {})
    if pq_inv.get("enabled"):
        print("\nParquet investigations (enabled):")
        if pq_inv.get("compression"):
            comp = pq_inv["compression"]
            codecs = comp.get("codecs", [])
            print(f"  - compression: codecs={codecs}")
        if pq_inv.get("precision_loss"):
            print("  - precision_loss: true")
        if pq_inv.get("int32_storage"):
            print("  - int32_storage: true")
        rq = pq_inv.get("remote_query", {})
        if rq.get("enabled"):
            print(f"  - remote_query: {rq.get('remote_url', '(no URL)')}")
    else:
        # Legacy flat list compression variants
        pq_comp = cfg.get("parquet_compression", [])
        if pq_comp:
            print(f"\nCompression variants ({len(pq_comp)}):")
            for comp in pq_comp:
                label = comp["codec"] + (f" level={comp['level']}" if comp.get("level") else "")
                print(f"  - {label}")

    # Tuned comparison config section
    tuned_cfg = cfg.get("tuned_comparison", {})
    if tuned_cfg.get("enabled"):
        sizes = tuned_cfg.get("block_sizes_minutes", [])
        print(f"\nTuned comparison (enabled): block_sizes_minutes={sizes}")

    print(f"\nWindow sizes: {cfg.get('window_sizes', [])}")
    print(f"Repetitions: {cfg.get('repetitions', 3)}")
    print(f"\nTotal benchmark runs: ~{_estimate_runs(cfg, selected)}")


def _save_results(output: dict, out_path: Path) -> None:
    """Atomically overwrite the results file with the current output dict."""
    tmp = out_path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(output, f, indent=2)
    tmp.replace(out_path)


def run_benchmarks(cfg: dict, args: argparse.Namespace) -> None:
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
        canonical_pq, detected_fmt, sample_freq = ingest(input_path, cache_dir, sample_freq)
        info = StudyInfo.from_parquet(canonical_pq, sample_freq=sample_freq)
        short_name = study_cfg["name"][:30]
        output_base = cache_dir / f"{short_name}_variants"
        variant_specs = cfg.get("variants", [])
        paths = generate_variants(canonical_pq, info, variant_specs, output_base)
        source_type = detected_fmt
        study_dir = input_path

        pq_inv = cfg.get("parquet_investigations", {})
        if pq_inv.get("enabled") and paths.get(FormatKey.PARQUET):
            if pq_inv.get("compression"):
                _setup_parquet_compression_variants(
                    paths, paths[FormatKey.PARQUET], output_base, short_name, cfg)
            if pq_inv.get("int32_storage"):
                _setup_int32_variants(paths, output_base, short_name)

        tuned_cfg = cfg.get("tuned_comparison", {})
        if tuned_cfg.get("enabled") and paths.get(FormatKey.PARQUET):
            _setup_tuned_variants(paths, output_base, info, cfg)

        selected_ids = {cat_id for cat_id, _, _ in selected}
        if not pq_inv.get("enabled"):
            if Category.COMPRESSION in selected_ids and paths.get(FormatKey.PARQUET):
                _setup_parquet_compression_variants(
                    paths, paths[FormatKey.PARQUET], output_base, short_name, cfg)
            if Category.INT32_STORAGE in selected_ids and paths.get(FormatKey.PARQUET):
                _setup_int32_variants(paths, output_base, short_name)
        if not tuned_cfg.get("enabled"):
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
            "paths": {k: str(v) for k, v in paths.items()},
        }
        output["studies"].append(study_meta)

        # Build the effective benchmark list.
        effective = list(selected)
        already = {cat_id for cat_id, _, _ in selected}
        if pq_inv.get("enabled"):
            _inv_map = {
                "compression": Category.COMPRESSION,
                "precision_loss": Category.PRECISION_LOSS,
                "int32_storage": Category.INT32_STORAGE,
            }
            for cfg_key, cat in _inv_map.items():
                if pq_inv.get(cfg_key) and cat not in already and cat in BENCHMARKS:
                    effective.append((cat, *BENCHMARKS[cat]))
                    already.add(cat)
            rq = pq_inv.get("remote_query", {})
            if rq.get("enabled") and Category.REMOTE_QUERY not in already:
                if Category.REMOTE_QUERY in BENCHMARKS:
                    effective.append((Category.REMOTE_QUERY, *BENCHMARKS[Category.REMOTE_QUERY]))
                    already.add(Category.REMOTE_QUERY)
        if tuned_cfg.get("enabled") and Category.TUNED_COMPARISON not in already:
            if Category.TUNED_COMPARISON in BENCHMARKS:
                effective.append((Category.TUNED_COMPARISON, *BENCHMARKS[Category.TUNED_COMPARISON]))

        for _, cat_name, bench_fn in effective:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark EEG format operations.")
    parser.add_argument("--config", default="benchmark/config/default.yaml", help="Path to YAML config file")
    parser.add_argument("--categories", nargs="*", help="Benchmark categories to run")
    parser.add_argument("--output", default=None, help="Output JSON file path (default: benchmark/results/<run_id>_benchmark_results.json)")
    parser.add_argument("--dry-run", action="store_true", help="Print planned work without downloading or running benchmarks")
    parser.add_argument("--sas-token", default=None, help="Azure Blob SAS token (optional). Overrides AZURE_STORAGE_SAS_TOKEN if provided")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_benchmarks(cfg, args)


if __name__ == "__main__":
    main()

