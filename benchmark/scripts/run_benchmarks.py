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

from benchmark.core.azure_storage import download_study
from benchmark.core.bench_utils import _estimate_runs, _print_result
from benchmark.core.benchmarks import BENCHMARKS
from benchmark.core.remote import bench_remote_query
from benchmark.core.setup import _setup_h5_variants, _setup_tuned_variants, setup_study
from benchmark.core.study_info import _study_info, _system_info, load_config


def _selected_benchmarks(cfg: dict, args: argparse.Namespace) -> list[tuple[str, str, object]]:
    categories = args.categories if args.categories else cfg.get("benchmarks", list(BENCHMARKS.keys()))
    selected = []
    for cat in categories:
        if cat in BENCHMARKS:
            selected.append((cat, *BENCHMARKS[cat]))
        else:
            print(f"  [warn] Unknown benchmark category: {cat}")
    return selected


def _print_dry_run(cfg: dict, args: argparse.Namespace, selected: list[tuple[str, str, object]]) -> None:
    print("\n=== DRY RUN ===")
    print(f"Config: {args.config}")
    print(f"Cache dir: {Path(cfg.get('cache_dir', '.benchmark_cache'))}")
    print("\nStudies:")
    for study in cfg.get("studies", []):
        src = study.get("source", "parquet")
        path = study.get("remote_parquet_url") or study.get("blob_prefix", "")
        print(f"  - {study['name']} (source: {src}): {path}")
    print(f"\nBenchmarks ({len(selected)}):")
    for cat_id, cat_name, _ in selected:
        print(f"  - [{cat_id}] {cat_name}")
    print(f"\nCompression variants ({len(cfg.get('parquet_compression', []))}):")
    for comp in cfg.get("parquet_compression", []):
        label = comp["codec"] + (f" level={comp['level']}" if comp.get("level") else "")
        print(f"  - {label}")
    print(f"\nWindow sizes: {cfg.get('window_sizes', [])}")
    print(f"Repetitions: {cfg.get('repetitions', 3)}")
    print(f"\nTotal benchmark runs: ~{_estimate_runs(cfg, selected)}")


def run_benchmarks(cfg: dict, args: argparse.Namespace) -> dict:
    run_id = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    selected = _selected_benchmarks(cfg, args)
    if args.dry_run:
        _print_dry_run(cfg, args, selected)
        return {}

    output = {
        "run_id": run_id,
        "system": _system_info(),
        "config_file": str(args.config),
        "studies": [],
        "benchmarks": [],
    }

    for study_cfg in cfg.get("studies", []):
        print(f"\n{'=' * 60}\nStudy: {study_cfg['name']}\n{'=' * 60}")
        source_type = study_cfg.get("source", "parquet")
        study_dir = download_study(cfg, study_cfg, args)
        paths = setup_study(study_dir, cfg, cache_dir, source_type=source_type, study_cfg=study_cfg)
        info = _study_info(study_dir if source_type == "erd" else paths.get("parquet", study_dir), source_type=source_type, study_cfg=study_cfg)

        raw_name = study_dir.name
        short_name = raw_name[:40] if len(raw_name) > 40 else raw_name
        output_base = cache_dir / f"{short_name}_exports"
        if paths.get("parquet"):
            _setup_h5_variants(paths, output_base, short_name, info)
            if "tuned_comparison" in {cat_id for cat_id, _, _ in selected}:
                _setup_tuned_variants(paths, output_base, info, cfg)

        study_meta = {
            "name": study_cfg["name"],
            "source_type": source_type,
            "local_source": str(study_dir),
            "sample_freq": info.sample_freq,
            "channels": len(info.channel_labels),
            "duration_seconds": round((info.end_stamp - info.start_stamp + 1) / info.sample_freq, 1),
            "paths": {k: str(v) for k, v in paths.items()},
        }
        output["studies"].append(study_meta)

        for cat_id, cat_name, bench_fn in selected:
            print(f"\n-- {cat_name} --")
            results = bench_fn(info, paths, cfg, args) if bench_fn is bench_remote_query else bench_fn(info, paths, cfg)
            for result in results:
                result = dict(result)
                result["study"] = study_cfg["name"]
                output["benchmarks"].append(result)
                _print_result(result)

    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark EEG format operations.")
    parser.add_argument("--config", default="benchmark/config/default.yaml", help="Path to YAML config file")
    parser.add_argument("--categories", nargs="*", help="Benchmark categories to run")
    parser.add_argument("--output", default=None, help="Output JSON file path (default: benchmark/results/<run_id>.json)")
    parser.add_argument("--dry-run", action="store_true", help="Print planned work without downloading or running benchmarks")
    parser.add_argument("--sas-token", default=None, help="Azure Blob SAS token (optional). Overrides AZURE_STORAGE_SAS_TOKEN if provided")
    args = parser.parse_args()

    cfg = load_config(args.config)
    result = run_benchmarks(cfg, args)
    if args.dry_run:
        return

    out_path = Path(args.output) if args.output else Path("benchmark/results") / f"{result['run_id']}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved results to {out_path}")


if __name__ == "__main__":
    main()

