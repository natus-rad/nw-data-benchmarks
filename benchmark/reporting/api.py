from __future__ import annotations

import argparse
import json
from pathlib import Path
from string import Template
from typing import Any

from .common import (
    MISSING_SECTION_MESSAGE,
    ReportGenerationError,
    format_mib,
    format_rate,
    format_seconds,
    label,
    markdown_table,
    median_by_format,
    rows_for,
    rows_for_categories,
)
from .html import render_html
from .sections import (
    baseline_placeholder_specs,
    baseline_section_categories,
    render_baseline_comparison,
    render_tuned_comparison,
    report_section_specs,
    tuned_placeholder_specs,
    tuned_section_categories,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO_ROOT / "benchmark" / "results"
TEMPLATE_PATH = REPO_ROOT / "benchmark" / "docs" / "benchmark_report.template.md"
DEFAULT_OUTPUT = REPO_ROOT / "benchmark" / "docs" / "benchmark_report.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate benchmark/docs/benchmark_report.md from a benchmark result JSON file."
    )
    parser.add_argument("--input", type=Path, help="Path to a benchmark result JSON file. Defaults to the latest file in benchmark/results/.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output Markdown path. Defaults to benchmark/docs/benchmark_report.md.")
    parser.add_argument("--template", type=Path, default=TEMPLATE_PATH, help="Markdown template path. Defaults to benchmark/docs/benchmark_report.template.md.")
    parser.add_argument("--html", action="store_true", help="Also emit a self-contained HTML report next to the Markdown output.")
    return parser.parse_args()


def latest_results_file(results_dir: Path) -> Path:
    files = list(results_dir.glob("*_benchmark_results.json"))
    if not files:
        raise ReportGenerationError(f"No benchmark result JSON files found in {results_dir.as_posix()}.")
    return max(files, key=lambda path: (path.stat().st_mtime, path.name))


def load_results(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ReportGenerationError(f"Unable to read results file {path}: {exc}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ReportGenerationError(f"Invalid JSON in {path}: {exc}") from exc
    validate_results(payload, path)
    return payload


def validate_results(payload: dict[str, Any], path: Path) -> None:
    required_top_level = ["run_id", "system", "studies", "benchmarks"]
    missing = [key for key in required_top_level if key not in payload]
    if missing:
        raise ReportGenerationError(f"Results file {path} is missing required top-level field(s): {', '.join(missing)}.")
    if not isinstance(payload["studies"], list) or not payload["studies"]:
        raise ReportGenerationError(f"Results file {path} has no study metadata in 'studies'.")
    if not isinstance(payload["benchmarks"], list) or not payload["benchmarks"]:
        raise ReportGenerationError(f"Results file {path} has no benchmark rows in 'benchmarks'.")
    study = payload["studies"][0]
    study_required = ["name", "channels", "sample_freq", "total_stamps", "duration_seconds"]
    missing = [key for key in study_required if key not in study]
    if missing:
        raise ReportGenerationError(
            f"Results file {path} is missing required study field(s): {', '.join(missing)}. "
            "The benchmark runner must emit explicit total_stamps; the report generator will not infer it from duration_seconds."
        )


def render_report(payload: dict[str, Any], template_text: str, source_path: Path) -> str:
    try:
        study = payload["studies"][0]
        benchmarks = payload["benchmarks"]
        categories = {row.get("category") for row in benchmarks if row.get("category")}
        template = Template(template_text)
        return template.substitute(
            run_id=payload["run_id"],
            source_file=source_path.name,
            benchmark_count=str(len(benchmarks)),
            category_count=str(len(categories)),
            overview=build_overview(study, payload["system"], categories),
            summary=build_summary(payload),
            key_observations=build_key_observations(payload),
            sections=build_sections(payload),
            **build_section_placeholders(payload),
        ).rstrip() + "\n"
    except KeyError as exc:
        raise ReportGenerationError(f"Results file {source_path} is missing required field '{exc.args[0]}' needed to render the report.") from exc
    except (TypeError, ValueError) as exc:
        raise ReportGenerationError(f"Results file {source_path} contains data that could not be interpreted for report generation: {exc}") from exc


def build_overview(study: dict[str, Any], system: dict[str, Any], categories: set[str]) -> str:
    duration_hours = study["duration_seconds"] / 3600.0
    rows = [
        ["Study", str(study["name"])],
        ["Channels", str(study["channels"])],
        ["Sample rate", f"{study['sample_freq']:.1f} Hz"],
        ["Duration", f"{duration_hours:.2f} h ({study['total_stamps']:,} samples)"],
        ["System", f"{system.get('os', 'unknown')} / Python {system.get('python', 'unknown')} / {system.get('cpu_count', '?')} CPU threads / {system.get('ram_gb', '?')} GiB RAM"],
        ["Categories present", ", ".join(sorted(categories))],
    ]
    note = (
        "The report is generated directly from benchmark result JSON. "
        "Sections for categories not present in the input file are called out explicitly, so partial benchmark runs still produce a readable report. "
        "All reported throughput values use the theoretical decoded float32 payload size: rows × channels × 4 bytes. "
        "For repeated-read benchmarks, `wall_clock_seconds` is the warm-cache-leaning median across repetitions and `first_wall_clock_seconds` records the first repetition as the closest available cold-start proxy without explicit OS cache eviction. "
        "When available, `peak_rss_mib` reports sampled peak process resident memory during the benchmark invocation. "
        "HDF5 timings in this benchmark use a custom benchmark-specific `chunk_index` lookup structure built at conversion time, which intentionally gives HDF5 a best-case seek/read path rather than representing plain generic HDF5 without that helper."
    )
    return note + "\n\n" + markdown_table(["Property", "Value"], rows)


def build_summary(payload: dict[str, Any]) -> str:
    rows = []
    random_rows = rows_for(payload, "random_access")
    if random_rows:
        medians = median_by_format(random_rows, "wall_clock_seconds")
        best_format, best_value = min(medians.items(), key=lambda item: item[1])
        rows.append(["Random access (warm-cache-leaning median 1-minute read)", label(best_format), format_seconds(best_value)])
    subset_rows = [row for row in rows_for(payload, "channel_subset") if str(row.get("channels")) == "4"]
    if subset_rows:
        best = min(subset_rows, key=lambda row: row["wall_clock_seconds"])
        rows.append(["4-channel subset", label(best["format"]), format_seconds(best["wall_clock_seconds"])])
    pipeline_rows = rows_for(payload, "filter_pipeline_full")
    if pipeline_rows:
        best = min(pipeline_rows, key=lambda row: row["wall_clock_seconds"])
        rows.append(["Full-study filter pipeline", label(best["format"]), format_seconds(best["wall_clock_seconds"])])
    scaling_rows = rows_for(payload, "window_scaling")
    if scaling_rows:
        best = max(scaling_rows, key=lambda row: row["mib_per_sec"])
        rows.append(["Peak window-scaling throughput", f"{label(best['format'])} @ {best['window_seconds']}s", format_rate(best["mib_per_sec"])])
    tuned_rows = rows_for(payload, "tuned_full_study")
    if tuned_rows:
        best = min(tuned_rows, key=lambda row: row["wall_clock_seconds"])
        rows.append(["Best tuned full-study read", f"{label(best['format'])} ({best['block_size']})", format_seconds(best["wall_clock_seconds"])])
    if not rows:
        return "*No summary metrics could be derived from the input file.*"
    return markdown_table(["Area", "Winner", "Result"], rows)


def build_key_observations(payload: dict[str, Any]) -> str:
    bullets: list[str] = []
    random_rows = rows_for(payload, "random_access")
    if len({row.get('format') for row in random_rows}) >= 2:
        medians = median_by_format(random_rows, "wall_clock_seconds")
        ranking = sorted(medians.items(), key=lambda item: item[1])
        (best_fmt, best_val), (second_fmt, second_val) = ranking[:2]
        bullets.append(f"**Random access:** {label(best_fmt)} has the lowest warm-cache-leaning median 1-minute read time at {format_seconds(best_val)}, about {second_val / best_val:.2f}× faster than {label(second_fmt)}.")
    compression_rows = rows_for(payload, "compression")
    if compression_rows:
        smallest = min(compression_rows, key=lambda row: row["file_size_bytes"])
        fastest = min(compression_rows, key=lambda row: row["wall_clock_seconds"])
        bullets.append(f"**Compression trade-off:** smallest Parquet artifact is {smallest['codec']} at {format_mib(smallest['file_size_mib'])}, while the fastest warm-cache 1-minute read is {fastest['codec']} at {format_seconds(fastest['wall_clock_seconds'])}.")
    int32_rows = rows_for(payload, "int32_storage")
    if int32_rows:
        smallest = min(int32_rows, key=lambda row: row["file_size_bytes"])
        bullets.append(f"**Int32 variants:** the most compact measured variant is {smallest['mode']} ({smallest.get('codec', 'n/a')}) at {format_mib(smallest['file_size_mib'])}; its reported SNR vs float32 is {smallest['snr_vs_float32_db']} dB.")
    remote_rows = rows_for(payload, "remote_query")
    if remote_rows:
        best = min(remote_rows, key=lambda row: row["total_wall_seconds"])
        bullets.append(f"**Remote access:** the fastest remote query path in this run is {best['method']} for {best['channel_subset']} at {format_seconds(best['total_wall_seconds'])} total over {best['n_windows']} windows.")
    if not bullets:
        return "- No cross-category observations available for this result file."
    return "\n".join(f"- {bullet}" for bullet in bullets)


def build_sections(payload: dict[str, Any]) -> str:
    sections = [section(spec["title"], spec["rows_getter"](payload), spec["renderer"], payload) for spec in report_section_specs()]
    sections.append(section("J. Tuned Format Comparison", rows_for_categories(payload, tuned_section_categories()), lambda rows, p: render_tuned_comparison(p), payload))
    sections.append(section("K. Baseline Format Comparison", rows_for_categories(payload, baseline_section_categories()), lambda rows, p: render_baseline_comparison(p), payload))
    return "\n\n".join(sections)


def build_section_placeholders(payload: dict[str, Any]) -> dict[str, str]:
    placeholders = {spec["placeholder"]: render_section_results(spec["rows_getter"](payload), spec["renderer"], payload) for spec in report_section_specs() + tuned_placeholder_specs() + baseline_placeholder_specs()}
    full_rows = rows_for(payload, "tuned_full_study")
    placeholders["j_notes"] = "\n\nPer-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons." if full_rows else ""
    baseline_rows = rows_for_categories(payload, baseline_section_categories())
    placeholders["k_notes"] = "\n\nBenchmark K runs the Benchmark J workload family on the resolved baseline input artifact(s) without generating tuned comparison variants." if baseline_rows else ""
    return placeholders


def render_section_results(rows: list[dict[str, Any]], renderer, payload: dict[str, Any]) -> str:
    if not rows:
        return MISSING_SECTION_MESSAGE
    return renderer(rows, payload)


def section(title: str, rows: list[dict[str, Any]], renderer, payload: dict[str, Any]) -> str:
    if not rows:
        return f"## {title}\n\n{MISSING_SECTION_MESSAGE}"
    return f"## {title}\n\n{renderer(rows, payload)}"


def generate_report(input_path: Path, output_path: Path = DEFAULT_OUTPUT,
                    template_path: Path = TEMPLATE_PATH, html: bool = False) -> tuple[Path, Path | None]:
    input_path = input_path.resolve()
    payload = load_results(input_path)
    try:
        template_text = template_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ReportGenerationError(f"Unable to read template file {template_path}: {exc}") from exc
    rendered = render_report(payload, template_text, input_path)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    html_path = None
    if html:
        html_path = output_path.with_suffix(".html")
        html_path.write_text(render_html(rendered), encoding="utf-8")
    return output_path, html_path


def main() -> int:
    args = parse_args()
    try:
        input_path = args.input.resolve() if args.input else latest_results_file(RESULTS_DIR)
        output_path, html_path = generate_report(input_path, output_path=args.output, template_path=args.template, html=args.html)
        print(f"Markdown report: {output_path}")
        if html_path is not None:
            print(f"HTML report: {html_path}")
    except ReportGenerationError as exc:
        print(f"Error: {exc}")
        return 1
    return 0


__all__ = ["DEFAULT_OUTPUT", "REPO_ROOT", "RESULTS_DIR", "TEMPLATE_PATH", "ReportGenerationError", "build_key_observations", "build_overview", "build_section_placeholders", "build_sections", "build_summary", "generate_report", "latest_results_file", "load_results", "main", "parse_args", "render_report", "render_section_results", "section", "validate_results"]
