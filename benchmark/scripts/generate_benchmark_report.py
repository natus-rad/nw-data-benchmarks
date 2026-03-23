from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from pathlib import Path
from string import Template
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

RESULTS_DIR = REPO_ROOT / "benchmark" / "results"
TEMPLATE_PATH = REPO_ROOT / "benchmark" / "docs" / "benchmark_report.template.md"
DEFAULT_OUTPUT = REPO_ROOT / "benchmark" / "docs" / "benchmark_report.md"

from benchmark.core.constants import FORMAT_LABELS, FORMAT_ORDER


class ReportGenerationError(RuntimeError):
    """Raised when benchmark report generation cannot proceed."""


MISSING_SECTION_MESSAGE = "*This category was not present in the input results file.*"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate benchmark/docs/benchmark_report.md from a benchmark result JSON file."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Path to a benchmark result JSON file. Defaults to the latest file in benchmark/results/.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output Markdown path. Defaults to benchmark/docs/benchmark_report.md.",
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=TEMPLATE_PATH,
        help="Markdown template path. Defaults to benchmark/docs/benchmark_report.template.md.",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        default=False,
        help="Also generate an HTML report alongside the Markdown output.",
    )
    return parser.parse_args()


def latest_results_file(results_dir: Path) -> Path:
    files = list(results_dir.glob("*_benchmark_results.json"))
    if not files:
        raise ReportGenerationError(
            f"No benchmark result JSON files found in {results_dir.as_posix()}."
        )
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
        raise ReportGenerationError(
            f"Results file {path} is missing required top-level field(s): {', '.join(missing)}."
        )
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
        summary = build_summary(payload)
        observations = build_key_observations(payload)
        sections = build_sections(payload)
        section_placeholders = build_section_placeholders(payload)

        template = Template(template_text)
        return template.substitute(
            run_id=payload["run_id"],
            source_file=source_path.name,
            benchmark_count=str(len(benchmarks)),
            category_count=str(len(categories)),
            overview=build_overview(study, payload["system"], categories),
            summary=summary,
            key_observations=observations,
            sections=sections,
            **section_placeholders,
        ).rstrip() + "\n"
    except KeyError as exc:
        raise ReportGenerationError(
            f"Results file {source_path} is missing required field '{exc.args[0]}' needed to render the report."
        ) from exc
    except (TypeError, ValueError) as exc:
        raise ReportGenerationError(
            f"Results file {source_path} contains data that could not be interpreted for report generation: {exc}"
        ) from exc


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
    study = payload["studies"][0]
    rows = []

    random_rows = rows_for(payload, "random_access")
    if random_rows:
        medians = median_by_format(random_rows, "wall_clock_seconds")
        best_format, best_value = min(medians.items(), key=lambda item: item[1])
        rows.append(["Random access (warm median 1-minute read)", label(best_format), format_seconds(best_value)])

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
        rows.append([
            "Peak window-scaling throughput",
            f"{label(best['format'])} @ {best['window_seconds']}s",
            format_rate(best["mib_per_sec"]),
        ])

    tuned_rows = rows_for(payload, "tuned_full_study")
    if tuned_rows:
        best = min(tuned_rows, key=lambda row: row["wall_clock_seconds"])
        rows.append([
            "Best tuned full-study read",
            f"{label(best['format'])} ({best['block_size']})",
            format_seconds(best["wall_clock_seconds"]),
        ])

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
        bullets.append(
            f"**Random access:** {label(best_fmt)} has the lowest warm-median 1-minute read time at {format_seconds(best_val)}, about {second_val / best_val:.2f}× faster than {label(second_fmt)}."
        )

    compression_rows = rows_for(payload, "compression")
    if compression_rows:
        smallest = min(compression_rows, key=lambda row: row["file_size_bytes"])
        fastest = min(compression_rows, key=lambda row: row["wall_clock_seconds"])
        bullets.append(
            f"**Compression trade-off:** smallest Parquet artifact is {smallest['codec']} at {format_mib(smallest['file_size_mib'])}, while the fastest warm-cache 1-minute read is {fastest['codec']} at {format_seconds(fastest['wall_clock_seconds'])}."
        )

    int32_rows = rows_for(payload, "int32_storage")
    if int32_rows:
        smallest = min(int32_rows, key=lambda row: row["file_size_bytes"])
        bullets.append(
            f"**Int32 variants:** the most compact measured variant is {smallest['mode']} ({smallest.get('codec', 'n/a')}) at {format_mib(smallest['file_size_mib'])}; its reported SNR vs float32 is {smallest['snr_vs_float32_db']} dB."
        )

    remote_rows = rows_for(payload, "remote_query")
    if remote_rows:
        best = min(remote_rows, key=lambda row: row["total_wall_seconds"])
        bullets.append(
            f"**Remote access:** the fastest remote query path in this run is {best['method']} for {best['channel_subset']} at {format_seconds(best['total_wall_seconds'])} total over {best['n_windows']} windows."
        )

    if not bullets:
        return "- No cross-category observations available for this result file."
    return "\n".join(f"- {bullet}" for bullet in bullets)


def build_sections(payload: dict[str, Any]) -> str:
    sections = [
        section(spec["title"], spec["rows_getter"](payload), spec["renderer"], payload)
        for spec in report_section_specs()
    ]
    sections.append(
        section(
            "J. Tuned Format Comparison",
            rows_for_categories(payload, tuned_section_categories()),
            lambda rows, p: render_tuned_comparison(p),
            payload,
        )
    )
    sections.append(
        section(
            "K. Baseline Format Comparison",
            rows_for_categories(payload, baseline_section_categories()),
            lambda rows, p: render_baseline_comparison(p),
            payload,
        )
    )
    return "\n\n".join(sections)


def build_section_placeholders(payload: dict[str, Any]) -> dict[str, str]:
    placeholders = {
        spec["placeholder"]: render_section_results(spec["rows_getter"](payload), spec["renderer"], payload)
        for spec in report_section_specs() + tuned_placeholder_specs() + baseline_placeholder_specs()
    }
    full_rows = rows_for(payload, "tuned_full_study")
    placeholders["j_notes"] = (
        "\n\nPer-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons."
        if full_rows else ""
    )
    baseline_rows = rows_for_categories(payload, baseline_section_categories())
    placeholders["k_notes"] = (
        "\n\nBenchmark K runs the Benchmark J workload family on the resolved baseline input artifact(s) without generating tuned comparison variants."
        if baseline_rows else ""
    )
    return placeholders


def render_section_results(rows: list[dict[str, Any]], renderer, payload: dict[str, Any]) -> str:
    if not rows:
        return MISSING_SECTION_MESSAGE
    return renderer(rows, payload)


def section(title: str, rows: list[dict[str, Any]], renderer, payload: dict[str, Any]) -> str:
    if not rows:
        return f"## {title}\n\n{MISSING_SECTION_MESSAGE}"
    return f"## {title}\n\n{renderer(rows, payload)}"


def report_section_specs() -> list[dict[str, Any]]:
    return [
        section_spec("A. Random Access", "a_results", "random_access", render_random_access),
        section_spec("B. Channel Subset", "b_results", "channel_subset", render_channel_subset),
        section_spec("C. Re-montage", "c_results", "remontage", render_remontage),
        section_spec("D.1 Full-Study Filter Pipeline", "d1_results", "filter_pipeline_full", render_filter_pipeline),
        section_spec("D.2 Sliding FFT", "d2_results", "sliding_fft_full", render_sliding_fft),
        section_spec("E. Window Scaling", "e_results", "window_scaling", render_window_scaling),
        section_spec("F. Compression", "f_results", "compression", render_compression),
        section_spec("G. Precision Loss", "g_results", "precision_loss", render_precision_loss),
        section_spec("H. Int32 Storage", "h_results", "int32_storage", render_int32_storage),
        section_spec("I. Remote Query", "i_results", "remote_query", render_remote_query),
    ]


def tuned_placeholder_specs() -> list[dict[str, Any]]:
    return [
        section_spec(
            "J.1 Random Access",
            "j1_results",
            "tuned_random_access",
            lambda rows, _: pivot_table(rows, "block_size", "format", "wall_clock_seconds", "time"),
        ),
        section_spec(
            "J.2 Channel Subset",
            "j2_results",
            "tuned_channel_subset",
            lambda rows, _: pivot_table(rows, "block_size", "format", "wall_clock_seconds", "time"),
        ),
        section_spec(
            "J.3 Throughput vs Window Size",
            "j3_results",
            "tuned_window_scaling",
            lambda rows, _: tuned_window_scaling_table(rows),
        ),
        section_spec(
            "J.4 Full-Study Sequential Read",
            "j4_results",
            "tuned_full_study",
            lambda rows, _: pivot_table(rows, "block_size", "format", "wall_clock_seconds", "time"),
        ),
    ]


def tuned_section_categories() -> list[str]:
    return [spec["category"] for spec in tuned_placeholder_specs()]


def baseline_placeholder_specs() -> list[dict[str, Any]]:
    return [
        section_spec(
            "K.1 Random Access",
            "k1_results",
            "baseline_random_access",
            lambda rows, _: pivot_table(
                rows, "artifact", "format", "wall_clock_seconds", "time_rate",
                row_header="Artifact", row_sort_key=lambda value: str(value),
            ),
        ),
        section_spec(
            "K.2 Channel Subset",
            "k2_results",
            "baseline_channel_subset",
            lambda rows, _: pivot_table(
                rows, "artifact", "format", "wall_clock_seconds", "time_rate",
                row_header="Artifact", row_sort_key=lambda value: str(value),
            ),
        ),
        section_spec(
            "K.3 Throughput vs Window Size",
            "k3_results",
            "baseline_window_scaling",
            lambda rows, _: comparison_window_scaling_table(rows),
        ),
        section_spec(
            "K.4 Full-Study Sequential Read",
            "k4_results",
            "baseline_full_study",
            lambda rows, _: pivot_table(
                rows, "artifact", "format", "wall_clock_seconds", "time_rate",
                row_header="Artifact", row_sort_key=lambda value: str(value),
            ),
        ),
    ]


def baseline_section_categories() -> list[str]:
    return [spec["category"] for spec in baseline_placeholder_specs()]


def section_spec(title: str, placeholder: str, category: str, renderer) -> dict[str, Any]:
    return {
        "title": title,
        "placeholder": placeholder,
        "category": category,
        "renderer": renderer,
        "rows_getter": lambda payload, category=category: rows_for(payload, category),
    }


def render_random_access(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    formats = formats_in_rows(rows)
    positions = sorted({row["position"] for row in rows}, key=percent_sort_key)
    table_rows = []
    for position in positions:
        pivot = {row["format"]: row for row in rows if row["position"] == position}
        cells = [position]
        for fmt in formats:
            row = pivot.get(fmt)
            cells.append(metric_cell(row, "wall_clock_seconds", "mib_per_sec") if row else "—")
        table_rows.append(cells)
    medians = median_by_format(rows, "wall_clock_seconds")
    best_fmt, best_val = min(medians.items(), key=lambda item: item[1])
    note = f"{label(best_fmt)} has the lowest warm-median 1-minute read time across read positions at {format_seconds(best_val)}."
    return note + "\n\n" + markdown_table(["Position", *[label(fmt) for fmt in formats]], table_rows)


def render_channel_subset(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    formats = formats_in_rows(rows)
    subsets = sorted({str(row["channels"]) for row in rows}, key=subset_sort_key)
    table_rows = []
    notes = []
    for subset in subsets:
        pivot = {row["format"]: row for row in rows if str(row["channels"]) == subset}
        best = min(pivot.values(), key=lambda row: row["wall_clock_seconds"])
        notes.append(f"{subset} channels → {label(best['format'])} is fastest at {format_seconds(best['wall_clock_seconds'])}.")
        table_rows.append([
            subset,
            *[metric_cell(pivot.get(fmt), "wall_clock_seconds", "mib_per_sec") if pivot.get(fmt) else "—" for fmt in formats],
        ])
    return " ".join(notes) + "\n\n" + markdown_table(["Channels", *[label(fmt) for fmt in formats]], table_rows)


def render_remontage(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    ordered = sorted(rows, key=lambda row: row["wall_clock_seconds"])
    avg_share = statistics.mean(row["montage_seconds"] / row["wall_clock_seconds"] for row in ordered)
    table = markdown_table(
        ["Format", "Read", "Montage", "Total", "Montage share"],
        [
            [
                label(row["format"]),
                format_seconds(row["read_seconds"]),
                format_seconds(row["montage_seconds"]),
                timing_cell(row, "wall_clock_seconds"),
                f"{100 * row['montage_seconds'] / row['wall_clock_seconds']:.1f}%",
            ]
            for row in ordered
        ],
    )
    return f"Montage is a relatively small fraction of end-to-end time in this benchmark (average {avg_share * 100:.1f}% of total wall time).\n\n{table}"


def render_filter_pipeline(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    ordered = sorted(rows, key=lambda row: row["wall_clock_seconds"])
    table = markdown_table(
        ["Format", "Read", "Montage", "Filter", "Total", "Throughput"],
        [
            [
                label(row["format"]),
                format_seconds(row["read_seconds"]),
                format_seconds(row["montage_seconds"]),
                format_seconds(row["filter_seconds"]),
                timing_cell(row, "wall_clock_seconds"),
                format_rate(row["mib_per_sec"]),
            ]
            for row in ordered
        ],
    )
    fastest = ordered[0]
    return f"For the full-study read → montage → filter pipeline, {label(fastest['format'])} is fastest at {format_seconds(fastest['wall_clock_seconds'])}.\n\n{table}"


def render_sliding_fft(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    ordered = sorted(rows, key=lambda row: row["wall_clock_seconds"])
    fft_windows = ordered[0].get("fft_windows_computed")
    table = markdown_table(
        ["Format", "Read", "Montage", "Filter", "FFT", "Total"],
        [
            [
                label(row["format"]),
                format_seconds(row["read_seconds"]),
                format_seconds(row["montage_seconds"]),
                format_seconds(row["filter_seconds"]),
                format_seconds(row["fft_seconds"]),
                timing_cell(row, "wall_clock_seconds"),
            ]
            for row in ordered
        ],
    )
    intro = f"This stage computed {fft_windows:,} overlapping FFT windows across the full study." if fft_windows else "Sliding FFT benchmark results."
    return intro + "\n\n" + table


def render_window_scaling(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    formats = formats_in_rows(rows)
    windows = sorted({int(row["window_seconds"]) for row in rows})
    table_rows = []
    for window in windows:
        pivot = {row["format"]: row for row in rows if int(row["window_seconds"]) == window}
        table_rows.append([f"{window}s", *[format_rate(pivot[fmt]["mib_per_sec"]) if fmt in pivot else "—" for fmt in formats]])
    best = max(rows, key=lambda row: row["mib_per_sec"])
    return (
        f"Best measured throughput is {format_rate(best['mib_per_sec'])} from {label(best['format'])} at a {best['window_seconds']}s window.\n\n"
        + markdown_table(["Window", *[label(fmt) for fmt in formats]], table_rows)
    )


def render_compression(rows: list[dict[str, Any]], payload: dict[str, Any]) -> str:
    study = payload["studies"][0]
    raw_mib = raw_float32_mib(study)
    ordered = sorted(rows, key=lambda row: row["file_size_bytes"])

    def _ratio_text(file_size_mib: float) -> str:
        return f"{raw_mib / file_size_mib:.2f}×" if file_size_mib > 0 else "n/a"

    table = markdown_table(
        ["Codec", "1-minute read", "Artifact size", "Ratio vs raw float32"],
        [
            [
                row["codec"],
                timing_cell(row, "wall_clock_seconds"),
                format_mib(row["file_size_mib"]),
                _ratio_text(row["file_size_mib"]),
            ]
            for row in ordered
        ],
    )
    smallest = ordered[0]
    fastest = min(rows, key=lambda row: row["wall_clock_seconds"])
    note = (
        f"Against a raw float32 baseline of {format_mib(raw_mib)}, the smallest Parquet artifact is {smallest['codec']} at {format_mib(smallest['file_size_mib'])}. "
        f"The fastest warm-cache 1-minute read is {fastest['codec']} at {format_seconds(fastest['wall_clock_seconds'])}."
    )
    return note + "\n\n" + table


def render_precision_loss(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    row = rows[0]
    channels = sorted(row.get("channels", []), key=lambda item: item["max_abs_error"], reverse=True)[:5]
    intro = (
        f"EDF round-trip quantization for a {row['window_seconds']}s window produced worst-case max absolute error {row['worst_max_abs_error']:.8f} µV "
        f"with average SNR {row['avg_snr_db']:.2f} dB across {row['num_channels']} channels."
    )
    if not channels:
        return intro
    table = markdown_table(
        ["Channel", "Max abs error (µV)", "RMS error (µV)", "SNR (dB)"],
        [
            [
                entry["channel"],
                f"{entry['max_abs_error']:.8f}",
                f"{entry['rms_error']:.8f}",
                f"{entry['snr_db']:.2f}",
            ]
            for entry in channels
        ],
    )
    return intro + "\n\nTop channels by max absolute error:\n\n" + table


def render_int32_storage(rows: list[dict[str, Any]], payload: dict[str, Any]) -> str:
    study = payload["studies"][0]
    raw_mib = raw_float32_mib(study)
    size_rows = unique_by(rows, lambda row: (row["mode"], row.get("codec")))
    size_rows = sorted(size_rows, key=lambda row: row["file_size_bytes"])

    def _ratio_text(file_size_mib: float) -> str:
        return f"{raw_mib / file_size_mib:.2f}×" if file_size_mib > 0 else "n/a"

    size_table = markdown_table(
        ["Mode", "Codec", "Artifact size", "Ratio vs raw float32", "SNR vs float32"],
        [
            [
                row["mode"],
                row.get("codec", "—"),
                format_mib(row["file_size_mib"]),
                _ratio_text(row["file_size_mib"]),
                str(row["snr_vs_float32_db"]),
            ]
            for row in size_rows
        ],
    )

    representative = []
    for mode in sorted({row["mode"] for row in rows}):
        mode_rows = [row for row in rows if row["mode"] == mode]
        preferred = [row for row in mode_rows if row.get("codec") in {"zstd", "zstd_3"}]
        pick = min(preferred or mode_rows, key=lambda row: row["wall_clock_seconds"])
        representative.append(pick)
    representative.sort(key=lambda row: row["wall_clock_seconds"])
    perf_table = markdown_table(
        ["Mode", "Read method", "Codec", "1-minute read", "Throughput"],
        [
            [
                row["mode"],
                row.get("read_method", "—"),
                row.get("codec", "—"),
                timing_cell(row, "wall_clock_seconds"),
                format_rate(row["mib_per_sec"]),
            ]
            for row in representative
        ],
    )
    smallest = size_rows[0]
    return (
        f"The most compact measured storage mode is {smallest['mode']} ({smallest.get('codec', '—')}) at {format_mib(smallest['file_size_mib'])}.\n\n"
        f"### H.1 Size / Precision\n\n{size_table}\n\n### H.2 Representative Read Performance\n\n{perf_table}"
    )


def render_remote_query(rows: list[dict[str, Any]], payload: dict[str, Any]) -> str:
    full_study_rows = rows_for(payload, "remote_query_full_study")

    def _summarize(values: list[Any], *, suffix: str = "") -> str:
        uniq = []
        for value in values:
            if value is None or value in uniq:
                continue
            uniq.append(value)
        if not uniq:
            return "not reported"
        return ", ".join(f"{value}{suffix}" for value in uniq)

    ordered = sorted(rows, key=lambda row: row["total_wall_seconds"])
    settings = (
        "Received settings: "
        f"n_random_points={_summarize([row.get('n_windows') for row in ordered])}; "
        f"window_sec={_summarize([row.get('window_seconds') for row in ordered], suffix='s')}; "
        f"full_study_chunk_sec={_summarize([row.get('chunk_seconds') for row in full_study_rows], suffix='s')}."
    )
    table = markdown_table(
        ["Method", "Format", "Channel subset", "Total time", "Avg/window", "Throughput"],
        [
            [
                row["method"],
                label(row["format"]),
                row["channel_subset"],
                timing_cell(row, "total_wall_seconds"),
                format_seconds(row["avg_wall_per_window"]),
                format_rate(row["mib_per_sec"]) if "mib_per_sec" in row else "—",
            ]
            for row in ordered
        ],
    )
    estimated = any(bool(row.get("download_estimated")) for row in rows)
    note = "EDF download time in this run is marked as estimated." if estimated else "All reported remote timings are direct measurements."
    return settings + "\n\n" + note + "\n\n" + table


def render_tuned_comparison(payload: dict[str, Any]) -> str:
    sections = ["This section compares matched block-size variants generated for Benchmark J."]
    random_rows = rows_for(payload, "tuned_random_access")
    if random_rows:
        sections.append("### J.1 Random Access\n\n" + pivot_table(random_rows, "block_size", "format", "wall_clock_seconds", "time"))
    subset_rows = rows_for(payload, "tuned_channel_subset")
    if subset_rows:
        sections.append("### J.2 Channel Subset\n\n" + pivot_table(subset_rows, "block_size", "format", "wall_clock_seconds", "time"))
    scaling_rows = rows_for(payload, "tuned_window_scaling")
    if scaling_rows:
        sections.append("### J.3 Throughput vs Window Size\n\n" + tuned_window_scaling_table(scaling_rows))
    full_rows = rows_for(payload, "tuned_full_study")
    if full_rows:
        sections.append("### J.4 Full-Study Sequential Read\n\n" + pivot_table(full_rows, "block_size", "format", "wall_clock_seconds", "time"))
        sections.append(
            "Per-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons."
        )
    return "\n\n".join(sections)


def render_baseline_comparison(payload: dict[str, Any]) -> str:
    sections = [
        "This section runs the Benchmark J workload suite on the resolved baseline input artifact(s) only, without generating tuned comparison variants. Reported MiB/s values use theoretical decoded float32 payload size = rows × channels × 4 bytes."
    ]
    random_rows = rows_for(payload, "baseline_random_access")
    if random_rows:
        sections.append(
            "### K.1 Random Access\n\n"
            + pivot_table(
                random_rows, "artifact", "format", "wall_clock_seconds", "time_rate",
                row_header="Artifact", row_sort_key=lambda value: str(value),
            )
        )
    subset_rows = rows_for(payload, "baseline_channel_subset")
    if subset_rows:
        sections.append(
            "### K.2 Channel Subset\n\n"
            + pivot_table(
                subset_rows, "artifact", "format", "wall_clock_seconds", "time_rate",
                row_header="Artifact", row_sort_key=lambda value: str(value),
            )
        )
    scaling_rows = rows_for(payload, "baseline_window_scaling")
    if scaling_rows:
        sections.append("### K.3 Throughput vs Window Size\n\n" + comparison_window_scaling_table(scaling_rows))
    full_rows = rows_for(payload, "baseline_full_study")
    if full_rows:
        sections.append(
            "### K.4 Full-Study Sequential Read\n\n"
            + pivot_table(
                full_rows, "artifact", "format", "wall_clock_seconds", "time_rate",
                row_header="Artifact", row_sort_key=lambda value: str(value),
            )
        )
    return "\n\n".join(sections)


def pivot_table(rows: list[dict[str, Any]], row_key: str, col_key: str, value_key: str,
                value_kind: str, row_header: str = "Block size", row_sort_key=None) -> str:
    columns = formats_in_rows(rows)
    if row_sort_key is None:
        row_sort_key = block_sort_key if row_key == "block_size" else lambda value: str(value)
    row_values = sorted({row[row_key] for row in rows}, key=row_sort_key)
    body = []
    for row_value in row_values:
        pivot = {row[col_key]: row for row in rows if row[row_key] == row_value}
        cells = [str(row_value)]
        for column in columns:
            row = pivot.get(column)
            if not row:
                cells.append("—")
            elif value_kind == "time":
                cells.append(timing_cell(row, value_key))
            elif value_kind == "time_rate":
                cells.append(metric_cell(row, value_key, "mib_per_sec"))
            else:
                cells.append(format_rate(row[value_key]))
        body.append(cells)
    return markdown_table([row_header, *[label(column) for column in columns]], body)


def comparison_window_scaling_table(rows: list[dict[str, Any]], qualifier_key: str | None = None,
                                    qualifier_label: str | None = None) -> str:
    columns = formats_in_rows(rows)
    window_sizes = sorted({row["window_seconds"] for row in rows})
    body = []
    for ws in window_sizes:
        ws_rows = [row for row in rows if row["window_seconds"] == ws]
        by_format = {fmt: [r for r in ws_rows if r["format"] == fmt] for fmt in columns}
        cells = [f"{ws}s"]
        for fmt in columns:
            fmt_rows = by_format.get(fmt, [])
            if not fmt_rows:
                cells.append("—")
                continue
            best = max(fmt_rows, key=lambda r: r["mib_per_sec"])
            if qualifier_key and qualifier_label:
                cells.append(f"{format_rate(best['mib_per_sec'])} ({best[qualifier_key]} {qualifier_label})")
            else:
                cells.append(format_rate(best["mib_per_sec"]))
        body.append(cells)
    if qualifier_key and qualifier_label:
        note = (
            f"Each cell shows the best throughput across all {qualifier_label}s tested for that "
            f"window × format combination, with the winning {qualifier_label} in parentheses."
        )
    else:
        note = "Each cell shows the measured throughput for the baseline input artifact(s) at that window size."
    return note + "\n\n" + markdown_table(["Window", *[label(c) for c in columns]], body)


def tuned_window_scaling_table(rows: list[dict[str, Any]]) -> str:
    return comparison_window_scaling_table(rows, qualifier_key="block_size", qualifier_label="block")


def rows_for(payload: dict[str, Any], category: str) -> list[dict[str, Any]]:
    return [row for row in payload["benchmarks"] if row.get("category") == category]


def rows_for_categories(payload: dict[str, Any], categories: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for category in categories:
        rows.extend(rows_for(payload, category))
    return rows


def formats_in_rows(rows: list[dict[str, Any]]) -> list[str]:
    formats = {row.get("format") for row in rows if row.get("format")}
    ordered = [fmt for fmt in FORMAT_ORDER if fmt in formats]
    order_by_format: dict[str, float] = {}
    for row in rows:
        fmt = row.get("format")
        if not fmt or fmt in FORMAT_ORDER:
            continue
        order = row.get("artifact_order")
        if fmt not in order_by_format or (isinstance(order, (int, float)) and order < order_by_format[fmt]):
            order_by_format[fmt] = float(order) if isinstance(order, (int, float)) else float("inf")
    extras = sorted(formats.difference(ordered), key=lambda fmt: (order_by_format.get(fmt, float("inf")), fmt))
    return ordered + extras


def median_by_format(rows: list[dict[str, Any]], key: str) -> dict[str, float]:
    result = {}
    for fmt in formats_in_rows(rows):
        values = [float(row[key]) for row in rows if row.get("format") == fmt]
        if values:
            result[fmt] = statistics.median(values)
    return result


def unique_by(rows: list[dict[str, Any]], key_func) -> list[dict[str, Any]]:
    seen = set()
    unique_rows = []
    for row in rows:
        marker = key_func(row)
        if marker in seen:
            continue
        seen.add(marker)
        unique_rows.append(row)
    return unique_rows


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    rendered_rows = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        rendered_rows.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(rendered_rows)


def metric_cell(row: dict[str, Any] | None, time_key: str, rate_key: str) -> str:
    if row is None:
        return "—"
    return f"{timing_cell(row, time_key)} / {format_rate(row[rate_key])}"


def timing_cell(row: dict[str, Any], time_key: str) -> str:
    text = format_seconds(row[time_key])
    notes = []
    if time_key == "wall_clock_seconds" and "first_wall_clock_seconds" in row:
        first = float(row["first_wall_clock_seconds"])
        if abs(first - float(row[time_key])) > 5e-7:
            notes.append(f"first {format_seconds(first)}")
    peak_rss_mib = row.get("peak_rss_mib")
    if peak_rss_mib is not None:
        notes.append(f"peak {format_mib(peak_rss_mib)}")
    if notes:
        text += f" ({'; '.join(notes)})"
    return text


def label(value: str) -> str:
    return FORMAT_LABELS.get(value, value)


def format_seconds(value: float) -> str:
    value = float(value)
    if value >= 100:
        return f"{value:.1f}s"
    if value >= 10:
        return f"{value:.2f}s"
    if value >= 1:
        return f"{value:.3f}s"
    return f"{value:.4f}s"


def format_rate(value: float) -> str:
    return f"{float(value):.1f} MiB/s"


def format_mib(value: float) -> str:
    return f"{float(value):,.1f} MiB"


def raw_float32_mib(study: dict[str, Any]) -> float:
    total_samples = int(study["total_stamps"])
    channels = int(study["channels"])
    raw_bytes = total_samples * ((channels * 4) + 8)
    return raw_bytes / (1024 * 1024)


def percent_sort_key(value: str) -> float:
    return float(str(value).rstrip("%"))


def subset_sort_key(value: str) -> tuple[int, str]:
    value = str(value)
    return (9999, value) if value == "all" else (int(value), value)


def block_sort_key(value: str) -> int:
    value = str(value).strip().lower()
    if value.endswith("m"):
        return int(value[:-1])
    return 10_000


_HTML_CSS = """
:root{
  --bg:#0f1117;--surface:#1a1d27;--surface2:#22263a;--border:#2e3250;
  --accent:#6c8ef5;--accent2:#a78bfa;--green:#34d399;
  --text:#e2e8f0;--muted:#94a3b8;--code-bg:#111827;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font:15px/1.75 'Segoe UI',system-ui,sans-serif}
.layout{display:grid;grid-template-columns:240px 1fr;max-width:1280px;margin:0 auto;min-height:100vh}
nav.toc{position:sticky;top:0;height:100vh;overflow-y:auto;padding:32px 18px;
  border-right:1px solid var(--border);background:var(--surface);font-size:.82rem}
nav.toc h2{color:var(--muted);font-size:.75rem;font-weight:700;letter-spacing:.1em;
  text-transform:uppercase;margin-bottom:14px}
nav.toc a{display:block;color:var(--muted);text-decoration:none;padding:3px 0;
  border-left:2px solid transparent;padding-left:10px;line-height:1.4}
nav.toc a.sub{color:var(--text);padding-left:22px;font-size:.78rem}
nav.toc a:hover{border-left-color:var(--accent);color:var(--accent)}
.page{padding:48px 52px 100px;max-width:900px}
h1{font-size:2rem;font-weight:700;color:var(--accent);margin:0 0 6px}
.subtitle{color:var(--muted);font-size:.88rem;margin-bottom:36px}
h2{font-size:1.3rem;font-weight:600;color:var(--accent2);
  margin:52px 0 14px;padding-bottom:8px;border-bottom:1px solid var(--border)}
h3{font-size:1rem;font-weight:600;color:#c4b5fd;margin:32px 0 10px}
p{margin:10px 0}
strong{color:#f1f5f9}
em{color:var(--muted);font-style:italic}
code{background:var(--code-bg);color:#7dd3fc;padding:1px 5px;border-radius:4px;
  font:12px/1.5 'Cascadia Code','Fira Code',monospace}
ul,ol{margin:10px 0 10px 28px}
li{margin:4px 0}
table{width:100%;border-collapse:collapse;margin:18px 0;font-size:.88rem}
th{background:var(--surface2);color:var(--accent);text-align:left;
  padding:9px 14px;border:1px solid var(--border);font-weight:600}
td{padding:8px 14px;border:1px solid var(--border);vertical-align:top;
  font-variant-numeric:tabular-nums}
tr:hover td{background:var(--surface2)}
td.win{color:var(--green);font-weight:600}
td.na{color:var(--muted);font-style:italic}
"""

_HTML_SCRIPT = """
const obs=new IntersectionObserver(es=>{es.forEach(e=>{
  const a=document.querySelector('nav.toc a[href="#'+e.target.id+'"]');
  if(a){a.style.borderLeftColor=e.isIntersecting?'var(--accent)':'transparent';
       a.style.color=e.isIntersecting?'var(--accent)':'';}
});},{rootMargin:'-10% 0px -80% 0px'});
document.querySelectorAll('h2[id],h3[id]').forEach(h=>obs.observe(h));
"""


def _html_esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _html_inline(s: str) -> str:
    s = _html_esc(s)
    s = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', s)
    s = re.sub(r'_(.+?)_', r'<em>\1</em>', s)
    s = re.sub(r'`(.+?)`', r'<code>\1</code>', s)
    return s


def _html_slug(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', s.lower()).strip('-')


def _html_table(lines: list[str]) -> str:
    rows = [[c.strip() for c in line.strip().strip('|').split('|')] for line in lines]
    if len(rows) < 2:
        return ""
    header, _, *body = rows
    html = ['<table><thead><tr>']
    for h in header:
        html.append(f'<th>{_html_inline(h)}</th>')
    html.append('</tr></thead><tbody>')
    for row in body:
        # Highest MiB/s in row = winner (green)
        tput_nums = []
        for i, cell in enumerate(row[1:], 1):
            m = re.search(r'^([\d.]+)\s*MiB/s', cell)
            if m:
                tput_nums.append((float(m.group(1)), i))
        # Lowest leading seconds (e.g. "0.045s" or "0.045s (first 0.090s)") = winner
        time_nums = []
        for i, cell in enumerate(row[1:], 1):
            m = re.search(r'^([\d.]+)s(?:\s|$)', cell.strip())
            if m:
                time_nums.append((float(m.group(1)), i))
        win_idx = (
            max(tput_nums, key=lambda x: x[0])[1] if tput_nums
            else min(time_nums, key=lambda x: x[0])[1] if time_nums
            else -1
        )
        html.append('<tr>')
        for i, cell in enumerate(row):
            cls = ' class="win"' if i == win_idx else ''
            if cell in ('—', '*not available*') or 'not present' in cell.lower():
                cls = ' class="na"'
            html.append(f'<td{cls}>{_html_inline(cell)}</td>')
        html.append('</tr>')
    html.append('</tbody></table>')
    return ''.join(html)


def render_html(md_text: str) -> str:
    """Convert a rendered benchmark Markdown report to a self-contained HTML page."""
    lines = md_text.splitlines()
    sections: list[tuple[str, str, bool]] = []  # (anchor, label, is_sub)
    body: list[str] = []
    h1_text = ''
    subtitle_text = ''
    i = 0

    while i < len(lines):
        line = lines[i]
        if line.startswith('# ') and not line.startswith('## '):
            h1_text = line[2:].strip(); i += 1; continue
        if line.startswith('## '):
            text = line[3:].strip(); anc = _html_slug(text)
            sections.append((anc, text, False))
            body.append(f'<h2 id="{anc}">{_html_inline(text)}</h2>'); i += 1; continue
        if line.startswith('### '):
            text = line[4:].strip(); anc = _html_slug(text)
            sections.append((anc, text, True))
            body.append(f'<h3 id="{anc}">{_html_inline(text)}</h3>'); i += 1; continue
        if line.startswith('_') and 'Generated from' in line and not subtitle_text:
            subtitle_text = _html_inline(line.strip('_').strip()); i += 1; continue
        if line.startswith('|'):
            tbl_lines = []
            while i < len(lines) and lines[i].startswith('|'):
                tbl_lines.append(lines[i]); i += 1
            body.append(_html_table(tbl_lines)); continue
        if line.startswith('- '):
            items = []
            while i < len(lines) and lines[i].startswith('- '):
                items.append(f'<li>{_html_inline(lines[i][2:])}</li>'); i += 1
            body.append('<ul>' + ''.join(items) + '</ul>'); continue
        if not line.strip(): i += 1; continue
        body.append(f'<p>{_html_inline(line)}</p>'); i += 1

    toc = ['<nav class="toc"><h2>Contents</h2>']
    for anc, lbl, is_sub in sections:
        cls = ' class="sub"' if is_sub else ''
        toc.append(f'<a href="#{anc}"{cls}>{_html_esc(lbl)}</a>')
    toc.append('</nav>')

    return (
        f'<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8">\n'
        f'<meta name="viewport" content="width=device-width,initial-scale=1">\n'
        f'<title>Benchmark Report &mdash; nw-data-benchmarks</title>\n'
        f'<style>{_HTML_CSS}</style></head>\n'
        f'<body><div class="layout">\n'
        f'{"".join(toc)}\n'
        f'<div class="page">\n'
        f'<h1>{_html_esc(h1_text)}</h1>\n'
        f'<p class="subtitle">{subtitle_text}</p>\n'
        f'{"".join(body)}\n'
        f'</div></div>\n'
        f'<script>{_HTML_SCRIPT}</script>\n'
        f'</body></html>'
    )


def generate_report(input_path: Path,
                    output_path: Path = DEFAULT_OUTPUT,
                    template_path: Path = TEMPLATE_PATH,
                    html: bool = False) -> tuple[Path, Path | None]:
    """Generate Markdown (and optionally HTML) report artifacts from results."""
    input_path = input_path.resolve()
    payload = load_results(input_path)
    try:
        template_text = template_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ReportGenerationError(
            f"Unable to read template file {template_path}: {exc}"
        ) from exc

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
        output_path, html_path = generate_report(
            input_path,
            output_path=args.output,
            template_path=args.template,
            html=args.html,
        )
        print(f"Markdown report: {output_path}")
        if html_path is not None:
            print(f"HTML report: {html_path}")
    except ReportGenerationError as exc:
        print(f"Error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())