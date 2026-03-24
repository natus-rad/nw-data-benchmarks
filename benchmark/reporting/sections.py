from __future__ import annotations

import statistics
from typing import Any, Callable

from .common import (
    block_sort_key,
    format_mib,
    format_rate,
    format_seconds,
    formats_in_rows,
    label,
    markdown_table,
    median_by_format,
    metric_cell,
    raw_float32_mib,
    rows_for,
    timing_cell,
    unique_by,
    percent_sort_key,
    subset_sort_key,
)


def section_spec(title: str, placeholder: str, category: str, renderer: Callable) -> dict[str, Any]:
    return {
        "title": title,
        "placeholder": placeholder,
        "category": category,
        "renderer": renderer,
        "rows_getter": lambda payload, category=category: rows_for(payload, category),
    }


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
        section_spec("J.1 Random Access", "j1_results", "tuned_random_access", lambda rows, _: pivot_table(rows, "block_size", "format", "wall_clock_seconds", "time")),
        section_spec("J.2 Channel Subset", "j2_results", "tuned_channel_subset", lambda rows, _: pivot_table(rows, "block_size", "format", "wall_clock_seconds", "time")),
        section_spec("J.3 Throughput vs Window Size", "j3_results", "tuned_window_scaling", lambda rows, _: tuned_window_scaling_table(rows)),
        section_spec("J.4 Full-Study Sequential Read", "j4_results", "tuned_full_study", lambda rows, _: pivot_table(rows, "block_size", "format", "wall_clock_seconds", "time")),
    ]


def tuned_section_categories() -> list[str]:
    return [spec["category"] for spec in tuned_placeholder_specs()]


def baseline_placeholder_specs() -> list[dict[str, Any]]:
    return [
        section_spec("K.1 Random Access", "k1_results", "baseline_random_access", lambda rows, _: pivot_table(rows, "artifact", "format", "wall_clock_seconds", "time_rate", row_header="Artifact", row_sort_key=lambda value: str(value))),
        section_spec("K.2 Channel Subset", "k2_results", "baseline_channel_subset", lambda rows, _: pivot_table(rows, "artifact", "format", "wall_clock_seconds", "time_rate", row_header="Artifact", row_sort_key=lambda value: str(value))),
        section_spec("K.3 Throughput vs Window Size", "k3_results", "baseline_window_scaling", lambda rows, _: comparison_window_scaling_table(rows)),
        section_spec("K.4 Full-Study Sequential Read", "k4_results", "baseline_full_study", lambda rows, _: pivot_table(rows, "artifact", "format", "wall_clock_seconds", "time_rate", row_header="Artifact", row_sort_key=lambda value: str(value))),
    ]


def baseline_section_categories() -> list[str]:
    return [spec["category"] for spec in baseline_placeholder_specs()]


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
    note = f"{label(best_fmt)} has the lowest warm-cache-leaning median 1-minute read time across read positions at {format_seconds(best_val)}."
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
        table_rows.append([subset, *[metric_cell(pivot.get(fmt), "wall_clock_seconds", "mib_per_sec") if pivot.get(fmt) else "—" for fmt in formats]])
    return " ".join(notes) + "\n\n" + markdown_table(["Channels", *[label(fmt) for fmt in formats]], table_rows)


def render_remontage(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    ordered = sorted(rows, key=lambda row: row["wall_clock_seconds"])
    avg_share = statistics.mean(row["montage_seconds"] / row["wall_clock_seconds"] for row in ordered)
    table = markdown_table(
        ["Format", "Read", "Montage", "Total", "Montage share"],
        [[label(row["format"]), format_seconds(row["read_seconds"]), format_seconds(row["montage_seconds"]), timing_cell(row, "wall_clock_seconds"), f"{100 * row['montage_seconds'] / row['wall_clock_seconds']:.1f}%"] for row in ordered],
    )
    return f"Montage is a relatively small fraction of end-to-end time in this benchmark (average {avg_share * 100:.1f}% of total wall time).\n\n{table}"


def render_filter_pipeline(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    ordered = sorted(rows, key=lambda row: row["wall_clock_seconds"])
    table = markdown_table(
        ["Format", "Read", "Montage", "Filter", "Total", "Throughput"],
        [[label(row["format"]), format_seconds(row["read_seconds"]), format_seconds(row["montage_seconds"]), format_seconds(row["filter_seconds"]), timing_cell(row, "wall_clock_seconds"), format_rate(row["mib_per_sec"])] for row in ordered],
    )
    fastest = ordered[0]
    return f"For the full-study read → montage → filter pipeline, {label(fastest['format'])} is fastest at {format_seconds(fastest['wall_clock_seconds'])}.\n\n{table}"


def render_sliding_fft(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    ordered = sorted(rows, key=lambda row: row["wall_clock_seconds"])
    fft_windows = ordered[0].get("fft_windows_computed")
    table = markdown_table(
        ["Format", "Read", "Montage", "Filter", "FFT", "Total"],
        [[label(row["format"]), format_seconds(row["read_seconds"]), format_seconds(row["montage_seconds"]), format_seconds(row["filter_seconds"]), format_seconds(row["fft_seconds"]), timing_cell(row, "wall_clock_seconds")] for row in ordered],
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
    return f"Best measured throughput is {format_rate(best['mib_per_sec'])} from {label(best['format'])} at a {best['window_seconds']}s window.\n\n" + markdown_table(["Window", *[label(fmt) for fmt in formats]], table_rows)


def render_compression(rows: list[dict[str, Any]], payload: dict[str, Any]) -> str:
    study = payload["studies"][0]
    raw_mib = raw_float32_mib(study)
    ordered = sorted(rows, key=lambda row: row["file_size_bytes"])

    def _ratio_text(file_size_mib: float) -> str:
        return f"{raw_mib / file_size_mib:.2f}×" if file_size_mib > 0 else "n/a"

    table = markdown_table(
        ["Codec", "1-minute read", "Artifact size", "Ratio vs raw float32"],
        [[row["codec"], timing_cell(row, "wall_clock_seconds"), format_mib(row["file_size_mib"]), _ratio_text(row["file_size_mib"])] for row in ordered],
    )
    smallest = ordered[0]
    fastest = min(rows, key=lambda row: row["wall_clock_seconds"])
    note = f"Against a raw float32 baseline of {format_mib(raw_mib)}, the smallest Parquet artifact is {smallest['codec']} at {format_mib(smallest['file_size_mib'])}. The fastest warm-cache 1-minute read is {fastest['codec']} at {format_seconds(fastest['wall_clock_seconds'])}."
    return note + "\n\n" + table


def render_precision_loss(rows: list[dict[str, Any]], _: dict[str, Any]) -> str:
    row = rows[0]
    channels = sorted(row.get("channels", []), key=lambda item: item["max_abs_error"], reverse=True)[:5]
    intro = f"EDF round-trip quantization for a {row['window_seconds']}s window produced worst-case max absolute error {row['worst_max_abs_error']:.8f} µV with average SNR {row['avg_snr_db']:.2f} dB across {row['num_channels']} channels."
    if not channels:
        return intro
    table = markdown_table(
        ["Channel", "Max abs error (µV)", "RMS error (µV)", "SNR (dB)"],
        [[entry["channel"], f"{entry['max_abs_error']:.8f}", f"{entry['rms_error']:.8f}", f"{entry['snr_db']:.2f}"] for entry in channels],
    )
    return intro + "\n\nTop channels by max absolute error:\n\n" + table


def render_int32_storage(rows: list[dict[str, Any]], payload: dict[str, Any]) -> str:
    study = payload["studies"][0]
    raw_mib = raw_float32_mib(study)
    size_rows = sorted(unique_by(rows, lambda row: (row["mode"], row.get("codec"))), key=lambda row: row["file_size_bytes"])

    def _ratio_text(file_size_mib: float) -> str:
        return f"{raw_mib / file_size_mib:.2f}×" if file_size_mib > 0 else "n/a"

    size_table = markdown_table(
        ["Mode", "Codec", "Artifact size", "Ratio vs raw float32", "SNR vs float32"],
        [[row["mode"], row.get("codec", "—"), format_mib(row["file_size_mib"]), _ratio_text(row["file_size_mib"]), str(row["snr_vs_float32_db"])] for row in size_rows],
    )
    representative = []
    for mode in sorted({row["mode"] for row in rows}):
        mode_rows = [row for row in rows if row["mode"] == mode]
        preferred = [row for row in mode_rows if row.get("codec") in {"zstd", "zstd_3"}]
        representative.append(min(preferred or mode_rows, key=lambda row: row["wall_clock_seconds"]))
    representative.sort(key=lambda row: row["wall_clock_seconds"])
    perf_table = markdown_table(
        ["Mode", "Read method", "Codec", "1-minute read", "Throughput"],
        [[row["mode"], row.get("read_method", "—"), row.get("codec", "—"), timing_cell(row, "wall_clock_seconds"), format_rate(row["mib_per_sec"])] for row in representative],
    )
    smallest = size_rows[0]
    return f"The most compact measured storage mode is {smallest['mode']} ({smallest.get('codec', '—')}) at {format_mib(smallest['file_size_mib'])}.\n\n### H.1 Size / Precision\n\n{size_table}\n\n### H.2 Representative Read Performance\n\n{perf_table}"


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
        [[row["method"], label(row["format"]), row["channel_subset"], timing_cell(row, "total_wall_seconds"), format_seconds(row["avg_wall_per_window"]), format_rate(row["mib_per_sec"]) if "mib_per_sec" in row else "—"] for row in ordered],
    )
    note = "EDF download time in this run is marked as estimated." if any(bool(row.get("download_estimated")) for row in rows) else "All reported remote timings are direct measurements."
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
        sections.append("Per-variant artifact sizes are not currently recorded in the result JSON, so this generated report limits Benchmark J to performance-derived comparisons.")
    return "\n\n".join(sections)


def render_baseline_comparison(payload: dict[str, Any]) -> str:
    sections = ["This section runs the Benchmark J workload suite on the resolved baseline input artifact(s) only, without generating tuned comparison variants. Reported MiB/s values use theoretical decoded float32 payload size = rows × channels × 4 bytes."]
    random_rows = rows_for(payload, "baseline_random_access")
    if random_rows:
        sections.append("### K.1 Random Access\n\n" + pivot_table(random_rows, "artifact", "format", "wall_clock_seconds", "time_rate", row_header="Artifact", row_sort_key=lambda value: str(value)))
    subset_rows = rows_for(payload, "baseline_channel_subset")
    if subset_rows:
        sections.append("### K.2 Channel Subset\n\n" + pivot_table(subset_rows, "artifact", "format", "wall_clock_seconds", "time_rate", row_header="Artifact", row_sort_key=lambda value: str(value)))
    scaling_rows = rows_for(payload, "baseline_window_scaling")
    if scaling_rows:
        sections.append("### K.3 Throughput vs Window Size\n\n" + comparison_window_scaling_table(scaling_rows))
    full_rows = rows_for(payload, "baseline_full_study")
    if full_rows:
        sections.append("### K.4 Full-Study Sequential Read\n\n" + pivot_table(full_rows, "artifact", "format", "wall_clock_seconds", "time_rate", row_header="Artifact", row_sort_key=lambda value: str(value)))
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
    note = (
        f"Each cell shows the best throughput across all {qualifier_label}s tested for that window × format combination, with the winning {qualifier_label} in parentheses."
        if qualifier_key and qualifier_label
        else "Each cell shows the measured throughput for the baseline input artifact(s) at that window size."
    )
    return note + "\n\n" + markdown_table(["Window", *[label(c) for c in columns]], body)


def tuned_window_scaling_table(rows: list[dict[str, Any]]) -> str:
    return comparison_window_scaling_table(rows, qualifier_key="block_size", qualifier_label="block")


__all__ = [
    "baseline_placeholder_specs",
    "baseline_section_categories",
    "comparison_window_scaling_table",
    "pivot_table",
    "render_baseline_comparison",
    "render_channel_subset",
    "render_compression",
    "render_filter_pipeline",
    "render_int32_storage",
    "render_precision_loss",
    "render_random_access",
    "render_remote_query",
    "render_remontage",
    "render_sliding_fft",
    "render_tuned_comparison",
    "render_window_scaling",
    "report_section_specs",
    "section_spec",
    "tuned_placeholder_specs",
    "tuned_section_categories",
    "tuned_window_scaling_table",
]
