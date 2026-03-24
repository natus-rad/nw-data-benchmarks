from __future__ import annotations

import re
import statistics
from typing import Any, Callable

from benchmark.core.constants import FORMAT_LABELS, FORMAT_ORDER


class ReportGenerationError(RuntimeError):
    """Raised when benchmark report generation cannot proceed."""


MISSING_SECTION_MESSAGE = "*This category was not present in the input results file.*"


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
    extras = sorted(
        formats.difference(ordered),
        key=lambda fmt: (order_by_format.get(fmt, float("inf")), label(fmt).lower(), fmt),
    )
    return ordered + extras


def median_by_format(rows: list[dict[str, Any]], key: str) -> dict[str, float]:
    result = {}
    for fmt in formats_in_rows(rows):
        values = [float(row[key]) for row in rows if row.get("format") == fmt]
        if values:
            result[fmt] = statistics.median(values)
    return result


def unique_by(rows: list[dict[str, Any]], key_func: Callable[[dict[str, Any]], object]) -> list[dict[str, Any]]:
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


def _codec_label(value: str) -> str:
    return {
        "lz4": "LZ4",
        "snappy": "snappy",
        "zstd": "zstd",
        "gzip": "gzip",
        "none": "uncompressed",
    }.get(value, value)


def _layout_label(value: str) -> str:
    return {
        "col": "columnar",
        "columnar": "columnar",
        "rg": "row-group",
        "rowgroup": "row-group",
    }.get(value, value)


def _humanize_dynamic_format(value: str) -> str:
    if value.startswith("variant__"):
        value = value[len("variant__"):]

    pq_match = re.fullmatch(r"pq_([0-9]+(?:\.[0-9]+)?[smh])_([a-z0-9_]+)", value)
    if pq_match:
        block_size, codec = pq_match.groups()
        return f"Parquet {block_size} {_codec_label(codec)}"

    hdf5_match = re.fullmatch(r"h(?:df5|5)_([a-z]+)_([0-9]+(?:\.[0-9]+)?[smh])(?:_([a-z0-9_]+))?", value)
    if hdf5_match:
        layout, block_size, codec = hdf5_match.groups()
        layout_text = _layout_label(layout)
        codec_text = f" {_codec_label(codec)}" if codec else ""
        return f"HDF5 {layout_text} {block_size}{codec_text}"

    return value


def label(value: str) -> str:
    return FORMAT_LABELS.get(value, _humanize_dynamic_format(value))


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


def block_sort_key(value: str) -> float:
    value_str = str(value).strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([sm])", value_str)
    if match:
        amount = float(match.group(1))
        unit = match.group(2)
        return amount if unit == "s" else amount * 60.0
    try:
        return float(value_str) * 60.0
    except ValueError:
        return 10_000.0


__all__ = [
    "MISSING_SECTION_MESSAGE",
    "ReportGenerationError",
    "block_sort_key",
    "format_mib",
    "format_rate",
    "format_seconds",
    "formats_in_rows",
    "label",
    "markdown_table",
    "median_by_format",
    "metric_cell",
    "percent_sort_key",
    "raw_float32_mib",
    "rows_for",
    "rows_for_categories",
    "subset_sort_key",
    "timing_cell",
    "unique_by",
]
