from __future__ import annotations

try:
    from benchmark.reporting import (
        DEFAULT_OUTPUT,
        REPO_ROOT,
        RESULTS_DIR,
        TEMPLATE_PATH,
        ReportGenerationError,
        block_sort_key,
        metric_cell,
        build_key_observations,
        build_overview,
        build_section_placeholders,
        build_sections,
        build_summary,
        generate_report,
        latest_results_file,
        load_results,
        main,
        parse_args,
        render_html,
        render_report,
        render_section_results,
        section,
        timing_cell,
        validate_results,
    )
except ModuleNotFoundError as exc:
    if exc.name == "benchmark" and __package__ in (None, ""):
        raise SystemExit(
            "Run this CLI as a module from the repository root: "
            "python -m benchmark.scripts.generate_benchmark_report"
        ) from exc
    raise


__all__ = [
    "DEFAULT_OUTPUT",
    "REPO_ROOT",
    "RESULTS_DIR",
    "TEMPLATE_PATH",
    "ReportGenerationError",
    "block_sort_key",
    "metric_cell",
    "build_key_observations",
    "build_overview",
    "build_section_placeholders",
    "build_sections",
    "build_summary",
    "generate_report",
    "latest_results_file",
    "load_results",
    "main",
    "parse_args",
    "render_html",
    "render_report",
    "render_section_results",
    "section",
    "timing_cell",
    "validate_results",
]


if __name__ == "__main__":
    raise SystemExit(main())
