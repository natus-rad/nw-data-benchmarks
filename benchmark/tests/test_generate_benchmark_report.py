import tempfile
import unittest
from pathlib import Path

from benchmark.scripts.generate_benchmark_report import (
    ReportGenerationError,
    generate_report,
    latest_results_file,
    render_report,
    validate_results,
)


class GenerateBenchmarkReportTests(unittest.TestCase):
    def test_latest_results_file_picks_newest_matching_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "2026-01-01T00-00-00_benchmark_results.json"
            newer = root / "2026-01-02T00-00-00_benchmark_results.json"
            other = root / "ignore_me.json"
            older.write_text("{}", encoding="utf-8")
            newer.write_text("{}", encoding="utf-8")
            other.write_text("{}", encoding="utf-8")
            self.assertEqual(latest_results_file(root), newer)

    def test_render_report_includes_derived_sections_and_missing_category_notes(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 2,
                    "sample_freq": 100.0,
                    "total_stamps": 1000,
                    "duration_seconds": 10.0,
                }
            ],
            "benchmarks": [
                {"category": "random_access", "format": "parquet", "position": "0%", "wall_clock_seconds": 0.5, "mib_per_sec": 10.0},
                {"category": "random_access", "format": "edf", "position": "0%", "wall_clock_seconds": 1.0, "mib_per_sec": 5.0},
                {"category": "compression", "format": "parquet", "codec": "snappy", "file_size_bytes": 1024, "file_size_mib": 1.0, "wall_clock_seconds": 0.2},
                {"category": "compression", "format": "parquet", "codec": "zstd_3", "file_size_bytes": 512, "file_size_mib": 0.5, "wall_clock_seconds": 0.3},
                {
                    "category": "precision_loss",
                    "window_seconds": 60,
                    "num_channels": 2,
                    "worst_max_abs_error": 0.01,
                    "avg_snr_db": 95.0,
                    "channels": [{"channel": "Fp1", "max_abs_error": 0.01, "rms_error": 0.001, "snr_db": 95.0}],
                },
            ],
        }
        template = "# Report\n\n${overview}\n\n${summary}\n\n${key_observations}\n\n${sections}\n"
        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))
        self.assertIn("Random access (warm median 1-minute read)", rendered)
        self.assertIn("Ratio vs raw float32", rendered)
        self.assertIn("rows × channels × 4 bytes", rendered)
        self.assertIn("intentionally gives HDF5 a best-case seek/read path", rendered)
        self.assertIn("This category was not present in the input results file.", rendered)

    def test_latest_results_file_errors_when_directory_has_no_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ReportGenerationError):
                latest_results_file(Path(tmp))

    def test_render_report_keeps_j1_random_access_separate_from_j4_full_study(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 46,
                    "sample_freq": 256.0,
                    "total_stamps": 11854000,
                    "duration_seconds": 46304.7,
                }
            ],
            "benchmarks": [
                {
                    "category": "tuned_random_access",
                    "format": "parquet_snappy",
                    "block_size": "5m",
                    "variant": "tuned_pq_5m",
                    "window_seconds": 60,
                    "wall_clock_seconds": 0.066929,
                    "mib_per_sec": 40.271,
                },
                {
                    "category": "tuned_random_access",
                    "format": "hdf5_lz4",
                    "block_size": "5m",
                    "variant": "tuned_h5_5m",
                    "window_seconds": 60,
                    "wall_clock_seconds": 0.055349,
                    "mib_per_sec": 48.697,
                },
                {
                    "category": "tuned_full_study",
                    "format": "parquet_snappy",
                    "block_size": "5m",
                    "variant": "tuned_pq_5m",
                    "total_samples": 11854000,
                    "wall_clock_seconds": 14.594,
                    "mib_per_sec": 142.534,
                },
                {
                    "category": "tuned_full_study",
                    "format": "hdf5_lz4",
                    "block_size": "5m",
                    "variant": "tuned_h5_5m",
                    "total_samples": 11854000,
                    "wall_clock_seconds": 12.216,
                    "mib_per_sec": 170.279,
                },
            ],
        }
        template = "# Report\n\n## J\n\n${j1_results}\n\n${j4_results}${j_notes}\n"
        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))
        self.assertIn("| 5m | 0.0669s | 0.0553s |", rendered)
        self.assertIn("| 5m | 14.59s | 12.22s |", rendered)

    def test_render_report_includes_separate_k_section(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 46,
                    "sample_freq": 256.0,
                    "total_stamps": 11854000,
                    "duration_seconds": 46304.7,
                }
            ],
            "benchmarks": [
                {
                    "category": "tuned_random_access",
                    "format": "parquet_snappy",
                    "block_size": "5m",
                    "variant": "tuned_pq_5m",
                    "window_seconds": 60,
                    "wall_clock_seconds": 0.066929,
                    "mib_per_sec": 40.271,
                },
                {
                    "category": "baseline_random_access",
                    "format": "baseline_parquet",
                    "artifact": "Baseline input",
                    "variant": "baseline_parquet",
                    "window_seconds": 60,
                    "wall_clock_seconds": 0.081,
                    "mib_per_sec": 33.1,
                },
                {
                    "category": "baseline_full_study",
                    "format": "baseline_parquet",
                    "artifact": "Baseline input",
                    "variant": "baseline_parquet",
                    "total_samples": 11854000,
                    "wall_clock_seconds": 16.2,
                    "mib_per_sec": 128.0,
                },
            ],
        }
        template = "# Report\n\n${sections}\n"

        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))

        self.assertIn("## J. Tuned Format Comparison", rendered)
        self.assertIn("## K. Baseline Format Comparison", rendered)
        self.assertIn("Baseline input Parquet", rendered)
        self.assertIn("| Baseline input | 0.0810s / 33.1 MiB/s |", rendered)
        self.assertIn("| Baseline input | 16.20s / 128.0 MiB/s |", rendered)

    def test_render_report_handles_zero_artifact_size_without_crashing(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 2,
                    "sample_freq": 100.0,
                    "total_stamps": 1000,
                    "duration_seconds": 10.0,
                }
            ],
            "benchmarks": [
                {"category": "compression", "format": "parquet", "codec": "snappy", "file_size_bytes": 0, "file_size_mib": 0.0, "wall_clock_seconds": 0.2},
            ],
        }
        template = "# Report\n\n${f_results}\n"

        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))

        self.assertIn("n/a", rendered)

    def test_render_report_supports_per_section_placeholders(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 46,
                    "sample_freq": 256.0,
                    "total_stamps": 11854000,
                    "duration_seconds": 46304.7,
                }
            ],
            "benchmarks": [
                {"category": "random_access", "format": "parquet", "position": "0%", "wall_clock_seconds": 0.05, "mib_per_sec": 50.0},
                {"category": "random_access", "format": "edf", "position": "0%", "wall_clock_seconds": 0.10, "mib_per_sec": 25.0},
            ],
        }
        template = "# Report\n\n## A\n\n${a_results}\n\n## E\n\n${e_results}\n"
        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))
        self.assertIn("Parquet has the lowest warm-median 1-minute read time", rendered)
        self.assertIn("*This category was not present in the input results file.*", rendered)

    def test_render_report_remote_query_section_shows_received_settings(self) -> None:
        payload = {
            "run_id": "2026-03-23T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 46,
                    "sample_freq": 256.0,
                    "total_stamps": 11854000,
                    "duration_seconds": 46304.7,
                }
            ],
            "benchmarks": [
                {
                    "category": "remote_query",
                    "format": "parquet_float32_snappy",
                    "method": "duckdb_remote",
                    "channel_subset": "all",
                    "n_channels": 46,
                    "n_windows": 10,
                    "window_seconds": 600,
                    "total_wall_seconds": 4.0,
                    "avg_wall_per_window": 0.4,
                    "mib_per_sec": 120.0,
                },
                {
                    "category": "remote_query_full_study",
                    "format": "parquet_float32_snappy",
                    "method": "duckdb_full_study",
                    "channel_subset": "all",
                    "n_channels": 46,
                    "chunk_seconds": 300,
                    "n_chunks": 4,
                    "total_rows": 1000,
                    "total_wall_seconds": 3.0,
                    "mib_per_sec": 140.0,
                },
            ],
        }
        template = "# Report\n\n${i_results}\n"

        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))

        self.assertIn("Received settings: n_random_points=10; window_sec=600s; full_study_chunk_sec=300s.", rendered)
        self.assertIn("All reported remote timings are direct measurements.", rendered)

    def test_render_report_remote_query_section_handles_missing_full_study_chunk_setting(self) -> None:
        payload = {
            "run_id": "2026-03-23T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 46,
                    "sample_freq": 256.0,
                    "total_stamps": 11854000,
                    "duration_seconds": 46304.7,
                }
            ],
            "benchmarks": [
                {
                    "category": "remote_query",
                    "format": "edf",
                    "method": "full_download_then_read",
                    "channel_subset": "10-20 (19ch)",
                    "n_channels": 19,
                    "n_windows": 6,
                    "window_seconds": 120,
                    "download_estimated": True,
                    "total_wall_seconds": 8.0,
                    "avg_wall_per_window": 0.6,
                },
            ],
        }
        template = "# Report\n\n${i_results}\n"

        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))

        self.assertIn("Received settings: n_random_points=6; window_sec=120s; full_study_chunk_sec=not reported.", rendered)
        self.assertIn("EDF download time in this run is marked as estimated.", rendered)

    def test_render_report_preserves_variant_order_for_core_results(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 2,
                    "sample_freq": 100.0,
                    "total_stamps": 1000,
                    "duration_seconds": 10.0,
                }
            ],
            "benchmarks": [
                {"category": "random_access", "format": "pq_fast", "artifact_order": 0, "position": "0%", "wall_clock_seconds": 0.05, "mib_per_sec": 50.0},
                {"category": "random_access", "format": "h5_mid", "artifact_order": 1, "position": "0%", "wall_clock_seconds": 0.10, "mib_per_sec": 25.0},
                {"category": "random_access", "format": "pq_slow", "artifact_order": 2, "position": "0%", "wall_clock_seconds": 0.20, "mib_per_sec": 12.5},
            ],
        }
        template = "# Report\n\n${a_results}\n"

        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))

        self.assertLess(rendered.index("pq_fast"), rendered.index("h5_mid"))
        self.assertLess(rendered.index("h5_mid"), rendered.index("pq_slow"))

    def test_render_report_uses_results_filename_not_absolute_path(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [{"name": "demo", "channels": 2, "sample_freq": 100.0, "total_stamps": 1000, "duration_seconds": 10.0}],
            "benchmarks": [{"category": "random_access", "format": "parquet", "position": "0%", "wall_clock_seconds": 0.5, "mib_per_sec": 10.0}],
        }
        template = "# Report\n\n_Generated from `${source_file}` (run `${run_id}`)._\n"

        rendered = render_report(payload, template, Path("C:/secret/dev/path/demo_benchmark_results.json"))

        self.assertIn("Generated from `demo_benchmark_results.json`", rendered)
        self.assertNotIn("C:/secret/dev/path", rendered)

    def test_validate_results_requires_explicit_total_stamps(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [{"name": "demo", "channels": 2, "sample_freq": 100.0, "duration_seconds": 12.5}],
            "benchmarks": [{"category": "random_access", "format": "parquet", "position": "0%", "wall_clock_seconds": 0.5, "mib_per_sec": 10.0}],
        }

        with self.assertRaisesRegex(ReportGenerationError, "explicit total_stamps"):
            validate_results(payload, Path("benchmark/results/demo.json"))

    def test_generate_report_writes_markdown_and_html(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "demo_benchmark_results.json"
            template_path = root / "report.template.md"
            output_path = root / "report.md"
            input_path.write_text(
                """
                {
                  \"run_id\": \"2026-03-21T00-00-00\",
                  \"system\": {\"os\": \"Windows\", \"python\": \"3.12\", \"cpu_count\": 8, \"ram_gb\": 16},
                  \"studies\": [{\"name\": \"demo\", \"channels\": 2, \"sample_freq\": 100.0, \"total_stamps\": 1000, \"duration_seconds\": 10.0}],
                  \"benchmarks\": [{\"category\": \"random_access\", \"format\": \"parquet\", \"position\": \"0%\", \"wall_clock_seconds\": 0.5, \"mib_per_sec\": 10.0}]
                }
                """.strip(),
                encoding="utf-8",
            )
            template_path.write_text("# Report\n\n${overview}\n", encoding="utf-8")

            report_md, report_html = generate_report(
                input_path,
                output_path=output_path,
                template_path=template_path,
                html=True,
            )

            self.assertEqual(report_md, output_path.resolve())
            self.assertEqual(report_html, output_path.with_suffix(".html").resolve())
            self.assertTrue(report_md.exists())
            self.assertTrue(report_html.exists())


if __name__ == "__main__":
    unittest.main()